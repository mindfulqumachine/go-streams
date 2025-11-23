package stream

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
)

// ============================================================================
// COMBINATORS AND ROUTING
// ============================================================================

// MergeN combines multiple parent streams of the same type into a single output stream.
// It uses a homogeneous fan-in pattern and is highly efficient (zero-copy).
// The output order is non-deterministic.
//
// Parameters:
//   parents: A variadic list of streams to merge.
//
// Returns:
//   Stream[T]: A single stream containing elements from all parent streams.
func MergeN[T any](parents ...Stream[T]) Stream[T] {
	return Stream[T]{
		pipe: func(ctx context.Context) (<-chan *Vector[T], Execution) {
			out := make(chan *Vector[T], ChannelBuffer*len(parents))
			g, gCtx := errgroup.WithContext(ctx)

			var parentExecs []Execution
			var mu sync.Mutex

			// Initialize all parent streams and start forwarders.
			for _, parent := range parents {
				in, exec := parent.pipe(gCtx)
				mu.Lock()
				parentExecs = append(parentExecs, exec)
				mu.Unlock()

				g.Go(func() error {
					return mergeWorker(gCtx, in, out)
				})
			}

			// Combine the forwarders (workerExec) and the parent executions.
			workerExec := executionFromErrGroup(g)
			cleanup := func() { close(out) }
			exec := combineExecutions(workerExec, cleanup, parentExecs...)

			return out, exec
		},
	}
}

// mergeWorker forwards vectors from an input channel to a shared output channel.
// It handles cancellation and ensures proper draining of the input on failure.
//
// Parameters:
//   ctx: The context for cancellation.
//   in: The input channel of vectors.
//   out: The shared output channel.
//
// Returns:
//   error: An error if the context is cancelled or forwarding fails.
func mergeWorker[T any](ctx context.Context, in <-chan *Vector[T], out chan<- *Vector[T]) error {
	// Forwarding loop with draining on cancellation.
	for {
		select {
		case batch, ok := <-in:
			if !ok {
				return nil
			}
			select {
			case out <- batch:
				// Ownership transferred (zero-copy).
			case <-ctx.Done():
				// Cancelled during send. Release pending and drain this specific input.
				batch.Release()
				drain(in)
				return ctx.Err()
			}
		case <-ctx.Done():
			// Cancelled while waiting. Drain this specific input.
			drain(in)
			return ctx.Err()
		}
	}
}

// Dispatch splits a single input stream into two separate output streams (Left and Right) based on a routing function.
// It processes elements in parallel up to 'dop' (degree of parallelism).
// This is a heterogeneous split (1-to-2).
//
// Parameters:
//   parent: The input stream to split.
//   dop: The degree of parallelism for the router.
//   router: A function that routes each item. It receives the item and two emit functions (emitLeft, emitRight).
//
// Returns:
//   Stream[L]: The "Left" output stream.
//   Stream[R]: The "Right" output stream.
func Dispatch[In, L, R any](
	parent Stream[In],
	dop int,
	router func(item In, emitLeft func(L), emitRight func(R)) error,
) (Stream[L], Stream[R]) {
	dop = sanitizeDOP(dop)

	// We need a mechanism to ensure the hub is initialized exactly once,
	// even if the output streams are consumed independently.
	var once sync.Once
	var hubExec Execution
	var outL chan *Vector[L]
	var outR chan *Vector[R]

	initHub := func(ignoredCtx context.Context) {
		// 1. Initialize resources.
		// Use a detached context for the hub and upstream pipeline to prevent
		// one consumer's cancellation from killing the shared pipeline.
		// TODO: For infinite streams, we should cancel this context when both consumers are done.
		hubCtx := context.Background()

		in, parentExec := parent.pipe(hubCtx)
		outL = make(chan *Vector[L], ChannelBuffer)
		outR = make(chan *Vector[R], ChannelBuffer)
		poolL, poolR := newVecPool[L](), newVecPool[R]()

		g, gCtx := errgroup.WithContext(hubCtx)

		// 2. Start workers.
		for i := 0; i < dop; i++ {
			g.Go(func() error {
				return dispatchWorker(gCtx, in, outL, outR, poolL, poolR, router)
			})
		}

		workerExec := executionFromErrGroup(g)

		// 3. Orchestration and Draining.
		drainerDone := make(chan struct{})
		go func() {
			defer close(drainerDone)
			<-workerExec.Done
			if gCtx.Err() != nil {
				drain(in)
			}
		}()

		// 4. Combine executions.
		cleanup := func() { close(outL); close(outR) }
		hubExec = combineExecutions(workerExec, cleanup, parentExec, executionFromChan(drainerDone))
	}

	// Define the lazy pipe functions for the output streams.
	streamL := Stream[L]{
		pipe: func(ctx context.Context) (<-chan *Vector[L], Execution) {
			once.Do(func() { initHub(ctx) })
			return outL, hubExec
		},
	}
	streamR := Stream[R]{
		pipe: func(ctx context.Context) (<-chan *Vector[R], Execution) {
			once.Do(func() { initHub(ctx) })
			return outR, hubExec
		},
	}

	return streamL, streamR
}

// dispatchWorker implements the routing logic for Dispatch.
// It consumes vectors from the input, applies the router, and sends results to the appropriate output channel.
//
// Parameters:
//   ctx: The context for cancellation.
//   in: The input channel of vectors.
//   outL: The output channel for "Left" items.
//   outR: The output channel for "Right" items.
//   poolL: The pool for "Left" vectors.
//   poolR: The pool for "Right" vectors.
//   router: The routing function.
//
// Returns:
//   error: An error if routing fails or the context is cancelled.
func dispatchWorker[In, L, R any](
	ctx context.Context,
	in <-chan *Vector[In],
	outL chan<- *Vector[L],
	outR chan<- *Vector[R],
	poolL *vecPool[L],
	poolR *vecPool[R],
	router func(In, func(L), func(R)) error,
) error {
	// Worker-local buffers minimize contention.
	chunkL, chunkR := poolL.Get(), poolR.Get()

	// Ensure local chunks are released on exit (handles cancellation/error case).
	defer func() {
		if chunkL != nil {
			chunkL.Release()
		}
		if chunkR != nil {
			chunkR.Release()
		}
	}()

	// Flush helpers. Return error on cancellation or downstream blockage.
	flushL := func() error {
		if len(chunkL.Data) == 0 {
			return nil
		}
		select {
		case outL <- chunkL:
			chunkL = poolL.Get()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	flushR := func() error {
		if len(chunkR.Data) == 0 {
			return nil
		}
		select {
		case outR <- chunkR:
			chunkR = poolR.Get()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Emitter functions (CPS).
	var workerErr error
	emitL := func(item L) {
		if workerErr != nil {
			return
		}
		chunkL.Data = append(chunkL.Data, item)
		if len(chunkL.Data) == VectorSize {
			if err := flushL(); err != nil {
				workerErr = err
			}
		}
	}
	emitR := func(item R) {
		if workerErr != nil {
			return
		}
		chunkR.Data = append(chunkR.Data, item)
		if len(chunkR.Data) == VectorSize {
			if err := flushR(); err != nil {
				workerErr = err
			}
		}
	}

	// Main routing loop.
	for {
		select {
		case batchIn, ok := <-in:
			if !ok {
				// Input closed. Proceed to final flush.
				goto finalFlush
			}

			// Process batch.
			for _, item := range batchIn.Data {
				// Check for errors from router or previous emissions.
				if workerErr != nil {
					break
				}
				if err := router(item, emitL, emitR); err != nil {
					workerErr = fmt.Errorf("Dispatch router error: %w", err)
					break
				}
			}
			batchIn.Release()

			if workerErr != nil {
				return workerErr
			}

		case <-ctx.Done():
			return ctx.Err() // Cancelled. defer handles release. Drainer handles 'in'.
		}
	}

finalFlush:
	// Only reached when input is closed (and assuming ctx is not Done).
	if err := flushL(); err != nil {
		return err
	}
	if err := flushR(); err != nil {
		return err
	}

	// Prevent deferred Release() as buffers might have been transferred or replaced by new ones.
	// We rely on the fact that flushL/R acquires new chunks upon success.
	// The final acquired chunks (which are empty) need to be released.
	chunkL.Release()
	chunkR.Release()
	// We must set the local variables to nil so the deferred Release (which calls Release on the variable) becomes a no-op.
	chunkL = nil
	chunkR = nil
	return nil
}
