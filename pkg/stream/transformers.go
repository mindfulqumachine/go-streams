package stream

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// ============================================================================
// TRANSFORMATION OPERATORS (FUNCTORS)
// ============================================================================

// ParMap applies a mapper function to each element of the parent stream concurrently.
// It is optimized for CPU-bound tasks by processing items in parallel across multiple workers.
// The output order of elements is not guaranteed to match the input order.
//
// Parameters:
//   parent: The input stream to transform.
//   dop: The degree of parallelism (number of concurrent workers).
//   mapper: The function to apply to each element. It returns the transformed element or an error.
//
// Returns:
//   Stream[Out]: A new stream containing the transformed elements.
func ParMap[In, Out any](
	parent Stream[In],
	dop int,
	mapper func(In) (Out, error),
) Stream[Out] {
	dop = sanitizeDOP(dop)

	return Stream[Out]{
		pipe: func(ctx context.Context) (<-chan *Vector[Out], Execution) {
			// 1. Initialize parent stream and resources.
			in, parentExec := parent.pipe(ctx)
			out := make(chan *Vector[Out], ChannelBuffer) // Buffer scales with DOP
			pool := newVecPool[Out]()

			g, gCtx := errgroup.WithContext(ctx)

			// 2. Start workers.
			for i := 0; i < dop; i++ {
				g.Go(func() error {
					return parMapWorker(gCtx, in, out, pool, mapper)
				})
			}

			workerExec := executionFromErrGroup(g)

			// 3. Orchestration and Draining.
			// We must ensure 'in' is drained if workers stop prematurely (error/cancellation).
			drainerDone := make(chan struct{})
			go func() {
				defer close(drainerDone)
				<-workerExec.Done
				// If context is cancelled (or error occurred), workers stopped. Drain 'in'.
				if gCtx.Err() != nil {
					drain(in)
				}
				// Otherwise, workers finished normally because 'in' was closed.
			}()

			// 4. Combine executions.
			cleanup := func() { close(out) }
			exec := combineExecutions(workerExec, cleanup, parentExec, executionFromChan(drainerDone))

			return out, exec
		},
	}
}

// parMapWorker implements the optimized batch-level processing logic for ParMap.
// It consumes vectors from the input channel, maps the elements, and sends vectors to the output channel.
//
// Parameters:
//   ctx: The context for cancellation.
//   in: The input channel of vectors.
//   out: The output channel for processed vectors.
//   pool: The pool for allocating output vectors.
//   mapper: The function to apply to each element.
//
// Returns:
//   error: An error if processing fails or the context is cancelled.
func parMapWorker[In, Out any](
	ctx context.Context,
	in <-chan *Vector[In],
	out chan<- *Vector[Out],
	pool *vecPool[Out],
	mapper func(In) (Out, error),
) error {
	for {
		select {
		case batchIn, ok := <-in:
			if !ok {
				return nil // Input closed normally.
			}

			// Optimization: Pre-size the output vector to avoid appends/reallocations.
			batchOut := pool.Get()
			if cap(batchOut.Data) < len(batchIn.Data) {
				// If the pool gave us a vector that is too small (e.g. first run or varying input sizes),
				// we must ensure sufficient capacity.
				batchOut.Release()
				batchOut = &Vector[Out]{
					Data: make([]Out, len(batchIn.Data)),
					pool: pool,
				}
			} else {
				// Slice to the exact length needed.
				batchOut.Data = batchOut.Data[:len(batchIn.Data)]
			}

			// Process the batch (CPU-bound part).
			var mapErr error
			for i, item := range batchIn.Data {
				mappedItem, err := mapper(item)
				if err != nil {
					mapErr = err
					break
				}
				batchOut.Data[i] = mappedItem // Direct assignment, no append.
			}

			// Release input batch.
			batchIn.Release()

			if mapErr != nil {
				// Release output batch and return. Drainer handles 'in'.
				batchOut.Release()
				return fmt.Errorf("ParMap error: %w", mapErr)
			}

			// Send the processed batch downstream.
			select {
			case out <- batchOut:
			case <-ctx.Done():
				// Cancelled during send. Release buffer and return. Drainer handles 'in'.
				batchOut.Release()
				return ctx.Err()
			}

		case <-ctx.Done():
			return ctx.Err() // Cancelled while waiting. Drainer handles 'in'.
		}
	}
}
