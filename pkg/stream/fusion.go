package stream

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

// ============================================================================
// FUSED OPERATORS
// ============================================================================

// MergeMap (N-to-1 Fused Merge and Map) combines N streams and maps elements.
// It eliminates the intermediate channel between MergeN and ParMap, improving performance.
//
// Parameters:
//   mapper: The function to map elements from the merged stream.
//   parents: The input streams to merge.
//
// Returns:
//   Stream[Out]: A single stream containing the mapped elements.
func MergeMap[In, Out any](
	mapper func(In) Out,
	parents ...Stream[In],
) Stream[Out] {
	return Stream[Out]{
		pipe: func(ctx context.Context) (<-chan *Vector[Out], Execution) {
			// Output channel size scales with number of parents to avoid contention.
			out := make(chan *Vector[Out], ChannelBuffer*len(parents))
			g, gCtx := errgroup.WithContext(ctx)

			var parentExecs []Execution
			var mu sync.Mutex

			// Initialize all parent streams and start fused workers.
			for _, parent := range parents {
				in, exec := parent.pipe(gCtx)
				mu.Lock()
				parentExecs = append(parentExecs, exec)
				mu.Unlock()

				g.Go(func() error {
					return mergeMapWorker(gCtx, in, out, mapper)
				})
			}

			workerExec := executionFromErrGroup(g)
			cleanup := func() { close(out) }
			exec := combineExecutions(workerExec, cleanup, parentExecs...)

			return out, exec
		},
	}
}

// mergeMapWorker fuses the merge and map operations for a single input stream.
// It consumes from the input, applies the map function, and sends to the shared output.
//
// Parameters:
//   ctx: The context for cancellation.
//   in: The input channel.
//   out: The output channel.
//   mapper: The mapping function.
//
// Returns:
//   error: An error if the context is cancelled.
func mergeMapWorker[In, Out any](
	ctx context.Context,
	in <-chan *Vector[In],
	out chan<- *Vector[Out],
	mapper func(In) Out,
) error {
	pool := newVecPool[Out]()
	chunk := pool.Get()

	defer func() {
		if chunk != nil {
			chunk.Release()
		}
	}()

	flush := func() error {
		if len(chunk.Data) == 0 {
			return nil
		}
		select {
		case out <- chunk:
			chunk = pool.Get()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for {
		select {
		case batchIn, ok := <-in:
			if !ok {
				return flush()
			}

			for _, item := range batchIn.Data {
				chunk.Data = append(chunk.Data, mapper(item))
				if len(chunk.Data) == VectorSize {
					if err := flush(); err != nil {
						batchIn.Release()
						return err
					}
				}
			}
			batchIn.Release()

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// DispatchAndMap2 (1-to-2 Fused Map and Dispatch) routes elements to 2 outputs.
// It combines routing and mapping into a single step, reducing overhead.
//
// Parameters:
//   parent: The input stream.
//   dop: The degree of parallelism.
//   router: A function returning 0 for Out1 or 1 for Out2.
//   map1: The mapping function for the first output.
//   map2: The mapping function for the second output.
//
// Returns:
//   Stream[Out1]: The first output stream.
//   Stream[Out2]: The second output stream.
func DispatchAndMap2[In, Out1, Out2 any](
	parent Stream[In],
	dop int,
	router func(In) int,
	map1 func(In) Out1,
	map2 func(In) Out2,
) (Stream[Out1], Stream[Out2]) {
	dop = sanitizeDOP(dop)
	var once sync.Once
	var hubExec Execution
	var out1 chan *Vector[Out1]
	var out2 chan *Vector[Out2]

	initHub := func(ignoredCtx context.Context) {
		hubCtx := context.Background()
		in, parentExec := parent.pipe(hubCtx)
		out1 = make(chan *Vector[Out1], ChannelBuffer)
		out2 = make(chan *Vector[Out2], ChannelBuffer)
		pool1, pool2 := newVecPool[Out1](), newVecPool[Out2]()

		g, gCtx := errgroup.WithContext(hubCtx)

		for i := 0; i < dop; i++ {
			g.Go(func() error {
				return dispatchAndMap2Worker(gCtx, in, out1, out2, pool1, pool2, router, map1, map2)
			})
		}

		workerExec := executionFromErrGroup(g)
		drainerDone := make(chan struct{})
		go func() {
			defer close(drainerDone)
			<-workerExec.Done
			if gCtx.Err() != nil {
				drain(in)
			}
		}()

		cleanup := func() { close(out1); close(out2) }
		hubExec = combineExecutions(workerExec, cleanup, parentExec, executionFromChan(drainerDone))
	}

	stream1 := Stream[Out1]{
		pipe: func(ctx context.Context) (<-chan *Vector[Out1], Execution) {
			once.Do(func() { initHub(ctx) })
			return out1, hubExec
		},
	}
	stream2 := Stream[Out2]{
		pipe: func(ctx context.Context) (<-chan *Vector[Out2], Execution) {
			once.Do(func() { initHub(ctx) })
			return out2, hubExec
		},
	}

	return stream1, stream2
}

// dispatchAndMap2Worker performs fused dispatch and map operations for 2 outputs.
//
// Parameters:
//   ctx: Context for cancellation.
//   in: Input channel.
//   out1: Output channel for first stream.
//   out2: Output channel for second stream.
//   pool1: Pool for first output type.
//   pool2: Pool for second output type.
//   router: Routing function.
//   map1: Map function for first output.
//   map2: Map function for second output.
//
// Returns:
//   error: Error on failure or cancellation.
func dispatchAndMap2Worker[In, Out1, Out2 any](
	ctx context.Context,
	in <-chan *Vector[In],
	out1 chan<- *Vector[Out1],
	out2 chan<- *Vector[Out2],
	pool1 *vecPool[Out1],
	pool2 *vecPool[Out2],
	router func(In) int,
	map1 func(In) Out1,
	map2 func(In) Out2,
) error {
	chunk1, chunk2 := pool1.Get(), pool2.Get()

	defer func() {
		if chunk1 != nil {
			chunk1.Release()
		}
		if chunk2 != nil {
			chunk2.Release()
		}
	}()

	flush1 := func() error {
		if len(chunk1.Data) == 0 {
			return nil
		}
		select {
		case out1 <- chunk1:
			chunk1 = pool1.Get()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	flush2 := func() error {
		if len(chunk2.Data) == 0 {
			return nil
		}
		select {
		case out2 <- chunk2:
			chunk2 = pool2.Get()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for {
		select {
		case batchIn, ok := <-in:
			if !ok {
				if err := flush1(); err != nil {
					return err
				}
				return flush2()
			}

			for _, item := range batchIn.Data {
				idx := router(item)
				if idx == 0 {
					chunk1.Data = append(chunk1.Data, map1(item))
					if len(chunk1.Data) == VectorSize {
						if err := flush1(); err != nil {
							batchIn.Release()
							return err
						}
					}
				} else {
					chunk2.Data = append(chunk2.Data, map2(item))
					if len(chunk2.Data) == VectorSize {
						if err := flush2(); err != nil {
							batchIn.Release()
							return err
						}
					}
				}
			}
			batchIn.Release()

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// DispatchAndMap3 (1-to-3 Fused Map and Dispatch) routes elements to 3 outputs.
// It combines routing and mapping into a single step for three destination streams.
//
// Parameters:
//   parent: The input stream.
//   dop: The degree of parallelism.
//   router: A function returning 0, 1, or 2 for Out1, Out2, Out3 respectively.
//   map1: The mapping function for the first output.
//   map2: The mapping function for the second output.
//   map3: The mapping function for the third output.
//
// Returns:
//   Stream[Out1]: The first output stream.
//   Stream[Out2]: The second output stream.
//   Stream[Out3]: The third output stream.
func DispatchAndMap3[In, Out1, Out2, Out3 any](
	parent Stream[In],
	dop int,
	router func(In) int,
	map1 func(In) Out1,
	map2 func(In) Out2,
	map3 func(In) Out3,
) (Stream[Out1], Stream[Out2], Stream[Out3]) {
	dop = sanitizeDOP(dop)
	var once sync.Once
	var hubExec Execution
	var out1 chan *Vector[Out1]
	var out2 chan *Vector[Out2]
	var out3 chan *Vector[Out3]

	initHub := func(ignoredCtx context.Context) {
		hubCtx := context.Background()
		in, parentExec := parent.pipe(hubCtx)
		out1 = make(chan *Vector[Out1], ChannelBuffer)
		out2 = make(chan *Vector[Out2], ChannelBuffer)
		out3 = make(chan *Vector[Out3], ChannelBuffer)
		pool1, pool2, pool3 := newVecPool[Out1](), newVecPool[Out2](), newVecPool[Out3]()

		g, gCtx := errgroup.WithContext(hubCtx)

		for i := 0; i < dop; i++ {
			g.Go(func() error {
				return dispatchAndMap3Worker(gCtx, in, out1, out2, out3, pool1, pool2, pool3, router, map1, map2, map3)
			})
		}

		workerExec := executionFromErrGroup(g)
		drainerDone := make(chan struct{})
		go func() {
			defer close(drainerDone)
			<-workerExec.Done
			if gCtx.Err() != nil {
				drain(in)
			}
		}()

		cleanup := func() { close(out1); close(out2); close(out3) }
		hubExec = combineExecutions(workerExec, cleanup, parentExec, executionFromChan(drainerDone))
	}

	stream1 := Stream[Out1]{
		pipe: func(ctx context.Context) (<-chan *Vector[Out1], Execution) {
			once.Do(func() { initHub(ctx) })
			return out1, hubExec
		},
	}
	stream2 := Stream[Out2]{
		pipe: func(ctx context.Context) (<-chan *Vector[Out2], Execution) {
			once.Do(func() { initHub(ctx) })
			return out2, hubExec
		},
	}
	stream3 := Stream[Out3]{
		pipe: func(ctx context.Context) (<-chan *Vector[Out3], Execution) {
			once.Do(func() { initHub(ctx) })
			return out3, hubExec
		},
	}

	return stream1, stream2, stream3
}

// dispatchAndMap3Worker performs fused dispatch and map operations for 3 outputs.
//
// Parameters:
//   ctx: Context for cancellation.
//   in: Input channel.
//   out1, out2, out3: Output channels.
//   pool1, pool2, pool3: Vector pools.
//   router: Routing function.
//   map1, map2, map3: Map functions.
//
// Returns:
//   error: Error on failure or cancellation.
func dispatchAndMap3Worker[In, Out1, Out2, Out3 any](
	ctx context.Context,
	in <-chan *Vector[In],
	out1 chan<- *Vector[Out1],
	out2 chan<- *Vector[Out2],
	out3 chan<- *Vector[Out3],
	pool1 *vecPool[Out1],
	pool2 *vecPool[Out2],
	pool3 *vecPool[Out3],
	router func(In) int,
	map1 func(In) Out1,
	map2 func(In) Out2,
	map3 func(In) Out3,
) error {
	chunk1, chunk2, chunk3 := pool1.Get(), pool2.Get(), pool3.Get()

	defer func() {
		if chunk1 != nil {
			chunk1.Release()
		}
		if chunk2 != nil {
			chunk2.Release()
		}
		if chunk3 != nil {
			chunk3.Release()
		}
	}()

	flush1 := func() error {
		if len(chunk1.Data) == 0 {
			return nil
		}
		select {
		case out1 <- chunk1:
			chunk1 = pool1.Get()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	flush2 := func() error {
		if len(chunk2.Data) == 0 {
			return nil
		}
		select {
		case out2 <- chunk2:
			chunk2 = pool2.Get()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	flush3 := func() error {
		if len(chunk3.Data) == 0 {
			return nil
		}
		select {
		case out3 <- chunk3:
			chunk3 = pool3.Get()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for {
		select {
		case batchIn, ok := <-in:
			if !ok {
				if err := flush1(); err != nil {
					return err
				}
				if err := flush2(); err != nil {
					return err
				}
				return flush3()
			}

			for _, item := range batchIn.Data {
				idx := router(item)
				switch idx {
				case 0:
					chunk1.Data = append(chunk1.Data, map1(item))
					if len(chunk1.Data) == VectorSize {
						if err := flush1(); err != nil {
							batchIn.Release()
							return err
						}
					}
				case 1:
					chunk2.Data = append(chunk2.Data, map2(item))
					if len(chunk2.Data) == VectorSize {
						if err := flush2(); err != nil {
							batchIn.Release()
							return err
						}
					}
				default:
					chunk3.Data = append(chunk3.Data, map3(item))
					if len(chunk3.Data) == VectorSize {
						if err := flush3(); err != nil {
							batchIn.Release()
							return err
						}
					}
				}
			}
			batchIn.Release()

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
