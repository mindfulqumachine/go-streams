package stream

import (
	"context"
	"testing"

	"golang.org/x/sync/errgroup"
)

// BenchmarkFusedPipeline measures the throughput of a fused topology:
// [GenA, GenB] -> MergeN -> (Fused Map+Dispatch) -> [ReduceL, ReduceR]
// This eliminates the intermediate channel between Map and Dispatch.
// BenchmarkFusedPipeline measures throughput.
func BenchmarkFusedPipeline(b *testing.B) {

	const BatchSize = 1000000

	gen := func(count int) func(emit func(int)) error {
		return func(emit func(int)) error {
			for i := 0; i < count; i++ {
				emit(i)
			}
			return nil
		}
	}
	reducer := func(acc int, _ int) int { return acc + 1 }

	// Fused Worker Logic
	fusedWorker := func(
		ctx context.Context,
		in <-chan *Vector[int],
		outL chan<- *Vector[int],
		outR chan<- *Vector[int],
		poolL *vecPool[int],
		poolR *vecPool[int],
	) error {
		chunkL, chunkR := poolL.Get(), poolR.Get()

		defer func() {
			if chunkL != nil {
				chunkL.Release()
			}
			if chunkR != nil {
				chunkR.Release()
			}
		}()

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

		for {
			select {
			case batchIn, ok := <-in:
				if !ok {
					goto finalFlush
				}

				for _, item := range batchIn.Data {
					// --- FUSED LOGIC START ---
					mapped := item * 2
					if mapped%2 == 0 {
						chunkL.Data = append(chunkL.Data, mapped)
						if len(chunkL.Data) == VectorSize {
							if err := flushL(); err != nil {
								batchIn.Release()
								return err
							}
						}
					} else {
						chunkR.Data = append(chunkR.Data, mapped)
						if len(chunkR.Data) == VectorSize {
							if err := flushR(); err != nil {
								batchIn.Release()
								return err
							}
						}
					}
					// --- FUSED LOGIC END ---
				}
				batchIn.Release()

			case <-ctx.Done():
				return ctx.Err()
			}
		}

	finalFlush:
		if err := flushL(); err != nil {
			return err
		}
		if err := flushR(); err != nil {
			return err
		}
		chunkL.Release()
		chunkR.Release()
		chunkL = nil
		chunkR = nil
		return nil
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// 1. Setup Pipeline
		countA := BatchSize / 2
		countB := BatchSize - countA
		srcA := FromGenerator(gen(countA))
		srcB := FromGenerator(gen(countB))
		merged := MergeN(srcA, srcB)

		outL := make(chan *Vector[int], ChannelBuffer)
		outR := make(chan *Vector[int], ChannelBuffer)
		poolL, poolR := newVecPool[int](), newVecPool[int]()

		hubCtx := context.Background()
		in, parentExec := merged.pipe(hubCtx)
		g, gCtx := errgroup.WithContext(hubCtx)

		// Start Fused Workers (DOP=8)
		for w := 0; w < 8; w++ {
			g.Go(func() error {
				return fusedWorker(gCtx, in, outL, outR, poolL, poolR)
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
		cleanup := func() { close(outL); close(outR) }
		_ = combineExecutions(workerExec, cleanup, parentExec, executionFromChan(drainerDone))

		// 2. Execute and Wait
		type result struct {
			count int
			err   error
		}
		resCh := make(chan result, 2)
		consume := func(ch <-chan *Vector[int]) {
			total := 0
			for batch := range ch {
				for _, item := range batch.Data {
					total = reducer(total, item)
				}
				batch.Release()
			}
			resCh <- result{total, nil}
		}
		go consume(outL)
		go consume(outR)

		r1 := <-resCh
		r2 := <-resCh

		total := r1.count + r2.count
		if total != BatchSize {
			b.Fatalf("Expected %d, got %d", BatchSize, total)
		}
	}
}

// BenchmarkDispatchAndMap2 measures the new library operator.
func BenchmarkDispatchAndMap2(b *testing.B) {
	ctx := context.Background()
	const BatchSize = 1000000

	gen := func(count int) func(emit func(int)) error {
		return func(emit func(int)) error {
			for i := 0; i < count; i++ {
				emit(i)
			}
			return nil
		}
	}
	reducer := func(acc int, _ int) int { return acc + 1 }

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		countA := BatchSize / 2
		countB := BatchSize - countA
		srcA := FromGenerator(gen(countA))
		srcB := FromGenerator(gen(countB))
		merged := MergeN(srcA, srcB)

		// Use the new DispatchAndMap2 operator
		left, right := DispatchAndMap2(
			merged,
			8,
			func(item int) int { return (item * 2) % 2 }, // Router: 0 (Even) -> Left, 1 (Odd) -> Right
			func(item int) int { return item * 2 },       // Map1
			func(item int) int { return item * 2 },       // Map2
		)

		type result struct {
			count int
			err   error
		}
		resCh := make(chan result, 2)
		go func() { c, err := Reduce(ctx, left, 0, reducer); resCh <- result{c, err} }()
		go func() { c, err := Reduce(ctx, right, 0, reducer); resCh <- result{c, err} }()

		r1 := <-resCh
		r2 := <-resCh

		total := r1.count + r2.count
		if total != BatchSize {
			b.Fatalf("Expected %d, got %d", BatchSize, total)
		}
	}
}

// BenchmarkMergeMap measures the new library operator.
func BenchmarkMergeMap(b *testing.B) {
	ctx := context.Background()
	const BatchSize = 1000000

	gen := func(count int) func(emit func(int)) error {
		return func(emit func(int)) error {
			for i := 0; i < count; i++ {
				emit(i)
			}
			return nil
		}
	}
	// Note: MergeMap takes a simple mapper func(In) Out.
	simpleMapper := func(in int) int { return in * 2 }

	router := func(item int, emitL func(int), emitR func(int)) error {
		if item%2 == 0 {
			emitL(item)
		} else {
			emitR(item)
		}
		return nil
	}
	reducer := func(acc int, _ int) int { return acc + 1 }

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		countA := BatchSize / 2
		countB := BatchSize - countA
		srcA := FromGenerator(gen(countA))
		srcB := FromGenerator(gen(countB))

		// Use the new MergeMap operator
		// Fuses MergeN and ParMap
		mergedMapped := MergeMap(simpleMapper, srcA, srcB)

		// Dispatch (standard)
		left, right := Dispatch(mergedMapped, 8, router)

		type result struct {
			count int
			err   error
		}
		resCh := make(chan result, 2)
		go func() { c, err := Reduce(ctx, left, 0, reducer); resCh <- result{c, err} }()
		go func() { c, err := Reduce(ctx, right, 0, reducer); resCh <- result{c, err} }()

		r1 := <-resCh
		r2 := <-resCh

		total := r1.count + r2.count
		if total != BatchSize {
			b.Fatalf("Expected %d, got %d", BatchSize, total)
		}
	}
}
