package stream

import (
	"context"
	"testing"
)

// TestPipelineIntegrity verifies that no items are lost in the complex pipeline.
func TestPipelineIntegrity(t *testing.T) {
	count := 100
	ctx := context.Background()

	gen := func(count int) func(emit func(int)) error {
		return func(emit func(int)) error {
			for i := 0; i < count; i++ {
				emit(i)
			}
			return nil
		}
	}

	mapper := func(in int) (int, error) {
		return in * 2, nil
	}

	router := func(item int, emitL func(int), emitR func(int)) error {
		if item%2 == 0 {
			emitL(item)
		} else {
			emitR(item)
		}
		return nil
	}

	reducer := func(acc int, _ int) int {
		return acc + 1
	}

	// 1. Sources
	countA := count / 2
	countB := count - countA
	srcA := FromGenerator(gen(countA))
	srcB := FromGenerator(gen(countB))

	// 2. Merge
	merged := MergeN(srcA, srcB)

	// 3. Map
	mapped := ParMap(merged, 8, mapper)

	// 4. Dispatch
	left, right := Dispatch(mapped, 8, router)

	// 5. Reduce
	type result struct {
		count int
		err   error
	}
	resCh := make(chan result, 2)

	go func() {
		c, err := Reduce(ctx, left, 0, reducer)
		resCh <- result{c, err}
	}()
	go func() {
		c, err := Reduce(ctx, right, 0, reducer)
		resCh <- result{c, err}
	}()

	r1 := <-resCh
	r2 := <-resCh

	if r1.err != nil {
		t.Fatalf("Left failed: %v", r1.err)
	}
	if r2.err != nil {
		t.Fatalf("Right failed: %v", r2.err)
	}

	total := r1.count + r2.count
	if total != count {
		t.Fatalf("Expected %d items, got %d (Left: %d, Right: %d)", count, total, r1.count, r2.count)
	}
}

// BenchmarkComplexPipeline measures throughput.
// We run the pipeline b.N times, each time processing a fixed batch of items.
func BenchmarkComplexPipeline(b *testing.B) {
	ctx := context.Background()
	// Fixed batch size per op to amortize startup costs but keep it measurable.
	const BatchSize = 1000000

	gen := func(count int) func(emit func(int)) error {
		return func(emit func(int)) error {
			for i := 0; i < count; i++ {
				emit(i)
			}
			return nil
		}
	}
	mapper := func(in int) (int, error) { return in * 2, nil }
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
		// 1. Setup Pipeline
		countA := BatchSize / 2
		countB := BatchSize - countA
		srcA := FromGenerator(gen(countA))
		srcB := FromGenerator(gen(countB))
		merged := MergeN(srcA, srcB)
		mapped := ParMap(merged, 8, mapper)
		left, right := Dispatch(mapped, 8, router)

		// 2. Execute and Wait
		type result struct {
			count int
			err   error
		}
		resCh := make(chan result, 2)
		go func() { c, err := Reduce(ctx, left, 0, reducer); resCh <- result{c, err} }()
		go func() { c, err := Reduce(ctx, right, 0, reducer); resCh <- result{c, err} }()

		r1 := <-resCh
		r2 := <-resCh

		if r1.err != nil {
			b.Fatalf("Pipeline failed: %v", r1.err)
		}
		if r2.err != nil {
			b.Fatalf("Pipeline failed: %v", r2.err)
		}

		total := r1.count + r2.count
		if total != BatchSize {
			b.Fatalf("Expected %d, got %d", BatchSize, total)
		}
	}
}
