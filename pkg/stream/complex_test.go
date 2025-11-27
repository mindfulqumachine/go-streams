package stream

import (
	"context"
	"testing"
)

func TestComplexPipeline(t *testing.T) {
	ctx := context.Background()

	// 1. Source
	count := 1000
	gen := func(ctx context.Context, out chan<- int) error {
		for i := 0; i < count; i++ {
			out <- i
		}
		return nil
	}
	source := Source(gen)

	// 2. Broadcast (Split into 2)
	var b1, b2 Stream[int]

	Broadcast(source, func(b *Broadcaster[int]) {
		// Branch 1: Map -> Async
		b1 = Split(b, func(i int) int { return i * 2 }).
			Async(128)

		// Branch 2: Map -> Async
		b2 = Split(b, func(i int) int { return i * 3 }).
			Async(128)
	})

	// 5. Merge
	merged := Merge(b1, b2)

	// 6. Sink
	sink := NewCollectorSink[int]()

	// 7. Run
	if err := merged.To(sink).Run(ctx); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// 8. Verify
	results := sink.(*CollectorSink[int]).Results()
	if len(results) != count*2 {
		t.Errorf("Expected %d results, got %d", count*2, len(results))
	}

	// Verify sums
	sum := 0
	for _, v := range results {
		sum += v
	}

	// Expected sum: Sum(0..999)*2 + Sum(0..999)*3 = Sum(0..999)*5
	expectedSum := 0
	for i := 0; i < count; i++ {
		expectedSum += i * 5
	}

	if sum != expectedSum {
		t.Errorf("Expected sum %d, got %d", expectedSum, sum)
	}
}
