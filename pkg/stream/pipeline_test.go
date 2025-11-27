package stream

import (
	"context"
	"testing"
)

func TestLinearPipeline(t *testing.T) {
	ctx := context.Background()

	// 1. Source
	gen := func(ctx context.Context, out chan<- int) error {
		for i := 0; i < 100; i++ {
			out <- i
		}
		return nil
	}
	source := Source(gen)

	// 2. Map (Double)
	doubled := source.Via(Map(func(i int) int {
		return i * 2
	}))

	// 3. Filter (Even only - redundant but testing logic)
	filtered := doubled.Via(Filter(func(i int) bool {
		return i%2 == 0
	}))

	// 4. Sink
	sink := NewCollectorSink[int]()

	// 5. Run
	if err := filtered.To(sink).Run(ctx); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// 8. Verify
	results := sink.(*CollectorSink[int]).Results()
	if len(results) != 100 { // Assuming 'count' was meant to be 100 based on the generator
		t.Errorf("Expected %d results, got %d", 100, len(results))
	}
	for i, v := range results {
		expected := i * 2
		if v != expected {
			t.Errorf("Index %d: expected %d, got %d", i, expected, v)
		}
	}
}

func TestAsyncPipeline(t *testing.T) {
	ctx := context.Background()

	gen := func(ctx context.Context, out chan<- int) error {
		for i := 0; i < 1000; i++ {
			out <- i
		}
		return nil
	}

	// Source -> Async -> Map -> Sink
	pipeline := Source(gen).
		Async(128).
		Via(Map(func(i int) int { return i + 1 }))

	sink := NewCollectorSink[int]()

	if err := pipeline.To(sink).Run(ctx); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	results := sink.(*CollectorSink[int]).Results()
	if len(results) != 1000 {
		t.Errorf("Expected 1000 results, got %d", len(results))
	}
}
