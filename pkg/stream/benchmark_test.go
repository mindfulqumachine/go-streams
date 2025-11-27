package stream

import (
	"context"
	"testing"
)

func BenchmarkThroughput_Linear(b *testing.B) {
	ctx := context.Background()

	// Generator that yields b.N items
	gen := func(ctx context.Context, out chan<- int) error {
		for i := 0; i < b.N; i++ {
			out <- i
		}
		return nil
	}

	// Pipeline: Source -> Map -> Filter -> Sink
	pipeline := Source(gen).
		Via(Compose(
			Map(func(i int) int { return i * 2 }),
			Filter(func(i int) bool { return true }),
		)).
		To(NewDiscardSink[int]())

	b.ResetTimer()
	if err := pipeline.Run(ctx); err != nil {
		b.Fatalf("Run failed: %v", err)
	}
}

func BenchmarkThroughput_Async(b *testing.B) {
	ctx := context.Background()

	gen := func(ctx context.Context, out chan<- int) error {
		for i := 0; i < b.N; i++ {
			out <- i
		}
		return nil
	}

	// Pipeline: Source -> Async -> Map -> Sink
	pipeline := Source(gen).
		Async(1024).
		Via(Map(func(i int) int { return i * 2 })).
		To(NewDiscardSink[int]())

	b.ResetTimer()
	if err := pipeline.Run(ctx); err != nil {
		b.Fatalf("Run failed: %v", err)
	}
}

func BenchmarkThroughput_Range(b *testing.B) {
	ctx := context.Background()

	// Pipeline: Range -> Map -> Sink
	// No channel overhead.
	pipeline := Range(0, b.N).
		Via(Map(func(i int) int { return i * 2 })).
		To(NewDiscardSink[int]())

	b.ResetTimer()
	if err := pipeline.Run(ctx); err != nil {
		b.Fatalf("Run failed: %v", err)
	}
}
