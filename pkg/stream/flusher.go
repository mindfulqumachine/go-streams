package stream

import (
	"context"
	"runtime"
)

// flusher is a helper to manage vector batching and flushing to an output pipe.
// It is not thread-safe and should be used by a single worker goroutine.
type flusher[T any] struct {
	out   *RingBuffer[*Vector[T]]
	pool  *vecPool[T]
	chunk *Vector[T]
}

func newFlusher[T any](out *RingBuffer[*Vector[T]], pool *vecPool[T]) *flusher[T] {
	return &flusher[T]{
		out:   out,
		pool:  pool,
		chunk: pool.Get(),
	}
}

// Add appends an item to the current batch. If the batch is full, it flushes it.
//
//go:inline
func (f *flusher[T]) Add(ctx context.Context, item T) error {
	f.chunk.Data = append(f.chunk.Data, item)
	if len(f.chunk.Data) == cap(f.chunk.Data) {
		return f.Flush(ctx)
	}
	return nil
}

// Flush sends the current batch to the output pipe if it's not empty.
// It acquires a new batch from the pool upon success.
func (f *flusher[T]) Flush(ctx context.Context) error {
	if len(f.chunk.Data) == 0 {
		return nil
	}

	for !f.out.Offer(f.chunk) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		runtime.Gosched()
	}

	f.chunk = f.pool.Get()
	return nil
}

// Close releases the current batch back to the pool.
// It should be called when the worker is done (e.g., via defer).
func (f *flusher[T]) Close() {
	if f.chunk != nil {
		f.chunk.Release()
		f.chunk = nil
	}
}
