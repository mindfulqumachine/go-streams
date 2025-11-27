package stream

import (
	"context"
	"sync"
)

// ============================================================================
// VECTOR & POOLING
// ============================================================================

// Vector holds a slice of data and a reference to its origin pool.
// It is the fundamental unit of data transport in the pipeline.
type Vector[T any] struct {
	Data []T
	pool *vecPool[T]
}

// Release returns the vector to its origin pool.
// It must be called exactly once by the consumer when the data is no longer needed.
func (v *Vector[T]) Release() {
	if v == nil {
		return
	}
	if v.pool != nil {
		v.pool.Put(v)
		v.pool = nil
	}
}

type vecPool[T any] struct {
	pool sync.Pool
	size int
}

func newVecPool[T any](size int) *vecPool[T] {
	return &vecPool[T]{
		size: size,
		pool: sync.Pool{
			New: func() any {
				return &Vector[T]{
					Data: make([]T, 0, size),
				}
			},
		},
	}
}

func (p *vecPool[T]) Get() *Vector[T] {
	v := p.pool.Get().(*Vector[T])
	v.Data = v.Data[:0] // Reset length, keep capacity
	v.pool = p
	return v
}

func (p *vecPool[T]) Put(v *Vector[T]) {
	p.pool.Put(v)
}

// ============================================================================
// BLUEPRINT INTERFACES (DEFINITION)
// ============================================================================

// Stream represents a source of data.
// It is a blueprint that can be run to produce a flow of vectors.
type Stream[T any] interface {
	// Via transforms this stream into another stream using a Flow.
	Via(flow Flow[T, T]) Stream[T]
	// Async inserts an asynchronous boundary with the given buffer size.
	Async(capacity int) Stream[T]
	// To connects this stream to a Sink, creating a Runnable.
	To(sink Sink[T]) Runnable
}

// Flow represents a transformation stage.
// It can be a simple function (Map, Filter) or a complex composed logic.
type Flow[In, Out any] interface {
	// Apply transforms an input stream into an output stream.
	// This is used by the Runtime to build the execution graph.
	Apply(input Stream[In]) Stream[Out]
}

// Sink represents a terminal stage.
type Sink[T any] interface {
	// Consume consumes the stream.
	Consume(ctx context.Context, input Stream[T]) error
}

// Runnable represents a complete pipeline ready to be executed.
type Runnable interface {
	// Run executes the pipeline.
	Run(ctx context.Context) error
}
