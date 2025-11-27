package stream

import (
	"context"
)

// ============================================================================
// BASE STREAM
// ============================================================================

type baseStream[T any] struct {
	self Stream[T]
}

func (s *baseStream[T]) Via(flow Flow[T, T]) Stream[T] {
	return flow.Apply(s.self)
}

func (s *baseStream[T]) Async(capacity int) Stream[T] {
	as := &asyncStream[T]{
		parent:   s.self,
		capacity: capacity,
	}
	as.self = as
	return as
}

func (s *baseStream[T]) To(sink Sink[T]) Runnable {
	return &runnableImpl[T]{
		stream: s.self,
		sink:   sink,
	}
}

// ============================================================================
// CONCRETE STAGES
// ============================================================================

type sourceStream[T any] struct {
	baseStream[T]
	generator func(context.Context, chan<- T) error
}

type rangeStream struct {
	baseStream[int]
	start, end int
}

type mappedStream[In, Out any] struct {
	baseStream[Out]
	parent Stream[In]
	mapper func(In) Out
}

type filteredStream[T any] struct {
	baseStream[T]
	parent    Stream[T]
	predicate func(T) bool
}

// Source creates a Stream from a generator function.
func Source[T any](gen func(context.Context, chan<- T) error) Stream[T] {
	s := &sourceStream[T]{
		generator: gen,
	}
	s.self = s
	return s
}

// Range creates a stream of integers from start to end (exclusive).
// It is optimized to not use channels.
func Range(start, end int) Stream[int] {
	s := &rangeStream{
		start: start,
		end:   end,
	}
	s.self = s
	return s
}

// ============================================================================
// FLOW IMPLEMENTATIONS
// ============================================================================

// Map creates a Flow that applies a function to each element.
func Map[In, Out any](f func(In) Out) Flow[In, Out] {
	return &mapFlow[In, Out]{mapper: f}
}

type mapFlow[In, Out any] struct {
	mapper func(In) Out
}

func (m *mapFlow[In, Out]) Apply(input Stream[In]) Stream[Out] {
	s := &mappedStream[In, Out]{
		parent: input,
		mapper: m.mapper,
	}
	s.self = s
	return s
}

// Filter creates a Flow that keeps elements satisfying the predicate.
func Filter[T any](predicate func(T) bool) Flow[T, T] {
	return &filterFlow[T]{predicate: predicate}
}

type filterFlow[T any] struct {
	predicate func(T) bool
}

func (f *filterFlow[T]) Apply(input Stream[T]) Stream[T] {
	s := &filteredStream[T]{
		parent:    input,
		predicate: f.predicate,
	}
	s.self = s
	return s
}

// Compose composes two flows: f1 then f2.
func Compose[A, B, C any](f1 Flow[A, B], f2 Flow[B, C]) Flow[A, C] {
	return &composedFlow[A, B, C]{f1: f1, f2: f2}
}

type composedFlow[A, B, C any] struct {
	f1 Flow[A, B]
	f2 Flow[B, C]
}

func (c *composedFlow[A, B, C]) Apply(input Stream[A]) Stream[C] {
	return c.f2.Apply(c.f1.Apply(input))
}

// ============================================================================
// RUNNABLE
// ============================================================================

type runnableImpl[T any] struct {
	stream Stream[T]
	sink   Sink[T]
}

func (r *runnableImpl[T]) Run(ctx context.Context) error {
	return execute(ctx, r.stream, r.sink)
}
