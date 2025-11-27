package stream

import (
	"context"
	"fmt"
	"go-streams/pkg/stream/queue"
	"sync"
)

// ============================================================================
// RUNTIME
// ============================================================================

// asyncStream represents a stream that should run asynchronously from its parent.
type asyncStream[T any] struct {
	baseStream[T]
	parent   Stream[T]
	capacity int
}

// ============================================================================
// RUNTIME EXECUTION
// ============================================================================

// Receiver is a function that accepts a vector.
type Receiver[T any] func(*Vector[T]) error

// executable is an internal interface that all stream implementations must satisfy.
type executable[T any] interface {
	run(ctx context.Context, next Receiver[T]) error
}

// execute runs the pipeline.
func execute[T any](ctx context.Context, stream Stream[T], sink Sink[T]) error {
	// Check if the stream is executable
	exec, ok := stream.(executable[T])
	if !ok {
		return fmt.Errorf("stream type %T is not executable", stream)
	}

	// For the Sink, we need to adapt it.
	// If the Sink is a standard sink, it expects to consume a stream.
	// But we are driving the execution (Push).
	// We need a Sink that accepts a Receiver?
	// Or we just run the stream and pipe it to the Sink?

	// If Sink is just a consumer, we can wrap the "next" receiver to call Sink.
	// But Sink.Consume(stream) implies Sink pulls.

	// Let's assume for this high-perf implementation, we want to support
	// sinks that implement an internal "push" interface, or we adapt.

	// Let's try to see if Sink implements a "ReceiverProvider" or similar.
	// For now, let's just implement a simple Sink that we can push to.

	// Hack: If Sink is our internal sink type, use it.
	// Otherwise, we fail for now.

	// Let's implement a standard "Collector" sink for testing.
	if s, ok := sink.(*CollectorSink[T]); ok {
		return exec.run(ctx, func(v *Vector[T]) error {
			s.collect(v)
			return nil
		})
	}

	// DiscardSink for benchmarking
	if _, ok := sink.(*DiscardSink[T]); ok {
		return exec.run(ctx, func(v *Vector[T]) error {
			v.Release()
			return nil
		})
	}

	return fmt.Errorf("unknown sink type: %T", sink)
}

// --- Source Execution ---

func (s *sourceStream[T]) run(ctx context.Context, next Receiver[T]) error {
	outCh := make(chan T, 1024)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(outCh)
		if err := s.generator(ctx, outCh); err != nil {
			// Log error?
		}
	}()

	// Vectorizer
	pool := newVecPool[T](1024)
	vec := pool.Get()

	for item := range outCh {
		vec.Data = append(vec.Data, item)
		if len(vec.Data) >= pool.size {
			if err := next(vec); err != nil {
				vec.Release()
				return err
			}
			vec = pool.Get()
		}
	}
	if len(vec.Data) > 0 {
		if err := next(vec); err != nil {
			vec.Release()
			return err
		}
	} else {
		vec.Release()
	}
	wg.Wait()
	return nil
}

// --- Map Execution ---

func (s *mappedStream[In, Out]) run(ctx context.Context, next Receiver[Out]) error {
	// We need to run the PARENT, and provide a receiver that maps and calls NEXT.
	parentExec, ok := s.parent.(executable[In])
	if !ok {
		return fmt.Errorf("parent stream is not executable")
	}

	pool := newVecPool[Out](1024)

	return parentExec.run(ctx, func(inVec *Vector[In]) error {
		// Map the vector
		// We allocate a new vector for output.
		// Optimization: If In == Out and we can mutate in place?
		// For now, safe copy.

		outVec := pool.Get()
		// Pre-allocate data slice
		if cap(outVec.Data) < len(inVec.Data) {
			outVec.Data = make([]Out, 0, len(inVec.Data))
		}

		for _, item := range inVec.Data {
			outVec.Data = append(outVec.Data, s.mapper(item))
		}

		// Release input vector as we are done with it
		inVec.Release()

		// Push output
		if err := next(outVec); err != nil {
			outVec.Release()
			return err
		}
		return nil
	})
}

// --- Filter Execution ---

func (s *filteredStream[T]) run(ctx context.Context, next Receiver[T]) error {
	parentExec, ok := s.parent.(executable[T])
	if !ok {
		return fmt.Errorf("parent stream is not executable")
	}

	pool := newVecPool[T](1024)

	return parentExec.run(ctx, func(inVec *Vector[T]) error {
		outVec := pool.Get()

		for _, item := range inVec.Data {
			if s.predicate(item) {
				outVec.Data = append(outVec.Data, item)
			}
		}

		inVec.Release()

		if len(outVec.Data) > 0 {
			if err := next(outVec); err != nil {
				outVec.Release()
				return err
			}
		} else {
			outVec.Release()
		}
		return nil
	})
}

// --- Async Execution ---

func (s *asyncStream[T]) run(ctx context.Context, next Receiver[T]) error {
	// 1. Create Queue
	q := queue.NewRingBuffer[*Vector[T]](s.capacity)

	// 2. Spawn Producer (Parent -> Queue)
	parentExec, ok := s.parent.(executable[T])
	if !ok {
		return fmt.Errorf("parent stream is not executable")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer q.Close()

		err := parentExec.run(ctx, func(vec *Vector[T]) error {
			// We need to clone the vector because the RingBuffer is async.
			// The upstream might reuse the vector if we just returned.
			// Wait, our Vector pool logic:
			// "Release returns the vector to its origin pool."
			// If we pass the pointer to RingBuffer, who owns it?
			// The Consumer (this function's next) will Release it.
			// So we can just pass the pointer.
			// BUT, the upstream `run` loop might reuse the struct if it wasn't released?
			// No, `pool.Get()` returns a fresh one (or reused one).
			// Once we pass it to `next`, we assume ownership transfer.
			// The upstream `mappedStream` does `inVec.Release()` immediately after calling `next`.
			// Wait! `mappedStream` calls `next(outVec)`.
			// If `next` (this function) puts it in a queue and returns, `mappedStream` continues.
			// `mappedStream` does NOT release `outVec`. It expects `next` to handle it.
			// Correct.

			// So we just Offer to Queue.
			// If Queue is full?
			for !q.Offer(vec) {
				// Spin or yield
				// runtime.Gosched()
				// Check context
				select {
				case <-ctx.Done():
					vec.Release() // Drop it
					return ctx.Err()
				default:
					// Busy wait for now
				}
			}
			return nil
		})
		if err != nil {
			// Log error
		}
	}()

	// 3. Consume Queue -> Next
	for {
		vec, ok := q.Poll()
		if !ok {
			if q.IsClosed() {
				break
			}
			// Spin/Yield
			continue
		}

		if err := next(vec); err != nil {
			vec.Release()
			return err
		}
	}
	wg.Wait()
	return nil
}

// --- Range Execution ---

func (s *rangeStream) run(ctx context.Context, next Receiver[int]) error {
	pool := newVecPool[int](1024)
	vec := pool.Get()

	for i := s.start; i < s.end; i++ {
		vec.Data = append(vec.Data, i)
		if len(vec.Data) >= pool.size {
			if err := next(vec); err != nil {
				vec.Release()
				return err
			}
			vec = pool.Get()
		}
	}
	if len(vec.Data) > 0 {
		if err := next(vec); err != nil {
			vec.Release()
			return err
		}
	} else {
		vec.Release()
	}
	return nil
}

// --- Sinks ---

type CollectorSink[T any] struct {
	results []T
	mu      sync.Mutex
}

func (s *CollectorSink[T]) Consume(ctx context.Context, stream Stream[T]) error {
	// This method is unused in the Push model if we cast to collectorSink.
	return nil
}

func (s *CollectorSink[T]) collect(v *Vector[T]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, v.Data...)
	v.Release()
}

// NewCollectorSink creates a sink that collects all elements into a slice.
// Useful for testing.
func NewCollectorSink[T any]() Sink[T] {
	return &CollectorSink[T]{}
}

// Results returns the collected results.
func (s *CollectorSink[T]) Results() []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Return copy
	res := make([]T, len(s.results))
	copy(res, s.results)
	return res
}

// DiscardSink discards all elements.
type DiscardSink[T any] struct{}

func (s *DiscardSink[T]) Consume(ctx context.Context, stream Stream[T]) error {
	return nil
}

func NewDiscardSink[T any]() Sink[T] {
	return &DiscardSink[T]{}
}
