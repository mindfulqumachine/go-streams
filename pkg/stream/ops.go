package stream

import (
	"context"
	"sync"

	"go-streams/pkg/stream/queue"
)

// ============================================================================
// FAN-OUT (BROADCAST)
// ============================================================================

// Broadcaster allows defining multiple output routes from a single input.
type Broadcaster[In any] struct {
	input      Stream[In]
	routes     []*broadcastRoute[In]
	controller *broadcastController[In]
}

type broadcastRoute[In any] struct {
	push  func(context.Context, In) error
	flush func(context.Context)
	close func()
}

type broadcastController[In any] struct {
	input     Stream[In]
	routes    []*broadcastRoute[In]
	done      chan struct{}
	startOnce sync.Once
}

func (c *broadcastController[In]) start(ctx context.Context) {
	c.startOnce.Do(func() {
		go c.run(ctx)
	})
}

func (c *broadcastController[In]) run(ctx context.Context) {
	defer close(c.done)
	defer func() {
		for _, r := range c.routes {
			r.close()
		}
	}()

	exec, ok := c.input.(executable[In])
	if !ok {
		return
	}

	err := exec.run(ctx, func(vec *Vector[In]) error {
		for _, item := range vec.Data {
			for _, r := range c.routes {
				if err := r.push(ctx, item); err != nil {
					return err
				}
			}
		}
		// Flush all routes after each batch to ensure latency
		for _, r := range c.routes {
			r.flush(ctx)
		}
		vec.Release()
		return nil
	})

	// Final flush
	for _, r := range c.routes {
		r.flush(ctx)
	}

	if err != nil {
		// Log
	}
}

// Broadcast creates a broadcaster and executes the setup function.
func Broadcast[In any](input Stream[In], setup func(*Broadcaster[In])) {
	ctrl := &broadcastController[In]{
		input: input,
		done:  make(chan struct{}),
	}

	b := &Broadcaster[In]{
		input:      input,
		controller: ctrl,
	}

	setup(b)

	// Pass routes to controller
	ctrl.routes = b.routes
}

// Split creates a new output stream by applying the mapper to the input.
func Split[In, Out any](b *Broadcaster[In], mapper func(In) Out) Stream[Out] {
	q := queue.NewRingBuffer[*Vector[Out]](1024)
	pool := newVecPool[Out](1024)
	var currentBatch *Vector[Out]

	push := func(ctx context.Context, item In) error {
		if currentBatch == nil {
			currentBatch = pool.Get()
		}
		currentBatch.Data = append(currentBatch.Data, mapper(item))
		if len(currentBatch.Data) >= 1024 { // Batch size
			for !q.Offer(currentBatch) {
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
			currentBatch = nil
		}
		return nil
	}

	flush := func(ctx context.Context) {
		if currentBatch != nil {
			if len(currentBatch.Data) > 0 {
				for !q.Offer(currentBatch) {
					if ctx.Err() != nil {
						return
					}
				}
			} else {
				currentBatch.Release()
			}
			currentBatch = nil
		}
	}

	b.routes = append(b.routes, &broadcastRoute[In]{
		push:  push,
		flush: flush,
		close: func() { q.Close() },
	})

	s := &broadcastBranchGeneric[Out]{
		q:       q,
		starter: b.controller.start,
		done:    b.controller.done,
	}
	s.self = s
	return s
}

type broadcastBranchGeneric[T any] struct {
	baseStream[T]
	q       queue.Queue[*Vector[T]]
	starter func(context.Context)
	done    <-chan struct{}
}

func (b *broadcastBranchGeneric[T]) run(ctx context.Context, next Receiver[T]) error {
	b.starter(ctx)
	for {
		vec, ok := b.q.Poll()
		if !ok {
			if b.q.IsClosed() {
				break
			}
			select {
			case <-b.done:
				if b.q.IsClosed() {
					return nil
				}
			default:
			}
			continue
		}
		if err := next(vec); err != nil {
			vec.Release()
			return err
		}
	}
	return nil
}

// Merge combines multiple streams into a single stream.
// It reads from all inputs concurrently and emits elements as they arrive.
func Merge[T any](inputs ...Stream[T]) Stream[T] {
	s := &mergedStream[T]{
		inputs: inputs,
	}
	s.self = s
	return s
}

type mergedStream[T any] struct {
	baseStream[T]
	inputs []Stream[T]
}

func (m *mergedStream[T]) run(ctx context.Context, next Receiver[T]) error {
	// We need to run all inputs concurrently.
	// Since inputs push data, we need to synchronize their pushes.
	// `next` is not thread-safe (it might be writing to a non-thread-safe sink or stage).
	// So we need a Mutex or a Channel to serialize.

	// High-perf: Use a MPSC queue?
	// Or just a Mutex if contention is low?
	// If inputs are Async, they are pushing from different goroutines.
	// Contention might be high.

	// Let's use a Mutex for correctness first.
	var mu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(m.inputs))

	for _, input := range m.inputs {
		go func(s Stream[T]) {
			defer wg.Done()

			exec, ok := s.(executable[T])
			if !ok {
				return
			}

			err := exec.run(ctx, func(vec *Vector[T]) error {
				mu.Lock()
				defer mu.Unlock()
				return next(vec)
			})
			if err != nil {
				// Handle error
			}
		}(input)
	}

	wg.Wait()
	return nil
}

// ============================================================================
// QUEUE STREAM (Helper)
// ============================================================================

type queueStream[T any] struct {
	baseStream[T]
	q queue.Queue[*Vector[T]]
}

func (qs *queueStream[T]) run(ctx context.Context, next Receiver[T]) error {
	for {
		vec, ok := qs.q.Poll()
		if !ok {
			if qs.q.IsClosed() {
				break
			}
			continue
		}
		if err := next(vec); err != nil {
			vec.Release()
			return err
		}
	}
	return nil
}
