package queue

import (
	"runtime"
	"sync/atomic"
)

// Queue is the interface for passing data between stages.
type Queue[T any] interface {
	// Offer adds an item to the queue. Returns false if full.
	Offer(item T) bool
	// Poll removes an item from the queue. Returns item and true if found, or zero and false if empty.
	Poll() (T, bool)
	// Close marks the queue as closed.
	Close()
	// IsClosed returns true if the queue is closed and empty.
	IsClosed() bool
}

// RingBuffer is a lock-free Single-Producer Single-Consumer (SPSC) queue.
// It is optimized for high-throughput passing of pointers between two goroutines.
// It is NOT safe for multiple producers or multiple consumers.
type RingBuffer[T any] struct {
	// Cache line padding to prevent false sharing
	_padding0 [8]uint64
	head      uint64
	_padding1 [8]uint64
	tail      uint64
	_padding2 [8]uint64
	mask      uint64
	buffer    []T
	closed    int32
}

// NewRingBuffer creates a new SPSC RingBuffer with the given capacity.
// Capacity is rounded up to the next power of 2.
func NewRingBuffer[T any](capacity int) *RingBuffer[T] {
	if capacity < 2 {
		capacity = 2
	}
	// Round up to power of 2
	// See: https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
	capacity--
	capacity |= capacity >> 1
	capacity |= capacity >> 2
	capacity |= capacity >> 4
	capacity |= capacity >> 8
	capacity |= capacity >> 16
	capacity++

	return &RingBuffer[T]{
		buffer: make([]T, capacity),
		mask:   uint64(capacity - 1),
	}
}

// Offer adds an item to the queue.
// Returns false if the queue is full.
// Only safe for a single producer.
func (rb *RingBuffer[T]) Offer(item T) bool {
	tail := atomic.LoadUint64(&rb.tail)
	head := atomic.LoadUint64(&rb.head)

	if tail-head > rb.mask {
		return false // Full
	}

	rb.buffer[tail&rb.mask] = item
	atomic.StoreUint64(&rb.tail, tail+1)
	return true
}

// Poll removes an item from the queue.
// Returns false if the queue is empty.
// Only safe for a single consumer.
func (rb *RingBuffer[T]) Poll() (T, bool) {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)

	if head == tail {
		var zero T
		return zero, false // Empty
	}

	item := rb.buffer[head&rb.mask]
	// Help GC by nil-ing out the slot if T is a pointer
	var zero T
	rb.buffer[head&rb.mask] = zero

	atomic.StoreUint64(&rb.head, head+1)
	return item, true
}

func (rb *RingBuffer[T]) Close() {
	atomic.StoreInt32(&rb.closed, 1)
}

func (rb *RingBuffer[T]) IsClosed() bool {
	return atomic.LoadInt32(&rb.closed) == 1 && atomic.LoadUint64(&rb.head) == atomic.LoadUint64(&rb.tail)
}

// SpinPoll polls the queue, spinning briefly if empty before returning false.
// This is useful to reduce latency when the consumer is faster than the producer.
func (rb *RingBuffer[T]) SpinPoll(spins int) (T, bool) {
	for i := 0; i < spins; i++ {
		if item, ok := rb.Poll(); ok {
			return item, true
		}
		runtime.Gosched()
	}
	var zero T
	return zero, false
}
