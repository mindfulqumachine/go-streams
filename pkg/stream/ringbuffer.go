package stream

import (
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
)

// RingBuffer is a lock-free Single-Producer Single-Consumer (SPSC) queue.
// It is optimized for high-throughput passing of pointers between two goroutines.
// It is NOT safe for multiple producers or multiple consumers.
type RingBuffer[T any] struct {
	_padding0 [8]uint64
	head      uint64
	_padding1 [8]uint64
	tail      uint64
	_padding2 [8]uint64
	mask      uint64
	buffer    []T
	closed    int32
	mu        sync.Mutex
}

func NewRingBuffer[T any](capacity int) *RingBuffer[T] {
	if capacity < 2 {
		capacity = 2
	}
	// Round up to power of 2
	capacity--
	capacity |= capacity >> 1
	capacity = 1 << bits.Len(uint(capacity-1))
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

	if tail-head >= rb.mask+1 {
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
	rb.mu.Lock()
	defer rb.mu.Unlock()

	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)

	if head == tail {
		var zero T
		return zero, false // Empty
	}

	item := rb.buffer[head&rb.mask]
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
