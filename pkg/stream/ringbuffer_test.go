package stream

import (
	"runtime"
	"sync"
	"testing"
)

func TestRingBuffer(t *testing.T) {
	rb := NewRingBuffer[int](8)

	// Concurrent SPSC
	const N = 100000
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			for !rb.Offer(i) {
				runtime.Gosched()
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			var val int
			var ok bool
			for {
				val, ok = rb.Poll()
				if ok {
					break
				}
				runtime.Gosched()
			}
			if val != i {
				t.Errorf("Expected %d, got %d. Head=%d, Tail=%d", i, val, rb.head, rb.tail)
				return
			}
		}
	}()

	wg.Wait()
}

func BenchmarkRingBuffer(b *testing.B) {
	rb := NewRingBuffer[int](1024)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if rb.Offer(1) {
				rb.Poll()
			}
		}
	})
}
