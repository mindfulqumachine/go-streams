package queue

import (
	"sync"
	"testing"
)

func TestRingBuffer_SPSC(t *testing.T) {
	rb := NewRingBuffer[int](1024)
	count := 100_000

	var wg sync.WaitGroup
	wg.Add(2)

	// Producer
	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			for !rb.Offer(i) {
				// Spin wait
			}
		}
		rb.Close()
	}()

	// Consumer
	go func() {
		defer wg.Done()
		received := 0
		for {
			val, ok := rb.Poll()
			if !ok {
				if rb.IsClosed() {
					break
				}
				continue
			}
			if val != received {
				t.Errorf("Expected %d, got %d", received, val)
			}
			received++
		}
		if received != count {
			t.Errorf("Expected %d items, got %d", count, received)
		}
	}()

	wg.Wait()
}

func TestRingBuffer_Capacity(t *testing.T) {
	rb := NewRingBuffer[int](4) // Should round to 4

	if !rb.Offer(1) {
		t.Fatal("Failed to offer 1")
	}
	if !rb.Offer(2) {
		t.Fatal("Failed to offer 2")
	}
	if !rb.Offer(3) {
		t.Fatal("Failed to offer 3")
	}
	if !rb.Offer(4) {
		t.Fatal("Failed to offer 4")
	}
	if rb.Offer(5) {
		t.Fatal("Should be full")
	}

	val, ok := rb.Poll()
	if !ok || val != 1 {
		t.Fatal("Failed to poll 1")
	}

	if !rb.Offer(5) {
		t.Fatal("Failed to offer 5 after poll")
	}
}
