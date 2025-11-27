package stream

import (
	"context"
	"fmt"
	"testing"
)

func TestBroadcastClosure(t *testing.T) {
	ctx := context.Background()

	// Source: 0..9
	gen := func(ctx context.Context, out chan<- int) error {
		for i := 0; i < 10; i++ {
			out <- i
		}
		return nil
	}
	source := Source(gen)

	// Broadcast with Closure
	var s1 Stream[string]
	var s2 Stream[int]

	Broadcast(source, func(b *Broadcaster[int]) {
		s1 = Split(b, func(i int) string { return fmt.Sprintf("str-%d", i) })
		s2 = Split(b, func(i int) int { return i * 10 })
	})

	// Sinks
	sink1 := NewCollectorSink[string]()
	sink2 := NewCollectorSink[int]()

	// Run
	errCh := make(chan error, 2)
	go func() { errCh <- s1.To(sink1).Run(ctx) }()
	go func() { errCh <- s2.To(sink2).Run(ctx) }()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("Run failed: %v", err)
		}
	}

	// Verify
	res1 := sink1.(*CollectorSink[string]).Results()
	res2 := sink2.(*CollectorSink[int]).Results()

	if len(res1) != 10 {
		t.Errorf("Expected 10 strings, got %d", len(res1))
	}
	if len(res2) != 10 {
		t.Errorf("Expected 10 ints, got %d", len(res2))
	}

	if res1[0] != "str-0" {
		t.Errorf("Expected str-0, got %s", res1[0])
	}
	if res2[0] != 0 {
		t.Errorf("Expected 0, got %d", res2[0])
	}
	if res2[1] != 10 {
		t.Errorf("Expected 10, got %d", res2[1])
	}
}
