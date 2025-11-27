# go-streams: High-Performance Stream Processing in Go

`go-streams` is a high-throughput, low-latency stream processing library for Go. It is designed for building complex, type-safe data pipelines that can process millions of events per second on a single machine.

## Key Features

*   **High Performance**: Built on a push-based runtime with zero-allocation `Vector[T]` batching and lock-free `RingBuffer` queues.
*   **Type Safety**: Fully leverages Go generics for compile-time type checking of complex pipelines.
*   **Composability**: Construct pipelines using simple, reusable `Flow` components (`Map`, `Filter`) that can be composed functionally.
*   **Flexible Topologies**: Support for linear pipelines, fan-out (`Broadcast`), fan-in (`Merge`), and asynchronous boundaries.
*   **Backpressure**: Implicit backpressure management via bounded queues.

## Installation

```bash
go get github.com/blitz/go-streams
```

## Core Concepts

*   **Stream[T]**: A source of data.
*   **Flow[In, Out]**: A transformation stage (e.g., `Map`, `Filter`). Flows can be composed (`f1.Via(f2)`).
*   **Sink[T]**: A terminal stage that consumes data.
*   **Async**: Explicit asynchronous boundaries to pipeline work across goroutines.

## Example

Here is a complex example demonstrating filtering, mapping, broadcasting, and merging:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"go-streams/pkg/stream"
)

func main() {
	ctx := context.Background()

	// 1. Source: Generator
	src := stream.Source(func(ctx context.Context, out chan<- int) error {
		for i := 0; i < 100; i++ {
			out <- i
		}
		return nil
	})

	// 2. Linear Pipeline: Filter -> Map
	// Use .Apply() for type-changing or complex flows.
	processed := stream.Compose(
		stream.Filter(func(i int) bool { return i%2 == 0 }), // Keep evens
		stream.Map(func(i int) int { return i * 10 }),       // Multiply by 10
	).Apply(src)

	// 3. Broadcast (Fan-Out)
	// Split the stream into two branches with different types.
	var textStream stream.Stream[string]
	var metricStream stream.Stream[int]

	stream.Broadcast(processed, func(b *stream.Broadcaster[int]) {
		// Branch 1: Convert to string
		textStream = stream.Split(b, func(i int) string {
			return fmt.Sprintf("value-%d", i)
		})

		// Branch 2: Keep as int, maybe for metrics
		metricStream = stream.Split(b, func(i int) int {
			return i
		})
	})

	// 4. Async Processing & Merge (Fan-In)
	// Process branches in parallel and merge them (conceptually, here we merge to a sink)
	// For demonstration, let's just run them.

	// Sinks
	logSink := stream.NewCollectorSink[string]()
	metricSink := stream.NewCollectorSink[int]()

	// Run the pipelines
	// Note: In a real app, you might merge these or run them in an ErrGroup.
	go textStream.To(logSink).Run(ctx)
	metricStream.To(metricSink).Run(ctx)
}
```

## Architecture

### Vectorized Processing
Data is processed in batches (`Vector[T]`) to minimize function call overhead and improve cache locality. Vectors are pooled to ensure zero allocations during steady-state processing.

### Asynchronous Boundaries
You can insert an async boundary anywhere in the pipeline using `.Async(capacity)`. This spawns a new goroutine and a ring buffer, allowing the upstream and downstream stages to run in parallel (pipelining).

```go
stream.Source(gen).
    Via(heavyMap).
    Async(1024). // <--- Pipeline break
    Via(anotherHeavyMap).
    To(sink)
```

### Broadcast & Merge
*   **Broadcast**: Splits a stream into multiple branches. It uses a closure-based API to allow for type-safe heterogeneous splits.
*   **Merge**: Combines multiple streams of the same type into a single stream.

## Performance

`go-streams` is optimized for high throughput. Internal benchmarks on a standard developer machine show:

*   **RingBuffer**: ~2.2 Billion ops/sec (~0.45 ns/op)
*   **Pipeline (Range Source)**: ~150 Million ops/sec (~6.68 ns/op)
*   **Pipeline (Channel Source)**: ~10 Million ops/sec (~100 ns/op)

To run the benchmarks yourself:

```bash
go test -bench=. -benchmem -v ./pkg/stream
```

## License

MIT
