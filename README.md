# go-streams

`go-streams` is a high-performance, concurrent, vectorized stream processing library for Go. It provides a type-safe, structured concurrency model for building efficient data pipelines that maximize CPU utilization while minimizing memory allocations.

## Features

- **Zero-Allocation Memory Management**: Uses smart `Vector[T]` pooling to minimize GC pressure and allocations during stream processing.
- **Structured Concurrency**: Built on `errgroup` and `context` to ensure pipelines are robust, cancellable, and never leak goroutines.
- **Parallelism**: First-class support for parallel processing operators (`ParMap`, `Dispatch`) to fully utilize multi-core systems.
- **Type Safe**: Leverages Go 1.18+ Generics for a clean, strictly typed API without `interface{}` overhead.
- **Vectorized Processing**: Processes data in batches (vectors) internally to reduce channel synchronization overhead and improve cache locality.

## Installation

To install `go-streams` in your Go project:

```bash
go get github.com/blitz/go-streams
```

## Core Concepts

- **Stream[T]**: A lazy, potentially infinite sequence of data of type `T`. It is the primary abstraction for building pipelines.
- **Vector[T]**: A batch of data items. The library processes items in vectors (arrays) rather than individually to amortize overhead.
- **Execution**: A handle to the lifecycle of a running pipeline stage, allowing for error propagation and graceful shutdown.
- **Operators**: Functions that transform, combine, or consume streams (e.g., `ParMap`, `MergeN`, `Reduce`).

## Usage Guide

### 1. Creating a Source

Sources are the starting point of a pipeline. You can create a stream from a generator function using `FromGenerator`.

```go
// Generates numbers 0 to 9
generator := func(emit func(int)) error {
    for i := 0; i < 10; i++ {
        emit(i)
    }
    return nil
}
source := stream.FromGenerator(generator)
```

### 2. Transforming Data

Transformers modify the data in the stream. `ParMap` allows you to apply a function to each element concurrently.

```go
// Double the number. Performed in parallel with a concurrency of 4.
doubled := stream.ParMap(source, 4, func(i int) (int, error) {
    return i * 2, nil
})
```

### 3. Combining Streams

You can merge multiple streams or dispatch a single stream into multiple outputs.

- **MergeN**: Combines multiple streams of the same type into one.
- **Dispatch**: Routes elements to different streams based on a condition.
- **Fusion**: Specialized operators like `MergeMap` or `DispatchAndMap2` combine operations for higher efficiency.

### 4. Consuming Data (Terminals)

Terminals trigger the execution of the pipeline and consume the results. `Reduce` is a common terminal operator.

```go
// Sum all numbers.
sum, err := stream.Reduce(ctx, doubled, 0, func(acc int, item int) int {
    return acc + item
})
```

## Example

Here is a complete example of how to use `go-streams` to build a pipeline.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"go-streams/pkg/stream"
)

func main() {
	ctx := context.Background()

	// 1. Create a Source
	generator := func(emit func(int)) error {
		for i := 0; i < 10; i++ {
			emit(i)
		}
		return nil
	}
	source := stream.FromGenerator(generator)

	// 2. Transform
	doubled := stream.ParMap(source, 4, func(i int) (int, error) {
		return i * 2, nil
	})

	// 3. Sink
	sum, err := stream.Reduce(ctx, doubled, 0, func(acc int, item int) int {
		return acc + item
	})

	if err != nil {
		panic(err)
	}

	fmt.Printf("Sum: %d\n", sum) // Output: Sum: 90
}
```

## Documentation

For detailed documentation of all functions and types, please refer to the code documentation or use `go doc`.

### Key Types

*   `Stream[T]`: The main pipeline type.
*   `Vector[T]`: The internal batch data structure.
*   `Execution`: Lifecycle management handle.

### Key Functions

*   `FromGenerator`: Create a source stream.
*   `ParMap`: Parallel mapping.
*   `MergeN`: Merge streams.
*   `Dispatch`: Split streams.
*   `Reduce`: Consume and fold stream.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue.

## License

MIT
