# go-streams API Documentation

This document provides a detailed reference for the `go-streams` library, a high-performance, type-safe stream processing framework for Go.

## Core Types

### `Stream[T]`
The fundamental interface representing a sequence of data items of type `T`. Streams are lazy and push-based.

### `Flow[In, Out]`
Represents a reusable transformation stage that converts a `Stream[In]` into a `Stream[Out]`.

### `Sink[T]`
Represents the terminal stage of a pipeline that consumes data.

### `Runnable`
Represents a fully constructed pipeline (Source -> Stages -> Sink) that is ready to be executed.

---

## Sources

### `Source[T]`
```go
func Source[T any](gen func(context.Context, chan<- T) error) Stream[T]
```
Creates a new stream from a generator function. The generator runs in a separate goroutine and pushes data to a channel.
- **gen**: A function that accepts a context and a write-only channel. It should write items to the channel and return `nil` on success or an error on failure.

**Example:**
```go
src := stream.Source(func(ctx context.Context, out chan<- int) error {
    out <- 1
    out <- 2
    return nil
})
```

### `Range`
```go
func Range(start, end int) Stream[int]
```
Creates a high-performance stream of integers from `start` (inclusive) to `end` (exclusive). This source is optimized to bypass channels and push directly to the runtime, offering superior throughput.

**Example:**
```go
// Generates 0, 1, 2, ..., 999
src := stream.Range(0, 1000)
```

---

## Transformations

### `Map`
```go
func Map[In, Out any](f func(In) Out) Flow[In, Out]
```
Creates a flow that transforms each input element using the provided function `f`.
- **f**: A pure function that takes an `In` and returns an `Out`.

**Example:**
```go
// Int -> String
flow := stream.Map(func(i int) string { return fmt.Sprint(i) })
```

### `Filter`
```go
func Filter[T any](predicate func(T) bool) Flow[T, T]
```
Creates a flow that only emits elements that satisfy the `predicate`.
- **predicate**: A function that returns `true` to keep the item, `false` to drop it.

**Example:**
```go
// Keep evens
flow := stream.Filter(func(i int) bool { return i%2 == 0 })
```

### `Compose`
```go
func Compose[A, B, C any](f1 Flow[A, B], f2 Flow[B, C]) Flow[A, C]
```
Composes two flows `f1` and `f2` into a single flow. Data passes through `f1` then `f2`. This is useful for creating reusable composite stages.

**Example:**
```go
// Filter then Map
flow := stream.Compose(
    stream.Filter(isEven),
    stream.Map(double),
)
```

---

## Stream Methods

### `Via`
```go
func (s Stream[T]) Via(flow Flow[T, T]) Stream[T]
```
Transforms the stream using a `Flow` that preserves the type `T`.
*Note: For type-changing transformations (e.g., `int` to `string`), use `Flow.Apply(Stream)` instead.*

**Example:**
```go
processed := src.Via(stream.Filter(isEven))
```

### `Async`
```go
func (s Stream[T]) Async(capacity int) Stream[T]
```
Inserts an asynchronous boundary (buffer) into the pipeline. This spawns a new goroutine for the downstream stages, allowing them to run in parallel with upstream stages (pipelining).
- **capacity**: The size of the internal ring buffer.

**Example:**
```go
// Decouple heavy stages
src.Via(heavyStage1).Async(1024).Via(heavyStage2)
```

### `To`
```go
func (s Stream[T]) To(sink Sink[T]) Runnable
```
Connects the stream to a `Sink`, creating a `Runnable` pipeline.

**Example:**
```go
runnable := src.To(sink)
```

---

## Combinators

### `Broadcast`
```go
func Broadcast[In any](input Stream[In], setup func(*Broadcaster[In]))
```
Splits a stream into multiple branches. The `setup` function is used to define the branches. This API allows for heterogeneous output types.

**Example:**
```go
stream.Broadcast(src, func(b *stream.Broadcaster[int]) {
    // Branch 1: Integers
    stream.Split(b, func(i int) int { return i })
    
    // Branch 2: Strings
    stream.Split(b, func(i int) string { return fmt.Sprint(i) })
})
```

### `Split`
```go
func Split[In, Out any](b *Broadcaster[In], mapper func(In) Out) Stream[Out]
```
Defines a new branch in a `Broadcast` setup. It applies a `mapper` to convert the input to the branch's specific output type.

### `Merge`
```go
func Merge[T any](inputs ...Stream[T]) Stream[T]
```
Merges multiple streams of the same type into a single stream. Order is not guaranteed.

**Example:**
```go
merged := stream.Merge(stream1, stream2)
```

---

## Sinks

### `NewCollectorSink`
```go
func NewCollectorSink[T any]() Sink[T]
```
Creates a sink that collects all items into a slice. Useful for testing and debugging. Use `.Results()` to retrieve the data.

### `NewDiscardSink`
```go
func NewDiscardSink[T any]() Sink[T]
```
Creates a sink that discards all data. Useful for benchmarking and performance testing.

---

## Execution

### `Run`
```go
func (r Runnable) Run(ctx context.Context) error
```
Executes the pipeline. Blocks until completion or error.

**Example:**
```go
if err := runnable.Run(context.Background()); err != nil {
    log.Fatal(err)
}
```
