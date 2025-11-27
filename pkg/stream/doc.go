// Package stream provides a high-performance, vectorized, and concurrent stream processing framework.
//
// It leverages Go generics to offer type-safe data pipelines that are optimized for throughput
// and memory efficiency. The library uses batching (vectors) internally to amortize the cost
// of channel operations and synchronization.
//
// Key features include:
//   - Zero-allocation vector pooling.
//   - Structured concurrency for robust error handling and cancellation.
//   - Parallel operators (ParMap, Dispatch) for multi-core utilization.
//
// Basic usage involves creating a Source, applying Transformations, and consuming the result with a Terminal operator.
package stream
