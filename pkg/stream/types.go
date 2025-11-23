package stream

import (
	"context"
)

// ============================================================================
// ZERO-ALLOCATION MEMORY MANAGEMENT (SMART VECTORS)
// ============================================================================

// Vector holds a slice of data and a reference to its origin pool.
// It is the fundamental unit of data transport in the pipeline.
type Vector[T any] struct {
	// Data is the slice containing the actual elements of the batch.
	Data []T
	// pool is the reference to the pool this vector originated from.
	// It is used to return the vector to the correct pool upon Release.
	pool *vecPool[T]
}

// Release returns the vector to its origin pool for reuse.
//
// It must be called exactly once by the consumer when the data is no longer needed
// to prevent memory leaks and ensure efficient pooling.
//
// This method takes no parameters and returns nothing.
func (v *Vector[T]) Release() {
	if v == nil {
		return
	}
	if v.pool != nil {
		v.pool.Put(v)
		// Defensive programming: nil out pool to prevent double-release.
		// We do NOT nil out Data, to preserve capacity for reuse.
		v.pool = nil
	}
}

// ============================================================================
// CORE TYPES & ABSTRACTIONS (STRUCTURED CONCURRENCY)
// ============================================================================

// Execution represents the lifecycle of a running pipeline stage.
// It allows monitoring for completion and retrieving any errors that occurred during processing.
type Execution struct {
	// Done is a channel that signals completion (successful or failed) when closed.
	Done <-chan struct{}
	// Err is a function that returns the error that caused the pipeline to stop, or nil if successful.
	Err func() error
}

// Stream represents a lazy, vectorized, parallel data pipeline.
// It encapsulates the logic to build and run a processing stage.
type Stream[T any] struct {
	// pipe initializes the stage, returning the data channel and execution handle.
	//
	// Parameters:
	//   ctx: The context for cancellation and lifecycle management.
	//
	// Returns:
	//   <-chan *Vector[T]: A channel yielding batches of data.
	//   Execution: A handle to monitor the stage's lifecycle.
	pipe func(ctx context.Context) (<-chan *Vector[T], Execution)
}
