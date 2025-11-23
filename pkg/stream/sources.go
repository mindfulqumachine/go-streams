package stream

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// ============================================================================
// SOURCE OPERATORS
// ============================================================================

// FromGenerator creates a source stream from a user-provided generator function.
// The generator function receives an 'emit' callback to push individual items into the stream.
// The generator runs in its own goroutine and should return nil on success or an error to fail the stream.
//
// Parameters:
//   gen: A function that generates data. It receives an `emit` function to output data and returns an error if generation fails.
//
// Returns:
//   Stream[T]: A new stream containing the generated items.
func FromGenerator[T any](gen func(emit func(T)) error) Stream[T] {
	return Stream[T]{
		pipe: func(ctx context.Context) (<-chan *Vector[T], Execution) {
			out := make(chan *Vector[T], ChannelBuffer)
			pool := newVecPool[T]()

			// Use errgroup for structured concurrency.
			g, gCtx := errgroup.WithContext(ctx)

			g.Go(func() error {
				defer close(out)
				batch := pool.Get()
				var opErr error

				// Emitter function (Continuation-Passing Style)
				emit := func(item T) {
					// Stop emitting if an error occurred or context is cancelled.
					if opErr != nil || gCtx.Err() != nil {
						return
					}

					batch.Data = append(batch.Data, item)
					if len(batch.Data) == VectorSize {
						select {
						case out <- batch:
							batch = pool.Get() // Ownership transferred.
						case <-gCtx.Done():
							opErr = gCtx.Err()
							return
						}
					}
				}

				genErr := gen(emit)

				// Handle cleanup of the final batch.
				if genErr != nil || opErr != nil {
					batch.Release()
					if genErr != nil {
						return genErr
					}
					return opErr
				}

				// Flush remaining items.
				if len(batch.Data) > 0 {
					select {
					case out <- batch:
					case <-gCtx.Done():
						batch.Release()
						return gCtx.Err()
					}
				} else {
					batch.Release()
				}
				return nil
			})

			return out, executionFromErrGroup(g)
		},
	}
}
