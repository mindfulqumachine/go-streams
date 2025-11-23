package stream

import "context"

// ============================================================================
// TERMINALS (SINKS / FOLDS)
// ============================================================================

// Reduce consumes the entire stream and folds the results into a single accumulator using the provided function.
// It blocks until the stream is exhausted, the context is cancelled, or an error occurs.
//
// Parameters:
//   ctx: The context for cancellation.
//   s: The stream to reduce.
//   init: The initial value of the accumulator.
//   fn: The reduction function that combines the accumulator and the next element.
//
// Returns:
//   Acc: The final accumulated value.
//   error: An error if the pipeline fails or is cancelled.
func Reduce[T, Acc any](
	ctx context.Context,
	s Stream[T],
	init Acc,
	fn func(Acc, T) Acc,
) (Acc, error) {
	// Create a cancellable context for the execution. If the processing loop is cancelled
	// externally, we must ensure the pipeline shuts down gracefully.
	execCtx, cancelExec := context.WithCancel(ctx)
	defer cancelExec() // Ensures pipeline shutdown if Reduce returns prematurely.

	dataCh, exec := s.pipe(execCtx)
	acc := init

	// Processing loop
loop:
	for {
		select {
		case batch, ok := <-dataCh:
			if !ok {
				break loop // Data stream finished.
			}
			// Use a closure to guarantee batch release even if fn() panics (if not using errgroup for the reducer itself).
			func() {
				defer batch.Release()
				for _, item := range batch.Data {
					acc = fn(acc, item)
				}
			}()
		case <-ctx.Done():
			// External cancellation occurred.
			// cancelExec() is called by defer, initiating pipeline shutdown.
			break loop
		}
	}

	// Wait for the entire pipeline execution to fully complete and check for errors.
	<-exec.Done
	if err := exec.Err(); err != nil {
		// If the pipeline failed internally (e.g., ParMap error).
		// We prioritize the internal error over the external cancellation signal if both occurred.
		if ctx.Err() == nil || (err != context.Canceled && err != context.DeadlineExceeded) {
			return acc, err
		}
	}

	// If externally cancelled and no internal error occurred.
	return acc, ctx.Err()
}
