package stream

import (
	"sync"

	"golang.org/x/sync/errgroup"
)

// ============================================================================
// EXECUTION UTILITIES
// ============================================================================

// drain consumes and releases all vectors from a channel until it is closed.
// This is critical for preventing upstream deadlocks during failures or cancellation
// by ensuring that producers do not block on a full channel.
//
// Parameters:
//   ch: The channel to drain.
func drain[T any](ch <-chan *Vector[T]) {
	for vec := range ch {
		vec.Release()
	}
}

// executionFromErrGroup creates an Execution handle from an errgroup.Group.
// It starts a goroutine to wait for the group to complete and captures the result.
//
// Parameters:
//   g: The errgroup.Group to monitor.
//
// Returns:
//   Execution: An execution handle enabling monitoring of the group's lifecycle.
func executionFromErrGroup(g *errgroup.Group) Execution {
	done := make(chan struct{})
	var execErr error
	go func() {
		execErr = g.Wait()
		close(done)
	}()
	return Execution{
		Done: done,
		Err:  func() error { return execErr },
	}
}

// combineExecutions merges multiple execution handles and synchronization points into a single Execution.
// It ensures correct ordering: Wait for Primary -> Run Cleanup -> Wait for Dependencies (Parent/Drainer).
//
// Parameters:
//   primary: The main execution to wait for (e.g., worker completion).
//   cleanup: A function to run after the primary execution completes (e.g., closing output channels).
//   dependencies: Additional executions to wait for after cleanup (e.g., upstream stages).
//
// Returns:
//   Execution: A combined execution handle that signals done when all parts are complete.
func combineExecutions(primary Execution, cleanup func(), dependencies ...Execution) Execution {
	done := make(chan struct{})
	var combinedErr error
	var mu sync.Mutex

	captureErr := func(err error) {
		if err != nil {
			mu.Lock()
			defer mu.Unlock()
			if combinedErr == nil {
				combinedErr = err
			}
		}
	}

	go func() {
		defer close(done)

		// 1. Wait for the primary execution (e.g., workers).
		<-primary.Done
		captureErr(primary.Err())

		// 2. Run cleanup (e.g., close output channels).
		if cleanup != nil {
			cleanup()
		}

		// 3. Wait for all dependencies (e.g., parent stages, drainers).
		var wg sync.WaitGroup
		for _, dep := range dependencies {
			if dep.Done == nil {
				continue
			} // Handle nil/dummy dependencies
			wg.Add(1)
			go func(e Execution) { defer wg.Done(); <-e.Done; captureErr(e.Err()) }(dep)
		}
		wg.Wait()
	}()

	return Execution{
		Done: done,
		Err: func() error {
			mu.Lock()
			defer mu.Unlock()
			return combinedErr
		},
	}
}

// executionFromChan creates an Execution handle from a done channel.
// This is useful for treating a simple signal channel as a full Execution object.
//
// Parameters:
//   done: The channel that signals completion when closed.
//
// Returns:
//   Execution: An execution handle wrapping the channel, with a nil error function.
func executionFromChan(done <-chan struct{}) Execution {
	return Execution{Done: done, Err: func() error { return nil }}
}
