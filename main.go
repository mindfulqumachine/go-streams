package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"go-streams/pkg/stream"
)

// ============================================================================
// DOMAIN LOGIC (EXAMPLE USAGE)
// ============================================================================

// Data Types
type RawLog struct {
	ID     int
	Source string
	Level  int
}
type AnalyzedEvent struct {
	ID        int
	Category  string
	LatencyMs int
}
type SecurityAlert struct {
	ID       int
	Severity string
}
type Metric struct {
	Name  string
	Value float64
}

// Generators
func generateLogs(source string, count int) func(emit func(RawLog)) error {
	return func(emit func(RawLog)) error {
		for i := range count {
			emit(RawLog{ID: i, Source: source, Level: i % 5})
		}
		return nil
	}
}

// Parallel Processor (Simulates CPU-bound work and potential failure)
func analyzeLog(log RawLog) (AnalyzedEvent, error) {
	// Simulate intensive work
	time.Sleep(2 * time.Microsecond)

	// Simulate rare transient failure
	if log.Source == "B" && log.ID > 0 && log.ID%290000 == 0 {
		return AnalyzedEvent{}, fmt.Errorf("transient analysis failure at ID %d", log.ID)
	}

	category := "INFO"
	if log.Level == 3 {
		category = "WARN"
	}
	if log.Level >= 4 {
		category = "ERROR"
	}

	return AnalyzedEvent{ID: log.ID, Category: category, LatencyMs: log.Level * 10}, nil
}

// Router Logic
func routeEvent(evt AnalyzedEvent, emitAlert func(SecurityAlert), emitMetric func(Metric)) error {
	// Simulate routing work
	time.Sleep(1 * time.Microsecond)

	if evt.Category == "ERROR" {
		emitAlert(SecurityAlert{ID: evt.ID, Severity: "HIGH"})
	} else {
		emitMetric(Metric{Name: "Latency", Value: float64(evt.LatencyMs)})
	}
	return nil
}

// ============================================================================
// EXPLICIT PIPELINE DEMONSTRATION
// ============================================================================

func main() {
	// Set a timeout for the entire pipeline execution.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	P := runtime.GOMAXPROCS(0)
	fmt.Printf("--- Constructing Pipeline (Cores: %d, VectorSize: %d) ---\n", P, stream.VectorSize)
	startTime := time.Now()

	// Topology Definition:
	// [GenA, GenB] -> (MergeN) -> [RawLogs]
	//              -> (ParMap: Analyze) -> [AnalyzedEvents]
	//              -> (Dispatch: Route) -> [Alerts], [Metrics]

	// 1. Sources
	logsA := stream.FromGenerator(generateLogs("A", 500_000))
	logsB := stream.FromGenerator(generateLogs("B", 300_000))

	// 2. Merge (Zero-copy)
	allLogs := stream.MergeN(logsA, logsB)

	// 3. Intensive Parallel Processing (CPU-bound)
	// Utilize all cores for analysis.
	analyzedEvents := stream.ParMap(allLogs, P, analyzeLog)

	// 4. Parallel Routing (CPU-bound)
	// Utilize all cores for routing.
	alerts, metrics := stream.Dispatch(analyzedEvents, P, routeEvent)

	// 5. Execution / Sinks
	// We use a WaitGroup to handle the parallel output streams.
	var wg sync.WaitGroup
	wg.Add(2)

	var pipelineErr error
	var mu sync.Mutex

	// Helper to capture the first error encountered by any sink.
	captureErr := func(err error) {
		if err != nil {
			mu.Lock()
			defer mu.Unlock()
			if pipelineErr == nil {
				pipelineErr = err
			}
			cancel() // Signal other sinks to stop if an error occurs.
		}
	}

	// Alerts Sink
	go func() {
		defer wg.Done()
		count, err := stream.Reduce(ctx, alerts, 0, func(acc int, _ SecurityAlert) int {
			return acc + 1
		})
		fmt.Printf("Alerts Sink: %d processed.\n", count)
		captureErr(err)
	}()

	// Metrics Sink
	go func() {
		defer wg.Done()
		count, err := stream.Reduce(ctx, metrics, 0, func(acc int, _ Metric) int {
			return acc + 1
		})
		fmt.Printf("Metrics Sink: %d processed.\n", count)
		captureErr(err)
	}()

	wg.Wait()
	duration := time.Since(startTime)

	if pipelineErr != nil {
		fmt.Printf("Pipeline Status: FAILED (%v)\n", pipelineErr)
	} else {
		fmt.Printf("Pipeline Status: SUCCESS\n")
	}
	fmt.Printf("--- Pipeline Complete in %s ---\n", duration)
}
