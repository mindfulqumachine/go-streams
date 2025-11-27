package stream_test

import (
	"context"
	"fmt"
	"testing"

	"go-streams/pkg/stream"
)

// Types for the example
type SensorData struct {
	ID     int
	Region string
}

type Metric struct {
	Val int
}

func TestComplexExample(t *testing.T) {
	ctx := context.Background()

	// --- 1. Define Logic (Pure Functions) ---
	validID := func(id int) bool { return id > 0 }
	enrich := func(id int) SensorData { return SensorData{ID: id, Region: "US"} }
	parseLog := func(l string) Metric { return Metric{Val: len(l)} }
	toMetric := func(d SensorData) Metric { return Metric{Val: d.ID} }
	toBlob := func(d SensorData) []byte { return []byte(fmt.Sprintf("%v", d)) }

	// Generators
	sensorGen := func(ctx context.Context, out chan<- int) error {
		for i := 1; i <= 10; i++ {
			out <- i
		}
		return nil
	}
	logGen := func(ctx context.Context, out chan<- string) error {
		for i := 0; i < 5; i++ {
			out <- "log line"
		}
		return nil
	}

	// --- 2. Build Sources & Linear Pipelines ---
	// Flow 1: Sensor Data (int -> SensorData)
	sensors := stream.Source(sensorGen)
	enrichedSensors := stream.Compose(
		stream.Filter(validID),
		stream.Map(enrich),
	).Apply(sensors)

	// Flow 2: Log Metrics (string -> Metric)
	logs := stream.Source(logGen)
	logMetrics := stream.Compose(
		stream.Map(parseLog),
		stream.Filter(func(m Metric) bool { return m.Val > 0 }),
	).Apply(logs)

	// --- 3. Split & Async (Fan-Out) ---
	// Broadcast sensors for (1) Archival and (2) Analytics
	var archivalStream stream.Stream[[]byte]
	var sensorMetrics stream.Stream[Metric]

	stream.Broadcast(enrichedSensors, func(b *stream.Broadcaster[SensorData]) {
		// Branch 1: Archival (SensorData -> []byte)
		archivalStream = stream.Split(b, toBlob)

		// Branch 2: Analytics Preparation (SensorData -> Metric)
		sensorMetrics = stream.Split(b, toMetric)
	})

	// --- 4. Merge (Fan-In) ---
	// Combine Log Metrics and Sensor Metrics
	allMetrics := stream.Merge(
		logMetrics.Async(1024),
		sensorMetrics.Async(1024),
	)

	// --- 5. Execution ---
	// Sinks
	dbSink := stream.NewCollectorSink[Metric]()
	fileSink := stream.NewCollectorSink[[]byte]()

	// Run both sinks
	errCh := make(chan error, 2)

	go func() {
		errCh <- allMetrics.To(dbSink).Run(ctx)
	}()
	go func() {
		errCh <- archivalStream.To(fileSink).Run(ctx)
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("Run failed: %v", err)
		}
	}

	// Verify
	metrics := dbSink.(*stream.CollectorSink[Metric]).Results()
	blobs := fileSink.(*stream.CollectorSink[[]byte]).Results()

	// 10 sensors -> 10 metrics + 5 logs -> 5 metrics = 15 metrics
	if len(metrics) != 15 {
		t.Errorf("Expected 15 metrics, got %d", len(metrics))
	}
	// 10 sensors -> 10 blobs
	if len(blobs) != 10 {
		t.Errorf("Expected 10 blobs, got %d", len(blobs))
	}
}
