package stream

import "runtime"

// ============================================================================
// SYSTEM CONFIGURATION
// ============================================================================

// VectorSize defines the default capacity and flush threshold for data batches (vectors).
// A larger size improves throughput by reducing channel overhead, but increases latency.
const VectorSize = 8192

// ChannelBuffer defines the buffer size for the channels connecting pipeline stages.
// It provides backpressure to prevent faster stages from overwhelming slower ones.
const ChannelBuffer = 1024 // Default backpressure buffer size

// sanitizeDOP ensures the Degree of Parallelism (dop) is a valid positive integer.
//
// If dop is less than or equal to 0, it defaults to the number of logical CPUs available (GOMAXPROCS).
//
// Parameters:
//   dop: The requested degree of parallelism.
//
// Returns:
//   int: The sanitized degree of parallelism.
func sanitizeDOP(dop int) int {
	if dop <= 0 {
		return runtime.GOMAXPROCS(0)
	}
	return dop
}
