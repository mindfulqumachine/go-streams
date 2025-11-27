package stream

import "runtime"

// ============================================================================
// SYSTEM CONFIGURATION
// ============================================================================

const DefaultVectorSize = 1024
const ChannelBuffer = 1024 // Default backpressure buffer size

func sanitizeDOP(dop int) int {
	if dop <= 0 {
		return runtime.GOMAXPROCS(0)
	}
	return dop
}
