package stream

// StreamConfig holds configuration for stream operators.
type StreamConfig struct {
	BatchSize int
}

// Option is a functional option for configuring stream operators.
type Option func(*StreamConfig)

// DefaultConfig returns the default configuration.
func DefaultConfig() StreamConfig {
	return StreamConfig{
		BatchSize: DefaultVectorSize,
	}
}

// WithBatchSize sets the batch size (vector capacity) for the operator.
func WithBatchSize(size int) Option {
	return func(c *StreamConfig) {
		if size > 0 {
			c.BatchSize = size
		}
	}
}

// ApplyOptions applies the given options to the default configuration.
func ApplyOptions(opts ...Option) StreamConfig {
	config := DefaultConfig()
	for _, opt := range opts {
		opt(&config)
	}
	return config
}
