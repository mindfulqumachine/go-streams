package stream

import "sync"

// vecPool manages a pool of Vector objects to minimize memory allocations.
// It wraps sync.Pool to provide type-safe access to Vector[T].
type vecPool[T any] struct {
	// pool is the underlying sync.Pool used for storing vectors.
	pool sync.Pool
}

// newVecPool creates a new pool that manages Smart Vectors directly.
//
// Returns:
//   *vecPool[T]: A pointer to the newly created vector pool.
func newVecPool[T any]() *vecPool[T] {
	p := &vecPool[T]{}
	p.pool.New = func() any {
		return &Vector[T]{
			Data: make([]T, 0, VectorSize),
			pool: p,
		}
	}
	return p
}

// Get retrieves a vector from the pool or creates a new one if the pool is empty.
// It sets the vector's pool reference to this pool to enable proper recycling.
//
// Returns:
//   *Vector[T]: A vector ready for use.
func (p *vecPool[T]) Get() *Vector[T] {
	v := p.pool.Get().(*Vector[T])
	v.pool = p // Restore pool reference for recycled vectors
	return v
}

// Put returns a vector to the pool for reuse.
// It resets the vector's data slice length to 0 to clear it while preserving capacity.
//
// Parameters:
//   vec: The vector to return to the pool.
func (p *vecPool[T]) Put(vec *Vector[T]) {
	// Reset slice length to 0, keeping the underlying capacity.
	vec.Data = vec.Data[:0]
	p.pool.Put(vec)
}
