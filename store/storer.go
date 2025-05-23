package store

type DataStorer[K comparable, V any] interface {
	Get(key K) (V, error)
	Set(key K, Value V)
	Delete(key K)
}
