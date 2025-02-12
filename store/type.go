package store

import (
	"sync"
)

type InMemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}
