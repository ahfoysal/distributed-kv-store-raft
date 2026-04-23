package kv

import (
	"encoding/json"
	"sync"
)

type Op struct {
	Kind  string `json:"kind"` // "set" | "delete"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Apply(command string) {
	var op Op
	if err := json.Unmarshal([]byte(command), &op); err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch op.Kind {
	case "set":
		s.data[op.Key] = op.Value
	case "delete":
		delete(s.data, op.Key)
	}
}

func EncodeSet(key, value string) string {
	b, _ := json.Marshal(Op{Kind: "set", Key: key, Value: value})
	return string(b)
}

func EncodeDelete(key string) string {
	b, _ := json.Marshal(Op{Kind: "delete", Key: key})
	return string(b)
}

// Snapshot returns a point-in-time copy of the full key/value map for
// persistence. Safe to call concurrently with Apply/Get.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Restore replaces the in-memory state with the given map. Used on startup
// after loading a persisted snapshot.
func (s *Store) Restore(data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]string, len(data))
	for k, v := range data {
		s.data[k] = v
	}
}

// Len returns the number of keys (convenience for tests/status).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
