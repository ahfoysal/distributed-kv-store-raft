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
