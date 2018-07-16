package storage

import (
	. "github.com/FMNSSun/mydb"
	"sync"
	"log"
	"bytes"
)

type MemoryStorage struct {
	m map[KeyHash][]*kv
	mutex *sync.RWMutex
}

type kv struct {
	key []byte
	value []byte
}

func NewMemoryStorage() Storage {
	return &MemoryStorage{
		m: make(map[KeyHash][]*kv),
		mutex: &sync.RWMutex{},
	}
}

func (m *MemoryStorage) Get(key []byte) ([]byte, StorageError) {
	log.Printf("[MEMORYSTORAGE] Get: %x", key)

	m.mutex.RLock()

	val, ok := m.m[Hash(key)]

	m.mutex.RUnlock()

	if !ok {
		return nil, StorageErrorf(ERR_NOTEXISTS, "Entry does not exist.")
	}

	for _, candidate := range val {
		if bytes.Equal(candidate.key, key) {
			return candidate.value, nil
		}
	}

	return nil, StorageErrorf(ERR_NOTEXISTS, "Entry does not exist.")
}

func (m *MemoryStorage) Put(key []byte, value []byte) StorageError {
	log.Printf("[MEMORYSTORAGE] Put: %x", key)

	m.mutex.Lock()

	val, ok := m.m[Hash(key)]

	if !ok {
		kvs := make([]*kv, 1)
		kvs[0] = &kv{key: key, value: value}
		m.m[Hash(key)] = kvs
	} else {
		for _, candidate := range val {
			if bytes.Equal(candidate.key, key) {
				candidate.value = value
				break
			}
		}
	}

	m.mutex.Unlock()

	return nil
}
