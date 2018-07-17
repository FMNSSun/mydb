package storage

import (
	. "github.com/FMNSSun/mydb"
	"sync"
	"bytes"
)

type MemoryStorage struct {
	m map[KeyHash][]*kv
	mutex *sync.RWMutex
	logger Logger
}

type kv struct {
	key []byte
	value []byte
}

func NewMemoryStorage(logger Logger) Storage {
	return &MemoryStorage{
		m: make(map[KeyHash][]*kv),
		mutex: &sync.RWMutex{},
		logger: logger,
	}
}

func (m *MemoryStorage) Get(key []byte) ([]byte, StorageError) {
	m.logger.Outf(LOGLVL_INFO, "[MEMORYSTORAGE] Get: %x", key)

	m.mutex.RLock()

	val, ok := m.m[Hash(key)]

	m.mutex.RUnlock()

	if !ok {
		return nil, nil
	}

	for _, candidate := range val {
		if bytes.Equal(candidate.key, key) {
			return candidate.value, nil
		}
	}

	return nil, StorageErrorf(ERR_NOTEXISTS, "Entry does not exist.")
}

func (m *MemoryStorage) Put(key []byte, value []byte) StorageError {
	m.logger.Outf(LOGLVL_INFO, "[MEMORYSTORAGE] Put: %x", key)

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
