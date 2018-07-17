package storage

import (
	"path/filepath"
	"sync"
	"os"
	"encoding/hex"
	"io/ioutil"

	. "github.com/FMNSSun/mydb"
)

type SimpleDiskStorage struct {
	basePath string
	locks []*sync.RWMutex
	logger Logger
}

func NewSimpleDiskStorage(basePath string, logger Logger) Storage {
	locks := make([]*sync.RWMutex, 256)

	for i := 0; i < len(locks); i++ {
		locks[i] = &sync.RWMutex{}
	}

	return &SimpleDiskStorage{
		basePath: basePath,
		locks: locks,
		logger: logger,
	}
}

func escape(key []byte) string {
	return hex.EncodeToString(key)
}

func (ds *SimpleDiskStorage) Get(key []byte) ([]byte, StorageError) {
	lhack := byte(0)

	for _, v := range key {
		lhack ^= v
	}

	ds.locks[lhack].RLock()
	defer ds.locks[lhack].RUnlock()

	escKey := escape(key)

	fpath := filepath.Join(ds.basePath, escKey)

	_, err := os.Stat(fpath)

	if os.IsNotExist(err) {
		return nil, nil
	}

	flgs := os.O_RDONLY

	f, err := os.OpenFile(fpath, flgs, 0755)

	if err != nil {
		ds.logger.Outf(LOGLVL_ERROR, "[DIOSTORAGE] ERROR: %s", err.Error())
		return nil, StorageErrorf2(ERR_STORAGE, err, "Could not open file: %q.", fpath)
	}

	data, err := ioutil.ReadAll(f)

	if err != nil {
		ds.logger.Outf(LOGLVL_ERROR, "[DIOSTORAGE] ERROR: %s", err.Error())
		f.Close()
		return nil, StorageErrorf2(ERR_STORAGE, err, "Could not read file: %q.", fpath)
	}

	err = f.Close()

	if err != nil {
		ds.logger.Outf(LOGLVL_ERROR, "[DIOSTORAGE] ERROR: %s", err.Error())
		return nil, StorageErrorf2(ERR_STORAGE, err, "Could not close file: %q", fpath)
	}

	return data, nil
}

func (ds *SimpleDiskStorage) Put(key, value []byte) StorageError {
	lhack := byte(0)

	for _, v := range key {
		lhack ^= v
	}

	ds.locks[lhack].Lock()
	defer ds.locks[lhack].Unlock()

	escKey := escape(key)

	flgs := os.O_WRONLY | os.O_CREATE | os.O_TRUNC

	f, err := os.OpenFile(filepath.Join(ds.basePath, escKey), flgs, 0755)

	if err != nil {
		ds.logger.Outf(LOGLVL_ERROR, "[DIOSTORAGE] ERROR: %s", err.Error())
		return StorageErrorf2(ERR_STORAGE, err, "Could not open file.")
	}

	_, err = f.Write(value)

	if err != nil {
		ds.logger.Outf(LOGLVL_ERROR, "[DIOSTORAGE] ERROR: %s", err.Error())
		f.Close()
		return StorageErrorf2(ERR_STORAGE, err, "Could not write to file.")
	}

	err = f.Sync()

	if err != nil {
		// TODO: What do we do with the file then? If we leave it in a partial state
		// then reads will read garbage? (mroman)
		ds.logger.Outf(LOGLVL_ERROR, "[DIOSTORAGE] ERROR: %s", err.Error())
		f.Close()
		return StorageErrorf2(ERR_STORAGE, err, "Could not sync file.")
	}

	err = f.Close()

	if err != nil {
		ds.logger.Outf(LOGLVL_ERROR, "[DIOSTORAGE] ERROR: %s", err.Error())
		return StorageErrorf2(ERR_STORAGE, err, "Could not close file.")
	}

	return nil
}





