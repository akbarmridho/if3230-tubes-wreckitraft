package raft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"sync"
)

type Store struct {
	BaseDir string
	Lock    sync.RWMutex
}

func (s Store) filepath(key StableKey) string {
	return fmt.Sprintf("%s/%s.gob", s.BaseDir, key)
}

func (s Store) GetLogs() ([]Log, error) {
	s.Lock.RLock()
	defer s.Lock.RUnlock()
	file, err := os.Open(s.filepath("logs"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrLogNotFound
		}
		return nil, err
	}
	defer file.Close()

	var logs []Log
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&logs); err != nil {
		return nil, err
	}

	return logs, ErrLogNotFound
}

func (s Store) StoreLogs(logs []Log) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	if err := os.MkdirAll(s.BaseDir, os.ModePerm); err != nil {
		return err
	}

	file, err := os.Create(s.filepath("logs"))
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(logs); err != nil {
		return err
	}

	return nil
}

func (s Store) Set(key StableKey, val uint64) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()

	if err := os.MkdirAll(s.BaseDir, os.ModePerm); err != nil {
		return err
	}

	file, err := os.Create(s.filepath(key))
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(val); err != nil {
		return err
	}

	return nil
}

func (s Store) Get(key StableKey) (*uint64, error) {
	s.Lock.RLock()
	defer s.Lock.RUnlock()

	file, err := os.Open(s.filepath(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	defer file.Close()

	var val uint64
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&val); err != nil {
		return nil, err
	}

	return &val, nil
}

var ErrKeyNotFound = errors.New("key not found")
var ErrLogNotFound = errors.New("log not found")
