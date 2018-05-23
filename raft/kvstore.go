package raft

import (
	"sync"
)

type kvstore struct {
	mu			sync.Mutex
	kvStore		map[string]string
}

func NewKVStore()*kvstore{
	return &kvstore{
		kvStore: make(map[string]string),
	}
}

func (k *kvstore)Lookup(key string)(string, bool){
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.kvStore[key]
	if !ok {
		return "", false
	} else {
		return v, true
	}
}