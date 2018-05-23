package raft

import (
	"bytes"
	"sync"
	"encoding/gob"
	"log"
)

type kvstore struct {
	mu			sync.Mutex
	kvStore		map[string]string
	
	proposeC	chan<- string
}

type kv struct{
	k	string
	v	string
}

func NewKVStore(proposeC chan<- string)*kvstore{
	return &kvstore{
		kvStore: make(map[string]string),
		proposeC: proposeC,
	}
}

func (kvs *kvstore)Lookup(key string)(string, bool){
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	v, ok := kvs.kvStore[key]
	if !ok {
		return "", false
	} else {
		return v, true
	}
}

func (kvs *kvstore)Propose(k string, v string){
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil{
		log.Fatal(err)
	}
	kvs.proposeC <- buf.String()
}