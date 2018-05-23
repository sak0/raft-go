package main

import (
	"flag"
	"fmt"
	
	"github.com/coreos/etcd/raft/raftpb"
	myraft "github.com/sak0/raft-go/raft"
)

func main() {
	id := flag.Int("id", 1, "node ID")
	cluster := flag.String("cluster", "http://127.0.0.1:12380,http://127.0.0.1:22380,http://127.0.0.1:32380", 
		"comma separated cluster peers")
	flag.Parse()
	
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)
	proposeC := make(chan string)
	defer close(proposeC)
	
	commitC, errorC, snapshotterReady := myraft.CreateRaftNode(id, cluster, proposeC, confChangeC)
	fmt.Printf("create raft node done: %v %v %v\n", commitC, errorC, snapshotterReady)
	
	kvs := myraft.NewKVStore(proposeC)
	myraft.ServeHTTPAPI(kvs, errorC)
}