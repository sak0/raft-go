package main

import (
	"flag"
	"fmt"
	
	myraft "github.com/sak0/raft-go/raft"
)

func main(){
	id := flag.Int("id", 1, "node ID")
	cluster := flag.String("cluster", "http://127.0.0.1:12380,http://127.0.0.1:22380,http://127.0.0.1:32380", 
		"comma separated cluster peers")
	flag.Parse()
	
	myraft.CreateRaftNode(id, cluster)
	fmt.Printf("create raft node.\n")
}