package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	
	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
)

const (
	raftId = 0x01
)

type raftNode struct {
	id			int
	
	storage 	*raft.MemoryStorage
	node        raft.Node
}
func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

func main(){
	var rc *raftNode = &raftNode{id: raftId}
	rc.storage = raft.NewMemoryStorage()
	c := &raft.Config{
		ID:				uint64(rc.id),
		ElectionTick:	10,
		HeartbeatTick:	1,
		Storage:		rc.storage,
		MaxSizePerMsg:	4096,
		MaxInflightMsgs:256,
	}
	rc.node = raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})
	fmt.Printf("node: %+v", rc.node)
	
	transport := &rafthttp.Transport{
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}
	mux := transport.Handler()
	log.Fatal(http.ListenAndServe("127.0.0.1:12380", mux))
}