package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	
	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
)

type raftNode struct {
	id			int
	peers       []string
	
	storage     *raft.MemoryStorage
	node        raft.Node
	transport   *rafthttp.Transport
}
func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

func (rc *raftNode) ServeEndpoint(){
	url, _ := url.Parse(rc.peers[rc.id - 1])
	mux := rc.transport.Handler()
	log.Fatal(http.ListenAndServe(url.Host, mux))
}

func (rc *raftNode) ServeChannels(){
	for {
		select {
			case rd := <-rc.node.Ready():
				fmt.Printf("receive node ready: %+v", rd)
				rc.storage.Append(rd.Entries)
				rc.transport.Send(rd.Messages)
				rc.node.Advance()
		}
	}
}

func main(){
	id := flag.Int("id", 1, "node ID")
	cluster := flag.String("cluster", "http://127.0.0.1:12380,http://127.0.0.1:22380,http://127.0.0.1:32380", 
		"comma separated cluster peers")
	flag.Parse()
	
	var rc *raftNode = &raftNode{id: *id}
	rc.peers = strings.Split(*cluster, ",")
	rc.storage = raft.NewMemoryStorage()
	c := &raft.Config{
		ID:				uint64(rc.id),
		ElectionTick:	10,
		HeartbeatTick:	1,
		Storage:		rc.storage,
		MaxSizePerMsg:	4096,
		MaxInflightMsgs:256,
	}
	rpeers := make([]raft.Peer, len(rc.peers))
	for i, _ := range rc.peers{
		rpeers[i] = raft.Peer{ID: uint64(i+1)}
	}
	rc.node = raft.StartNode(c, rpeers)
	fmt.Printf("node: %+v", rc.node)
	
	ticker := time.NewTicker(100 * time.Millisecond)
	go func(){
		for {
			select{
				case <-ticker.C:
					rc.node.Tick()
			}
		}
	}()
	
	rc.transport = &rafthttp.Transport{
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}
	rc.transport.Start()
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}
	
	go rc.ServeEndpoint()
	go rc.ServeChannels()
	
	if err, ok := <-rc.transport.ErrorC; ok {
		log.Fatal(err)
	}
}