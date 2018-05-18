package raft

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	
	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/types"	
	"github.com/coreos/etcd/raft"
    "github.com/coreos/etcd/raft/raftpb"
    "github.com/coreos/etcd/raftsnap"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
)

type raftNode struct {
	id			     int
	peers            []string
	
	node             raft.Node
	storage          *raft.MemoryStorage
	snapshotter      *raftsnap.Snapshotter
	transport        *rafthttp.Transport
	wal              *wal.WAL
	
	snapdir          string
	waldir           string
	
	confState        raftpb.ConfState
	
	lastIndex        uint64
	appliedIndex     uint64
	snapshotIndex    uint64
	
	proposeC         chan string
	confChangeC      chan raftpb.ConfChange
	commitC          chan *string
	errorC           chan error
	snapshotterReady chan *raftsnap.Snapshotter
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

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Index,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *raftNode) publishSnapshot(snap raftpb.Snapshot){
	if raft.IsEmptySnap(snap) {
		return
	}
	fmt.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer fmt.Printf("finish publishing snapshot at index %d", rc.snapshotIndex)
	if snap.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("couldn't publish snapshot index %d <= node applied index %d", snap.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- nil
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index
}

func (rc *raftNode) ServeChannels(){
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
			case <-ticker.C:
				//fmt.Printf("node.Tick Status %+v\n", rc.node.Status())
				rc.node.Tick()
			case rd := <-rc.node.Ready():
				nodeStatus := rc.node.Status()
				fmt.Printf("***receive node ready msg***\n")
				for i, msg := range rd.Messages {
					fmt.Printf("%v(%d) - (%d)[%v][%d] %d -> %d\n", 
					nodeStatus.SoftState.RaftState, nodeStatus.SoftState.Lead, 
					i, msg.Type, msg.Term, msg.From, msg.To)
				}
				rc.wal.Save(rd.HardState, rd.Entries)
				if !raft.IsEmptySnap(rd.Snapshot) {
					fmt.Printf("+++++++++receive rd.Snapshot %+v\n", rd.Snapshot)
					rc.saveSnap(rd.Snapshot)
					rc.storage.ApplySnapshot(rd.Snapshot)
					rc.publishSnapshot(rd.Snapshot)
				}
				
				rc.storage.Append(rd.Entries)
				rc.transport.Send(rd.Messages)
				rc.node.Advance()
		}
	}
}

func CreateRaftNode(id *int, cluster *string, proposeC chan string, 
	confChangeC chan raftpb.ConfChange)(chan *string, chan error, chan *raftsnap.Snapshotter){
	errorC := make(chan error)
	var rc *raftNode = &raftNode{
		id:          	  	*id,
		snapdir: 	      	fmt.Sprintf("snap-%d", *id),
		waldir:           	fmt.Sprintf("wal-%d", *id),
		
		commitC:          	make(chan *string),
		proposeC:         	proposeC,
		confChangeC:      	confChangeC,
		errorC:           	errorC,
		snapshotterReady:	make(chan *raftsnap.Snapshotter, 1),
	}
	
	rc.peers = strings.Split(*cluster, ",")
	rc.storage = raft.NewMemoryStorage()
	
	if !Exists(rc.snapdir) {
		err := os.Mkdir(rc.snapdir, 0755)
		if err != nil {
			panic(fmt.Sprintf("Mkdir %s failed. Error: %v", rc.snapdir, err))
		}
	}
	rc.snapshotter = raftsnap.New(rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter
	
	oldwal := wal.Exist(rc.waldir)
	if !oldwal {
		if err := os.Mkdir(rc.waldir, 0755); err != nil {
			log.Fatalf("Mkdir %s failed. Error: %v", rc.waldir, err)
		}
		w, err := wal.Create(rc.waldir, nil)
		if err != nil {
			log.Fatal("Create wal failed. Error: %v", err)
		}
		w.Close()
	}
	snap, err := rc.snapshotter.Load()
	if err != nil && err != raftsnap.ErrNoSnapshot {
		log.Fatalf("load snapshot failed, %v", err)
	}
	walsnap := walpb.Snapshot{}
	if snap != nil {
		walsnap.Index, walsnap.Term = snap.Metadata.Index, snap.Metadata.Term
		rc.storage.ApplySnapshot(*snap)
	}
	w, err := wal.Open(rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("Open wal failed, Error: %v", err)
	}
	_, state, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("Read wal failed, Error: %v", err)
	}
	rc.storage.SetHardState(state)
	rc.storage.Append(ents)
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents) - 1].Index
	} 
	// else  rc.commitC <- nil
	
	rc.wal = w
	
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
	return rc.commitC, errorC, rc.snapshotterReady
}