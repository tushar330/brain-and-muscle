package raft

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/akashtripathi12/brain-and-muscle/internal/wal"
	uuid "github.com/google/uuid"
)

// ELECTION RPCs

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// REPLICATION & HEARTBEAT RPCs

type AppendEntriesArgs struct {
	Term         uint64
	LeaderId     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []wal.LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term         uint64
	Success      bool
	HighestIndex uint64
}

// SNAPSHOT RPCs

type InstallSnapshotArgs struct {
	Term              uint64 // Leader's current term
	LeaderId          string // So follower can redirect clients
	LastIncludedIndex uint64 // The snapshot replaces all entries up through and including this index
	LastIncludedTerm  uint64 // Term of lastIncludedIndex
	Data              []byte // The raw binary snapshot of the KV Store
}

type InstallSnapshotReply struct {
	Term uint64 // The Follower's currentTerm, for the Leader to update itself if it's behind
}

// ACTOR EVENTS

type VoteReplyEvent struct {
	PeerID string
	Reply  RequestVoteReply
}

type AppendReplyEvent struct {
	PeerID string
	Reply  AppendEntriesReply
}

type SingleNodeCommitEvent struct {
	Index uint64
}

type RetrySyncEvent struct {
	PeerID string
}

// TCP WRAPPERS

// SendInstallSnapshotRPC transmits the entire compacted state machine over the network.
func (b *BrainState) SendInstallSnapshotRPC(peerID string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	payloadBytes, _ := json.Marshal(args)

	// Ensure you define protocol.OpInstallSnapshot (e.g., 0x05) in your protocol package!
	frame := protocol.PackEnvelope(protocol.OpInstallSnapshot, b.NodeID, uuid.New().String(), payloadBytes)

	conn, err := b.getPeerConn(peerID)
	if err != nil {
		return InstallSnapshotReply{}, err
	}

	//  Snapshots are large. Give the network 10 seconds to push the binary blob.
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err = conn.Write(frame); err != nil {
		b.dropPeerConn(peerID, conn)
		return InstallSnapshotReply{}, err
	}

	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		b.dropPeerConn(peerID, conn)
		return InstallSnapshotReply{}, err
	}

	bodyLen := binary.BigEndian.Uint32(header[0:4])
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(conn, body); err != nil {
		b.dropPeerConn(peerID, conn)
		return InstallSnapshotReply{}, err
	}

	_, _, _, respBytes, err := protocol.UnpackEnvelope(body)
	if err != nil {
		b.dropPeerConn(peerID, conn)
		return InstallSnapshotReply{}, err
	}

	var reply InstallSnapshotReply
	json.Unmarshal(respBytes, &reply)
	return reply, nil
}

// ProcessInstallSnapshot runs on a wiped or severely lagging FOLLOWER.
// It catches the binary state from the Leader and completely overwrites its own memory.
func (b *BrainState) ProcessInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 1. Standard Raft Term Check
	if args.Term < b.CurrentTerm {
		return InstallSnapshotReply{Term: b.CurrentTerm}
	}

	if args.Term > b.CurrentTerm {
		b.CurrentTerm = args.Term
		b.VotedFor = ""
		b.State = "FOLLOWER"
		b.PersistMeta()
	}
	b.LeaderID = args.LeaderId
	b.resetElectionTimer()

	// 2.  FIX 1: Strictly Less Than
	if args.LastIncludedIndex < b.LastIncludedIndex {
		return InstallSnapshotReply{Term: b.CurrentTerm}
	}

	// Ignore if we have the EXACT SAME snapshot
	if args.LastIncludedIndex == b.LastIncludedIndex && args.LastIncludedTerm == b.LastIncludedTerm {
		return InstallSnapshotReply{Term: b.CurrentTerm}
	}

	log.Printf("[raft-sync] Catching up! Installing snapshot from Leader up to Index %d", args.LastIncludedIndex)

	// 3. Write the binary snapshot to the hard drive
	tmpFile := b.snapPath + ".tmp"
	os.WriteFile(tmpFile, args.Data, 0644)
	os.Rename(tmpFile, b.snapPath)

	// 4. Overwrite the DAG Execution Engine's RAM
	b.Engine.ImportSnapshot(args.Data)

	// 5. Throw away the entire old Raft Log and create a new starting point
	b.Log = []wal.LogEntry{{
		Term: args.LastIncludedTerm,
	}}
	b.LastIncludedIndex = args.LastIncludedIndex
	b.LastIncludedTerm = args.LastIncludedTerm

	// 6. Fast-forward the commit and applied indexes so we don't replay ghosts
	b.commitIndex = args.LastIncludedIndex
	b.lastApplied = args.LastIncludedIndex

	// 7.  FIX 2: Manually wipe and rewrite raft_state.jsonl safely
	if b.walFile != nil {
		b.walFile.Close()
	}
	filename := b.NodeID + "_raft_state.jsonl"
	os.Remove(filename) // Destroy the ghost file

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err == nil {
		b.walFile = f
		b.PersistMeta()
		b.PersistAppend(b.Log[0])
	}

	log.Printf("[raft-sync] Snapshot install complete. Node is back online.")
	return InstallSnapshotReply{Term: b.CurrentTerm}
}

// getPeerConn retrieves an active connection or dials a new one safely
func (b *BrainState) getPeerConn(peerID string) (net.Conn, error) {
	b.peerConnsMu.Lock()
	if conn, exists := b.peerConns[peerID]; exists {
		b.peerConnsMu.Unlock()
		return conn, nil
	}
	b.peerConnsMu.Unlock()

	// Dial outside the lock so we don't freeze the Raft node
	newConn, err := net.DialTimeout("tcp", peerID, 50*time.Millisecond)
	if err != nil {
		return nil, err
	}

	b.peerConnsMu.Lock()
	defer b.peerConnsMu.Unlock()

	// Check if another goroutine beat us to it while we were dialing
	if existingConn, exists := b.peerConns[peerID]; exists {
		newConn.Close() // Don't leak the extra connection
		return existingConn, nil
	}

	b.peerConns[peerID] = newConn
	return newConn, nil
}

// dropPeerConn ONLY drops the connection if it matches the one that actually failed
func (b *BrainState) dropPeerConn(peerID string, failedConn net.Conn) {
	if failedConn == nil {
		return
	}

	b.peerConnsMu.Lock()
	defer b.peerConnsMu.Unlock()

	// Only close and delete if the map still holds the exact connection that failed.
	// This prevents us from accidentally destroying a newly established, healthy connection.
	if currentConn, exists := b.peerConns[peerID]; exists && currentConn == failedConn {
		currentConn.Close()
		delete(b.peerConns, peerID)
	}
}

func (b *BrainState) sendRequestVoteRPC(peerID string, args RequestVoteArgs) RequestVoteReply {
	mu := b.getPeerLock(peerID)
	mu.Lock()
	defer mu.Unlock()

	payloadBytes, _ := json.Marshal(args)
	// Assuming OpRequestVote is defined in protocol package
	frame := protocol.PackEnvelope(protocol.OpRequestVote, b.NodeID, uuid.New().String(), payloadBytes)

	// Note: b.peers map/slice would store actual IP addresses in production
	conn, err := b.getPeerConn(peerID)
	if err != nil {
		return RequestVoteReply{}
	}

	conn.SetDeadline(time.Now().Add(3000 * time.Millisecond))
	if _, err = conn.Write(frame); err != nil {
		b.dropPeerConn(peerID, conn)
		return RequestVoteReply{}
	}

	// Wait for response and unpack
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		b.dropPeerConn(peerID, conn)
		return RequestVoteReply{}
	}

	bodyLen := binary.BigEndian.Uint32(header[0:4])
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(conn, body); err != nil {
		b.dropPeerConn(peerID, conn)
		return RequestVoteReply{}
	}

	_, _, _, respBytes, err := protocol.UnpackEnvelope(body)
	if err != nil {
		b.dropPeerConn(peerID, conn)
		return RequestVoteReply{}
	}

	var reply RequestVoteReply
	json.Unmarshal(respBytes, &reply)
	return reply
}

// Removed the b.getPeerLock() from inside here. The caller now handles it.
func (b *BrainState) SendAppendEntriesRPC(peerID string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	payloadBytes, _ := json.Marshal(args)
	frame := protocol.PackEnvelope(protocol.OpAppendEntries, b.NodeID, uuid.New().String(), payloadBytes)

	conn, err := b.getPeerConn(peerID)
	if err != nil {
		return AppendEntriesReply{}, err
	}

	conn.SetDeadline(time.Now().Add(3000 * time.Millisecond))
	if _, err = conn.Write(frame); err != nil {
		b.dropPeerConn(peerID, conn)
		return AppendEntriesReply{}, err
	}

	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		b.dropPeerConn(peerID, conn)
		return AppendEntriesReply{}, err
	}

	bodyLen := binary.BigEndian.Uint32(header[0:4])
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(conn, body); err != nil {
		b.dropPeerConn(peerID, conn)
		return AppendEntriesReply{}, err
	}

	_, _, _, respBytes, err := protocol.UnpackEnvelope(body)
	if err != nil {
		b.dropPeerConn(peerID, conn)
		return AppendEntriesReply{}, err
	}

	var reply AppendEntriesReply
	json.Unmarshal(respBytes, &reply)
	return reply, nil
}

// getPeerLock safely retrieves a dedicated mutex for a specific peer
func (b *BrainState) getPeerLock(peerID string) *sync.Mutex {
	b.peerConnsMu.Lock()
	defer b.peerConnsMu.Unlock()

	if b.peerLocks == nil {
		b.peerLocks = make(map[string]*sync.Mutex)
	}
	if b.peerLocks[peerID] == nil {
		b.peerLocks[peerID] = &sync.Mutex{}
	}
	return b.peerLocks[peerID]
}
