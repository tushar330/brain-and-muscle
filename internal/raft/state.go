package raft

import (
	"bufio"
	"encoding/json"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/wal"
)

type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
}

type ClientReply string

type Engine interface {
	ExportSnapshot() []byte
	ImportSnapshot([]byte) error
}

type BrainState struct {
	// Concurrency / Application Bridge
	NodeID                string
	pendingClientRequests map[uint64]chan ClientReply
	applyCh               chan ApplyMsg
	applyQueue            chan ApplyMsg
	RaftInbox             chan interface{}
	DiskSyncChannel       chan []byte

	// Persistent State (Saved to WAL)
	walFile           *os.File
	CurrentTerm       uint64
	VotedFor          string
	Log               []wal.LogEntry
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Engine            Engine
	snapPath          string

	// Volatile State
	State       string // "LEADER", "FOLLOWER", or "CANDIDATE"
	commitIndex uint64
	lastApplied uint64
	LeaderID    string

	// Candidate Volatile State
	votesReceived int

	// Leader Volatile State
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	peers      []string

	// Election Timer State
	lastHeartbeat   time.Time
	electionTimeout time.Duration

	// Persistent connection pool for peers
	peerConns   map[string]net.Conn
	peerConnsMu sync.Mutex
	peerLocks   map[string]*sync.Mutex
	mu          sync.Mutex
}

type RaftRecord struct {
	Type              string        `json:"type"` // "META", "APPEND", or "TRUNCATE"
	CurrentTerm       uint64        `json:"current_term,omitempty"`
	VotedFor          string        `json:"voted_for,omitempty"`
	Entry             *wal.LogEntry `json:"entry,omitempty"`
	TruncateIdx       uint64        `json:"truncate_idx,omitempty"`
	LastIncludedIndex uint64        `json:"last_included_index,omitempty"`
	LastIncludedTerm  uint64        `json:"last_included_term,omitempty"`
}

type SnapshotReplyEvent struct {
	PeerID string
	Reply  InstallSnapshotReply
}

// Thread-safe helper to append a single line to the Raft WAL
func (b *BrainState) appendRaftRecord(rec RaftRecord) {
	data, _ := json.Marshal(rec)
	data = append(data, '\n')

	if b.walFile != nil {
		b.walFile.Write(data)
		b.walFile.Sync()
	}
}

// PersistMeta records changes to the node's term or vote.
func (b *BrainState) PersistMeta() {
	b.appendRaftRecord(RaftRecord{
		Type:              "META",
		CurrentTerm:       b.CurrentTerm,
		VotedFor:          b.VotedFor,
		LastIncludedIndex: b.LastIncludedIndex,
		LastIncludedTerm:  b.LastIncludedTerm,
	})
}

// PersistAppend records a newly appended log entry.
func (b *BrainState) PersistAppend(entry wal.LogEntry) {
	b.appendRaftRecord(RaftRecord{
		Type:  "APPEND",
		Entry: &entry,
	})
}

// PersistTruncate records when a Follower deletes conflicting logs.
func (b *BrainState) PersistTruncate(index uint64) {
	b.appendRaftRecord(RaftRecord{
		Type:        "TRUNCATE",
		TruncateIdx: index,
	})
}

// Restore reads the append-only log line-by-line
func (b *BrainState) Restore() {
	filename := b.NodeID + "_raft_state.jsonl"
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("[raft] failed to open %s: %v", filename, err)
	}
	b.walFile = f

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var rec RaftRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			log.Printf("[raft] WARNING: dropping corrupt WAL line (likely torn write from crash): %v", err)
			break
		}

		switch rec.Type {
		case "META":
			b.CurrentTerm = rec.CurrentTerm
			b.VotedFor = rec.VotedFor
			b.LastIncludedIndex = rec.LastIncludedIndex
			b.LastIncludedTerm = rec.LastIncludedTerm
		case "APPEND":
			if rec.Entry != nil {
				b.Log = append(b.Log, *rec.Entry)
			}
		case "TRUNCATE":
			// Corrected truncate index logic for offsets
			if rec.TruncateIdx >= b.LastIncludedIndex && rec.TruncateIdx < b.lastLogIndex()+1 {
				physIdx := b.physicalIndex(rec.TruncateIdx)
				b.Log = b.Log[:physIdx]
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("[raft] CRITICAL: scanner error on raft_state.jsonl: %v", err)
	}

	b.commitIndex = b.LastIncludedIndex
	b.lastApplied = b.LastIncludedIndex

	log.Printf("[boot] Raft state restored. Resuming from Index %d", b.LastIncludedIndex)
}

// physicalIndex converts a logical Raft index to a physical slice offset.
func (b *BrainState) physicalIndex(logicalIndex uint64) int {
	if logicalIndex < b.LastIncludedIndex {
		return -1
	}
	return int(logicalIndex - b.LastIncludedIndex)
}

// lastLogIndex returns the logical index of the last entry in the log.
func (b *BrainState) lastLogIndex() uint64 {
	return b.LastIncludedIndex + uint64(len(b.Log)) - 1
}

// GetLogTerm returns the term of the logical log index.
func (b *BrainState) GetLogTerm(logicalIndex uint64) uint64 {
	if logicalIndex == b.LastIncludedIndex {
		return b.LastIncludedTerm
	}
	physIdx := b.physicalIndex(logicalIndex)
	if physIdx < 0 || physIdx >= len(b.Log) {
		return 0
	}
	return b.Log[physIdx].Term
}

// LastApplied returns the index of the last log entry applied to the state machine.
func (b *BrainState) LastApplied() uint64 {
	return b.lastApplied
}

// CompactRaftState throws away logs prior to the snapshotIndex and rewrites the jsonl file.
func (b *BrainState) CompactRaftState(snapshotIndex uint64) {
	if snapshotIndex <= b.LastIncludedIndex || snapshotIndex > b.lastLogIndex() {
		return
	}

	log.Printf("[raft] Compacting Consensus State (Truncating up to index %d)", snapshotIndex)

	// 1. Snapshot the required log window.
	// Raft requires at least one entry at physical [0] to maintain index arithmetic.
	// This entry will be the "dummy" representing the last included index.
	physIdx := b.physicalIndex(snapshotIndex)
	if physIdx < 0 || physIdx >= len(b.Log) {
		log.Printf("[raft] CRITICAL BUG: CompactRaftState index %d out of range (Current Log: %d to %d)", snapshotIndex, b.LastIncludedIndex, b.lastLogIndex())
		return
	}

	b.LastIncludedTerm = b.Log[physIdx].Term
	var newLog []wal.LogEntry
	newLog = append(newLog, b.Log[physIdx])
	if physIdx+1 < len(b.Log) {
		newLog = append(newLog, b.Log[physIdx+1:]...)
	}
	b.Log = newLog
	b.LastIncludedIndex = snapshotIndex

	// 2. Rotate the persistent file.
	if b.walFile != nil {
		b.walFile.Close()
	}

	filename := b.NodeID + "_raft_state.jsonl"
	os.Remove(filename)

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("[raft] catastrophic failure Rotating Raft WAL: %v", err)
	}
	b.walFile = f

	// 3. Persist the current Hard State and the compacted Log.
	b.PersistMeta()
	for _, entry := range b.Log {
		b.PersistAppend(entry)
	}

	log.Printf("[raft] Compaction complete. Log size reduced to %d entries.", len(b.Log))
}

func NewBrainState(nodeID string, peers []string, applyCh chan ApplyMsg, engine Engine, snapPath string) *BrainState {
	b := &BrainState{
		NodeID:                nodeID,
		peers:                 peers,
		pendingClientRequests: make(map[uint64]chan ClientReply),
		matchIndex:            make(map[string]uint64),
		nextIndex:             make(map[string]uint64),
		applyCh:               applyCh,
		applyQueue:            make(chan ApplyMsg, 10000),
		State:                 "FOLLOWER",
		Log:                   make([]wal.LogEntry, 1),
		RaftInbox:             make(chan interface{}, 1000),
		peerConns:             make(map[string]net.Conn),
		DiskSyncChannel:       make(chan []byte, 10000),
		Engine:                engine,
		snapPath:              snapPath,
	}

	go func() {
		for msg := range b.applyQueue {
			b.applyCh <- msg
		}
	}()
	b.Restore()
	b.resetElectionTimer()
	return b
}

// ProcessAppendReply runs on the LEADER when a follower ACKs a log replication
func (b *BrainState) ProcessAppendReply(followerID string, reply AppendEntriesReply) {
	if reply.Term > b.CurrentTerm {
		b.State = "FOLLOWER"
		b.CurrentTerm = reply.Term
		b.VotedFor = ""
		b.PersistMeta()
		// We do not close pendingClientRequests here. Let the TCP 5s timeout
		// handle it, or implement an explicit leadership-lost broadcast.
		return
	}

	// 2. Log Inconsistency Backoff
	if !reply.Success {
		if b.nextIndex[followerID] > 1 {
			b.nextIndex[followerID]--
		}

		// Ensure we haven't been deposed in a different goroutine before retrying
		if b.State == "LEADER" {
			go b.SendAppendEntries(followerID)
		}
		return
	}

	// 3. Match Index Update
	b.matchIndex[followerID] = reply.HighestIndex
	b.nextIndex[followerID] = reply.HighestIndex + 1

	replicatedIndices := make([]uint64, 0)
	replicatedIndices = append(replicatedIndices, b.lastLogIndex()) // Include the leader
	for _, peer := range b.peers {
		if !b.isSelf(peer) && peer != "" {
			replicatedIndices = append(replicatedIndices, b.matchIndex[peer])
		}
	}

	sort.Slice(replicatedIndices, func(i, j int) bool {
		return replicatedIndices[i] > replicatedIndices[j]
	})

	totalNodes := len(replicatedIndices)
	quorumIdx := totalNodes / 2 // Works mathematically whether N is even or odd
	newCommitIndex := replicatedIndices[quorumIdx]

	// ✅ FIX BUG 3: Slice Bounds Check Shield
	if newCommitIndex > b.lastLogIndex() {
		return
	}

	// Raft Rule: A leader can only advance commit index if the entry is from its CURRENT term
	if newCommitIndex > b.commitIndex && b.GetLogTerm(newCommitIndex) == b.CurrentTerm {
		b.commitIndex = newCommitIndex
		b.applyCommittedEntries()
	}
}

// ProcessSnapshotReply handles the response from a follower after an InstallSnapshot RPC.
func (b *BrainState) ProcessSnapshotReply(followerID string, reply InstallSnapshotReply) {
	if reply.Term > b.CurrentTerm {
		b.State = "FOLLOWER"
		b.CurrentTerm = reply.Term
		b.VotedFor = ""
		b.PersistMeta()
		return
	}

	// If successful (or at least term matches), we update their nextIndex to the index AFTER the snapshot.
	// Since we don't have a Success flag in the user's InstallSnapshotReply, we assume match on term parity.
	b.matchIndex[followerID] = b.LastIncludedIndex
	b.nextIndex[followerID] = b.LastIncludedIndex + 1
}

// ProcessSingleNodeCommit handles auto-committing a task when running in a standalone, single-node cluster setting.
func (b *BrainState) ProcessSingleNodeCommit(ev SingleNodeCommitEvent) {
	if ev.Index > b.commitIndex {
		b.commitIndex = ev.Index
		b.applyCommittedEntries()
	}
}

// applyCommittedEntries safely flushes committed logs down the applyCh without blocking
func (b *BrainState) applyCommittedEntries() {
	if b.commitIndex <= b.lastApplied {
		return
	}

	var batch []ApplyMsg

	for idx := b.lastApplied + 1; idx <= b.commitIndex; idx++ {
		physIdx := b.physicalIndex(idx)
		if physIdx < 0 || physIdx >= len(b.Log) {
			log.Printf("[raft] CRITICAL ERROR: applyCommittedEntries index %d out of physical range (lastIncluded: %d)", idx, b.LastIncludedIndex)
			break
		}
		entry := b.Log[physIdx]

		batch = append(batch, ApplyMsg{
			CommandValid: true,
			Command:      entry.Payload,
			CommandIndex: idx,
		})

		if replyChan, exists := b.pendingClientRequests[idx]; exists {
			select {
			case replyChan <- "TASK_COMMITTED":
				// Client received the ACK
			default:
				// Client TCP handler timed out and stopped listening. Drop safely.
				log.Printf("[raft] Client abandoned task %d before commit", idx)
			}
			close(replyChan)
			delete(b.pendingClientRequests, idx)
		}

		b.lastApplied = idx
	}

	for _, msg := range batch {
		b.applyQueue <- msg
	}
}

// resetElectionTimer is called whenever we get a valid heartbeat or grant a vote.
func (b *BrainState) resetElectionTimer() {
	b.lastHeartbeat = time.Now()
	ms := 5000 + rand.Intn(1000)
	b.electionTimeout = time.Duration(ms) * time.Millisecond
}

// Propose allows the TCP layer to submit a new client task to the Raft cluster.
// It returns false if this node is not the Leader.
func (b *BrainState) Propose(payload []byte) (uint64, uint64, bool) {
	if b.State != "LEADER" {
		return 0, 0, false
	}

	entry := wal.LogEntry{
		Term:    b.CurrentTerm,
		Payload: payload,
	}

	b.Log = append(b.Log, entry)
	b.PersistAppend(entry)
	index := b.lastLogIndex()

	// The leader always has its own log entry
	b.matchIndex[b.NodeID] = index

	// Immediately trigger replication to followers
	b.SendAppendEntriesToAll()

	totalNodes := 1
	for _, p := range b.peers {
		if p != b.NodeID && p != "" {
			totalNodes++
		}
	}

	if totalNodes == 1 {
		log.Printf("[raft] Single node detected. Auto-committing task at index %d", index)

		go func(idx uint64) {
			time.Sleep(10 * time.Millisecond)

			b.RaftInbox <- SingleNodeCommitEvent{
				Index: idx,
			}
		}(index)
	}

	return index, b.CurrentTerm, true
}

// RegisterClientChannel allows the TCP handler to block and wait for consensus
func (b *BrainState) RegisterClientChannel(index uint64, ch chan ClientReply) {
	b.pendingClientRequests[index] = ch
}

func (b *BrainState) isSelf(peer string) bool {
	return peer == b.NodeID || strings.HasPrefix(peer, b.NodeID+":")
}
