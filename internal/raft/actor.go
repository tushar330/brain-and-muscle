package raft

import (
	"strings"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/wal"
)

// ProcessAppendEntries runs on a FOLLOWER when receiving logs/heartbeats from the Leader
func (b *BrainState) ProcessAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	// 1. The Stale Term Check (Reject old leaders)
	if args.Term < b.CurrentTerm {
		return AppendEntriesReply{
			Term:    b.CurrentTerm,
			Success: false,
		}
	}

	// 2. Acknowledge valid leader
	if args.Term > b.CurrentTerm || b.State == "CANDIDATE" {
		b.State = "FOLLOWER"
		b.CurrentTerm = args.Term
		b.VotedFor = ""
		b.PersistMeta()
	}
	b.LeaderID = args.LeaderId
	b.resetElectionTimer() // Suppress our own election!

	// 3. The Consistency Check
	myLastIndex := b.lastLogIndex()
	if args.PrevLogIndex > myLastIndex {
		// We are missing data. Tell leader to back up.
		return AppendEntriesReply{Term: b.CurrentTerm, Success: false}
	}
	if args.PrevLogIndex >= b.LastIncludedIndex && b.GetLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		// Our log at this index conflicts with the leader's.
		return AppendEntriesReply{Term: b.CurrentTerm, Success: false}
	}

	// 4. Safely Truncate and Append
	insertIndex := args.PrevLogIndex + 1
	newEntriesIndex := 0

	for {
		if insertIndex > b.lastLogIndex() || newEntriesIndex >= len(args.Entries) {
			break
		}
		if b.GetLogTerm(insertIndex) != args.Entries[newEntriesIndex].Term {
			b.PersistTruncate(insertIndex)
			break
		}
		insertIndex++
		newEntriesIndex++
	}

	// If there are new entries we don't have, append them (and truncate conflicting ones)
	if newEntriesIndex < len(args.Entries) {
		physInsertIdx := b.physicalIndex(insertIndex)
		if physInsertIdx < 0 {
			// This should not happen in basic replication unless we are replacing the dummy entry
			physInsertIdx = 0
		}
		b.Log = append(b.Log[:physInsertIdx], args.Entries[newEntriesIndex:]...)
		for _, e := range args.Entries[newEntriesIndex:] {
			b.PersistAppend(e)

			if !strings.HasPrefix(string(e.Payload), "NOOP:") {
				b.DiskSyncChannel <- e.Payload
			}
		}
	}

	// 5. Update Follower's Commit Index
	if args.LeaderCommit > b.commitIndex {
		// Set commitIndex = min(leaderCommit, index of last new entry)
		lastEntryIndex := b.lastLogIndex()
		if args.LeaderCommit < lastEntryIndex {
			b.commitIndex = args.LeaderCommit
		} else {
			b.commitIndex = lastEntryIndex
		}
		b.applyCommittedEntries()
	}

	return AppendEntriesReply{
		Term:         b.CurrentTerm,
		Success:      true,
		HighestIndex: b.lastLogIndex(),
	}
}

// SendAppendEntriesToAll is the Leader's Fan-Out function
func (b *BrainState) SendAppendEntriesToAll() {
	for _, peer := range b.peers {
		if !b.isSelf(peer) && peer != "" {
			b.SendAppendEntries(peer)
		}
	}
}

// SendAppendEntries builds the zero-allocation payload for a specific follower
func (b *BrainState) SendAppendEntries(peer string) {
	nextIdx := b.nextIndex[peer]

	if nextIdx <= b.LastIncludedIndex {

		args := InstallSnapshotArgs{
			Term:              b.CurrentTerm,
			LeaderId:          b.NodeID,
			LastIncludedIndex: b.LastIncludedIndex,
			LastIncludedTerm:  b.LastIncludedTerm,
			Data:              b.Engine.ExportSnapshot(),
		}

		go func() {
			mu := b.getPeerLock(peer)
			if !mu.TryLock() {
				return // Previous snapshot is still transmitting!
			}
			defer mu.Unlock()

			reply, err := b.SendInstallSnapshotRPC(peer, args)
			if err == nil {
				b.RaftInbox <- SnapshotReplyEvent{PeerID: peer, Reply: reply}
			} else {
				// If it fails, eagerly retry so we don't stall
				go func(target string) {
					time.Sleep(10 * time.Millisecond)
					b.RaftInbox <- RetrySyncEvent{PeerID: target}
				}(peer)
			}
		}()
		return // ABORT normal AppendEntries
	}

	lastLogIndex := b.lastLogIndex()

	if nextIdx > lastLogIndex+1 {
		nextIdx = lastLogIndex + 1
	}

	prevLogIdx := nextIdx - 1
	prevLogTerm := uint64(0)
	if prevLogIdx >= b.LastIncludedIndex {
		prevLogTerm = b.GetLogTerm(prevLogIdx)
	}

	var entriesToSend []wal.LogEntry
	if nextIdx <= lastLogIndex {
		physNextIdx := b.physicalIndex(nextIdx)
		if physNextIdx >= 0 && physNextIdx < len(b.Log) {
			src := b.Log[physNextIdx:]
			entriesToSend = make([]wal.LogEntry, len(src))
			copy(entriesToSend, src)
		}
	}

	// Prepare arguments synchronously
	args := AppendEntriesArgs{
		Term:         b.CurrentTerm,
		LeaderId:     b.NodeID,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		Entries:      entriesToSend,
		LeaderCommit: b.commitIndex,
	}

	// Spawn the background worker strictly for the blocking network call
	go func(targetPeer string, reqArgs AppendEntriesArgs) {
		mu := b.getPeerLock(targetPeer)

		// If we can't get the lock immediately, a previous RPC is still hanging.
		// Drop this heartbeat to prevent a goroutine leak.
		if !mu.TryLock() {
			return
		}
		defer mu.Unlock()

		reply, err := b.SendAppendEntriesRPC(targetPeer, reqArgs)
		if err != nil {
			// sleep for 10ms and inject a retry event directly into the inbox.
			go func(target string) {
				time.Sleep(10 * time.Millisecond)
				b.RaftInbox <- RetrySyncEvent{PeerID: target}
			}(targetPeer)
			return
		}

		b.RaftInbox <- AppendReplyEvent{
			PeerID: targetPeer,
			Reply:  reply,
		}
	}(peer, args)
}
