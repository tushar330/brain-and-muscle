package raft

import (
	"log"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/wal"
)

// CheckElection verifies whether the election timer has expired.
func (b *BrainState) CheckElection() {
	if b.State == "LEADER" {
		return
	}

	if time.Since(b.lastHeartbeat) >= b.electionTimeout {
		log.Printf("[raft] Election timeout reached. Campaigning for Term %d", b.CurrentTerm+1)
		b.startElection()
		b.resetElectionTimer()
	}
}

// startElection triggers when the follower's heartbeat timer expires
func (b *BrainState) startElection() {
	b.State = "CANDIDATE"
	b.CurrentTerm++
	b.VotedFor = b.NodeID
	b.PersistMeta()
	b.votesReceived = 1 // We vote for ourselves

	totalNodes := 1
	for _, p := range b.peers {
		if !b.isSelf(p) && p != "" { //  BUG FIXED: Ghost Quorum terminated
			totalNodes++
		}
	}

	majority := (totalNodes / 2) + 1
	if b.votesReceived >= majority {
		log.Printf("[raft] Single-node majority achieved. Becoming LEADER instantly.")
		b.becomeLeader()
		return
	}

	lastLogIndex := b.lastLogIndex()
	lastLogTerm := b.GetLogTerm(lastLogIndex)

	args := RequestVoteArgs{
		Term:         b.CurrentTerm,
		CandidateID:  b.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, peer := range b.peers {
		if !b.isSelf(peer) && peer != "" { //  BUG FIXED: Stop dialing ourselves
			go func(targetPeer string) {
				reply := b.sendRequestVoteRPC(targetPeer, args)
				b.RaftInbox <- VoteReplyEvent{
					PeerID: targetPeer,
					Reply:  reply,
				}
			}(peer)
		}
	}
}

func (b *BrainState) ProcessVoteReply(event VoteReplyEvent) {
	if event.Reply.Term > b.CurrentTerm {
		b.State = "FOLLOWER"
		b.CurrentTerm = event.Reply.Term
		b.VotedFor = ""
		b.PersistMeta()
		return
	}

	// Shield against delayed network packets
	if event.Reply.Term < b.CurrentTerm || b.State != "CANDIDATE" {
		return
	}

	if event.Reply.VoteGranted {
		b.votesReceived++
		totalNodes := 1
		for _, p := range b.peers {
			if !b.isSelf(p) && p != "" { //  BUG FIXED: Ghost Quorum terminated
				totalNodes++
			}
		}

		majority := (totalNodes / 2) + 1

		if b.votesReceived >= majority { //  BUG FIXED: Use >= instead of ==
			b.becomeLeader()
		}
	}
}

func (b *BrainState) becomeLeader() {
	log.Printf("[raft] 👑 Won election! Transitioning to LEADER for Term %d", b.CurrentTerm)
	b.State = "LEADER"
	b.LeaderID = b.NodeID

	b.Log = append(b.Log, wal.LogEntry{
		Term:    b.CurrentTerm,
		Payload: []byte("NOOP:LEADER_ELECTED"),
	})

	lastLogIndex := b.lastLogIndex()
	for _, peer := range b.peers {
		b.nextIndex[peer] = lastLogIndex + 1
		b.matchIndex[peer] = 0
	}

	// Instantly suppress follower elections
	b.SendAppendEntriesToAll()
}

func (b *BrainState) ProcessRequestVote(args RequestVoteArgs) RequestVoteReply {
	if args.Term < b.CurrentTerm {
		return RequestVoteReply{Term: b.CurrentTerm, VoteGranted: false}
	}

	if args.Term > b.CurrentTerm {
		b.State = "FOLLOWER"
		b.CurrentTerm = args.Term
		b.VotedFor = ""
		b.PersistMeta()
	}

	if b.VotedFor != "" && b.VotedFor != args.CandidateID {
		return RequestVoteReply{Term: b.CurrentTerm, VoteGranted: false}
	}

	// Lexicographical log freshness check
	myLastIndex := b.lastLogIndex()
	myLastTerm := b.GetLogTerm(myLastIndex)

	isUpToDate := false
	if args.LastLogTerm > myLastTerm {
		isUpToDate = true
	} else if args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex {
		isUpToDate = true
	}

	if isUpToDate {
		b.VotedFor = args.CandidateID
		b.resetElectionTimer()
		b.PersistMeta()
		return RequestVoteReply{Term: b.CurrentTerm, VoteGranted: true}
	}

	return RequestVoteReply{Term: b.CurrentTerm, VoteGranted: false}
}
