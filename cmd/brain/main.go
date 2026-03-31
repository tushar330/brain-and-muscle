package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/kv"
	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/akashtripathi12/brain-and-muscle/internal/raft"
	"github.com/akashtripathi12/brain-and-muscle/internal/wal"
)

const (
	workerTimeout   = 15 * time.Second
	watcherInterval = 5 * time.Second
	maxMessageBytes = 10 * 1024 * 1024
)

// OutgoingMessage represents a thread-safe payload ready for the Egress Queue
type OutgoingMessage struct {
	OpCode    uint8
	RequestID string
	Payload   interface{}
}

type BrainEvent struct {
	WorkerID string
	OpCode   uint8
	Body     []byte
	Conn     net.Conn                                              // Kept exclusively for WAL engine backward compatibility
	Send     func(opCode uint8, reqID string, payload interface{}) // The Thread-Safe Egress Callback
}

var eventInbox = make(chan BrainEvent, 10000)

func main() {
	// Identity & Topology
	peerString := envOrDefault("PEERS", "") // e.g., "localhost:8081,localhost:8082"
	var peers []string
	if peerString != "" {
		peers = strings.Split(peerString, ",")
	}

	// Initialize Lock-Free Store and Group Commit WAL
	store := kv.NewKVStore()

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		log.Fatal("NODE_ID environment variable is required")
	}

	// Make sure they are prefixed!
	walFile := nodeID + "_engine.wal"
	snapFile := nodeID + "_snapshot.bin"

	walEngine, err := wal.NewGroupCommitWAL(walFile, snapFile)
	if err != nil {
		log.Fatalf("Failed to initialize WAL: %v", err)
	}

	// Create the bridge channel
	applyCh := make(chan raft.ApplyMsg, 1000)

	// Initialize the singleton Raft State Machine
	raftNode := raft.NewBrainState(nodeID, peers, applyCh, store, snapFile)

	// PHASE 1a: SNAPSHOT RECOVERY
	snapData, err := os.ReadFile(snapFile)
	if err == nil && len(snapData) > 0 {
		log.Println("[boot] Phase 1a: Loading Memory Snapshot...")
		store.ImportSnapshot(snapData)
		log.Println("[boot] Phase 1a Complete: Restored from snapshot.")
	}

	// PHASE 1b: LOCAL RECOVERY (Network is DOWN)
	log.Println("[boot] Phase 1b: Replaying Write-Ahead Log...")
	entries, _ := walEngine.ReadAll()
	for _, entry := range entries {
		store.ApplyReplayEvent(entry)
	}
	log.Printf("[boot] Phase 1b Complete: Reconstructed %d events.", len(entries))

	// PHASE 2: RECONCILIATION
	startHeartbeatWatcher()
	startRaftHeartbeat(raftNode)
	log.Println("[boot] Phase 2: Heartbeat watcher started (Ghost tasks will fail in 15s).")

	// PHASE 3: OPEN FOR BUSINESS
	go BrainProcessLoop(store, walEngine, raftNode)

	go StartExecutionEngine(store, applyCh, walEngine, raftNode)

	port := envOrDefault("PORT", "8080")
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Close()

	log.Printf("[boot] Phase 3: Brain is listening on :%s", port)

	rand.Seed(time.Now().UnixNano())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("\n[system] SIGTERM received. Shutting down gracefully...")
		walEngine.Enqueue(wal.WriteRequest{
			// Force a final flush of the WAL buffer before exit
		})
		time.Sleep(100 * time.Millisecond) // Give WAL a moment to flush
		os.Exit(0)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("failed to accept connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

// handleConnection safely routes TCP ingress and implements the Actor Egress Pattern
func handleConnection(conn net.Conn) {
	workerAddr := conn.RemoteAddr().String()
	log.Printf("new raw connection from %s", workerAddr)

	egressQueue := make(chan OutgoingMessage, 100)
	closeSignal := make(chan struct{})

	// 🚨 THE EGRESS GOROUTINE 🚨
	// This is the ONLY thread allowed to write to `conn`.
	// This eliminates all TOCTOU panics during Raft elections.
	go func() {
		defer conn.Close()
		for {
			select {
			case msg := <-egressQueue:
				// Execute the raw network write safely
				protocol.SendResponse(conn, msg.OpCode, msg.RequestID, msg.Payload)
			case <-closeSignal:
				// The reader loop died, tear down the socket gracefully
				return
			}
		}
	}()

	// Ensure the writer is killed when the reader loop inevitably exits
	defer close(closeSignal)

	// Egress Callback injected into the BrainEvent
	sendFunc := func(opCode uint8, reqID string, payload interface{}) {
		select {
		case egressQueue <- OutgoingMessage{OpCode: opCode, RequestID: reqID, Payload: payload}:
		default:
			log.Printf("[conn] egress queue full for %s, dropping response", workerAddr)
		}
	}

	for {
		// Only set the strict deadline for the header. If it hangs, we drop.
		conn.SetReadDeadline(time.Now().Add(workerTimeout))

		header := make([]byte, 5)
		if _, err := io.ReadFull(conn, header); err != nil {
			if err != io.EOF {
				log.Printf("read error from %s: %v", workerAddr, err)
			}
			return
		}

		// Header cleared. Give ample time to ingest large payloads without timing out.
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		bodyLen := binary.BigEndian.Uint32(header[0:4])
		opCode := header[4]

		if bodyLen > maxMessageBytes {
			log.Printf("payload too large from %s", workerAddr)
			return
		}

		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(conn, body); err != nil {
			log.Printf("error reading body from %s: %v", workerAddr, err)
			return
		}

		select {
		case eventInbox <- BrainEvent{
			WorkerID: workerAddr,
			OpCode:   opCode,
			Body:     body,
			Conn:     conn,     // Legacy bypass for the WAL layer
			Send:     sendFunc, // The thread-safe injection
		}:
			// Successfully queued
		case <-time.After(2 * time.Second):
			log.Printf("[conn] inbox full — dropping message from %s", workerAddr)
			sendFunc(protocol.OpError, "", "server_overloaded")
			return
		}
	}
}

func BrainProcessLoop(store *kv.KVStore, walEngine *wal.GroupCommitWAL, raftNode *raft.BrainState) {
	log.Println("[actor] BrainProcessLoop started. Operating in lock-free mode.")

	for {
		var event BrainEvent
		var msg protocol.Message

		select {
		// --- 1. RAFT INTERNAL EVENTS (Thread-Safe!) ---
		case raftEvent := <-raftNode.RaftInbox:
			switch ev := raftEvent.(type) {
			case raft.VoteReplyEvent:
				raftNode.ProcessVoteReply(ev)
			case raft.AppendReplyEvent:
				raftNode.ProcessAppendReply(ev.PeerID, ev.Reply)
			case raft.SingleNodeCommitEvent:
				raftNode.ProcessSingleNodeCommit(ev)
			case raft.RetrySyncEvent:
				raftNode.SendAppendEntries(ev.PeerID)
			case raft.SnapshotReplyEvent:
				raftNode.ProcessSnapshotReply(ev.PeerID, ev.Reply)
			}
			continue

		case entry := <-raftNode.DiskSyncChannel:
			if raftNode.State != "LEADER" {
				log.Printf("[raft] Follower enqueuing replicated entry to WAL (Payload: %s)", string(entry))
				walEngine.Enqueue(wal.WriteRequest{
					Payload: entry,
				})
			}

		// --- 2. TCP CLIENT & WORKER EVENTS ---
		case event = <-eventInbox:
			if event.OpCode != protocol.OpTick && event.OpCode != protocol.OpRaftTick {
				nodeID, reqID, _, payloadBytes, err := protocol.UnpackEnvelope(event.Body)

				if err != nil {
					log.Printf("[actor] bad envelope from %s: %v", event.WorkerID, err)
					continue
				}

				msg = protocol.Message{
					OpCode:    event.OpCode,
					NodeID:    nodeID,
					RequestID: reqID,
					Payload:   payloadBytes,
				}
			}

			// --- THE GHOST LEADER GATE ---
			requiresLeader := false
			switch event.OpCode {
			case protocol.OpRegisterWorker,
				protocol.OpHeartbeat,
				protocol.OpRequestTask,
				protocol.OpSubmitResult,
				protocol.OpTaskFailed:
				requiresLeader = true
			}

			if requiresLeader && raftNode.State != "LEADER" {
				if raftNode.LeaderID != "" {
					event.Send(protocol.OpRedirect, msg.RequestID, protocol.RedirectPayload{
						LeaderID: raftNode.LeaderID,
					})
				} else {
					event.Send(protocol.OpError, msg.RequestID, "ELECTION_IN_PROGRESS")
				}
				continue
			}

			switch event.OpCode {

			// EPHEMERAL STATE (No WAL, Instant TCP ACK)

			case protocol.OpRaftTick:
				raftNode.CheckElection()

				if raftNode.State == "LEADER" {
					raftNode.SendAppendEntriesToAll()
				}

			case protocol.OpRegisterWorker:
				var req protocol.RegisterWorkerPayload
				json.Unmarshal(msg.Payload, &req)
				store.RegisterWorker(msg.NodeID, req.TotalCPU, req.TotalRAM)
				event.Send(protocol.OpWorkerRegistered, msg.RequestID, nil)

			case protocol.OpHeartbeat:
				var hb protocol.HeartbeatPayload
				json.Unmarshal(msg.Payload, &hb)
				store.UpdateWorkerHeartbeat(msg.NodeID, hb.AvailCPU, hb.AvailRAM)
				event.Send(protocol.OpHeartbeatAck, msg.RequestID, nil)

			case protocol.OpGetTaskStatus:
				var req protocol.GetTaskStatusPayload
				json.Unmarshal(msg.Payload, &req)

				if task, ok := store.GetTask(req.TaskID); ok {
					event.Send(protocol.OpTaskStatus, msg.RequestID, protocol.TaskStatusPayload{
						TaskID: task.ID,
						Status: int(task.Status),
					})
				} else if job, ok := store.GetJob(req.TaskID); ok {
					event.Send(protocol.OpTaskStatus, msg.RequestID, protocol.TaskStatusPayload{
						TaskID: req.TaskID,
						Status: int(job.Status),
					})
				} else {
					event.Send(protocol.OpError, msg.RequestID, nil)
				}

			// DURABLE STATE (Mutate Memory → Push to WAL → WAL handles TCP ACK)

			case protocol.OpTick:

				if raftNode.State == "LEADER" {
					deadWorkers := store.GetDeadWorkers(workerTimeout)
					for _, workerID := range deadWorkers {
						log.Printf("[watcher] worker %s died — requeueing tasks", workerID)

						deadTaskIDs := store.GetWorkerTaskIDs(workerID)
						for _, taskID := range deadTaskIDs {
							entry := wal.LogEntry{Event: wal.EventTaskFailed, TaskID: taskID, WorkerID: workerID}
							walEngine.Enqueue(wal.WriteRequest{Entry: entry})
							b, _ := json.Marshal(entry)
							safePayload := append([]byte("SYNC:"), b...)
							raftNode.Propose(safePayload)
						}

						store.RemoveWorker(workerID)
					}
				}

				tFreed, jFreed := store.SweepTerminalState(5 * time.Minute)
				if tFreed > 0 || jFreed > 0 {
					log.Printf("[sweeper] freed %d tasks, %d jobs from memory", tFreed, jFreed)
				}

			case protocol.OpRequestTask:
				var req protocol.RequestTaskPayload
				json.Unmarshal(msg.Payload, &req)

				store.UpdateWorkerHeartbeat(msg.NodeID, req.AvailCPU, req.AvailRAM)

				if req.QueueName == "" {
					req.QueueName = "default"
				}

				task, ok := store.PopBestFitTask(req.QueueName, req.AvailCPU, req.AvailRAM)
				if !ok {
					event.Send(protocol.OpNoTasksAvailable, msg.RequestID, nil)
					continue
				}

				store.TransitionTaskState(task.ID, kv.StatusPending, kv.StatusRunning, msg.NodeID)

				entry := wal.LogEntry{Event: wal.EventTaskDispatched, TaskID: task.ID, WorkerID: msg.NodeID}
				walEngine.Enqueue(wal.WriteRequest{
					Entry:     entry,
					Conn:      event.Conn, // WAL still gets the raw connection
					OpCode:    protocol.OpTaskAssigned,
					RequestID: msg.RequestID,
					Payload: protocol.AssignedTask{
						ID: task.ID, Payload: task.Payload, QueueName: task.QueueName,
						RequiredCPU: task.RequiredCPU, RequiredRAM: task.RequiredRAM,
					},
				})

				b, _ := json.Marshal(entry)
				safePayload := append([]byte("SYNC:"), b...)
				raftNode.Propose(safePayload)

			case protocol.OpSubmitTask:
				safePayload := append([]byte("TASK:"), msg.Payload...)
				index, _, isLeader := raftNode.Propose(safePayload)

				if !isLeader {
					event.Send(protocol.OpRedirect, msg.RequestID, protocol.RedirectPayload{
						LeaderID: raftNode.LeaderID,
					})
					continue
				}

				replyCh := make(chan raft.ClientReply, 1)
				raftNode.RegisterClientChannel(index, replyCh)
				raftNode.SendAppendEntriesToAll()

				go func(send func(uint8, string, interface{}), reqID string, taskIdx uint64) {
					select {
					case <-replyCh:
						send(protocol.OpTaskAccepted, reqID, protocol.TaskAcceptedPayload{
							TaskID: fmt.Sprintf("task-%d", taskIdx),
						})
					case <-time.After(5 * time.Second):
						send(protocol.OpError, reqID, "CONSENSUS_TIMEOUT")
					}
				}(event.Send, msg.RequestID, index)

			case protocol.OpSubmitResult:
				var envelope protocol.SubmitResultPayload
				json.Unmarshal(msg.Payload, &envelope)

				submittedTask, taskFound := store.GetTask(envelope.TaskID)
				isDAG := taskFound && submittedTask.JobID != ""

				if isDAG {
					var dagResult protocol.TaskCompletePayload
					json.Unmarshal([]byte(envelope.ResultPayload), &dagResult)

					if dagResult.JobType == "GATHER" {
						entry := wal.LogEntry{Event: wal.EventTaskCompleted, TaskID: envelope.TaskID, Payload: msg.Payload}
						walEngine.Enqueue(wal.WriteRequest{
							Entry: entry, Conn: event.Conn, OpCode: protocol.OpTaskCompleted, RequestID: msg.RequestID,
						})
						b, _ := json.Marshal(entry)
						safePayload := append([]byte("SYNC:"), b...)
						raftNode.Propose(safePayload)
					} else {
						mapPayload, _ := json.Marshal(protocol.MapCompletedPayload{
							ChunkIndex: dagResult.ChunkIndex,
							ResultURI:  dagResult.ResultURI,
						})
						entryMap := wal.LogEntry{Event: wal.EventMAPCompleted, JobID: dagResult.JobID, Payload: mapPayload}
						entryTask := wal.LogEntry{Event: wal.EventTaskCompleted, TaskID: envelope.TaskID, Payload: msg.Payload}

						walEngine.Enqueue(wal.WriteRequest{
							Entry: entryMap, Conn: event.Conn, OpCode: protocol.OpTaskCompleted, RequestID: msg.RequestID,
						})

						b1, _ := json.Marshal(entryMap)
						safe1 := append([]byte("SYNC:"), b1...)
						raftNode.Propose(safe1)

						b2, _ := json.Marshal(entryTask)
						safe2 := append([]byte("SYNC:"), b2...)
						raftNode.Propose(safe2)
					}
				} else {
					entry := wal.LogEntry{Event: wal.EventTaskCompleted, TaskID: envelope.TaskID, Payload: msg.Payload}
					walEngine.Enqueue(wal.WriteRequest{
						Entry: entry, Conn: event.Conn, OpCode: protocol.OpTaskCompleted, RequestID: msg.RequestID,
					})
					b, _ := json.Marshal(entry)
					safePayload := append([]byte("SYNC:"), b...)
					raftNode.Propose(safePayload)
				}

			case protocol.OpTaskFailed:
				var req protocol.TaskFailedPayload
				json.Unmarshal(msg.Payload, &req)

				entry := wal.LogEntry{Event: wal.EventTaskFailed, TaskID: req.TaskID, WorkerID: msg.NodeID}
				walEngine.Enqueue(wal.WriteRequest{
					Entry:     entry,
					Conn:      event.Conn,
					OpCode:    protocol.OpTaskFailedAck,
					RequestID: msg.RequestID,
				})
				b, _ := json.Marshal(entry)
				safePayload := append([]byte("SYNC:"), b...)
				raftNode.Propose(safePayload)

			// RAFT CONSENSUS PROTOCOL
			case protocol.OpInstallSnapshot:
				var args raft.InstallSnapshotArgs
				if err := json.Unmarshal(msg.Payload, &args); err != nil {
					log.Printf("Failed to unmarshal InstallSnapshotArgs: %v", err)
					continue
				}

				// Pass it to the Receiver!
				reply := raftNode.ProcessInstallSnapshot(args)

				protocol.SendResponse(event.Conn, msg.OpCode, msg.RequestID, reply)

			case protocol.OpRequestVote:
				var args raft.RequestVoteArgs
				json.Unmarshal(msg.Payload, &args)
				reply := raftNode.ProcessRequestVote(args)

				// 🚨 Thread-Safe Write 🚨
				event.Send(protocol.OpRequestVoteAck, msg.RequestID, reply)

			case protocol.OpAppendEntries:
				var args raft.AppendEntriesArgs
				json.Unmarshal(msg.Payload, &args)
				reply := raftNode.ProcessAppendEntries(args)

				// 🚨 Thread-Safe Write 🚨
				event.Send(protocol.OpAppendEntriesAck, msg.RequestID, reply)

			}
		}
	}
}

func startHeartbeatWatcher() {
	go func() {
		ticker := time.NewTicker(watcherInterval)
		defer ticker.Stop()
		for range ticker.C {
			eventInbox <- BrainEvent{OpCode: protocol.OpTick}
		}
	}()
}

func startRaftHeartbeat(raftNode *raft.BrainState) {
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			eventInbox <- BrainEvent{OpCode: protocol.OpRaftTick}
		}
	}()
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
