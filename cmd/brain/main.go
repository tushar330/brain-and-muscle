package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/db"
	"github.com/akashtripathi12/brain-and-muscle/internal/kv"
	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/akashtripathi12/brain-and-muscle/internal/raft"
	"github.com/akashtripathi12/brain-and-muscle/internal/storage"
	"github.com/akashtripathi12/brain-and-muscle/internal/wal"
	"github.com/joho/godotenv"
)

const (
	workerTimeout   = 120 * time.Second
	watcherInterval = 30 * time.Second
	maxMessageBytes = 10 * 1024 * 1024
)

// OutgoingMessage: Thread-safe Egress payload.
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

var (
	connRegistryMu sync.RWMutex
	connRegistry   = make(map[string]func(uint8, string, interface{}))
)

func main() {
	// 1. MUST BE FIRST: Load environment variables
	if err := godotenv.Load(); err != nil {
		if err := godotenv.Load("../../.env"); err != nil {
			log.Println("[boot] Warning: No .env file found, using system environment variables")
		}
	}

	// 2. Identity & Topology
	peerString := envOrDefault("PEERS", "")
	var peers []string
	if peerString != "" {
		peers = strings.Split(peerString, ",")
	}

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		log.Fatal("NODE_ID environment variable is required")
	}

	// 3. Initialize Azure Blob Storage
	azureConn := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	azureContainer := envOrDefault("AZURE_CONTAINER_NAME", "task-outputs")
	if azureConn != "" {
		if err := storage.InitBlobService(azureConn, azureContainer); err != nil {
			log.Printf("[boot] Failed to initialize Azure Storage: %v", err)
		} else {
			log.Println("[boot] Azure Storage initialized successfully")
		}
	} else {
		log.Println("[boot] Warning: AZURE_STORAGE_CONNECTION_STRING not set, local mode only")
	}

	// 4. Initialize Database (Supabase/Postgres) & Memory Store
	store := kv.NewKVStore()
	if err := db.InitDB(); err != nil {
		log.Fatalf("[boot] Failed to initialize database: %v", err)
	}

	// 5. Initialize Lock-Free Store and Group Commit WAL
	walFile := nodeID + "_engine.wal"
	snapFile := nodeID + "_snapshot.bin"

	walEngine, err := wal.NewGroupCommitWAL(walFile, snapFile)
	if err != nil {
		log.Fatalf("[boot] Failed to initialize WAL: %v", err)
	}

	// 6. Initialize Raft Consensus
	applyCh := make(chan raft.ApplyMsg, 1000)
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
	startRaftHeartbeat()
	log.Println("[boot] Phase 2: Heartbeat watcher started (Ghost tasks will fail in 15s).")

	// PHASE 3: OPEN FOR BUSINESS
	go BrainProcessLoop(store, walEngine, raftNode)
	go StartExecutionEngine(store, applyCh, walEngine, raftNode)

	brainHost := envOrDefault("BRAIN_HOST", "0.0.0.0")
	httpPort := envOrDefault("HTTP_PORT", "8888")
	go StartAPIGateway(store, raftNode, brainHost, httpPort)

	tcpPort := envOrDefault("PORT", "9000")
	listener, err := net.Listen("tcp", net.JoinHostPort(brainHost, tcpPort))
	if err != nil {
		log.Fatalf("failed to start listener: %v", err)
	}

	// Listen for Muscle TCP connections.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // listener was closed (graceful shutdown)
			}
			go handleConnection(conn)
		}
	}()

	log.Printf("[boot] Phase 3: Brain is listening on %s:%s (Control Plane & TCP)", brainHost, tcpPort)

	// PHASE 4: GRACEFUL SHUTDOWN
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Block here until a termination signal is received
	<-c

	log.Println("\n[system] SIGTERM received. Shutting down gracefully...")

	// 1. Stop accepting new TCP connections
	listener.Close()

	// 2. Safely flush and close the Write-Ahead Log
	if err := walEngine.Close(); err != nil {
		log.Printf("[system] Error closing WAL: %v", err)
	} else {
		log.Println("[system] WAL successfully flushed and closed.")
	}

	// 3. Close Database connection pool
	if db.Pool != nil {
		db.Pool.Close()
		log.Println("[system] Database connections closed.")
	}

	log.Println("[system] Brain node shut down cleanly.")
}

// handleConnection safely routes TCP ingress and implements the Actor Egress Pattern
func handleConnection(conn net.Conn) {
	workerAddr := conn.RemoteAddr().String()
	log.Printf("new raw connection from %s", workerAddr)

	egressQueue := make(chan OutgoingMessage, 100)
	closeSignal := make(chan struct{})

	//  THE EGRESS GOROUTINE
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

		header := make([]byte, 9)
		if _, err := io.ReadFull(conn, header); err != nil {
			if err != io.EOF {
				log.Printf("read error from %s: %v", workerAddr, err)
			}
			return
		}

		// 🛡️ SECURITY: Verify BAM! Magic Word
		if string(header[0:4]) != protocol.MagicWord {
			log.Printf("[security] 🚫 Dropping non-BAM connection from %s", workerAddr)
			return
		}

		// Header cleared. Give ample time to ingest large payloads without timing out.
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))

		bodyLen := binary.BigEndian.Uint32(header[4:8])
		opCode := header[8]

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

				if msg.NodeID != "" && event.Send != nil {
					connRegistryMu.Lock()
					connRegistry[msg.NodeID] = event.Send
					connRegistryMu.Unlock()
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

				//  NEW: Capture Worker IP
				workerIP, _, _ := net.SplitHostPort(event.Conn.RemoteAddr().String())
				if workerIP == "" || workerIP == "::1" {
					workerIP = "127.0.0.1"
				}

				store.RegisterWorker(msg.NodeID, workerIP, req.TotalCPU, req.TotalRAM, req.AvailCPU, req.AvailRAM, req.OccupiedPorts, req.ActiveTaskIDs)
				store.ReconcileOccupiedState(msg.NodeID, req.OccupiedPorts)
				event.Send(protocol.OpWorkerRegistered, msg.RequestID, nil)

			case protocol.OpHeartbeat:
				var hb protocol.HeartbeatPayload
				json.Unmarshal(msg.Payload, &hb)

				//  NEW: Capture Worker IP
				workerIP, _, _ := net.SplitHostPort(event.Conn.RemoteAddr().String())
				if workerIP == "" || workerIP == "::1" {
					workerIP = "127.0.0.1"
				}

				store.UpdateWorkerHeartbeat(msg.NodeID, workerIP, hb.AvailCPU, hb.AvailRAM, hb.OccupiedPorts, hb.ActiveTaskIDs)
				store.ReconcileOccupiedState(msg.NodeID, hb.OccupiedPorts)
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

				//  NEW: Capture Worker IP
				workerIP, _, _ := net.SplitHostPort(event.Conn.RemoteAddr().String())
				if workerIP == "" || workerIP == "::1" {
					workerIP = "127.0.0.1"
				}

				store.UpdateWorkerHeartbeat(msg.NodeID, workerIP, req.AvailCPU, req.AvailRAM, req.OccupiedPorts, req.ActiveTaskIDs)
				store.ReconcileOccupiedState(msg.NodeID, req.OccupiedPorts)

				if req.QueueName == "" {
					req.QueueName = "default"
				}

				dbTask, dbErr := db.CheckoutNextTask(context.Background(), req.AvailCPU, req.AvailRAM, msg.NodeID)
				if dbErr != nil {
					// HYBRID DISPATCHER FALLBACK: If no Postgres tasks, try the Memory Queue (Raft)!
					memTask, exists := store.PopBestFitTask(msg.NodeID, req.AvailCPU, req.AvailRAM, req.OccupiedPorts)
					if !exists {
						event.Send(protocol.OpNoTasksAvailable, msg.RequestID, nil)
						continue
					}

					log.Printf("[Dispatcher] Task %s checked out from Memory (Hybrid Fallback)", memTask.ID)

					// Process Memory Task
					var p protocol.Payload
					json.Unmarshal([]byte(memTask.Payload), &p)

					entry := wal.LogEntry{Event: wal.EventTaskDispatched, TaskID: memTask.ID, WorkerID: msg.NodeID}
					// Inject UploadURL for Memory Path tasks too!
					uploadURL, _ := storage.GenerateWriteSAS(memTask.ID + ".json")
					logUploadURL, _ := storage.GenerateWriteSAS("logs/" + memTask.ID + ".txt")

					walEngine.Enqueue(wal.WriteRequest{
						Entry: entry, Conn: event.Conn, OpCode: protocol.OpTaskAssigned, RequestID: msg.RequestID,
						Payload: protocol.AssignedTask{
							ID:           memTask.ID,
							Payload:      memTask.Payload,
							UploadURL:    uploadURL,
							LogUploadURL: logUploadURL,
							RequiredCPU:  memTask.RequiredCPU,
							RequiredRAM:  memTask.RequiredRAM,
						},
					})

					b, _ := json.Marshal(entry)
					safePayload := append([]byte("SYNC:"), b...)
					raftNode.Propose(safePayload)
					continue
				}

				// --- POSTGRES BATCH TASK PATH (Priority 1) ---
				log.Printf("[Dispatcher] Task %s checked out from Postgres", dbTask.ID)

				// Update our in-memory state for UI/Raft purposes
				store.TransitionTaskState(dbTask.ID, kv.StatusPending, kv.StatusRunning, msg.NodeID)

				//  INJECT THE DATABASE UUID INTO THE RAW PAYLOAD
				// Because the frontend only submitted a generic SubmitTaskPayload to Postgres,
				// it lacked the Postgres-generated UUID. If we don't inject it here, the Muscle
				// worker pulls an empty JobID and routes all logs to `/logs ""` instead of `/logs <uuid>`.
				var parsedPayload map[string]interface{}
				json.Unmarshal(dbTask.Payload, &parsedPayload)
				parsedPayload["job_id"] = dbTask.ID
				updatedPayloadBytes, _ := json.Marshal(parsedPayload)

				entry := wal.LogEntry{Event: wal.EventTaskDispatched, TaskID: dbTask.ID, WorkerID: msg.NodeID}

				// Generate Azure SAS Token for the Muscle
				uploadURL, _ := storage.GenerateWriteSAS(dbTask.ID + ".json")
				logUploadURL, _ := storage.GenerateWriteSAS("logs/" + dbTask.ID + ".txt")

				reqCPU, _ := parsedPayload["required_cpu"].(float64)
				reqRAM, _ := parsedPayload["required_ram"].(float64)

				walEngine.Enqueue(wal.WriteRequest{
					Entry:     entry,
					Conn:      event.Conn,
					OpCode:    protocol.OpTaskAssigned,
					RequestID: msg.RequestID,
					Payload: protocol.AssignedTask{
						ID:           dbTask.ID,
						Payload:      string(updatedPayloadBytes),
						UploadURL:    uploadURL,
						LogUploadURL: logUploadURL,
						RequiredCPU:  reqCPU,
						RequiredRAM:  int(reqRAM),
					},
				})

				b, _ := json.Marshal(entry)
				safePayload := append([]byte("SYNC:"), b...)
				raftNode.Propose(safePayload)

			case protocol.OpServiceReady:
				// 1. We received the IP address callback from the Muscle!
				// We must durably commit this to the Raft log so the Execution Engine can transition to Phase 2.

				// 2. Wrap the raw JSON payload in our specific SYNC prefix
				safePayload := append([]byte("SYNC:SERVICE_READY:"), msg.Payload...)
				raftNode.Propose(safePayload)

				// 3. Send a quick ACK to the Muscle so its connection manager doesn't timeout
				event.Send(protocol.OpServiceReady, msg.RequestID, nil)

			case protocol.OpSubmitTask:
				var envelope protocol.SubmitResultPayload
				json.Unmarshal(msg.Payload, &envelope)

				//  NEW: Mark as completed in Supabase and save the Azure Blob URL!
				// Assuming the Muscle sends the BlobURL inside the ResultPayload
				// You may need to parse it, or pass it explicitly in the envelope.
				dbErr := db.CompleteTask(context.Background(), envelope.TaskID, envelope.ResultPayload)
				if dbErr != nil {
					log.Printf("[db] ⚠️ Failed to update Supabase for task %s: %v", envelope.TaskID, dbErr)
				}

				// ... (Keep the rest of your existing DAG/Job handling logic below) ...
				submittedTask, _ := store.GetTask(envelope.TaskID)

				maxCPU, maxRAM := store.GetMaxWorkerCapacity()
				// Only reject if at least one worker is online to give us a baseline
				if maxCPU > 0 && (submittedTask.RequiredCPU > maxCPU || float64(submittedTask.RequiredRAM) > float64(maxRAM)) {
					event.Send(protocol.OpError, msg.RequestID, fmt.Sprintf("TASK_TOO_LARGE: Cluster max is %.1f CPU / %d MB RAM", maxCPU, maxRAM))
					continue
				}

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

				var resultData map[string]interface{}
				json.Unmarshal([]byte(envelope.ResultPayload), &resultData)

				blobURL := ""
				if urlVal, ok := resultData["blob_url"].(string); ok {
					blobURL = urlVal
				}

				//  SANITY CHECK: Only sync to Postgres if we have a real Blob URL.
				// If it's just an inline result string, we keep Postgres result_blob_url empty
				// to avoid corrupting the download button in the frontend.
				dbBlobURL := ""
				if strings.HasPrefix(blobURL, "http") {
					dbBlobURL = blobURL
				}

				dbErr := db.CompleteTask(context.Background(), envelope.TaskID, dbBlobURL)
				if dbErr != nil {
					log.Printf("[db] Warning: Failed to update Supabase for task %s: %v", envelope.TaskID, dbErr)
				} else {
					log.Printf("[db] Task %s fully completed! Blob: %s", envelope.TaskID, blobURL)
				}

				// Sync memory state with Postgres.
				store.CompleteTask(envelope.TaskID, dbBlobURL)
				// Automated Resource Cleanup Sweep
				submittedTask, taskFound := store.GetTask(envelope.TaskID)
				if taskFound && submittedTask.JobID != "" {
					if job, ok := store.GetJob(submittedTask.JobID); ok && job.Status == kv.JobCompleted {
						liveSiblings := store.GetJobLiveTasks(submittedTask.JobID)
						if len(liveSiblings) > 0 {
							log.Printf("[cleanup] 🧹 Job %s completed. Sweeping %d sibling tasks...", submittedTask.JobID, len(liveSiblings))
							for _, sID := range liveSiblings {
								if sID == envelope.TaskID {
									continue // Skip self
								}
								// Send kill signal
								if sTask, ok := store.GetTask(sID); ok {
									connRegistryMu.RLock()
									workerSend, isOnline := connRegistry[sTask.AssignedWorkerID]
									connRegistryMu.RUnlock()
									if isOnline {
										log.Printf("[cleanup] 💀 Killing sibling %s on worker %s", sID, sTask.AssignedWorkerID)
										workerSend(protocol.OpKillTask, "cleanup", protocol.KillTaskPayload{TaskID: sID})
									}
								}
							}
						}
					}
				}

				isDAG := taskFound && submittedTask.JobID != ""

				if isDAG {
					var dagResult protocol.TaskCompletePayload
					if err := json.Unmarshal([]byte(envelope.ResultPayload), &dagResult); err != nil {
						log.Printf("[actor] result payload unmarshal error for task %s: %v", envelope.TaskID, err)
					}

					//  RECOVERY: If JobType is missing (Azure mode), recover from store!
					effectiveJobType := dagResult.JobType
					if effectiveJobType == "" {
						effectiveJobType = submittedTask.JobType
					}

					//  RECOVERY: If result is a JSON blob_url pointer (Claim Check), extract it!
					effectiveResultURI := dagResult.ResultURI
					if effectiveResultURI == "" {
						var blobPtr struct {
							BlobURL string `json:"blob_url"`
						}
						if err := json.Unmarshal([]byte(envelope.ResultPayload), &blobPtr); err == nil && blobPtr.BlobURL != "" {
							effectiveResultURI = blobPtr.BlobURL
						} else {
							effectiveResultURI = envelope.ResultPayload // fallback
						}
					}

					//  RECOVERY: Recover ChunkIndex from original task payload if missing
					effectiveChunkIndex := dagResult.ChunkIndex
					if dagResult.JobType == "" { // Likely Azure mode
						var originalPayload protocol.Payload
						if err := json.Unmarshal([]byte(submittedTask.Payload), &originalPayload); err == nil {
							effectiveChunkIndex = originalPayload.ChunkIndex
						}
					}

					switch effectiveJobType {
					case "GATHER":
						entry := wal.LogEntry{Event: wal.EventTaskCompleted, TaskID: envelope.TaskID, Payload: msg.Payload}
						walEngine.Enqueue(wal.WriteRequest{
							Entry: entry, Conn: event.Conn, OpCode: protocol.OpTaskCompleted, RequestID: msg.RequestID,
						})
						b, _ := json.Marshal(entry)
						safePayload := append([]byte("SYNC:"), b...)
						raftNode.Propose(safePayload)
					case "MAP":
						mapPayload, _ := json.Marshal(protocol.MapCompletedPayload{
							ChunkIndex: effectiveChunkIndex,
							ResultURI:  effectiveResultURI,
						})
						entryMap := wal.LogEntry{Event: wal.EventMAPCompleted, JobID: submittedTask.JobID, Payload: mapPayload}
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
					default:
						entry := wal.LogEntry{Event: wal.EventTaskCompleted, TaskID: envelope.TaskID, Payload: msg.Payload}
						walEngine.Enqueue(wal.WriteRequest{
							Entry: entry, Conn: event.Conn, OpCode: protocol.OpTaskCompleted, RequestID: msg.RequestID,
						})
						b, _ := json.Marshal(entry)
						safePayload := append([]byte("SYNC:"), b...)
						raftNode.Propose(safePayload)
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

			case protocol.OpKillTask:
				var req protocol.KillTaskPayload
				json.Unmarshal(msg.Payload, &req)

				// 1. Verify the task exists and is running
				task, ok := store.GetTask(req.TaskID)
				if !ok {
					event.Send(protocol.OpError, msg.RequestID, "Task not found")
					continue
				}
				if task.Status != kv.StatusRunning {
					event.Send(protocol.OpError, msg.RequestID, "Task is not running. Status: "+strconv.Itoa(int(task.Status)))
					continue
				}

				// 2. Look up the Worker's TCP socket in our routing registry
				connRegistryMu.RLock()
				workerSend, isOnline := connRegistry[task.AssignedWorkerID]
				connRegistryMu.RUnlock()

				if !isOnline {
					event.Send(protocol.OpError, msg.RequestID, "Worker "+task.AssignedWorkerID+" is currently offline")
					continue
				}

				// 3. 🎯 ASSASSINATION ORDER: Forward the kill signal to the target Muscle!
				workerSend(protocol.OpKillTask, msg.RequestID, req)

				// 4. Tell the Client we successfully fired the missile
				event.Send(protocol.OpTaskAccepted, msg.RequestID, "Kill signal forwarded to "+task.AssignedWorkerID)

			case protocol.OpKillTaskAck:
				// 1. The Muscle has successfully destroyed the daemon.
				var req protocol.KillTaskPayload
				json.Unmarshal(msg.Payload, &req)

				// 2. Make it crash-fault tolerant!
				entry := wal.LogEntry{Event: wal.EventTaskKilled, TaskID: req.TaskID}
				walEngine.Enqueue(wal.WriteRequest{
					Entry:     entry,
					Conn:      event.Conn,
					OpCode:    protocol.OpTaskCompleted, // Send a standard ACK back to the worker
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

				//  Thread-Safe Write
				event.Send(protocol.OpRequestVoteAck, msg.RequestID, reply)

			case protocol.OpAppendEntries:
				var args raft.AppendEntriesArgs
				json.Unmarshal(msg.Payload, &args)
				reply := raftNode.ProcessAppendEntries(args)

				//  Thread-Safe Write
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

func startRaftHeartbeat() {
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
