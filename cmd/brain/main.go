package main

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/kv"
	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/akashtripathi12/brain-and-muscle/internal/wal"
	"github.com/google/uuid"
)

const (
	workerTimeout   = 15 * time.Second
	watcherInterval = 5 * time.Second
	maxMessageBytes = 10 * 1024 * 1024
)

type BrainEvent struct {
	WorkerID string
	OpCode   uint8
	Body     []byte
	Conn     net.Conn
}

var eventInbox = make(chan BrainEvent, 10000)

func main() {
	// 1. Initialize Lock-Free Store and Group Commit WAL
	store := kv.NewKVStore()
	walEngine, err := wal.NewGroupCommitWAL("engine.wal")
	if err != nil {
		log.Fatalf("Failed to initialize WAL: %v", err)
	}

	// PHASE 1a: SNAPSHOT RECOVERY
	snapData, err := os.ReadFile("snapshot.bin")
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
	log.Println("[boot] Phase 2: Heartbeat watcher started (Ghost tasks will fail in 15s).")

	// PHASE 3: OPEN FOR BUSINESS
	go BrainProcessLoop(store, walEngine)

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Close()

	log.Println("[boot] Phase 3: Brain is listening on :8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("failed to accept connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

// handleConnection reads framed messages from a single worker connection and
// forwards them to the BrainProcessLoop actor via eventInbox.
func handleConnection(conn net.Conn) {
	defer conn.Close()
	workerAddr := conn.RemoteAddr().String()
	log.Printf("new raw connection from %s", workerAddr)

	for {
		// reset deadline before each blocking read so a dead client
		// cannot pin this goroutine forever.
		conn.SetReadDeadline(time.Now().Add(workerTimeout))

		header := make([]byte, 5)
		if _, err := io.ReadFull(conn, header); err != nil {
			if err != io.EOF {
				log.Printf("read error from %s: %v", workerAddr, err)
			}
			return
		}

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

		eventInbox <- BrainEvent{
			WorkerID: workerAddr,
			OpCode:   opCode,
			Body:     body,
			Conn:     conn,
		}
	}
}

func BrainProcessLoop(store *kv.KVStore, walEngine *wal.GroupCommitWAL) {
	log.Println("[actor] BrainProcessLoop started. Operating in lock-free mode.")

	var diskEvents int
	const snapshotThreshold = 30

	for event := range eventInbox {
		var msg protocol.Message
		if event.OpCode != protocol.OpTick {
			if err := json.Unmarshal(event.Body, &msg); err != nil {
				log.Printf("[actor] bad JSON from %s: %v", event.WorkerID, err)
				continue
			}
		}

		walWritten := false // true only when a WAL entry was actually enqueued

		switch event.OpCode {

		// EPHEMERAL STATE (No WAL, Instant TCP ACK)

		case protocol.OpRegisterWorker:
			var req protocol.RegisterWorkerPayload
			json.Unmarshal(msg.Payload, &req)
			store.RegisterWorker(msg.NodeID, req.TotalCPU, req.TotalRAM)
			protocol.SendResponse(event.Conn, protocol.OpWorkerRegistered, msg.RequestID, nil)

		case protocol.OpHeartbeat:
			var hb protocol.HeartbeatPayload
			json.Unmarshal(msg.Payload, &hb)
			store.UpdateWorkerHeartbeat(msg.NodeID, hb.AvailCPU, hb.AvailRAM)
			protocol.SendResponse(event.Conn, protocol.OpHeartbeatAck, msg.RequestID, nil)

		case protocol.OpGetTaskStatus:
			var req protocol.GetTaskStatusPayload
			json.Unmarshal(msg.Payload, &req)

			if task, ok := store.GetTask(req.TaskID); ok {
				protocol.SendResponse(event.Conn, protocol.OpTaskStatus, msg.RequestID, protocol.TaskStatusPayload{
					TaskID: task.ID,
					Status: int(task.Status),
				})
			} else if job, ok := store.GetJob(req.TaskID); ok {
				protocol.SendResponse(event.Conn, protocol.OpTaskStatus, msg.RequestID, protocol.TaskStatusPayload{
					TaskID: req.TaskID,
					Status: int(job.Status),
				})
			} else {
				protocol.SendResponse(event.Conn, protocol.OpError, msg.RequestID, nil)
			}

		// DURABLE STATE (Mutate Memory → Push to WAL → WAL handles TCP ACK)

		case protocol.OpTick: // Internal event from startHeartbeatWatcher
			deadWorkers := store.GetDeadWorkers(workerTimeout)
			for _, workerID := range deadWorkers {
				log.Printf("[watcher] worker %s died — requeueing tasks", workerID)

				deadTaskIDs := store.RequeueDeadWorkerTasks(workerID)
				for _, taskID := range deadTaskIDs {
					walEngine.Enqueue(wal.WriteRequest{
						Entry: wal.LogEntry{Event: wal.EventTaskFailed, TaskID: taskID, WorkerID: workerID},
					})
					walWritten = true // WAL entry actually enqueued → count it
				}

				store.RemoveWorker(workerID)
			}

			tFreed, jFreed := store.SweepTerminalState(5 * time.Minute)
			if tFreed > 0 || jFreed > 0 {
				log.Printf("[sweeper] freed %d tasks, %d jobs from memory", tFreed, jFreed)
				// walWritten intentionally NOT set — no WAL entry was written.
			}

		case protocol.OpRequestTask:
			var req protocol.RequestTaskPayload
			json.Unmarshal(msg.Payload, &req)
			if req.QueueName == "" {
				req.QueueName = "default"
			}

			task, ok := store.PopBestFitTask(req.QueueName, req.AvailCPU, req.AvailRAM)
			if !ok {
				protocol.SendResponse(event.Conn, protocol.OpNoTasksAvailable, msg.RequestID, nil)
				continue
			}

			store.TransitionTaskState(task.ID, kv.StatusPending, kv.StatusRunning, msg.NodeID)

			walEngine.Enqueue(wal.WriteRequest{
				Entry:     wal.LogEntry{Event: wal.EventTaskDispatched, TaskID: task.ID, WorkerID: msg.NodeID},
				Conn:      event.Conn,
				OpCode:    protocol.OpTaskAssigned,
				RequestID: msg.RequestID,
				Payload: protocol.AssignedTask{
					ID: task.ID, Payload: task.Payload, QueueName: task.QueueName,
					RequiredCPU: task.RequiredCPU, RequiredRAM: task.RequiredRAM,
				},
			})
			walWritten = true

		case protocol.OpSubmitTask:
			var req protocol.SubmitTaskPayload
			if err := json.Unmarshal(msg.Payload, &req); err != nil {
				protocol.SendResponse(event.Conn, protocol.OpError, msg.RequestID, nil)
				continue
			}
			if req.QueueName == "" {
				req.QueueName = "default"
			}

			if req.JobType == "SCATTER" && req.TotalMAPs > 1 {
				handleScatterJob(store, walEngine, req, event.Conn, msg.RequestID)
			} else {
				task := &kv.Task{
					ID:          uuid.New().String(),
					QueueName:   req.QueueName,
					Payload:     req.Payload,
					Status:      kv.StatusPending,
					RequiredCPU: req.RequiredCPU,
					RequiredRAM: req.RequiredRAM,
					CreatedAt:   time.Now(),
				}
				store.SetTask(task)

				walEngine.Enqueue(wal.WriteRequest{
					Entry:     wal.LogEntry{Event: wal.EventTaskQueued, TaskID: task.ID, Payload: msg.Payload},
					Conn:      event.Conn,
					OpCode:    protocol.OpTaskAccepted,
					RequestID: msg.RequestID,
					Payload:   protocol.TaskAcceptedPayload{TaskID: task.ID},
				})
			}
			walWritten = true

		case protocol.OpSubmitResult:
			var envelope protocol.SubmitResultPayload
			json.Unmarshal(msg.Payload, &envelope)

			submittedTask, taskFound := store.GetTask(envelope.TaskID)
			isDAG := taskFound && submittedTask.JobID != ""

			if isDAG {
				var dagResult protocol.TaskCompletePayload
				json.Unmarshal([]byte(envelope.ResultPayload), &dagResult)

				if dagResult.JobType == "GATHER" {
					store.CompleteTask(envelope.TaskID, envelope.ResultPayload)
					walEngine.Enqueue(wal.WriteRequest{
						Entry: wal.LogEntry{Event: wal.EventTaskCompleted, TaskID: envelope.TaskID, Payload: msg.Payload},
						Conn:  event.Conn, OpCode: protocol.OpTaskCompleted, RequestID: msg.RequestID,
					})
				} else { // MAP chunk
					store.RecordMAPComplete(dagResult.JobID, dagResult.ChunkIndex, dagResult.ResultURI)
					store.CompleteTask(envelope.TaskID, dagResult.ResultURI)

					mapPayload, _ := json.Marshal(protocol.MapCompletedPayload{
						ChunkIndex: dagResult.ChunkIndex,
						ResultURI:  dagResult.ResultURI,
					})
					walEngine.Enqueue(wal.WriteRequest{
						Entry: wal.LogEntry{Event: wal.EventMAPCompleted, JobID: dagResult.JobID, Payload: mapPayload},
						Conn:  event.Conn, OpCode: protocol.OpTaskCompleted, RequestID: msg.RequestID,
					})
				}
			} else {
				store.CompleteTask(envelope.TaskID, envelope.ResultPayload)
				walEngine.Enqueue(wal.WriteRequest{
					Entry: wal.LogEntry{Event: wal.EventTaskCompleted, TaskID: envelope.TaskID, Payload: msg.Payload},
					Conn:  event.Conn, OpCode: protocol.OpTaskCompleted, RequestID: msg.RequestID,
				})
			}
			walWritten = true

		case protocol.OpTaskFailed:
			var req protocol.TaskFailedPayload
			json.Unmarshal(msg.Payload, &req)

			store.HandleMAPFailure(req.TaskID, msg.NodeID)

			walEngine.Enqueue(wal.WriteRequest{
				Entry:     wal.LogEntry{Event: wal.EventTaskFailed, TaskID: req.TaskID, WorkerID: msg.NodeID},
				Conn:      event.Conn,
				OpCode:    protocol.OpTaskFailedAck,
				RequestID: msg.RequestID,
			})
			walWritten = true

		default:
			log.Printf("[actor] dropped unknown opcode 0x%X from %s", event.OpCode, event.WorkerID)
		}

		// only advance diskEvents when a WAL entry was actually enqueued.
		if walWritten {
			diskEvents++
			if diskEvents >= snapshotThreshold {
				snapData := store.ExportSnapshot()
				walEngine.Enqueue(wal.WriteRequest{
					IsSnapshot:   true,
					SnapshotData: snapData,
				})
				diskEvents = 0
				log.Printf("[actor] WAL threshold reached (%d events) — snapshot compaction triggered", snapshotThreshold)
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

// handleScatterJob generates all UUIDs, mutates memory, and writes the entire
// DAG structure to the WAL as a single atomic entry.
func handleScatterJob(store *kv.KVStore, walEngine *wal.GroupCommitWAL, req protocol.SubmitTaskPayload, conn net.Conn, requestID string) {
	jobID := uuid.New().String()

	var userPayload map[string]interface{}
	json.Unmarshal([]byte(req.Payload), &userPayload)

	// Build the GATHER task payload.
	gatherPayloadMap := make(map[string]interface{})
	for k, v := range userPayload {
		gatherPayloadMap[k] = v
	}
	gatherPayloadMap["job_type"] = "GATHER"
	gatherPayloadMap["job_id"] = jobID
	gatherPayloadBytes, _ := json.Marshal(gatherPayloadMap)

	gatherTask := &kv.Task{
		ID:          uuid.New().String(),
		JobID:       jobID,
		QueueName:   req.QueueName,
		Status:      kv.StatusPending,
		RequiredCPU: req.RequiredCPU,
		RequiredRAM: req.RequiredRAM,
		Payload:     string(gatherPayloadBytes),
		CreatedAt:   time.Now(),
	}

	// Register the job and gatherTask in memory before writing anything to WAL.
	store.RegisterJob(jobID, req.TotalMAPs, gatherTask)

	// Build all MAP tasks in memory and collect their serialised forms.
	mapTasksRaw := make([]json.RawMessage, 0, req.TotalMAPs)

	for i := 0; i < req.TotalMAPs; i++ {
		mapPayload := protocol.Payload{
			JobType:       "MAP",
			JobID:         jobID,
			ChunkIndex:    i,
			AttemptNumber: 1,
		}
		if cmd, ok := userPayload["command"].(string); ok {
			mapPayload.Command = cmd
		}
		if args, ok := userPayload["args"].([]interface{}); ok {
			for _, a := range args {
				if s, ok := a.(string); ok {
					mapPayload.Args = append(mapPayload.Args, s)
				}
			}
		}

		rawPayload, _ := json.Marshal(mapPayload)
		mapTask := &kv.Task{
			ID:            uuid.New().String(),
			JobID:         jobID,
			QueueName:     req.QueueName,
			Payload:       string(rawPayload),
			Status:        kv.StatusPending,
			RequiredCPU:   req.RequiredCPU,
			RequiredRAM:   req.RequiredRAM,
			MaxRetries:    3,
			AttemptNumber: 1,
			CreatedAt:     time.Now(),
		}
		store.SetTask(mapTask)

		taskBytes, _ := json.Marshal(mapTask)
		mapTasksRaw = append(mapTasksRaw, taskBytes)
	}

	// Serialise the gather task for the WAL payload.
	gatherBytes, _ := json.Marshal(gatherTask)

	jobPayload, _ := json.Marshal(protocol.JobRegisteredPayload{
		TotalMAPs:  req.TotalMAPs,
		GatherTask: gatherBytes,
		MapTasks:   mapTasksRaw,
	})

	walEngine.Enqueue(wal.WriteRequest{
		Entry:     wal.LogEntry{Event: wal.EventJobRegistered, JobID: jobID, Payload: jobPayload},
		Conn:      conn,
		OpCode:    protocol.OpTaskAccepted,
		RequestID: requestID,
		Payload:   protocol.TaskAcceptedPayload{TaskID: jobID},
	})
}
