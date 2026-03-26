package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/kv"
	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/google/uuid"
)

const (
	workerTimeout   = 15 * time.Second
	watcherInterval = 5 * time.Second
	maxMessageBytes = 10 * 1024 * 1024 // 10 MB — must match client.go
)

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Close()

	log.Println("Brain is listening on :8080")

	store := kv.NewKVStore()
	startHeartbeatWatcher(store)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("failed to accept connection: %v", err)
			continue
		}
		go handleConnection(conn, store)
	}
}

func handleConnection(conn net.Conn, store *kv.KVStore) {
	defer conn.Close()
	log.Printf("new connection from %s", conn.RemoteAddr())

	for {
		header := make([]byte, 4)
		if _, err := io.ReadFull(conn, header); err != nil {
			if err == io.EOF {
				log.Printf("connection closed by %s", conn.RemoteAddr())
			} else {
				log.Printf("error reading header from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}

		bodyLen := binary.BigEndian.Uint32(header)
		if bodyLen > maxMessageBytes {
			log.Printf("payload too large: %d bytes from %s", bodyLen, conn.RemoteAddr())
			return
		}

		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(conn, body); err != nil {
			log.Printf("error reading body: %v", err)
			return
		}

		var msg protocol.Message
		if err := json.Unmarshal(body, &msg); err != nil {
			log.Printf("failed to unmarshal message: %v", err)
			continue
		}

		switch msg.OpCode {

		// Registration
		case protocol.OpRegisterWorker:
			var req protocol.RegisterWorkerPayload
			if err := json.Unmarshal(msg.Payload, &req); err != nil {
				log.Printf("[register] bad payload from %s: %v", msg.NodeID, err)
				sendResponse(conn, protocol.OpError, msg.RequestID, nil)
				continue
			}
			store.RegisterWorker(msg.NodeID, req.TotalCPU, req.TotalRAM)
			log.Printf("[register] worker %s — %d CPU / %d MB RAM",
				msg.NodeID, req.TotalCPU, req.TotalRAM)
			sendResponse(conn, protocol.OpWorkerRegistered, msg.RequestID, nil)

		// Heartbeat
		case protocol.OpHeartbeat:
			var hb protocol.HeartbeatPayload
			if err := json.Unmarshal(msg.Payload, &hb); err != nil {
				log.Printf("[heartbeat] no resource payload from %s: %v", msg.NodeID, err)
			}
			store.UpdateWorkerHeartbeat(msg.NodeID, hb.AvailCPU, hb.AvailRAM)
			sendResponse(conn, protocol.OpHeartbeatAck, msg.RequestID, nil)

		// Task request
		case protocol.OpRequestTask:
			var req protocol.RequestTaskPayload
			if err := json.Unmarshal(msg.Payload, &req); err != nil || req.QueueName == "" {
				req.QueueName = "default"
			}

			task, ok := store.PopBestFitTask(req.QueueName, req.AvailCPU, req.AvailRAM)
			if !ok {
				sendResponse(conn, protocol.OpNoTasksAvailable, msg.RequestID, nil)
				continue
			}

			log.Printf("[scheduler] assigning task %s (%d CPU / %d RAM) to worker %s (avail: %d CPU / %d RAM)",
				task.ID, task.RequiredCPU, task.RequiredRAM,
				msg.NodeID, req.AvailCPU, req.AvailRAM)

			if err := store.TransitionTaskState(task.ID, kv.StatusPending, kv.StatusRunning, msg.NodeID); err != nil {
				log.Printf("[scheduler] failed to mark task %s running: %v", task.ID, err)
				sendResponse(conn, protocol.OpError, msg.RequestID, nil)
				continue
			}

			payload := protocol.AssignedTask{
				ID:          task.ID,
				Payload:     task.Payload,
				QueueName:   task.QueueName,
				RequiredCPU: task.RequiredCPU,
				RequiredRAM: task.RequiredRAM,
			}

			if err := sendResponse(conn, protocol.OpTaskAssigned, msg.RequestID, payload); err != nil {
				log.Printf("[scheduler] failed to send task %s to worker: %v", task.ID, err)
				if revertErr := store.TransitionTaskState(task.ID, kv.StatusRunning, kv.StatusPending, ""); revertErr != nil {
					log.Printf("[scheduler] failed to revert task %s to pending: %v", task.ID, revertErr)
				}
			}

		// Task submission
		case protocol.OpSubmitTask:
			var req protocol.SubmitTaskPayload
			if err := json.Unmarshal(msg.Payload, &req); err != nil || req.QueueName == "" {
				req.QueueName = "default"
			}

			if req.JobType == "SCATTER" && req.TotalMAPs > 1 {
				jobID := handleScatterJob(store, req)
				log.Printf("[submit] SCATTER job %s accepted (%d MAPs)", jobID, req.TotalMAPs)
				sendResponse(conn, protocol.OpTaskAccepted, msg.RequestID,
					protocol.TaskAcceptedPayload{TaskID: jobID})
			} else {
				task := &kv.Task{
					ID:          uuid.New().String(),
					QueueName:   req.QueueName,
					Payload:     req.Payload,
					Status:      kv.StatusPending,
					RequiredCPU: req.RequiredCPU,
					RequiredRAM: req.RequiredRAM,
					MaxRetries:  0,
					CreatedAt:   time.Now(),
				}
				store.SetTask(task)
				log.Printf("[submit] standard task %s queued (%d CPU / %d RAM)",
					task.ID, task.RequiredCPU, task.RequiredRAM)
				sendResponse(conn, protocol.OpTaskAccepted, msg.RequestID,
					protocol.TaskAcceptedPayload{TaskID: task.ID})
			}

		// Result submission
		case protocol.OpSubmitResult:
			var envelope protocol.SubmitResultPayload
			if err := json.Unmarshal(msg.Payload, &envelope); err != nil || envelope.TaskID == "" {
				log.Printf("[result] invalid envelope: %v", err)
				sendResponse(conn, protocol.OpError, msg.RequestID, nil)
				continue
			}

			var dagResult protocol.TaskCompletePayload
			isDAG := json.Unmarshal([]byte(envelope.ResultPayload), &dagResult) == nil && dagResult.JobID != ""

			if isDAG {
				if dagResult.JobType == "GATHER" {
					// The barrier task has landed — the DAG is complete.
					if err := store.CompleteTask(envelope.TaskID, envelope.ResultPayload); err != nil {
						log.Printf("[result] failed to complete GATHER task %s: %v", envelope.TaskID, err)
					} else {
						log.Printf("[result] 🚀 DAG WORKFLOW COMPLETE! Job %s final output → %s", dagResult.JobID, dagResult.ResultURI)
					}
				} else {
					// MAP chunk completed — record the URI in the job tracker ...
					if err := store.RecordMAPComplete(dagResult.JobID, dagResult.ChunkIndex, dagResult.ResultURI); err != nil {
						log.Printf("[result] failed to record MAP chunk %d for job %s: %v", dagResult.ChunkIndex, dagResult.JobID, err)
					} else {
						// ... and seal the task's own status so the heartbeat watcher doesn't re-queue it as a ghost after the worker disconnects.
						if err := store.CompleteTask(envelope.TaskID, dagResult.ResultURI); err != nil {
							log.Printf("[result] failed to seal MAP task %s status: %v", envelope.TaskID, err)
						}
						log.Printf("[result] job %s chunk %d completed by %s → %s", dagResult.JobID, dagResult.ChunkIndex, msg.NodeID, dagResult.ResultURI)
					}
				}
			} else {
				// VERSION A: STANDARD TASK COMPLETION
				if err := store.CompleteTask(envelope.TaskID, envelope.ResultPayload); err != nil {
					log.Printf("[result] failed to complete standard task %s: %v", envelope.TaskID, err)
				} else {
					log.Printf("[result] task %s completed by %s", envelope.TaskID, msg.NodeID)
				}
			}

			sendResponse(conn, protocol.OpTaskCompleted, msg.RequestID, nil)

		// Task status
		case protocol.OpGetTaskStatus:
			var req protocol.GetTaskStatusPayload
			if err := json.Unmarshal(msg.Payload, &req); err != nil || req.TaskID == "" {
				sendResponse(conn, protocol.OpError, msg.RequestID, nil)
				continue
			}
			task, ok := store.GetTask(req.TaskID)
			if !ok {
				sendResponse(conn, protocol.OpError, msg.RequestID, nil)
				continue
			}
			sendResponse(conn, protocol.OpTaskStatus, msg.RequestID, protocol.TaskStatusPayload{
				TaskID: task.ID,
				Status: int(task.Status),
			})

		// Task failed
		case protocol.OpTaskFailed:
			var req protocol.TaskFailedPayload
			if err := json.Unmarshal(msg.Payload, &req); err != nil || req.TaskID == "" {
				log.Printf("[failed] invalid payload: %v", err)
				sendResponse(conn, protocol.OpError, msg.RequestID, nil)
				continue
			}

			store.HandleMAPFailure(req.TaskID, msg.NodeID)
			log.Printf("[failed] task %s reported failed by worker %s", req.TaskID, msg.NodeID)
			sendResponse(conn, protocol.OpTaskFailedAck, msg.RequestID, nil)

		default:
			log.Printf("[brain] unknown opcode %q from %s", msg.OpCode, msg.NodeID)
		}
	}
}

func sendResponse(conn net.Conn, opCode string, requestID string, payload interface{}) error {
	msg := protocol.Message{
		OpCode:    opCode,
		RequestID: requestID,
		Timestamp: time.Now(),
	}

	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal payload: %w", err)
		}
		msg.Payload = raw
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal response: %w", err)
	}

	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(body)))
	_, err = conn.Write(append(header, body...))
	return err
}

func startHeartbeatWatcher(store *kv.KVStore) {
	go func() {
		ticker := time.NewTicker(watcherInterval)
		defer ticker.Stop()

		for range ticker.C {
			deadWorkers := store.GetDeadWorkers(workerTimeout)
			for _, workerID := range deadWorkers {
				log.Printf("[watcher] worker %s missed heartbeat deadline — requeueing tasks", workerID)
				store.RequeueDeadWorkerTasks(workerID)
				store.RemoveWorker(workerID)
			}
		}
	}()
}

// handleScatterJob fans a SCATTER submission into N MAP tasks and pre-stages the GATHER task in the job registry.
func handleScatterJob(store *kv.KVStore, req protocol.SubmitTaskPayload) string {
	jobID := uuid.New().String()

	var userPayload map[string]interface{}
	if err := json.Unmarshal([]byte(req.Payload), &userPayload); err != nil {
		userPayload = map[string]interface{}{"command": req.Payload}
	}

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

	store.RegisterJob(jobID, req.TotalMAPs, gatherTask)

	for i := 0; i < req.TotalMAPs; i++ {
		taskID := uuid.New().String()

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
					mapPayload.InputURIs = append(mapPayload.InputURIs, s)
				}
			}
		}

		rawPayload, _ := json.Marshal(mapPayload)

		mapTask := &kv.Task{
			ID:            taskID,
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
	}

	return jobID
}
