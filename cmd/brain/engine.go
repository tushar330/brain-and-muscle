package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/kv"
	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/akashtripathi12/brain-and-muscle/internal/raft"
	"github.com/akashtripathi12/brain-and-muscle/internal/wal"
)

// StartExecutionEngine listens to the Raft commit channel and mutates the DAG state.
// Because it reads from Raft, it is guaranteed to execute deterministically across all nodes.
func StartExecutionEngine(store *kv.KVStore, applyCh chan raft.ApplyMsg, walEngine *wal.GroupCommitWAL, raftNode *raft.BrainState) {
	log.Println("[engine] DAG Execution Engine started. Listening to Raft...")

	const snapshotThreshold = 30
	var commitCount int

	// THE IDEMPOTENCY SHIELD
	seenRequests := make(map[uint64]bool)
	var seenHistory []uint64 // Acts as a FIFO queue

	maxRestoredRaftIndex := store.GetMaxRaftIndex()

	for msg := range applyCh {
		if !msg.CommandValid {
			continue
		}

		if msg.CommandIndex <= maxRestoredRaftIndex {
			continue
		}

		// --- THE DISTRIBUTED SNAPSHOT TRIGGER ---
		// We count every valid Raft command towards the snapshot threshold.
		// By doing this here, we ensure both TASK and SYNC commands are counted.
		commitCount++
		if commitCount >= snapshotThreshold {
			log.Printf("[engine] Distributed Snapshot Threshold reached (%d commits) — compacting all logs at Index %d", commitCount, msg.CommandIndex)

			// 1. Export the current consistency state of the KV Store
			snapData := store.ExportSnapshot()

			// 2. Truncate the Task Engine WAL and save the snapshot
			walEngine.Enqueue(wal.WriteRequest{
				IsSnapshot:   true,
				SnapshotData: snapData,
			})

			// 3. Truncate the Raft Consensus State (Consensus-safe truncation)
			// (Uses the current Raft index as the new Log Start boundary)
			raftNode.CompactRaftState(msg.CommandIndex)

			commitCount = 0
		}

		// THE O(1) IDEMPOTENCY SHIELD WITH FIFO CLEANUP
		if seenRequests[msg.CommandIndex] {
			log.Printf("[engine] Idempotency cache hit! Skipping index %d.", msg.CommandIndex)
			continue
		}

		seenRequests[msg.CommandIndex] = true
		seenHistory = append(seenHistory, msg.CommandIndex)

		// Safely pop the oldest index regardless of sequence gaps
		if len(seenHistory) > 5000 {
			// Evict the oldest 2500 entries in one shot instead of one-at-a-time
			for _, old := range seenHistory[:2500] {
				delete(seenRequests, old)
			}
			// copy remaining to a fresh backing array — old array becomes GC-eligible
			trimmed := make([]uint64, 2500)
			copy(trimmed, seenHistory[2500:])
			seenHistory = trimmed
		}

		// STRICT BYTE-PREFIX ROUTING (Prevents JSON Prototype Pollution)
		var isTask bool
		var isSync bool
		var cleanPayload []byte

		if len(msg.Command) > 5 && string(msg.Command[:5]) == "TASK:" {
			isTask = true
			cleanPayload = msg.Command[5:]
		} else if len(msg.Command) > 5 && string(msg.Command[:5]) == "SYNC:" {
			isSync = true
			cleanPayload = msg.Command[5:]
		} else if len(msg.Command) >= 4 && string(msg.Command[:4]) == "NOOP" {
			// Explicitly ignore no-ops (like leader election markers) to avoid log noise
			continue
		} else {
			log.Printf("[engine] WARNING: Dropped command with missing or invalid prefix at index %d", msg.CommandIndex)
			continue
		}

		// ROUTE 1: DISTRIBUTED STATE TRANSITIONS (Task Completed, Failed, etc)
		if isSync {
			var walEntry wal.LogEntry
			if err := json.Unmarshal(cleanPayload, &walEntry); err == nil && walEntry.Event != 0 {
				log.Printf("[engine] Executing distributed State Transition (Event %v, Task %s)", walEntry.Event, walEntry.TaskID)
				store.ApplyReplayEvent(walEntry)
			} else {
				log.Printf("[engine] Failed to decode SYNC payload: %v", err)
			}
			continue
		}

		// ROUTE 2: NEW DAG/TASK SUBMISSIONS
		if isTask {
			var req protocol.SubmitTaskPayload
			if err := json.Unmarshal(cleanPayload, &req); err != nil {
				log.Printf("[engine] Failed to decode TASK payload: %v", err)
				continue
			}

			if req.JobType == "SCATTER" && req.TotalMAPs > 1 {
				log.Printf("[engine] Executing committed SCATTER job (Raft Index: %d)", msg.CommandIndex)
				handleScatterJobInEngine(store, req, msg.CommandIndex)
			} else {
				log.Printf("[engine] Executing committed Standard Task (Raft Index: %d)", msg.CommandIndex)

				task := &kv.Task{
					ID:          fmt.Sprintf("task-%d", msg.CommandIndex),
					QueueName:   req.QueueName,
					Payload:     req.Payload,
					Status:      kv.StatusPending,
					RequiredCPU: req.RequiredCPU,
					RequiredRAM: req.RequiredRAM,
					MaxRetries:  3,
					CreatedAt:   time.Now(),
				}
				store.SetTask(task)
				log.Printf("[engine] Standard task %s queued on %q", task.ID, task.QueueName)
			}
		}
	}
}

// handleScatterJobInEngine is the engine-side equivalent of handleScatterJob.
// It performs only in-memory mutations (RegisterJob + SetTask for each MAP).
// No WAL writes and no TCP responses are issued here because:
//   - Raft already durably committed the payload before this function runs.
//   - The client was already ACK'd by the goroutine in BrainProcessLoop.
func handleScatterJobInEngine(store *kv.KVStore, req protocol.SubmitTaskPayload, cmdIndex uint64) {
	jobID := fmt.Sprintf("job-%d", cmdIndex)

	var userPayload map[string]interface{}
	if err := json.Unmarshal([]byte(req.Payload), &userPayload); err != nil {
		log.Printf("[engine] SCATTER: failed to parse user payload: %v", err)
		return
	}

	// Build the GATHER task payload.
	gatherPayloadMap := make(map[string]interface{})
	for k, v := range userPayload {
		gatherPayloadMap[k] = v
	}
	gatherPayloadMap["job_type"] = "GATHER"
	gatherPayloadMap["job_id"] = jobID
	gatherPayloadBytes, _ := json.Marshal(gatherPayloadMap)

	gatherTask := &kv.Task{
		ID:          fmt.Sprintf("task-%d-gather", cmdIndex),
		JobID:       jobID,
		QueueName:   req.QueueName,
		Status:      kv.StatusPending,
		RequiredCPU: req.RequiredCPU,
		RequiredRAM: req.RequiredRAM,
		Payload:     string(gatherPayloadBytes),
		CreatedAt:   time.Now(),
	}

	// Register the job and gatherTask in memory.
	store.RegisterJob(jobID, req.TotalMAPs, gatherTask)

	// Build and register all MAP tasks in memory.
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
			ID:            fmt.Sprintf("task-%d-map-%d", cmdIndex, i),
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

	log.Printf("[engine] SCATTER job %s registered: %d MAP tasks + 1 GATHER task queued on %q",
		jobID, req.TotalMAPs, req.QueueName)
}
