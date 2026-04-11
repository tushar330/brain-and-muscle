package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"context"

	"github.com/akashtripathi12/brain-and-muscle/internal/db"
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
		var isProject bool
		var cleanPayload []byte

		if len(msg.Command) > 5 && string(msg.Command[:5]) == "TASK:" {
			isTask = true
			cleanPayload = msg.Command[5:]
		} else if len(msg.Command) > 5 && string(msg.Command[:5]) == "SYNC:" {
			isSync = true
			cleanPayload = msg.Command[5:]
		} else if len(msg.Command) > 8 && string(msg.Command[:8]) == "PROJECT:" {
			isProject = true
			cleanPayload = msg.Command[8:]
		} else if len(msg.Command) >= 4 && string(msg.Command[:4]) == "NOOP" {
			// Explicitly ignore no-ops (like leader election markers) to avoid log noise
			continue
		} else {
			log.Printf("[engine] WARNING: Dropped command with missing or invalid prefix at index %d", msg.CommandIndex)
			continue
		}

		// ROUTE 0: PROJECT REGISTRATION (Control Plane Sync)
		if isProject {
			var reg protocol.RegisterProjectPayload
			if err := json.Unmarshal(cleanPayload, &reg); err != nil {
				log.Printf("[engine] Failed to decode PROJECT payload: %v", err)
				continue
			}

			log.Printf("[engine] Registering Project: %s (User: %s)", reg.RepoFullName, reg.UserID)

			// EXECUTE THE EVOLUTION PLAN: Insert into public.projects
			// This is safe because only the Leader (or all nodes in determinism) will execute this
			// and since we are using Raft, the order is guaranteed.
			query := `
				INSERT INTO projects (user_id, repo_full_name, webhook_secret)
				VALUES ($1, $2, $3)
				ON CONFLICT (repo_full_name) DO UPDATE SET webhook_secret = EXCLUDED.webhook_secret;
			`
			_, err := db.Pool.Exec(context.Background(), query, reg.UserID, reg.RepoFullName, reg.WebhookSecret)
			if err != nil {
				log.Printf("[engine] CRITICAL: Failed to insert project into DB: %v", err)
			}
			continue
		}

		// ROUTE 1: DISTRIBUTED STATE TRANSITIONS (Task Completed, Failed, etc)
		if isSync {
			if len(msg.Command) >= 19 && string(msg.Command[:19]) == "SYNC:SERVICE_READY:" {

				// Extract purely the JSON part (everything after the 19-char prefix)
				jsonPayload := msg.Command[19:]

				var readyPayload protocol.ServiceReadyPayload
				if err := json.Unmarshal(jsonPayload, &readyPayload); err == nil {
					handleServiceReadyEvent(store, readyPayload)
				} else {
					log.Printf("[engine] Failed to decode SERVICE_READY payload: %v", err)
				}
				continue
			}

			var walEntry wal.LogEntry
			if err := json.Unmarshal(cleanPayload, &walEntry); err == nil && walEntry.Event != 0 {
				log.Printf("[engine] Executing distributed State Transition (Event %v, Task %s)", walEntry.Event, walEntry.TaskID)
				store.ApplyReplayEvent(walEntry)

				if walEntry.Event == 1 { // Event 1 = TaskCompleted
					log.Printf("[engine] State Transition applied for %s", walEntry.TaskID)
				}
			} else {
				log.Printf("[engine] Failed to decode SYNC payload: %v", err)
			}
			continue
		}

		// ROUTE 2: NEW DAG/TASK SUBMISSIONS
		if isTask {
			cleanPayload := msg.Command[5:]
			var req protocol.SubmitTaskPayload
			if err := json.Unmarshal(cleanPayload, &req); err != nil {
				log.Printf("[engine] Failed to decode TASK payload: %v", err)
				continue
			}

			log.Printf("[engine] 🏗️ Processing Raft Task %s (User: %s, JobType: %s, CPU: %.1f, RAM: %d)", req.JobID, req.UserID, req.JobType, req.RequiredCPU, req.RequiredRAM)

			//  UNIT SAFETY: If RAM > 1 million (1TB+), it's probably bytes (e.g. 256*1024*1024).
			// Normalize to MB internally so the scheduler comparisons work.
			if req.RequiredRAM > 1000000 {
				log.Printf("[engine] ⚖️ Normalizing RAM for %s from bytes (%d) to MB (%d)", req.JobID, req.RequiredRAM, req.RequiredRAM/(1024*1024))
				req.RequiredRAM /= (1024 * 1024)
			}

			if req.JobType == "SCATTER" && req.TotalMAPs > 1 {
				log.Printf("[engine] Executing committed SCATTER job (Raft Index: %d)", msg.CommandIndex)
				handleScatterJobInEngine(store, req, msg.CommandIndex)
			} else if req.JobType == "ML_TRAIN" {
				handleMLJobPhase1(store, req, msg.CommandIndex)
			} else if req.JobType == protocol.JobTypeService {
				//  NEW: Intercept Daemon Compute
				handleServiceJob(store, req, msg.CommandIndex)
			} else {
				jobID := req.JobID
				if jobID == "" {
					jobID = fmt.Sprintf("job-%d", msg.CommandIndex)
				}
				req.JobID = jobID

				// Make sure you register the job state so Phase 2 knows about the path!
				store.RegisterJob(jobID, "MAP", req.TotalMAPs, req.UserScriptPath, req.GatherScriptPath, req.BuildCommand, req.RunCommand, req.RootDirectory, req.UserID, req.RequiredCPU, req.RequiredRAM)

				for i := 0; i < req.TotalMAPs; i++ {
					// 1. BUILD THE JSON PAYLOAD
					mapPayload := protocol.Payload{
						JobType:        "MAP",
						JobID:          jobID,
						ChunkIndex:     i,
						UserScriptPath: req.UserScriptPath, //  FIX: Pass the script path to the worker!
						Env:            req.Env,
						Args:           req.Args, //  ADD MISSING ARGS mapping!
						BuildCommand:   req.BuildCommand,
						RunCommand:     req.RunCommand,
						RootDirectory:  req.RootDirectory,
						RequiredCPU:    req.RequiredCPU, //  NEW
						RequiredRAM:    req.RequiredRAM, //  NEW
					}
					rawMapPayload, _ := json.Marshal(mapPayload)

					// 2. INJECT IT INTO THE TASK
					task := &kv.Task{
						ID:          fmt.Sprintf("%s-map-%d", jobID, i),
						JobID:       jobID,
						JobType:     "MAP",
						QueueName:   req.QueueName,
						Payload:     string(rawMapPayload),
						Status:      kv.StatusPending,
						RequiredCPU: req.RequiredCPU,
						RequiredRAM: req.RequiredRAM,
						ExposedPort: req.ExposedPort, //  NEW
						MaxRetries:  3,
						CreatedAt:   time.Now(),
						UserID:      req.UserID, //  Propagate Identity
					}
					store.SetTask(task)
				}
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
	jobID := req.JobID
	if jobID == "" {
		jobID = fmt.Sprintf("job-%d", cmdIndex)
	}

	var userPayload map[string]interface{}
	if err := json.Unmarshal([]byte(req.Payload), &userPayload); err != nil {
		log.Printf("[engine] SCATTER: failed to parse user payload: %v", err)
		return
	}

	// Register the job and gatherTask in memory.
	store.RegisterJob(jobID, "MAP", req.TotalMAPs, req.UserScriptPath, req.GatherScriptPath, req.BuildCommand, req.RunCommand, req.RootDirectory, req.UserID, req.RequiredCPU, req.RequiredRAM)

	// Build and register all MAP tasks in memory.
	for i := 0; i < req.TotalMAPs; i++ {
		mapPayload := protocol.Payload{
			JobType:        "MAP",
			JobID:          jobID,
			ChunkIndex:     i,
			UserScriptPath: req.UserScriptPath, //  Propagate script path!
			AttemptNumber:  1,
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
		//  NEW
		mapPayload.RequiredCPU = req.RequiredCPU
		mapPayload.RequiredRAM = req.RequiredRAM

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
			UserID:        req.UserID, //  Propagate Identity
		}
		store.SetTask(mapTask)
	}

	log.Printf("[engine] SCATTER job %s registered: %d MAP tasks + 1 GATHER task queued on %q",
		jobID, req.TotalMAPs, req.QueueName)
}

func handleMLJobPhase1(store *kv.KVStore, req protocol.SubmitTaskPayload, cmdIndex uint64) {
	jobID := req.JobID
	if jobID == "" {
		jobID = fmt.Sprintf("job-ml-%d", cmdIndex)
	}

	store.RegisterMLJob(jobID, req.TotalMAPs, req.UserScriptPath, req.GatherScriptPath, req.BuildCommand, req.RunCommand, req.RootDirectory, req.UserID, req.RequiredCPU, req.RequiredRAM)

	//  FIX 1: Create and marshal the JSON payload for the Muscle
	psPayload := protocol.Payload{
		JobType:        "ML_PS",
		JobID:          jobID,
		UserScriptPath: req.UserScriptPath,
		BuildCommand:   req.BuildCommand,
		RunCommand:     req.RunCommand,
		RootDirectory:  req.RootDirectory,
		Args:           req.Args,        //  ADD MISSING ARGS mapping!
		RequiredCPU:    req.RequiredCPU, //  NEW
		RequiredRAM:    req.RequiredRAM, //  NEW
		Env:            make(map[string]string),
	}

	//  INJECT ML CONTROLS
	psPayload.Env["JOB_ID"] = jobID
	psPayload.Env["ROLE"] = "PS"
	psPayload.Env["TOTAL_WORKERS"] = fmt.Sprintf("%d", req.TotalMAPs)

	// Copy existing env from request
	for k, v := range req.Env {
		psPayload.Env[k] = v
	}

	rawPsPayload, _ := json.Marshal(psPayload)

	psTask := &kv.Task{
		ID:          jobID,
		JobID:       jobID,
		JobType:     "ML_PS",
		QueueName:   req.QueueName,
		Payload:     string(rawPsPayload),
		Status:      kv.StatusPending,
		RequiredCPU: req.RequiredCPU,
		RequiredRAM: req.RequiredRAM,
		MaxRetries:  3,
		UserID:      req.UserID, //  Propagate Identity
	}

	store.SetTask(psTask)
	log.Printf("[engine] Phase 1: ML_PS task queued for Job %s.", jobID)
}

func handleServiceReadyEvent(store *kv.KVStore, payload protocol.ServiceReadyPayload) {
	// 1. 🚦 Register the service endpoint instantly
	store.RegisterServiceEndpoint(payload.JobID, payload.Address)

	if payload.ServiceRole == "BACKEND_API" {

		//  NEW: Auto-recover JobMeta and Task if they are missing
		// Since the Muscle worker told us this service is running, but the Brain
		// lost it from memory (e.g. restart without snapshot), we recreate it!
		if payload.WorkerID != "" && (payload.RecoveredCPU > 0 || payload.RecoveredRAM > 0) {
			hostIP, _, _ := net.SplitHostPort(payload.Address)

			//  Extract UserID from OriginalPayload if possible
			recoveredUserID := SystemUserID
			var origSub protocol.SubmitTaskPayload
			if err := json.Unmarshal([]byte(payload.OriginalSubmitBytes), &origSub); err == nil && origSub.UserID != "" {
				recoveredUserID = origSub.UserID
			}

			store.RecoverLostServiceJob(payload.JobID, payload.RecoveredCPU, payload.RecoveredRAM, payload.WorkerID, hostIP, payload.OriginalSubmitBytes, recoveredUserID)
		}

		return
	}

	jobPtr, success := store.UpdateJobTopology(payload.JobID, payload.ServiceRole, payload.Address)
	if !success {
		return
	}

	//  PROTECTION: Only spawn workers if they don't already exist for this job!
	// This prevents duplicate worker waves after a Brain restart or PS re-attachment.
	existingTasks := store.GetTasksForJob(payload.JobID)
	for _, t := range existingTasks {
		if t.JobType == "ML_WORKER" {
			log.Printf("[engine] ML_WORKERs already exist for Job %s — skipping Phase 2 trigger.", payload.JobID)
			return
		}
	}

	for i := 0; i < jobPtr.TotalMAPs; i++ {
		workerPayload := protocol.Payload{
			JobType:        "ML_WORKER",
			JobID:          jobPtr.JobID,
			ChunkIndex:     i,
			TopologyMap:    jobPtr.TopologyMap,
			UserScriptPath: jobPtr.UserScriptPath,
			BuildCommand:   jobPtr.BuildCommand,
			RunCommand:     jobPtr.RunCommand,
			RootDirectory:  jobPtr.RootDirectory,
		}

		rawPayload, _ := json.Marshal(workerPayload)

		workerTask := &kv.Task{
			ID:      fmt.Sprintf("%s-worker-%d", jobPtr.JobID, i),
			JobID:   jobPtr.JobID,
			JobType: "ML_WORKER",
			Payload: string(rawPayload),
			Status:  kv.StatusPending,
			UserID:  jobPtr.UserID, //  Propagate Identity from jobPtr
		}
		store.SetTask(workerTask)
	}

	log.Printf("[engine] Phase 2: Topology established (%s). Spawning %d ML_WORKERs.", payload.Address, jobPtr.TotalMAPs)
}

func handleServiceJob(store *kv.KVStore, req protocol.SubmitTaskPayload, cmdIndex uint64) {
	jobID := req.JobID
	if jobID == "" {
		jobID = fmt.Sprintf("service-%d", cmdIndex)
	}

	taskID := jobID

	// Store original payload JSON for restart support
	originalPayloadBytes, _ := json.Marshal(req)
	store.RegisterJobWithPayload(jobID, protocol.JobTypeService, 1, req.UserScriptPath, "", req.BuildCommand, req.RunCommand, req.RootDirectory, string(originalPayloadBytes), req.UserID, req.RequiredCPU, req.RequiredRAM)

	// 1. Build the Payload for the Muscle
	servicePayload := protocol.Payload{
		JobType:             protocol.JobTypeService,
		JobID:               jobID,
		UserScriptPath:      req.UserScriptPath,
		ExposedPort:         req.ExposedPort,
		RequiredCPU:         req.RequiredCPU,
		RequiredRAM:         req.RequiredRAM,
		SourceType:          req.SourceType,
		Env:                 req.Env,
		Args:                req.Args, //  Propagate args
		BuildCommand:        req.BuildCommand,
		RunCommand:          req.RunCommand,
		RootDirectory:       req.RootDirectory,
		GitRepo:             req.GitRepo,
		OriginalSubmitBytes: string(originalPayloadBytes),
	}
	rawPayload, _ := json.Marshal(servicePayload)

	// 2. Create a single Task (No chunking needed)
	serviceTask := &kv.Task{
		ID:          taskID,
		JobID:       jobID,
		JobType:     protocol.JobTypeService,
		QueueName:   req.QueueName,
		Payload:     string(rawPayload),
		Status:      kv.StatusPending, //  Set to Pending so workers can claim it
		RequiredCPU: req.RequiredCPU,
		RequiredRAM: req.RequiredRAM,
		ExposedPort: req.ExposedPort,
		MaxRetries:  5,
		CreatedAt:   time.Now(),
		UserID:      req.UserID, //  Propagate Identity
	}

	store.SetTask(serviceTask)
	log.Printf("[engine] SERVICE registered/updated: %s (source: %s), awaiting deployment on port %d", jobID, req.SourceType, req.ExposedPort)
}
