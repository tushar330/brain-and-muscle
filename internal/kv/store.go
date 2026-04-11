package kv

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"context"

	"github.com/akashtripathi12/brain-and-muscle/internal/db"
	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/akashtripathi12/brain-and-muscle/internal/wal"
)

const SystemUserID = "00000000-0000-0000-0000-000000000000"

type TaskStatus int
type JobStatus int

const (
	StatusPending TaskStatus = iota
	StatusRunning
	StatusCompleted
	StatusProvisioning
	StatusFailed
	StatusKilled
)

const (
	JobProvisioning JobStatus = iota
	JobRunning
	JobCompleted
	JobFailed
	JobRestarting //  Protected state during manual restarts
)

type Task struct {
	ID               string     `json:"id"`
	JobID            string     `json:"job_id"`
	JobType          string     `json:"job_type"`
	QueueName        string     `json:"queue_name"`
	Payload          string     `json:"payload"`
	Result           string     `json:"result"`
	Status           TaskStatus `json:"status"`
	RequiredCPU      float64    `json:"required_cpu"`
	RequiredRAM      int        `json:"required_ram"`
	ExposedPort      int        `json:"exposed_port"`
	AssignedWorkerID string     `json:"assigned_worker_id"`
	AttemptNumber    int        `json:"attempt_number"`
	MaxRetries       int        `json:"max_retries"`
	RetryCount       int        `json:"retry_count"`
	CreatedAt        time.Time  `json:"created_at"`
	CompletedAt      time.Time  `json:"completed_at"`
	LastStatusUpdate time.Time  `json:"last_status_update"`
	WorkerIP         string     `json:"worker_ip"`
	UserID           string     `json:"user_id"`
}

type Worker struct {
	ID             string               `json:"id"`
	IP             string               `json:"ip"`
	TotalCPU       float64              `json:"total_cpu"`
	TotalRAM       int                  `json:"total_ram"`
	AvailCPU       float64              `json:"avail_cpu"`
	AvailRAM       int                  `json:"avail_ram"`
	OccupiedPorts  map[int]string       `json:"occupied_ports,omitempty"`
	ActiveTaskIDs  []string             `json:"active_task_ids,omitempty"`
	LastHeartbeat  time.Time            `json:"last_heartbeat"`
	LastLoggedSkip map[string]time.Time `json:"-"` // Internal only
}

type Job struct {
	ID           string
	Type         string
	Status       string
	TotalWorkers int
	TopologyMap  map[string]string
	UserID       string //  Multi-Tenancy ID
}

type JobMeta struct {
	JobID            string            `json:"job_id"`
	JobType          string            `json:"job_type"`
	TotalMAPs        int               `json:"total_maps"`
	PendingMAPs      int               `json:"pending_maps"`
	FailedMAPs       int               `json:"failed_maps"`
	Status           JobStatus         `json:"status"`
	Result           string            `json:"result"`
	ResultURIs       map[int]string    `json:"result_uris"`
	TopologyMap      map[string]string `json:"topology_map"`
	UserScriptPath   string            `json:"user_script_path"`
	GatherScriptPath string            `json:"gather_script_path"`
	BuildCommand     string            `json:"build_command"`
	RunCommand       string            `json:"run_command"`
	RootDirectory    string            `json:"root_directory"`
	AssignedWorkerID string            `json:"assigned_worker_id"`
	OriginalPayload  string            `json:"original_payload"`
	CompletedAt      time.Time         `json:"completed_at"`
	UserID           string            `json:"user_id"`
	RequiredCPU      float64           `json:"required_cpu"`
	RequiredRAM      int               `json:"required_ram"`
	CreatedAt        time.Time         `json:"created_at"`
}

type JobSummary struct {
	JobID            string    `json:"job_id"`
	JobType          string    `json:"job_type"`
	Status           JobStatus `json:"status"`
	RequiredCPU      float64   `json:"required_cpu"`
	RequiredRAM      int       `json:"required_ram"`
	AssignedWorkerID string    `json:"assigned_worker_id"`
	Result           string    `json:"result,omitempty"`
	CompletedAt      time.Time `json:"completed_at,omitempty"`
	PublicRoute      string    `json:"public_route,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UserID           string    `json:"user_id"` //  Multi-Tenancy ID
}

type KVStore struct {
	mu       sync.RWMutex
	tasks    map[string]*Task
	workers  map[string]*Worker
	queues   map[string][]string
	jobs     map[string]*JobMeta
	services map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		mu:       sync.RWMutex{},
		tasks:    make(map[string]*Task),
		workers:  make(map[string]*Worker),
		queues:   make(map[string][]string),
		jobs:     make(map[string]*JobMeta),
		services: make(map[string]string),
	}
}

// Replay engine

// ApplyReplayEvent is the deterministic replay engine.
// It routes each WAL entry to the same store method that originally produced it.
// Called only during Phase 1 boot — never during live operation.
func (kv *KVStore) ApplyReplayEvent(entry wal.LogEntry) {
	switch entry.Event {

	case wal.EventTaskQueued:
		var task Task
		json.Unmarshal(entry.Payload, &task)
		kv.SetTask(&task)

		//  AUTO-JOB REGISTRATION: If this is a SERVICE, ensure it has a JobMeta entry!
		if task.JobType == protocol.JobTypeService {
			kv.RegisterServiceJob(task.JobID, task.RequiredCPU, task.RequiredRAM, task.UserID, task.Payload)
		}

	case wal.EventTaskDispatched:
		if task, ok := kv.tasks[entry.TaskID]; ok {
			kv.removeFromQueue(task.QueueName, entry.TaskID)
		}
		kv.TransitionTaskState(entry.TaskID, StatusPending, StatusRunning, entry.WorkerID)

	case wal.EventTaskReverted:
		kv.TransitionTaskState(entry.TaskID, StatusRunning, StatusPending, "")

	case wal.EventTaskCompleted:
		var envelope protocol.SubmitResultPayload
		json.Unmarshal(entry.Payload, &envelope)
		kv.CompleteTask(entry.TaskID, envelope.ResultPayload)

		//  REPLAY TRIGGER: Ensure we check for job completion during Raft replay!
		if entry.JobID != "" {
			kv.checkAndFinalizeMLJobInternal(entry.JobID)
		}

	case wal.EventTaskFailed:
		kv.HandleMAPFailure(entry.TaskID, entry.WorkerID)

	case wal.EventJobRegistered:
		var payload protocol.JobRegisteredPayload
		json.Unmarshal(entry.Payload, &payload)
		//  REPLAY COMPATIBILITY: Passing SystemUserID or extracting from entry if available
		kv.RegisterJob(entry.JobID, "MAP", payload.TotalMAPs, payload.UserScriptPath, payload.GatherScriptPath, payload.BuildCommand, payload.RunCommand, payload.RootDirectory, SystemUserID, payload.RequiredCPU, payload.RequiredRAM)
		for _, raw := range payload.MapTasks {
			var mapTask Task
			json.Unmarshal(raw, &mapTask)
			// Ensure mapTask has a UserID if we can recover it
			mapTask.UserID = SystemUserID
			// SetTask handles queue insertion only for StatusPending tasks.
			kv.SetTask(&mapTask)
		}

	case wal.EventServiceReady:
		var payload protocol.ServiceReadyPayload
		json.Unmarshal(entry.Payload, &payload)

		if payload.ServiceRole == "BACKEND_API" {
			kv.RegisterServiceEndpoint(payload.JobID, payload.Address)
		} else {
			// Legacy Topology Map for ML Parameter Servers
			kv.UpdateJobTopology(payload.JobID, payload.ServiceRole, payload.Address)
		}

	case wal.EventMAPCompleted:
		var payload protocol.MapCompletedPayload
		json.Unmarshal(entry.Payload, &payload)
		kv.RecordMAPComplete(entry.JobID, payload.ChunkIndex, payload.ResultURI)

	case wal.EventMAPFailed:
		kv.RecordMAPFailed(entry.JobID)

	case wal.EventTaskKilled:
		kv.KillTask(entry.TaskID)
	}
}

// ─── Task operations ─────────────────────────────────────────────────────────

func (kv *KVStore) MarkJobCompleted(jobID string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if job, ok := kv.jobs[jobID]; ok {
		job.Status = JobCompleted
		log.Printf("[store] ✅ Job %s marked as COMPLETED", jobID)

		//  SYNC TO DB: Persistence Ledger
		payload, _ := json.Marshal(job)
		db.SyncJob(context.Background(), jobID, job.UserID, int(JobCompleted), string(payload), &job.Result, job.JobType)
	}
}

func (kv *KVStore) RegisterJob(jobID string, jobType string, totalMAPs int, scriptPath string, gatherPath string, buildCmd, runCmd, rootDir string, userID string, cpu float64, ram int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  MIGRATION: Sanitize identity
	safeUserID := userID
	if safeUserID == "" {
		safeUserID = "00000000-0000-0000-0000-000000000000"
	}

	kv.jobs[jobID] = &JobMeta{
		JobID:            jobID,
		JobType:          jobType,
		TotalMAPs:        totalMAPs,
		PendingMAPs:      totalMAPs,
		Status:           JobProvisioning,
		ResultURIs:       make(map[int]string),
		UserScriptPath:   scriptPath,
		GatherScriptPath: gatherPath,
		BuildCommand:     buildCmd,
		RunCommand:       runCmd,
		RootDirectory:    rootDir,
		UserID:           safeUserID,
		RequiredCPU:      cpu,
		RequiredRAM:      ram,
		CreatedAt:        time.Now(),
	}

	log.Printf("[store] 📝 Job %s registered (Type: %s, CPU: %.1f, RAM: %d, User: %s)", jobID, jobType, cpu, ram, userID)

	//  SYNC TO DB: Persistence Ledger
	payload, _ := json.Marshal(kv.jobs[jobID])
	db.SyncJob(context.Background(), jobID, safeUserID, int(JobRunning), string(payload), nil, jobType)
}

func (kv *KVStore) SetTask(task *Task) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.tasks[task.ID] = task
	if task.Status == StatusPending {
		kv.enqueueTask(task)
	}
}

// PopBestFitTask selects the highest-scoring pending task whose resource
// requirements (CPU, RAM, and Ports) fit within the available limits.
func (kv *KVStore) PopBestFitTask(workerID string, availCPU float64, availRAM int, workerOccupiedPorts map[int]string) (Task, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 1. Get the worker from memory to double-check their ports
	worker, exists := kv.workers[workerID]
	if !exists {
		return Task{}, false
	}

	queueName := "default" // Simplified for now, or use worker's queue
	ids := kv.queues[queueName]
	if len(ids) == 0 {
		return Task{}, false
	}

	// Guard: prevent division-by-zero in the normalised scoring below.
	if availCPU <= 0 {
		availCPU = 0.1
	}
	if availRAM <= 0 {
		availRAM = 1
	}

	// Single-pass: build a clean queue (no stale IDs) and pick the best task.
	bestID := ""
	bestScore := -1.0
	cleanIDs := make([]string, 0, len(ids))

	for _, id := range ids {
		t, ok := kv.tasks[id]
		if !ok || t.Status != StatusPending {
			// stale ID — exclude from rebuilt queue so it is evicted.
			continue
		}
		cleanIDs = append(cleanIDs, id)

		cpu := t.RequiredCPU
		if cpu <= 0 {
			cpu = 1
		}
		ram := t.RequiredRAM
		if ram <= 0 {
			ram = 1
		}

		if cpu > availCPU || ram > availRAM {
			if time.Since(worker.LastLoggedSkip[t.ID]) > 1*time.Minute {
				log.Printf("[scheduler] Skipping task %s for worker %s: Insufficient resources (%.1f/%.1f CPU, %d/%d MB RAM)", t.ID, workerID, cpu, availCPU, ram, availRAM)
				if worker.LastLoggedSkip == nil {
					worker.LastLoggedSkip = make(map[string]time.Time)
				}
				worker.LastLoggedSkip[t.ID] = time.Now()
			}
			continue
		}

		//  PORT CHECK
		// Now using Dynamic Port Allocation! The Muscle will pick a free port, so the Brain
		// doesn't need to check for host-level collisions here for Services.
		// (If you want to allow forcing a host port in the future, you'd add it back here).

		// available RAM. Maximum possible value is 2.0 (task fills node exactly).
		cpuScore := cpu / availCPU
		ramScore := float64(ram) / float64(availRAM)
		score := cpuScore + ramScore
		if score > bestScore {
			bestScore = score
			bestID = id
		}
	}

	// Always persist the cleaned queue, even when no task fits.
	if bestID == "" {
		kv.queues[queueName] = cleanIDs
		return Task{}, false
	}

	// Rebuild final queue: cleanIDs minus the winner.
	finalIDs := make([]string, 0, len(cleanIDs)-1)
	for _, id := range cleanIDs {
		if id != bestID {
			finalIDs = append(finalIDs, id)
		}
	}
	kv.queues[queueName] = finalIDs

	// Mark Task as Running
	t := kv.tasks[bestID]
	t.Status = StatusRunning
	t.AssignedWorkerID = workerID
	t.WorkerIP = worker.IP //  NEW: Capture IP at assignment time
	t.LastStatusUpdate = time.Now()

	//  NEW: Update parent Job status and assignment
	if job, ok := kv.jobs[t.JobID]; ok {
		job.Status = JobRunning
		job.AssignedWorkerID = workerID
	}

	return *t, true
}

func (kv *KVStore) GetTask(id string) (Task, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	task, ok := kv.tasks[id]
	if !ok {
		return Task{}, false
	}
	return *task, true
}

func (kv *KVStore) GetJob(jobID string) (JobMeta, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	job, ok := kv.jobs[jobID]
	if !ok {
		return JobMeta{}, false
	}
	return *job, true
}

// HasPendingServiceTask checks if a SERVICE task already exists for the given
// script and port. Used to prevent duplicate service deployments when Raft
// indices shift across restarts.
func (kv *KVStore) HasPendingServiceTask(scriptPath string, exposedPort int) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	for _, t := range kv.tasks {
		if t.JobType == "SERVICE" && t.ExposedPort == exposedPort {
			//  Only block if the existing task is actually PENDING or PROVISIONING.
			// If it's already RUNNING, we allow a new Shadow Deployment to replace it.
			if t.Status != StatusPending && t.Status != StatusProvisioning {
				continue
			}

			// Parse the payload to check the script path
			var p struct {
				UserScriptPath string `json:"user_script_path"`
			}
			json.Unmarshal([]byte(t.Payload), &p)
			if p.UserScriptPath == scriptPath {
				return true
			}
		}
	}
	return false
}

func (kv *KVStore) GetAllNodes() []Worker {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	nodes := make([]Worker, 0, len(kv.workers))
	for _, w := range kv.workers {
		nodes = append(nodes, *w)
	}
	return nodes
}

func (kv *KVStore) GetMaxWorkerCapacity() (maxCPU float64, maxRAM int) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	for _, w := range kv.workers {
		// Only consider workers that have heartbeated in the last 60s
		if time.Since(w.LastHeartbeat) > 60*time.Second {
			continue
		}
		if w.TotalCPU > maxCPU {
			maxCPU = w.TotalCPU
		}
		if w.TotalRAM > maxRAM {
			maxRAM = w.TotalRAM
		}
	}
	return
}

func (kv *KVStore) GetAllJobs() []JobSummary {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	summaries := make([]JobSummary, 0, len(kv.jobs))
	for jobID, j := range kv.jobs {
		// Default values from JobMeta
		summary := JobSummary{
			JobID:            jobID,
			JobType:          j.JobType,
			Status:           j.Status,
			AssignedWorkerID: j.AssignedWorkerID,
			Result:           j.Result,
			CompletedAt:      j.CompletedAt,
			UserID:           j.UserID,
		}

		// Find resource info from one of the job's tasks
		for _, t := range kv.tasks {
			if t.JobID == jobID {
				summary.RequiredCPU = t.RequiredCPU
				summary.RequiredRAM = t.RequiredRAM
				summary.AssignedWorkerID = t.AssignedWorkerID
				summary.CreatedAt = t.CreatedAt
				break
			}
		}

		// Add common service prefix if it's a SERVICE type
		if j.JobType == "SERVICE" {
			summary.PublicRoute = "/proxy/" + jobID
		}

		summaries = append(summaries, summary)
	}
	return summaries
}

func (kv *KVStore) GetTasksForJob(jobID string) []Task {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	tsks := make([]Task, 0)
	for _, t := range kv.tasks {
		if t.JobID == jobID {
			tsks = append(tsks, *t)
		}
	}
	return tsks
}

func (kv *KVStore) GetMaxRaftIndex() uint64 {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	maxIdx := uint64(0)
	for id := range kv.tasks {
		parts := strings.Split(id, "-")
		if len(parts) >= 2 {
			if idx, err := strconv.ParseUint(parts[1], 10, 64); err == nil && idx > maxIdx {
				maxIdx = idx
			}
		}
	}
	for id := range kv.jobs {
		parts := strings.Split(id, "-")
		if len(parts) >= 2 {
			if idx, err := strconv.ParseUint(parts[1], 10, 64); err == nil && idx > maxIdx {
				maxIdx = idx
			}
		}
	}
	return maxIdx
}

func (kv *KVStore) TransitionTaskState(id string, from, to TaskStatus, workerID string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	task, ok := kv.tasks[id]
	if !ok {
		//  HYBRID QUEUE SHIM: If the ID checked out from Postgres is an overarching Job UUID
		// (e.g. for batch jobs), then update the Job's assignment instead of erroring out.
		// This fixes the log stream proxy returning 404s for unassigned jobs.
		if job, jobOk := kv.jobs[id]; jobOk {
			if to == StatusRunning {
				job.AssignedWorkerID = workerID
				job.Status = JobRunning
			} else if to == StatusPending || to == StatusCompleted || to == StatusFailed {
				job.AssignedWorkerID = ""
				if to == StatusCompleted {
					job.Status = JobCompleted
				} else if to == StatusFailed {
					job.Status = JobFailed
				}
			}
			return nil
		}
		return fmt.Errorf("task %s not found", id)
	}

	if task.Status != from {
		return fmt.Errorf("expected status %v, got %v", from, task.Status)
	}

	task.Status = to
	task.LastStatusUpdate = time.Now()
	switch to {
	case StatusRunning:
		task.AssignedWorkerID = workerID
	case StatusPending:
		task.AssignedWorkerID = ""
		kv.enqueueTask(task)
	case StatusCompleted, StatusFailed:
		task.AssignedWorkerID = ""
		task.CompletedAt = time.Now()
	}
	return nil
}

func (kv *KVStore) CompleteTask(taskID, result string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  Normalization: If result is a JSON blob_url pointer, extract it!
	var blobPtr struct {
		BlobURL string `json:"blob_url"`
	}
	effectiveResult := result
	if err := json.Unmarshal([]byte(result), &blobPtr); err == nil && blobPtr.BlobURL != "" {
		effectiveResult = blobPtr.BlobURL
		log.Printf("[store] 🧼 Result normalized from JSON pointer to raw URL")
	} else {
		log.Printf("[store] ℹ️ Result stored as-is (not a JSON blob_url pointer or unmarshal failed)")
	}

	task, ok := kv.tasks[taskID]
	if !ok {
		//  HYBRID QUEUE SHIM: Complete the overarching Job if the ID matches.
		// For legacy jobs, ID is same. For services, ID is jobID.
		if job, jobOk := kv.jobs[taskID]; jobOk {
			job.Status = JobCompleted
			job.Result = effectiveResult
			job.CompletedAt = time.Now()
			log.Printf("[store] ✅ Job %s marked as COMPLETED (Hybrid Shim). Result: %s", taskID, effectiveResult)

			//  SYNC TO DB: Persistence Ledger
			payload, _ := json.Marshal(job)
			// Use context.Background() as this is a background state transition
			db.SyncJob(context.Background(), taskID, job.UserID, int(JobCompleted), string(payload), &effectiveResult, job.JobType)
		}
		return
	}

	// Sync task state to job if applicable
	if job, jobOk := kv.jobs[task.JobID]; jobOk && task.JobID != "" {
		// If it's a singleton service or ML Parameter Server, we sync the job status too!
		if task.JobType == "SERVICE" || task.JobType == "ML_PS" {
			job.Status = JobCompleted
			job.Result = effectiveResult
			job.CompletedAt = time.Now()
			log.Printf("[store] ✅ Job %s synced for %s COMPLETED.", task.JobID, task.JobType)

			//  SYNC TO DB: Persistence Ledger (was missing!)
			payload, _ := json.Marshal(job)
			db.SyncJob(context.Background(), task.JobID, job.UserID, int(JobCompleted), string(payload), &effectiveResult, job.JobType)
		}
	}

	task.Status = StatusCompleted
	task.Result = effectiveResult
	task.CompletedAt = time.Now()

	//  EAGER RESOURCE RELEASE: Immediately reclaim CPU/RAM in the Brain's view!
	if task.AssignedWorkerID != "" {
		if worker, exists := kv.workers[task.AssignedWorkerID]; exists {
			worker.AvailCPU += task.RequiredCPU
			worker.AvailRAM += task.RequiredRAM
			log.Printf("[reclaim] 🔋 Recovered %.1f CPU, %d MB RAM from %s (Task %s Finished)", task.RequiredCPU, task.RequiredRAM, task.AssignedWorkerID, taskID)
		}
	}

	//  NEW: If this was a GATHER task, mark the parent Job as Completed too!
	if task.JobType == "GATHER" && task.JobID != "" {
		if job, ok := kv.jobs[task.JobID]; ok {
			job.Status = JobCompleted
			job.Result = effectiveResult
			job.CompletedAt = time.Now()
			log.Printf("[job] %s fully completed (GATHER phase finished)", task.JobID)

			//  SYNC TO DB: Persistence Ledger
			payload, _ := json.Marshal(job)
			db.SyncJob(context.Background(), task.JobID, job.UserID, int(JobCompleted), string(payload), &effectiveResult, job.JobType)
		}
	}

	//  ML COMPLETION: Trigger automated finalisation if this was an ML worker
	if task.JobID != "" {
		kv.checkAndFinalizeMLJobInternal(task.JobID)
	}
}

// checkAndFinalizeMLJobInternal is the central point of failure detection for ML jobs.
// It assumes the caller ALREADY holds the kv.mu lock OR it manages its own locking.
// To be safe, we wrap it in a lock-aware helper.
func (kv *KVStore) checkAndFinalizeMLJobInternal(jobID string) {
	// Re-verify the job exists
	job, ok := kv.jobs[jobID]
	if !ok || job.Status == JobCompleted {
		return
	}

	isMLJob := false
	allWorkersDone := true
	workerCount := 0

	for _, t := range kv.tasks {
		if t.JobID == jobID && t.JobType == "ML_WORKER" {
			isMLJob = true
			workerCount++
			if t.Status != StatusCompleted {
				allWorkersDone = false
				break
			}
		}
	}

	// Terminal transition: if ML and all workers are home, we are done
	if isMLJob && allWorkersDone && workerCount > 0 {
		job.Status = JobCompleted
		job.CompletedAt = time.Now()
		log.Printf("[store] Job %s fully completed (All %d workers finished)", jobID, workerCount)

		// SYNC TO DB: Persistence Ledger (was missing!)
		payload, _ := json.Marshal(job)
		db.SyncJob(context.Background(), jobID, job.UserID, int(JobCompleted), string(payload), &job.Result, job.JobType)
	}
}

func (kv *KVStore) KillTask(taskID string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	task, ok := kv.tasks[taskID]
	if !ok {
		//  HYBRID QUEUE SHIM: Kill the overarching Job if the ID matches.
		if job, jobOk := kv.jobs[taskID]; jobOk {
			// 🔒 STATUS LOCK: Don't kill if a restart already moved it back to Provisioning/Pending
			if job.Status == JobProvisioning {
				log.Printf("[store] ⚠️ Ignoring stale Kill signal for Job %s (New update in progress)", taskID)
				return
			}
			job.Status = JobFailed // Mark as failed/cancelled
			job.CompletedAt = time.Now()
			log.Printf("[store] 💀 Job %s marked as KILLED (Hybrid Shim)", taskID)
		}
		return
	}

	//  PARTIAL STATUS LOCK:
	// If a new update is already pending/provisioning, we SKIP marking the status as Failed/Killed.
	// HOWEVER, we MUST continue so we can clear the old AssignedWorkerID and Routing entries,
	// otherwise the path is blocked for the new version.
	isFreshRestart := (task.Status == StatusPending || task.Status == StatusProvisioning)
	if isFreshRestart {
		log.Printf("[store] ♻️  Processing Kill for a Task in Restart (%s). Clearing resources but preserving PENDING status.", taskID)
	}

	// Sync task state to job if applicable
	if job, jobOk := kv.jobs[task.JobID]; jobOk && task.JobID != "" {
		if task.JobType == "SERVICE" {
			job.Status = JobFailed
			job.CompletedAt = time.Now()
			log.Printf("[store] 💀 Job %s synced for SERVICE KILLED (Hybrid Shim).", task.JobID)
		}
	}

	if !isFreshRestart {
		task.Status = StatusKilled
		task.CompletedAt = time.Now()

		//  EAGER RESOURCE RELEASE: Reclaim resources on Kill
		if task.AssignedWorkerID != "" {
			if worker, exists := kv.workers[task.AssignedWorkerID]; exists {
				worker.AvailCPU += task.RequiredCPU
				worker.AvailRAM += task.RequiredRAM
				log.Printf("[reclaim] 🔋 Recovered %.1f CPU, %d MB RAM from %s (Task %s Killed)", task.RequiredCPU, task.RequiredRAM, task.AssignedWorkerID, taskID)
			}
		}
	}
	task.AssignedWorkerID = ""
	task.CompletedAt = time.Now()

	if task.JobType == "SERVICE" {
		delete(kv.services, taskID)
		log.Printf("[registry] 🚫 API Gateway Route Removed: /proxy/%s", taskID)
	}

	log.Printf("[store] 💀 Task %s has been marked as KILLED", taskID)
}

func (kv *KVStore) HandleMAPFailure(taskID, ownerWorkerID string) {
	kv.mu.Lock()
	// REMOVED: defer kv.mu.Unlock()

	task, ok := kv.tasks[taskID]
	if !ok {
		//  HYBRID QUEUE SHIM: Mark the overarching Job as FAILED if the ID matches.
		if job, jobOk := kv.jobs[taskID]; jobOk {
			if job.Status == JobRestarting {
				log.Printf("[store] 🛡️ Ignoring failure for %s (Job is Restarting)", taskID)
			} else {
				job.Status = JobFailed
				job.CompletedAt = time.Now()
				log.Printf("[store] ❌ Job %s marked as FAILED (Hybrid Shim)", taskID)

				//  SYNC TO DB: Persistence Ledger
				payload, _ := json.Marshal(job)
				db.SyncJob(context.Background(), taskID, job.UserID, int(JobFailed), string(payload), nil, job.JobType)
			}
		}
		kv.mu.Unlock()
		return
	}

	//  HYBRID QUEUE SHIM: If this is a SERVICE task, check heartbeats before failing.
	// Services are persistent; they should only fail if they exhaust retries
	// OR if they are genuinely lost.
	// Note: job lookup is handled below in the RecordMAPFailed path.

	if ownerWorkerID != "" && task.AssignedWorkerID != ownerWorkerID {
		log.Printf("[fail] task %s no longer owned by %s (current: %q) — skipping", taskID, ownerWorkerID, task.AssignedWorkerID)
		kv.mu.Unlock()
		return
	}

	if task.JobID == "" {
		//  EAGER RESOURCE RELEASE: Standalone failure
		if task.AssignedWorkerID != "" {
			if worker, exists := kv.workers[task.AssignedWorkerID]; exists {
				worker.AvailCPU += task.RequiredCPU
				worker.AvailRAM += task.RequiredRAM
				log.Printf("[reclaim] 🔋 Recovered %.1f CPU, %d MB RAM from %s (Task %s Failed)", task.RequiredCPU, task.RequiredRAM, task.AssignedWorkerID, taskID)
			}
		}

		task.Status = StatusFailed
		task.AssignedWorkerID = ""
		task.CompletedAt = time.Now()
		log.Printf("[fail] standalone task %s failed (no retry)", taskID)
		kv.mu.Unlock()
		return
	}

	//  EAGER RESOURCE RELEASE: Release regardless of retry
	if task.AssignedWorkerID != "" {
		if worker, exists := kv.workers[task.AssignedWorkerID]; exists {
			worker.AvailCPU += task.RequiredCPU
			worker.AvailRAM += task.RequiredRAM
			log.Printf("[reclaim] 🔋 Recovered %.1f CPU, %d MB RAM from %s (Task %s Retrying/Failed)", task.RequiredCPU, task.RequiredRAM, task.AssignedWorkerID, taskID)
		}
	}

	task.RetryCount++
	if task.RetryCount <= task.MaxRetries {
		task.AttemptNumber++
		task.Status = StatusPending
		task.AssignedWorkerID = ""
		kv.enqueueTask(task)
		log.Printf("[retry] task %s attempt %d/%d requeued", taskID, task.RetryCount, task.MaxRetries)
		kv.mu.Unlock()
		return
	}

	task.Status = StatusFailed
	task.AssignedWorkerID = ""
	task.CompletedAt = time.Now()
	jobID := task.JobID // Save the ID to use outside the lock
	log.Printf("[fail] task %s exhausted %d retries", taskID, task.MaxRetries)

	//  FIX 2: Release the lock BEFORE calling RecordMAPFailed to prevent deadlock
	kv.mu.Unlock()

	kv.RecordMAPFailed(jobID)
}

func (kv *KVStore) GetWorkerTaskIDs(workerID string) []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	var deadTaskIDs []string
	for id, task := range kv.tasks {
		if task.Status == StatusRunning && task.AssignedWorkerID == workerID {
			deadTaskIDs = append(deadTaskIDs, id)
		}
	}
	return deadTaskIDs
}

// SweepTerminalState safely removes tasks and jobs that have been completed or
// failed for longer than the given TTL, preventing unbounded memory growth.
func (kv *KVStore) SweepTerminalState(ttl time.Duration) (int, int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	now := time.Now()
	tasksFreed, jobsFreed := 0, 0

	for id, job := range kv.jobs {
		if (job.Status == JobCompleted || job.Status == JobFailed) && !job.CompletedAt.IsZero() {
			if now.Sub(job.CompletedAt) > ttl {
				delete(kv.jobs, id)
				jobsFreed++
			}
		}
	}

	for id, task := range kv.tasks {
		if (task.Status == StatusCompleted || task.Status == StatusFailed) && !task.CompletedAt.IsZero() {
			if now.Sub(task.CompletedAt) > ttl {
				delete(kv.tasks, id)
				tasksFreed++
			}
		}
	}

	return tasksFreed, jobsFreed
}

// removeFromQueue removes a single taskID from the named queue in O(n).
// Used by ApplyReplayEvent (Bug 1 fix) and internally by PopBestFitTask.
func (kv *KVStore) removeFromQueue(queueName string, taskID string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	q := kv.queues[queueName]
	for i, id := range q {
		if id == taskID {
			kv.queues[queueName] = append(q[:i], q[i+1:]...)
			return
		}
	}
}

func (kv *KVStore) enqueueTask(task *Task) {
	qName := task.QueueName
	if qName == "" {
		qName = "default"
	}
	kv.queues[qName] = append(kv.queues[qName], task.ID)
}

func (kv *KVStore) RecordMAPComplete(jobID string, chunkIndex int, resultURI string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	job, ok := kv.jobs[jobID]
	if !ok {
		return fmt.Errorf("job %s not found", jobID)
	}
	if job.Status != JobRunning {
		return fmt.Errorf("job %s is not running (status: %v)", jobID, job.Status)
	}

	job.ResultURIs[chunkIndex] = resultURI
	job.PendingMAPs--
	log.Printf("[job] %s MAP chunk %d complete (pending: %d)", jobID, chunkIndex, job.PendingMAPs)

	if job.PendingMAPs == 0 {
		if job.FailedMAPs > 0 {
			job.Status = JobFailed
			job.CompletedAt = time.Now()
			log.Printf("[job] %s failed — %d MAP(s) exhausted retries", jobID, job.FailedMAPs)
		} else {
			// Note: We don't set JobCompleted yet if we have a GATHER stage.
			// However, if there is no GATHER, we should finish it.
			if job.GatherScriptPath == "" {
				job.Status = JobCompleted
				job.CompletedAt = time.Now()
				log.Printf("[job] %s fully completed (No GATHER script required)", jobID)
				return nil
			}

			// 1. Gather all the MAP outputs
			orderedURIs := make([]string, job.TotalMAPs)
			for i := 0; i < job.TotalMAPs; i++ {
				orderedURIs[i] = job.ResultURIs[i]
			}

			// 2.  DYNAMICALLY BUILD THE POLYGLOT GATHER TASK
			gatherPayload := protocol.Payload{
				JobType:        "GATHER",
				JobID:          jobID,
				InputURIs:      orderedURIs,
				UserScriptPath: job.GatherScriptPath, // Uses the script path passed by the client!
			}
			rawGatherPayload, _ := json.Marshal(gatherPayload)

			gatherTask := &Task{
				ID:          fmt.Sprintf("%s-gather", jobID),
				JobID:       jobID,
				JobType:     "GATHER",
				QueueName:   "default",
				Payload:     string(rawGatherPayload),
				Status:      StatusPending,
				RequiredCPU: 1,
				RequiredRAM: 512,
			}

			// 3. Inject it into the system
			kv.tasks[gatherTask.ID] = gatherTask
			kv.enqueueTask(gatherTask)
			log.Printf("[job] %s complete — dynamically enqueuing GATHER task %s", jobID, gatherTask.ID)
		}
	}
	return nil
}

func (kv *KVStore) RecordMAPFailed(jobID string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	job, ok := kv.jobs[jobID]
	if !ok {
		return
	}

	if job.Status == JobRestarting {
		log.Printf("[job] 🛡️ Ignoring RecordMAPFailed for %s (Job is Restarting)", jobID)
		return
	}

	// Guard: if PendingMAPs is already 0, this failure came from the GATHER
	// stage (not a MAP stage). Mark the job failed without decrementing.
	if job.PendingMAPs <= 0 {
		job.Status = JobFailed
		job.CompletedAt = time.Now()
		log.Printf("[job] %s failed at GATHER stage", jobID)
		return
	}

	job.FailedMAPs++
	job.PendingMAPs--

	if job.PendingMAPs == 0 {
		job.Status = JobFailed
		job.CompletedAt = time.Now()
		log.Printf("[job] %s failed — %d MAP task(s) exhausted retries", jobID, job.FailedMAPs)
	}
}

// Queue introspection

func (kv *KVStore) QueueDepth(queueName string) int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return len(kv.queues[queueName])
}

// Worker registry

func (kv *KVStore) RegisterWorker(id string, ip string, totalCPU float64, totalRAM int, availCPU float64, availRAM int, occupied map[int]string, activeTaskIDs []string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	existing := kv.workers[id]
	w := &Worker{
		ID:             id,
		IP:             ip, //  CAPTURE IP
		TotalCPU:       totalCPU,
		TotalRAM:       totalRAM,
		AvailCPU:       availCPU,
		AvailRAM:       availRAM,
		OccupiedPorts:  occupied,
		ActiveTaskIDs:  activeTaskIDs,
		LastLoggedSkip: make(map[string]time.Time),
	}
	if existing != nil {
		w.LastHeartbeat = existing.LastHeartbeat
	}
	kv.workers[id] = w
}

func (kv *KVStore) GetWorker(workerID string) (Worker, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	w, ok := kv.workers[workerID]
	if !ok {
		return Worker{}, false
	}
	return *w, true
}

func (kv *KVStore) UpdateWorkerHeartbeat(workerID string, ip string, availCPU float64, availRAM int, occupied map[int]string, activeTaskIDs []string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w, ok := kv.workers[workerID]
	if !ok {
		w = &Worker{ID: workerID, IP: ip}
		kv.workers[workerID] = w
	}
	w.IP = ip //  UPDATE IP
	w.AvailCPU = availCPU
	w.AvailRAM = availRAM
	w.OccupiedPorts = occupied
	w.ActiveTaskIDs = activeTaskIDs
	w.LastHeartbeat = time.Now()

	//  RECONCILIATION: Zombie Cleanup
	// If the Brain thinks a task is Running on this worker, but the worker didn't report it...
	// it's a "Zombie" (e.g., container crashed or worker rebooted).
	// HOWEVER: if the worker's OccupiedPorts confirm the job's container is alive,
	// it's NOT a zombie — the worker may just not have fully re-adopted it in-process yet.

	// Build a set of job IDs that have an occupied port (confirmed alive by Docker)
	occupiedJobIDs := make(map[string]bool)
	for _, jobID := range occupied {
		occupiedJobIDs[jobID] = true
	}

	for taskID, t := range kv.tasks {
		if t.AssignedWorkerID == workerID && t.Status == StatusRunning {
			//  GRACE PERIOD: Don't reclaim if the task was assigned/updated in the last 45 seconds.
			// This gives the worker time to receive the task and boot the container before the next heartbeat.
			if time.Since(t.LastStatusUpdate) < 45*time.Second {
				continue
			}

			found := false
			for _, activeID := range activeTaskIDs {
				if activeID == taskID {
					found = true
					break
				}
			}

			if !found {
				// Before marking as zombie, check if the container is still running
				// via the worker's OccupiedPorts. The task's JobID maps to the port.
				if t.JobID != "" && occupiedJobIDs[t.JobID] {
					// Container is confirmed alive by Docker — NOT a zombie.
					// The worker's Go process just hasn't re-adopted it in activeDaemons yet.
					continue
				}

				log.Printf("[reconcile] 🧟 Zombie Task Found: %s on %s. Worker did not report it (and grace period expired). Reclaiming resources.", taskID, workerID)
				t.Status = StatusPending
				t.AssignedWorkerID = ""
				// Resources will be naturally reclaimed because the worker's heartbeat already set the physical AvailCPU/RAM.
			}
		}
	}
}

//	NEW: THE ADOPTIVE RE-ATTACHMENT ENGINE
//
// This method reconcile's the worker's reported Docker state with the Brain's task list.
// If a daemon is running in Docker but Pending in the Brain, we re-attach it!
func (kv *KVStore) ReconcileOccupiedState(workerID string, occupied map[int]string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for port, jobID := range occupied {
		task, exists := kv.tasks[jobID]
		if !exists {
			continue
		}

		//  Get the worker to fetch its IP
		w, ok := kv.workers[workerID]
		if !ok {
			continue
		}

		// Only re-attach if it's a Service and NOT already running exactly where it should be.
		//  CREATION GUARD: Do NOT adopt/rescue a task that was created in the last 60 seconds.
		// This prevents the Brain from hijacking a "ghost" container that is currently being killed during a push.
		if task.JobType == protocol.JobTypeService && (task.Status == StatusPending || task.Status == StatusFailed) {
			if time.Since(task.CreatedAt) < 60*time.Second {
				// Too young — let the worker kill the old one and start fresh.
				continue
			}

			log.Printf("[attachment] ⚡ Re-attaching Job %s to worker %s (Port %d already active)", jobID, workerID, port)

			// 1. Move from Queue to Running
			kv.removeFromQueueInternal(task.QueueName, task.ID)
			task.Status = StatusRunning
			task.AssignedWorkerID = workerID
			task.WorkerIP = w.IP               //  Save captured IP
			task.LastStatusUpdate = time.Now() //  RESET GRACE PERIOD

			// 2. Restore the parent job's status to Running
			if job, ok := kv.jobs[jobID]; ok {
				job.Status = JobRunning
				job.AssignedWorkerID = workerID
				job.CompletedAt = time.Time{} // Clear so sweeper doesn't delete it
			}

			// 3. Restore Routing
			address := fmt.Sprintf("%s:%d", w.IP, port)
			kv.services[jobID] = address
			log.Printf("[attachment] 🌐 Route Restored Index-Free: /proxy/%s -> %s", jobID, address)
		}
	}
}

// Internal version of removeFromQueue that assumes lock is held
func (kv *KVStore) removeFromQueueInternal(queueName string, taskID string) {
	if queueName == "" {
		queueName = "default"
	}
	q := kv.queues[queueName]
	for i, id := range q {
		if id == taskID {
			kv.queues[queueName] = append(q[:i], q[i+1:]...)
			return
		}
	}
}

func (kv *KVStore) GetDeadWorkers(deadline time.Duration) []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	var dead []string
	cutoff := time.Now().Add(-deadline)
	for id, w := range kv.workers {
		if !w.LastHeartbeat.IsZero() && w.LastHeartbeat.Before(cutoff) {
			dead = append(dead, id)
		}
	}
	return dead
}

func (kv *KVStore) RemoveWorker(workerID string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.workers, workerID)
}

func (kv *KVStore) SetJobStatus(jobID string, status JobStatus) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if job, ok := kv.jobs[jobID]; ok {
		job.Status = status
		log.Printf("[store] 🔄 Job %s status updated to %v", jobID, status)
	}
}

// Snapshot compaction

type StateSnapShot struct {
	Tasks  map[string]*Task    `json:"tasks"`
	Queues map[string][]string `json:"queues"`
	Jobs   map[string]*JobMeta `json:"jobs"`
}

func (kv *KVStore) ExportSnapshot() []byte {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	snap := StateSnapShot{
		Tasks:  kv.tasks,
		Queues: kv.queues,
		Jobs:   kv.jobs,
	}
	data, _ := json.Marshal(snap)
	return data
}

func (kv *KVStore) ImportSnapshot(data []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var snap StateSnapShot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	kv.tasks = snap.Tasks
	kv.queues = snap.Queues
	kv.jobs = snap.Jobs

	for _, t := range kv.tasks {
		if t.Status == StatusRunning && t.AssignedWorkerID != "" {
			if _, exists := kv.workers[t.AssignedWorkerID]; !exists {
				kv.workers[t.AssignedWorkerID] = &Worker{
					ID:            t.AssignedWorkerID,
					LastHeartbeat: time.Now(),
				}
			}
		}
	}
	return nil
}

func (kv *KVStore) RegisterMLJob(jobID string, totalWorkers int, scriptPath, gatherPath string, buildCmd, runCmd, rootDir string, userID string, cpu float64, ram int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  MIGRATION: Sanitize identity
	safeUserID := userID
	if safeUserID == "" {
		safeUserID = "00000000-0000-0000-0000-000000000000"
	}

	kv.jobs[jobID] = &JobMeta{
		JobID:            jobID,
		JobType:          "ML_TRAIN",
		Status:           JobProvisioning,
		TotalMAPs:        totalWorkers,
		PendingMAPs:      totalWorkers,
		RequiredCPU:      cpu,
		RequiredRAM:      ram,
		ResultURIs:       make(map[int]string),
		TopologyMap:      make(map[string]string),
		UserScriptPath:   scriptPath,
		GatherScriptPath: gatherPath,
		BuildCommand:     buildCmd,
		RunCommand:       runCmd,
		RootDirectory:    rootDir,
		UserID:           safeUserID,
		CreatedAt:        time.Now(),
	}

	//  SYNC TO DB: Persistence Ledger
	payload, _ := json.Marshal(kv.jobs[jobID])
	db.SyncJob(context.Background(), jobID, safeUserID, int(JobProvisioning), string(payload), nil, "ML_TRAIN")
}

func (kv *KVStore) UpdateJobTopology(jobID string, role string, address string) (*JobMeta, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	job, exists := kv.jobs[jobID]
	if !exists || (job.Status != JobProvisioning && job.Status != JobRunning) {
		return nil, false
	}

	job.TopologyMap[role] = address
	job.Status = JobRunning

	//  SYNC TO DB: Persistence Ledger
	payload, _ := json.Marshal(job)
	db.SyncJob(context.Background(), jobID, job.UserID, int(JobRunning), string(payload), nil, job.JobType)

	return job, true
}

// service functions (Hosting)
func (kv *KVStore) RegisterServiceJob(jobID string, cpu float64, ram int, userID string, payload string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  Skip if already exists (might be a replay of a known job)
	if _, exists := kv.jobs[jobID]; exists {
		return
	}

	kv.jobs[jobID] = &JobMeta{
		JobID:           jobID,
		JobType:         protocol.JobTypeService,
		Status:          JobProvisioning,
		TotalMAPs:       1,
		PendingMAPs:     1,
		RequiredCPU:     cpu,
		RequiredRAM:     ram,
		ResultURIs:      make(map[int]string),
		UserID:          userID,
		OriginalPayload: payload,
		CreatedAt:       time.Now(),
	}

	log.Printf("[store] 📝 Job %s registered as SERVICE (CPU: %.1f, RAM: %d, User: %s)", jobID, cpu, ram, userID)

	//  SYNC TO DB: Persistence Ledger
	metaPayload, _ := json.Marshal(kv.jobs[jobID])
	db.SyncJob(context.Background(), jobID, userID, int(JobProvisioning), string(metaPayload), nil, protocol.JobTypeService)
}

func (kv *KVStore) RegisterServiceEndpoint(jobID, address string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.services[jobID] = address
	log.Printf("[registry] 🌐 API Gateway Route Added: /proxy/%s -> %s", jobID, address)

	//  SYNC STATUS: Update job to Running when endpoint is ready
	if job, ok := kv.jobs[jobID]; ok {
		job.Status = JobRunning
		log.Printf("[store] ⚡ Job %s marked as RUNNING in database (Endpoint Ready)", jobID)

		payload, _ := json.Marshal(job)
		db.SyncJob(context.Background(), jobID, job.UserID, int(JobRunning), string(payload), nil, job.JobType)
	}
}

func (kv *KVStore) GetServiceEndpoint(jobID string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	address, exists := kv.services[jobID]
	return address, exists
}

func (kv *KVStore) RemoveServiceEndpoint(jobID string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.services, jobID)
	log.Printf("[registry] 🚫 API Gateway Route Removed: /proxy/%s", jobID)
}

// RegisterJobWithPayload creates a JobMeta and stores the original payload for restart.
func (kv *KVStore) RegisterJobWithPayload(jobID string, jobType string, totalMAPs int, scriptPath string, gatherPath string, buildCmd, runCmd, rootDir string, originalPayload string, userID string, cpu float64, ram int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  MIGRATION: Sanitize identity
	safeUserID := userID
	if safeUserID == "" {
		safeUserID = "00000000-0000-0000-0000-000000000000"
	}

	kv.jobs[jobID] = &JobMeta{
		JobID:            jobID,
		JobType:          jobType,
		TotalMAPs:        totalMAPs,
		PendingMAPs:      totalMAPs,
		Status:           JobProvisioning, // Start as Provisioning until Ready
		ResultURIs:       make(map[int]string),
		UserScriptPath:   scriptPath,
		GatherScriptPath: gatherPath,
		BuildCommand:     buildCmd,
		RunCommand:       runCmd,
		RootDirectory:    rootDir,
		OriginalPayload:  originalPayload,
		UserID:           safeUserID,
		RequiredCPU:      cpu,
		RequiredRAM:      ram,
		CreatedAt:        time.Now(),
	}

	log.Printf("[store] 📝 Job %s registered with payload (Type: %s, CPU: %.1f, RAM: %d, User: %s)", jobID, jobType, cpu, ram, userID)

	//  SYNC TO DB: Persistence Ledger
	payload, _ := json.Marshal(kv.jobs[jobID])
	db.SyncJob(context.Background(), jobID, safeUserID, int(JobProvisioning), string(payload), nil, jobType)
}

// GetDaemonTaskForJob finds the daemon task associated with a job.
func (kv *KVStore) GetDaemonTaskForJob(jobID string) (Task, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	task, ok := kv.tasks[jobID]
	if !ok {
		return Task{}, false
	}
	return *task, true
}

// PrepareJobForRestart removes the old job and task state, returning the original payload.
// GetJobPayload returns the original deployment payload for a job WITHOUT deleting it.
func (kv *KVStore) GetJobPayload(jobID string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	job, ok := kv.jobs[jobID]
	if !ok {
		return "", false
	}
	return job.OriginalPayload, true
}

func (kv *KVStore) PrepareJobForRestart(jobID string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	job, exists := kv.jobs[jobID]
	if !exists {
		return "", false
	}

	originalPayload := job.OriginalPayload

	// Remove all tasks for this job
	for id, t := range kv.tasks {
		if t.JobID == jobID {
			delete(kv.tasks, id)
		}
	}

	// Remove the job itself
	delete(kv.jobs, jobID)

	// Remove service endpoint
	delete(kv.services, jobID)

	log.Printf("[store] 🔄 Job %s cleaned up for restart", jobID)
	return originalPayload, originalPayload != ""
}

// RemoveJob removes a job and all its tasks from memory.
func (kv *KVStore) RemoveJob(jobID string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for id, t := range kv.tasks {
		if t.JobID == jobID {
			delete(kv.tasks, id)
		}
	}

	delete(kv.jobs, jobID)
	delete(kv.services, jobID)
	log.Printf("[store] 💀 Job %s and all related tasks removed", jobID)
}

// GetJobLiveTasks returns a list of TaskIDs for a job that are currently in a non-terminal state.
func (kv *KVStore) GetJobLiveTasks(jobID string) []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	var live []string
	for id, t := range kv.tasks {
		if t.JobID == jobID && (t.Status == StatusRunning || t.Status == StatusProvisioning || t.Status == StatusPending) {
			live = append(live, id)
		}
	}
	return live
}

// RecoverLostServiceJob reconstructs the JobMeta and Task if they were lost during a Brain restart
// (due to Raft replay skipping) but the Muscle worker reported them as still physically running.
// RecoverLostServiceJob is used when the Brain discovers a running container that it doesn't remember.
func (kv *KVStore) RecoverLostServiceJob(jobID string, cpu float64, ram int, workerID string, workerIP string, originalSubmitBytes string, userID string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Only recover if missing
	if _, exists := kv.jobs[jobID]; !exists {
		kv.jobs[jobID] = &JobMeta{
			JobID:            jobID,
			JobType:          protocol.JobTypeService,
			Status:           JobRunning,
			TotalMAPs:        1,
			PendingMAPs:      0,
			RequiredCPU:      cpu, //  Recovered from worker
			RequiredRAM:      ram, //  Recovered from worker
			ResultURIs:       make(map[int]string),
			UserScriptPath:   "Adopted Service (Recovered)",
			AssignedWorkerID: workerID,
			OriginalPayload:  originalSubmitBytes,
			UserID:           userID,
			CreatedAt:        time.Now(),
		}
		log.Printf("[store] ♻️ Adoptive Recovery: Recreated JobMeta for %s", jobID)

		//  SYNC TO DB: Persistence Ledger
		payload, _ := json.Marshal(kv.jobs[jobID])
		db.SyncJob(context.Background(), jobID, userID, int(JobRunning), string(payload), nil, protocol.JobTypeService)
	}

	if _, exists := kv.tasks[jobID]; !exists {
		task := &Task{
			ID:               jobID,
			JobID:            jobID,
			JobType:          protocol.JobTypeService,
			QueueName:        "default",
			Status:           StatusRunning,
			RequiredCPU:      cpu,
			RequiredRAM:      ram,
			AssignedWorkerID: workerID,
			WorkerIP:         workerIP,
			CreatedAt:        time.Now(),
			UserID:           userID,
		}
		kv.tasks[jobID] = task
		log.Printf("[store] ♻️ Adoptive Recovery: Recreated Task %s", jobID)

		// Wait, we need to put it into active assignment map/lists if necessary!
		// However, it's just a map in `kv.tasks`, so it's already recorded.
	}
}
