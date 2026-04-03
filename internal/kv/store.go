package kv

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/akashtripathi12/brain-and-muscle/internal/wal"
)

type TaskStatus int
type JobStatus int

const (
	StatusPending TaskStatus = iota
	StatusRunning
	StatusCompleted
	StatusFailed
)

const (
	JobProvisioning JobStatus = iota
	JobRunning
	JobCompleted
	JobFailed
)

type Task struct {
	ID               string
	JobID            string
	JobType          string
	QueueName        string
	Payload          string
	Result           string
	Status           TaskStatus
	RequiredCPU      int
	RequiredRAM      int
	AssignedWorkerID string
	AttemptNumber    int
	MaxRetries       int
	RetryCount       int
	CreatedAt        time.Time
	CompletedAt      time.Time
}

type Worker struct {
	ID            string
	TotalCPU      int
	TotalRAM      int
	AvailCPU      int
	AvailRAM      int
	LastHeartbeat time.Time
}

type Job struct {
	ID           string
	Type         string
	Status       string
	TotalWorkers int
	TopologyMap  map[string]string
}

type JobMeta struct {
	JobID            string
	JobType          string
	TotalMAPs        int
	PendingMAPs      int
	FailedMAPs       int
	Status           JobStatus
	ResultURIs       map[int]string
	TopologyMap      map[string]string
	UserScriptPath   string
	GatherScriptPath string
	CompletedAt      time.Time
}

type KVStore struct {
	mu      sync.RWMutex
	tasks   map[string]*Task
	workers map[string]*Worker
	queues  map[string][]string
	jobs    map[string]*JobMeta
}

func NewKVStore() *KVStore {
	return &KVStore{
		mu:      sync.RWMutex{},
		tasks:   make(map[string]*Task),
		workers: make(map[string]*Worker),
		queues:  make(map[string][]string),
		jobs:    make(map[string]*JobMeta),
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

	case wal.EventTaskFailed:
		kv.HandleMAPFailure(entry.TaskID, entry.WorkerID)

	case wal.EventJobRegistered:
		var payload protocol.JobRegisteredPayload
		json.Unmarshal(entry.Payload, &payload)
		kv.RegisterJob(entry.JobID, payload.TotalMAPs, payload.UserScriptPath, payload.GatherScriptPath)
		for _, raw := range payload.MapTasks {
			var mapTask Task
			json.Unmarshal(raw, &mapTask)
			// SetTask handles queue insertion only for StatusPending tasks.
			kv.SetTask(&mapTask)
		}

	case wal.EventMAPCompleted:
		var payload protocol.MapCompletedPayload
		json.Unmarshal(entry.Payload, &payload)
		kv.RecordMAPComplete(entry.JobID, payload.ChunkIndex, payload.ResultURI)

	case wal.EventMAPFailed:
		kv.RecordMAPFailed(entry.JobID)
	}
}

// ─── Task operations ─────────────────────────────────────────────────────────

func (kv *KVStore) SetTask(task *Task) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.tasks[task.ID] = task
	if task.Status == StatusPending {
		kv.enqueueTask(task)
	}
}

// PopBestFitTask selects the highest-scoring pending task whose resource
// requirements fit within availCPU / availRAM.
func (kv *KVStore) PopBestFitTask(queueName string, availCPU, availRAM int) (Task, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ids := kv.queues[queueName]
	if len(ids) == 0 {
		return Task{}, false
	}

	// Guard: prevent division-by-zero in the normalised scoring below.
	if availCPU <= 0 {
		availCPU = 1
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

		if cpu <= availCPU && ram <= availRAM {
			// normalised score — fraction of available CPU + fraction of
			// available RAM. Maximum possible value is 2.0 (task fills node exactly).
			cpuScore := float64(cpu) / float64(availCPU)
			ramScore := float64(ram) / float64(availRAM)
			score := cpuScore + ramScore
			if score > bestScore {
				bestScore = score
				bestID = id
			}
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

	return *kv.tasks[bestID], true
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
		return fmt.Errorf("task %s not found", id)
	}
	if task.Status != from {
		return fmt.Errorf("expected status %v, got %v", from, task.Status)
	}

	task.Status = to
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
 
	task, ok := kv.tasks[taskID]
	if !ok {
		return
	}
 
	task.Status = StatusCompleted
	task.Result = result
	task.CompletedAt = time.Now()
 
	// 🚨 NEW: If this was a GATHER task, mark the parent Job as Completed too!
	if task.JobType == "GATHER" && task.JobID != "" {
		if job, ok := kv.jobs[task.JobID]; ok {
			job.Status = JobCompleted
			job.CompletedAt = time.Now()
			log.Printf("[job] %s fully completed (GATHER phase finished)", task.JobID)
		}
	}
}

func (kv *KVStore) HandleMAPFailure(taskID, ownerWorkerID string) {
	kv.mu.Lock()
	// REMOVED: defer kv.mu.Unlock()

	task, ok := kv.tasks[taskID]
	if !ok {
		kv.mu.Unlock()
		return
	}

	if ownerWorkerID != "" && task.AssignedWorkerID != ownerWorkerID {
		log.Printf("[fail] task %s no longer owned by %s (current: %q) — skipping", taskID, ownerWorkerID, task.AssignedWorkerID)
		kv.mu.Unlock()
		return
	}

	if task.JobID == "" {
		task.Status = StatusFailed
		task.AssignedWorkerID = ""
		task.CompletedAt = time.Now()
		log.Printf("[fail] standalone task %s failed (no retry)", taskID)
		kv.mu.Unlock()
		return
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

	// 🚨 FIX 2: Release the lock BEFORE calling RecordMAPFailed to prevent deadlock
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

// Job registry (Version B)

// RegisterJob creates the JobMeta and pre-stages the GATHER task.
func (kv *KVStore) RegisterJob(jobID string, totalMAPs int, scriptPath string, gatherPath string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.jobs[jobID] = &JobMeta{
		JobID:            jobID,
		JobType:          "MAP",
		TotalMAPs:        totalMAPs,
		PendingMAPs:      totalMAPs,
		Status:           JobRunning,
		ResultURIs:       make(map[int]string),
		UserScriptPath:   scriptPath,
		GatherScriptPath: gatherPath,
	}
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
			}

			// 1. Gather all the MAP outputs
			orderedURIs := make([]string, job.TotalMAPs)
			for i := 0; i < job.TotalMAPs; i++ {
				orderedURIs[i] = job.ResultURIs[i]
			}

			// 2. 🚨 DYNAMICALLY BUILD THE POLYGLOT GATHER TASK
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

func (kv *KVStore) RegisterWorker(id string, totalCPU, totalRAM int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	existing := kv.workers[id]
	w := &Worker{
		ID:       id,
		TotalCPU: totalCPU,
		TotalRAM: totalRAM,
		AvailCPU: totalCPU,
		AvailRAM: totalRAM,
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

func (kv *KVStore) UpdateWorkerHeartbeat(workerID string, availCPU, availRAM int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w, ok := kv.workers[workerID]
	if !ok {
		w = &Worker{ID: workerID}
		kv.workers[workerID] = w
	}
	w.AvailCPU = availCPU
	w.AvailRAM = availRAM
	w.LastHeartbeat = time.Now()
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

func (kv *KVStore) RegisterMLJob(jobID string, totalWorkers int, scriptPath, gatherPath string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.jobs[jobID] = &JobMeta{
		JobID:            jobID,
		JobType:          "ML_TRAIN",
		Status:           JobProvisioning,
		TotalMAPs:        totalWorkers,
		PendingMAPs:      totalWorkers,
		ResultURIs:       make(map[int]string),
		TopologyMap:      make(map[string]string),
		UserScriptPath:   scriptPath,
		GatherScriptPath: gatherPath,
	}
}

func (kv *KVStore) UpdateJobTopology(jobID string, role string, address string) (*JobMeta, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	job, exists := kv.jobs[jobID]
	if !exists || job.Status != JobProvisioning {
		return nil, false
	}

	job.TopologyMap[role] = address
	job.Status = JobRunning
	return job, true
}
