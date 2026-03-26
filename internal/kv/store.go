package kv

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
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
	JobRunning JobStatus = iota
	JobCompleted
	JobFailed
)

type Task struct {
	ID               string
	JobID            string // empty for Version A standalone tasks
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

type JobMeta struct {
	mu          sync.Mutex // guards PendingMAPs, FailedMAPs, Status — not the map itself
	JobID       string
	TotalMAPs   int
	PendingMAPs int
	FailedMAPs  int
	GatherTask  *Task
	Status      JobStatus
	ResultURIs  map[int]string // chunkIndex → result URI, built as MAPs complete
}

type KVStore struct {
	tasks   map[string]*Task
	tasksMu sync.RWMutex

	workers   map[string]*Worker
	workersMu sync.RWMutex

	// queues holds task IDs in FIFO order (oldest first).
	queues   map[string][]string
	queuesMu sync.Mutex

	// jobs tracks in-flight SCATTER jobs.
	// jobsMu guards the map; per-job state is guarded by JobMeta.mu.
	jobs   map[string]*JobMeta
	jobsMu sync.RWMutex
}

func NewKVStore() *KVStore {
	return &KVStore{
		tasks:   make(map[string]*Task),
		workers: make(map[string]*Worker),
		queues:  make(map[string][]string),
		jobs:    make(map[string]*JobMeta),
	}
}

// Task operations

// SetTask stores a task and enqueues it if StatusPending.
func (kv *KVStore) SetTask(task *Task) {
	kv.tasksMu.Lock()
	kv.tasks[task.ID] = task
	kv.tasksMu.Unlock()

	if task.Status == StatusPending {
		kv.enqueueTask(task)
	}
}

// PopBestFitTask finds the largest pending task fitting within availCPU/RAM.
// Locking order: queuesMu → tasksMu.RLock → queuesMu (commit) → tasksMu.RLock (verify).
// Queue removal is the exclusive assignment gate — no double-assignment is possible.
func (kv *KVStore) PopBestFitTask(queueName string, availCPU, availRAM int) (Task, bool) {
	// Step 1 — snapshot the queue.
	kv.queuesMu.Lock()
	ids := kv.queues[queueName]
	if len(ids) == 0 {
		kv.queuesMu.Unlock()
		return Task{}, false
	}
	snapshot := make([]string, len(ids))
	copy(snapshot, ids)
	kv.queuesMu.Unlock()

	// Step 2 — scan for best-fit candidate (read-only).
	kv.tasksMu.RLock()
	bestIdx := -1
	bestScore := -1
	for i, id := range snapshot {
		t, ok := kv.tasks[id]
		if !ok || t.Status != StatusPending {
			continue
		}
		cpu, ram := t.RequiredCPU, t.RequiredRAM
		if cpu <= 0 {
			cpu = 1
		}
		if ram <= 0 {
			ram = 1
		}
		if cpu <= availCPU && ram <= availRAM {
			score := cpu + ram
			if score > bestScore {
				bestScore = score
				bestIdx = i
			}
		}
	}
	kv.tasksMu.RUnlock()

	if bestIdx == -1 {
		return Task{}, false
	}

	targetID := snapshot[bestIdx]

	// Step 3 — atomically remove from live queue.
	kv.queuesMu.Lock()
	current := kv.queues[queueName]
	removed := false
	for i, id := range current {
		if id == targetID {
			kv.queues[queueName] = append(current[:i], current[i+1:]...)
			removed = true
			break
		}
	}
	kv.queuesMu.Unlock()

	if !removed {
		return Task{}, false // another worker grabbed it between steps 2 and 3
	}

	// Step 4 — return a copy.
	kv.tasksMu.RLock()
	t, ok := kv.tasks[targetID]
	kv.tasksMu.RUnlock()

	if !ok || t.Status != StatusPending {
		log.Printf("[pop] task %s no longer pending after dequeue (status=%v) — discarding", targetID, t.Status)
		return Task{}, false
	}

	return *t, true
}

// GetTask returns a point-in-time copy of a task by ID.
func (kv *KVStore) GetTask(id string) (Task, bool) {
	kv.tasksMu.RLock()
	defer kv.tasksMu.RUnlock()
	task, ok := kv.tasks[id]
	if !ok {
		return Task{}, false
	}
	return *task, true
}

// TransitionTaskState moves a task between states under tasksMu.
// Used by the Brain to mark Running on assignment, or revert to Pending on
// send failure. The enqueue side-effect runs outside the lock.
//
// Completion → CompleteTask. Failure → HandleMAPFailure.
func (kv *KVStore) TransitionTaskState(id string, from, to TaskStatus, workerID string) error {
	var needsEnqueue *Task

	kv.tasksMu.Lock()
	task, ok := kv.tasks[id]
	if !ok {
		kv.tasksMu.Unlock()
		return fmt.Errorf("task %s not found", id)
	}
	if task.Status != from {
		kv.tasksMu.Unlock()
		return fmt.Errorf("expected status %v, got %v", from, task.Status)
	}

	task.Status = to
	switch to {
	case StatusRunning:
		task.AssignedWorkerID = workerID
	case StatusPending:
		task.AssignedWorkerID = ""
		needsEnqueue = task
	case StatusCompleted, StatusFailed:
		task.AssignedWorkerID = ""
		task.CompletedAt = time.Now()
	}
	kv.tasksMu.Unlock()

	if needsEnqueue != nil {
		kv.enqueueTask(needsEnqueue)
	}
	return nil
}

// CompleteTask marks a Version A standalone task completed with its result.
// Version B MAP completions go through RecordMAPComplete instead.
func (kv *KVStore) CompleteTask(taskID string, result string) error {
	kv.tasksMu.Lock()
	defer kv.tasksMu.Unlock()

	task, ok := kv.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	if task.Status != StatusRunning {
		return fmt.Errorf("task %s is not running (status: %v)", taskID, task.Status)
	}

	task.Result = result
	task.Status = StatusCompleted
	task.CompletedAt = time.Now()
	return nil
}

// HandleMAPFailure is the single failure entry point for both explicit worker
// failures (OpTaskFailed) and silent node deaths (heartbeat watcher).
//
// ownerWorkerID: when non-empty, the task is only processed if this worker still
// owns it. This closes the race window where a dead-worker requeue races with a
// new assignment to a healthy worker.
//
// BUG 2 FIX: Version A standalone tasks (JobID == "") fail immediately — no retry.
// BUG 1 FIX: Status is set to StatusPending before re-enqueuing.
func (kv *KVStore) HandleMAPFailure(taskID, ownerWorkerID string) {
	var needsRequeue *Task
	var jobIDToFail string
	var retryCount, maxRetries int

	kv.tasksMu.Lock()
	task, ok := kv.tasks[taskID]
	if !ok {
		kv.tasksMu.Unlock()
		return
	}

	// Ownership check: if a specific worker is required and it's no longer the owner, skip.
	if ownerWorkerID != "" && task.AssignedWorkerID != ownerWorkerID {
		kv.tasksMu.Unlock()
		log.Printf("[fail] task %s no longer owned by %s (current owner: %q) — skipping",
			taskID, ownerWorkerID, task.AssignedWorkerID)
		return
	}

	// Version A: fail immediately, no retry.
	if task.JobID == "" {
		task.Status = StatusFailed
		task.AssignedWorkerID = ""
		task.CompletedAt = time.Now()
		kv.tasksMu.Unlock()
		log.Printf("[fail] standalone task %s failed (no retry)", taskID)
		return
	}

	// Version B MAP task: attempt retry.
	task.RetryCount++
	retryCount = task.RetryCount
	maxRetries = task.MaxRetries

	if task.RetryCount <= task.MaxRetries {
		task.AttemptNumber++
		task.Status = StatusPending // BUG 1 FIX: must be Pending before enqueue
		task.AssignedWorkerID = ""
		needsRequeue = task
	} else {
		task.Status = StatusFailed
		task.AssignedWorkerID = ""
		task.CompletedAt = time.Now()
		jobIDToFail = task.JobID
	}

	kv.tasksMu.Unlock()

	if needsRequeue != nil {
		kv.enqueueTask(needsRequeue)
		log.Printf("[retry] task %s attempt %d/%d requeued", taskID, retryCount, maxRetries)
		return
	}

	log.Printf("[fail] task %s exhausted %d retries", taskID, maxRetries)
	if jobIDToFail != "" {
		kv.RecordMAPFailed(jobIDToFail)
	}
}

// RequeueDeadWorkerTasks routes tasks from a dead worker through HandleMAPFailure.
// The ownership check inside HandleMAPFailure is the authoritative gate — we do NOT
// pre-clear AssignedWorkerID here to avoid a race where a newly re-assigned task
// is mistakenly re-processed.
func (kv *KVStore) RequeueDeadWorkerTasks(workerID string) {
	kv.tasksMu.RLock()
	var deadTaskIDs []string
	for id, task := range kv.tasks {
		if task.Status == StatusRunning && task.AssignedWorkerID == workerID {
			deadTaskIDs = append(deadTaskIDs, id)
		}
	}
	kv.tasksMu.RUnlock()

	for _, taskID := range deadTaskIDs {
		kv.HandleMAPFailure(taskID, workerID)
	}

	if len(deadTaskIDs) > 0 {
		log.Printf("[watcher] routed %d tasks from dead worker %s through failure handler",
			len(deadTaskIDs), workerID)
	}
}

// Internal helpers

// enqueueTask appends a task ID to its queue. Never holds tasksMu when called.
func (kv *KVStore) enqueueTask(task *Task) {
	qName := task.QueueName
	if qName == "" {
		qName = "default"
	}
	kv.queuesMu.Lock()
	kv.queues[qName] = append(kv.queues[qName], task.ID)
	kv.queuesMu.Unlock()
}

// Job registry (Version B)

// RegisterJob stores the pre-staged GATHER task and initialises the job counter.
// The GATHER task sits in the tasks map with StatusPending but is NOT enqueued —
// it stays invisible to workers until RecordMAPComplete enqueues it.
func (kv *KVStore) RegisterJob(jobID string, totalMAPs int, gatherTask *Task) {
	kv.tasksMu.Lock()
	kv.tasks[gatherTask.ID] = gatherTask
	kv.tasksMu.Unlock()

	kv.jobsMu.Lock()
	defer kv.jobsMu.Unlock()

	kv.jobs[jobID] = &JobMeta{
		JobID:       jobID,
		GatherTask:  gatherTask,
		TotalMAPs:   totalMAPs,
		PendingMAPs: totalMAPs,
		Status:      JobRunning,
		ResultURIs:  make(map[int]string),
	}
}

// RecordMAPComplete decrements the job counter and enqueues the GATHER task
// when all MAPs finish successfully.
// Decrement + evaluate are atomic under job.mu (TOCTOU fix).
// The enqueue side-effect runs outside job.mu (contention minimisation).
func (kv *KVStore) RecordMAPComplete(jobID string, chunkIndex int, resultURI string) error {
	kv.jobsMu.RLock()
	job, ok := kv.jobs[jobID]
	kv.jobsMu.RUnlock()
	if !ok {
		return fmt.Errorf("job %s not found", jobID)
	}

	var taskToEnqueue *Task

	job.mu.Lock()

	if job.Status != JobRunning {
		job.mu.Unlock()
		return fmt.Errorf("job %s is not running (status: %v)", jobID, job.Status)
	}

	job.ResultURIs[chunkIndex] = resultURI
	job.PendingMAPs--

	if job.PendingMAPs == 0 {
		if job.FailedMAPs > 0 {
			job.Status = JobFailed
			log.Printf("[job] %s failed — %d MAP(s) exhausted retries", jobID, job.FailedMAPs)
		} else {
			job.Status = JobCompleted

			orderedURIs := make([]string, job.TotalMAPs)
			for i := 0; i < job.TotalMAPs; i++ {
				orderedURIs[i] = job.ResultURIs[i]
			}

			// Unmarshal the pre-staged GATHER payload to inject InputURIs.
			var payloadMap map[string]interface{}
			if err := json.Unmarshal([]byte(job.GatherTask.Payload), &payloadMap); err != nil {
				job.mu.Unlock()
				return fmt.Errorf("gather task payload unmarshal for job %s: %w", jobID, err)
			}
			payloadMap["input_uris"] = orderedURIs
			updated, err := json.Marshal(payloadMap)
			if err != nil {
				job.mu.Unlock()
				return fmt.Errorf("gather task payload marshal for job %s: %w", jobID, err)
			}

			// Hold tasksMu while mutating GatherTask.Payload: the same *Task pointer
			// is stored in kv.tasks and read by concurrent workers under tasksMu.RLock.
			kv.tasksMu.Lock()
			job.GatherTask.Payload = string(updated)
			kv.tasksMu.Unlock()

			taskToEnqueue = job.GatherTask
			log.Printf("[job] %s complete — enqueuing GATHER task %s", jobID, job.GatherTask.ID)
		}
	}

	job.mu.Unlock()

	if taskToEnqueue != nil {
		kv.enqueueTask(taskToEnqueue)
	}

	return nil
}

// RecordMAPFailed increments the failure counter and marks the job failed when the last pending MAP slot is consumed.
func (kv *KVStore) RecordMAPFailed(jobID string) {
	kv.jobsMu.RLock()
	job, ok := kv.jobs[jobID]
	kv.jobsMu.RUnlock()
	if !ok {
		return
	}

	job.mu.Lock()
	defer job.mu.Unlock()

	job.FailedMAPs++
	job.PendingMAPs--

	if job.PendingMAPs == 0 {
		job.Status = JobFailed
		log.Printf("[job] %s failed — %d MAP task(s) exhausted retries", jobID, job.FailedMAPs)
	}
}

// Queue introspection

// Worker registry

func (kv *KVStore) RegisterWorker(id string, totalCPU, totalRAM int) {
	kv.workersMu.Lock()
	defer kv.workersMu.Unlock()

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

func (kv *KVStore) UpdateWorkerHeartbeat(workerID string, availCPU, availRAM int) {
	kv.workersMu.Lock()
	defer kv.workersMu.Unlock()

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
	kv.workersMu.Lock()
	defer kv.workersMu.Unlock()

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
	kv.workersMu.Lock()
	defer kv.workersMu.Unlock()
	delete(kv.workers, workerID)
}
