package kv

import (
	"encoding/json"
	"fmt"
	"log"
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
	JobRunning JobStatus = iota
	JobCompleted
	JobFailed
)

type Task struct {
	ID               string
	JobID            string
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
	JobID       string
	TotalMAPs   int
	PendingMAPs int
	FailedMAPs  int
	GatherTask  *Task
	Status      JobStatus
	ResultURIs  map[int]string
}

type KVStore struct {
	tasks   map[string]*Task
	workers map[string]*Worker
	queues  map[string][]string
	jobs    map[string]*JobMeta
}

func NewKVStore() *KVStore {
	return &KVStore{
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
		// During live operation, PopBestFitTask removes the task from
		// the queue BEFORE TransitionTaskState marks it Running. Replay skips
		// PopBestFitTask, so the task stays in kv.queues as a ghost entry forever.
		// We must explicitly dequeue here to match the live code path.
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
		// Replay restores the whole SCATTER job (job meta + all MAP tasks) from a
		// single WAL record, so there is no partial-replay window.
		var payload protocol.JobRegisteredPayload
		json.Unmarshal(entry.Payload, &payload)
		var gatherTask Task
		json.Unmarshal(payload.GatherTask, &gatherTask)
		kv.RegisterJob(entry.JobID, payload.TotalMAPs, &gatherTask)
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
	kv.tasks[task.ID] = task
	if task.Status == StatusPending {
		kv.enqueueTask(task)
	}
}

// PopBestFitTask selects the highest-scoring pending task whose resource
// requirements fit within availCPU / availRAM.
func (kv *KVStore) PopBestFitTask(queueName string, availCPU, availRAM int) (Task, bool) {
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
	task, ok := kv.tasks[id]
	if !ok {
		return Task{}, false
	}
	return *task, true
}

func (kv *KVStore) GetJob(jobID string) (JobMeta, bool) {
	job, ok := kv.jobs[jobID]
	if !ok {
		return JobMeta{}, false
	}
	return *job, true
}

func (kv *KVStore) TransitionTaskState(id string, from, to TaskStatus, workerID string) error {
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

func (kv *KVStore) CompleteTask(taskID string, result string) error {
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

func (kv *KVStore) HandleMAPFailure(taskID, ownerWorkerID string) {
	task, ok := kv.tasks[taskID]
	if !ok {
		return
	}

	if ownerWorkerID != "" && task.AssignedWorkerID != ownerWorkerID {
		log.Printf("[fail] task %s no longer owned by %s (current: %q) — skipping",
			taskID, ownerWorkerID, task.AssignedWorkerID)
		return
	}

	// Version A standalone task: fail immediately, no retry.
	if task.JobID == "" {
		task.Status = StatusFailed
		task.AssignedWorkerID = ""
		task.CompletedAt = time.Now()
		log.Printf("[fail] standalone task %s failed (no retry)", taskID)
		return
	}

	// Version B MAP task: retry up to MaxRetries.
	task.RetryCount++
	if task.RetryCount <= task.MaxRetries {
		task.AttemptNumber++
		task.Status = StatusPending
		task.AssignedWorkerID = ""
		kv.enqueueTask(task)
		log.Printf("[retry] task %s attempt %d/%d requeued", taskID, task.RetryCount, task.MaxRetries)
		return
	}

	task.Status = StatusFailed
	task.AssignedWorkerID = ""
	task.CompletedAt = time.Now()
	log.Printf("[fail] task %s exhausted %d retries", taskID, task.MaxRetries)
	kv.RecordMAPFailed(task.JobID)
}

func (kv *KVStore) RequeueDeadWorkerTasks(workerID string) []string {
	var deadTaskIDs []string
	for id, task := range kv.tasks {
		if task.Status == StatusRunning && task.AssignedWorkerID == workerID {
			deadTaskIDs = append(deadTaskIDs, id)
		}
	}
	for _, taskID := range deadTaskIDs {
		kv.HandleMAPFailure(taskID, workerID)
	}
	if len(deadTaskIDs) > 0 {
		log.Printf("[watcher] routed %d tasks from dead worker %s through failure handler",
			len(deadTaskIDs), workerID)
	}
	return deadTaskIDs
}

// SweepTerminalState safely removes tasks and jobs that have been completed or
// failed for longer than the given TTL, preventing unbounded memory growth.
func (kv *KVStore) SweepTerminalState(ttl time.Duration) (int, int) {
	now := time.Now()
	tasksFreed, jobsFreed := 0, 0

	for id, job := range kv.jobs {
		if job.Status == JobCompleted || job.Status == JobFailed {
			if job.GatherTask != nil {
				deadline := job.GatherTask.CompletedAt
				if deadline.IsZero() {
					deadline = job.GatherTask.CreatedAt
				}
				if !deadline.IsZero() && now.Sub(deadline) > ttl {
					delete(kv.jobs, id)
					jobsFreed++
				}
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
func (kv *KVStore) RegisterJob(jobID string, totalMAPs int, gatherTask *Task) {
	// Store a canonical copy in kv.tasks.
	taskCopy := *gatherTask
	kv.tasks[taskCopy.ID] = &taskCopy

	kv.jobs[jobID] = &JobMeta{
		JobID:       jobID,
		GatherTask:  kv.tasks[taskCopy.ID], // points to the canonical copy above
		TotalMAPs:   totalMAPs,
		PendingMAPs: totalMAPs,
		Status:      JobRunning,
		ResultURIs:  make(map[int]string),
	}
}

func (kv *KVStore) RecordMAPComplete(jobID string, chunkIndex int, resultURI string) error {
	job, ok := kv.jobs[jobID]
	if !ok {
		return fmt.Errorf("job %s not found", jobID)
	}
	if job.Status != JobRunning {
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

			var payloadMap map[string]interface{}
			if err := json.Unmarshal([]byte(job.GatherTask.Payload), &payloadMap); err != nil {
				return fmt.Errorf("gather payload unmarshal for job %s: %w", jobID, err)
			}
			payloadMap["input_uris"] = orderedURIs
			updated, err := json.Marshal(payloadMap)
			if err != nil {
				return fmt.Errorf("gather payload marshal for job %s: %w", jobID, err)
			}
			job.GatherTask.Payload = string(updated)

			kv.enqueueTask(job.GatherTask)
			log.Printf("[job] %s complete — enqueuing GATHER task %s", jobID, job.GatherTask.ID)
		}
	}
	return nil
}

func (kv *KVStore) RecordMAPFailed(jobID string) {
	job, ok := kv.jobs[jobID]
	if !ok {
		return
	}

	// Guard: if PendingMAPs is already 0, this failure came from the GATHER
	// stage (not a MAP stage). Mark the job failed without decrementing.
	if job.PendingMAPs <= 0 {
		job.Status = JobFailed
		log.Printf("[job] %s failed at GATHER stage", jobID)
		return
	}

	job.FailedMAPs++
	job.PendingMAPs--

	if job.PendingMAPs == 0 {
		job.Status = JobFailed
		log.Printf("[job] %s failed — %d MAP task(s) exhausted retries", jobID, job.FailedMAPs)
	}
}

// Queue introspection

func (kv *KVStore) QueueDepth(queueName string) int {
	return len(kv.queues[queueName])
}

// Worker registry

func (kv *KVStore) RegisterWorker(id string, totalCPU, totalRAM int) {
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
	w, ok := kv.workers[workerID]
	if !ok {
		return Worker{}, false
	}
	return *w, true
}

func (kv *KVStore) UpdateWorkerHeartbeat(workerID string, availCPU, availRAM int) {
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
	delete(kv.workers, workerID)
}

// Snapshot compaction

type StateSnapShot struct {
	Tasks  map[string]*Task    `json:"tasks"`
	Queues map[string][]string `json:"queues"`
	Jobs   map[string]*JobMeta `json:"jobs"`
}

func (kv *KVStore) ExportSnapshot() []byte {
	snap := StateSnapShot{
		Tasks:  kv.tasks,
		Queues: kv.queues,
		Jobs:   kv.jobs,
	}
	data, _ := json.Marshal(snap)
	return data
}

func (kv *KVStore) ImportSnapshot(data []byte) error {
	var snap StateSnapShot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	kv.tasks = snap.Tasks
	kv.queues = snap.Queues
	kv.jobs = snap.Jobs

	// Re-link GatherTask pointers in JobMeta to the authoritative *Task in
	// kv.tasks. This is necessary after JSON round-trip because json.Unmarshal
	// allocates separate objects for the embedded GatherTask and the map entry.
	for _, job := range kv.jobs {
		if job.GatherTask != nil {
			if originalTask, ok := kv.tasks[job.GatherTask.ID]; ok {
				job.GatherTask = originalTask
			}
		}
	}

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
