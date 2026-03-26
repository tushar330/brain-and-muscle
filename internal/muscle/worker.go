package muscle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
)

// s3Bucket is the storage bucket name. Override with S3_BUCKET env var.
var s3Bucket = func() string {
	if b := os.Getenv("S3_BUCKET"); b != "" {
		return b
	}
	return "bucket"
}()

// ResourcePool is a custom weighted semaphore that tracks CPU and RAM.
type ResourcePool struct {
	mu       sync.Mutex
	cond     *sync.Cond
	availCPU int
	availRAM int
	totalCPU int
	totalRAM int
}

func NewResourcePool(cpu, ram int) *ResourcePool {
	p := &ResourcePool{
		totalCPU: cpu,
		totalRAM: ram,
		availCPU: cpu,
		availRAM: ram,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Acquire blocks until the requested resources are available, then deducts them.
func (p *ResourcePool) Acquire(cpu, ram int) error {
	if cpu > p.totalCPU || ram > p.totalRAM {
		return fmt.Errorf("task requires %d CPU / %d RAM but node total is %d / %d",
			cpu, ram, p.totalCPU, p.totalRAM)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// 'for' not 'if': when we wake up another goroutine may have taken the
	// resources a microsecond before us, so we must re-check the condition.
	for p.availCPU < cpu || p.availRAM < ram {
		p.cond.Wait()
	}

	p.availCPU -= cpu
	p.availRAM -= ram
	return nil
}

// Release returns resources to the pool and wakes all waiting goroutines.
func (p *ResourcePool) Release(cpu, ram int) {
	p.mu.Lock()
	p.availCPU += cpu
	p.availRAM += ram
	p.mu.Unlock()

	p.cond.Broadcast() // wake EVERYONE — each will re-check the condition
}

// Available returns a point-in-time snapshot of free resources.
// Used by HeartbeatLoop and TaskRequestLoop to advertise capacity to the Brain.
func (p *ResourcePool) Available() (cpu, ram int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.availCPU, p.availRAM
}

// Total returns the node's hard limits (set once at startup).
func (p *ResourcePool) Total() (cpu, ram int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.totalCPU, p.totalRAM
}

const (
	heartbeatEvery  = 5 * time.Second
	backoffDuration = 2 * time.Second
	queueName       = "default"

	// Fallback per-task cost if a task arrives with zero resource requirements.
	defaultTaskCPU = 1
	defaultTaskRAM = 256 // MB
)

type Worker struct {
	cm        *ConnectionManager
	semaphore *ResourcePool
}

// NewWorker creates a Worker with a resource pool sized to the node's actual limits.
func NewWorker(cm *ConnectionManager, totalCPU, totalRAM int) *Worker {
	return &Worker{
		cm:        cm,
		semaphore: NewResourcePool(totalCPU, totalRAM),
	}
}

// Register sends OpRegisterWorker to the Brain with this node's total capacity.
// Must be called after the ConnectionManager is healthy.
func (w *Worker) Register() {
	totalCPU, totalRAM := w.semaphore.Total()

	resp, err := w.cm.SendRequest(protocol.OpRegisterWorker, protocol.RegisterWorkerPayload{
		TotalCPU: totalCPU,
		TotalRAM: totalRAM,
	})
	if err != nil {
		log.Printf("[register] failed: %v", err)
		return
	}
	if resp.OpCode != protocol.OpWorkerRegistered {
		log.Printf("[register] unexpected ack: %s", resp.OpCode)
		return
	}
	log.Printf("[register] registered with Brain — %d CPU / %d MB RAM", totalCPU, totalRAM)
}

// HeartbeatLoop sends a HEARTBEAT with current free resources every heartbeatEvery interval.
// The Brain uses this to keep its worker-registry fresh for scheduling decisions.
func (w *Worker) HeartbeatLoop() {
	ticker := time.NewTicker(heartbeatEvery)
	defer ticker.Stop()

	for range ticker.C {
		availCPU, availRAM := w.semaphore.Available()

		resp, err := w.cm.SendRequest(protocol.OpHeartbeat, protocol.HeartbeatPayload{
			AvailCPU: availCPU,
			AvailRAM: availRAM,
		})
		if err != nil {
			log.Printf("[heartbeat] failed: %v", err)
			continue
		}
		if resp.OpCode != protocol.OpHeartbeatAck {
			log.Printf("[heartbeat] unexpected response: %s", resp.OpCode)
		}
	}
}

// TaskRequestLoop polls the Brain for work, advertising current free resources so the Brain's best-fit scheduler can match tasks to this node's headroom.
func (w *Worker) TaskRequestLoop() {
	for {
		w.cm.WaitUntilHealthy()

		// Tell the Brain exactly how much headroom we have right now.
		availCPU, availRAM := w.semaphore.Available()

		resp, err := w.cm.SendRequest(protocol.OpRequestTask, protocol.RequestTaskPayload{
			QueueName: queueName,
			AvailCPU:  availCPU,
			AvailRAM:  availRAM,
		})
		if err != nil {
			log.Printf("[task] request failed: %v", err)
			time.Sleep(backoffDuration)
			continue
		}

		switch resp.OpCode {

		case protocol.OpNoTasksAvailable:
			log.Printf("[task] queue empty or no fitting task — backing off %v", backoffDuration)
			time.Sleep(backoffDuration)

		case protocol.OpTaskAssigned:
			var task protocol.AssignedTask
			if err := json.Unmarshal(resp.Payload, &task); err != nil {
				log.Printf("[task] failed to unmarshal assigned task: %v", err)
				continue
			}

			cpu, ram := taskResources(task)

			if err := w.semaphore.Acquire(cpu, ram); err != nil {
				log.Printf("[task] local acquire failed for task %s: %v", task.ID, err)
				w.cm.SendRequest(protocol.OpTaskFailed, protocol.TaskFailedPayload{
					TaskID: task.ID,
					Reason: fmt.Sprintf("local resource acquire failed: %v", err),
				})
				continue
			}

			log.Printf("[task] acquired %d CPU / %d MB RAM for task %s", cpu, ram, task.ID)
			go w.runTask(task, cpu, ram)

		default:
			log.Printf("[task] unexpected opcode: %s", resp.OpCode)
		}
	}
}

// taskResources extracts the resource requirements from the task payload,
// falling back to safe defaults when the fields are absent.
func taskResources(task protocol.AssignedTask) (cpu, ram int) {
	cpu = task.RequiredCPU
	ram = task.RequiredRAM
	if cpu <= 0 {
		cpu = defaultTaskCPU
	}
	if ram <= 0 {
		ram = defaultTaskRAM
	}
	return
}

func (w *Worker) runTask(task protocol.AssignedTask, cpu, ram int) {
	defer func() {
		w.semaphore.Release(cpu, ram)
		log.Printf("[exec] released %d CPU / %d MB RAM (task %s)", cpu, ram, task.ID)
	}()

	log.Printf("[exec] starting task %s (payload: %s)", task.ID, task.Payload)

	result := executeAssignedTask(task)

	if result.TaskID == "" {
		w.cm.SendRequest(protocol.OpTaskFailed, protocol.TaskFailedPayload{
			TaskID: task.ID,
			Reason: "unknown job_type",
		})
		return
	}

	rawResultBytes, err := json.Marshal(result)
	if err != nil {
		log.Printf("[exec] failed to marshal result for task %s: %v", task.ID, err)
		return
	}

	resp, err := w.cm.SendRequest(protocol.OpSubmitResult, protocol.SubmitResultPayload{
		TaskID:        task.ID,
		ResultPayload: string(rawResultBytes),
	})
	if err != nil {
		log.Printf("[exec] failed to submit result for task %s: %v", task.ID, err)
		return
	}
	if resp.OpCode != protocol.OpTaskCompleted {
		log.Printf("[exec] unexpected ack for task %s: %s", task.ID, resp.OpCode)
		return
	}

	log.Printf("[exec] task %s complete → %s", task.ID, result.ResultURI)
}

// executeAssignedTask is the top-level execution router.
func executeAssignedTask(assigned protocol.AssignedTask) protocol.TaskCompletePayload {
	var payload protocol.Payload

	if err := json.Unmarshal([]byte(assigned.Payload), &payload); err != nil {
		log.Printf("[exec] failed to unmarshal payload for task %s: %v", assigned.ID, err)
		return protocol.TaskCompletePayload{}
	}

	switch payload.JobType {
	case "MAP":
		return executeMAPTask(assigned.ID, payload)
	case "GATHER":
		return executeGatherTask(assigned.ID, payload)
	case "":
		return executeStandardTask(assigned.ID, payload)
	default:
		log.Printf("[exec] unknown job_type=%q for task %s — reporting failure", payload.JobType, assigned.ID)
		return protocol.TaskCompletePayload{TaskID: assigned.ID}
	}
}

// executeMAPTask runs a single fan-out chunk and returns a deterministic, attempt-scoped Claim Check URI so the Brain can distinguish retries.
func executeMAPTask(taskID string, p protocol.Payload) protocol.TaskCompletePayload {
	log.Printf("[map] task=%s job=%s chunk=%d attempt=%d",
		taskID, p.JobID, p.ChunkIndex, p.AttemptNumber)

	time.Sleep(2 * time.Second)

	uri := fmt.Sprintf("s3://%s/%s/chunk_%d_attempt_%d.json",
		s3Bucket, p.JobID, p.ChunkIndex, p.AttemptNumber)

	log.Printf("[map] task=%s done → %s", taskID, uri)
	return protocol.TaskCompletePayload{
		TaskID:     taskID,
		JobID:      p.JobID,
		JobType:    "MAP",
		ChunkIndex: p.ChunkIndex,
		ResultURI:  uri,
	}
}

// executeGatherTask sits behind the scatter/gather barrier.
// It receives the set of MAP output URIs (Claim Checks) injected by the Brain and merges them into a single final result.
func executeGatherTask(taskID string, p protocol.Payload) protocol.TaskCompletePayload {
	log.Printf("[gather] task=%s job=%s merging %d inputs",
		taskID, p.JobID, len(p.InputURIs))

	// In production: download each URI from Supabase storage and merge locally.
	// For now, log each URI to prove the data plane wiring is correct.
	for i, uri := range p.InputURIs {
		log.Printf("[gather] task=%s input[%d] = %s", taskID, i, uri)
		// TODO: download uri → local tmp file
	}

	time.Sleep(1 * time.Second)

	finalURI := fmt.Sprintf("s3://%s/%s/final_result.json", s3Bucket, p.JobID)
	log.Printf("[gather] task=%s done → %s", taskID, finalURI)
	return protocol.TaskCompletePayload{
		TaskID:    taskID,
		JobID:     p.JobID,
		JobType:   "GATHER",
		ResultURI: finalURI,
	}
}

// executeStandardTask handles legacy Version A tasks.
// It runs the ExecPayload command via os/exec and returns captured stdout.
func executeStandardTask(taskID string, p protocol.Payload) protocol.TaskCompletePayload {
	log.Printf("[standard] task=%s command=%q", taskID, p.Command)

	parts := strings.Fields(p.Command)
	if len(parts) == 0 {
		log.Printf("[standard] task=%s empty command", taskID)
		return protocol.TaskCompletePayload{TaskID: taskID}
	}

	cmd := exec.Command(parts[0], parts[1:]...) //nolint:gosec
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		log.Printf("[standard] task=%s failed: %v | stderr: %s", taskID, err, stderr.String())
		return protocol.TaskCompletePayload{TaskID: taskID, ResultURI: stderr.String()}
	}

	result := strings.TrimSpace(stdout.String())
	log.Printf("[standard] task=%s done → %q", taskID, result)
	return protocol.TaskCompletePayload{TaskID: taskID, ResultURI: result}
}
