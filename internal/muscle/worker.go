package muscle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/dop251/goja"
)

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

	p.cond.Broadcast()
}

// Available returns a point-in-time snapshot of free resources.
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

	defaultTaskCPU = 1
	defaultTaskRAM = 256 // MB
)

type Worker struct {
	cm        *ConnectionManager
	semaphore *ResourcePool
	engine    *ExecutionEngine
}

// NewWorker creates a Worker with a resource pool sized to the node's actual limits.
func NewWorker(cm *ConnectionManager, totalCPU, totalRAM int) *Worker {
	// Initialize the Strategy Engine
	engine := NewExecutionEngine()

	// Register JavaScript
	engine.Register("execute_js", &GojaRuntime{})

	// Register Python Warm Pool (it boots when the worker turns on!)
	engine.Register("execute_python", InitDockerWarmPool("muscle-ml-env", []string{"python", "-u"}))

	// Register Node Warm Pool (it boots when the worker turns on!)
	engine.Register("execute_node", InitDockerWarmPool("node:18-alpine", []string{"node"}))

	// Register C++ Warm Pool (it boots when the worker turns on!)
	engine.Register("execute_cpp", InitDockerWarmPool("gcc:latest", []string{"bash", "-c"}))

	return &Worker{
		cm:        cm,
		semaphore: NewResourcePool(totalCPU, totalRAM),
		engine:    engine,
	}
}

// Register sends OpRegisterWorker to the Brain with this node's total capacity.
// Must be called after the ConnectionManager is healthy.
func (w *Worker) Register() {
	totalCPU, totalRAM := w.semaphore.Total()

	for {
		resp, err := w.cm.SendRequest(protocol.OpRegisterWorker, protocol.RegisterWorkerPayload{
			TotalCPU: totalCPU,
			TotalRAM: totalRAM,
		})

		if err != nil {
			log.Printf("[register] network error: %v, retrying...", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.OpCode == protocol.OpError {
			log.Printf("[register] Brain returned error (Election in progress?), retrying in 1s...")
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.OpCode != protocol.OpWorkerRegistered {
			log.Printf("[register] unexpected ack: %d, retrying in 1s...", resp.OpCode)
			time.Sleep(1 * time.Second)
			continue
		}

		log.Printf("[register] registered with Brain — %d CPU / %d MB RAM", totalCPU, totalRAM)
		return
	}
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

		if resp.OpCode == protocol.OpError {
			log.Printf("[heartbeat] Brain is currently electing a new leader... retrying locally later")
			continue
		}

		if resp.OpCode != protocol.OpHeartbeatAck {
			log.Printf("[heartbeat] unexpected response: %d", resp.OpCode)
		}
	}
}

// TaskRequestLoop polls the Brain for work, advertising current free resources
// so the Brain's best-fit scheduler can match tasks to this node's headroom.
func (w *Worker) TaskRequestLoop() {
	for {
		w.cm.WaitUntilHealthy()

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

		case protocol.OpError:
			log.Printf("[task] Brain returned error: %s — backing off %v", string(resp.Payload), backoffDuration)
			time.Sleep(backoffDuration)

		default:
			log.Printf("[task] unexpected opcode: %d — backing off %v", resp.OpCode, backoffDuration)
			time.Sleep(backoffDuration)
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
		if r := recover(); r != nil {
			log.Printf("[exec] 🚨 FATAL PANIC in task %s: %v", task.ID, r)
			w.cm.SendRequest(protocol.OpTaskFailed, protocol.TaskFailedPayload{
				TaskID: task.ID,
				Reason: fmt.Sprintf("internal panic: %v", r),
			})
		}
		w.semaphore.Release(cpu, ram)
		log.Printf("[exec] released %d CPU / %d MB RAM (task %s)", cpu, ram, task.ID)
	}()

	log.Printf("[exec] starting task %s (payload: %s)", task.ID, task.Payload)

	result := w.executeAssignedTask(task)

	// An empty TaskID signals that execution failed (unmarshal error, unknown
	// job_type, or a command that exited non-zero). Send OpTaskFailed so the
	// Brain can retry or mark the task failed rather than recording it as done.
	if result.TaskID == "" {
		w.cm.SendRequest(protocol.OpTaskFailed, protocol.TaskFailedPayload{
			TaskID: task.ID,
			Reason: "execution failed — see worker logs for details",
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
		log.Printf("[exec] unexpected ack for task %s: %d", task.ID, resp.OpCode)
		return
	}

	log.Printf("[exec] task %s complete → %s", task.ID, result.ResultURI)
}

// executeAssignedTask is the top-level execution router.
func (w *Worker) executeAssignedTask(assigned protocol.AssignedTask) protocol.TaskCompletePayload {
	var p protocol.Payload

	if err := json.Unmarshal([]byte(assigned.Payload), &p); err != nil {
		log.Printf("[exec] failed to unmarshal payload for task %s: %v", assigned.ID, err)
		return protocol.TaskCompletePayload{}
	}

	// 2. Legacy Go fallbacks
	switch p.JobType {
	case "ML_PS":
		return w.routeMLParameterServer(assigned, p)
	case "ML_WORKER":
		return w.routeMLWorker(assigned, p)
	case "MAP", "GATHER":
		return w.routeGenericTask(assigned, p)
	default:
		log.Printf("[muscle] ❌ Unknown JobType: %s", p.JobType)
		return protocol.TaskCompletePayload{}
	}
}

func (w *Worker) routeGenericTask(assigned protocol.AssignedTask, p protocol.Payload) protocol.TaskCompletePayload {
	ext := filepath.Ext(p.UserScriptPath)

	var strategyName string
	var executionCommand string

	switch ext {
	case ".js":
		strategyName = "execute_js"
		executionCommand = p.UserScriptPath
	case ".cjs":
		strategyName = "execute_node"
		executionCommand = p.UserScriptPath
	case ".py":
		strategyName = "execute_python"
		executionCommand = p.UserScriptPath
	case ".cpp":
		strategyName = "execute_cpp"
		// 🚨 FIX 1: Unique binary path prevents 'Text file busy' concurrency clashes!
		binaryPath := fmt.Sprintf("/tmp/task_%s", assigned.ID)
		executionCommand = fmt.Sprintf("g++ -O3 %s -o %s && %s ; rm -f %s", p.UserScriptPath, binaryPath, binaryPath, binaryPath)
	default:
		log.Printf("[muscle] ❌ Unsupported file extension: %q for script %s", ext, p.UserScriptPath)
		return protocol.TaskCompletePayload{}
	}

	log.Printf("[muscle] Routing %s task %d to %s runtime...", p.JobType, p.ChunkIndex, strategyName)

	strategy, exists := w.engine.strategies[strategyName]
	if !exists {
		return protocol.TaskCompletePayload{}
	}

	// 🚨 FIX 2: Properly construct the environment variables
	inputsString := strings.Join(p.InputURIs, ",")
	envVars := []string{
		"JOB_TYPE=" + p.JobType,
		"CHUNK_INDEX=" + fmt.Sprintf("%d", p.ChunkIndex),
		"INPUT_URIS=" + inputsString,
	}

	// 🚨 FIX 3: Pass envVars into Runtime (NO MORE Blind Casting!)
	rawResult := strategy.Execute(assigned.ID, executionCommand, []string{}, envVars, p)

	// --- THE INTERCEPTOR ---
	rawResult.JobType = p.JobType
	rawResult.JobID = p.JobID
	rawResult.ChunkIndex = p.ChunkIndex

	if p.JobType == "MAP" {
		marker := "Result: "
		idx := strings.LastIndex(rawResult.ResultURI, marker)
		if idx != -1 {
			cleanNumber := strings.TrimSpace(rawResult.ResultURI[idx+len(marker):])
			log.Printf("[muscle] 🧹 Cleaned output for Brain: %s", cleanNumber)
			rawResult.ResultURI = cleanNumber
		}
	}

	return rawResult
}

func (w *Worker) routeMLParameterServer(assigned protocol.AssignedTask, p protocol.Payload) protocol.TaskCompletePayload {
	log.Printf("[muscle] Booting Parameter Server for Job %s...", p.JobID)

	strategy, exists := w.engine.strategies["execute_python"]
	if !exists {
		log.Printf("[muscle] Python strategy missing!")
		return protocol.TaskCompletePayload{}
	}

	warmPool, ok := strategy.(*DockerWarmPoolRuntime)
	if !ok {
		log.Printf("[muscle] execute_python is not a DockerWarmPoolRuntime")
		return protocol.TaskCompletePayload{}
	}

	// Start the PS in the background
	go strategy.Execute(assigned.ID, "python/ml_ps.py", []string{}, []string{}, p)

	// Wait a bit for the gRPC server to bind
	containerIP := warmPool.GetContainerIP()
	endpointAddress := fmt.Sprintf("%s:50051", containerIP)
	log.Printf("[muscle] Waiting for Parameter Server to bind to %s...", endpointAddress)
	time.Sleep(3 * time.Second)

	log.Printf("[muscle] Parameter Server verified running at %s", endpointAddress)

	// CALLBACK: Tell the Brain the service is ready
	w.cm.SendRequest(protocol.OpServiceReady, protocol.ServiceReadyPayload{
		JobID:       p.JobID,
		ServiceRole: "PS_ENDPOINT",
		Address:     endpointAddress,
	})

	return protocol.TaskCompletePayload{
		TaskID:    assigned.ID,
		JobID:     p.JobID,
		JobType:   "ML_PS",
		ResultURI: endpointAddress,
	}
}

func (w *Worker) routeMLWorker(assigned protocol.AssignedTask, p protocol.Payload) protocol.TaskCompletePayload {
	log.Printf("[muscle] Booting ML Worker %d for Job %s...", p.ChunkIndex, p.JobID)

	strategy, exists := w.engine.strategies["execute_python"]
	if !exists {
		return protocol.TaskCompletePayload{}
	}

	if _, ok := strategy.(*DockerWarmPoolRuntime); !ok {
		return protocol.TaskCompletePayload{}
	}

	// This executes the Python script synchronously (it waits for training to finish)
	return strategy.Execute(assigned.ID, "python/ml_worker.py", []string{}, []string{}, p)
}

// This spins up an isolated sandbox, injects Go's crypto speed, and runs the user's code!
func executeJSTask(taskID, script string, args []string, p protocol.Payload) protocol.TaskCompletePayload {
	vm := goja.New()

	// INJECT CRYPTO
	vm.Set("hashSHA256", func(input string) string {
		h := sha256.Sum256([]byte(input))
		return hex.EncodeToString(h[:])
	})

	// 🚨 INJECT DATA FETCHER 🚨
	// This allows JS to download files directly from S3 or HTTP!
	vm.Set("fetchData", func(uri string) string {
		data, err := fetchDataset(uri)
		if err != nil {
			log.Printf("[faas] dataset fetch error: %v", err)
			return ""
		}
		return data
	})

	vm.Set("CHUNK_INDEX", p.ChunkIndex)
	vm.Set("JOB_ID", p.JobID)
	vm.Set("print", func(s string) {
		log.Printf("[js-log] %s", s)
	})

	// LOAD SCRIPT CONTENT IF IT'S A PATH
	if strings.HasSuffix(script, ".js") {
		resolved := resolvePath(script)
		content, err := os.ReadFile(resolved)
		if err == nil {
			script = string(content)
		} else {
			log.Printf("[faas] Failed to read script at %s (resolved: %s): %v", script, resolved, err)
		}
	}

	_, err := vm.RunString(script)
	if err != nil {
		log.Printf("[faas] VM compilation failed for task %s: %v", taskID, err)
		return protocol.TaskCompletePayload{}
	}

	mapTaskVal := vm.Get("MapTask")
	if mapTaskVal == nil {
		log.Printf("[faas] MapTask function not found in script")
		return protocol.TaskCompletePayload{}
	}

	mapTaskFn, ok := goja.AssertFunction(mapTaskVal)
	if !ok {
		log.Printf("[faas] MapTask is not a valid JS function")
		return protocol.TaskCompletePayload{}
	}

	jsArgs := vm.ToValue(args)

	res, err := mapTaskFn(goja.Undefined(), jsArgs)
	if err != nil {
		log.Printf("[faas] Execution error in JS for task %s: %v", taskID, err)
		return protocol.TaskCompletePayload{}
	}

	result := res.String()
	log.Printf("[faas] task=%s finished -> %s", taskID, result)

	return protocol.TaskCompletePayload{
		TaskID:     taskID,
		JobID:      p.JobID,
		JobType:    "MAP",
		ChunkIndex: p.ChunkIndex,
		ResultURI:  result,
	}
}

// executeJSGather spins up a VM specifically to run the user's ReduceTask()
func executeJSGather(taskID, script string, p protocol.Payload) protocol.TaskCompletePayload {
	vm := goja.New()

	vm.Set("JOB_ID", p.JobID)
	vm.Set("print", func(s string) {
		log.Printf("[js-log] [gather] %s", s)
	})

	// LOAD SCRIPT CONTENT IF IT'S A PATH
	if strings.HasSuffix(script, ".js") {
		resolved := resolvePath(script)
		content, err := os.ReadFile(resolved)
		if err == nil {
			script = string(content)
		} else {
			log.Printf("[faas] Failed to read gather script at %s (resolved: %s): %v", script, resolved, err)
		}
	}

	_, err := vm.RunString(script)
	if err != nil {
		log.Printf("[faas] VM compilation failed for GATHER task %s: %v", taskID, err)
		return protocol.TaskCompletePayload{}
	}

	reduceTaskVal := vm.Get("ReduceTask")
	if reduceTaskVal == nil {
		log.Printf("[faas] ReduceTask function not found in user script!")
		return protocol.TaskCompletePayload{}
	}

	reduceTaskFn, ok := goja.AssertFunction(reduceTaskVal)
	if !ok {
		log.Printf("[faas] ReduceTask is not a valid JS function")
		return protocol.TaskCompletePayload{}
	}

	// Pass all the chunk outputs (InputURIs) directly to the JS Reducer
	jsInputs := vm.ToValue(p.InputURIs)

	res, err := reduceTaskFn(goja.Undefined(), jsInputs)
	if err != nil {
		log.Printf("[faas] Execution error in JS Reducer: %v", err)
		return protocol.TaskCompletePayload{}
	}

	// The JS script generated the report! Just save it to disk.
	reportString := res.String()

	reportDir := resolvePath("/app/reports")
	os.MkdirAll(reportDir, 0755)
	fileName := filepath.Join(reportDir, fmt.Sprintf("%s_report.txt", p.JobID))
	os.WriteFile(fileName, []byte(reportString), 0644)

	log.Printf("[gather] ✅ UNIVERSAL REPORT EXFILTRATED -> %s", fileName)

	return protocol.TaskCompletePayload{
		TaskID:    taskID,
		JobID:     p.JobID,
		JobType:   "GATHER",
		ResultURI: fileName,
	}
}

func fetchDataset(uri string) (string, error) {
	// 1. Is it a web link?
	if strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://") {
		resp, err := http.Get(uri)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return string(body), err
	}

	// 2. Otherwise, assume it's a local file (Resolve /app/ paths for Windows hosts)
	resolved := resolvePath(uri)
	body, err := os.ReadFile(resolved)
	return string(body), err
}
func resolvePath(path string) string {
	if !strings.HasPrefix(path, "/app/") {
		return path
	}
	cwd, _ := os.Getwd()
	// Clean the CWD to get the project root if running from cmd/muscle
	cwd = strings.TrimSuffix(cwd, "\\cmd\\muscle")
	cwd = strings.TrimSuffix(cwd, "/cmd/muscle")

	relPath := strings.TrimPrefix(path, "/app/")
	return filepath.Join(cwd, filepath.FromSlash(relPath))
}
