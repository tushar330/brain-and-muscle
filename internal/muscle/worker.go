package muscle

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/akashtripathi12/brain-and-muscle/internal/storage"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/joho/godotenv"
)

// ResourcePool: CPU/RAM tracking semaphore.
type ResourcePool struct {
	mu       sync.Mutex
	cond     *sync.Cond
	availCPU float64
	availRAM int
	totalCPU float64
	totalRAM int
}

func NewResourcePool(cpu float64, ram int) *ResourcePool {
	p := &ResourcePool{
		totalCPU: cpu,
		totalRAM: ram,
		availCPU: cpu,
		availRAM: ram,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Acquire: Blocks until resources available.
func (p *ResourcePool) Acquire(cpu float64, ram int) error {
	if cpu > p.totalCPU || ram > p.totalRAM {
		return fmt.Errorf("task requires %.1f CPU / %d RAM but node total is %.1f / %d",
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

// Release: Returns resources and wakes waiters.
func (p *ResourcePool) Release(cpu float64, ram int) {
	p.mu.Lock()
	p.availCPU += cpu
	p.availRAM += ram
	p.mu.Unlock()

	p.cond.Broadcast()
}

// Available: Snapshot of free resources.
func (p *ResourcePool) Available() (cpu float64, ram int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.availCPU, p.availRAM
}

// Total: Node hard limits.
func (p *ResourcePool) Total() (cpu float64, ram int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.totalCPU, p.totalRAM
}

const (
	heartbeatEvery  = 5 * time.Second
	backoffDuration = 2 * time.Second
	queueName       = "default"

	defaultTaskCPU float64 = 0.1
	defaultTaskRAM int     = 128 // MB
)

type Worker struct {
	cm            *ConnectionManager
	semaphore     *ResourcePool
	engine        *ExecutionEngine
	activeDaemons map[string]context.CancelFunc
	daemonMu      sync.RWMutex                            //  Changed to RWMutex for lock-free reads
	adoptedJobs   map[string]protocol.ServiceReadyPayload //  NEW: JobID -> Full restoration payload
	usedPorts     map[int]string                          //  NEW: Port -> JobID
	killedTasks   map[string]bool                         //  NEW: Tracking intentional deletions
	hostRoot      string                                  //  NEW: Root directory on host mapped to /app

	// In-memory log buffer for Goja/in-process tasks
	jobLogsMu  sync.Mutex
	subsMu     sync.RWMutex             //  NEW: Dedicated lock for log subscription map
	jobLogs    map[string][]string      // jobID -> log lines
	jobLogSubs map[string][]chan string // jobID -> active subscribers

	tempRoot string //  NEW: Shared directory for cloning/copying within appDir
}

// NewWorker creates a Worker with a resource pool sized to the node's actual limits.
func NewWorker(cm *ConnectionManager, totalCPU float64, totalRAM int, appDir string) *Worker {
	engine := NewExecutionEngine()

	// HARDENING: Force absolute paths for Docker volume stability on Windows
	absAppDir, err := filepath.Abs(appDir)
	if err != nil {
		log.Printf("[worker] ⚠️ Could not resolve absolute path for %s: %v. Using original.", appDir, err)
		absAppDir = appDir
	}
	hostRoot := absAppDir

	w := &Worker{
		cm:            cm,
		semaphore:     NewResourcePool(totalCPU, totalRAM),
		engine:        engine,
		activeDaemons: make(map[string]context.CancelFunc),
		adoptedJobs:   make(map[string]protocol.ServiceReadyPayload),
		usedPorts:     make(map[int]string),
		killedTasks:   make(map[string]bool),
		hostRoot:      hostRoot,
		jobLogs:       make(map[string][]string),
		jobLogSubs:    make(map[string][]chan string),
		tempRoot:      filepath.Join(hostRoot, "tmp"),
	}

	cacheRoot := filepath.Join(hostRoot, "cache")
	if err := os.MkdirAll(cacheRoot, 0755); err != nil {
		log.Printf("[worker] ⚠️ Could not create cache directory %s: %v", cacheRoot, err)
	}

	logsRoot := filepath.Join(hostRoot, "logs")
	if err := os.MkdirAll(logsRoot, 0755); err != nil {
		log.Printf("[worker] ⚠️ Could not create logs directory %s: %v", logsRoot, err)
	}

	// 1. Service Runtimes (Port-Bound, Long-Running)
	engine.Register("execute_python_service", NewDockerFreshRuntime("python:3.9-slim", []string{"python3", "-u"}, []string{"50051"}, hostRoot, w.WriteJobLog, cacheRoot))
	engine.Register("execute_node", NewDockerFreshRuntime("node:18-alpine", []string{"sh", "-c"}, []string{"8080"}, hostRoot, w.WriteJobLog, cacheRoot))
	engine.Register("execute_go", NewDockerFreshRuntime("golang:1.25-alpine", []string{"sh", "-c"}, []string{"8080"}, hostRoot, w.WriteJobLog, cacheRoot))

	//  ENSURE SHARED TMP EXISTS
	if err := os.MkdirAll(w.tempRoot, 0755); err != nil {
		log.Printf("[worker] ⚠️ Could not create temp directory %s: %v", w.tempRoot, err)
	}

	w.adoptExistingContainers()

	// 2. Ephemeral Task Runtimes (Cleaned up immediately)
	engine.Register("execute_js_task", NewDockerTaskRuntime("node:18-alpine", []string{"node"}, w.WriteJobLog, hostRoot))
	engine.Register("execute_python_task", NewDockerTaskRuntime("python:3.9-slim", []string{"python3", "-u"}, w.WriteJobLog, hostRoot))
	engine.Register("execute_cpp_task", NewDockerTaskRuntime("gcc:latest", []string{"bash", "-c"}, w.WriteJobLog, hostRoot))
	engine.Register("execute_go_task", NewDockerTaskRuntime("golang:1.25-alpine", []string{"sh", "-c"}, w.WriteJobLog, hostRoot))

	cm.SetPushHandler(w.handlePushMessage)

	return w
}

func (w *Worker) StartLogServer(port int) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/logs/", w.handleLogStreaming)

	log.Printf("[muscle] Log streaming server starting on :%d", port)

	// Create the listener synchronously
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		// HARDENING: If port is busy, don't crash. Just log it.
		// This happens if the user launches the UI while the background service is already running.
		if strings.Contains(err.Error(), "bind") || strings.Contains(err.Error(), "address already in use") {
			log.Printf("[muscle] ⚠️ Port :%d is already in use. Skipping log server and continuing...", port)
			return nil
		}
		return err
	}

	go func() {
		if err := http.Serve(l, mux); err != nil {
			log.Printf("[muscle] Log server exited: %v", err)
		}
	}()
	return nil
}

func (w *Worker) handleLogStreaming(res http.ResponseWriter, req *http.Request) {
	// Extract jobId from "/logs/{jobId}"
	jobId := strings.TrimPrefix(req.URL.Path, "/logs/")
	if jobId == "" {
		http.Error(res, "Missing JobID", http.StatusBadRequest)
		return
	}
	jobId = filepath.Base(jobId)

	log.Printf("[muscle] 📺 Streaming logs for job: %s", jobId)
	ctx := req.Context()

	// 1. Setup the HTTP Stream headers
	res.Header().Set("Content-Type", "text/plain; charset=utf-8")
	res.Header().Set("Transfer-Encoding", "chunked")
	res.Header().Set("X-Content-Type-Options", "nosniff")
	res.WriteHeader(http.StatusOK)
	flusher, canFlush := res.(http.Flusher)

	// 2. Playback: Send all historical logs from disk first
	logFile := filepath.Join(w.hostRoot, "logs", jobId+".txt")
	if f, err := os.Open(logFile); err == nil {
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			fmt.Fprintln(res, scanner.Text())
			if canFlush {
				flusher.Flush()
			}
		}
		f.Close()
	}

	// 3. Subscription: Check if task is active to decide if we should follow
	//  REFINEMENT: If we can't find it in Daemons, also check if there's a live subscription channel.
	// This covers standard batch tasks that are currently running.
	w.daemonMu.RLock()
	_, isDaemon := w.activeDaemons[jobId]
	if !isDaemon {
		_, isDaemon = w.adoptedJobs[jobId]
	}

	w.subsMu.RLock()
	_, hasSub := w.jobLogSubs[jobId]
	w.subsMu.RUnlock()
	w.daemonMu.RUnlock()

	isActive := isDaemon || hasSub

	if !isActive {
		log.Printf("[muscle] 🏁 Job %s not active and no live subscribers. Stream finished after playback.", jobId)
		return
	}

	// 4. Subscribe to live logs
	logCh := make(chan string, 100)
	w.subsMu.Lock()
	if w.jobLogSubs == nil {
		w.jobLogSubs = make(map[string][]chan string)
	}
	w.jobLogSubs[jobId] = append(w.jobLogSubs[jobId], logCh)
	w.subsMu.Unlock()

	defer func() {
		w.subsMu.Lock()
		subs := w.jobLogSubs[jobId]
		for i, ch := range subs {
			if ch == logCh {
				w.jobLogSubs[jobId] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		w.subsMu.Unlock()
	}()

	// 5. Follow: Stream live logs until disconnected or task ends
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-logCh:
			if !ok {
				return
			}
			fmt.Fprintln(res, line)
			if canFlush {
				flusher.Flush()
			}
		}
	}
}

// WriteJobLog appends a log line for a job/task and notifies all subscribers.
func (w *Worker) WriteJobLog(jobID string, taskID string, line string) {
	w.jobLogsMu.Lock()
	//  PERSIST TO DISK: Resilience Buffer
	if jobID != "" {
		// Sanitize JobID for filename
		safeJobID := filepath.Base(jobID)
		logFile := filepath.Join(w.hostRoot, "logs", safeJobID+".txt")

		f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			f.WriteString(line + "\n")
			f.Close()
		}
		w.jobLogs[jobID] = append(w.jobLogs[jobID], line)
	}
	w.jobLogsMu.Unlock()

	// 1. Notify subscribers (using subsMu for read-locking)
	if jobID != "" {
		w.subsMu.RLock()
		defer w.subsMu.RUnlock()
		if w.jobLogSubs != nil {
			for _, ch := range w.jobLogSubs[jobID] {
				select {
				case ch <- line:
				default:
				}
			}
		}
	}
}

// CloseJobLogs closes all subscriber channels for a job and removes the buffer.
func (w *Worker) CloseJobLogs(jobID string) {
	w.subsMu.Lock()
	defer w.subsMu.Unlock()
	if w.jobLogSubs != nil {
		for _, ch := range w.jobLogSubs[jobID] {
			close(ch)
		}
		delete(w.jobLogSubs, jobID)
	}
	// Keep the buffer around for a bit so late-arriving viewers can see historical output
}

// adoptExistingContainers asks the Docker daemon for a list of all currently running containers.
func (w *Worker) adoptExistingContainers() {
	log.Println("🔍 Scanning Docker for surviving containers...")

	// Ask Docker for the names of all running containers
	out, err := exec.Command("docker", "ps", "--format", "{{.Names}}").Output()
	if err != nil {
		log.Printf("⚠️ Failed to scan Docker: %v", err)
		return
	}

	lines := strings.Split(string(out), "\n")
	for _, containerName := range lines {
		name := strings.TrimSpace(containerName)

		// Look for containers created by our PaaS
		if strings.HasPrefix(name, "bam-job-") {
			jobID := strings.TrimPrefix(name, "bam-job-")

			//  DISCOVER THE STATUS AND RESOURCES!
			// We ask Docker for the host port, the status, AND the resource labels we attached.
			inspectOut, err := exec.Command("docker", "inspect", "--format",
				"{{range $p, $conf := .NetworkSettings.Ports}}{{if $conf}}{{(index $conf 0).HostPort}}{{end}}{{end}}|{{index .Config.Labels \"bam.required_cpu\"}}|{{index .Config.Labels \"bam.required_ram\"}}|{{.State.Status}}::-::{{index .Config.Labels \"bam.original_submit\"}}",
				name).Output()

			if err == nil {
				inspectStr := strings.TrimSpace(string(inspectOut))
				var originalSubmit string
				var head string

				idx := strings.Index(inspectStr, "::-::")
				if idx != -1 {
					head = inspectStr[:idx]
					originalSubmit = inspectStr[idx+5:]
				} else {
					head = inspectStr
				}

				parts := strings.Split(head, "|")
				if len(parts) >= 3 {
					hostPortStr := parts[0]
					cpuStr := parts[1]
					ramStr := parts[2]
					statusStr := parts[3]

					// 1. Restore Routing Address && Build Payload
					if hostPortStr != "" {
						ip := getLocalIP() //  USE DYNAMIC DETECTION!
						address := fmt.Sprintf("%s:%s", ip, hostPortStr)

						payload := protocol.ServiceReadyPayload{
							JobID:               jobID,
							ServiceRole:         "BACKEND_API",
							Address:             address,
							OriginalSubmitBytes: originalSubmit,
						}

						if port, err := strconv.Atoi(hostPortStr); err == nil {
							w.usedPorts[port] = jobID
						}

						// 2. RECONCILE RESOURCES && ADOPT GHOSTS
						//  ONLY reserve resources if the container is actually RUNNING.
						// Otherwise, it's a "Zombie" that we should clean up.
						if statusStr == "running" {
							cpu, _ := strconv.ParseFloat(cpuStr, 64)
							ram, _ := strconv.Atoi(ramStr)
							if cpu > 0 || ram > 0 {
								err := w.semaphore.Acquire(cpu, ram) // Reserve them in the pool
								if err == nil {
									log.Printf("♻️ ADOPTED %s: Recovered %.1f CPU / %d MB RAM from container footprint", jobID, cpu, ram)
									payload.RecoveredCPU = cpu
									payload.RecoveredRAM = ram
								} else {
									log.Printf("⚠️ ADOPTED %s: Failed to reserve recovered resources: %v", jobID, err)
								}
							}

							w.adoptedJobs[jobID] = payload
							log.Printf("♻️ ADOPTED %s: Address restored to %s", jobID, address)
						} else {
							log.Printf("🗑️ CLEANUP %s: Found non-running container (%s). Removing zombie...", jobID, statusStr)
							exec.Command("docker", "rm", "-f", name).Run()
						}
					}
				}
			} else {
				log.Printf("♻️ ADOPTED %s: Inspection failed: %v", jobID, err)
			}
		}
	}
}

// Register sends OpRegisterWorker to the Brain with this node's total capacity.
// Must be called after the ConnectionManager is healthy.
func (w *Worker) Register() {
	totalCPU, totalRAM := w.semaphore.Total()
	for {
		availCPU, availRAM := w.semaphore.Available()
		activeTasks := w.getActiveTaskIDs()
		resp, err := w.cm.SendRequest(protocol.OpRegisterWorker, protocol.RegisterWorkerPayload{
			TotalCPU:      totalCPU,
			TotalRAM:      totalRAM,
			AvailCPU:      availCPU,
			AvailRAM:      availRAM,
			OccupiedPorts: w.usedPorts,
			ActiveTaskIDs: activeTasks,
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

		log.Printf("[register] registered with Brain — %.1f CPU / %d MB RAM", totalCPU, totalRAM)

		//  PROACTIVE RESTORATION
		// Immediately notify the Brain about any adopted services!
		for jobID, payload := range w.adoptedJobs {
			log.Printf("[register] ⚡ Restoring routing for job %s at %s...", jobID, payload.Address)
			payload.WorkerID = w.cm.nodeID //  Inject identity for task recovery
			w.cm.SendRequest(protocol.OpServiceReady, payload)
		}

		return
	}
}

// HeartbeatLoop: Periodic resource status update.
func (w *Worker) HeartbeatLoop() {
	ticker := time.NewTicker(heartbeatEvery)
	defer ticker.Stop()
	for range ticker.C {
		var availCPU, availRAM = w.semaphore.Available()
		activeTasks := w.getActiveTaskIDs()

		resp, err := w.cm.SendRequest(protocol.OpHeartbeat, protocol.HeartbeatPayload{
			AvailCPU:      availCPU,
			AvailRAM:      availRAM,
			OccupiedPorts: w.usedPorts,
			ActiveTaskIDs: activeTasks,
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

func (w *Worker) getActiveTaskIDs() []string {
	w.daemonMu.Lock()
	defer w.daemonMu.Unlock()

	ids := make([]string, 0, len(w.activeDaemons)+len(w.adoptedJobs))
	for id := range w.activeDaemons {
		// ActiveDaemons are keyed by TaskID (e.g., service-2-daemon)
		ids = append(ids, id)
	}
	for jobID := range w.adoptedJobs {
		// AdoptedJobs are keyed by JobID (e.g., service-2).
		// We map them to the daemon task ID for consistency with the Brain's task tracking.
		ids = append(ids, fmt.Sprintf("%s-daemon", jobID))
	}
	return ids
}

// TaskRequestLoop: Poll Brain for work.
func (w *Worker) TaskRequestLoop() {
	for {
		w.cm.WaitUntilHealthy()

		availCPU, availRAM := w.semaphore.Available()

		activeTasks := w.getActiveTaskIDs()

		resp, err := w.cm.SendRequest(protocol.OpRequestTask, protocol.RequestTaskPayload{
			QueueName:     queueName,
			AvailCPU:      availCPU,
			AvailRAM:      availRAM,
			OccupiedPorts: w.usedPorts,
			ActiveTaskIDs: activeTasks,
		})
		if err != nil {
			log.Printf("[task] request failed: %v", err)
			time.Sleep(backoffDuration)
			continue
		}

		switch resp.OpCode {

		case protocol.OpNoTasksAvailable:
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

			log.Printf("[task] acquired %.1f CPU / %d MB RAM for task %s", cpu, ram, task.ID)
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

// taskResources: Extract CPU/RAM from payload.
func taskResources(task protocol.AssignedTask) (cpu float64, ram int) {
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

func (w *Worker) runTask(task protocol.AssignedTask, cpu float64, ram int) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[exec]  FATAL PANIC in task %s: %v", task.ID, r)
			w.cm.SendRequest(protocol.OpTaskFailed, protocol.TaskFailedPayload{
				TaskID: task.ID,
				Reason: fmt.Sprintf("internal panic: %v", r),
			})
			w.semaphore.Release(cpu, ram)
		}
	}()

	//  NEW: Create a Cancelable Context for the Task Lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	shouldCancel := true // Default to cleanup on exit (Batch mode)
	defer func() {
		if shouldCancel {
			cancel()
		}
	}()

	//  NEW: Register in Active Daemons so HandleKillTask can find it
	w.daemonMu.Lock()
	w.activeDaemons[task.ID] = cancel
	w.daemonMu.Unlock()

	defer func() {
		// Only remove if we are cancelling (Batch mode).
		// For Daemons, the activeDaemons entry MUST persist until explicit Brain kill!
		if shouldCancel {
			w.daemonMu.Lock()
			delete(w.activeDaemons, task.ID)
			w.daemonMu.Unlock()
		}
	}()

	log.Printf("[exec] starting task %s (payload: %s)", task.ID, task.Payload)

	//  NEW: Start Cloud Native Log Sync
	if task.LogUploadURL != "" {
		go w.startCloudLogSync(ctx, task)
	}

	result := w.executeAssignedTask(ctx, task)

	// An empty TaskID signals that execution failed (unmarshal error, unknown
	// job_type, or a command that exited non-zero). Send OpTaskFailed so the
	// Brain can retry or mark the task failed rather than recording it as done.
	if result.TaskID == "" {
		w.daemonMu.RLock()
		wasKilled := w.killedTasks[task.ID]
		w.daemonMu.RUnlock()

		if !wasKilled {
			w.cm.SendRequest(protocol.OpTaskFailed, protocol.TaskFailedPayload{
				TaskID: task.ID,
				Reason: "execution failed — see worker logs for details",
			})
		} else {
			log.Printf("[exec] Task %s was intentionally killed during startup — ignoring failure.", task.ID)
		}
		w.semaphore.Release(cpu, ram)
		return
	}

	//  NEW: Bypass resource release for Daemon Compute
	if result.ResultURI == "DAEMON_RUNNING" {
		log.Printf("[exec] Task %s is a Daemon. Holding %.1f CPU / %d MB RAM indefinitely.", task.ID, cpu, ram)
		shouldCancel = false // 🎈 HANDOVER: Leave context open for global registry!
		// We DO NOT release the semaphore. We DO NOT send OpSubmitResult.
		return
	}

	rawResultBytes, err := json.Marshal(result)
	if err != nil {
		log.Printf("[exec] failed to marshal result for task %s: %v", task.ID, err)
		w.semaphore.Release(cpu, ram)
		return
	}

	log.Printf("[exec] task %s complete → %s", task.ID, result.ResultURI)

	// The payload we ultimately send back to the Brain over TCP.
	// Default: inline raw result (no Azure configured).
	resultPayload := string(rawResultBytes)

	//  NEW: Azure Blob "Claim Check" — upload heavy bytes directly to Azure,
	// then send only a tiny JSON pointer back over the control-plane TCP link.
	if task.UploadURL != "" {
		log.Printf("[exec] ☁️ Uploading task %s results directly to Azure...", task.ID)
		if err := w.UploadToAzure(rawResultBytes, task.UploadURL); err != nil {
			log.Printf("[exec] ❌ Azure Upload Failed for task %s: %v", task.ID, err)
			w.cm.SendRequest(protocol.OpTaskFailed, protocol.TaskFailedPayload{
				TaskID: task.ID,
				Reason: fmt.Sprintf("Azure upload failed: %v", err),
			})
			w.semaphore.Release(cpu, ram)
			return
		}
		log.Printf("[exec] ✅ Azure Upload Successful for task %s — sending blob URL pointer to Brain", task.ID)
		// Strip the SAS query string so the Brain stores only the stable base URL.
		blobBaseURL := task.UploadURL
		if idx := strings.Index(blobBaseURL, "?"); idx != -1 {
			blobBaseURL = blobBaseURL[:idx]
		}
		resultPayload = fmt.Sprintf(`{"blob_url":"%s"}`, blobBaseURL)
	}

	// Ensure final log flush occurs!
	if task.LogUploadURL != "" {
		w.syncLogToCloud(task)
	}

	_, err = w.cm.SendRequest(protocol.OpSubmitResult, protocol.SubmitResultPayload{
		TaskID:        task.ID,
		ResultPayload: resultPayload,
	})

	w.CloseJobLogs(result.JobID)
	if task.ID != result.JobID {
		w.CloseJobLogs(task.ID)
	}

	w.semaphore.Release(cpu, ram)
}

// ☁️ syncLogToCloud reads the local job log and overwrites the Azure Blob Log
func (w *Worker) syncLogToCloud(task protocol.AssignedTask) {
	var p protocol.Payload
	json.Unmarshal([]byte(task.Payload), &p)

	jobIDForLog := p.JobID
	if jobIDForLog == "" {
		jobIDForLog = task.ID
	}

	safeJobID := filepath.Base(jobIDForLog)
	logFile := filepath.Join(w.hostRoot, "logs", safeJobID+".txt")

	data, err := os.ReadFile(logFile)
	if err != nil {
		return // File might not exist yet
	}

	// We append "x-ms-blob-type: BlockBlob" in UploadToAzure to overwrite it wholly
	w.UploadLogToAzure(data, task.LogUploadURL)
}

// ☁️ UploadLogToAzure performs a direct HTTP PUT to the Azure SAS URL as text/plain
func (w *Worker) UploadLogToAzure(outputData []byte, uploadURL string) error {
	req, err := http.NewRequest("PUT", uploadURL, bytes.NewReader(outputData))
	if err != nil {
		return err
	}

	req.Header.Set("x-ms-blob-type", "BlockBlob")
	req.Header.Set("Content-Type", "text/plain")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("azure upload failed with status: %d", resp.StatusCode)
	}
	return nil
}

// ☁️ startCloudLogSync periodically pushes logs to Azure
func (w *Worker) startCloudLogSync(ctx context.Context, task protocol.AssignedTask) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// The runTask loop handles the final flush
			return
		case <-ticker.C:
			w.syncLogToCloud(task)
		}
	}
}

// UploadToAzure performs a direct HTTP PUT to the Azure SAS URL
func (w *Worker) UploadToAzure(outputData []byte, uploadURL string) error {
	req, err := http.NewRequest("PUT", uploadURL, strings.NewReader(string(outputData)))
	if err != nil {
		return err
	}

	// Azure requires this header for Block Blobs
	req.Header.Set("x-ms-blob-type", "BlockBlob")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("azure returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// executeAssignedTask routes a task to the appropriate Docker or Native runtime strategy.
func (w *Worker) executeAssignedTask(ctx context.Context, assigned protocol.AssignedTask) protocol.TaskCompletePayload {
	var p protocol.Payload

	if err := json.Unmarshal([]byte(assigned.Payload), &p); err != nil {
		log.Printf("[exec] failed to unmarshal payload for task %s: %v", assigned.ID, err)
		return protocol.TaskCompletePayload{}
	}

	// 2. Legacy Go fallbacks
	switch p.JobType {
	case protocol.JobTypeMLPS:
		return w.routeMLParameterServer(ctx, assigned, p)
	case protocol.JobTypeMLWorker:
		return w.routeMLWorker(ctx, assigned, p)
	case protocol.JobTypeService:
		return w.routeService(ctx, assigned, p)
	case "MAP", "GATHER":
		return w.routeGenericTask(ctx, assigned, p)
	default:
		log.Printf("[muscle] ❌ Unknown JobType: %s", p.JobType)
		return protocol.TaskCompletePayload{}
	}
}

func (w *Worker) routeGenericTask(ctx context.Context, assigned protocol.AssignedTask, p protocol.Payload) protocol.TaskCompletePayload {
	//  FIX: Ensure the raw script is accessible inside the Docker container!
	// If it's a URL (Cloud Staged), download it first.
	// Otherwise, copy from the local path.
	if p.UserScriptPath != "" {
		var scriptContent []byte
		var err error

		if strings.HasPrefix(p.UserScriptPath, "http") {
			log.Printf("[muscle] 🌐 Downloading script from URL: %s", p.UserScriptPath)
			scriptContent, err = storage.DownloadBlob(ctx, p.UserScriptPath)
		} else {
			scriptContent, err = os.ReadFile(p.UserScriptPath)
		}

		if err == nil {
			// Strip SAS query params from the filename (e.g. "script.py?se=..." → "script.py")
			baseName := filepath.Base(p.UserScriptPath)
			if idx := strings.Index(baseName, "?"); idx != -1 {
				baseName = baseName[:idx]
			}
			safeBase := fmt.Sprintf("%s_%s", assigned.ID, baseName)
			safeScriptPath := filepath.Join(w.tempRoot, safeBase)
			os.WriteFile(safeScriptPath, scriptContent, 0644)
			p.UserScriptPath = safeScriptPath
		} else {
			log.Printf("[muscle] ⚠️ Warning: Failed to read UserScriptPath %s: %v", p.UserScriptPath, err)
		}
	}

	// Strip SAS query params before detecting extension (e.g. ".py?se=..." → ".py")
	scriptURLForExt := p.UserScriptPath
	if idx := strings.Index(scriptURLForExt, "?"); idx != -1 {
		scriptURLForExt = scriptURLForExt[:idx]
	}
	ext := filepath.Ext(scriptURLForExt)

	var strategyName string
	var executionCommand string

	switch ext {
	case ".js", ".cjs":
		strategyName = "execute_js_task"

		argsJSON, _ := json.Marshal(p.Args)
		if len(p.Args) == 0 {
			argsJSON = []byte("[]")
		}

		urisJSON, _ := json.Marshal(p.InputURIs)
		if len(p.InputURIs) == 0 {
			urisJSON = []byte("[]")
		}

		//  DOCKER SHIM: Emulate GojaVM's implicit execution and return capture!
		shimContent := fmt.Sprintf(`const fs = require('fs');

global.fetchData = function(url) {
    if (url.startsWith('http://') || url.startsWith('https://')) {
        // Warning: HTTP fetch is async in Node, but Goja was sync.
		// For backward compatibility on local paths, we read sync.
        return fs.readFileSync(url, 'utf8');
    }
    return fs.readFileSync(url, 'utf8');
};

const code = fs.readFileSync('%s', 'utf8');
eval(code);

const args = %s;
global.args = args; // Compatibility with Goja implicit globals

const uris = %s;
global.chunkResults = uris; // Compatibility with Goja implicit globals

global.CHUNK_INDEX = parseInt(process.env.CHUNK_INDEX || "0");
global.JOB_ID = process.env.JOB_ID || "";
global.DATASET_PATH = process.env.DATASET_PATH || "";

if (process.env.JOB_TYPE === 'MAP') {
    let fn = undefined;
    if (typeof MapTask !== 'undefined') fn = MapTask;
    else if (typeof map_task !== 'undefined') fn = map_task;
    
    if (fn) {
        let res = fn(args);
        console.log('\nResult: ' + (typeof res === 'object' ? JSON.stringify(res) : res));
    }
} else if (process.env.JOB_TYPE === 'GATHER') {
    let fn = undefined;
    if (typeof ReduceTask !== 'undefined') fn = ReduceTask;
    else if (typeof reduce_task !== 'undefined') fn = reduce_task;
    
    if (fn) {
        let res = fn(uris);
        console.log('\nResult: ' + (typeof res === 'object' ? JSON.stringify(res) : res));
    }
}
`, filepath.ToSlash(w.translateToContainerPath(p.UserScriptPath)), string(argsJSON), string(urisJSON))

		shimPath := filepath.Join(w.hostRoot, fmt.Sprintf("shim_%s.js", assigned.ID))
		os.WriteFile(shimPath, []byte(shimContent), 0644)

		// Map the execution command to the generated shim file
		executionCommand = shimPath
	case ".py":
		strategyName = "execute_python_task"
		executionCommand = p.UserScriptPath
	case ".cpp":
		strategyName = "execute_cpp_task"
		//  FIX 1: Unique binary path prevents 'Text file busy' concurrency clashes!
		binaryPath := fmt.Sprintf("/tmp/task_%s", assigned.ID)
		executionCommand = fmt.Sprintf("g++ -O3 %s -o %s && %s ; rm -f %s", p.UserScriptPath, binaryPath, binaryPath, binaryPath)
	case ".go":
		strategyName = "execute_go_task"
		executionCommand = fmt.Sprintf("go run %s", p.UserScriptPath)
	default:
		log.Printf("[muscle] ❌ Unsupported file extension: %q for script %s", ext, p.UserScriptPath)
		return protocol.TaskCompletePayload{}
	}

	log.Printf("[muscle] Routing %s task %d to %s runtime...", p.JobType, p.ChunkIndex, strategyName)

	strategy, exists := w.engine.strategies[strategyName]
	if !exists {
		return protocol.TaskCompletePayload{}
	}

	argsJSON, _ := json.Marshal(p.Args)
	if len(p.Args) == 0 {
		argsJSON = []byte("[]")
	}

	urisJSON, _ := json.Marshal(p.InputURIs)
	if len(p.InputURIs) == 0 {
		urisJSON = []byte("[]")
	}

	//  FIX 2: Properly construct the environment variables
	envVars := []string{
		"JOB_TYPE=" + p.JobType,
		"CHUNK_INDEX=" + fmt.Sprintf("%d", p.ChunkIndex),
		"ARGS=" + string(argsJSON),
		"INPUT_URIS=" + string(urisJSON),
		"BRAIN_URL=http://" + w.getGatewayAddr() + ":8888",
	}

	for k, v := range p.Env {
		envVars = append(envVars, fmt.Sprintf("%s=%s", k, v))
	}

	//  FIX 3: Pass envVars into Runtime (NO MORE Blind Casting!)
	rawResult := strategy.Execute(ctx, assigned.ID, executionCommand, []string{}, envVars, p)

	// CLEANUP SHIM
	if ext == ".js" || ext == ".cjs" {
		os.Remove(executionCommand)
	}

	// --- THE INTERCEPTOR ---
	rawResult.JobType = p.JobType
	rawResult.JobID = p.JobID
	rawResult.ChunkIndex = p.ChunkIndex

	// Extract 'Result: ' block for BOTH MAP and GATHER
	marker := "Result: "
	idx := strings.LastIndex(rawResult.ResultURI, marker)
	if idx != -1 {
		cleanNumber := strings.TrimSpace(rawResult.ResultURI[idx+len(marker):])
		log.Printf("[muscle] 🧹 Cleaned output for Brain: %s", cleanNumber)
		rawResult.ResultURI = cleanNumber
	}

	return rawResult
}

func (w *Worker) routeMLParameterServer(ctx context.Context, assigned protocol.AssignedTask, p protocol.Payload) protocol.TaskCompletePayload {
	// ML Parameter Server is exactly like a Service endpoint, so we route it through the robust Service engine
	// which dynamically maps host ports and guarantees health checks and cleanup!
	p.ExposedPort = 50051 // The internal port of the gRPC server inside the container
	p.JobType = protocol.JobTypeService

	// Force the command using absolute python path and a relative script path!
	// Inside the container /app contains the contents of temp_scripts, so we just use the filename.
	// Strip SAS query params before extracting basename for container commands
	basename := filepath.Base(p.UserScriptPath)
	if idx := strings.Index(basename, "?"); idx != -1 {
		basename = basename[:idx]
	}
	p.RunCommand = "/usr/local/bin/python3 -u " + basename

	//  FIX: Force the JOB_TYPE check so it runs as the PS!
	if p.Env == nil {
		p.Env = make(map[string]string)
	}
	p.Env["JOB_TYPE"] = "ML_PS"
	p.Env["JOB_ID"] = p.JobID

	//  FIX: Tell routeService THIS is a Parameter Server role, NOT a generic API.
	p.ServiceRole = "PS_ENDPOINT"

	result := w.routeService(ctx, assigned, p)
	if result.ResultURI == "DAEMON_RUNNING" {
		result.JobType = "ML_PS" // Re-assign so Brain knows it's the PS
	}
	return result
}

func (w *Worker) routeMLWorker(ctx context.Context, assigned protocol.AssignedTask, p protocol.Payload) protocol.TaskCompletePayload {
	log.Printf("[muscle] Booting ML Worker %d for Job %s...", p.ChunkIndex, p.JobID)

	//  FIX: Ensure the raw script is accessible inside the Docker container sandbox!
	if p.UserScriptPath != "" {
		var scriptContent []byte
		var err error

		if strings.HasPrefix(p.UserScriptPath, "http") {
			log.Printf("[muscle] 🌐 Downloading script from URL: %s", p.UserScriptPath)
			scriptContent, err = storage.DownloadBlob(context.Background(), p.UserScriptPath)
		} else {
			scriptContent, err = os.ReadFile(p.UserScriptPath)
		}

		if err == nil {
			// Strip SAS query params from the filename
			baseName := filepath.Base(p.UserScriptPath)
			if idx := strings.Index(baseName, "?"); idx != -1 {
				baseName = baseName[:idx]
			}
			safeBase := fmt.Sprintf("%s_%s", assigned.ID, baseName)
			safeScriptPath := filepath.Join(w.tempRoot, safeBase)
			os.WriteFile(safeScriptPath, scriptContent, 0644)
			p.UserScriptPath = safeScriptPath
		} else {
			log.Printf("[muscle] ⚠️ Warning: Failed to read UserScriptPath %s: %v", p.UserScriptPath, err)
		}
	}

	strategy, exists := w.engine.strategies["execute_python_task"]
	if !exists {
		return protocol.TaskCompletePayload{}
	}

	//  FIX: Construct environment variables for the ML WORKER
	envVars := []string{
		"JOB_TYPE=ML_WORKER",
		fmt.Sprintf("CHUNK_INDEX=%d", p.ChunkIndex),
		"JOB_ID=" + p.JobID,
		"BRAIN_URL=http://" + w.getGatewayAddr() + ":8888",
	}

	// Inject the dynamic Service Discovery IP!
	if psEndpoint, exists := p.TopologyMap["PS_ENDPOINT"]; exists {
		envVars = append(envVars, "PS_ENDPOINT="+psEndpoint)
	}

	// Merge with user-defined Env
	for k, v := range p.Env {
		envVars = append(envVars, fmt.Sprintf("%s=%s", k, v))
	}

	// Resolve the script to run (usually the one stored in p.UserScriptPath)
	executionCommand := p.UserScriptPath

	return strategy.Execute(context.Background(), assigned.ID, executionCommand, []string{}, envVars, p)
}

// prepareServiceSource resolves the source directory and auto-detects the entrypoint.
// For GitHub sources, it clones the repo. For local paths, it validates the directory.
// Returns: (hostDir, entrypoint relative to hostDir, strategy name, error)
func (w *Worker) prepareServiceSource(p *protocol.Payload) (string, string, string, error) {
	sourceType := p.SourceType
	source := p.UserScriptPath

	if sourceType == "" || sourceType == "github" {
		// HARDENING: Even if it SAYS github, check if it's a local absolute path
		if filepath.IsAbs(source) || (runtime.GOOS == "windows" && len(source) > 1 && source[1] == ':') {
			// If it's an absolute path and exists locally, force LOCAL
			if _, err := os.Stat(source); err == nil {
				if sourceType == "github" {
					log.Printf("[source] Warning: Payload requested 'github' for local path %q. Forcing 'local'.", source)
				}
				sourceType = "local"
			}
		}

		// Legacy auto-detection if still empty
		if sourceType == "" {
			if strings.Contains(source, "blob.core.windows.net") {
				// Azure Blob Storage URL — NOT a git repo!
				sourceType = "blob"
			} else if strings.HasPrefix(source, "https://") || strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "git@") {
				sourceType = "github"
			} else {
				sourceType = "local"
			}
		}
	}

	var hostDir string

	if sourceType == "github" {
		// Clone the GitHub repo into a temp directory
		tmpDir := filepath.Join(w.tempRoot, fmt.Sprintf("bam-clone-%s-%d", p.JobID, time.Now().UnixNano()))
		log.Printf("[source] Cloning GitHub repo: %s -> %s", source, tmpDir)

		cmd := exec.Command("git", "clone", "--depth", "1", source, tmpDir)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return "", "", "", fmt.Errorf("git clone failed: %w", err)
		}

		hostDir = tmpDir
		log.Printf("[source] Cloned successfully to %s", tmpDir)
	} else if sourceType == "blob" {
		// Azure Blob Storage URL — download the single file into a temp directory
		tmpDir := filepath.Join(w.tempRoot, fmt.Sprintf("bam-blob-%s-%d", p.JobID, time.Now().UnixNano()))
		if err := os.MkdirAll(tmpDir, 0755); err != nil {
			return "", "", "", fmt.Errorf("failed to create blob temp dir: %w", err)
		}

		// Extract filename from the URL (strip SAS query params)
		blobBasename := path.Base(source)
		if idx := strings.Index(blobBasename, "?"); idx != -1 {
			blobBasename = blobBasename[:idx]
		}

		log.Printf("[source] Downloading blob script: %s -> %s/%s", source, tmpDir, blobBasename)
		scriptContent, err := storage.DownloadBlob(context.Background(), source)
		if err != nil {
			return "", "", "", fmt.Errorf("blob download failed: %w", err)
		}

		scriptPath := filepath.Join(tmpDir, blobBasename)
		if err := os.WriteFile(scriptPath, scriptContent, 0644); err != nil {
			return "", "", "", fmt.Errorf("failed to write blob script: %w", err)
		}

		hostDir = tmpDir
		log.Printf("[source] Blob script downloaded to %s", scriptPath)
	} else {
		// Local path — use directly
		// If it's a container path (/app/...), resolve to host
		if strings.HasPrefix(source, "/app/") {
			hostDir = resolvePath(source)
		} else {
			hostDir = source
		}

		// Validate the directory exists
		info, err := os.Stat(hostDir)
		if err != nil {
			return "", "", "", fmt.Errorf("source directory not found: %s — %w", hostDir, err)
		}
		if !info.IsDir() {
			// User pointed to a file — use the directory containing it
			hostDir = filepath.Dir(hostDir)
		}

		// Prevent cross-platform pollution
		// Create a clean temp copy of the local directory to avoid Docker reading Windows `node_modules` or `__pycache__`
		tmpDir := filepath.Join(w.tempRoot, fmt.Sprintf("bam-local-%s-%d", p.JobID, time.Now().UnixNano()))
		log.Printf("[source] Copying local path (excluding node_modules) safely: %s -> %s", hostDir, tmpDir)

		if err := copyDirectorySafely(hostDir, tmpDir); err != nil {
			return "", "", "", fmt.Errorf("safe local copy failed: %w", err)
		}

		hostDir = tmpDir
		log.Printf("[source] Safety copy complete: %s", hostDir)
	}

	// Auto-detect the project type and entrypoint (ONLY if manual commands are missing!)
	if p.RunCommand == "" {
		entrypoint, strategyName, err := autoDetectProject(hostDir)
		if err != nil {
			return "", "", "", fmt.Errorf("could not auto-detect project: %w", err)
		}
		log.Printf("[source] Auto-detected: strategy=%s, entrypoint=%s", strategyName, entrypoint)
		return hostDir, entrypoint, strategyName, nil
	}

	// Manual commands provided — use shell strategy but respect file extension OR command keywords for base image!
	log.Printf("[source] Manual commands provided (%s). Detecting base image...", p.RunCommand)

	// Strip SAS query params before detecting extension
	scriptURLForExt := p.UserScriptPath
	if idx := strings.Index(scriptURLForExt, "?"); idx != -1 {
		scriptURLForExt = scriptURLForExt[:idx]
	}
	ext := filepath.Ext(scriptURLForExt)
	strategyName := ""

	//  HEURISTIC 1: Check the command string itself (most reliable for manual overrides)
	lcCmd := strings.ToLower(p.RunCommand)
	if strings.Contains(lcCmd, "python") || strings.Contains(lcCmd, "pip ") {
		strategyName = "execute_python_service"
	} else if strings.Contains(lcCmd, "go run") || strings.Contains(lcCmd, "go build") {
		strategyName = "execute_go"
	} else if strings.Contains(lcCmd, "node ") || strings.Contains(lcCmd, "npm ") || strings.Contains(lcCmd, "npx ") {
		strategyName = "execute_node"
	} else if ext == ".py" {
		//  HEURISTIC 2: Fall back to extension
		strategyName = "execute_python_service"
	} else if ext == ".go" {
		strategyName = "execute_go"
	} else {
		//  DEFAULT FALLBACK: Node (but log a warning)
		log.Printf("[source] ⚠️ Unknown runtime for command %q and ext %q. Defaulting to Node.", p.RunCommand, ext)
		strategyName = "execute_node"
	}

	return hostDir, "", strategyName, nil
}

// autoDetectProject: Detect project type and entrypoint.
func autoDetectProject(dir string) (entrypoint string, strategyName string, err error) {
	// 1. Check for Go project (go.mod)
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
		strategyName = "execute_go"
		entrypoint = findGoEntrypoint(dir)
		if entrypoint != "" {
			return entrypoint, strategyName, nil
		}
	}

	// 2. Check for Node.js project (package.json)
	if _, err := os.Stat(filepath.Join(dir, "package.json")); err == nil {
		strategyName = "execute_node"
		entrypoint = findNodeEntrypoint(dir)
		if entrypoint != "" {
			return entrypoint, strategyName, nil
		}
	}

	// 3. Check for Python project (requirements.txt or *.py)
	if _, err := os.Stat(filepath.Join(dir, "requirements.txt")); err == nil {
		strategyName = "execute_python"
		entrypoint = findPythonEntrypoint(dir)
		if entrypoint != "" {
			return entrypoint, strategyName, nil
		}
	}

	// 4. Fallback: look for common entry files
	for _, candidate := range []string{"main.go", "main.py", "app.py", "index.js", "server.js", "app.js"} {
		if _, err := os.Stat(filepath.Join(dir, candidate)); err == nil {
			ext := filepath.Ext(candidate)
			return candidate, resolveStrategyName(ext), nil
		}
	}

	return "", "", fmt.Errorf("no recognizable project found in %s (looked for go.mod, package.json, requirements.txt, or common entry files)", dir)
}

// findGoEntrypoint discovers the main.go in a Go project.
func findGoEntrypoint(dir string) string {
	// Priority 1: cmd/*/main.go (standard Go project layout)
	cmdDir := filepath.Join(dir, "cmd")
	if entries, err := os.ReadDir(cmdDir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				candidate := filepath.Join("cmd", e.Name(), "main.go")
				if _, err := os.Stat(filepath.Join(dir, candidate)); err == nil {
					return candidate
				}
			}
		}
	}

	// Priority 2: cmd/main.go
	if _, err := os.Stat(filepath.Join(dir, "cmd", "main.go")); err == nil {
		return "cmd/main.go"
	}

	// Priority 3: main.go at root
	if _, err := os.Stat(filepath.Join(dir, "main.go")); err == nil {
		return "main.go"
	}

	return ""
}

// findNodeEntrypoint discovers the entry point for a Node.js project.
func findNodeEntrypoint(dir string) string {
	// Read package.json to find "main" or "scripts.start"
	data, err := os.ReadFile(filepath.Join(dir, "package.json"))
	if err == nil {
		var pkg map[string]interface{}
		if json.Unmarshal(data, &pkg) == nil {
			// Check "main" field
			if main, ok := pkg["main"].(string); ok && main != "" {
				if _, err := os.Stat(filepath.Join(dir, main)); err == nil {
					return main
				}
			}
		}
	}

	// Fallback to common names
	for _, candidate := range []string{"index.js", "server.js", "app.js", "src/index.js", "src/server.js"} {
		if _, err := os.Stat(filepath.Join(dir, candidate)); err == nil {
			return candidate
		}
	}

	return ""
}

// findPythonEntrypoint discovers the entry point for a Python project.
func findPythonEntrypoint(dir string) string {
	for _, candidate := range []string{"app.py", "main.py", "manage.py", "server.py", "run.py", "src/main.py", "src/app.py"} {
		if _, err := os.Stat(filepath.Join(dir, candidate)); err == nil {
			return candidate
		}
	}
	return ""
}

// GetFreePort is now canonicalized in runtime.go

func (w *Worker) routeService(ctx context.Context, assigned protocol.AssignedTask, p protocol.Payload) protocol.TaskCompletePayload {
	hostPort := GetFreePort()
	if hostPort == 0 {
		log.Printf("[muscle] Failed to allocate dynamic host port")
		return protocol.TaskCompletePayload{}
	}
	p.HostPort = hostPort // Store for runtime and reporting!

	log.Printf("[muscle] Booting Backend Service for Job %s (Internal: %d -> Host: %d)...", p.JobID, p.ExposedPort, p.HostPort)

	// SOURCE PREPARATION: Clone repo or resolve local path, auto-detect entrypoint
	hostDir, entrypoint, strategyName, err := w.prepareServiceSource(&p)
	if err != nil {
		log.Printf("[muscle] Source preparation failed for job %s: %v", p.JobID, err)
		return protocol.TaskCompletePayload{}
	}

	// Set the source directory for the Docker runtime to bind-mount
	p.SourceDir = hostDir

	strategy, exists := w.engine.strategies[strategyName]
	if !exists {
		log.Printf("[muscle] strategy %s not found", strategyName)
		return protocol.TaskCompletePayload{}
	}

	//  NEW: Use the global Lifecycle Context passed from runTask
	// No more local cancel creation — runTask owns the context now!

	go func() {
		//  SMART ENVIRONMENT INJECTION
		// Automatically find and load .env files from the project directory
		projectEnvVars := w.findAndLoadDotEnvFromDir(hostDir)

		// Filter out any PORT= from project env so ours always wins
		envVars := make([]string, 0, len(projectEnvVars)+1+len(p.Env))
		for _, v := range projectEnvVars {
			if !strings.HasPrefix(v, "PORT=") {
				envVars = append(envVars, v)
			}
		}

		//  USER-DEFINED ENV VARS (via payload)
		for k, v := range p.Env {
			envVars = append(envVars, fmt.Sprintf("%s=%s", k, v))
		}

		envVars = append(envVars, fmt.Sprintf("PORT=%d", p.ExposedPort))

		// Build the execution command based on strategy
		var cmd string
		if p.RunCommand != "" {
			// MANUAL OVERRIDE
			//  CRITICAL: Ensure we are in the /app directory before running manual paths
			cmd = fmt.Sprintf("cd /app && %s", p.RunCommand)
			if p.BuildCommand != "" {
				cmd = fmt.Sprintf("cd /app && %s && %s", p.BuildCommand, p.RunCommand)
			}
			log.Printf("[exec] MANUAL command: %s", cmd)
		} else if strategyName == "execute_go" {
			// Go projects: cd into /app (where project is mounted) and run
			projectDir := "/app"
			if p.RootDirectory != "" {
				projectDir = filepath.ToSlash(filepath.Join("/app", p.RootDirectory))
			}
			cmd = fmt.Sprintf("cd '%s' && go run '%s'", projectDir, filepath.ToSlash(entrypoint))
			log.Printf("[muscle] Starting container-internal Build & Run for %s: %s", assigned.ID, cmd)
			log.Printf("[exec] Go project command: %s", cmd)
		} else if strategyName == "execute_node" {
			// Node.js: install deps then run
			cmd = fmt.Sprintf("npm install --production 2>&1 && node '%s'", filepath.ToSlash(entrypoint))
			log.Printf("[muscle] 🏗️  Starting container-internal Build & Run for %s: %s", assigned.ID, cmd)
			log.Printf("[exec] Node.js project command: %s", cmd)
		} else if strategyName == "execute_python_service" {
			// Python Services: pip install then run
			cmd = fmt.Sprintf("pip install -r requirements.txt 2>&1; python3 -u '%s'", filepath.ToSlash(entrypoint))
			log.Printf("[muscle] 🏗️  Starting container-internal Build & Run for %s: %s", assigned.ID, cmd)
			log.Printf("[exec] Python project command: %s", cmd)
		} else {
			cmd = filepath.ToSlash(entrypoint)
		}

		// --- SUPERVISOR LOOP ---
		backoff := 2 * time.Second
		maxRetries := 5
		attempts := 0

		for {
			attempts++

			//  Pass the cancellable ctx to Execute
			result := strategy.Execute(ctx, assigned.ID, cmd, []string{}, envVars, p)

			//  EXTRACT HOST PORT FROM RESULT (if dynamic)
			if result.ChunkIndex > 0 {
				p.HostPort = result.ChunkIndex
			}

			// Check if this was an intentional kill from the Brain
			select {
			case <-ctx.Done():
				log.Printf("[supervisor] Service %s stopped intentionally (Brain signal).", assigned.ID)
				goto cleanup
			default:
				// Not a deliberate kill -> It crashed!
			}

			if attempts >= maxRetries {
				log.Printf("[supervisor] Service %s crashed too many times (%d). Giving up.", assigned.ID, attempts)
				// Notify the Brain of the permanent failure
				w.cm.SendRequest(protocol.OpTaskFailed, protocol.TaskFailedPayload{
					TaskID: assigned.ID,
					Reason: "Service crashed repeatedly during startup.",
				})
				goto cleanup
			}

			log.Printf("[supervisor] Service %s exited unexpectedly. Restarting in %v (Attempt %d/%d)...", assigned.ID, backoff, attempts, maxRetries)

			select {
			case <-time.After(backoff):
				backoff *= 2 // Exponential backoff
				continue     // RESTART
			case <-ctx.Done():
				goto cleanup
			}
		}

	cleanup:
		// CLEANUP ON EXIT (Whether it crashed or was intentionally killed)
		w.daemonMu.Lock()
		delete(w.activeDaemons, assigned.ID)
		// Clean up port tracking so heartbeat stops reporting this dead container
		if p.HostPort > 0 {
			delete(w.usedPorts, p.HostPort)
		}
		delete(w.adoptedJobs, p.JobID)
		w.daemonMu.Unlock()

		// --- PHASE 8: COLD STORAGE UPLOAD ---
		if assigned.UploadURL != "" {
			w.jobLogsMu.Lock()
			lines := w.jobLogs[p.JobID]
			fullLog := strings.Join(lines, "\n")
			w.jobLogsMu.Unlock()

			if fullLog != "" {
				log.Printf("[muscle] Uploading final logs for %s to cold storage...", p.JobID)
				err := w.UploadToAzure([]byte(fullLog), assigned.UploadURL)
				if err != nil {
					log.Printf("[muscle] Cold storage upload failed: %v", err)
				} else {
					log.Printf("[muscle] Final logs uploaded for %s", p.JobID)
				}
			}
		}

		// ALWAYS RELEASE RESOURCES WHEN THE GOROUTINE EXITS
		w.semaphore.Release(assigned.RequiredCPU, assigned.RequiredRAM)
	}()

	// 2. Readiness Probe
	localIP := getLocalIP()
	probeAddress := fmt.Sprintf("127.0.0.1:%d", p.HostPort)
	gatewayAddress := fmt.Sprintf("%s:%d", localIP, p.HostPort)

	log.Printf("[muscle] Waiting for Service to bind to http://%s/health (Gateway: %s)...", probeAddress, gatewayAddress)

	isReady := false
	for attempt := 0; attempt < 120; attempt++ { // 240s total timeout
		// Attempt 1: HTTP Health check
		resp, err := http.Get(fmt.Sprintf("http://%s/health", probeAddress))
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			isReady = true
			break
		}
		if err == nil {
			resp.Body.Close()
		}

		// Attempt 2: TCP Fallback (If port is open, the app is running!)
		conn, err := net.DialTimeout("tcp", probeAddress, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			log.Printf("⚠️ [muscle] Port %d is OPEN, but /health returns error. Considering READY anyway...", p.HostPort)
			isReady = true
			break
		}

		time.Sleep(2 * time.Second)
	}

	if !isReady {
		log.Printf(" [muscle] SERVICE TIMEOUT: Could not reach port %d after 240 seconds.", p.HostPort)
		w.cm.SendRequest(protocol.OpTaskFailed, protocol.TaskFailedPayload{
			TaskID: assigned.ID,
			Reason: fmt.Sprintf("Readiness probe failed for %s", probeAddress),
		})
		return protocol.TaskCompletePayload{}
	}

	log.Printf("[muscle] ✅ Service verified running at %s", gatewayAddress)

	// 3. Notify the Brain that the Service is ready
	role := p.ServiceRole
	if role == "" {
		role = "BACKEND_API"
	}

	w.cm.SendRequest(protocol.OpServiceReady, protocol.ServiceReadyPayload{
		JobID:       p.JobID,
		ServiceRole: role,
		Address:     gatewayAddress,
	})

	//  Track this service for re-registration after Brain reconnect
	if p.HostPort > 0 {
		w.daemonMu.Lock()
		w.usedPorts[p.HostPort] = p.JobID
		w.adoptedJobs[p.JobID] = protocol.ServiceReadyPayload{
			JobID:        p.JobID,
			ServiceRole:  role,
			Address:      gatewayAddress,
			RecoveredCPU: p.RequiredCPU,
			RecoveredRAM: p.RequiredRAM,
		}
		w.daemonMu.Unlock()
	}

	// 4. Return the Magic String
	return protocol.TaskCompletePayload{
		TaskID:    assigned.ID,
		JobID:     p.JobID,
		JobType:   protocol.JobTypeService,
		ResultURI: "DAEMON_RUNNING",
	}
}

func resolveStrategyName(ext string) string {
	switch ext {
	case ".js":
		return "execute_js"
	case ".cjs":
		return "execute_node"
	case ".py":
		return "execute_python"
	case ".cpp":
		return "execute_cpp"
	case ".go":
		return "execute_go"
	default:
		return ""
	}
}

// translateToContainerPath converts a host-absolute path to a container-absolute path
// assuming the hostRoot maps to /app.
func (w *Worker) translateToContainerPath(hostPath string) string {
	// 1. Convert to forward slashes for Linux compatibility
	cleanHostPath := filepath.ToSlash(hostPath)
	cleanHostRoot := filepath.ToSlash(w.hostRoot)

	// 2. If it's inside the host root, map it to /app
	if strings.HasPrefix(strings.ToLower(cleanHostPath), strings.ToLower(cleanHostRoot)) {
		rel := cleanHostPath[len(cleanHostRoot):]
		return path.Join("/app", rel)
	}

	// 3. If it's outside (rare for our setup), just return the slashified path
	return cleanHostPath
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

func (w *Worker) HandleKillTask(taskID string) {
	w.daemonMu.Lock()
	cancel, exists := w.activeDaemons[taskID]
	w.daemonMu.Unlock()

	if exists {
		log.Printf("[muscle] 💀 RECEIVED KILL SIGNAL FOR TASK %s. Executing termination...", taskID)
		w.daemonMu.Lock()
		w.killedTasks[taskID] = true
		// Also shield the base JobID (without -daemon) to silence the readiness probe in runTask
		jobID := strings.TrimSuffix(taskID, "-daemon")
		w.killedTasks[jobID] = true
		w.daemonMu.Unlock()
		cancel() // This cascades down to the Docker/Goja context and kills the process!

		//  FIX: Defensive nil-check prevents panics if the network drops (or in tests!)
		if w.cm != nil {
			w.cm.SendRequest(protocol.OpKillTaskAck, protocol.KillTaskPayload{
				TaskID: taskID,
			})
		}
	} else {
		// Check if it's an adopted job!
		jobID := strings.TrimSuffix(taskID, "-daemon")
		w.daemonMu.Lock()
		payload, isAdopted := w.adoptedJobs[jobID]
		w.daemonMu.Unlock()

		if isAdopted {
			log.Printf("[muscle] 💀 RECEIVED KILL SIGNAL FOR ADOPTED TASK %s. Executing forced termination...", taskID)
			w.daemonMu.Lock()
			w.killedTasks[taskID] = true
			w.killedTasks[jobID] = true // Shield base ID too
			w.daemonMu.Unlock()

			// Connect to Docker to forcefully destroy the adopted container
			cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
			if err == nil {
				containerName := fmt.Sprintf("bam-job-%s", jobID)
				cli.ContainerKill(context.Background(), containerName, "SIGKILL")
				cli.ContainerRemove(context.Background(), containerName, container.RemoveOptions{Force: true})
				log.Printf("[muscle] 💀 Adopted container %s completely destroyed.", containerName)
				cli.Close()
			} else {
				log.Printf(" [muscle] Docker connection failed during kill: %v", err)
			}

			// Cleanup local state
			w.daemonMu.Lock()
			parts := strings.Split(payload.Address, ":")
			if len(parts) == 2 {
				if port, err := strconv.Atoi(parts[1]); err == nil {
					delete(w.usedPorts, port)
				}
			}
			delete(w.adoptedJobs, jobID)
			w.daemonMu.Unlock()

			if w.cm != nil {
				w.cm.SendRequest(protocol.OpKillTaskAck, protocol.KillTaskPayload{
					TaskID: taskID,
				})
			}
		} else {
			log.Printf("[muscle] ⚠️ Received kill signal for %s, but it is not running here.", taskID)
		}
	}
}

func (w *Worker) getGatewayAddr() string {
	host := w.cm.GetActiveHost()
	//  Intelligent Transition for Docker Sandbox
	if host == "localhost" || host == "127.0.0.1" || host == "0.0.0.0" || host == "[::1]" || host == "::1" {
		// host.docker.internal is the canonical way for containers on Windows/Mac
		// to reach the host machine without hardcoding local LAN IPs.
		return "host.docker.internal"
	}
	return host
}

func getLocalIP() string {
	// 1. FAST PATH: Prioritize Tailscale (if active)
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ip := ipnet.IP.To4(); ip != nil {
					// Tailscale IPs always start with 100. (CGNAT range)
					if strings.HasPrefix(ip.String(), "100.") {
						log.Printf("[network] 🚀 Prioritizing Tailscale IP: %s", ip.String())
						return ip.String()
					}
				}
			}
		}
	}

	// 2. SECOND PATH: Dial internet to see which interface is used
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Printf("[network] ⚠️ No internet reachability, defaulting to loopback.")
		return "127.0.0.1"
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

func (w *Worker) handlePushMessage(msg protocol.Message) {
	if msg.OpCode == protocol.OpKillTask {
		var req protocol.KillTaskPayload
		if err := json.Unmarshal(msg.Payload, &req); err != nil {
			log.Printf("[push] Failed to decode KillTaskPayload: %v", err)
			return
		}

		// Pass it to the function you already wrote!
		w.HandleKillTask(req.TaskID)
	} else {
		log.Printf("[push] Unhandled push OpCode: 0x%X", msg.OpCode)
	}
}

// findAndLoadDotEnvFromDir looks for a .env file directly in the given host directory.
func (w *Worker) findAndLoadDotEnvFromDir(dir string) []string {
	log.Printf("[exec] 🔍 Looking for .env in %s...", dir)

	envFile := filepath.Join(dir, ".env")
	if _, err := os.Stat(envFile); err == nil {
		envs, err := godotenv.Read(envFile)
		if err == nil {
			var res []string
			for k, v := range envs {
				res = append(res, fmt.Sprintf("%s=%s", k, v))
			}
			log.Printf("[exec] ✅ Injecting %d variables from %s", len(res), envFile)
			return res
		}
	}
	return nil
}

// copyDirectorySafely performs a deep copy of a directory while ignoring massive/platform-specific generated data
func copyDirectorySafely(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Relativize path
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		targetPath := filepath.Join(dst, relPath)

		//  IGNORE LARGE/SYSTEM SPECIFIC DIRECTORIES
		if info.IsDir() {
			name := info.Name()
			if name == "node_modules" || name == ".git" || name == ".venv" || name == "__pycache__" || name == "dist" || name == "build" {
				return filepath.SkipDir
			}
			return os.MkdirAll(targetPath, info.Mode())
		}

		// Skip symlinks
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}

		// Read and write the file
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(targetPath, data, info.Mode())
	})
}
