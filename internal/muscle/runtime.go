package muscle

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"net"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
)

// GetFreePort asks the OS for a random open port
func GetFreePort() int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// 1. THE STRATEGY INTERFACE
type RuntimeStrategy interface {
	Execute(ctx context.Context, taskID string, script string, args []string, envVars []string, p protocol.Payload) protocol.TaskCompletePayload
}

// 2. THE REGISTRY / ENGINE
type ExecutionEngine struct {
	strategies map[string]RuntimeStrategy
}

func NewExecutionEngine() *ExecutionEngine {
	return &ExecutionEngine{
		strategies: make(map[string]RuntimeStrategy),
	}
}

func (e *ExecutionEngine) Register(command string, strategy RuntimeStrategy) {
	e.strategies[command] = strategy
}

// ==========================================
// 3. THE DOCKER WARM POOL STRATEGY (PYTHON)
// ==========================================
type DockerWarmPoolRuntime struct {
	cli         *client.Client
	containerID string
	execCmd     []string
	hostRoot    string
}

// FIX: Added 'exposedPorts []string' and 'hostRoot string' to signature
func InitDockerWarmPool(imgName string, execCmd []string, exposedPorts []string, hostRoot string) *DockerWarmPoolRuntime {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("[warmpool] failed to connect to Docker daemon: %v", err)
	}

	cwd := hostRoot

	//  Standardized Auto-Pull
	ensureImagePresent(ctx, cli, imgName, nil)

	//  NEW: Set up Port Bindings for the Host OS
	portSet := nat.PortSet{}
	portMap := nat.PortMap{}

	for _, port := range exposedPorts {
		p, err := nat.NewPort("tcp", port)
		if err == nil {
			portSet[p] = struct{}{}
			// Bind the container port to the Host OS on all network interfaces (0.0.0.0)
			portMap[p] = []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: port}}
		}
	}

	//  NEW: Stable Naming for Warm Pools
	poolName := fmt.Sprintf("bam-pool-%s", strings.ReplaceAll(imgName, ":", "-"))

	var containerID string

	//  Check if already exists!
	inspect, err := cli.ContainerInspect(ctx, poolName)
	if err == nil {
		log.Printf("♻️ [warmpool] REUSED existing container: %s (ID: %s)", poolName, inspect.ID[:12])
		containerID = inspect.ID
	} else {
		// Only create if it doesn't exist
		// Create a sleeping container
		resp, err := cli.ContainerCreate(ctx, &container.Config{
			Image:      imgName,
			Cmd:        []string{"sleep", "infinity"},
			WorkingDir: "/app",
			Labels: map[string]string{
				"bam.job_id":       poolName,
				"bam.required_cpu": "0", // Warm pools are pre-initialized shared resources
				"bam.required_ram": "0",
			},
			ExposedPorts: portSet, //  Apply exposed ports
		}, &container.HostConfig{
			AutoRemove: false, //  MUST BE FALSE FOR IMMORTALITY
			Binds: []string{
				fmt.Sprintf("%s:/app", cwd),
				fmt.Sprintf("%s/reports:/app/reports", cwd),
			},
			PortBindings: portMap, //  Apply host port mappings
			RestartPolicy: container.RestartPolicy{
				Name: "unless-stopped", //  SURVIVE DOCKER REBOOTS
			},
		}, nil, nil, poolName)

		if err != nil {
			log.Fatalf(" [warmpool] FATAL: Could not create container. Error: %v", err)
		}
		containerID = resp.ID
	}

	if err := cli.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		log.Fatalf(" [warmpool] FATAL: Failed to start container: %v", err)
	}

	displayID := containerID
	if len(displayID) > 12 {
		displayID = displayID[:12]
	}
	log.Printf("✅ [warmpool] %s initialized! (ID: %s)", imgName, displayID)

	return &DockerWarmPoolRuntime{
		cli:         cli,
		containerID: containerID,
		execCmd:     execCmd,
		hostRoot:    cwd,
	}
}

func (d *DockerWarmPoolRuntime) Execute(ctx context.Context, taskID string, script string, args []string, envVars []string, p protocol.Payload) protocol.TaskCompletePayload {
	mappedScript := script
	if strings.Contains(mappedScript, d.hostRoot) {
		mappedScript = strings.ReplaceAll(mappedScript, d.hostRoot, "/app")
		mappedScript = strings.ReplaceAll(mappedScript, "\\", "/")
	}
	fullCmd := append(d.execCmd, mappedScript)
	fullCmd = append(fullCmd, args...)

	//  Translate Host UserScriptPath to Container /app path
	mappedScriptPath := p.UserScriptPath
	if strings.HasPrefix(mappedScriptPath, d.hostRoot) {
		rel := strings.TrimPrefix(mappedScriptPath, d.hostRoot)
		rel = strings.ReplaceAll(rel, "\\", "/")
		mappedScriptPath = "/app" + rel
	}

	// Combine passed envVars with defaults
	finalEnv := append(envVars, "PYTHONPATH=/app/python", "USER_SCRIPT_PATH="+mappedScriptPath)

	// Inject the dynamic Service Discovery IP!
	if psEndpoint, exists := p.TopologyMap["PS_ENDPOINT"]; exists {
		finalEnv = append(finalEnv, "PS_ENDPOINT="+psEndpoint)
	}

	execResp, err := d.cli.ContainerExecCreate(ctx, d.containerID, container.ExecOptions{
		AttachStdout: true,
		AttachStderr: true,
		Env:          finalEnv,
		Cmd:          fullCmd,
	})

	if err != nil {
		log.Printf("[polyglot] Failed to create exec session: %v", err)
		return protocol.TaskCompletePayload{}
	}

	attachResp, err := d.cli.ContainerExecAttach(ctx, execResp.ID, container.ExecStartOptions{})
	if err != nil {
		log.Printf("[polyglot] Failed to attach: %v", err)
		return protocol.TaskCompletePayload{}
	}
	defer attachResp.Close()

	var stdout, stderr bytes.Buffer
	stdcopy.StdCopy(&stdout, &stderr, attachResp.Reader)

	if stderr.Len() > 0 {
		log.Printf(" [polyglot] SCRIPT ERROR: %s", stderr.String())
	}

	resultStr := strings.TrimSpace(stdout.String())
	log.Printf("[polyglot] Executed %s -> %s", p.Command, resultStr)

	return protocol.TaskCompletePayload{
		TaskID:     taskID,
		JobID:      p.JobID,
		JobType:    p.JobType,
		ChunkIndex: p.ChunkIndex,
		ResultURI:  resultStr,
	}
}

// ==========================================
// 4. THE DOCKER FRESH RUNTIME (FOR SERVICES)
// ==========================================
// This runtime creates a NEW container for every task and destroys it on exit.
// This is essential for SERVICE jobs that bind to host ports.
type DockerFreshRuntime struct {
	cli       *client.Client
	image     string
	execCmd   []string
	ports     []string
	hostRoot  string
	logWriter func(jobID, taskID, line string)
	cacheRoot string
}

func NewDockerFreshRuntime(imgName string, execCmd []string, exposedPorts []string, hostRoot string, logWriter func(string, string, string), cacheRoot string) *DockerFreshRuntime {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("[fresh] failed to connect to Docker daemon: %v", err)
	}
	return &DockerFreshRuntime{
		cli:       cli,
		image:     imgName,
		execCmd:   execCmd,
		ports:     exposedPorts,
		hostRoot:  hostRoot,
		logWriter: logWriter,
		cacheRoot: cacheRoot,
	}
}

func (d *DockerFreshRuntime) Execute(ctx context.Context, taskID string, script string, args []string, envVars []string, p protocol.Payload) protocol.TaskCompletePayload {
	var mountDir string
	if p.SourceDir != "" {
		mountDir = p.SourceDir
	} else {
		mountDir = d.hostRoot
	}

	// 1. Port Mappings
	portSet := nat.PortSet{}
	portMap := nat.PortMap{}

	// Use the task's ExposedPort if set, otherwise fall back to the strategy default
	ports := d.ports
	if p.ExposedPort > 0 {
		ports = []string{fmt.Sprintf("%d", p.ExposedPort)}
	}

	hostPortAllocated := 0
	for _, port := range ports {
		lp, _ := nat.NewPort("tcp", port)
		portSet[lp] = struct{}{}

		// If the worker allocated a dynamic host port, use it!
		hostPortStr := port
		if p.HostPort > 0 {
			hostPortStr = fmt.Sprintf("%d", p.HostPort)
			hostPortAllocated, _ = strconv.Atoi(hostPortStr)
		} else if p.ExposedPort > 0 {
			//  BLUE/GREEN: Dynamically allocate a free port if not provided
			hostPortAllocated = GetFreePort()
			hostPortStr = fmt.Sprintf("%d", hostPortAllocated)
			log.Printf("[fresh] 💡 Dynamic Port Allocation: Assigned %s -> %s", port, hostPortStr)
		}

		portMap[lp] = []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: hostPortStr}}
	}

	//  INJECT PORT ENV VAR (so the app knows where to bind)
	if p.ExposedPort > 0 {
		envVars = append(envVars, fmt.Sprintf("PORT=%d", p.ExposedPort))
	}

	mappedScript := script
	if strings.Contains(mappedScript, mountDir) {
		mappedScript = strings.ReplaceAll(mappedScript, mountDir, "/app")
		mappedScript = strings.ReplaceAll(mappedScript, "\\", "/")
	}

	// 2. Command Construction
	var fullCmd []string

	// Smart Command Detection:
	// If the script already looks like a full command line (e.g. "python3 -u ...")
	// we should run it via 'sh -c' to avoid doubling up the default execCmd.
	isFullCmd := strings.Contains(mappedScript, " ") ||
		(len(d.execCmd) > 0 && strings.HasPrefix(mappedScript, d.execCmd[0]))

	if isFullCmd {
		// Use sh -c to execute the full command string
		fullCmd = []string{"sh", "-c", mappedScript}
	} else {
		// Regular case: append script to the default runner
		fullCmd = append(d.execCmd, mappedScript)
		fullCmd = append(fullCmd, args...)
	}

	//  Standardized Container Naming
	containerName := fmt.Sprintf("bam-job-%s", taskID)

	var containerID string

	// 1. Check if already exists!
	inspect, err := d.cli.ContainerInspect(ctx, containerName)
	if err == nil {
		//  FIX: Only reuse the container if it is currently RUNNING
		if inspect.State.Running {
			log.Printf("♻️ [fresh] REUSED RUNNING container: %s (ID: %s)", containerName, inspect.ID[:12])
			containerID = inspect.ID
		} else {
			log.Printf("🗑️ [fresh] Found DEAD container %s. Removing it to start fresh...", containerName)
			d.cli.ContainerRemove(context.Background(), inspect.ID, container.RemoveOptions{Force: true})
		}
	}

	if containerID == "" {
		//  ROBUSTNESS: Check if image exists, pull if missing!
		ensureImagePresent(ctx, d.cli, d.image, func(line string) {
			if d.logWriter != nil {
				d.logWriter(p.JobID, taskID, line)
			}
		})

		// 2. Create Container
		resp, err := d.cli.ContainerCreate(ctx, &container.Config{
			Image:      d.image,
			Cmd:        fullCmd,
			Env:        envVars,
			WorkingDir: filepath.ToSlash(filepath.Join("/app", p.RootDirectory)),
			Labels: map[string]string{
				"bam.job_id":          p.JobID,
				"bam.required_cpu":    fmt.Sprintf("%.1f", p.RequiredCPU),
				"bam.required_ram":    fmt.Sprintf("%d", p.RequiredRAM),
				"bam.original_submit": p.OriginalSubmitBytes,
			},
			ExposedPorts: portSet,
			Volumes: map[string]struct{}{
				"/app/node_modules": {},
				"/app/.venv":        {},
			},
		}, &container.HostConfig{
			AutoRemove:   false,
			PortBindings: portMap,
			Resources: container.Resources{
				NanoCPUs: int64(p.RequiredCPU * 1e9),
				Memory:   int64(p.RequiredRAM * 1024 * 1024),
			},
			Binds: []string{
				fmt.Sprintf("%s:/app", mountDir),
				//  PERSISTENT CACHES
				fmt.Sprintf("%s/go-mod:/go/pkg/mod", d.cacheRoot),
				fmt.Sprintf("%s/node-modules:/root/.npm", d.cacheRoot),
				fmt.Sprintf("%s/pip-cache:/root/.cache", d.cacheRoot),
			},
			RestartPolicy: container.RestartPolicy{
				Name: "unless-stopped",
			},
		}, nil, nil, containerName)

		if err != nil {
			log.Printf("[fresh] failed to create container: %v", err)
			return protocol.TaskCompletePayload{}
		}
		containerID = resp.ID
	}

	// 3. Start with Retry Logic (for port allocation lag)
	var startErr error
	for i := 0; i < 3; i++ {
		startErr = d.cli.ContainerStart(ctx, containerID, container.StartOptions{})
		if startErr == nil {
			break
		}
		if strings.Contains(startErr.Error(), "port is already allocated") {
			log.Printf("[fresh] ⏳ Port busy, retrying in 1s... (attempt %d/3)", i+1)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	if startErr != nil {
		log.Printf("[fresh] failed to start container: %v", startErr)
		return protocol.TaskCompletePayload{}
	}

	log.Printf("[fresh] service container %s started for task %s (mount: %s)", containerID[:12], taskID, mountDir)

	// Stream logs in background so we can see the console output
	go func() {
		out, err := d.cli.ContainerLogs(context.Background(), containerID, container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
		})
		if err == nil {
			defer out.Close()
			// stdcopy.StdCopy(os.Stdout, os.Stderr, out)
			// Read Docker multiplexed stream
			pr, pw := io.Pipe()

			go func() {
				// stdcopy strips the 8-byte Docker multiplex headers and redirects payload
				stdcopy.StdCopy(pw, pw, out)
				pw.Close()
			}()

			// Read the clean output line by line
			scanner := bufio.NewScanner(pr)
			for scanner.Scan() {
				line := scanner.Text()
				// Write to the global job log multiplexer for the frontend
				if d.logWriter != nil {
					d.logWriter(p.JobID, taskID, line)
				}
			}
		}
	}()

	// Wait for container to exit or context to be cancelled
	statusCh, errCh := d.cli.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			log.Printf("[fresh] wait error: %v", err)
		}
	case <-statusCh:
		log.Printf("[fresh] container %s exited naturally", containerID[:12])
		d.cli.ContainerRemove(context.Background(), containerID, container.RemoveOptions{Force: true})
	case <-ctx.Done():
		log.Printf("[fresh] context cancelled, killing and removing container %s...", containerID[:12])
		// Force kill the container
		d.cli.ContainerKill(context.Background(), containerID, "SIGKILL")
		// Give it a moment to stop
		time.Sleep(500 * time.Millisecond)
		// Force remove the container so it can be re-created on restart
		d.cli.ContainerRemove(context.Background(), containerID, container.RemoveOptions{Force: true})
		log.Printf("[fresh] container %s killed and removed", containerID[:12])
	}

	return protocol.TaskCompletePayload{
		TaskID:     taskID,
		JobID:      p.JobID,
		ResultURI:  "DAEMON_RUNNING",
		ChunkIndex: hostPortAllocated, //  HACK: Pass the assigned host port back in ChunkIndex!
	}
}

// ==========================================================================
// 5. THE DOCKER TASK RUNTIME (For MAP, GATHER, ML_WORKER)
// ==========================================================================
// This runtime executes a generic script in a fresh container, streams logs
// to the Job Logger, returns the result, and immediately destroys the container.
type DockerTaskRuntime struct {
	cli       *client.Client
	image     string
	execCmd   []string
	logWriter func(jobID, taskID, line string)
	hostRoot  string
}

func NewDockerTaskRuntime(imgName string, execCmd []string, logWriter func(jobID, taskID, line string), hostRoot string) *DockerTaskRuntime {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("[task] failed to connect to Docker daemon: %v", err)
	}
	return &DockerTaskRuntime{
		cli:       cli,
		image:     imgName,
		execCmd:   execCmd,
		logWriter: logWriter,
		hostRoot:  hostRoot,
	}
}

func (d *DockerTaskRuntime) Execute(ctx context.Context, taskID string, script string, args []string, envVars []string, p protocol.Payload) protocol.TaskCompletePayload {
	var mountDir string
	if p.SourceDir != "" {
		mountDir = p.SourceDir
	} else {
		mountDir = d.hostRoot
	}

	mappedScript := script
	if strings.Contains(mappedScript, mountDir) {
		mappedScript = strings.ReplaceAll(mappedScript, mountDir, "/app")
		mappedScript = strings.ReplaceAll(mappedScript, "\\", "/")
	}

	var fullCmd []string

	// Smart Command Detection for Tasks:
	isFullCmd := strings.Contains(mappedScript, " ") ||
		(len(d.execCmd) > 0 && strings.HasPrefix(mappedScript, d.execCmd[0]))

	if isFullCmd {
		fullCmd = []string{"sh", "-c", mappedScript}
	} else {
		fullCmd = append(d.execCmd, mappedScript)
		fullCmd = append(fullCmd, args...)
	}

	// Append dynamic injection variables for standard scripts
	finalEnv := append(envVars, "PYTHONPATH=/app/python", "NODE_PATH=/app/node_modules")
	if psEndpoint, exists := p.TopologyMap["PS_ENDPOINT"]; exists {
		finalEnv = append(finalEnv, "PS_ENDPOINT="+psEndpoint)
	}

	//  UNIQUE CONTAINER NAME FOR TASKS TO AVOID COLLISIONS
	containerName := fmt.Sprintf("bam-task-%s", taskID)

	ensureImagePresent(ctx, d.cli, d.image, func(line string) {
		if d.logWriter != nil {
			d.logWriter(p.JobID, taskID, line)
		}
	})

	resp, err := d.cli.ContainerCreate(ctx, &container.Config{
		Image:      d.image,
		Cmd:        fullCmd,
		Env:        finalEnv,
		WorkingDir: filepath.ToSlash(filepath.Join("/app", p.RootDirectory)),
		Labels: map[string]string{
			"bam.job_id":  p.JobID,
			"bam.task_id": taskID,
		},
	}, &container.HostConfig{
		AutoRemove: false, // We remove it manually to ensure we capture the exit status
		Resources: container.Resources{
			NanoCPUs: int64(p.RequiredCPU * 1e9),
			Memory:   int64(p.RequiredRAM * 1024 * 1024),
		},
		Binds: []string{
			fmt.Sprintf("%s:/app", mountDir),
		},
	}, nil, nil, containerName)

	if err != nil {
		log.Printf("[task] failed to create container for task %s: %v", taskID, err)
		return protocol.TaskCompletePayload{}
	}

	containerID := resp.ID

	if err := d.cli.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		log.Printf("[task] failed to start container %s: %v", containerID[:12], err)
		return protocol.TaskCompletePayload{}
	}

	log.Printf("[task] container %s started for task %s", containerID[:12], taskID)

	// 2. Attach to Logs and Multiplex!
	out, logsErr := d.cli.ContainerLogs(context.Background(), containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	})

	var stdoutBuf bytes.Buffer

	if logsErr == nil {
		// Read Docker multiplexed stream
		pr, pw := io.Pipe()

		go func() {
			// stdcopy strips the 8-byte Docker multiplex headers and redirects payload
			stdcopy.StdCopy(pw, pw, out)
			pw.Close()
		}()

		// Read the clean output line by line
		go func() {
			scanner := bufio.NewScanner(pr)
			for scanner.Scan() {
				line := scanner.Text()
				// Write to the global job log multiplexer for the frontend
				if d.logWriter != nil {
					d.logWriter(p.JobID, taskID, line)
				}
				// Also save to stdout buffer to look for ResultURI at the end
				stdoutBuf.WriteString(line + "\n")
			}
		}()
	} else {
		log.Printf("[task] failed to attach logs for %s: %v", taskID, logsErr)
	}

	// 3. Wait for execution to finish
	statusCh, errCh := d.cli.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			log.Printf("[task] wait error: %v", err)
		}
	case <-statusCh:
		log.Printf("[task] container %s exited naturally", containerID[:12])
	case <-ctx.Done():
		log.Printf("[task] context cancelled! Killing container...")
		d.cli.ContainerKill(context.Background(), containerID, "SIGKILL")
	}

	// 4. Force Cleanup
	d.cli.ContainerRemove(context.Background(), containerID, container.RemoveOptions{Force: true})
	log.Printf("[task] 🧹 Cleaned up ephemeral container %s", containerID[:12])

	// 5. Build the return payload
	resultStr := strings.TrimSpace(stdoutBuf.String())

	// If it's a map task, look for the 'Result: ' token
	if p.JobType == "MAP" {
		marker := "Result: "
		idx := strings.LastIndex(resultStr, marker)
		if idx != -1 {
			// Extract just the part after the marker up to the newline
			cleanEnd := resultStr[idx+len(marker):]
			if firstNewline := strings.Index(cleanEnd, "\n"); firstNewline != -1 {
				cleanEnd = cleanEnd[:firstNewline]
			}
			resultStr = strings.TrimSpace(cleanEnd)
		}
	}

	return protocol.TaskCompletePayload{
		TaskID:     taskID,
		JobID:      p.JobID,
		JobType:    p.JobType,
		ChunkIndex: p.ChunkIndex,
		ResultURI:  resultStr,
	}
}

// function to fetch the IP address of the Docker container
func (d *DockerWarmPoolRuntime) GetContainerIP() string {
	ctx := context.Background()

	// Inspect the container via the Docker API
	inspect, err := d.cli.ContainerInspect(ctx, d.containerID)
	if err != nil {
		log.Printf("[warmpool] Failed to inspect container: %v", err)
		return ""
	}

	var ip string
	if len(inspect.NetworkSettings.Networks) > 0 {
		for _, network := range inspect.NetworkSettings.Networks {
			if network.IPAddress != "" {
				ip = network.IPAddress
				break
			}
		}
	}

	if ip == "" {
		ip = inspect.NetworkSettings.IPAddress
	}

	return ip
}

// SHARED HELPER: Ensure a Docker image is present locally, pulling if missing.
func ensureImagePresent(ctx context.Context, cli *client.Client, imgName string, logFunc func(string)) {
	_, _, err := cli.ImageInspectWithRaw(ctx, imgName)
	if err == nil {
		return // Already here!
	}

	log.Printf("[docker] 📥 Image %s not found locally. Pulling from registry...", imgName)
	if logFunc != nil {
		logFunc(fmt.Sprintf("%s [system] 📥 Image %s not found locally. Initializing pull...", time.Now().Format("2006/01/02 15:04:05"), imgName))
	}

	out, err := cli.ImagePull(ctx, imgName, image.PullOptions{})
	if err != nil {
		log.Printf("[docker]  Failed to pull image %s: %v", imgName, err)
		if logFunc != nil {
			logFunc(fmt.Sprintf("%s [system]  ERROR: Failed to pull image: %v", time.Now().Format("2006/01/02 15:04:05"), err))
		}
		return
	}
	defer out.Close()

	//  SMART PROGRESS TRACKING
	// Parse the Docker JSON stream to provide real-time feedback in the UI
	scanner := bufio.NewScanner(out)
	var lastStatus string
	for scanner.Scan() {
		var msg struct {
			Status   string `json:"status"`
			Progress string `json:"progress"`
			ID       string `json:"id"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &msg); err == nil {
			if msg.Status != "" && (msg.Status != lastStatus || msg.Progress != "") {
				display := fmt.Sprintf("[docker] %s", msg.Status)
				if msg.ID != "" {
					display = fmt.Sprintf("[docker] %s: %s", msg.ID, msg.Status)
				}
				if msg.Progress != "" {
					display += " " + msg.Progress
				}

				// Only log status changes or lines with progress bars to avoid flooding
				if msg.Progress != "" || msg.Status != lastStatus {
					if logFunc != nil {
						logFunc(fmt.Sprintf("%s %s", time.Now().Format("15:04:05"), display))
					}
					lastStatus = msg.Status
				}
			}
		}
	}

	log.Printf("[docker] ✅ Successfully pulled image %s", imgName)
	if logFunc != nil {
		logFunc(fmt.Sprintf("%s [system] ✅ Image %s pulled successfully.", time.Now().Format("2006/01/02 15:04:05"), imgName))
	}
}

func UploadToAzure(outputData []byte, sasURL string) (string, error) {
	if sasURL == "" {
		return "", fmt.Errorf("no SAS URL provided by Brain")
	}

	req, err := http.NewRequest("PUT", sasURL, bytes.NewBuffer(outputData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	//  CRITICAL AZURE HEADER: Microsoft will reject the upload without this exact string.
	req.Header.Set("x-ms-blob-type", "BlockBlob")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(outputData)))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("network error uploading to Azure: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("azure rejected upload with status: %d", resp.StatusCode)
	}

	log.Printf("[muscle] ☁️ Successfully pushed %d bytes directly to Azure Blob Storage", len(outputData))

	// Return the base URL (without the secret signature) so the frontend can use it
	// Or simply return the original sasURL if you want to reuse the token.
	return sasURL, nil
}
