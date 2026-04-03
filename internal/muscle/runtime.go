package muscle

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

// 1. THE STRATEGY INTERFACE
type RuntimeStrategy interface {
	Execute(taskID string, script string, args []string, envVars []string, p protocol.Payload) protocol.TaskCompletePayload
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
}

func InitDockerWarmPool(imgName string, execCmd []string) *DockerWarmPoolRuntime {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("[warmpool] failed to connect to Docker daemon: %v", err)
	}

	// NEW: Get the physical path to your project root
	cwd, _ := os.Getwd()
	cwd = strings.TrimSuffix(cwd, "\\cmd\\muscle") // Handle Windows path if run from subfolder
	cwd = strings.TrimSuffix(cwd, "/cmd/muscle")   // Handle Linux/Mac path

	log.Printf("[warmpool] Checking/Pulling image: %s ...", imgName)
	reader, err := cli.ImagePull(ctx, imgName, image.PullOptions{})
	if err == nil {
		io.Copy(os.Stdout, reader)
		reader.Close()
	}

	// Create a sleeping container
	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image:      imgName,
		Cmd:        []string{"sleep", "infinity"},
		WorkingDir: "/app", // Set working directory for Python
	}, &container.HostConfig{
		AutoRemove: true,
		Binds: []string{
			fmt.Sprintf("%s:/app", cwd),
			fmt.Sprintf("%s/reports:/app/reports", cwd), // Added reports volume
		},
	}, nil, nil, "")

	if err != nil {
		log.Fatalf("🚨 [warmpool] FATAL: Could not create container. Error: %v", err)
	}

	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		log.Fatalf("🚨 [warmpool] FATAL: Failed to start container: %v", err)
	}

	displayID := resp.ID
	if len(displayID) > 12 {
		displayID = displayID[:12]
	}
	log.Printf("✅ [warmpool] %s initialized! (ID: %s)", imgName, displayID)

	return &DockerWarmPoolRuntime{
		cli:         cli,
		containerID: resp.ID,
		execCmd:     execCmd,
	}
}

func (d *DockerWarmPoolRuntime) Execute(taskID string, script string, args []string, envVars []string, p protocol.Payload) protocol.TaskCompletePayload {
	ctx := context.Background()

	fullCmd := append(d.execCmd, script)
	fullCmd = append(fullCmd, args...)

	// Combine passed envVars with defaults
	finalEnv := append(envVars, "PYTHONPATH=/app/python", "USER_SCRIPT_PATH="+p.UserScriptPath)

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
		log.Printf("🚨 [polyglot] SCRIPT ERROR: %s", stderr.String())
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
// 4. THE JAVASCRIPT STRATEGY (GOJA)
// ==========================================
type GojaRuntime struct{}

func (g *GojaRuntime) Execute(taskID string, script string, args []string, envVars []string, p protocol.Payload) protocol.TaskCompletePayload {
	// We route it to your existing JS functions based on job type
	if p.JobType == "GATHER" {
		return executeJSGather(taskID, script, p)
	}
	return executeJSTask(taskID, script, args, p)
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
