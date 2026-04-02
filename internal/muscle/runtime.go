package muscle

import (
	"bytes"
	"context"
	"encoding/json"
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
	Execute(taskID string, script string, args []string, p protocol.Payload) protocol.TaskCompletePayload
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

	log.Printf("[warmpool] Checking/Pulling image: %s (this might take a minute)...", imgName)
	reader, err := cli.ImagePull(ctx, imgName, image.PullOptions{})
	if err == nil {
		io.Copy(os.Stdout, reader)
		reader.Close()
	}

	// 🚨 NEW: Get the Muscle's own Container ID
	muscleContainerID, _ := os.Hostname()

	// Create a sleeping container
	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image: imgName,
		Cmd:   []string{"sleep", "infinity"},
	}, &container.HostConfig{
		// 🚨 THE FIX: Inherit the exact physical volume mounts from the Muscle worker!
		VolumesFrom: []string{muscleContainerID},
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
	log.Printf("🔥 [warmpool] %s initialized! (ID: %s)", imgName, displayID)

	return &DockerWarmPoolRuntime{
		cli:         cli,
		containerID: resp.ID,
		execCmd:     execCmd,
	}
}

func (d *DockerWarmPoolRuntime) Execute(taskID string, script string, args []string, p protocol.Payload) protocol.TaskCompletePayload {
	ctx := context.Background()

	if p.JobType == "GATHER" {
		inputsJSON, _ := json.Marshal(p.InputURIs)
		args = append(args, string(inputsJSON))
	}

	fullCmd := append(d.execCmd, script)
	fullCmd = append(fullCmd, args...)

	execResp, err := d.cli.ContainerExecCreate(ctx, d.containerID, container.ExecOptions{
		AttachStdout: true,
		AttachStderr: true,
		Env: []string{
			"CHUNK_INDEX=" + fmt.Sprintf("%d", p.ChunkIndex),
			"JOB_TYPE=" + p.JobType,
		},
		Cmd: fullCmd,
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

func (g *GojaRuntime) Execute(taskID string, script string, args []string, p protocol.Payload) protocol.TaskCompletePayload {
	// We route it to your existing JS functions based on job type
	if p.JobType == "GATHER" {
		return executeJSGather(taskID, script, p)
	}
	return executeJSTask(taskID, script, args, p)
}
