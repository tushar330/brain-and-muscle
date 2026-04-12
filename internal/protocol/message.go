package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

const MagicWord = "BAM!" // 0x42 0x41 0x4d 0x21

// OpCodes
const (
	OpHeartbeat            uint8 = 0x01
	OpHeartbeatAck         uint8 = 0x02
	OpRegisterWorker       uint8 = 0x03
	OpWorkerRegistered     uint8 = 0x04
	OpRequestTask          uint8 = 0x05
	OpTaskAssigned         uint8 = 0x06
	OpNoTasksAvailable     uint8 = 0x07
	OpSubmitTask           uint8 = 0x08
	OpTaskAccepted         uint8 = 0x09
	OpSubmitResult         uint8 = 0x0A
	OpTaskCompleted        uint8 = 0x0B
	OpGetTaskStatus        uint8 = 0x0C
	OpTaskStatus           uint8 = 0x0D
	OpTaskFailed           uint8 = 0x0E
	OpTaskFailedAck        uint8 = 0x0F
	OpError                uint8 = 0x10
	OpTick                 uint8 = 0x11
	OpRequestVote          uint8 = 0x12
	OpRequestVoteReply     uint8 = 0x13
	OpAppendEntries        uint8 = 0x14
	OpAppendEntriesReply   uint8 = 0x15
	OpRequestVoteAck       uint8 = 0x16
	OpAppendEntriesAck     uint8 = 0x17
	OpRedirect             uint8 = 0x18
	OpRaftTick             uint8 = 0x19
	OpInstallSnapshot      uint8 = 0x1A
	OpInstallSnapshotReply uint8 = 0x1B
	OpServiceReady         uint8 = 0x1C
	OpServiceReadyAck      uint8 = 0x1D
	OpKillTask             uint8 = 0x1E
	OpKillTaskAck          uint8 = 0x1F
	OpRestartTask          uint8 = 0x20
)

type Message struct {
	OpCode    uint8           `json:"-"`
	NodeID    string          `json:"node_id"`
	RequestID string          `json:"request_id"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}

/*
	PackEnvelope creates a zero-allocation binary frame.

Format:
[4 bytes] Body Length
[1 byte]  OpCode
[2 bytes] NodeID Length
[N bytes] NodeID String
[2 bytes] RequestID Length
[N bytes] RequestID String
[8 bytes] Timestamp (Unix Nano)
[4 bytes] Payload Length
[N bytes] Payload Bytes
*/
func PackEnvelope(opCode uint8, nodeID string, reqID string, payload []byte) []byte {
	nodeIDLen := len(nodeID)
	reqIDLen := len(reqID)
	payloadLen := len(payload)

	// Calculate exact buffer size to avoid slice reallocation
	// Header format: [4 bytes Magic] [4 bytes Body Length] [1 byte OpCode]
	totalSize := 4 + 5 + 2 + nodeIDLen + 2 + reqIDLen + 8 + 4 + payloadLen

	buf := make([]byte, totalSize)
	offset := 0

	// Magic Word
	copy(buf[offset:offset+4], MagicWord)
	offset += 4

	// Frame Header
	bodyLength := uint32(totalSize - 9)
	binary.BigEndian.PutUint32(buf[offset:offset+4], bodyLength)
	offset += 4

	buf[offset] = opCode
	offset += 1

	// Message Body
	// NodeID
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(nodeIDLen))
	offset += 2
	copy(buf[offset:offset+nodeIDLen], nodeID)
	offset += nodeIDLen

	// RequestID
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(reqIDLen))
	offset += 2
	copy(buf[offset:offset+reqIDLen], reqID)
	offset += reqIDLen

	// Timestamp
	now := time.Now().UnixNano()
	binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(now))
	offset += 8

	// Payload
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(payloadLen))
	offset += 4
	copy(buf[offset:offset+payloadLen], payload)

	return buf
}

// UnpackEnvelope reads the raw body (excluding the 5-byte header) and extracts the fields without using reflection.
func UnpackEnvelope(body []byte) (nodeID string, reqID string, timestamp int64, payload []byte, err error) {
	if len(body) < 16 { // min body: nodeLen(2) + reqLen(2) + time(8) + payloadLen(4)
		return "", "", 0, nil, fmt.Errorf("body too short to unpack")
	}

	offset := 0 // Skip Magic (4) + Length (4) + OpCode (1) was handled by caller

	// NodeID
	nodeIDLen := int(binary.BigEndian.Uint16(body[offset : offset+2]))
	offset += 2
	if offset+nodeIDLen > len(body) {
		return "", "", 0, nil, fmt.Errorf("malformed NodeID length")
	}
	nodeID = string(body[offset : offset+nodeIDLen])
	offset += nodeIDLen

	// RequestID
	reqIDLen := int(binary.BigEndian.Uint16(body[offset : offset+2]))
	offset += 2
	if offset+reqIDLen > len(body) {
		return "", "", 0, nil, fmt.Errorf("malformed RequestID length")
	}
	reqID = string(body[offset : offset+reqIDLen])
	offset += reqIDLen

	// Timestamp
	if offset+8 > len(body) {
		return "", "", 0, nil, fmt.Errorf("malformed timestamp")
	}
	timestamp = int64(binary.BigEndian.Uint64(body[offset : offset+8]))
	offset += 8

	// Payload
	if offset+4 > len(body) {
		return "", "", 0, nil, fmt.Errorf("malformed payload length")
	}
	payloadLen := int(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4
	if offset+payloadLen > len(body) {
		return "", "", 0, nil, fmt.Errorf("payload truncated")
	}
	payload = body[offset : offset+payloadLen]

	return nodeID, reqID, timestamp, payload, nil
}

const (
	JobTypeMLPS     = "ML_PS"
	JobTypeMLWorker = "ML_WORKER"
	JobTypeService  = "SERVICE"
	JobTypeGeneric  = "GENERIC"
)

type Payload struct {
	JobType             string            `json:"job_type"`
	JobID               string            `json:"job_id"`
	ChunkIndex          int               `json:"chunk_index"`
	AttemptNumber       int               `json:"attempt_number"`
	Command             string            `json:"command"`
	Args                []string          `json:"args"`       // standard task arguments (Version A)
	InputURIs           []string          `json:"input_uris"` // MAP chunk outputs forwarded to GATHER (Version B)
	TopologyMap         map[string]string `json:"topology_map,omitempty"`
	Env                 map[string]string `json:"env,omitempty"`
	BuildCommand        string            `json:"build_command,omitempty"`  //  NEW (Manual build script)
	RunCommand          string            `json:"run_command,omitempty"`    //  NEW (Manual run script)
	RootDirectory       string            `json:"root_directory,omitempty"` //  NEW (Working directory relative to /app)
	UserScriptPath      string            `json:"user_script_path,omitempty"`
	GatherScriptPath    string            `json:"gather_script_path,omitempty"`
	ExposedPort         int               `json:"exposed_port,omitempty"`
	HostPort            int               `json:"host_port,omitempty"`
	RequiredCPU         float64           `json:"required_cpu,omitempty"`
	RequiredRAM         int               `json:"required_ram,omitempty"`
	SourceType          string            `json:"source_type,omitempty"`  // "github" | "local"
	SourceDir           string            `json:"source_dir,omitempty"`   // Host directory to bind-mount
	ServiceRole         string            `json:"service_role,omitempty"` //  NEW: "BACKEND_API" | "PS_ENDPOINT"
	GitRepo             string            `json:"git_repo,omitempty"`     //  NEW for Blue/Green
	OriginalSubmitBytes string            `json:"original_submit_bytes,omitempty"`
	ArchiveURL          string            `json:"archive_url,omitempty"`        //  NEW: For historical log recovery
	AssignedWorkerID    string            `json:"assigned_worker_id,omitempty"` //  NEW: For persistent worker recovery
}

type TaskCompletePayload struct {
	TaskID           string `json:"task_id"`
	JobID            string `json:"job_id"`
	JobType          string `json:"job_type"` // "MAP" | "GATHER" | "" (standard)
	ChunkIndex       int    `json:"chunk_index"`
	ResultURI        string `json:"result_uri"`
	UserScriptPath   string `json:"user_script_path,omitempty"`
	GatherScriptPath string `json:"gather_script_path,omitempty"`
}

// Registration

// RegisterWorkerPayload is sent once when a Muscle node first connects.
// It tells the Brain the hard limits of this node (Docker --cpus / --memory).
type RegisterWorkerPayload struct {
	TotalCPU      float64        `json:"total_cpu"`
	TotalRAM      int            `json:"total_ram"`
	AvailCPU      float64        `json:"avail_cpu"`                 //  NEW: Availability after adoption
	AvailRAM      int            `json:"avail_ram"`                 //  NEW
	OccupiedPorts map[int]string `json:"occupied_ports,omitempty"`  //  NEW: Port -> JobID
	ActiveTaskIDs []string       `json:"active_task_ids,omitempty"` //  NEW: For reconciliation
}

// Heartbeat

// HeartbeatPayload carries the node's current free resources on every tick.
type HeartbeatPayload struct {
	AvailCPU      float64        `json:"avail_cpu"`
	AvailRAM      int            `json:"avail_ram"`
	OccupiedPorts map[int]string `json:"occupied_ports,omitempty"`  //  NEW
	ActiveTaskIDs []string       `json:"active_task_ids,omitempty"` //  NEW
}

// Task submission (client → brain)

type SubmitTaskPayload struct {
	JobID            string            `json:"job_id,omitempty"`
	QueueName        string            `json:"queue_name"`
	Payload          string            `json:"payload"`
	RequiredCPU      float64           `json:"required_cpu"`
	RequiredRAM      int               `json:"required_ram"`
	JobType          string            `json:"job_type,omitempty"`
	TotalMAPs        int               `json:"total_maps,omitempty"`
	UserScriptPath   string            `json:"user_script_path,omitempty"`
	GatherScriptPath string            `json:"gather_script_path,omitempty"`
	Env              map[string]string `json:"env,omitempty"`
	Args             []string          `json:"args,omitempty"`
	ExposedPort      int               `json:"exposed_port,omitempty"`
	SourceType       string            `json:"source_type,omitempty"` // "github" | "local"
	BuildCommand     string            `json:"build_command,omitempty"`
	RunCommand       string            `json:"run_command,omitempty"`
	RootDirectory    string            `json:"root_directory,omitempty"`
	GitRepo          string            `json:"git_repo,omitempty"`
	ProjectID        string            `json:"project_id,omitempty"`
	CommitHash       string            `json:"commit_hash,omitempty"`
	BranchName       string            `json:"branch_name,omitempty"`
	UserID           string            `json:"user_id,omitempty"` //  Multi-Tenancy ID
	Code             string            `json:"code,omitempty"`    //  NEW: Raw code for cloud staging
	Extension        string            `json:"extension,omitempty"` //  NEW: File extension for staged code
}

type RegisterProjectPayload struct {
	UserID        string `json:"user_id"`
	RepoFullName  string `json:"repo_full_name"`
	WebhookSecret string `json:"webhook_secret"`
}

type TaskAcceptedPayload struct {
	TaskID string `json:"task_id"`
}

// Task request (muscle → brain)

// RequestTaskPayload is sent by a Muscle node when polling for work.
type RequestTaskPayload struct {
	QueueName     string         `json:"queue_name"`
	AvailCPU      float64        `json:"avail_cpu"`
	AvailRAM      int            `json:"avail_ram"`
	OccupiedPorts map[int]string `json:"occupied_ports,omitempty"`  //  NEW
	ActiveTaskIDs []string       `json:"active_task_ids,omitempty"` //  NEW
}

// Task assignment (brain → muscle)

type AssignedTask struct {
	ID               string  `json:"id"`
	Payload          string  `json:"payload"`
	QueueName        string  `json:"queue_name"`
	RequiredCPU      float64 `json:"required_cpu"`
	RequiredRAM      int     `json:"required_ram"`
	UserScriptPath   string  `json:"user_script_path,omitempty"`
	GatherScriptPath string  `json:"gather_script_path,omitempty"`
	UploadURL        string  `json:"upload_url,omitempty"`
	LogUploadURL     string  `json:"log_upload_url,omitempty"`
}

type ServiceReadyPayload struct {
	JobID               string  `json:"job_id"`
	ServiceRole         string  `json:"service_role"`
	Address             string  `json:"address"`
	WorkerID            string  `json:"worker_id,omitempty"`
	RecoveredCPU        float64 `json:"recovered_cpu,omitempty"`
	RecoveredRAM        int     `json:"recovered_ram,omitempty"`
	OriginalSubmitBytes string  `json:"original_submit_bytes,omitempty"`
}

// Result / failure

type SubmitResultPayload struct {
	TaskID        string `json:"task_id"`
	ResultPayload string `json:"result_payload"`
}

type TaskFailedPayload struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason"`
}

type GetTaskStatusPayload struct {
	TaskID string `json:"task_id"`
}

type TaskStatusPayload struct {
	TaskID string `json:"task_id"`
	Status int    `json:"status"`
}

type JobRegisteredPayload struct {
	TotalMAPs        int               `json:"total_maps"`
	MapTasks         []json.RawMessage `json:"map_tasks"`
	UserScriptPath   string            `json:"user_script_path,omitempty"`
	GatherScriptPath string            `json:"gather_script_path,omitempty"`
	BuildCommand     string            `json:"build_command,omitempty"`
	RunCommand       string            `json:"run_command,omitempty"`
	RootDirectory    string            `json:"root_directory,omitempty"`
	RequiredCPU      float64           `json:"required_cpu,omitempty"`
	RequiredRAM      int               `json:"required_ram,omitempty"`
}

type MapCompletedPayload struct {
	ChunkIndex int    `json:"chunk_index"`
	ResultURI  string `json:"result_uri"`
}

type RedirectPayload struct {
	LeaderID string `json:"leader_id"`
}

type KillTaskPayload struct {
	TaskID string `json:"task_id"`
}

// SendResponse is the single, canonical implementation for writing a framed response back to a worker connection.
func SendResponse(conn net.Conn, opCode uint8, requestID string, payload interface{}) {
	if conn == nil {
		return
	}

	var payloadBytes []byte
	if payload != nil {
		var err error
		payloadBytes, err = json.Marshal(payload)
		if err != nil {
			log.Printf("Failed to marshal payload: %v", err)
			return
		}
	}

	frame := PackEnvelope(opCode, "", requestID, payloadBytes)

	// write deadline prevents a stalled worker from blocking the WAL's flushAndAck loop, which serialises responses for all workers.
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	conn.Write(frame)
}
