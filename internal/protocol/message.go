package protocol

import (
	"encoding/json"
	"time"
)

// OpCodes
const (
	OpHeartbeat        = "HEARTBEAT"
	OpHeartbeatAck     = "HEARTBEAT_ACK"
	OpRegisterWorker   = "REGISTER_WORKER"
	OpWorkerRegistered = "WORKER_REGISTERED"
	OpRequestTask      = "REQUEST_TASK"
	OpTaskAssigned     = "TASK_ASSIGNED"
	OpNoTasksAvailable = "NO_TASKS_AVAILABLE"
	OpSubmitTask       = "SUBMIT_TASK"
	OpTaskAccepted     = "TASK_ACCEPTED"
	OpSubmitResult     = "SUBMIT_RESULT"
	OpTaskCompleted    = "TASK_COMPLETED"
	OpGetTaskStatus    = "GET_TASK_STATUS"
	OpTaskStatus       = "TASK_STATUS"
	OpTaskFailed       = "TASK_FAILED"
	OpTaskFailedAck    = "TASK_FAILED_ACK"
	OpError            = "ERROR"
)

type Message struct {
	OpCode    string          `json:"op_code"`
	NodeID    string          `json:"node_id"`
	RequestID string          `json:"request_id"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}

type Payload struct {
	JobType       string   `json:"job_type"`
	JobID         string   `json:"job_id"`
	ChunkIndex    int      `json:"chunk_index"`
	AttemptNumber int      `json:"attempt_number"`
	Command       string   `json:"command"`
	InputURIs     []string `json:"input_uris"`
}

type TaskCompletePayload struct {
	TaskID     string `json:"task_id"`
	JobID      string `json:"job_id"`
	JobType    string `json:"job_type"` // "MAP" | "GATHER" | "" (standard)
	ChunkIndex int    `json:"chunk_index"`
	ResultURI  string `json:"result_uri"`
}

// Registration

// RegisterWorkerPayload is sent once when a Muscle node first connects
// It tells the Brain the hard limits of this node (Docker --cpus / --memory)
type RegisterWorkerPayload struct {
	TotalCPU int `json:"total_cpu"`
	TotalRAM int `json:"total_ram"`
}

// Heartbeat

// HeartbeatPayload carries the node's current free resources on every tick
type HeartbeatPayload struct {
	AvailCPU int `json:"avail_cpu"`
	AvailRAM int `json:"avail_ram"`
}

// Task submission (client → brain)

type SubmitTaskPayload struct {
	QueueName   string `json:"queue_name"`
	Payload     string `json:"payload"`
	RequiredCPU int    `json:"required_cpu"`
	RequiredRAM int    `json:"required_ram"`
	JobType     string `json:"job_type,omitempty"`
	TotalMAPs   int    `json:"total_maps,omitempty"`
}

type TaskAcceptedPayload struct {
	TaskID string `json:"task_id"`
}

// Task request (muscle → brain)

// RequestTaskPayload is sent by a Muscle node when polling for work
type RequestTaskPayload struct {
	QueueName string `json:"queue_name"`
	AvailCPU  int    `json:"avail_cpu"`
	AvailRAM  int    `json:"avail_ram"`
}

// Task assignment (brain → muscle)

type AssignedTask struct {
	ID          string `json:"id"`
	Payload     string `json:"payload"`
	QueueName   string `json:"queue_name"`
	RequiredCPU int    `json:"required_cpu"`
	RequiredRAM int    `json:"required_ram"`
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
