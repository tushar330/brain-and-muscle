package protocol

import (
	"encoding/binary"
	"encoding/json"
	"net"
	"time"
)

// OpCodes
const (
	OpHeartbeat        uint8 = 0x01
	OpHeartbeatAck     uint8 = 0x02
	OpRegisterWorker   uint8 = 0x03
	OpWorkerRegistered uint8 = 0x04
	OpRequestTask      uint8 = 0x05
	OpTaskAssigned     uint8 = 0x06
	OpNoTasksAvailable uint8 = 0x07
	OpSubmitTask       uint8 = 0x08
	OpTaskAccepted     uint8 = 0x09
	OpSubmitResult     uint8 = 0x0A
	OpTaskCompleted    uint8 = 0x0B
	OpGetTaskStatus    uint8 = 0x0C
	OpTaskStatus       uint8 = 0x0D
	OpTaskFailed       uint8 = 0x0E
	OpTaskFailedAck    uint8 = 0x0F
	OpError            uint8 = 0x10
	OpTick             uint8 = 0x11
)

type Message struct {
	OpCode    uint8           `json:"-"`
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
	Args          []string `json:"args"`       // standard task arguments (Version A)
	InputURIs     []string `json:"input_uris"` // MAP chunk outputs forwarded to GATHER (Version B)
}

type TaskCompletePayload struct {
	TaskID     string `json:"task_id"`
	JobID      string `json:"job_id"`
	JobType    string `json:"job_type"` // "MAP" | "GATHER" | "" (standard)
	ChunkIndex int    `json:"chunk_index"`
	ResultURI  string `json:"result_uri"`
}

// Registration

// RegisterWorkerPayload is sent once when a Muscle node first connects.
// It tells the Brain the hard limits of this node (Docker --cpus / --memory).
type RegisterWorkerPayload struct {
	TotalCPU int `json:"total_cpu"`
	TotalRAM int `json:"total_ram"`
}

// Heartbeat

// HeartbeatPayload carries the node's current free resources on every tick.
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

// RequestTaskPayload is sent by a Muscle node when polling for work.
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

type JobRegisteredPayload struct {
	TotalMAPs  int               `json:"total_maps"`
	GatherTask []byte            `json:"gather_task"`
	MapTasks   []json.RawMessage `json:"map_tasks"`
}

type MapCompletedPayload struct {
	ChunkIndex int    `json:"chunk_index"`
	ResultURI  string `json:"result_uri"`
}

// SendResponse is the single, canonical implementation for writing a framed response back to a worker connection.
func SendResponse(conn net.Conn, opCode uint8, requestID string, payload interface{}) {
	if conn == nil {
		return
	}

	msg := Message{
		RequestID: requestID,
		Timestamp: time.Now(),
	}
	if payload != nil {
		raw, _ := json.Marshal(payload)
		msg.Payload = raw
	}

	body, _ := json.Marshal(msg)
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(body)))
	header[4] = opCode

	// write deadline prevents a stalled worker from blocking the WAL's flushAndAck loop, which serialises responses for all workers.
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	conn.Write(append(header, body...))
}
