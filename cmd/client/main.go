package main

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"net"
	"os"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/google/uuid"
)

func main() {
	brainAddr := os.Getenv("BRAIN_ADDR")
	if brainAddr == "" {
		brainAddr = "localhost:8080"
	}

	conn, err := net.Dial("tcp", brainAddr)
	if err != nil {
		log.Fatalf("failed to connect to brain at %s: %v", brainAddr, err)
	}
	defer conn.Close()

	// The User's base payload
	baseCommand := protocol.Payload{
		Command: "process_video",
	}
	rawCommand, err := json.Marshal(baseCommand)
	if err != nil {
		log.Fatalf("failed to marshal base command: %v", err)
	}

	// The Submission Envelope
	submitReq := protocol.SubmitTaskPayload{
		QueueName:   "default",
		RequiredCPU: 1,
		RequiredRAM: 512,
		JobType:     "SCATTER",
		TotalMAPs:   5,
		Payload:     string(rawCommand),
	}

	rawPayload, err := json.Marshal(submitReq)
	if err != nil {
		log.Fatalf("failed to marshal submit request: %v", err)
	}

	msg := protocol.Message{
		OpCode:    protocol.OpSubmitTask,
		NodeID:    "client-1",
		RequestID: uuid.New().String(), // full UUID — avoids the 8-char collision risk
		Payload:   rawPayload,
		Timestamp: time.Now(),
	}

	body, err := json.Marshal(msg)
	if err != nil {
		log.Fatalf("failed to marshal message: %v", err)
	}

	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(body)))

	if _, err := conn.Write(append(header, body...)); err != nil {
		log.Fatalf("failed to send message to brain: %v", err)
	}

	log.Printf("[client] submitted SCATTER job (5 MAPs) to %s", brainAddr)
}
