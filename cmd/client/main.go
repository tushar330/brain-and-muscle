package main

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/google/uuid"
)

func main() {
	// 1. Connect to the Brain
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("Failed to connect to Brain: %v", err)
	}
	defer conn.Close()

	log.Println("Connected to Brain. Preparing SCATTER job...")

	// 2. The User Payload (What the Muscle worker actually executes)
	// We pass dummy args here to simulate work.
	userPayload := map[string]interface{}{
		"command": "process_data",
		"args":    []string{"chunk_data"},
	}
	userPayloadBytes, _ := json.Marshal(userPayload)

	// 3. The SubmitTask Envelope
	submitReq := protocol.SubmitTaskPayload{
		QueueName:   "default",
		Payload:     string(userPayloadBytes),
		RequiredCPU: 1,
		RequiredRAM: 64,
		JobType:     "SCATTER",
		TotalMAPs:   10, // 100 Chunks! Plenty of time to crash the Brain.
	}
	submitReqBytes, _ := json.Marshal(submitReq)

	// 4. The Network Message
	msg := protocol.Message{
		RequestID: uuid.New().String(),
		Timestamp: time.Now(),
		Payload:   submitReqBytes,
	}
	msgBytes, _ := json.Marshal(msg)

	// 5. The 5-Byte Binary Frame (Method A Architecture)
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(msgBytes)))
	header[4] = protocol.OpSubmitTask // The all-important OpCode

	frame := append(header, msgBytes...)

	log.Println("Submitting SCATTER job (100 MAP chunks)...")
	if _, err = conn.Write(frame); err != nil {
		log.Fatalf("Failed to write to connection: %v", err)
	}

	// 6. Wait for the WAL to flush and the Brain to ACK
	respHeader := make([]byte, 5)
	if _, err := io.ReadFull(conn, respHeader); err != nil {
		log.Fatalf("Failed to read Brain response: %v", err)
	}

	respLen := binary.BigEndian.Uint32(respHeader[0:4])
	respOp := respHeader[4]

	respBody := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBody); err != nil {
		log.Fatalf("Failed to read Brain payload: %v", err)
	}

	if respOp == protocol.OpTaskAccepted {
		var ack protocol.Message
		json.Unmarshal(respBody, &ack)
		var accepted protocol.TaskAcceptedPayload
		json.Unmarshal(ack.Payload, &accepted)

		log.Printf("✅ SUCCESS! Brain accepted Job ID: %s", accepted.TaskID)
		log.Println("🔥 NOW HURRY! GO TO TERMINAL 1 AND PRESS CTRL+C TO CRASH THE BRAIN! 🔥")
	} else {
		log.Printf("❌ Unexpected response OpCode: 0x%X", respOp)
	}
}
