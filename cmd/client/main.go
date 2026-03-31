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
	targetNode := "brain-1" // Starting guess

	// Map internal cluster IDs to your localhost exposed ports
	topology := map[string]string{
		"brain-1": "localhost:8081",
		"brain-2": "localhost:8082",
		"brain-3": "localhost:8083",
	}

	userPayload := map[string]interface{}{
		"command": "process_data",
		"args":    []string{"chunk_data"},
	}
	userPayloadBytes, _ := json.Marshal(userPayload)

	submitReq := protocol.SubmitTaskPayload{
		QueueName:   "default",
		Payload:     string(userPayloadBytes),
		RequiredCPU: 1,
		RequiredRAM: 64,
		JobType:     "SCATTER",
		TotalMAPs:   50,
	}
	submitReqBytes, _ := json.Marshal(submitReq)

	log.Println("Initializing Smart Client Topology Router...")

	// The Autonomous Retry Loop
	for attempt := 1; attempt <= 5; attempt++ {
		targetAddr := topology[targetNode]
		log.Printf("[Attempt %d] Dialing %s at %s...", attempt, targetNode, targetAddr)

		conn, err := net.DialTimeout("tcp", targetAddr, 2*time.Second)
		if err != nil {
			log.Printf("❌ Failed to connect to %s. Retrying in 2s...", targetNode)
			time.Sleep(2 * time.Second)
			continue
		}

		reqID := uuid.New().String()
		frame := protocol.PackEnvelope(protocol.OpSubmitTask, "test-client", reqID, submitReqBytes)
		conn.Write(frame)

		// 🚨 THE DEADLINE SHIELD: Physically prevents infinite hanging
		conn.SetReadDeadline(time.Now().Add(7 * time.Second))

		respHeader := make([]byte, 5)
		if _, err := io.ReadFull(conn, respHeader); err != nil {
			log.Printf("❌ Network timeout/error waiting for Brain: %v", err)
			conn.Close()
			continue
		}

		respLen := binary.BigEndian.Uint32(respHeader[0:4])
		respOp := respHeader[4]
		respBody := make([]byte, respLen)
		io.ReadFull(conn, respBody)

		_, _, _, payloadBytes, _ := protocol.UnpackEnvelope(respBody)

		// Handle the response autonomously
		if respOp == protocol.OpTaskAccepted {
			var accepted protocol.TaskAcceptedPayload
			json.Unmarshal(payloadBytes, &accepted)
			log.Printf("✅ SUCCESS! Job ID: %s committed to the cluster.", accepted.TaskID)
			conn.Close()
			return

		} else if respOp == protocol.OpRedirect {
			var redirect protocol.RedirectPayload
			json.Unmarshal(payloadBytes, &redirect)

			if redirect.LeaderID == "" {
				log.Println("⚠️ Cluster is electing a new Leader. Backing off 2 seconds...")
				time.Sleep(2 * time.Second)
			} else {
				log.Printf("⚠️ Redirected! New Leader is: %s", redirect.LeaderID)
				targetNode = redirect.LeaderID
			}

		} else {
			log.Printf("❌ BRAIN ERROR: %s", string(payloadBytes))
		}

		conn.Close()
	}

	log.Fatalf("❌ FATAL: Failed to submit task after 5 attempts.")
}
