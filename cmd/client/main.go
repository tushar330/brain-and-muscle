package main

import (
	"encoding/json"
	"log"
	"net"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		log.Fatalf("Failed to connect to Brain: %v", err)
	}
	defer conn.Close()

	// Deploy the VEM_Backend Go API
	job := protocol.SubmitTaskPayload{
		QueueName:   "default",
		JobType:     protocol.JobTypeService,
		RequiredCPU: 2,
		RequiredRAM: 512,
		// Point to the entrypoint of your backend relative to the PaaS root
		UserScriptPath: "/app/VEM_Backend/cmd/api/main.go",
		ExposedPort:    7070,
	}

	payloadBytes, _ := json.Marshal(job)
	frame := protocol.PackEnvelope(protocol.OpSubmitTask, "deploy-client", "req-deploy-vem", payloadBytes)

	if _, err := conn.Write(frame); err != nil {
		log.Fatalf("Failed to deploy API: %v", err)
	}

	log.Println("VEM_Backend Deployment Submitted!")
}
