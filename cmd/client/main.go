// package main

// import (
// 	"encoding/json"
// 	"fmt"
// 	"log"
// 	"net"
// 	"time"

// 	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
// )

// func main() {
// 	// Connect to the Brain's client port
// 	conn, err := net.Dial("tcp", "127.0.0.1:9000")
// 	if err != nil {
// 		log.Fatalf("Failed to connect to Brain: %v", err)
// 	}
// 	defer conn.Close()

// 	// The Chaos Payload Array
// 	jobs := []protocol.SubmitTaskPayload{
// 		{
// 			QueueName:        "default",
// 			JobType:          "MAP",
// 			TotalMAPs:        4, // 4 Goja Tasks
// 			RequiredCPU:      1,
// 			RequiredRAM:      128,
// 			UserScriptPath:   "/app/javascript/test_goja.js",
// 			GatherScriptPath: "/app/javascript/test_goja.js",
// 		},
// 		{
// 			QueueName:        "default",
// 			JobType:          "MAP",
// 			TotalMAPs:        3, // 3 Docker Node Tasks
// 			RequiredCPU:      1,
// 			RequiredRAM:      256,
// 			UserScriptPath:   "/app/javascript/test_docker.cjs",
// 			GatherScriptPath: "/app/javascript/test_docker.cjs",
// 		},
// 		{
// 			QueueName:        "default",
// 			JobType:          "MAP",
// 			TotalMAPs:        3, // 3 Docker Python Tasks
// 			RequiredCPU:      1,
// 			RequiredRAM:      256,
// 			UserScriptPath:   "/app/python/test_python.py",
// 			GatherScriptPath: "/app/python/test_python.py",
// 		},
// 		{
// 			QueueName:        "default",
// 			JobType:          "MAP",
// 			TotalMAPs:        2, // 2 Docker C++ Tasks
// 			RequiredCPU:      1,
// 			RequiredRAM:      512,
// 			UserScriptPath:   "/app/cpp/number_cruncher.cpp",
// 			GatherScriptPath: "/app/cpp/number_cruncher.cpp",
// 		},
// 		{
// 			QueueName:        "default",
// 			JobType:          "MAP",
// 			TotalMAPs:        2, // 2 chunks of 10 rows each
// 			RequiredCPU:      1,
// 			RequiredRAM:      128,
// 			UserScriptPath:   "/app/javascript/sales_aggregator.js",
// 			GatherScriptPath: "/app/javascript/sales_aggregator.js",
// 			Args:             []string{"/app/datasets/sales.csv"},
// 		},
// 	}

// 	log.Println("🔥 INITIATING ABSOLUTE CHAOS 🔥")

// 	for i, job := range jobs {
// 		log.Printf("Submitting DAG Pipeline %d: %s (Total MAPs: %d)", i+1, job.UserScriptPath, job.TotalMAPs)

// 		payloadBytes, _ := json.Marshal(job)

// 		// 🚨 THE FIX: Use the correct signature for PackEnvelope (opCode, nodeID, reqID, payload)
// 		reqID := fmt.Sprintf("chaos-req-%d", i)
// 		frame := protocol.PackEnvelope(protocol.OpSubmitTask, "chaos-client", reqID, payloadBytes)

// 		// sendFrame sends the pre-packed binary frame directly
// 		sendFrame(conn, frame)

// 		// 100ms stagger ensures the Brain interleaves them beautifully in the Raft log
// 		time.Sleep(100 * time.Millisecond)
// 	}

// 	log.Println("✅ All payloads submitted to the Brain. Watch the Muscle logs!")
// }

// // sendFrame sends a pre-packed protocol frame (already contains the 5-byte header)
// func sendFrame(conn net.Conn, frame []byte) {
// 	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
// 	if _, err := conn.Write(frame); err != nil {
// 		log.Fatalf("Failed to send frame: %v", err)
// 	}
// }

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

	// We only need 1 MAP task to prove the .pkl dump works
	job := protocol.SubmitTaskPayload{
		QueueName:        "default",
		JobType:          "MAP",
		TotalMAPs:        1,
		RequiredCPU:      1,
		RequiredRAM:      512,
		UserScriptPath:   "/app/python/train_model.py",
		GatherScriptPath: "/app/python/train_model.py",
	}

	payloadBytes, _ := json.Marshal(job)

	// Wrap in the binary envelope (opCode, nodeID, reqID, payload)
	// PackEnvelope already includes the 5-byte header (length + opcode)
	frame := protocol.PackEnvelope(protocol.OpSubmitTask, "ml-client", "req-ml-01", payloadBytes)

	// Send the pre-packed frame directly
	if _, err := conn.Write(frame); err != nil {
		log.Fatalf("Failed to send job: %v", err)
	}

	log.Println("✅ ML Training Job submitted!")
}
