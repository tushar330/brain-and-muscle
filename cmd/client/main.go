package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/google/uuid"
)

func main() {
	// 1. Define CLI Flags
	scriptPath := flag.String("script", "", "Path to the FaaS file")
	jobID := flag.String("job", "universal-job-01", "The unique Job ID")
	chunks := flag.Int("chunks", 26, "Number of chunks")
	lang := flag.String("lang", "js", "Language runtime (js or python)") // 🚨 NEW

	flag.Parse()

	// Validation
	if *scriptPath == "" {
		log.Fatal("Usage: go run cmd/client/main.go --script <path.js> --chunks 26 [arg1 arg2 arg3...]")
	}

	// Read the physical JS file from disk
	scriptBytes, err := os.ReadFile(*scriptPath)
	if err != nil {
		log.Fatalf("Failed to read script file: %v", err)
	}

	var command string
	if *lang == "python" {
		command = "execute_python"
	} else {
		command = "execute_js"
	}

	// 2. Build the Trojan Horse Args
	// The first argument MUST be the raw script string.
	// We then append any extra arguments passed via the terminal (e.g., the hash, length, etc.)
	var faasArgs []string
	faasArgs = append(faasArgs, string(scriptBytes))
	faasArgs = append(faasArgs, flag.Args()...) // Grabs all non-flag arguments from the CLI

	userPayload := map[string]interface{}{
		"command": command,
		"args":    faasArgs,
	}
	userPayloadBytes, _ := json.Marshal(userPayload)

	// 3. Assemble the SCATTER request
	submitReq := protocol.SubmitTaskPayload{
		QueueName:   "default",
		Payload:     string(userPayloadBytes),
		RequiredCPU: 1,
		RequiredRAM: 64,
		JobType:     "SCATTER",
		TotalMAPs:   int(*chunks),
	}
	submitReqBytes, _ := json.Marshal(submitReq)

	log.Printf("🚀 Submitting Universal Job [%s] using script [%s] across %d chunks...", *jobID, *scriptPath, *chunks)
	submitTaskToBrain(submitReqBytes)
	log.Println("✅ Job successfully scattered to the cluster. Awaiting Gather report...")
}

// Network submission logic remains identical
func submitTaskToBrain(submitReqBytes []byte) {
	targetNode := "brain-1"

	topology := map[string]string{
		"brain-1": "localhost:8081",
		"brain-2": "localhost:8082",
		"brain-3": "localhost:8083",
	}

	for attempt := 1; attempt <= 3; attempt++ {
		targetAddr := topology[targetNode]

		conn, err := net.DialTimeout("tcp", targetAddr, 2*time.Second)
		if err != nil {
			log.Printf("❌ Failed to connect to %s.", targetNode)
			targetNode = shiftTarget(targetNode)
			time.Sleep(1 * time.Second)
			continue
		}

		reqID := uuid.New().String()
		frame := protocol.PackEnvelope(protocol.OpSubmitTask, "cli-client", reqID, submitReqBytes)
		conn.Write(frame)

		conn.SetReadDeadline(time.Now().Add(7 * time.Second))

		respHeader := make([]byte, 5)
		if _, err := io.ReadFull(conn, respHeader); err != nil {
			conn.Close()
			targetNode = shiftTarget(targetNode)
			continue
		}

		respLen := binary.BigEndian.Uint32(respHeader[0:4])
		respOp := respHeader[4]
		respBody := make([]byte, respLen)
		io.ReadFull(conn, respBody)

		_, _, _, payloadBytes, _ := protocol.UnpackEnvelope(respBody)

		if respOp == protocol.OpTaskAccepted {
			conn.Close()
			return
		} else if respOp == protocol.OpRedirect {
			var redirect protocol.RedirectPayload
			json.Unmarshal(payloadBytes, &redirect)
			if redirect.LeaderID != "" {
				targetNode = redirect.LeaderID
			}
			conn.Close()
		} else {
			conn.Close()
			return
		}
	}
	log.Printf("🚨 FAILED to submit task after 3 attempts.")
}

func shiftTarget(current string) string {
	if current == "brain-1" {
		return "brain-2"
	} else if current == "brain-2" {
		return "brain-3"
	}
	return "brain-1"
}
