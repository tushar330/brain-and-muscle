package main

import (
	"log"
	"os"
	"runtime"
	"strconv"

	"github.com/akashtripathi12/brain-and-muscle/internal/muscle"
)

func main() {
	brainAddr := envOrDefault("BRAIN_ADDR", "localhost:8080")
	workerID := envOrDefault("WORKER_ID", "muscle-1")

	// Read Docker-injected resource limits from the environment.
	// When running with `docker run --cpus="2" --memory="4g"`, set:
	//   -e TOTAL_CPU=2 -e TOTAL_RAM_MB=4096
	// Falls back to the host's full resources when running locally.
	totalCPU := envInt("TOTAL_CPU", runtime.NumCPU())
	totalRAM := envInt("TOTAL_RAM_MB", 4096) // MB

	log.Printf("muscle worker %s starting — brain at %s (%d CPU / %d MB RAM)",
		workerID, brainAddr, totalCPU, totalRAM)

	cm := muscle.NewConnectionManager(brainAddr, workerID)
	w := muscle.NewWorker(cm, totalCPU, totalRAM)

	go cm.Run() // connect + reconnect loop

	// Wait for the first healthy connection before announcing ourselves.
	// Register sends OpRegisterWorker so the Brain knows our hard limits.
	cm.WaitUntilHealthy()
	w.Register()

	go w.HeartbeatLoop() // sends live AvailCPU/RAM every 5s
	w.TaskRequestLoop()  // blocks main goroutine
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
