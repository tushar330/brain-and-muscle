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
	workerID := envOrDefault("WORKER_ID", "worker-local-1")
	totalCPU := envInt("TOTAL_CPU", runtime.NumCPU())
	totalRAM := envInt("TOTAL_RAM_MB", 8192)

	log.Printf("[worker] %s starting — brain at %s (%d CPU / %d MB RAM)",
		workerID, brainAddr, totalCPU, totalRAM)

	cm := muscle.NewConnectionManager(brainAddr, workerID)
	w := muscle.NewWorker(cm, totalCPU, totalRAM)

	go cm.Run()

	cm.WaitUntilHealthy()
	log.Printf("[worker] %s connected — registering with Brain", workerID)
	w.Register()

	go w.HeartbeatLoop()

	log.Printf("[worker] %s ready — polling for tasks", workerID)
	w.TaskRequestLoop() // blocks
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
