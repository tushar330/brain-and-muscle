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

	totalCPU := envInt("TOTAL_CPU", runtime.NumCPU())
	totalRAM := envInt("TOTAL_RAM_MB", 4096)

	log.Printf("muscle worker %s starting — brain at %s (%d CPU / %d MB RAM)",
		workerID, brainAddr, totalCPU, totalRAM)

	cm := muscle.NewConnectionManager(brainAddr, workerID)
	w := muscle.NewWorker(cm, totalCPU, totalRAM)

	// NEW: Tell the ConnectionManager to register us EVERY TIME it connects!
	cm.SetOnConnect(func() {
		log.Println("[system] TCP connected. Announcing resources to Brain...")
		w.Register()
	})

	// Run the connection loop (this will trigger the hook on the first connect too)
	go cm.Run()

	// Wait until the first connection is established and the hook fires
	cm.WaitUntilHealthy()

	go w.HeartbeatLoop()
	w.TaskRequestLoop()
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
