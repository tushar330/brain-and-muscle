package main

import (
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/akashtripathi12/brain-and-muscle/internal/muscle"
)

func main() {
	// Accept a comma-separated list of brain addresses
	brainAddrs := envOrDefault("BRAIN_ADDR", "localhost:8080")
	workerID := envOrDefault("WORKER_ID", "muscle-1")

	totalCPU := envInt("TOTAL_CPU", runtime.NumCPU())
	totalRAM := envInt("TOTAL_RAM_MB", 4096)

	log.Printf("muscle worker %s starting — brain at %s (%d CPU / %d MB RAM)",
		workerID, brainAddrs, totalCPU, totalRAM)

	// 🚨 THE TOPOLOGY FIX 🚨
	// Parse the comma-separated string into a slice of peers
	var peers []string
	if brainAddrs != "" {
		peers = strings.Split(brainAddrs, ",")
	} else {
		peers = []string{"localhost:8080"} // Fallback
	}

	// Pass the slice of peers to the new Round-Robin ConnectionManager
	cm := muscle.NewConnectionManager(peers, workerID)
	w := muscle.NewWorker(cm, totalCPU, totalRAM)

	var registerMu sync.Mutex

	cm.SetOnConnect(func() {
		if !registerMu.TryLock() {
			return
		}
		defer registerMu.Unlock()

		log.Println("[system] Reconnected. Re-registering with Brain...")
		w.Register()
	})

	go cm.Run()
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
