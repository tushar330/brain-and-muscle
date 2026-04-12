//go:build !cgo
// +build !cgo

package main

import (
	"log"

	"github.com/kardianos/service"
)

func runDesktopUI(s service.Service, cpu float64, ram int) {
	log.Println("[boot] BAM Muscle is running in HEADLESS mode. Desktop UI is disabled.")

	// Align with main.go and ui_cgo.go directory logic
	programData := "C:\\ProgramData"
	if v := os.Getenv("ProgramData"); v != "" {
		programData = v
	}
	appDir := filepath.Join(programData, "BrainAndMuscle")

	workerID, _ := getPersistentID(appDir)

	log.Println("[boot] Starting worker logic directly...")
	runMuscleWorker(workerID, cpu, ram, appDir)
}
