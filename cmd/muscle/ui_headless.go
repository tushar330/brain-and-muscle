//go:build !cgo
// +build !cgo

package main

import (
	"log"

	"github.com/kardianos/service"
)

func runDesktopUI(s service.Service, cpu float64, ram int) {
	log.Println("[boot] BAM Muscle is running in HEADLESS mode. Desktop UI is disabled.")

	// In headless mode, if the user tries to run interactively,
	// we just start the worker directly on the main thread.
	appDir := envOrDefault("BAM_APP_DIR", "brain-and-muscle-data")

	workerID, _ := getPersistentID(appDir)

	log.Println("[boot] Starting worker logic directly...")
	runMuscleWorker(workerID, cpu, ram, appDir)
}
