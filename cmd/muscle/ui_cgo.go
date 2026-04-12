//go:build cgo
// +build cgo

package main

import (
	_ "embed"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/getlantern/systray"
	"github.com/kardianos/service"
	"gopkg.in/natefinch/lumberjack.v2"
)

//go:embed icon.ico
var iconBytes []byte

func runDesktopUI(s service.Service, cpu float64, ram int) {
	programData := envOrDefault("ProgramData", "C:\\ProgramData")
	appDir := filepath.Join(programData, "BrainAndMuscle")
	logPath := filepath.Join(appDir, "muscle.log")

	// Check if the background service is doing the work
	status, _ := s.Status()
	isDaemonRunning := (status == service.StatusRunning)

	// Port probe logic redundant here as main already did it, but keeping for compatibility
	workerID, _ := getPersistentID(appDir)

	systray.Run(func() {
		onReady(workerID, cpu, ram, appDir, logPath, isDaemonRunning)
	}, onExit)
}

func onReady(workerID string, cpu float64, ram int, appDir, logPath string, isDaemonRunning bool) {
	systray.SetTitle("BAM Muscle")
	systray.SetTooltip("Brain and Muscle Worker Node")
	systray.SetIcon(iconBytes)

	mShow := systray.AddMenuItem("Show Logs", "Open the log file in Notepad")
	systray.AddSeparator()
	mQuit := systray.AddMenuItem("Close UI", "Exit the tray icon")

	go func() {
		for {
			select {
			case <-mShow.ClickedCh:
				cmd := exec.Command("notepad", logPath)
				_ = cmd.Start()
			case <-mQuit.ClickedCh:
				systray.Quit()
				os.Exit(0)
			}
		}
	}()

	if !isDaemonRunning {
		// Output exclusively to the log file so the terminal stays silent
		log.SetOutput(&lumberjack.Logger{
			Filename:   logPath,
			MaxSize:    10, // MB
			MaxBackups: 3,
			MaxAge:     28, // days
		})
		log.Println("[boot] Running in Portable Mode (No background service detected).")
		go runMuscleWorker(workerID, cpu, ram, appDir)
	} else {
		log.Println("[boot] Attached to existing background service. Acting as Log Viewer only.")
	}
}

func onExit() {
	// Cleanup here
}
