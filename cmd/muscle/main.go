package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/muscle"
	"github.com/getlantern/systray"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/kardianos/service"
	"github.com/shirou/gopsutil/v3/mem"
	"gopkg.in/natefinch/lumberjack.v2"
)

var iconBytes []byte

var (
	DefaultBrainURL string
	DefaultAPIKey   string

	// Global flags for resource limits
	cpuLimit = flag.Float64("cpu", 0, "Number of CPUs to dedicate (0 = 2.0 auto)")
	ramLimit = flag.Int("ram", 0, "RAM in MB to dedicate (0 = 2048 auto)")
	svcFlag  = flag.String("service", "", "Control the system service: install, uninstall, start, stop")
)

type program struct {
	exit chan struct{}
}

func (p *program) Start(s service.Service) error {
	// ONLY the background daemon uses this route.
	go p.runDaemon()
	return nil
}

func (p *program) Stop(s service.Service) error {
	close(p.exit)
	return nil
}

func (p *program) runDaemon() {
	// 1. Setup Shared Directory
	programData := envOrDefault("ProgramData", "C:\\ProgramData")
	appDir := filepath.Join(programData, "BrainAndMuscle")
	_ = os.MkdirAll(appDir, 0755)

	// --- NEW FIX START ---
	// Force Go to use a folder Docker can access (Avoids C:\Windows\SystemTemp trap in Session 0)
	tempDir := filepath.Join(appDir, "tmp")
	_ = os.MkdirAll(tempDir, 0755)
	os.Setenv("TMP", tempDir)
	os.Setenv("TEMP", tempDir)
	// --- NEW FIX END ---

	// 2. Setup Rotating Logger (We own the file lock here)
	logPath := filepath.Join(appDir, "muscle.log")
	log.SetOutput(&lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    10, // MB
		MaxBackups: 3,
		MaxAge:     28, // days
	})

	// 3. Do the actual compute work
	finalCPU, finalRAM := detectResources(*cpuLimit, *ramLimit)
	workerID, _ := getPersistentID(appDir)
	runMuscleWorker(workerID, finalCPU, finalRAM, appDir)
}

func detectResources(reqCPU float64, reqRAM int) (float64, int) {
	// User explicitly requested 0 or nothing - use fixed baseline or 50%
	if reqCPU <= 0 {
		reqCPU = 2.0 // Requested default
		if runtime.NumCPU() < 2 {
			reqCPU = 1.0
		}
	}

	if reqRAM <= 0 {
		reqRAM = 2048 // Requested default
		v, _ := mem.VirtualMemory()
		systemRAMHalf := int(v.Total / 1024 / 1024 / 2)
		if systemRAMHalf < reqRAM {
			reqRAM = systemRAMHalf
		}
	}

	log.Printf("[boot] BAM Muscle v1.0.5-Stealth - %.1f CPUs | %d MB RAM | ID: %s", reqCPU, reqRAM, "REPLACE_VERSION")
	return reqCPU, reqRAM
}

func runDesktopUI(s service.Service) {
	programData := envOrDefault("ProgramData", "C:\\ProgramData")
	appDir := filepath.Join(programData, "BrainAndMuscle")
	_ = os.MkdirAll(appDir, 0755)

	// --- NEW FIX START ---
	// Force Go to use a folder Docker can access (Avoids C:\Windows\SystemTemp trap in Session 0)
	tempDir := filepath.Join(appDir, "tmp")
	_ = os.MkdirAll(tempDir, 0755)
	os.Setenv("TMP", tempDir)
	os.Setenv("TEMP", tempDir)
	// --- NEW FIX END ---

	logPath := filepath.Join(appDir, "muscle.log")

	// Check if the background service is doing the work
	status, _ := s.Status()
	isDaemonRunning := (status == service.StatusRunning)

	// PORT PROBE: Second opinion for Windows Session 0 isolation
	isPortTaken := false
	conn, err := net.DialTimeout("tcp", "127.0.0.1:9012", 500*time.Millisecond)
	if err == nil {
		conn.Close()
		isPortTaken = true
	}

	if isPortTaken {
		isDaemonRunning = true
	}

	if !isDaemonRunning {
		// Portable Mode: Setup logger to BOTH file and console
		logFile := &lumberjack.Logger{
			Filename: logPath, MaxSize: 10, MaxBackups: 3, MaxAge: 28,
		}
		log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	} else {
		// Log Viewer Mode: Just redirect to console
		log.SetOutput(os.Stdout)
	}

	finalCPU, finalRAM := detectResources(*cpuLimit, *ramLimit)
	workerID, _ := getPersistentID(appDir)

	systray.Run(func() {
		onReady(workerID, finalCPU, finalRAM, appDir, logPath, isDaemonRunning)
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
				// Open the log file in Notepad (Showcase)
				cmd := exec.Command("notepad", logPath)
				_ = cmd.Start()
			case <-mQuit.ClickedCh:
				systray.Quit()
				os.Exit(0)
			}
		}
	}()

	// THE SMART CONTROLLER CHECK
	if !isDaemonRunning {
		log.Println("[boot] Running in Portable Mode (No background service detected).")
		// Kick off the actual worker logic
		go runMuscleWorker(workerID, cpu, ram, appDir)
	} else {
		log.Println("[boot] Attached to existing background service. Acting as Log Viewer only.")
	}
}

func onExit() {
	// Cleanup here
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Load environment variables
	if err := godotenv.Load(); err != nil {
		_ = godotenv.Load("../../.env")
	}

	flag.Parse()

	svcConfig := &service.Config{
		Name:        "BAM_Muscle",
		DisplayName: "Brain and Muscle Worker",
		Description: "Distributed Compute Node for the BAM Cluster. Always Running.",
	}

	prg := &program{exit: make(chan struct{})}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}

	// Handle service install/uninstall commands
	if *svcFlag != "" {
		if *svcFlag == "install" || *svcFlag == "uninstall" {
			checkAdmin()
		}
		err = service.Control(s, *svcFlag)
		if err != nil {
			log.Fatalf("Action %s failed: %v", *svcFlag, err)
		}
		fmt.Printf("Service %s successfully.\n", *svcFlag)

		if *svcFlag == "install" {
			fmt.Println("Starting service immediately in the background...")
			_ = service.Control(s, "start")
			fmt.Println("Success! The Muscle is now running invisibly in Session 0.")
		}
		return
	}

	// THE MAIN THREAD SPLIT
	if service.Interactive() {
		// User double-clicked: Run UI on Main Thread
		runDesktopUI(s)
	} else {
		// Windows Boot: Run Daemon safely in background
		err = s.Run()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func runMuscleWorker(workerID string, totalCPU float64, totalRAM int, appDir string) {
	// 2. Inject the compiled secrets into the environment if they exist
	if os.Getenv("BRAIN_URL") == "" && DefaultBrainURL != "" {
		os.Setenv("BRAIN_URL", DefaultBrainURL)
	}
	if os.Getenv("BAM_API_KEY") == "" && DefaultAPIKey != "" {
		os.Setenv("BAM_API_KEY", DefaultAPIKey)
	}

	os.Setenv("MUSCLE_ID", workerID)

	brainAddrs := envOrDefault("BRAIN_URL", envOrDefault("BRAIN_ADDR", "localhost:9000"))

	log.Printf("muscle worker %s active — brain at %s (%.1f CPU / %d MB RAM)",
		workerID, brainAddrs, totalCPU, totalRAM)

	var peers []string
	if brainAddrs != "" {
		peers = strings.Split(brainAddrs, ",")
	} else {
		peers = []string{"localhost:9000"}
	}

	cm := muscle.NewConnectionManager(peers, workerID)
	w := muscle.NewWorker(cm, totalCPU, totalRAM, appDir)

	var registerMu sync.Mutex
	cm.SetOnConnect(func() {
		if !registerMu.TryLock() {
			return
		}
		defer registerMu.Unlock()

		log.Println("[system] Connected to Brain. Registering...")
		w.Register()
	})

	go cm.Run()
	cm.WaitUntilHealthy()

	if err := w.StartLogServer(9012); err != nil {
		log.Printf("[muscle] Log server failed: %v", err)
		log.Println("[muscle] Worker cannot start without log server (Port conflict?). Aborting.")
		return
	}

	go w.HeartbeatLoop()
	w.TaskRequestLoop()
}

func getPersistentID(appDir string) (string, string) {
	idFile := filepath.Join(appDir, "muscle_id.txt")

	// 1. Try to read existing ID from file
	data, err := os.ReadFile(idFile)
	if err == nil {
		id := strings.TrimSpace(string(data))
		if id != "" {
			return id, appDir
		}
	}

	// 2. If file doesn't exist or is empty, create a new one
	newID := fmt.Sprintf("muscle-%s", uuid.New().String()[:8]) // Short UUID
	_ = os.WriteFile(idFile, []byte(newID), 0644)

	return newID, appDir
}

func checkAdmin() {
	_, err := os.Open("\\\\.\\PHYSICALDRIVE0")
	if err != nil {
		fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
		fmt.Println("ERROR: ADMINISTRATOR PRIVILEGES REQUIRED")
		fmt.Println("Please run this command in an Administrator terminal.")
		fmt.Println("Right-click PowerShell -> 'Run as Administrator'")
		fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
		os.Exit(1)
	}
}

// Helpers remain untouched
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
