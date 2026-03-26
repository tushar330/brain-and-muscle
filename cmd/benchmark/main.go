package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/muscle"
	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
)

const (
	brainAddr = "localhost:8080"
	numTasks  = 50
)

type taskSpec struct {
	cpu   int
	ram   int
	sleep string
	count int
	label string
}

var taskSpecs = []taskSpec{
	{cpu: 4, ram: 2048, sleep: "5", count: 5, label: "heavy (4CPU/2048MB)"},
	{cpu: 2, ram: 512, sleep: "3", count: 15, label: "medium (2CPU/512MB)"},
	{cpu: 1, ram: 256, sleep: "2", count: 30, label: "light (1CPU/256MB)"},
}

func main() {
	log.Printf("=== Brain-and-Muscle Benchmark (Multiplexed) ===")

	// 1. Boot up the single, persistent connection manager
	cm := muscle.NewConnectionManager(brainAddr, "benchmark-client")
	go cm.Run()
	cm.WaitUntilHealthy()
	log.Println("Multiplexed TCP connection established.")

	total := 0
	for _, s := range taskSpecs {
		total += s.count
	}

	taskIDs := make([]string, 0, total)
	var mu sync.Mutex

	submitStart := time.Now()
	var wg sync.WaitGroup

	// 2. Submit all tasks concurrently over the SINGLE connection
	for _, spec := range taskSpecs {
		for i := 0; i < spec.count; i++ {
			wg.Add(1)
			go func(s taskSpec) {
				defer wg.Done()

				payload := fmt.Sprintf(`{"command":"sleep","args":["%s"]}`, s.sleep)
				reqPayload := protocol.SubmitTaskPayload{
					QueueName:   "default",
					Payload:     payload,
					RequiredCPU: s.cpu,
					RequiredRAM: s.ram,
				}

				// cm.SendRequest is thread-safe!
				resp, err := cm.SendRequest(protocol.OpSubmitTask, reqPayload)
				if err != nil {
					log.Printf("submit failed: %v", err)
					return
				}

				var accepted protocol.TaskAcceptedPayload
				json.Unmarshal(resp.Payload, &accepted)

				mu.Lock()
				taskIDs = append(taskIDs, accepted.TaskID)
				mu.Unlock()
			}(spec)
		}
	}

	wg.Wait()
	submitDuration := time.Since(submitStart)
	log.Printf("Submitted %d tasks in %v", len(taskIDs), submitDuration.Round(time.Millisecond))

	// 3. Poll for status over the SINGLE connection
	log.Printf("Waiting for all tasks to complete...")

	var completed atomic.Int64
	var failed atomic.Int64

	for {
		time.Sleep(1 * time.Second)

		var doneCount int64 = 0
		var currentFail int64 = 0

		// We can poll concurrently now because SendRequest handles the multiplexing
		var pollWg sync.WaitGroup
		for _, id := range taskIDs {
			pollWg.Add(1)
			go func(taskID string) {
				defer pollWg.Done()

				resp, err := cm.SendRequest(protocol.OpGetTaskStatus, protocol.GetTaskStatusPayload{
					TaskID: taskID,
				})
				if err != nil {
					return
				}

				var status protocol.TaskStatusPayload
				json.Unmarshal(resp.Payload, &status)

				if status.Status == 2 { // StatusCompleted
					atomic.AddInt64(&doneCount, 1)
				} else if status.Status == 3 { // StatusFailed
					atomic.AddInt64(&currentFail, 1)
					atomic.AddInt64(&doneCount, 1) // Failed tasks are technically "done" running
				}
			}(id)
		}

		pollWg.Wait() // Wait for all 50 status checks to return

		completed.Store(doneCount - currentFail)
		failed.Store(currentFail)

		log.Printf("Progress: %d/%d complete", doneCount, len(taskIDs))

		if int(doneCount) == len(taskIDs) {
			break
		}
	}

	wallTime := time.Since(submitStart)

	fmt.Println("\n========== BENCHMARK RESULTS ==========")
	fmt.Printf("Total tasks:        %d\n", len(taskIDs))
	fmt.Printf("Completed:          %d\n", completed.Load())
	fmt.Printf("Failed:             %d\n", failed.Load())
	fmt.Printf("Submit time:        %v\n", submitDuration.Round(time.Millisecond))
	fmt.Printf("Wall time:          %v\n", wallTime.Round(time.Millisecond))
	fmt.Printf("Throughput:         %.2f tasks/sec\n", float64(len(taskIDs))/wallTime.Seconds())
	fmt.Println("========================================")
}
