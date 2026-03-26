package main

import (
	"log"
	"sync"
	"time"

	"github.com/akashtripathi12/brain-and-muscle/internal/muscle"
	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
)

func main() {
	cm := muscle.NewConnectionManager("localhost:8080", "load-tester")
	go cm.Run()
	cm.WaitUntilHealthy()

	log.Println("Firing 10,000 concurrent tasks...")
	start := time.Now()

	var wg sync.WaitGroup
	successCount := 0
	var countMu sync.Mutex

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(taskNum int) {
			defer wg.Done()
			req := protocol.SubmitTaskPayload{
				QueueName: "default",
				Payload:   "stress-test-payload",
			}
			// All 10,000 goroutines fighting for cm.connMu!
			_, err := cm.SendRequest(protocol.OpSubmitTask, req)
			if err == nil {
				countMu.Lock()
				successCount++
				countMu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	log.Printf("Load test complete in %v. Successful tasks: %d/10000", time.Since(start), successCount)
}
