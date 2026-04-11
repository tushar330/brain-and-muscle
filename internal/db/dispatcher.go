package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// DBTask represents a row fetched from the Postgres tasks table.
type DBTask struct {
	ID            string          `json:"id"`
	Payload       json.RawMessage `json:"payload"`
	ResultBlobURL *string         `json:"result_blob_url,omitempty"`
}

// DBJob matches the new jobs table structure
type DBJob struct {
	ID          string     `json:"job_id"`
	UserID      string     `json:"user_id"`
	JobType     string     `json:"job_type"`
	Status      int        `json:"status"`
	Payload     string     `json:"payload"`
	Result      *string    `json:"result"`
	CreatedAt   time.Time  `json:"created_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

type WorkAssignment struct {
	TaskID  string          `json:"task_id"`
	Payload json.RawMessage `json:"payload"`
}

var ErrNoTasksAvailable = errors.New("no pending tasks in the queue")

func FetchWork(brainURL string) (*WorkAssignment, *DBTask, error) {
	resp, err := http.Get(brainURL + "/api/work")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to contact brain: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil, nil // No work available right now
	}

	// 1. Decode the wrapper (to get the Postgres ID)
	var assignment WorkAssignment
	if err := json.NewDecoder(resp.Body).Decode(&assignment); err != nil {
		return nil, nil, fmt.Errorf("failed to decode assignment: %v", err)
	}

	// 2. Decode the inner payload into a DBTask
	var myTask DBTask
	myTask.ID = assignment.TaskID
	myTask.Payload = assignment.Payload

	return &assignment, &myTask, nil
}

// CheckoutNextTask grabs the oldest pending task, marks it running, and returns it.
func CheckoutNextTask(ctx context.Context, availCPU float64, availRAM int, workerID string) (*DBTask, error) {
	// THE MAGIC QUERY: Now with Resource-Awareness and Worker Persistence!
	// We use jsonb_set to inject the assigned_worker_id directly into the payload.
	query := `
		UPDATE tasks 
		SET status = 'running', 
		    updated_at = NOW(),
		    payload = jsonb_set(payload::jsonb, '{assigned_worker_id}', to_jsonb($3::text))
		WHERE id = (
			SELECT id FROM tasks 
			WHERE status = 'pending' 
			AND (payload::jsonb->>'job_type') NOT IN ('ML_TRAIN', 'SCATTER', 'MAP')
			AND (payload::jsonb->>'required_cpu')::float <= $1
			AND (payload::jsonb->>'required_ram')::int <= $2
			ORDER BY created_at ASC 
			LIMIT 1 
			FOR UPDATE SKIP LOCKED
		) 
		RETURNING id, payload;
	`

	var task DBTask
	err := Pool.QueryRow(ctx, query, availCPU, availRAM, workerID).Scan(&task.ID, &task.Payload)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// This isn't a crash, it just means the queue is empty right now.
			return nil, ErrNoTasksAvailable
		}
		return nil, fmt.Errorf("failed to checkout task: %v", err)
	}

	log.Printf("[Dispatcher] Task %s checked out and marked 'running'", task.ID)
	return &task, nil
}

// CompleteTask marks a task as done and saves the Azure Blob URL
func CompleteTask(ctx context.Context, taskID string, blobURL string) error {
	//  FIX: Sanitize ID for Postgres (Strip Map-Reduce suffixes like -map-0 or -gather)
	cleanID := taskID
	if idx := strings.Index(cleanID, "-map-"); idx != -1 {
		cleanID = cleanID[:idx]
	} else if idx := strings.Index(cleanID, "-gather"); idx != -1 {
		cleanID = cleanID[:idx]
	} else if idx := strings.Index(cleanID, "-worker-"); idx != -1 {
		cleanID = cleanID[:idx]
	}

	//  VALIDATION: Ensure it's a valid UUID format before hitting Postgres
	// (Prevents: invalid input syntax for type uuid)
	if len(cleanID) < 36 {
		log.Printf("[Dispatcher] ℹ️ Skipping Postgres sync for non-UUID task: %s (Raft-only)", taskID)
		return nil
	}

	// 1. Recover current payload so we can inject the result into JSON
	var currentPayload string
	err := Pool.QueryRow(ctx, "SELECT payload FROM tasks WHERE id = $1", cleanID).Scan(&currentPayload)
	if err != nil {
		return fmt.Errorf("failed to fetch task payload for update: %v", err)
	}

	var pMap map[string]interface{}
	json.Unmarshal([]byte(currentPayload), &pMap)
	pMap["archive_url"] = blobURL
	newPayload, _ := json.Marshal(pMap)

	// 2. Perform Atomic Update
	var blobParam *string
	if strings.HasPrefix(blobURL, "http") {
		blobParam = &blobURL
	}

	query := `
		UPDATE tasks 
		SET status = 'completed', result_blob_url = $1, payload = $2, updated_at = NOW() 
		WHERE id = $3;
	`

	tag, err := Pool.Exec(ctx, query, blobParam, string(newPayload), cleanID)
	if err != nil {
		return fmt.Errorf("failed to complete task: %v", err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("task %s not found in DB (ID: %s)", taskID, cleanID)
	}

	log.Printf("[Dispatcher] Task %s marked 'completed' and synced to JSON (CleanID: %s)", taskID, cleanID)
	return nil
}

// FailTask allows a Muscle to report that a job completely crashed
func FailTask(ctx context.Context, taskID string, errorMsg string) error {
	// We can append the error message into the JSON payload, or just mark it failed
	query := `
		UPDATE tasks 
		SET status = 'failed', updated_at = NOW() 
		WHERE id = $1 AND status = 'running';
	`

	_, err := Pool.Exec(ctx, query, taskID)
	if err != nil {
		return fmt.Errorf("failed to mark task as failed: %v", err)
	}

	log.Printf("[Dispatcher] ⚠️ Task %s marked 'failed'", taskID)
	return nil
}
