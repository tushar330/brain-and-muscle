package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is our global connection pool
var Pool *pgxpool.Pool

func InitDB() error {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return fmt.Errorf("DATABASE_URL is not set in .env")
	}

	// Create a connection pool (perfect for handling multiple Muscle requests)
	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return fmt.Errorf("error parsing db config: %v", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}

	// Test the connection
	if err := pool.Ping(context.Background()); err != nil {
		return fmt.Errorf("database ping failed: %v", err)
	}

	log.Println("Successfully connected to Supabase PostgreSQL pool")
	Pool = pool
	return nil
}

// SyncJob upserts the overarching Job state into Postgres
func SyncJob(ctx context.Context, jobID, userID string, status int, payload string, result *string, jobType string) error {
	//  MIGRATION FIX: Default empty identity to System
	safeUserID := userID
	if safeUserID == "" {
		safeUserID = "00000000-0000-0000-0000-000000000000"
	}

	query := `
		INSERT INTO jobs (id, user_id, status, payload, result, job_type, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
		ON CONFLICT (id) DO UPDATE 
		SET user_id = EXCLUDED.user_id,
		    status = EXCLUDED.status, 
		    result = EXCLUDED.result, 
		    completed_at = CASE WHEN EXCLUDED.status >= 2 THEN NOW() ELSE jobs.completed_at END;
	`
	//  DIAGNOSTIC: Log status transitions to help debug history stalls
	if status >= 2 {
		log.Printf("[db] 🏁 Syncing terminal state for %s: Status=%d", jobID, status)
	}

	_, err := Pool.Exec(ctx, query, jobID, safeUserID, status, payload, result, jobType)
	if err != nil {
		log.Printf("[db] ❌ SyncJob Error for %s: %v", jobID, err)
	}
	return err
}

// GetJobsHistory retrieves the most recent jobs for a user (excluding active services usually)
func GetJobsHistory(ctx context.Context, userID string, limit int) ([]DBJob, error) {
	query := `
		SELECT id, user_id, job_type, status, payload, result, created_at, completed_at 
		FROM jobs 
		WHERE user_id = $1
		ORDER BY created_at DESC 
		LIMIT $2;
	`
	rows, err := Pool.Query(ctx, query, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := []DBJob{}
	for rows.Next() {
		var j DBJob
		err := rows.Scan(&j.ID, &j.UserID, &j.JobType, &j.Status, &j.Payload, &j.Result, &j.CreatedAt, &j.CompletedAt)
		if err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// CreateTask pushes a new job into the queue with optional project/git metadata
func CreateTask(ctx context.Context, userID string, payload []byte, projectID *string, commitHash *string, branchName *string, jobType string, cpu float64, ram int) (string, error) {
	var taskID string

	//  ENHANCEMENT: Inject metadata into the JSON payload before saving
	var pMap map[string]interface{}
	json.Unmarshal(payload, &pMap)
	pMap["job_type"] = jobType
	pMap["required_cpu"] = cpu
	pMap["required_ram"] = ram
	pMap["status"] = "pending"
	pMap["user_id"] = userID // Ensure ID is in payload too
	updatedPayload, _ := json.Marshal(pMap)

	// Handle projectID as UUID (it might be nil/empty)
	var pID interface{}
	if projectID != nil && *projectID != "" {
		pID = *projectID
	} else {
		pID = nil
	}

	//  MIGRATION: userID is now TEXT/Email
	query := `
		INSERT INTO tasks (payload, project_id, commit_hash, branch_name, user_id) 
		VALUES ($1, $2, $3, $4, $5) 
		RETURNING id;
	`

	err := Pool.QueryRow(ctx, query, string(updatedPayload), pID, commitHash, branchName, userID).Scan(&taskID)
	if err != nil {
		return "", fmt.Errorf("failed to insert task (identity %s): %v", userID, err)
	}

	return taskID, nil
}

// GetTaskByID retrieves a single task record by its UUID
func GetTaskByID(ctx context.Context, id string) (*DBTask, error) {
	query := `SELECT id, payload, result_blob_url FROM tasks WHERE id = $1`
	var task DBTask
	err := Pool.QueryRow(ctx, query, id).Scan(&task.ID, &task.Payload, &task.ResultBlobURL)
	if err != nil {
		return nil, err
	}
	return &task, nil
}

// UpdateTaskResult persistently saves the final ArchiveURL (logs/results) to Postgres
func UpdateTaskResult(ctx context.Context, id string, archiveURL string) error {
	// 1. Get current payload
	task, err := GetTaskByID(ctx, id)
	if err != nil {
		return err
	}

	// 2. Inject archive_url into JSON
	var payload map[string]interface{}
	json.Unmarshal([]byte(task.Payload), &payload)
	payload["archive_url"] = archiveURL
	newPayload, _ := json.Marshal(payload)

	// 3. Update DB
	query := `UPDATE tasks SET payload = $1, status = 'completed', updated_at = NOW() WHERE id = $2`
	_, err = Pool.Exec(ctx, query, string(newPayload), id)
	return err
}
