package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"context"
	"sync"

	"github.com/akashtripathi12/brain-and-muscle/internal/db"
	"github.com/akashtripathi12/brain-and-muscle/internal/kv"
	"github.com/akashtripathi12/brain-and-muscle/internal/protocol"
	"github.com/akashtripathi12/brain-and-muscle/internal/raft"
	"github.com/akashtripathi12/brain-and-muscle/internal/storage"
	"github.com/golang-jwt/jwt/v5"
)

// normalizeGitURL: Standardize Git URLs for comparison.
func normalizeGitURL(u string) string {
	u = strings.ToLower(strings.TrimSpace(u))
	u = strings.TrimSuffix(u, "/")
	u = strings.TrimSuffix(u, ".git")
	return u
}

type contextKey string

const userIDKey contextKey = "userID"

const SystemUserID = "00000000-0000-0000-0000-000000000000"

// StartAPIGateway: Launch HTTP Ingress and Control API.
func StartAPIGateway(store *kv.KVStore, raftNode *raft.BrainState, brainHost string, httpPort string) {
	mux := http.NewServeMux()

	// Webhook Debounce Map
	var webhookDebounce sync.Map

	// --- MIDDLEWARE HELPERS ---

	apiKey := os.Getenv("BRAIN_API_KEY")
	if apiKey == "" {
		apiKey = "" // Must be provided via environment
	}

	withCORS := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, X-User-Identity, Authorization")

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			h(w, r)
		}
	}

	// JWT + API Key identity middleware.

	jwtSecret := os.Getenv("SUPABASE_JWT_SECRET")
	publicKeyPEM := os.Getenv("SUPABASE_PUBLIC_KEY")
	// Handle escaped newlines from .env
	publicKeyPEM = strings.ReplaceAll(publicKeyPEM, "\\n", "\n")

	withIdentity := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var userID string

			// Strategy A: Supabase JWT (User Identity) - Priority
			authHeader := r.Header.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
				token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
					switch token.Method.Alg() {
					case "HS256":
						return []byte(jwtSecret), nil
					case "ES256":
						return jwt.ParseECPublicKeyFromPEM([]byte(publicKeyPEM))
					default:
						return nil, fmt.Errorf("unsupported alg: %v", token.Method.Alg())
					}
				})

				if err == nil && token.Valid {
					if claims, ok := token.Claims.(jwt.MapClaims); ok {
						// Deep Search: Supabase often nests the email
						var email string
						if val, ok := claims["email"].(string); ok {
							email = val
						} else if meta, ok := claims["user_metadata"].(map[string]interface{}); ok {
							if val, ok := meta["email"].(string); ok {
								email = val
							}
						}

						if email != "" {
							userID = email
						} else if sub, ok := claims["sub"].(string); ok {
							userID = sub
						}
					}
				}
			}
			// Strategy B: X-API-Key (System/Internal Identity Backup)
			if userID == "" {
				providedKey := r.Header.Get("X-API-Key")
				if providedKey != "" && providedKey == apiKey {
					explicitIdentity := r.Header.Get("X-User-Identity")
					if explicitIdentity != "" {
						userID = explicitIdentity
					} else {
						userID = SystemUserID
					}
				}
			}

			if userID == "" {
				http.Error(w, "Unauthorized: Valid JWT or X-API-Key required", http.StatusUnauthorized)
				return
			}

			// Inject Identity into Context
			ctx := context.WithValue(r.Context(), userIDKey, userID)
			h(w, r.WithContext(ctx))
		}
	}

	// --- 1. THE INGRESS PROXY ---

	mux.HandleFunc("/proxy/", withCORS(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(r.URL.Path, "/", 4)
		if len(parts) < 3 {
			http.Error(w, "Invalid Proxy Route format. Use /proxy/{job_id}/...", http.StatusBadRequest)
			return
		}

		jobID := parts[2]
		targetAddress, exists := store.GetServiceEndpoint(jobID)
		if !exists {
			http.Error(w, fmt.Sprintf("Service %s is not running or not ready", jobID), http.StatusBadGateway)
			return
		}

		targetURL, err := url.Parse(fmt.Sprintf("http://%s", targetAddress))
		if err != nil {
			http.Error(w, "Internal proxy parsing error", http.StatusInternalServerError)
			return
		}

		proxy := httputil.NewSingleHostReverseProxy(targetURL)
		if len(parts) == 4 {
			r.URL.Path = "/" + parts[3]
		} else {
			r.URL.Path = "/"
		}

		proxy.ServeHTTP(w, r)
	}))

	// Control Plane API.

	// GET /api/nodes
	mux.HandleFunc("/api/nodes", withCORS(withIdentity(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		nodes := store.GetAllNodes()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(nodes)
	})))

	// GET /api/deployments
	mux.HandleFunc("/api/deployments", withCORS(withIdentity(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		userID := r.Context().Value(userIDKey).(string)
		allJobs := store.GetAllJobs()
		userJobs := []kv.JobSummary{}

		//  MULTI-TENANCY FILTER: Only return jobs owned by this user
		// Special Case: SystemUserID can see everything
		for _, j := range allJobs {
			if userID == SystemUserID || j.UserID == userID {
				userJobs = append(userJobs, j)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(userJobs)
	})))

	// GET /api/history — Retrieve persistent job history from Postgres
	mux.HandleFunc("/api/history", withCORS(withIdentity(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		userID := r.Context().Value(userIDKey).(string)
		history, err := db.GetJobsHistory(r.Context(), userID, 20)
		if err != nil {
			http.Error(w, "Failed to fetch history", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(history)
	})))

	// POST /api/deploy
	mux.HandleFunc("/api/deploy", withCORS(withIdentity(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var sub protocol.SubmitTaskPayload
		if err := json.NewDecoder(r.Body).Decode(&sub); err != nil {
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
			return
		}

		//  IDENTITY INJECTION: Seal the user's email into the payload immediately
		userID := r.Context().Value(userIDKey).(string)
		sub.UserID = userID

		if sub.UserScriptPath == "" && sub.Code == "" {
			http.Error(w, "user_script_path or code is required", http.StatusBadRequest)
			return
		}

		//  ADMISSION CONTROL: Pre-flight Resource Check
		maxCPU, maxRAM := store.GetMaxWorkerCapacity()
		if maxCPU == 0 {
			http.Error(w, "ADMISSION_DENIED: No workers are currently online", http.StatusServiceUnavailable)
			return
		}

		if sub.RequiredCPU > maxCPU || float64(sub.RequiredRAM) > float64(maxRAM) {
			errMsg := fmt.Sprintf("TASK_TOO_LARGE: Requested %.1f CPU / %d MB RAM, but cluster maximum is %.1f CPU / %d MB RAM",
				sub.RequiredCPU, sub.RequiredRAM, maxCPU, maxRAM)
			http.Error(w, errMsg, http.StatusBadRequest)
			return
		}

		// Persist to Supabase (Identity check)
		// We marshal it now that it has the UserID
		payloadBytes, _ := json.Marshal(sub)

		projectID := &sub.ProjectID
		commitHash := &sub.CommitHash
		branchName := &sub.BranchName
		if sub.ProjectID == "" {
			projectID = nil
		}
		if sub.CommitHash == "" {
			commitHash = nil
		}
		if sub.BranchName == "" {
			branchName = nil
		}

		taskID, dbErr := db.CreateTask(r.Context(), userID, payloadBytes, projectID, commitHash, branchName, sub.JobType, sub.RequiredCPU, sub.RequiredRAM)
		if dbErr != nil {
			http.Error(w, "Failed to persist task: "+dbErr.Error(), http.StatusInternalServerError)
			return
		}

		// Cloud Staging: If raw code was provided, stage it to Azure and use the URL
		if sub.Code != "" {
			ext := sub.Extension
			if ext == "" {
				ext = "txt"
			}
			blobName := fmt.Sprintf("scripts/%s.%s", taskID, ext)
			log.Printf("[gateway] ☁️ Staging raw code to Azure: %s", blobName)

			sasURL, err := storage.UploadBlob(r.Context(), blobName, []byte(sub.Code))
			if err != nil {
				log.Printf("[gateway] ❌ Cloud Staging failed: %v", err)
				http.Error(w, "Cloud Staging failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			sub.UserScriptPath = sasURL
			sub.Code = ""      // Clear raw code to keep Raft log small
			sub.Extension = "" // Clear extension
		}

		// Final Raft Proposal
		sub.JobID = taskID
		finalPayload, _ := json.Marshal(sub)
		safePayload := append([]byte("TASK:"), finalPayload...)

		log.Printf("[gateway] Deploying Task %s for %s (Script: %s)", taskID, userID, sub.UserScriptPath)
		index, _, isLeader := raftNode.Propose(safePayload)
		if !isLeader {
			http.Error(w, "Brain is not the Leader. Please redirect to: "+raftNode.LeaderID, http.StatusTemporaryRedirect)
			return
		}

		replyCh := make(chan raft.ClientReply, 1)
		raftNode.RegisterClientChannel(index, replyCh)
		raftNode.SendAppendEntriesToAll()

		select {
		case <-replyCh:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "committed",
				"index":  index,
				"job_id": sub.JobID,
				"git":    sub.GitRepo != "",
			})
		case <-time.After(10 * time.Second):
			http.Error(w, "Deployment timed out waiting for Raft consensus", http.StatusGatewayTimeout)
		}
	})))

	// POST /internal/projects/register
	mux.HandleFunc("/internal/projects/register", withCORS(withIdentity(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var reg protocol.RegisterProjectPayload
		if err := json.NewDecoder(r.Body).Decode(&reg); err != nil {
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
			return
		}

		payloadBytes, _ := json.Marshal(reg)
		safePayload := append([]byte("PROJECT:"), payloadBytes...)

		index, _, isLeader := raftNode.Propose(safePayload)
		if !isLeader {
			http.Error(w, "Brain is not the Leader. Please redirect to: "+raftNode.LeaderID, http.StatusTemporaryRedirect)
			return
		}

		replyCh := make(chan raft.ClientReply, 1)
		raftNode.RegisterClientChannel(index, replyCh)
		raftNode.SendAppendEntriesToAll()

		select {
		case <-replyCh:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "committed",
				"index":  index,
				"repo":   reg.RepoFullName,
			})
		case <-time.After(10 * time.Second):
			http.Error(w, "Project registration timed out waiting for Raft consensus", http.StatusGatewayTimeout)
		}
	})))

	// GET /api/logs/:jobId — Proxy to the worker's log server with chunked streaming
	mux.HandleFunc("/api/logs/", withCORS(withIdentity(func(w http.ResponseWriter, r *http.Request) {
		jobID := strings.TrimPrefix(r.URL.Path, "/api/logs/")
		if jobID == "" {
			http.Error(w, "Missing jobId", http.StatusBadRequest)
			return
		}

		// 1. Generate a temporary, secure Read SAS for the log file in Azure
		logBlobName := fmt.Sprintf("logs/%s.txt", jobID)

		signedURL, err := storage.GenerateReadSAS(logBlobName)
		if err != nil {
			log.Printf("[gateway] ❌ Failed to generate log link: %v", err)
			http.Error(w, "Failed to generate log link", http.StatusInternalServerError)
			return
		}

		// 2. Redirect the client directly to Azure Blob Storage
		http.Redirect(w, r, signedURL, http.StatusTemporaryRedirect)
	})))

	// GET /api/tasks/:jobId/download — Generate a temporary SAS URL for the frontend
	mux.HandleFunc("/api/tasks/", withCORS(withIdentity(func(w http.ResponseWriter, r *http.Request) {
		userID := r.Context().Value(userIDKey).(string)
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 5 || parts[4] != "download" {
			http.Error(w, "Invalid route. Use /api/tasks/{job_id}/download", http.StatusBadRequest)
			return
		}

		jobID := parts[3]

		// 1. Ask memory store first (fast)
		var baseURL string
		if job, ok := store.GetJob(jobID); ok && job.Result != "" {
			if userID != SystemUserID && job.UserID != userID {
				http.Error(w, "Unauthorized: You do not own this job", http.StatusForbidden)
				return
			}
			baseURL = job.Result
		}

		// 2. Fallback: Ask Postgres for the Base URL
		if baseURL == "" {
			log.Printf("[gateway] 🔍 Download Link %s NOT IN MEMORY. Checking Postgres...", jobID)
			// Relaxed query: Find any record for this ID that has a blob URL
			query := `SELECT result_blob_url FROM tasks WHERE id = $1`
			err := db.Pool.QueryRow(r.Context(), query, jobID).Scan(&baseURL)
			if err != nil {
				log.Printf("[gateway] ❌ Postgres Download Recovery Failed for %s: %v", jobID, err)
				http.Error(w, "Task not found in DB", http.StatusNotFound)
				return
			}
			log.Printf("[gateway] ✅ Recovered Download URL from Postgres: %s", baseURL)
		}

		// 3. Extract just the filename from the URL
		cleanURL := strings.TrimSpace(baseURL)

		//  ROBUST JSON DETECTION: If it starts with {, it's likely a wrapped pointer
		if strings.HasPrefix(cleanURL, "{") {
			var blobPtr struct {
				BlobURL string `json:"blob_url"`
			}
			if err := json.Unmarshal([]byte(cleanURL), &blobPtr); err == nil && blobPtr.BlobURL != "" {
				cleanURL = blobPtr.BlobURL
			}
		}

		// Ensure we don't have quotes or double braces left over
		cleanURL = strings.Trim(cleanURL, " \"{}")

		uParts := strings.Split(cleanURL, "/")
		fileName := uParts[len(uParts)-1]

		log.Printf("[gateway] 📄 Generating secure SAS for %s (blob: %s)", jobID, fileName)

		// 4. Generate the 15-minute secure download link
		secureDownloadURL, err := storage.GenerateReadSAS(fileName)
		if err != nil {
			http.Error(w, "Failed to generate secure link", http.StatusInternalServerError)
			return
		}

		// 5. Send it back to Next.js
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"job_id":       jobID,
			"download_url": secureDownloadURL,
		})
	})))

	// POST /api/kill/:jobId — Kill a running deployment
	mux.HandleFunc("/api/kill/", withCORS(withIdentity(func(w http.ResponseWriter, r *http.Request) {
		userID := r.Context().Value(userIDKey).(string)
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		jobID := strings.TrimPrefix(r.URL.Path, "/api/kill/")
		if jobID == "" {
			http.Error(w, "Missing jobId", http.StatusBadRequest)
			return
		}

		task, ok := store.GetDaemonTaskForJob(jobID)
		if !ok {
			http.Error(w, "No daemon task found for job "+jobID, http.StatusNotFound)
			return
		}

		if userID != SystemUserID && task.UserID != userID {
			http.Error(w, "Unauthorized: You do not own this task", http.StatusForbidden)
			return
		}

		if task.Status != kv.StatusRunning {
			http.Error(w, "Task is not running", http.StatusBadRequest)
			return
		}

		connRegistryMu.RLock()
		workerSend, isOnline := connRegistry[task.AssignedWorkerID]
		connRegistryMu.RUnlock()

		if !isOnline {
			http.Error(w, "Worker "+task.AssignedWorkerID+" is currently offline", http.StatusBadGateway)
			return
		}

		workerSend(protocol.OpKillTask, "", protocol.KillTaskPayload{
			TaskID: task.ID,
		})

		log.Printf("[gateway] Kill signal sent for job %s (task %s) to worker %s", jobID, task.ID, task.AssignedWorkerID)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "kill_signal_sent",
			"task_id": task.ID,
			"job_id":  jobID,
		})
	})))

	// DELETE /api/delete/:jobId — Delete a deployment entirely
	mux.HandleFunc("/api/delete/", withCORS(withIdentity(func(w http.ResponseWriter, r *http.Request) {
		userID := r.Context().Value(userIDKey).(string)
		if r.Method != "POST" && r.Method != "DELETE" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		jobID := strings.TrimPrefix(r.URL.Path, "/api/delete/")
		if jobID == "" {
			http.Error(w, "Missing jobId", http.StatusBadRequest)
			return
		}

		// Verify Ownership
		task, ok := store.GetDaemonTaskForJob(jobID)
		if ok {
			if userID != SystemUserID && task.UserID != userID {
				http.Error(w, "Unauthorized: You do not own this deployment", http.StatusForbidden)
				return
			}
		} else {
			// Fallback: Check Job directly if no task exists yet
			if job, ok := store.GetJob(jobID); ok {
				if userID != SystemUserID && job.UserID != userID {
					http.Error(w, "Unauthorized: You do not own this job", http.StatusForbidden)
					return
				}
			}
		}

		// Kill the container if running
		if ok && task.Status == kv.StatusRunning {
			connRegistryMu.RLock()
			workerSend, isOnline := connRegistry[task.AssignedWorkerID]
			connRegistryMu.RUnlock()

			if isOnline {
				workerSend(protocol.OpKillTask, "", protocol.KillTaskPayload{
					TaskID: task.ID,
				})
				time.Sleep(1 * time.Second)
			}
		}

		store.RemoveJob(jobID)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status": "deleted",
			"job_id": jobID,
		})
	})))

	// POST /api/webhooks/github — Automated In-Place Update
	mux.HandleFunc("/api/webhooks/github", withCORS(func(w http.ResponseWriter, r *http.Request) {
		event := r.Header.Get("X-GitHub-Event")

		if event == "ping" {
			log.Println("[webhook] 🔔 GitHub Ping received: Link established successfully.")
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method != "POST" {
			w.WriteHeader(http.StatusOK)
			return
		}

		var payload struct {
			Ref        string `json:"ref"`
			After      string `json:"after"` //  NEW: Commit Hash
			Repository struct {
				CloneURL string `json:"clone_url"`
				FullName string `json:"full_name"` //  NEW: For project mapping
			} `json:"repository"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid GitHub payload", http.StatusBadRequest)
			return
		}

		repoURL := payload.Repository.CloneURL
		repoFullName := payload.Repository.FullName
		commitHash := payload.After
		branchName := strings.TrimPrefix(payload.Ref, "refs/heads/")

		if event == "push" {
			//  DEBOUNCE LOGIC: Skip if we processed this exact commit recently.
			debounceKey := fmt.Sprintf("%s:%s", repoFullName, commitHash)
			actual, loaded := webhookDebounce.LoadOrStore(debounceKey, time.Now())
			if loaded {
				if time.Since(actual.(time.Time)) < 15*time.Second {
					log.Printf("[webhook] 🛡️ Ignoring duplicate push for %s (already processing commit %.7s)", repoFullName, commitHash)
					w.WriteHeader(http.StatusOK)
					return
				}
				//  Update the time if we are past the 15s window
				webhookDebounce.Store(debounceKey, time.Now())
			}

			log.Printf("[webhook] Incoming Push for %s (Branch: %s, Commit: %.7s)", repoFullName, branchName, commitHash)
		} else {
			log.Printf("[webhook] Incoming %s event for %s", strings.ToUpper(event), repoFullName)
		}

		//  EVOLUTION: Look up the Project ID in the new projects table
		var projectID string
		db.Pool.QueryRow(r.Context(), "SELECT id FROM projects WHERE repo_full_name = $1", repoFullName).Scan(&projectID)

		jobs := store.GetAllJobs()
		restartedCount := 0
		normRepoURL := normalizeGitURL(repoURL)

		for _, job := range jobs {
			originalPayloadStr, ok := store.GetJobPayload(job.JobID)
			if !ok {
				continue
			}

			var sub protocol.SubmitTaskPayload
			if err := json.Unmarshal([]byte(originalPayloadStr), &sub); err != nil {
				continue
			}

			effectiveGitURL := sub.GitRepo
			if effectiveGitURL == "" && sub.SourceType == "github" {
				effectiveGitURL = sub.UserScriptPath
			}

			if normalizeGitURL(effectiveGitURL) == normRepoURL {
				log.Printf("[webhook] 🚀 Restarting %s with updated metadata", job.JobID)

				// INJECT NEW METADATA
				sub.JobID = job.JobID
				sub.CommitHash = commitHash
				sub.BranchName = branchName
				sub.ProjectID = projectID

				// KILL OLD DAEMON
				task, ok := store.GetDaemonTaskForJob(job.JobID)
				if ok && task.Status == kv.StatusRunning {
					connRegistryMu.RLock()
					workerSend, isOnline := connRegistry[task.AssignedWorkerID]
					connRegistryMu.RUnlock()
					if isOnline {
						workerSend(protocol.OpKillTask, "", protocol.KillTaskPayload{TaskID: task.ID})
					}
				}

				// PROPOSE UPDATE
				restartBytes, _ := json.Marshal(sub)
				safePayload := append([]byte("TASK:"), restartBytes...)
				raftNode.Propose(safePayload)
				raftNode.SendAppendEntriesToAll()
				restartedCount++
			}
		}

		if event == "push" && restartedCount == 0 {
			log.Printf("[webhook] ℹ️ Push received for %s, but no active deployments found to update. Deploy the project first!", repoFullName)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"jobs_updated": restartedCount})
	}))

	server := &http.Server{
		Addr:    ":" + httpPort,
		Handler: withCORS(mux.ServeHTTP),
	}

	//  BLOCKING CALL: Keeps the Brain alive
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Gateway failure: %v", err)
	}
}
