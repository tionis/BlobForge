package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"

	"blobforge/server/auth"
	"blobforge/server/db"
	"blobforge/server/sse"
)

// S3Client interface for dependency injection and testing
type S3Client interface {
	PresignedDownloadURL(ctx context.Context, key string, expiry time.Duration) (string, error)
	PresignedUploadURL(ctx context.Context, key string, contentType string, expiry time.Duration) (string, error)
}

type Handler struct {
	db        *db.DB
	s3        S3Client
	sse       *sse.Hub
	urlExpiry time.Duration
}

func NewHandler(database *db.DB, s3Client S3Client, sseHub *sse.Hub) *Handler {
	return &Handler{
		db:        database,
		s3:        s3Client,
		sse:       sseHub,
		urlExpiry: 1 * time.Hour,
	}
}

// Helper functions

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// ============================================
// Worker endpoints
// ============================================

// POST /api/workers/register
func (h *Handler) RegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       string          `json:"id"`
		Type     string          `json:"type"`
		Metadata json.RawMessage `json:"metadata,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ID == "" || req.Type == "" {
		writeError(w, http.StatusBadRequest, "id and type are required")
		return
	}

	worker := &db.Worker{
		ID:       req.ID,
		Type:     req.Type,
		Metadata: req.Metadata,
	}
	if err := h.db.CreateWorker(worker); err != nil {
		log.Error().Err(err).Str("worker_id", req.ID).Msg("failed to register worker")
		writeError(w, http.StatusInternalServerError, "failed to register worker")
		return
	}

	log.Info().Str("worker_id", req.ID).Str("type", req.Type).Msg("worker registered")

	// Broadcast worker online
	h.sse.BroadcastWorkerStatus(req.ID, "online")

	writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

// POST /api/workers/{id}/heartbeat
func (h *Handler) WorkerHeartbeat(w http.ResponseWriter, r *http.Request) {
	workerID := chi.URLParam(r, "id")

	var req struct {
		JobID    *int64          `json:"job_id,omitempty"`
		Progress json.RawMessage `json:"progress,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := h.db.UpdateWorkerHeartbeat(workerID, req.JobID, req.Progress); err != nil {
		log.Error().Err(err).Str("worker_id", workerID).Msg("failed to update heartbeat")
		writeError(w, http.StatusInternalServerError, "failed to update heartbeat")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// POST /api/workers/{id}/unregister
func (h *Handler) UnregisterWorker(w http.ResponseWriter, r *http.Request) {
	workerID := chi.URLParam(r, "id")

	if err := h.db.SetWorkerOffline(workerID); err != nil {
		log.Error().Err(err).Str("worker_id", workerID).Msg("failed to unregister worker")
		writeError(w, http.StatusInternalServerError, "failed to unregister worker")
		return
	}

	log.Info().Str("worker_id", workerID).Msg("worker unregistered")

	// Broadcast worker offline
	h.sse.BroadcastWorkerStatus(workerID, "offline")

	writeJSON(w, http.StatusOK, map[string]string{"status": "unregistered"})
}

// GET /api/workers
func (h *Handler) ListWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := h.db.ListWorkers()
	if err != nil {
		log.Error().Err(err).Msg("failed to list workers")
		writeError(w, http.StatusInternalServerError, "failed to list workers")
		return
	}
	writeJSON(w, http.StatusOK, workers)
}

// POST /api/workers/{id}/drain
func (h *Handler) DrainWorker(w http.ResponseWriter, r *http.Request) {
	workerID := chi.URLParam(r, "id")

	if err := h.db.SetWorkerDraining(workerID); err != nil {
		log.Error().Err(err).Str("worker_id", workerID).Msg("failed to drain worker")
		writeError(w, http.StatusInternalServerError, "failed to drain worker")
		return
	}

	log.Info().Str("worker_id", workerID).Msg("worker set to draining")
	h.sse.BroadcastWorkerStatus(workerID, "draining")

	writeJSON(w, http.StatusOK, map[string]string{"status": "draining"})
}

// DELETE /api/workers/{id}
func (h *Handler) RemoveWorker(w http.ResponseWriter, r *http.Request) {
	workerID := chi.URLParam(r, "id")

	// Release any jobs first
	if err := h.db.ReleaseJobsFromWorker(workerID); err != nil {
		log.Error().Err(err).Str("worker_id", workerID).Msg("failed to release jobs from worker")
	}

	if err := h.db.DeleteWorker(workerID); err != nil {
		log.Error().Err(err).Str("worker_id", workerID).Msg("failed to remove worker")
		writeError(w, http.StatusInternalServerError, "failed to remove worker")
		return
	}

	log.Info().Str("worker_id", workerID).Msg("worker removed")
	h.sse.BroadcastWorkerStatus(workerID, "removed")

	writeJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

// ============================================
// Job endpoints
// ============================================

// POST /api/jobs
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Type        string          `json:"type"`
		SourceHash  string          `json:"source_hash"`
		SourcePath  string          `json:"source_path,omitempty"`
		SourceSize  int64           `json:"source_size,omitempty"`
		Priority    *int            `json:"priority,omitempty"`
		MaxAttempts *int            `json:"max_attempts,omitempty"`
		Tags        json.RawMessage `json:"tags,omitempty"`
		Metadata    json.RawMessage `json:"metadata,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Type == "" || req.SourceHash == "" {
		writeError(w, http.StatusBadRequest, "type and source_hash are required")
		return
	}

	// Check for existing job
	existing, err := h.db.GetJobByHash(req.Type, req.SourceHash)
	if err != nil {
		log.Error().Err(err).Msg("failed to check for existing job")
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}
	if existing != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"job_id":   existing.ID,
			"existing": true,
			"status":   existing.Status,
		})
		return
	}

	job := &db.Job{
		Type:       req.Type,
		SourceHash: req.SourceHash,
		SourcePath: req.SourcePath,
		SourceSize: req.SourceSize,
		Tags:       req.Tags,
		Metadata:   req.Metadata,
	}
	if req.Priority != nil {
		job.Priority = *req.Priority
	} else {
		job.Priority = 3 // Default priority
	}
	if req.MaxAttempts != nil {
		job.MaxAttempts = *req.MaxAttempts
	} else {
		job.MaxAttempts = 3
	}

	jobID, err := h.db.CreateJob(job)
	if err != nil {
		log.Error().Err(err).Msg("failed to create job")
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}

	log.Info().Int64("job_id", jobID).Str("type", req.Type).Msg("job created")

	// Broadcast job created
	h.sse.BroadcastJobCreated(jobID, req.Type)

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"job_id":   jobID,
		"existing": false,
	})
}

// GET /api/jobs
func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	jobType := r.URL.Query().Get("type")
	limit := 50
	offset := 0

	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 100 {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}

	jobs, total, err := h.db.ListJobs(status, jobType, limit, offset)
	if err != nil {
		log.Error().Err(err).Msg("failed to list jobs")
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"jobs":   jobs,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// GET /api/jobs/{id}
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job ID")
		return
	}

	job, err := h.db.GetJob(id)
	if err != nil {
		log.Error().Err(err).Int64("job_id", id).Msg("failed to get job")
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	if job == nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	writeJSON(w, http.StatusOK, job)
}

// POST /api/jobs/claim
func (h *Handler) ClaimJob(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkerID string `json:"worker_id"`
		JobType  string `json:"job_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.WorkerID == "" || req.JobType == "" {
		writeError(w, http.StatusBadRequest, "worker_id and job_type are required")
		return
	}

	job, err := h.db.ClaimJob(req.WorkerID, req.JobType)
	if err != nil {
		log.Error().Err(err).Str("worker_id", req.WorkerID).Msg("failed to claim job")
		writeError(w, http.StatusInternalServerError, "failed to claim job")
		return
	}

	if job == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"job": nil,
		})
		return
	}

	// Generate source download URL
	sourceURL, err := h.s3.PresignedDownloadURL(r.Context(), "sources/"+job.SourceHash, h.urlExpiry)
	if err != nil {
		log.Error().Err(err).Int64("job_id", job.ID).Msg("failed to generate source URL")
		writeError(w, http.StatusInternalServerError, "failed to generate source URL")
		return
	}

	// Generate upload URL for output
	outputKey := "outputs/" + job.SourceHash
	uploadURL, err := h.s3.PresignedUploadURL(r.Context(), outputKey, "application/octet-stream", h.urlExpiry)
	if err != nil {
		log.Error().Err(err).Int64("job_id", job.ID).Msg("failed to generate upload URL")
		writeError(w, http.StatusInternalServerError, "failed to generate upload URL")
		return
	}

	log.Info().Int64("job_id", job.ID).Str("worker_id", req.WorkerID).Msg("job claimed")

	// Broadcast job update
	h.sse.BroadcastJobUpdated(job.ID, "running")

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"job":        job,
		"source_url": sourceURL,
		"upload_url": uploadURL,
		"output_key": outputKey,
	})
}

// POST /api/jobs/{id}/complete
func (h *Handler) CompleteJob(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job ID")
		return
	}

	var req struct {
		WorkerID  string          `json:"worker_id"`
		OutputKey string          `json:"output_key,omitempty"`
		Result    json.RawMessage `json:"result,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.WorkerID == "" {
		writeError(w, http.StatusBadRequest, "worker_id is required")
		return
	}

	// Record output file if provided
	if req.OutputKey != "" {
		file := &db.File{
			JobID: id,
			Type:  "output",
			S3Key: req.OutputKey,
		}
		if _, err := h.db.CreateFile(file); err != nil {
			log.Error().Err(err).Int64("job_id", id).Msg("failed to record output file")
		}
	}

	if err := h.db.CompleteJob(id, req.WorkerID, req.Result); err != nil {
		log.Error().Err(err).Int64("job_id", id).Msg("failed to complete job")
		writeError(w, http.StatusInternalServerError, "failed to complete job")
		return
	}

	log.Info().Int64("job_id", id).Str("worker_id", req.WorkerID).Msg("job completed")

	// Broadcast job completed
	h.sse.BroadcastJobCompleted(id)

	writeJSON(w, http.StatusOK, map[string]string{"status": "completed"})
}

// POST /api/jobs/{id}/fail
func (h *Handler) FailJob(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job ID")
		return
	}

	var req struct {
		WorkerID string `json:"worker_id"`
		Error    string `json:"error"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.WorkerID == "" || req.Error == "" {
		writeError(w, http.StatusBadRequest, "worker_id and error are required")
		return
	}

	willRetry, err := h.db.FailJob(id, req.WorkerID, req.Error)
	if err != nil {
		log.Error().Err(err).Int64("job_id", id).Msg("failed to fail job")
		writeError(w, http.StatusInternalServerError, "failed to fail job")
		return
	}

	log.Warn().Int64("job_id", id).Str("worker_id", req.WorkerID).Str("error", req.Error).Bool("will_retry", willRetry).Msg("job failed")

	// Broadcast job failed
	h.sse.BroadcastJobFailed(id, req.Error)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":     "failed",
		"will_retry": willRetry,
	})
}

// POST /api/jobs/{id}/retry
func (h *Handler) RetryJob(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job ID")
		return
	}

	var req struct {
		ResetAttempts bool `json:"reset_attempts,omitempty"`
		Priority      *int `json:"priority,omitempty"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	if err := h.db.RetryJob(id, req.ResetAttempts, req.Priority); err != nil {
		log.Error().Err(err).Int64("job_id", id).Msg("failed to retry job")
		writeError(w, http.StatusInternalServerError, "failed to retry job")
		return
	}

	log.Info().Int64("job_id", id).Msg("job retried")

	// Broadcast job update
	h.sse.BroadcastJobUpdated(id, "pending")

	writeJSON(w, http.StatusOK, map[string]string{"status": "retrying"})
}

// POST /api/jobs/{id}/cancel
func (h *Handler) CancelJob(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job ID")
		return
	}

	if err := h.db.CancelJob(id); err != nil {
		log.Error().Err(err).Int64("job_id", id).Msg("failed to cancel job")
		writeError(w, http.StatusInternalServerError, "failed to cancel job")
		return
	}

	log.Info().Int64("job_id", id).Msg("job cancelled")

	// Broadcast job update
	h.sse.BroadcastJobUpdated(id, "cancelled")

	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

// POST /api/jobs/{id}/priority
func (h *Handler) UpdateJobPriority(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job ID")
		return
	}

	var req struct {
		Priority int `json:"priority"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Priority < 1 || req.Priority > 5 {
		writeError(w, http.StatusBadRequest, "priority must be between 1 and 5")
		return
	}

	if err := h.db.UpdateJobPriority(id, req.Priority); err != nil {
		log.Error().Err(err).Int64("job_id", id).Msg("failed to update job priority")
		writeError(w, http.StatusInternalServerError, "failed to update priority")
		return
	}

	log.Info().Int64("job_id", id).Int("priority", req.Priority).Msg("job priority updated")

	// Broadcast job update
	h.sse.BroadcastJobUpdated(id, "pending")

	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

// ============================================
// File endpoints
// ============================================

// GET /api/files/{job_id}/{type}/url
func (h *Handler) GetFileURL(w http.ResponseWriter, r *http.Request) {
	jobIDStr := chi.URLParam(r, "job_id")
	jobID, err := strconv.ParseInt(jobIDStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job ID")
		return
	}

	fileType := chi.URLParam(r, "type")
	if fileType != "source" && fileType != "output" {
		writeError(w, http.StatusBadRequest, "type must be 'source' or 'output'")
		return
	}

	job, err := h.db.GetJob(jobID)
	if err != nil {
		log.Error().Err(err).Int64("job_id", jobID).Msg("failed to get job")
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	if job == nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	var key string
	if fileType == "source" {
		key = "sources/" + job.SourceHash
	} else {
		key = "outputs/" + job.SourceHash
	}

	url, err := h.s3.PresignedDownloadURL(r.Context(), key, h.urlExpiry)
	if err != nil {
		log.Error().Err(err).Int64("job_id", jobID).Str("type", fileType).Msg("failed to generate URL")
		writeError(w, http.StatusInternalServerError, "failed to generate URL")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"url": url})
}

// POST /api/files/upload-url
func (h *Handler) GetUploadURL(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key         string `json:"key"`
		ContentType string `json:"content_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}
	if req.ContentType == "" {
		req.ContentType = "application/octet-stream"
	}

	url, err := h.s3.PresignedUploadURL(r.Context(), req.Key, req.ContentType, h.urlExpiry)
	if err != nil {
		log.Error().Err(err).Str("key", req.Key).Msg("failed to generate upload URL")
		writeError(w, http.StatusInternalServerError, "failed to generate upload URL")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"url": url, "key": req.Key})
}

// ============================================
// Stats endpoint
// ============================================

// GET /api/stats
func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.db.GetStats()
	if err != nil {
		log.Error().Err(err).Msg("failed to get stats")
		writeError(w, http.StatusInternalServerError, "failed to get stats")
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// ============================================
// Health check
// ============================================

// GET /health
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

// ============================================
// Admin endpoints
// ============================================

// GET /api/tokens
func (h *Handler) ListAPITokens(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")

	tokens, err := h.db.ListAPITokens(userID)
	if err != nil {
		log.Error().Err(err).Msg("failed to list API tokens")
		writeError(w, http.StatusInternalServerError, "failed to list tokens")
		return
	}

	writeJSON(w, http.StatusOK, tokens)
}

// POST /api/tokens
func (h *Handler) CreateAPIToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name      string   `json:"name"`
		IsAdmin   bool     `json:"is_admin"`
		Scopes    []string `json:"scopes,omitempty"`
		ExpiresIn int      `json:"expires_in,omitempty"` // days
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	// Generate token
	token, hash, err := auth.GenerateAPIToken()
	if err != nil {
		log.Error().Err(err).Msg("failed to generate API token")
		writeError(w, http.StatusInternalServerError, "failed to generate token")
		return
	}

	apiToken := &db.APIToken{
		Name:      req.Name,
		TokenHash: hash,
		IsAdmin:   req.IsAdmin,
		Scopes:    req.Scopes,
	}

	// Get current user if authenticated
	if user := auth.GetUser(r.Context()); user != nil {
		apiToken.UserID = user.ID
	}

	// Set expiration if specified
	if req.ExpiresIn > 0 {
		exp := time.Now().AddDate(0, 0, req.ExpiresIn)
		apiToken.ExpiresAt = &exp
	}

	id, err := h.db.CreateAPIToken(apiToken)
	if err != nil {
		log.Error().Err(err).Msg("failed to create API token")
		writeError(w, http.StatusInternalServerError, "failed to create token")
		return
	}

	log.Info().Int64("token_id", id).Str("name", req.Name).Msg("API token created")

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"id":    id,
		"token": token, // Only returned once!
		"name":  req.Name,
	})
}

// DELETE /api/tokens/{id}
func (h *Handler) DeleteAPIToken(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid token ID")
		return
	}

	if err := h.db.DeleteAPIToken(id); err != nil {
		log.Error().Err(err).Int64("token_id", id).Msg("failed to delete API token")
		writeError(w, http.StatusInternalServerError, "failed to delete token")
		return
	}

	log.Info().Int64("token_id", id).Msg("API token deleted")
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// GET /api/users
func (h *Handler) ListUsers(w http.ResponseWriter, r *http.Request) {
	users, err := h.db.ListUsers()
	if err != nil {
		log.Error().Err(err).Msg("failed to list users")
		writeError(w, http.StatusInternalServerError, "failed to list users")
		return
	}
	writeJSON(w, http.StatusOK, users)
}

// ============================================
// Admin worker management endpoints
// ============================================

// POST /api/admin/workers
// Creates a new worker with a generated secret
func (h *Handler) AdminCreateWorker(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ID == "" || req.Type == "" {
		writeError(w, http.StatusBadRequest, "id and type are required")
		return
	}

	// Check if worker already exists
	existing, err := h.db.GetWorker(req.ID)
	if err != nil {
		log.Error().Err(err).Str("worker_id", req.ID).Msg("failed to check existing worker")
		writeError(w, http.StatusInternalServerError, "failed to check existing worker")
		return
	}
	if existing != nil {
		writeError(w, http.StatusConflict, "worker already exists")
		return
	}

	// Generate worker secret
	secret, hash, err := auth.GenerateWorkerSecret()
	if err != nil {
		log.Error().Err(err).Msg("failed to generate worker secret")
		writeError(w, http.StatusInternalServerError, "failed to generate secret")
		return
	}

	var name *string
	if req.Name != "" {
		name = &req.Name
	}

	worker := &db.Worker{
		ID:         req.ID,
		Name:       name,
		Type:       req.Type,
		SecretHash: &hash,
		Enabled:    true,
	}
	if err := h.db.CreateWorker(worker); err != nil {
		log.Error().Err(err).Str("worker_id", req.ID).Msg("failed to create worker")
		writeError(w, http.StatusInternalServerError, "failed to create worker")
		return
	}

	log.Info().Str("worker_id", req.ID).Str("type", req.Type).Msg("admin created worker")

	// Return the secret only once
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"id":     req.ID,
		"name":   req.Name,
		"type":   req.Type,
		"secret": secret, // One-time display
	})
}

// POST /api/admin/workers/{id}/regenerate
// Regenerates a worker's secret
func (h *Handler) AdminRegenerateWorkerSecret(w http.ResponseWriter, r *http.Request) {
	workerID := chi.URLParam(r, "id")

	// Check worker exists
	existing, err := h.db.GetWorker(workerID)
	if err != nil {
		log.Error().Err(err).Str("worker_id", workerID).Msg("failed to check worker")
		writeError(w, http.StatusInternalServerError, "failed to check worker")
		return
	}
	if existing == nil {
		writeError(w, http.StatusNotFound, "worker not found")
		return
	}

	// Generate new secret
	secret, hash, err := auth.GenerateWorkerSecret()
	if err != nil {
		log.Error().Err(err).Msg("failed to generate worker secret")
		writeError(w, http.StatusInternalServerError, "failed to generate secret")
		return
	}

	if err := h.db.UpdateWorkerSecret(workerID, hash); err != nil {
		log.Error().Err(err).Str("worker_id", workerID).Msg("failed to update worker secret")
		writeError(w, http.StatusInternalServerError, "failed to update secret")
		return
	}

	log.Info().Str("worker_id", workerID).Msg("admin regenerated worker secret")

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":     workerID,
		"secret": secret, // One-time display
	})
}

// POST /api/admin/workers/{id}/enable
// Enables a worker
func (h *Handler) AdminEnableWorker(w http.ResponseWriter, r *http.Request) {
	workerID := chi.URLParam(r, "id")

	if err := h.db.SetWorkerEnabled(workerID, true); err != nil {
		if err.Error() == "sql: no rows in result set" {
			writeError(w, http.StatusNotFound, "worker not found")
			return
		}
		log.Error().Err(err).Str("worker_id", workerID).Msg("failed to enable worker")
		writeError(w, http.StatusInternalServerError, "failed to enable worker")
		return
	}

	log.Info().Str("worker_id", workerID).Msg("admin enabled worker")
	writeJSON(w, http.StatusOK, map[string]string{"status": "enabled"})
}

// POST /api/admin/workers/{id}/disable
// Disables a worker (revokes access)
func (h *Handler) AdminDisableWorker(w http.ResponseWriter, r *http.Request) {
	workerID := chi.URLParam(r, "id")

	if err := h.db.SetWorkerEnabled(workerID, false); err != nil {
		if err.Error() == "sql: no rows in result set" {
			writeError(w, http.StatusNotFound, "worker not found")
			return
		}
		log.Error().Err(err).Str("worker_id", workerID).Msg("failed to disable worker")
		writeError(w, http.StatusInternalServerError, "failed to disable worker")
		return
	}

	log.Info().Str("worker_id", workerID).Msg("admin disabled worker")
	writeJSON(w, http.StatusOK, map[string]string{"status": "disabled"})
}