package db

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func setupTestDB(t *testing.T) (*DB, func()) {
	t.Helper()

	// Create temp file
	f, err := os.CreateTemp("", "blobforge-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	db, err := New(f.Name())
	if err != nil {
		os.Remove(f.Name())
		t.Fatal(err)
	}

	if err := db.Migrate(); err != nil {
		db.Close()
		os.Remove(f.Name())
		t.Fatal(err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(f.Name())
	}

	return db, cleanup
}

func TestWorkerOperations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create worker
	worker := &Worker{
		ID:   "test-worker-1",
		Type: "pdf",
	}
	if err := db.CreateWorker(worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	// Get worker
	got, err := db.GetWorker("test-worker-1")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}
	if got == nil {
		t.Fatal("GetWorker returned nil")
	}
	if got.ID != "test-worker-1" {
		t.Errorf("got ID %q, want %q", got.ID, "test-worker-1")
	}
	if got.Type != "pdf" {
		t.Errorf("got Type %q, want %q", got.Type, "pdf")
	}
	if got.Status != WorkerStatusOnline {
		t.Errorf("got Status %q, want %q", got.Status, WorkerStatusOnline)
	}

	// List workers
	workers, err := db.ListWorkers()
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	if len(workers) != 1 {
		t.Errorf("got %d workers, want 1", len(workers))
	}

	// Update heartbeat
	jobID := int64(123)
	if err := db.UpdateWorkerHeartbeat("test-worker-1", &jobID, nil); err != nil {
		t.Fatalf("UpdateWorkerHeartbeat failed: %v", err)
	}

	got, _ = db.GetWorker("test-worker-1")
	if got.LastHeartbeat == nil {
		t.Error("LastHeartbeat should not be nil after heartbeat")
	}

	// Set offline
	if err := db.SetWorkerOffline("test-worker-1"); err != nil {
		t.Fatalf("SetWorkerOffline failed: %v", err)
	}

	got, _ = db.GetWorker("test-worker-1")
	if got.Status != WorkerStatusOffline {
		t.Errorf("got Status %q, want %q", got.Status, WorkerStatusOffline)
	}

	// Delete worker
	if err := db.DeleteWorker("test-worker-1"); err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}

	got, _ = db.GetWorker("test-worker-1")
	if got != nil {
		t.Error("Worker should be nil after deletion")
	}
}

func TestJobOperations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create job
	job := &Job{
		Type:        "pdf",
		SourceHash:  "abc123",
		SourcePath:  "/path/to/file.pdf",
		Priority:    2,
		MaxAttempts: 3,
	}
	jobID, err := db.CreateJob(job)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}
	if jobID == 0 {
		t.Error("CreateJob returned 0 job ID")
	}

	// Get job
	got, err := db.GetJob(jobID)
	if err != nil {
		t.Fatalf("GetJob failed: %v", err)
	}
	if got == nil {
		t.Fatal("GetJob returned nil")
	}
	if got.Type != "pdf" {
		t.Errorf("got Type %q, want %q", got.Type, "pdf")
	}
	if got.Status != JobStatusPending {
		t.Errorf("got Status %q, want %q", got.Status, JobStatusPending)
	}
	if got.Priority != 2 {
		t.Errorf("got Priority %d, want %d", got.Priority, 2)
	}

	// Get by hash
	gotByHash, err := db.GetJobByHash("pdf", "abc123")
	if err != nil {
		t.Fatalf("GetJobByHash failed: %v", err)
	}
	if gotByHash == nil {
		t.Fatal("GetJobByHash returned nil")
	}
	if gotByHash.ID != jobID {
		t.Errorf("got ID %d, want %d", gotByHash.ID, jobID)
	}

	// List jobs
	jobs, total, err := db.ListJobs("", "", 10, 0)
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Errorf("got %d jobs, want 1", len(jobs))
	}
	if total != 1 {
		t.Errorf("got total %d, want 1", total)
	}

	// List by status
	jobs, total, _ = db.ListJobs(JobStatusPending, "", 10, 0)
	if len(jobs) != 1 {
		t.Errorf("got %d pending jobs, want 1", len(jobs))
	}

	jobs, total, _ = db.ListJobs(JobStatusRunning, "", 10, 0)
	if len(jobs) != 0 {
		t.Errorf("got %d running jobs, want 0", len(jobs))
	}
}

func TestJobClaim(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create worker
	db.CreateWorker(&Worker{ID: "worker-1", Type: "pdf"})

	// Create job
	job := &Job{
		Type:        "pdf",
		SourceHash:  "hash1",
		MaxAttempts: 3,
	}
	jobID, _ := db.CreateJob(job)

	// Claim job
	claimed, err := db.ClaimJob("worker-1", "pdf")
	if err != nil {
		t.Fatalf("ClaimJob failed: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimJob returned nil")
	}
	if claimed.ID != jobID {
		t.Errorf("got ID %d, want %d", claimed.ID, jobID)
	}
	if claimed.Status != JobStatusRunning {
		t.Errorf("got Status %q, want %q", claimed.Status, JobStatusRunning)
	}
	if claimed.Attempts != 1 {
		t.Errorf("got Attempts %d, want 1", claimed.Attempts)
	}

	// Try to claim again (should return nil - no available jobs)
	claimed2, err := db.ClaimJob("worker-1", "pdf")
	if err != nil {
		t.Fatalf("Second ClaimJob failed: %v", err)
	}
	if claimed2 != nil {
		t.Error("Second ClaimJob should return nil")
	}
}

func TestJobComplete(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	db.CreateWorker(&Worker{ID: "worker-1", Type: "pdf"})
	job := &Job{Type: "pdf", SourceHash: "hash1", MaxAttempts: 3}
	jobID, _ := db.CreateJob(job)
	db.ClaimJob("worker-1", "pdf")

	// Complete job
	result := json.RawMessage(`{"pages": 10}`)
	if err := db.CompleteJob(jobID, "worker-1", result); err != nil {
		t.Fatalf("CompleteJob failed: %v", err)
	}

	got, _ := db.GetJob(jobID)
	if got.Status != JobStatusCompleted {
		t.Errorf("got Status %q, want %q", got.Status, JobStatusCompleted)
	}
	if got.CompletedAt == nil {
		t.Error("CompletedAt should not be nil")
	}
}

func TestJobFail(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	db.CreateWorker(&Worker{ID: "worker-1", Type: "pdf"})

	// Job with 2 max attempts
	job := &Job{Type: "pdf", SourceHash: "hash1", MaxAttempts: 2}
	jobID, _ := db.CreateJob(job)

	// First attempt - fail
	db.ClaimJob("worker-1", "pdf")
	willRetry, err := db.FailJob(jobID, "worker-1", "error 1")
	if err != nil {
		t.Fatalf("FailJob failed: %v", err)
	}
	if !willRetry {
		t.Error("First failure should allow retry")
	}

	got, _ := db.GetJob(jobID)
	if got.Status != JobStatusPending {
		t.Errorf("got Status %q, want %q (should be back to pending for retry)", got.Status, JobStatusPending)
	}

	// Second attempt - fail
	db.ClaimJob("worker-1", "pdf")
	willRetry, err = db.FailJob(jobID, "worker-1", "error 2")
	if err != nil {
		t.Fatalf("Second FailJob failed: %v", err)
	}
	if willRetry {
		t.Error("Second failure should NOT allow retry (max attempts reached)")
	}

	got, _ = db.GetJob(jobID)
	if got.Status != JobStatusDead {
		t.Errorf("got Status %q, want %q", got.Status, JobStatusDead)
	}
}

func TestJobRetry(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	db.CreateWorker(&Worker{ID: "worker-1", Type: "pdf"})
	job := &Job{Type: "pdf", SourceHash: "hash1", MaxAttempts: 2, Priority: 3}
	jobID, _ := db.CreateJob(job)

	// Fail to dead
	db.ClaimJob("worker-1", "pdf")
	db.FailJob(jobID, "worker-1", "error 1")
	db.ClaimJob("worker-1", "pdf")
	db.FailJob(jobID, "worker-1", "error 2")

	// Retry with reset and new priority
	newPriority := 1
	if err := db.RetryJob(jobID, true, &newPriority); err != nil {
		t.Fatalf("RetryJob failed: %v", err)
	}

	got, _ := db.GetJob(jobID)
	if got.Status != JobStatusPending {
		t.Errorf("got Status %q, want %q", got.Status, JobStatusPending)
	}
	if got.Attempts != 0 {
		t.Errorf("got Attempts %d, want 0 (reset)", got.Attempts)
	}
	if got.Priority != 1 {
		t.Errorf("got Priority %d, want 1", got.Priority)
	}
}

func TestJobCancel(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	job := &Job{Type: "pdf", SourceHash: "hash1", MaxAttempts: 3}
	jobID, _ := db.CreateJob(job)

	if err := db.CancelJob(jobID); err != nil {
		t.Fatalf("CancelJob failed: %v", err)
	}

	got, _ := db.GetJob(jobID)
	if got.Status != JobStatusCancelled {
		t.Errorf("got Status %q, want %q", got.Status, JobStatusCancelled)
	}
}

func TestStats(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create workers
	db.CreateWorker(&Worker{ID: "worker-1", Type: "pdf"})
	db.CreateWorker(&Worker{ID: "worker-2", Type: "pdf"})
	db.SetWorkerOffline("worker-2")

	// Create jobs in various states
	db.CreateJob(&Job{Type: "pdf", SourceHash: "hash1", MaxAttempts: 3})
	db.CreateJob(&Job{Type: "pdf", SourceHash: "hash2", MaxAttempts: 3})
	db.CreateJob(&Job{Type: "pdf", SourceHash: "hash3", MaxAttempts: 3})

	stats, err := db.GetStats()
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}

	if stats.TotalJobs != 3 {
		t.Errorf("got TotalJobs %d, want 3", stats.TotalJobs)
	}
	if stats.PendingJobs != 3 {
		t.Errorf("got PendingJobs %d, want 3", stats.PendingJobs)
	}
	if stats.TotalWorkers != 2 {
		t.Errorf("got TotalWorkers %d, want 2", stats.TotalWorkers)
	}
	if stats.OnlineWorkers != 1 {
		t.Errorf("got OnlineWorkers %d, want 1", stats.OnlineWorkers)
	}
	if stats.OfflineWorkers != 1 {
		t.Errorf("got OfflineWorkers %d, want 1", stats.OfflineWorkers)
	}
}

func TestStaleWorkers(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create worker with heartbeat (recently active)
	if err := db.CreateWorker(&Worker{ID: "worker-1", Type: "pdf"}); err != nil {
		t.Fatalf("CreateWorker worker-1 failed: %v", err)
	}
	if err := db.UpdateWorkerHeartbeat("worker-1", nil, nil); err != nil {
		t.Fatalf("UpdateWorkerHeartbeat failed: %v", err)
	}

	// Create worker without heartbeat (will be stale)
	if err := db.CreateWorker(&Worker{ID: "worker-2", Type: "pdf"}); err != nil {
		t.Fatalf("CreateWorker worker-2 failed: %v", err)
	}

	// Get stale workers - use 1 minute timeout to ensure only NULL heartbeat workers are caught
	// Worker-1 has a heartbeat of NOW, worker-2 has NULL heartbeat
	stale, err := db.GetStaleWorkers(1 * time.Minute)
	if err != nil {
		t.Fatalf("GetStaleWorkers failed: %v", err)
	}

	// Worker-2 has no heartbeat (NULL), so it should be stale
	if len(stale) != 1 {
		t.Errorf("got %d stale workers, want 1", len(stale))
	}
	if len(stale) > 0 && stale[0].ID != "worker-2" {
		t.Errorf("got stale worker %q, want worker-2", stale[0].ID)
	}
}

func TestUserOperations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Upsert user
	user := &User{
		ID:       "user-123",
		Email:    "test@example.com",
		Name:     "Test User",
		Groups:   []string{"users", "admins"},
		IsAdmin:  true,
		Provider: "oidc",
	}
	if err := db.UpsertUser(user); err != nil {
		t.Fatalf("UpsertUser failed: %v", err)
	}

	// Get user
	got, err := db.GetUser("user-123")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	if got == nil {
		t.Fatal("GetUser returned nil")
	}
	if got.Email != "test@example.com" {
		t.Errorf("got Email %q, want %q", got.Email, "test@example.com")
	}
	if !got.IsAdmin {
		t.Error("IsAdmin should be true")
	}
	if len(got.Groups) != 2 {
		t.Errorf("got %d groups, want 2", len(got.Groups))
	}

	// List users
	users, err := db.ListUsers()
	if err != nil {
		t.Fatalf("ListUsers failed: %v", err)
	}
	if len(users) != 1 {
		t.Errorf("got %d users, want 1", len(users))
	}
}

func TestSessionOperations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create user first
	db.UpsertUser(&User{ID: "user-1", Email: "test@example.com", Provider: "oidc"})

	// Create session
	expires := time.Now().Add(24 * time.Hour)
	if err := db.CreateSession("session-123", "user-1", expires); err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}

	// Get session
	session, err := db.GetSession("session-123")
	if err != nil {
		t.Fatalf("GetSession failed: %v", err)
	}
	if session == nil {
		t.Fatal("GetSession returned nil")
	}
	if session.UserID != "user-1" {
		t.Errorf("got UserID %q, want %q", session.UserID, "user-1")
	}

	// Delete session
	if err := db.DeleteSession("session-123"); err != nil {
		t.Fatalf("DeleteSession failed: %v", err)
	}

	session, _ = db.GetSession("session-123")
	if session != nil {
		t.Error("Session should be nil after deletion")
	}
}

func TestAPITokenOperations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create token
	token := &APIToken{
		Name:      "test-token",
		TokenHash: "hash123",
		IsAdmin:   true,
	}
	tokenID, err := db.CreateAPIToken(token)
	if err != nil {
		t.Fatalf("CreateAPIToken failed: %v", err)
	}
	if tokenID == 0 {
		t.Error("CreateAPIToken returned 0 token ID")
	}

	// Get by hash
	got, err := db.GetAPITokenByHash("hash123")
	if err != nil {
		t.Fatalf("GetAPITokenByHash failed: %v", err)
	}
	if got == nil {
		t.Fatal("GetAPITokenByHash returned nil")
	}
	if got.Name != "test-token" {
		t.Errorf("got Name %q, want %q", got.Name, "test-token")
	}
	if !got.IsAdmin {
		t.Error("IsAdmin should be true")
	}

	// Update last used
	if err := db.UpdateAPITokenLastUsed(tokenID); err != nil {
		t.Fatalf("UpdateAPITokenLastUsed failed: %v", err)
	}

	got, _ = db.GetAPITokenByHash("hash123")
	if got.LastUsedAt == nil {
		t.Error("LastUsedAt should not be nil after update")
	}

	// List tokens
	tokens, err := db.ListAPITokens("")
	if err != nil {
		t.Fatalf("ListAPITokens failed: %v", err)
	}
	if len(tokens) != 1 {
		t.Errorf("got %d tokens, want 1", len(tokens))
	}

	// Delete token
	if err := db.DeleteAPIToken(tokenID); err != nil {
		t.Fatalf("DeleteAPIToken failed: %v", err)
	}

	got, _ = db.GetAPITokenByHash("hash123")
	if got != nil {
		t.Error("Token should be nil after deletion")
	}
}
