package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	conn *sql.DB
}

// nullableJSON is a helper type for scanning nullable JSON columns
type nullableJSON []byte

func (n *nullableJSON) Scan(value interface{}) error {
	if value == nil {
		*n = nil
		return nil
	}
	switch v := value.(type) {
	case []byte:
		*n = v
		return nil
	case string:
		*n = []byte(v)
		return nil
	}
	return fmt.Errorf("cannot scan %T into nullableJSON", value)
}

// Job status constants
const (
	JobStatusPending   = "pending"
	JobStatusRunning   = "running"
	JobStatusCompleted = "completed"
	JobStatusFailed    = "failed"
	JobStatusDead      = "dead"
	JobStatusCancelled = "cancelled"
)

// Worker status constants
const (
	WorkerStatusOnline   = "online"
	WorkerStatusOffline  = "offline"
	WorkerStatusDraining = "draining"
)

type Worker struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Status        string          `json:"status"`
	LastHeartbeat *time.Time      `json:"last_heartbeat,omitempty"`
	CurrentJobID  *int64          `json:"current_job_id,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

type Job struct {
	ID          int64           `json:"id"`
	Type        string          `json:"type"`
	Status      string          `json:"status"`
	Priority    int             `json:"priority"`
	SourceHash  string          `json:"source_hash"`
	SourcePath  string          `json:"source_path,omitempty"`
	SourceSize  int64           `json:"source_size,omitempty"`
	WorkerID    *string         `json:"worker_id,omitempty"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	LastError   *string         `json:"last_error,omitempty"`
	Tags        json.RawMessage `json:"tags,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type File struct {
	ID          int64           `json:"id"`
	JobID       int64           `json:"job_id"`
	Type        string          `json:"type"` // "source" or "output"
	S3Key       string          `json:"s3_key"`
	Size        int64           `json:"size,omitempty"`
	ContentType string          `json:"content_type,omitempty"`
	Checksum    string          `json:"checksum,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

type Stats struct {
	TotalJobs      int `json:"total_jobs"`
	PendingJobs    int `json:"pending_jobs"`
	RunningJobs    int `json:"running_jobs"`
	CompletedJobs  int `json:"completed_jobs"`
	FailedJobs     int `json:"failed_jobs"`
	DeadJobs       int `json:"dead_jobs"`
	TotalWorkers   int `json:"total_workers"`
	OnlineWorkers  int `json:"online_workers"`
	OfflineWorkers int `json:"offline_workers"`
}

// Auth types

type User struct {
	ID        string    `json:"id"`
	Email     string    `json:"email"`
	Name      string    `json:"name"`
	Groups    []string  `json:"groups"`
	IsAdmin   bool      `json:"is_admin"`
	Provider  string    `json:"provider"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Session struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

type APIToken struct {
	ID         int64      `json:"id"`
	Name       string     `json:"name"`
	TokenHash  string     `json:"-"`
	UserID     string     `json:"user_id,omitempty"`
	IsAdmin    bool       `json:"is_admin"`
	Scopes     []string   `json:"scopes,omitempty"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
}

func New(dbPath string) (*DB, error) {
	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %w", err)
	}

	conn, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{conn: conn}, nil
}

func (db *DB) Close() error {
	return db.conn.Close()
}

// Migrate is now in migrations.go

// Worker operations

func (db *DB) CreateWorker(w *Worker) error {
	_, err := db.conn.Exec(`
		INSERT INTO workers (id, type, status, metadata, created_at, updated_at)
		VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			type = excluded.type,
			status = 'online',
			metadata = excluded.metadata,
			updated_at = CURRENT_TIMESTAMP
	`, w.ID, w.Type, WorkerStatusOnline, w.Metadata)
	return err
}

func (db *DB) UpdateWorkerHeartbeat(id string, jobID *int64, progress json.RawMessage) error {
	_, err := db.conn.Exec(`
		UPDATE workers 
		SET last_heartbeat = CURRENT_TIMESTAMP, 
			current_job_id = ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, jobID, id)
	return err
}

func (db *DB) SetWorkerOffline(id string) error {
	_, err := db.conn.Exec(`
		UPDATE workers 
		SET status = ?, current_job_id = NULL, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, WorkerStatusOffline, id)
	return err
}

func (db *DB) DeleteWorker(id string) error {
	_, err := db.conn.Exec(`DELETE FROM workers WHERE id = ?`, id)
	return err
}

func (db *DB) GetWorker(id string) (*Worker, error) {
	var w Worker
	var metadata nullableJSON
	err := db.conn.QueryRow(`
		SELECT id, type, status, last_heartbeat, current_job_id, metadata, created_at, updated_at
		FROM workers WHERE id = ?
	`, id).Scan(&w.ID, &w.Type, &w.Status, &w.LastHeartbeat, &w.CurrentJobID, &metadata, &w.CreatedAt, &w.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	w.Metadata = json.RawMessage(metadata)
	return &w, err
}

func (db *DB) ListWorkers() ([]Worker, error) {
	rows, err := db.conn.Query(`
		SELECT id, type, status, last_heartbeat, current_job_id, metadata, created_at, updated_at
		FROM workers ORDER BY status, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workers []Worker
	for rows.Next() {
		var w Worker
		var metadata nullableJSON
		if err := rows.Scan(&w.ID, &w.Type, &w.Status, &w.LastHeartbeat, &w.CurrentJobID, &metadata, &w.CreatedAt, &w.UpdatedAt); err != nil {
			return nil, err
		}
		w.Metadata = json.RawMessage(metadata)
		workers = append(workers, w)
	}
	return workers, nil
}

func (db *DB) GetStaleWorkers(timeout time.Duration) ([]Worker, error) {
	cutoff := time.Now().UTC().Add(-timeout)
	rows, err := db.conn.Query(`
		SELECT id, type, status, last_heartbeat, current_job_id, metadata, created_at, updated_at
		FROM workers 
		WHERE status = 'online' AND (last_heartbeat IS NULL OR last_heartbeat < ?)
	`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workers []Worker
	for rows.Next() {
		var w Worker
		var metadata nullableJSON
		if err := rows.Scan(&w.ID, &w.Type, &w.Status, &w.LastHeartbeat, &w.CurrentJobID, &metadata, &w.CreatedAt, &w.UpdatedAt); err != nil {
			return nil, err
		}
		w.Metadata = json.RawMessage(metadata)
		workers = append(workers, w)
	}
	return workers, nil
}

// Job operations

func (db *DB) CreateJob(j *Job) (int64, error) {
	result, err := db.conn.Exec(`
		INSERT INTO jobs (type, status, priority, source_hash, source_path, source_size, max_attempts, tags, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, j.Type, JobStatusPending, j.Priority, j.SourceHash, j.SourcePath, j.SourceSize, j.MaxAttempts, j.Tags, j.Metadata)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (db *DB) GetJob(id int64) (*Job, error) {
	var j Job
	var tags, metadata, result nullableJSON
	err := db.conn.QueryRow(`
		SELECT id, type, status, priority, source_hash, source_path, source_size, worker_id, 
			   started_at, completed_at, attempts, max_attempts, last_error, tags, metadata, result, 
			   created_at, updated_at
		FROM jobs WHERE id = ?
	`, id).Scan(&j.ID, &j.Type, &j.Status, &j.Priority, &j.SourceHash, &j.SourcePath, &j.SourceSize,
		&j.WorkerID, &j.StartedAt, &j.CompletedAt, &j.Attempts, &j.MaxAttempts, &j.LastError,
		&tags, &metadata, &result, &j.CreatedAt, &j.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	j.Tags = json.RawMessage(tags)
	j.Metadata = json.RawMessage(metadata)
	j.Result = json.RawMessage(result)
	return &j, err
}

func (db *DB) GetJobByHash(jobType, hash string) (*Job, error) {
	var j Job
	var tags, metadata, result nullableJSON
	err := db.conn.QueryRow(`
		SELECT id, type, status, priority, source_hash, source_path, source_size, worker_id, 
			   started_at, completed_at, attempts, max_attempts, last_error, tags, metadata, result, 
			   created_at, updated_at
		FROM jobs WHERE type = ? AND source_hash = ?
	`, jobType, hash).Scan(&j.ID, &j.Type, &j.Status, &j.Priority, &j.SourceHash, &j.SourcePath, &j.SourceSize,
		&j.WorkerID, &j.StartedAt, &j.CompletedAt, &j.Attempts, &j.MaxAttempts, &j.LastError,
		&tags, &metadata, &result, &j.CreatedAt, &j.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	j.Tags = json.RawMessage(tags)
	j.Metadata = json.RawMessage(metadata)
	j.Result = json.RawMessage(result)
	return &j, err
}

func (db *DB) ListJobs(status string, jobType string, limit, offset int) ([]Job, int, error) {
	// Build query
	query := `SELECT id, type, status, priority, source_hash, source_path, source_size, worker_id, 
			  started_at, completed_at, attempts, max_attempts, last_error, tags, metadata, result, 
			  created_at, updated_at FROM jobs WHERE 1=1`
	countQuery := `SELECT COUNT(*) FROM jobs WHERE 1=1`
	args := []interface{}{}

	if status != "" {
		query += ` AND status = ?`
		countQuery += ` AND status = ?`
		args = append(args, status)
	}
	if jobType != "" {
		query += ` AND type = ?`
		countQuery += ` AND type = ?`
		args = append(args, jobType)
	}

	// Get total count
	var total int
	if err := db.conn.QueryRow(countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	// Get jobs
	query += ` ORDER BY priority ASC, created_at DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		var tags, metadata, result nullableJSON
		if err := rows.Scan(&j.ID, &j.Type, &j.Status, &j.Priority, &j.SourceHash, &j.SourcePath,
			&j.SourceSize, &j.WorkerID, &j.StartedAt, &j.CompletedAt, &j.Attempts, &j.MaxAttempts,
			&j.LastError, &tags, &metadata, &result, &j.CreatedAt, &j.UpdatedAt); err != nil {
			return nil, 0, err
		}
		j.Tags = json.RawMessage(tags)
		j.Metadata = json.RawMessage(metadata)
		j.Result = json.RawMessage(result)
		jobs = append(jobs, j)
	}
	return jobs, total, nil
}

func (db *DB) ClaimJob(workerID string, jobType string) (*Job, error) {
	tx, err := db.conn.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Find next available job
	var j Job
	var tags, metadata nullableJSON
	err = tx.QueryRow(`
		SELECT id, type, status, priority, source_hash, source_path, source_size, 
			   attempts, max_attempts, tags, metadata, created_at, updated_at
		FROM jobs 
		WHERE status = ? AND type = ?
		ORDER BY priority ASC, created_at ASC
		LIMIT 1
	`, JobStatusPending, jobType).Scan(&j.ID, &j.Type, &j.Status, &j.Priority, &j.SourceHash,
		&j.SourcePath, &j.SourceSize, &j.Attempts, &j.MaxAttempts, &tags, &metadata,
		&j.CreatedAt, &j.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	j.Tags = json.RawMessage(tags)
	j.Metadata = json.RawMessage(metadata)

	// Claim the job
	_, err = tx.Exec(`
		UPDATE jobs 
		SET status = ?, worker_id = ?, started_at = CURRENT_TIMESTAMP, 
			attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, JobStatusRunning, workerID, j.ID)
	if err != nil {
		return nil, err
	}

	// Update worker's current job
	_, err = tx.Exec(`
		UPDATE workers SET current_job_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, j.ID, workerID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	j.Status = JobStatusRunning
	j.WorkerID = &workerID
	j.Attempts++
	return &j, nil
}

func (db *DB) CompleteJob(id int64, workerID string, result json.RawMessage) error {
	_, err := db.conn.Exec(`
		UPDATE jobs 
		SET status = ?, completed_at = CURRENT_TIMESTAMP, result = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ? AND worker_id = ?
	`, JobStatusCompleted, result, id, workerID)
	if err != nil {
		return err
	}

	// Clear worker's current job
	_, err = db.conn.Exec(`
		UPDATE workers SET current_job_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, workerID)
	return err
}

func (db *DB) FailJob(id int64, workerID string, errorMsg string) (bool, error) {
	// Get current job state
	var attempts, maxAttempts int
	err := db.conn.QueryRow(`SELECT attempts, max_attempts FROM jobs WHERE id = ?`, id).Scan(&attempts, &maxAttempts)
	if err != nil {
		return false, err
	}

	// Determine new status
	newStatus := JobStatusFailed
	willRetry := attempts < maxAttempts
	if !willRetry {
		newStatus = JobStatusDead
	} else {
		newStatus = JobStatusPending // Will be retried
	}

	_, err = db.conn.Exec(`
		UPDATE jobs 
		SET status = ?, last_error = ?, worker_id = NULL, updated_at = CURRENT_TIMESTAMP
		WHERE id = ? AND worker_id = ?
	`, newStatus, errorMsg, id, workerID)
	if err != nil {
		return false, err
	}

	// Clear worker's current job
	_, err = db.conn.Exec(`
		UPDATE workers SET current_job_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, workerID)
	return willRetry, err
}

func (db *DB) RetryJob(id int64, resetAttempts bool, priority *int) error {
	query := `UPDATE jobs SET status = ?, worker_id = NULL, updated_at = CURRENT_TIMESTAMP`
	args := []interface{}{JobStatusPending}

	if resetAttempts {
		query += `, attempts = 0`
	}
	if priority != nil {
		query += `, priority = ?`
		args = append(args, *priority)
	}
	query += ` WHERE id = ?`
	args = append(args, id)

	_, err := db.conn.Exec(query, args...)
	return err
}

func (db *DB) CancelJob(id int64) error {
	_, err := db.conn.Exec(`
		UPDATE jobs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, JobStatusCancelled, id)
	return err
}

func (db *DB) ReleaseJobsFromWorker(workerID string) error {
	_, err := db.conn.Exec(`
		UPDATE jobs 
		SET status = ?, worker_id = NULL, updated_at = CURRENT_TIMESTAMP
		WHERE worker_id = ? AND status = ?
	`, JobStatusPending, workerID, JobStatusRunning)
	return err
}

// File operations

func (db *DB) CreateFile(f *File) (int64, error) {
	result, err := db.conn.Exec(`
		INSERT INTO files (job_id, type, s3_key, size, content_type, checksum, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, f.JobID, f.Type, f.S3Key, f.Size, f.ContentType, f.Checksum, f.Metadata)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (db *DB) GetFilesByJob(jobID int64) ([]File, error) {
	rows, err := db.conn.Query(`
		SELECT id, job_id, type, s3_key, size, content_type, checksum, metadata, created_at
		FROM files WHERE job_id = ?
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []File
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.ID, &f.JobID, &f.Type, &f.S3Key, &f.Size, &f.ContentType, &f.Checksum, &f.Metadata, &f.CreatedAt); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, nil
}

func (db *DB) GetFile(jobID int64, fileType string) (*File, error) {
	var f File
	err := db.conn.QueryRow(`
		SELECT id, job_id, type, s3_key, size, content_type, checksum, metadata, created_at
		FROM files WHERE job_id = ? AND type = ?
	`, jobID, fileType).Scan(&f.ID, &f.JobID, &f.Type, &f.S3Key, &f.Size, &f.ContentType, &f.Checksum, &f.Metadata, &f.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &f, err
}

// Stats

func (db *DB) GetStats() (*Stats, error) {
	var s Stats

	// Job stats
	rows, err := db.conn.Query(`SELECT status, COUNT(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		s.TotalJobs += count
		switch status {
		case JobStatusPending:
			s.PendingJobs = count
		case JobStatusRunning:
			s.RunningJobs = count
		case JobStatusCompleted:
			s.CompletedJobs = count
		case JobStatusFailed:
			s.FailedJobs = count
		case JobStatusDead:
			s.DeadJobs = count
		}
	}

	// Worker stats
	rows, err = db.conn.Query(`SELECT status, COUNT(*) FROM workers GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		s.TotalWorkers += count
		switch status {
		case WorkerStatusOnline:
			s.OnlineWorkers = count
		case WorkerStatusOffline:
			s.OfflineWorkers = count
		}
	}

	return &s, nil
}

// Recent activity
func (db *DB) GetRecentJobs(limit int) ([]Job, error) {
	rows, err := db.conn.Query(`
		SELECT id, type, status, priority, source_hash, source_path, source_size, worker_id, 
			   started_at, completed_at, attempts, max_attempts, last_error, tags, metadata, result, 
			   created_at, updated_at
		FROM jobs ORDER BY updated_at DESC LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		var tags, metadata, result nullableJSON
		if err := rows.Scan(&j.ID, &j.Type, &j.Status, &j.Priority, &j.SourceHash, &j.SourcePath,
			&j.SourceSize, &j.WorkerID, &j.StartedAt, &j.CompletedAt, &j.Attempts, &j.MaxAttempts,
			&j.LastError, &tags, &metadata, &result, &j.CreatedAt, &j.UpdatedAt); err != nil {
			return nil, err
		}
		j.Tags = json.RawMessage(tags)
		j.Metadata = json.RawMessage(metadata)
		j.Result = json.RawMessage(result)
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// ============================================
// User operations
// ============================================

func (db *DB) UpsertUser(u *User) error {
	groupsJSON, _ := json.Marshal(u.Groups)
	_, err := db.conn.Exec(`
		INSERT INTO users (id, email, name, groups, is_admin, provider, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			email = excluded.email,
			name = excluded.name,
			groups = excluded.groups,
			is_admin = excluded.is_admin,
			updated_at = CURRENT_TIMESTAMP
	`, u.ID, u.Email, u.Name, groupsJSON, u.IsAdmin, u.Provider)
	return err
}

func (db *DB) GetUser(id string) (*User, error) {
	var u User
	var groupsJSON []byte
	err := db.conn.QueryRow(`
		SELECT id, email, name, groups, is_admin, provider, created_at, updated_at
		FROM users WHERE id = ?
	`, id).Scan(&u.ID, &u.Email, &u.Name, &groupsJSON, &u.IsAdmin, &u.Provider, &u.CreatedAt, &u.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if groupsJSON != nil {
		_ = json.Unmarshal(groupsJSON, &u.Groups)
	}
	return &u, nil
}

func (db *DB) ListUsers() ([]User, error) {
	rows, err := db.conn.Query(`
		SELECT id, email, name, groups, is_admin, provider, created_at, updated_at
		FROM users ORDER BY email
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		var groupsJSON []byte
		if err := rows.Scan(&u.ID, &u.Email, &u.Name, &groupsJSON, &u.IsAdmin, &u.Provider, &u.CreatedAt, &u.UpdatedAt); err != nil {
			return nil, err
		}
		if groupsJSON != nil {
			_ = json.Unmarshal(groupsJSON, &u.Groups)
		}
		users = append(users, u)
	}
	return users, nil
}

// ============================================
// Session operations
// ============================================

func (db *DB) CreateSession(id, userID string, expiresAt time.Time) error {
	_, err := db.conn.Exec(`
		INSERT INTO sessions (id, user_id, expires_at)
		VALUES (?, ?, ?)
	`, id, userID, expiresAt)
	return err
}

func (db *DB) GetSession(id string) (*Session, error) {
	var s Session
	err := db.conn.QueryRow(`
		SELECT id, user_id, expires_at, created_at
		FROM sessions WHERE id = ?
	`, id).Scan(&s.ID, &s.UserID, &s.ExpiresAt, &s.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &s, err
}

func (db *DB) DeleteSession(id string) error {
	_, err := db.conn.Exec(`DELETE FROM sessions WHERE id = ?`, id)
	return err
}

func (db *DB) DeleteExpiredSessions() (int64, error) {
	result, err := db.conn.Exec(`DELETE FROM sessions WHERE expires_at < CURRENT_TIMESTAMP`)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// ============================================
// API Token operations
// ============================================

func (db *DB) CreateAPIToken(t *APIToken) (int64, error) {
	scopesJSON, _ := json.Marshal(t.Scopes)
	result, err := db.conn.Exec(`
		INSERT INTO api_tokens (name, token_hash, user_id, is_admin, scopes, expires_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, t.Name, t.TokenHash, t.UserID, t.IsAdmin, scopesJSON, t.ExpiresAt)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (db *DB) GetAPITokenByHash(hash string) (*APIToken, error) {
	var t APIToken
	var scopesJSON []byte
	err := db.conn.QueryRow(`
		SELECT id, name, token_hash, user_id, is_admin, scopes, expires_at, last_used_at, created_at
		FROM api_tokens WHERE token_hash = ?
	`, hash).Scan(&t.ID, &t.Name, &t.TokenHash, &t.UserID, &t.IsAdmin, &scopesJSON, &t.ExpiresAt, &t.LastUsedAt, &t.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if scopesJSON != nil {
		_ = json.Unmarshal(scopesJSON, &t.Scopes)
	}
	return &t, nil
}

func (db *DB) GetAPIToken(id int64) (*APIToken, error) {
	var t APIToken
	var scopesJSON []byte
	err := db.conn.QueryRow(`
		SELECT id, name, token_hash, user_id, is_admin, scopes, expires_at, last_used_at, created_at
		FROM api_tokens WHERE id = ?
	`, id).Scan(&t.ID, &t.Name, &t.TokenHash, &t.UserID, &t.IsAdmin, &scopesJSON, &t.ExpiresAt, &t.LastUsedAt, &t.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if scopesJSON != nil {
		_ = json.Unmarshal(scopesJSON, &t.Scopes)
	}
	return &t, nil
}

func (db *DB) ListAPITokens(userID string) ([]APIToken, error) {
	query := `SELECT id, name, token_hash, user_id, is_admin, scopes, expires_at, last_used_at, created_at FROM api_tokens`
	args := []interface{}{}
	if userID != "" {
		query += ` WHERE user_id = ?`
		args = append(args, userID)
	}
	query += ` ORDER BY created_at DESC`

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tokens []APIToken
	for rows.Next() {
		var t APIToken
		var scopesJSON []byte
		if err := rows.Scan(&t.ID, &t.Name, &t.TokenHash, &t.UserID, &t.IsAdmin, &scopesJSON, &t.ExpiresAt, &t.LastUsedAt, &t.CreatedAt); err != nil {
			return nil, err
		}
		if scopesJSON != nil {
			_ = json.Unmarshal(scopesJSON, &t.Scopes)
		}
		tokens = append(tokens, t)
	}
	return tokens, nil
}

func (db *DB) UpdateAPITokenLastUsed(id int64) error {
	_, err := db.conn.Exec(`UPDATE api_tokens SET last_used_at = CURRENT_TIMESTAMP WHERE id = ?`, id)
	return err
}

func (db *DB) DeleteAPIToken(id int64) error {
	_, err := db.conn.Exec(`DELETE FROM api_tokens WHERE id = ?`, id)
	return err
}

// SetWorkerDraining marks a worker as draining (won't accept new jobs)
func (db *DB) SetWorkerDraining(id string) error {
	_, err := db.conn.Exec(`
		UPDATE workers 
		SET status = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, WorkerStatusDraining, id)
	return err
}

// UpdateJobPriority changes the priority of a job
func (db *DB) UpdateJobPriority(id int64, priority int) error {
	_, err := db.conn.Exec(`
		UPDATE jobs 
		SET priority = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, priority, id)
	return err
}
