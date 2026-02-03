package db

import (
	"fmt"

	"github.com/rs/zerolog/log"
)

// Migration represents a database migration
type Migration struct {
	Version     int
	Description string
	Up          string
}

// Migrations is the list of all migrations in order
var Migrations = []Migration{
	{
		Version:     1,
		Description: "Initial schema",
		Up: `
		CREATE TABLE IF NOT EXISTS workers (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'online',
			last_heartbeat DATETIME,
			current_job_id INTEGER,
			metadata JSON,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			type TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			priority INTEGER DEFAULT 3,
			source_hash TEXT NOT NULL,
			source_path TEXT,
			source_size INTEGER,
			worker_id TEXT,
			started_at DATETIME,
			completed_at DATETIME,
			attempts INTEGER DEFAULT 0,
			max_attempts INTEGER DEFAULT 3,
			last_error TEXT,
			tags JSON,
			metadata JSON,
			result JSON,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(type, source_hash)
		);

		CREATE INDEX IF NOT EXISTS idx_jobs_status_priority ON jobs(status, priority);
		CREATE INDEX IF NOT EXISTS idx_jobs_source_hash ON jobs(source_hash);
		CREATE INDEX IF NOT EXISTS idx_jobs_worker ON jobs(worker_id);

		CREATE TABLE IF NOT EXISTS files (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id INTEGER NOT NULL,
			type TEXT NOT NULL,
			s3_key TEXT NOT NULL,
			size INTEGER,
			content_type TEXT,
			checksum TEXT,
			metadata JSON,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (job_id) REFERENCES jobs(id)
		);

		CREATE INDEX IF NOT EXISTS idx_files_job ON files(job_id);
		CREATE INDEX IF NOT EXISTS idx_files_s3_key ON files(s3_key);
		`,
	},
	{
		Version:     2,
		Description: "Add authentication tables",
		Up: `
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			email TEXT UNIQUE,
			name TEXT,
			groups JSON,
			is_admin BOOLEAN DEFAULT FALSE,
			provider TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS sessions (
			id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL,
			expires_at DATETIME NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (user_id) REFERENCES users(id)
		);

		CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
		CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);

		CREATE TABLE IF NOT EXISTS api_tokens (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			token_hash TEXT UNIQUE NOT NULL,
			user_id TEXT,
			is_admin BOOLEAN DEFAULT FALSE,
			scopes JSON,
			expires_at DATETIME,
			last_used_at DATETIME,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (user_id) REFERENCES users(id)
		);

		CREATE INDEX IF NOT EXISTS idx_api_tokens_hash ON api_tokens(token_hash);
		CREATE INDEX IF NOT EXISTS idx_api_tokens_user ON api_tokens(user_id);
		`,
	},
	{
		Version:     3,
		Description: "Add job output_path column",
		Up: `
		ALTER TABLE jobs ADD COLUMN output_path TEXT;
		`,
	},
}

// GetSchemaVersion returns the current schema version using user_version pragma
func (db *DB) GetSchemaVersion() (int, error) {
	var version int
	err := db.conn.QueryRow("PRAGMA user_version").Scan(&version)
	return version, err
}

// SetSchemaVersion sets the schema version using user_version pragma
func (db *DB) SetSchemaVersion(version int) error {
	_, err := db.conn.Exec(fmt.Sprintf("PRAGMA user_version = %d", version))
	return err
}

// Migrate runs all pending migrations
func (db *DB) Migrate() error {
	currentVersion, err := db.GetSchemaVersion()
	if err != nil {
		return fmt.Errorf("failed to get schema version: %w", err)
	}

	log.Info().Int("current_version", currentVersion).Int("latest_version", len(Migrations)).Msg("checking migrations")

	for _, m := range Migrations {
		if m.Version <= currentVersion {
			continue
		}

		log.Info().Int("version", m.Version).Str("description", m.Description).Msg("applying migration")

		// Run migration in a transaction
		tx, err := db.conn.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		_, err = tx.Exec(m.Up)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d failed: %w", m.Version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit migration %d: %w", m.Version, err)
		}

		// Update version after successful migration
		if err := db.SetSchemaVersion(m.Version); err != nil {
			return fmt.Errorf("failed to update schema version: %w", err)
		}

		log.Info().Int("version", m.Version).Msg("migration applied successfully")
	}

	return nil
}

// MigrateToVersion migrates to a specific version (for testing)
func (db *DB) MigrateToVersion(targetVersion int) error {
	currentVersion, err := db.GetSchemaVersion()
	if err != nil {
		return fmt.Errorf("failed to get schema version: %w", err)
	}

	if targetVersion < currentVersion {
		return fmt.Errorf("downgrade migrations not supported (current: %d, target: %d)", currentVersion, targetVersion)
	}

	for _, m := range Migrations {
		if m.Version <= currentVersion || m.Version > targetVersion {
			continue
		}

		tx, err := db.conn.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		_, err = tx.Exec(m.Up)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d failed: %w", m.Version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit migration %d: %w", m.Version, err)
		}

		if err := db.SetSchemaVersion(m.Version); err != nil {
			return fmt.Errorf("failed to update schema version: %w", err)
		}
	}

	return nil
}
