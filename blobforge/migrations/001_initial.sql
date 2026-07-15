-- BlobForge Initial Schema
-- Creates all tables for the PostgreSQL backend.
-- S3 remains the blob store; this database handles metadata and queue state.

BEGIN;

-- Schema migrations tracking
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INT PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ DEFAULT NOW()
);

-- Files (replaces registry/manifest.json entries)
CREATE TABLE IF NOT EXISTS files (
    hash CHAR(64) PRIMARY KEY,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    source TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- File paths (one-to-many; indexed for path→hash lookups)
CREATE TABLE IF NOT EXISTS file_paths (
    id SERIAL PRIMARY KEY,
    file_hash CHAR(64) NOT NULL REFERENCES files(hash) ON DELETE CASCADE,
    path TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_file_paths_hash ON file_paths(file_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_paths_path ON file_paths(path);

-- File tags (normalized many-to-many)
CREATE TABLE IF NOT EXISTS file_tags (
    id SERIAL PRIMARY KEY,
    file_hash CHAR(64) NOT NULL REFERENCES files(hash) ON DELETE CASCADE,
    tag TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_file_tags_hash ON file_tags(file_hash);
CREATE INDEX IF NOT EXISTS idx_file_tags_tag ON file_tags(tag);

-- Jobs (replaces all queue/ prefixes)
CREATE TYPE job_status AS ENUM ('todo', 'processing', 'failed', 'dead', 'done');

CREATE TABLE IF NOT EXISTS jobs (
    file_hash CHAR(64) PRIMARY KEY REFERENCES files(hash) ON DELETE CASCADE,
    status job_status NOT NULL DEFAULT 'todo',
    priority TEXT NOT NULL DEFAULT '3_normal',
    worker_id TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    progress JSONB DEFAULT '{}',
    CONSTRAINT valid_priority CHECK (priority IN (
        '1_critical', '2_high', '3_normal', '4_low', '5_background'
    ))
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_priority ON jobs(status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_worker ON jobs(worker_id) WHERE status = 'processing';

-- Workers (replaces registry/workers/*.json)
CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    last_heartbeat TIMESTAMPTZ DEFAULT NOW(),
    metadata JSONB DEFAULT '{}',
    metrics JSONB DEFAULT '{}',
    system JSONB DEFAULT '{}',
    registered_at TIMESTAMPTZ DEFAULT NOW()
);

-- Job logs (replaces registry/logs/*/)
CREATE TABLE IF NOT EXISTS job_logs (
    id SERIAL PRIMARY KEY,
    file_hash CHAR(64) NOT NULL REFERENCES files(hash) ON DELETE CASCADE,
    log_type TEXT NOT NULL DEFAULT 'conversion',
    content TEXT,
    error_json JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_logs_hash ON job_logs(file_hash);

-- Remote config (replaces registry/config.json)
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Record this migration
INSERT INTO schema_migrations (version, name) VALUES (1, 'initial');

COMMIT;