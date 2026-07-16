import type { Client, InStatement, InValue, Row } from "@libsql/client";

export const PRIORITIES = ["1_critical", "2_high", "3_normal", "4_low", "5_background"] as const;
export type Priority = typeof PRIORITIES[number];
export type JobStatus = "todo" | "processing" | "failed" | "dead" | "done";

export interface JobRecord {
  file_hash: string;
  status: JobStatus;
  priority: Priority;
  retry_count: number;
  max_retries: number;
  worker_id: string | null;
  lease_token: string | null;
  lease_expires_at: number | null;
  created_at: number;
  updated_at: number;
  started_at: number | null;
  completed_at: number | null;
  available_at: number;
  error_message: string | null;
  progress_json: string;
  original_name: string | null;
  size_bytes: number;
}

const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS files (hash TEXT PRIMARY KEY CHECK(length(hash)=64), original_name TEXT, size_bytes INTEGER NOT NULL DEFAULT 0, source TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS file_paths (file_hash TEXT NOT NULL REFERENCES files(hash) ON DELETE CASCADE, path TEXT NOT NULL, PRIMARY KEY(file_hash,path))`,
  `CREATE TABLE IF NOT EXISTS file_tags (file_hash TEXT NOT NULL REFERENCES files(hash) ON DELETE CASCADE, tag TEXT NOT NULL, PRIMARY KEY(file_hash,tag))`,
  `CREATE TABLE IF NOT EXISTS jobs (file_hash TEXT PRIMARY KEY REFERENCES files(hash) ON DELETE CASCADE, status TEXT NOT NULL CHECK(status IN ('todo','processing','failed','dead','done')), priority TEXT NOT NULL CHECK(priority IN ('1_critical','2_high','3_normal','4_low','5_background')), retry_count INTEGER NOT NULL DEFAULT 0, max_retries INTEGER NOT NULL DEFAULT 3, worker_id TEXT, lease_token TEXT, lease_expires_at INTEGER, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, started_at INTEGER, completed_at INTEGER, available_at INTEGER NOT NULL, error_message TEXT, progress_json TEXT NOT NULL DEFAULT '{}')`,
  `CREATE INDEX IF NOT EXISTS jobs_claim_idx ON jobs(status,available_at,priority,created_at)`,
  `CREATE INDEX IF NOT EXISTS jobs_lease_idx ON jobs(status,lease_expires_at)`,
  `CREATE TABLE IF NOT EXISTS job_failures (id INTEGER PRIMARY KEY AUTOINCREMENT, file_hash TEXT NOT NULL REFERENCES files(hash) ON DELETE CASCADE, attempt INTEGER NOT NULL, worker_id TEXT, failed_at INTEGER NOT NULL, error_message TEXT NOT NULL, traceback TEXT, context_json TEXT NOT NULL DEFAULT '{}', progress_json TEXT NOT NULL DEFAULT '{}')`,
  `CREATE INDEX IF NOT EXISTS job_failures_job_idx ON job_failures(file_hash,failed_at DESC,id DESC)`,
  `CREATE TABLE IF NOT EXISTS workers (worker_id TEXT PRIMARY KEY, hostname TEXT NOT NULL, status TEXT NOT NULL, current_job TEXT, last_heartbeat INTEGER NOT NULL, registered_at INTEGER NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}', metrics_json TEXT NOT NULL DEFAULT '{}')`,
  `CREATE TABLE IF NOT EXISTS worker_credentials (worker_id TEXT PRIMARY KEY, label TEXT NOT NULL, token_hash TEXT NOT NULL UNIQUE, created_at INTEGER NOT NULL, created_by TEXT NOT NULL, revoked_at INTEGER, last_used_at INTEGER)`,
  `CREATE INDEX IF NOT EXISTS worker_credentials_token_idx ON worker_credentials(token_hash)`,
  `CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, actor TEXT NOT NULL, action TEXT NOT NULL, subject TEXT, detail_json TEXT NOT NULL DEFAULT '{}', created_at INTEGER NOT NULL)`,
];

const DEFAULT_CONFIG: Record<string, unknown> = {
  max_retries: 3,
  heartbeat_interval: 60,
  lease_seconds: 900,
  conversion_timeout: 3600,
};

function statement(sql: string, args: InValue[] = []): InStatement {
  return { sql, args };
}

function numeric(value: unknown): number {
  return typeof value === "bigint" ? Number(value) : Number(value ?? 0);
}

function nullableNumber(value: unknown): number | null {
  return value === null || value === undefined ? null : numeric(value);
}

function serializableRow(row: Row): Record<string, unknown> {
  return Object.fromEntries(Object.entries(row).map(([key, value]) => [
    key,
    typeof value === "bigint" ? value.toString() : value,
  ]));
}

function jobFromRow(row: Row): JobRecord {
  return {
    file_hash: String(row.file_hash),
    status: String(row.status) as JobStatus,
    priority: String(row.priority) as Priority,
    retry_count: numeric(row.retry_count),
    max_retries: numeric(row.max_retries),
    worker_id: row.worker_id === null ? null : String(row.worker_id),
    lease_token: row.lease_token === null ? null : String(row.lease_token),
    lease_expires_at: nullableNumber(row.lease_expires_at),
    created_at: numeric(row.created_at),
    updated_at: numeric(row.updated_at),
    started_at: nullableNumber(row.started_at),
    completed_at: nullableNumber(row.completed_at),
    available_at: numeric(row.available_at),
    error_message: row.error_message === null ? null : String(row.error_message),
    progress_json: String(row.progress_json || "{}"),
    original_name: row.original_name === null ? null : String(row.original_name),
    size_bytes: numeric(row.size_bytes),
  };
}

export class CoordinatorDatabase {
  private initialized?: Promise<void>;

  constructor(private readonly client: Client) {}

  ensureSchema(): Promise<void> {
    if (!this.initialized) {
      this.initialized = (async () => {
        await this.client.execute("PRAGMA foreign_keys=ON");
        await this.client.batch(SCHEMA.map((sql) => statement(sql)), "write");
        const timestamp = Date.now();
        await this.client.batch(Object.entries(DEFAULT_CONFIG).map(([key, value]) => statement(
          "INSERT OR IGNORE INTO config(key,value_json,updated_at) VALUES(?,?,?)",
          [key, JSON.stringify(value), timestamp],
        )), "write");
      })().catch((error) => {
        this.initialized = undefined;
        throw error;
      });
    }
    return this.initialized;
  }

  async getConfig(): Promise<Record<string, unknown>> {
    const result = await this.client.execute("SELECT key,value_json FROM config");
    return Object.fromEntries(result.rows.map((row) => [String(row.key), JSON.parse(String(row.value_json))]));
  }

  async updateConfig(values: Record<string, unknown>, actor: string): Promise<Record<string, unknown>> {
    const allowed = Object.keys(DEFAULT_CONFIG);
    const timestamp = Date.now();
    const statements: InStatement[] = [];
    for (const [key, value] of Object.entries(values)) {
      if (!allowed.includes(key) || typeof value !== "number" || !Number.isFinite(value) || value < 0) continue;
      statements.push(statement(
        "INSERT INTO config(key,value_json,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",
        [key, JSON.stringify(value), timestamp],
      ));
    }
    if (statements.length) await this.client.batch(statements, "write");
    await this.audit(actor, "config.update", null, values);
    return this.getConfig();
  }

  async enqueue(hash: string, body: Record<string, unknown>): Promise<JobRecord> {
    const timestamp = Date.now();
    const config = await this.getConfig();
    const paths = Array.isArray(body.paths) ? body.paths.filter((v): v is string => typeof v === "string") : [];
    const tags = Array.isArray(body.tags) ? body.tags.filter((v): v is string => typeof v === "string") : [];
    const statements: InStatement[] = [
      statement(`INSERT INTO files(hash,original_name,size_bytes,source,created_at,updated_at) VALUES(?,?,?,?,?,?)
        ON CONFLICT(hash) DO UPDATE SET original_name=COALESCE(excluded.original_name,files.original_name), size_bytes=CASE WHEN excluded.size_bytes>0 THEN excluded.size_bytes ELSE files.size_bytes END, source=COALESCE(excluded.source,files.source), updated_at=excluded.updated_at`, [hash, typeof body.original_name === "string" ? body.original_name : null, Number(body.size_bytes || 0), typeof body.source === "string" ? body.source : null, timestamp, timestamp]),
      statement(`INSERT INTO jobs(file_hash,status,priority,retry_count,max_retries,created_at,updated_at,available_at) VALUES(?,'todo',?,0,?,?,?,?)
        ON CONFLICT(file_hash) DO UPDATE SET priority=CASE WHEN jobs.status IN ('todo','failed') THEN excluded.priority ELSE jobs.priority END,updated_at=excluded.updated_at`, [hash, String(body.priority || "3_normal"), Number(config.max_retries || 3), timestamp, timestamp, timestamp]),
      ...paths.map((path) => statement("INSERT OR IGNORE INTO file_paths(file_hash,path) VALUES(?,?)", [hash, path])),
      ...tags.map((tag) => statement("INSERT OR IGNORE INTO file_tags(file_hash,tag) VALUES(?,?)", [hash, tag])),
    ];
    await this.client.batch(statements, "write");
    await this.audit("worker-api", "enqueue", hash, { priority: body.priority || "3_normal" });
    return (await this.getJob(hash))!;
  }

  async getJob(hash: string): Promise<JobRecord | null> {
    const result = await this.client.execute(statement(
      "SELECT j.*,f.original_name,f.size_bytes FROM jobs j JOIN files f ON f.hash=j.file_hash WHERE j.file_hash=?",
      [hash],
    ));
    return result.rows[0] ? jobFromRow(result.rows[0]) : null;
  }

  async getFileMetadata(hash: string): Promise<{ tags: string[]; paths: string[] }> {
    const results = await this.client.batch([
      statement("SELECT tag FROM file_tags WHERE file_hash=? ORDER BY tag", [hash]),
      statement("SELECT path FROM file_paths WHERE file_hash=? ORDER BY path", [hash]),
    ], "read");
    return {
      tags: results[0].rows.map((row) => String(row.tag)),
      paths: results[1].rows.map((row) => String(row.path)),
    };
  }

  async createWorkerCredential(workerId: string, label: string, tokenHash: string, actor: string): Promise<boolean> {
    const timestamp = Date.now();
    const result = await this.client.execute(statement(
      "INSERT OR IGNORE INTO worker_credentials(worker_id,label,token_hash,created_at,created_by,revoked_at,last_used_at) VALUES(?,?,?,?,?,NULL,NULL)",
      [workerId, label, tokenHash, timestamp, actor],
    ));
    if (!result.rowsAffected) return false;
    await this.audit(actor, "worker.create", workerId, { label });
    return true;
  }

  async authenticateWorkerToken(tokenHash: string): Promise<string | null> {
    const result = await this.client.execute(statement(
      "SELECT worker_id,last_used_at FROM worker_credentials WHERE token_hash=? AND revoked_at IS NULL",
      [tokenHash],
    ));
    if (!result.rows[0]) return null;
    const workerId = String(result.rows[0].worker_id);
    const timestamp = Date.now();
    if (numeric(result.rows[0].last_used_at) < timestamp - 300_000) {
      await this.client.execute(statement("UPDATE worker_credentials SET last_used_at=? WHERE worker_id=?", [timestamp, workerId]));
    }
    return workerId;
  }

  async revokeWorkerCredential(workerId: string, actor: string): Promise<boolean> {
    const timestamp = Date.now();
    const results = await this.client.batch([
      statement("UPDATE worker_credentials SET revoked_at=? WHERE worker_id=? AND revoked_at IS NULL", [timestamp, workerId]),
      statement("UPDATE jobs SET status='todo',available_at=?,updated_at=?,worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,progress_json='{}' WHERE status='processing' AND worker_id=?", [timestamp, timestamp, workerId]),
      statement("UPDATE workers SET status='stopped',current_job=NULL,last_heartbeat=? WHERE worker_id=?", [timestamp, workerId]),
    ], "write");
    if (!results[0].rowsAffected) return false;
    await this.audit(actor, "worker.revoke", workerId, {});
    return true;
  }

  async listWorkerCredentials(): Promise<Record<string, unknown>[]> {
    const result = await this.client.execute(statement(
      `SELECT c.worker_id,c.label,c.created_at,c.created_by,c.revoked_at,c.last_used_at,
              w.hostname,w.status,w.current_job,w.last_heartbeat,w.registered_at
       FROM worker_credentials c LEFT JOIN workers w ON w.worker_id=c.worker_id
       ORDER BY c.created_at DESC LIMIT 250`,
    ));
    return result.rows.map((row) => ({
      worker_id: String(row.worker_id), label: String(row.label), created_at: numeric(row.created_at),
      created_by: String(row.created_by), revoked_at: nullableNumber(row.revoked_at), last_used_at: nullableNumber(row.last_used_at),
      hostname: row.hostname === null ? null : String(row.hostname), status: row.status === null ? "never_connected" : String(row.status),
      current_job: row.current_job === null ? null : String(row.current_job), last_heartbeat: nullableNumber(row.last_heartbeat),
      registered_at: nullableNumber(row.registered_at),
    }));
  }

  async registerWorker(body: Record<string, unknown>): Promise<void> {
    const timestamp = Date.now();
    await this.client.execute(statement(`INSERT INTO workers(worker_id,hostname,status,current_job,last_heartbeat,registered_at,metadata_json,metrics_json) VALUES(?,?,'idle',NULL,?,?,?,'{}')
      ON CONFLICT(worker_id) DO UPDATE SET hostname=excluded.hostname,status='idle',last_heartbeat=excluded.last_heartbeat,metadata_json=excluded.metadata_json`, [String(body.worker_id), String(body.hostname || body.worker_id), timestamp, timestamp, JSON.stringify(body)]));
  }

  async workerHeartbeat(body: Record<string, unknown>): Promise<boolean> {
    const result = await this.client.execute(statement(
      "UPDATE workers SET status=?,current_job=?,last_heartbeat=?,metrics_json=? WHERE worker_id=?",
      [body.current_job ? "processing" : "idle", body.current_job ? String(body.current_job) : null, Date.now(), JSON.stringify(body.metrics || {}), String(body.worker_id)],
    ));
    return result.rowsAffected > 0;
  }

  async deregisterWorker(workerId: string): Promise<void> {
    await this.client.execute(statement("UPDATE workers SET status='stopped',current_job=NULL,last_heartbeat=? WHERE worker_id=?", [Date.now(), workerId]));
  }

  async recoverExpiredLeases(): Promise<number> {
    const timestamp = Date.now();
    const results = await this.client.batch([
      statement(`INSERT INTO job_failures(file_hash,attempt,worker_id,failed_at,error_message,traceback,context_json,progress_json)
        SELECT file_hash,retry_count+1,worker_id,?,'Worker lease expired',NULL,?,progress_json FROM jobs WHERE status='processing' AND lease_expires_at<?`,
        [timestamp, JSON.stringify({ stage: "lease", reason: "expired" }), timestamp]),
      statement(`UPDATE jobs SET status=CASE WHEN retry_count+1>max_retries THEN 'dead' ELSE 'todo' END,retry_count=retry_count+1,available_at=?,error_message='Worker lease expired',updated_at=?,worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,progress_json='{}' WHERE status='processing' AND lease_expires_at<? RETURNING file_hash`, [timestamp, timestamp, timestamp]),
      statement("UPDATE workers SET status='stale',current_job=NULL WHERE current_job IS NOT NULL AND NOT EXISTS(SELECT 1 FROM jobs WHERE jobs.file_hash=workers.current_job AND jobs.status='processing')"),
    ], "write");
    return results[0]?.rows.length || 0;
  }

  async claim(workerId: string, priorities: Priority[], leaseToken: string, leaseSeconds: number): Promise<JobRecord | null> {
    await this.recoverExpiredLeases();
    const timestamp = Date.now();
    const active = await this.client.execute(statement(
      "SELECT j.*,f.original_name,f.size_bytes FROM jobs j JOIN files f ON f.hash=j.file_hash WHERE j.status='processing' AND j.worker_id=? AND j.lease_expires_at>=? LIMIT 1",
      [workerId, timestamp],
    ));
    if (active.rows[0]) return jobFromRow(active.rows[0]);
    const selected = priorities.length ? priorities : [...PRIORITIES];
    const placeholders = selected.map(() => "?").join(",");
    const result = await this.client.execute(statement(
      `UPDATE jobs SET status='processing',worker_id=?,lease_token=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,progress_json='{}'
       WHERE file_hash=(SELECT file_hash FROM jobs WHERE status IN ('todo','failed') AND available_at<=? AND priority IN (${placeholders}) ORDER BY priority,created_at LIMIT 1)
       AND NOT EXISTS(SELECT 1 FROM jobs active WHERE active.status='processing' AND active.worker_id=?) RETURNING file_hash`,
      [workerId, leaseToken, timestamp + leaseSeconds * 1000, timestamp, timestamp, timestamp, ...selected, workerId],
    ));
    if (!result.rows[0]) {
      const repeated = await this.client.execute(statement(
        "SELECT j.*,f.original_name,f.size_bytes FROM jobs j JOIN files f ON f.hash=j.file_hash WHERE j.status='processing' AND j.worker_id=? AND j.lease_expires_at>=? LIMIT 1",
        [workerId, timestamp],
      ));
      return repeated.rows[0] ? jobFromRow(repeated.rows[0]) : null;
    }
    await this.client.execute(statement("UPDATE workers SET status='processing',current_job=?,last_heartbeat=? WHERE worker_id=?", [String(result.rows[0].file_hash), timestamp, workerId]));
    await this.audit(workerId, "claim", String(result.rows[0].file_hash), {});
    return this.getJob(String(result.rows[0].file_hash));
  }

  async jobHeartbeat(hash: string, workerId: string, leaseToken: string, progress: unknown, metrics: unknown, leaseSeconds: number): Promise<boolean> {
    const timestamp = Date.now();
    const results = await this.client.batch([
      statement("UPDATE jobs SET lease_expires_at=?,updated_at=?,progress_json=? WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=? AND lease_expires_at>=?", [timestamp + leaseSeconds * 1000, timestamp, JSON.stringify(progress || {}), hash, workerId, leaseToken, timestamp]),
      statement("UPDATE workers SET status='processing',current_job=?,last_heartbeat=?,metrics_json=? WHERE worker_id=? AND current_job=? AND EXISTS(SELECT 1 FROM jobs WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=?)", [hash, timestamp, JSON.stringify(metrics || {}), workerId, hash, hash, workerId, leaseToken]),
    ], "write");
    return results[0].rowsAffected > 0;
  }

  async validLease(hash: string, workerId: string, leaseToken: string): Promise<boolean> {
    const result = await this.client.execute(statement(
      "SELECT 1 valid FROM jobs WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=? AND lease_expires_at>=?",
      [hash, workerId, leaseToken, Date.now()],
    ));
    return Boolean(result.rows[0]);
  }

  async complete(hash: string, workerId: string, leaseToken: string, result: unknown, metrics: unknown): Promise<"completed" | "done" | "conflict"> {
    const timestamp = Date.now();
    const results = await this.client.batch([
      statement("UPDATE jobs SET status='done',completed_at=?,updated_at=?,worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,progress_json=? WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=?", [timestamp, timestamp, JSON.stringify(result || {}), hash, workerId, leaseToken]),
      statement("UPDATE workers SET status='idle',current_job=NULL,last_heartbeat=?,metrics_json=? WHERE worker_id=? AND current_job=?", [timestamp, JSON.stringify(metrics || {}), workerId, hash]),
    ], "write");
    if (results[0].rowsAffected) {
      await this.audit(workerId, "complete", hash, result || {});
      return "completed";
    }
    return (await this.getJob(hash))?.status === "done" ? "done" : "conflict";
  }

  async fail(hash: string, workerId: string, leaseToken: string, message: string, detail: unknown, metrics: unknown): Promise<JobRecord | null> {
    const timestamp = Date.now();
    const failureDetail = detail && typeof detail === "object" && !Array.isArray(detail) ? detail as Record<string, unknown> : {};
    const context = failureDetail.context && typeof failureDetail.context === "object" && !Array.isArray(failureDetail.context)
      ? failureDetail.context : {};
    const traceback = typeof failureDetail.traceback === "string" ? failureDetail.traceback.slice(0, 100_000) : null;
    const results = await this.client.batch([
      statement(`INSERT INTO job_failures(file_hash,attempt,worker_id,failed_at,error_message,traceback,context_json,progress_json)
        SELECT file_hash,retry_count+1,worker_id,?,?,?,?,progress_json FROM jobs
        WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=?`,
        [timestamp, message, traceback, JSON.stringify(context), hash, workerId, leaseToken]),
      statement(`UPDATE jobs SET status=CASE WHEN retry_count+1>max_retries THEN 'dead' ELSE 'failed' END,retry_count=retry_count+1,available_at=?+MIN(3600000,60000*(1 << retry_count)),error_message=?,updated_at=?,worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,progress_json='{}' WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=? RETURNING file_hash`, [timestamp, message, timestamp, hash, workerId, leaseToken]),
      statement("UPDATE workers SET status='idle',current_job=NULL,last_heartbeat=?,metrics_json=? WHERE worker_id=? AND current_job=?", [timestamp, JSON.stringify(metrics || {}), workerId, hash]),
    ], "write");
    return results[0].rowsAffected ? this.getJob(hash) : null;
  }

  async release(hash: string, workerId: string, leaseToken: string): Promise<"released" | "todo" | "conflict"> {
    const timestamp = Date.now();
    const results = await this.client.batch([
      statement("UPDATE jobs SET status='todo',available_at=?,updated_at=?,worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,progress_json='{}' WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=?", [timestamp, timestamp, hash, workerId, leaseToken]),
      statement("UPDATE workers SET status='idle',current_job=NULL,last_heartbeat=? WHERE worker_id=? AND current_job=?", [timestamp, workerId, hash]),
    ], "write");
    if (results[0].rowsAffected) return "released";
    return (await this.getJob(hash))?.status === "todo" ? "todo" : "conflict";
  }

  async adminRetry(hash: string): Promise<boolean> {
    const result = await this.client.execute(statement("UPDATE jobs SET status='todo',retry_count=0,available_at=?,error_message=NULL,updated_at=?,worker_id=NULL,lease_token=NULL,lease_expires_at=NULL WHERE file_hash=? AND status IN ('failed','dead')", [Date.now(), Date.now(), hash]));
    return result.rowsAffected > 0;
  }

  async adminCancel(hash: string): Promise<boolean> {
    const timestamp = Date.now();
    const results = await this.client.batch([
      statement("UPDATE jobs SET status='todo',available_at=?,updated_at=?,worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,progress_json='{}' WHERE file_hash=? AND status='processing'", [timestamp, timestamp, hash]),
      statement("UPDATE workers SET status='idle',current_job=NULL,last_heartbeat=? WHERE current_job=?", [timestamp, hash]),
    ], "write");
    return results[0].rowsAffected > 0;
  }

  async adminPriority(hash: string, priority: Priority): Promise<boolean> {
    const result = await this.client.execute(statement("UPDATE jobs SET priority=?,updated_at=? WHERE file_hash=? AND status IN ('todo','failed')", [priority, Date.now(), hash]));
    return result.rowsAffected > 0;
  }

  async snapshot(includeLeaseTokens = true, includeJobs = true): Promise<Record<string, unknown>> {
    await this.recoverExpiredLeases();
    const results = await this.client.batch([
      statement("SELECT status,COUNT(*) count FROM jobs GROUP BY status"),
      statement("SELECT priority,COUNT(*) count FROM jobs WHERE status IN ('todo','failed') GROUP BY priority"),
      statement(includeJobs
        ? "SELECT j.*,f.original_name,f.size_bytes FROM jobs j JOIN files f ON f.hash=j.file_hash ORDER BY CASE j.status WHEN 'processing' THEN 1 WHEN 'dead' THEN 2 WHEN 'failed' THEN 3 WHEN 'todo' THEN 4 ELSE 5 END,j.updated_at DESC LIMIT 250"
        : "SELECT j.*,f.original_name,f.size_bytes FROM jobs j JOIN files f ON f.hash=j.file_hash WHERE 0"),
      statement("SELECT * FROM workers ORDER BY last_heartbeat DESC LIMIT 250"),
      statement("SELECT key,value_json FROM config"),
    ], "read");
    const counts = Object.fromEntries(results[0].rows.map((row) => [String(row.status), numeric(row.count)]));
    const priority = Object.fromEntries(results[1].rows.map((row) => [String(row.priority), numeric(row.count)]));
    const jobs = results[2].rows.map((row) => {
      const job = jobFromRow(row);
      return { ...job, hash: job.file_hash, lease_token: includeLeaseTokens ? job.lease_token : null, progress: JSON.parse(job.progress_json || "{}") };
    });
    const workers = results[3].rows.map((row) => ({
      worker_id: String(row.worker_id), hostname: String(row.hostname), status: String(row.status),
      current_job: row.current_job === null ? null : String(row.current_job), last_heartbeat: numeric(row.last_heartbeat),
      registered_at: numeric(row.registered_at), metadata: JSON.parse(String(row.metadata_json || "{}")), metrics: JSON.parse(String(row.metrics_json || "{}")),
    }));
    const config = Object.fromEntries(results[4].rows.map((row) => [String(row.key), JSON.parse(String(row.value_json))]));
    return { counts, priority, jobs, workers, config };
  }

  async listJobs(filters: {
    query?: string;
    status?: JobStatus;
    priority?: Priority;
    limit?: number;
    offset?: number;
  }): Promise<{ jobs: Record<string, unknown>[]; total: number }> {
    const where: string[] = [];
    const args: InValue[] = [];
    if (filters.status) { where.push("j.status=?"); args.push(filters.status); }
    if (filters.priority) { where.push("j.priority=?"); args.push(filters.priority); }
    if (filters.query?.trim()) {
      const pattern = `%${filters.query.trim().toLowerCase()}%`;
      where.push(`(LOWER(j.file_hash) LIKE ? OR LOWER(COALESCE(f.original_name,'')) LIKE ? OR LOWER(COALESCE(f.source,'')) LIKE ?
        OR EXISTS(SELECT 1 FROM file_paths fp WHERE fp.file_hash=j.file_hash AND LOWER(fp.path) LIKE ?)
        OR EXISTS(SELECT 1 FROM file_tags ft WHERE ft.file_hash=j.file_hash AND LOWER(ft.tag) LIKE ?))`);
      args.push(pattern, pattern, pattern, pattern, pattern);
    }
    const clause = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const requestedLimit = Number.isFinite(filters.limit) ? Number(filters.limit) : 50;
    const requestedOffset = Number.isFinite(filters.offset) ? Number(filters.offset) : 0;
    const limit = Math.min(100, Math.max(1, Math.floor(requestedLimit)));
    const offset = Math.max(0, Math.floor(requestedOffset));
    const results = await this.client.batch([
      statement(`SELECT COUNT(*) count FROM jobs j JOIN files f ON f.hash=j.file_hash ${clause}`, args),
      statement(`SELECT j.*,f.original_name,f.size_bytes,f.source,
        (SELECT failed_at FROM job_failures jf WHERE jf.file_hash=j.file_hash ORDER BY failed_at DESC,id DESC LIMIT 1) latest_failure_at,
        (SELECT context_json FROM job_failures jf WHERE jf.file_hash=j.file_hash ORDER BY failed_at DESC,id DESC LIMIT 1) latest_failure_context,
        COALESCE((SELECT json_group_array(path) FROM file_paths WHERE file_hash=j.file_hash),'[]') paths_json,
        COALESCE((SELECT json_group_array(tag) FROM file_tags WHERE file_hash=j.file_hash),'[]') tags_json
        FROM jobs j JOIN files f ON f.hash=j.file_hash ${clause}
        ORDER BY j.updated_at DESC,j.file_hash LIMIT ? OFFSET ?`, [...args, limit, offset]),
    ], "read");
    return {
      total: numeric(results[0]!.rows[0]?.count),
      jobs: results[1]!.rows.map((row) => ({
        ...jobFromRow(row), hash: String(row.file_hash), lease_token: null,
        progress: JSON.parse(String(row.progress_json || "{}")), source: row.source === null ? null : String(row.source),
        latest_failure_at: nullableNumber(row.latest_failure_at),
        latest_failure_context: JSON.parse(String(row.latest_failure_context || "{}")),
        paths: JSON.parse(String(row.paths_json || "[]")), tags: JSON.parse(String(row.tags_json || "[]")),
      })),
    };
  }

  async listJobFailures(hash: string): Promise<Record<string, unknown>[]> {
    const result = await this.client.execute(statement(
      `SELECT id,attempt,worker_id,failed_at,error_message,traceback,context_json,progress_json
       FROM job_failures WHERE file_hash=? ORDER BY failed_at DESC,id DESC LIMIT 50`,
      [hash],
    ));
    return result.rows.map((row) => ({
      id: numeric(row.id), attempt: numeric(row.attempt),
      worker_id: row.worker_id === null ? null : String(row.worker_id), failed_at: numeric(row.failed_at),
      error_message: String(row.error_message), traceback: row.traceback === null ? null : String(row.traceback),
      context: JSON.parse(String(row.context_json || "{}")), progress: JSON.parse(String(row.progress_json || "{}")),
    }));
  }

  async exportBackup(): Promise<Record<string, unknown>> {
    const tableNames = ["files", "file_paths", "file_tags", "jobs", "job_failures", "workers", "worker_credentials", "config", "audit_log"];
    const results = await this.client.batch([
      statement(
        `SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' AND tbl_name IN (${tableNames.map(() => "?").join(",")}) ORDER BY type,name`,
        tableNames,
      ),
      ...tableNames.map((name) => statement(`SELECT * FROM ${name}`)),
    ], "read");
    const tables = Object.fromEntries(tableNames.map((name, index) => [
      name,
      results[index + 1]!.rows.map(serializableRow),
    ]));
    return {
      format: "blobforge-coordinator-backup",
      version: 1,
      created_at: new Date().toISOString(),
      schema: results[0]!.rows.map(serializableRow),
      tables,
    };
  }

  async audit(actor: string, action: string, subject: string | null, detail: unknown): Promise<void> {
    await this.client.execute(statement("INSERT INTO audit_log(actor,action,subject,detail_json,created_at) VALUES(?,?,?,?,?)", [actor, action, subject, JSON.stringify(detail || {}), Date.now()]));
  }
}
