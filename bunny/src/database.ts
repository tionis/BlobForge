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

export interface ImportItem {
  hash: string;
  status?: JobStatus;
  priority?: Priority;
  retry_count?: number;
  max_retries?: number;
  original_name?: string | null;
  size_bytes?: number;
  source?: string | null;
  paths?: string[];
  tags?: string[];
  error?: string | null;
}

const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS files (hash TEXT PRIMARY KEY CHECK(length(hash)=64), original_name TEXT, size_bytes INTEGER NOT NULL DEFAULT 0, source TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS file_paths (file_hash TEXT NOT NULL REFERENCES files(hash) ON DELETE CASCADE, path TEXT NOT NULL, PRIMARY KEY(file_hash,path))`,
  `CREATE TABLE IF NOT EXISTS file_tags (file_hash TEXT NOT NULL REFERENCES files(hash) ON DELETE CASCADE, tag TEXT NOT NULL, PRIMARY KEY(file_hash,tag))`,
  `CREATE TABLE IF NOT EXISTS jobs (file_hash TEXT PRIMARY KEY REFERENCES files(hash) ON DELETE CASCADE, status TEXT NOT NULL CHECK(status IN ('todo','processing','failed','dead','done')), priority TEXT NOT NULL CHECK(priority IN ('1_critical','2_high','3_normal','4_low','5_background')), retry_count INTEGER NOT NULL DEFAULT 0, max_retries INTEGER NOT NULL DEFAULT 3, worker_id TEXT, lease_token TEXT, lease_expires_at INTEGER, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, started_at INTEGER, completed_at INTEGER, available_at INTEGER NOT NULL, error_message TEXT, progress_json TEXT NOT NULL DEFAULT '{}')`,
  `CREATE INDEX IF NOT EXISTS jobs_claim_idx ON jobs(status,available_at,priority,created_at)`,
  `CREATE INDEX IF NOT EXISTS jobs_lease_idx ON jobs(status,lease_expires_at)`,
  `CREATE TABLE IF NOT EXISTS workers (worker_id TEXT PRIMARY KEY, hostname TEXT NOT NULL, status TEXT NOT NULL, current_job TEXT, last_heartbeat INTEGER NOT NULL, registered_at INTEGER NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}', metrics_json TEXT NOT NULL DEFAULT '{}')`,
  `CREATE TABLE IF NOT EXISTS job_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, file_hash TEXT NOT NULL REFERENCES files(hash) ON DELETE CASCADE, level TEXT NOT NULL, message TEXT NOT NULL, detail_json TEXT NOT NULL DEFAULT '{}', created_at INTEGER NOT NULL)`,
  `CREATE INDEX IF NOT EXISTS job_logs_hash_idx ON job_logs(file_hash,created_at DESC)`,
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
    const results = await this.client.batch([
      statement("INSERT INTO job_logs(file_hash,level,message,detail_json,created_at) SELECT ?,'error',?,?,? WHERE EXISTS(SELECT 1 FROM jobs WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=?)", [hash, message, JSON.stringify(detail || {}), timestamp, hash, workerId, leaseToken]),
      statement(`UPDATE jobs SET status=CASE WHEN retry_count+1>max_retries THEN 'dead' ELSE 'failed' END,retry_count=retry_count+1,available_at=?+MIN(3600000,60000*(1 << retry_count)),error_message=?,updated_at=?,worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,progress_json='{}' WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=? RETURNING file_hash`, [timestamp, message, timestamp, hash, workerId, leaseToken]),
      statement("UPDATE workers SET status='idle',current_job=NULL,last_heartbeat=?,metrics_json=? WHERE worker_id=? AND current_job=?", [timestamp, JSON.stringify(metrics || {}), workerId, hash]),
    ], "write");
    return results[1].rowsAffected ? this.getJob(hash) : null;
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

  async snapshot(includeLeaseTokens = true): Promise<Record<string, unknown>> {
    await this.recoverExpiredLeases();
    const results = await this.client.batch([
      statement("SELECT status,COUNT(*) count FROM jobs GROUP BY status"),
      statement("SELECT priority,COUNT(*) count FROM jobs WHERE status IN ('todo','failed') GROUP BY priority"),
      statement("SELECT j.*,f.original_name,f.size_bytes FROM jobs j JOIN files f ON f.hash=j.file_hash ORDER BY CASE j.status WHEN 'processing' THEN 1 WHEN 'dead' THEN 2 WHEN 'failed' THEN 3 WHEN 'todo' THEN 4 ELSE 5 END,j.updated_at DESC LIMIT 250"),
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

  async importItems(items: ImportItem[]): Promise<number> {
    const config = await this.getConfig();
    const timestamp = Date.now();
    const statements: InStatement[] = [];
    for (const item of items) {
      const status = item.status === "processing" ? "todo" : (item.status || "todo");
      const retry = Math.max(0, Math.floor(item.retry_count || 0));
      const maxRetries = Math.max(retry, Math.floor(item.max_retries || Number(config.max_retries || 3)));
      statements.push(statement(`INSERT INTO files(hash,original_name,size_bytes,source,created_at,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(hash) DO UPDATE SET original_name=COALESCE(excluded.original_name,files.original_name),size_bytes=CASE WHEN excluded.size_bytes>0 THEN excluded.size_bytes ELSE files.size_bytes END,source=COALESCE(excluded.source,files.source),updated_at=excluded.updated_at`, [item.hash, item.original_name || null, Math.max(0, item.size_bytes || 0), item.source || null, timestamp, timestamp]));
      statements.push(statement(`INSERT INTO jobs(file_hash,status,priority,retry_count,max_retries,created_at,updated_at,completed_at,available_at,error_message) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(file_hash) DO UPDATE SET status=excluded.status,priority=excluded.priority,retry_count=excluded.retry_count,max_retries=excluded.max_retries,updated_at=excluded.updated_at,completed_at=excluded.completed_at,available_at=excluded.available_at,error_message=excluded.error_message,worker_id=NULL,lease_token=NULL,lease_expires_at=NULL,progress_json='{}'`, [item.hash, status, item.priority || "3_normal", retry, maxRetries, timestamp, timestamp, status === "done" ? timestamp : null, timestamp, item.error || null]));
      for (const path of item.paths || []) statements.push(statement("INSERT OR IGNORE INTO file_paths(file_hash,path) VALUES(?,?)", [item.hash, path]));
      for (const tag of item.tags || []) statements.push(statement("INSERT OR IGNORE INTO file_tags(file_hash,tag) VALUES(?,?)", [item.hash, tag]));
    }
    await this.client.batch(statements, "write");
    await this.audit("migration-api", "import", null, { imported: items.length });
    return items.length;
  }

  async audit(actor: string, action: string, subject: string | null, detail: unknown): Promise<void> {
    await this.client.execute(statement("INSERT INTO audit_log(actor,action,subject,detail_json,created_at) VALUES(?,?,?,?,?)", [actor, action, subject, JSON.stringify(detail || {}), Date.now()]));
  }
}
