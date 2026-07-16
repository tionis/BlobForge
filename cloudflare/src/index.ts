import { DurableObject } from "cloudflare:workers";
import { APP_CSS, APP_JS, renderHome } from "./ui";

export interface Env {
  COORDINATOR: DurableObjectNamespace<BlobForgeCoordinator>;
  WORKER_API_TOKEN: string;
  MIGRATION_API_TOKEN?: string;
  ADMIN_ME?: string;
  SESSION_TTL_SECONDS?: string;
  LEASE_SECONDS?: string;
}

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

interface JobRow extends Record<string, SqlStorageValue> {
  file_hash: string;
  status: string;
  priority: string;
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

interface WorkerRow extends Record<string, SqlStorageValue> {
  worker_id: string;
  hostname: string;
  status: string;
  current_job: string | null;
  last_heartbeat: number;
  registered_at: number;
  metadata_json: string;
  metrics_json: string;
}

interface AuthAttemptRow extends Record<string, SqlStorageValue> {
  state: string;
  verifier: string;
  token_endpoint: string;
  issuer: string;
  expires_at: number;
}

const PRIORITIES = [
  "1_critical",
  "2_high",
  "3_normal",
  "4_low",
  "5_background",
] as const;

const SCHEMA = `
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS files (
  hash TEXT PRIMARY KEY CHECK(length(hash) = 64),
  original_name TEXT,
  size_bytes INTEGER NOT NULL DEFAULT 0,
  source TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS file_paths (
  file_hash TEXT NOT NULL REFERENCES files(hash) ON DELETE CASCADE,
  path TEXT NOT NULL,
  PRIMARY KEY (file_hash, path)
);
CREATE TABLE IF NOT EXISTS file_tags (
  file_hash TEXT NOT NULL REFERENCES files(hash) ON DELETE CASCADE,
  tag TEXT NOT NULL,
  PRIMARY KEY (file_hash, tag)
);
CREATE TABLE IF NOT EXISTS jobs (
  file_hash TEXT PRIMARY KEY REFERENCES files(hash) ON DELETE CASCADE,
  status TEXT NOT NULL CHECK(status IN ('todo','processing','failed','dead','done')),
  priority TEXT NOT NULL CHECK(priority IN ('1_critical','2_high','3_normal','4_low','5_background')),
  retry_count INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 3,
  worker_id TEXT,
  lease_token TEXT,
  lease_expires_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  started_at INTEGER,
  completed_at INTEGER,
  available_at INTEGER NOT NULL,
  error_message TEXT,
  progress_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS jobs_claim_idx ON jobs(status, available_at, priority, created_at);
CREATE INDEX IF NOT EXISTS jobs_lease_idx ON jobs(status, lease_expires_at);
CREATE TABLE IF NOT EXISTS workers (
  worker_id TEXT PRIMARY KEY,
  hostname TEXT NOT NULL,
  status TEXT NOT NULL,
  current_job TEXT,
  last_heartbeat INTEGER NOT NULL,
  registered_at INTEGER NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  metrics_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS job_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  file_hash TEXT NOT NULL REFERENCES files(hash) ON DELETE CASCADE,
  level TEXT NOT NULL,
  message TEXT NOT NULL,
  detail_json TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS job_logs_hash_idx ON job_logs(file_hash, created_at DESC);
CREATE TABLE IF NOT EXISTS config (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS auth_attempts (
  state TEXT PRIMARY KEY,
  verifier TEXT NOT NULL,
  token_endpoint TEXT NOT NULL,
  issuer TEXT NOT NULL,
  expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
  token_hash TEXT PRIMARY KEY,
  me TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_expiry_idx ON sessions(expires_at);
CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  subject TEXT,
  detail_json TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL
);
`;

const DEFAULT_CONFIG: Record<string, Json> = {
  max_retries: 3,
  heartbeat_interval: 60,
  lease_seconds: 900,
  conversion_timeout: 3600,
};

function json(data: Json, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers);
  headers.set("content-type", "application/json; charset=utf-8");
  headers.set("cache-control", "no-store");
  return new Response(JSON.stringify(data), { ...init, headers });
}

function error(message: string, status = 400): Response {
  return json({ error: message }, { status });
}

function html(body: string, status = 200): Response {
  return new Response(body, {
    status,
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
      "content-security-policy": "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
      "referrer-policy": "no-referrer",
      "x-content-type-options": "nosniff",
    },
  });
}

function now(): number {
  return Date.now();
}

function canonicalUrl(value: string): string {
  const url = new URL(value);
  url.hash = "";
  if (!url.pathname) url.pathname = "/";
  return url.toString();
}

function parseCookies(request: Request): Map<string, string> {
  const result = new Map<string, string>();
  for (const part of (request.headers.get("cookie") || "").split(";")) {
    const index = part.indexOf("=");
    if (index > 0) result.set(part.slice(0, index).trim(), part.slice(index + 1).trim());
  }
  return result;
}

function base64url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function randomToken(bytes = 32): string {
  const value = new Uint8Array(bytes);
  crypto.getRandomValues(value);
  return base64url(value);
}

async function sha256(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return base64url(new Uint8Array(digest));
}

async function secureEqual(left: string, right: string): Promise<boolean> {
  const [a, b] = await Promise.all([sha256(left), sha256(right)]);
  let diff = a.length ^ b.length;
  const length = Math.max(a.length, b.length);
  for (let i = 0; i < length; i++) {
    diff |= (a.charCodeAt(i % a.length) || 0) ^ (b.charCodeAt(i % b.length) || 0);
  }
  return diff === 0;
}

function parseJsonObject(value: string): Record<string, Json> {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function validHash(value: string): boolean {
  return /^[a-f0-9]{64}$/.test(value);
}

function validPriority(value: unknown): value is (typeof PRIORITIES)[number] {
  return typeof value === "string" && (PRIORITIES as readonly string[]).includes(value);
}

function cookieName(url: URL): string {
  return url.protocol === "https:" ? "__Host-blobforge_session" : "blobforge_session";
}

function sessionCookie(url: URL, token: string, maxAge: number): string {
  const secure = url.protocol === "https:" ? "; Secure" : "";
  return `${cookieName(url)}=${token}; Path=/; HttpOnly; SameSite=Lax${secure}; Max-Age=${maxAge}`;
}

function clearSessionCookie(url: URL): string {
  const secure = url.protocol === "https:" ? "; Secure" : "";
  return `${cookieName(url)}=; Path=/; HttpOnly; SameSite=Lax${secure}; Max-Age=0`;
}

function linkRelations(htmlText: string, rel: string): string[] {
  const results: string[] = [];
  for (const match of htmlText.matchAll(/<link\s+[^>]*>/gi)) {
    const tag = match[0];
    const attrs = new Map<string, string>();
    for (const attr of tag.matchAll(/([\w:-]+)\s*=\s*["']([^"']*)["']/g)) {
      attrs.set(attr[1]!.toLowerCase(), attr[2]!);
    }
    if ((attrs.get("rel") || "").split(/\s+/).includes(rel) && attrs.get("href")) {
      results.push(attrs.get("href")!);
    }
  }
  return results;
}

function headerLink(response: Response, rel: string): string | null {
  const value = response.headers.get("link");
  if (!value) return null;
  for (const part of value.split(/,(?=\s*<)/)) {
    const target = part.match(/<([^>]+)>/)?.[1];
    const relation = part.match(/rel\s*=\s*["']?([^"';,]+)/i)?.[1];
    if (target && relation?.split(/\s+/).includes(rel)) return target;
  }
  return null;
}

async function discoverIndieAuth(me: string): Promise<{ authorization: string; token: string; issuer: string }> {
  const profileUrl = new URL(me);
  if (profileUrl.protocol !== "https:") throw new Error("Admin profile must use HTTPS");
  const response = await fetch(profileUrl, { redirect: "follow", headers: { accept: "text/html" } });
  if (!response.ok) throw new Error(`Could not fetch admin profile (${response.status})`);
  const body = await response.text();
  const base = response.url || profileUrl.toString();
  const metadataRef = headerLink(response, "indieauth-metadata") || linkRelations(body, "indieauth-metadata")[0];
  if (metadataRef) {
    const metadataUrl = new URL(metadataRef, base);
    if (metadataUrl.protocol !== "https:") throw new Error("IndieAuth metadata must use HTTPS");
    const metadataResponse = await fetch(metadataUrl, { headers: { accept: "application/json" } });
    if (!metadataResponse.ok) throw new Error(`Could not fetch IndieAuth metadata (${metadataResponse.status})`);
    const metadata = (await metadataResponse.json()) as Record<string, unknown>;
    const authorization = String(metadata.authorization_endpoint || "");
    const token = String(metadata.token_endpoint || "");
    const issuer = String(metadata.issuer || "");
    if (!authorization || !token || !issuer) throw new Error("Incomplete IndieAuth metadata");
    for (const endpoint of [authorization, token, issuer]) {
      if (new URL(endpoint).protocol !== "https:") throw new Error("IndieAuth endpoints must use HTTPS");
    }
    return { authorization, token, issuer };
  }
  const authorizationRef = headerLink(response, "authorization_endpoint") || linkRelations(body, "authorization_endpoint")[0];
  const tokenRef = headerLink(response, "token_endpoint") || linkRelations(body, "token_endpoint")[0];
  if (!authorizationRef || !tokenRef) throw new Error("No IndieAuth endpoints found on admin profile");
  return {
    authorization: new URL(authorizationRef, base).toString(),
    token: new URL(tokenRef, base).toString(),
    issuer: new URL(authorizationRef, base).origin + "/",
  };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const id = env.COORDINATOR.idFromName("global");
    return env.COORDINATOR.get(id).fetch(request);
  },
} satisfies ExportedHandler<Env>;

export class BlobForgeCoordinator extends DurableObject<Env> {
  private readonly sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(SCHEMA);
    const timestamp = now();
    for (const [key, value] of Object.entries(DEFAULT_CONFIG)) {
      this.sql.exec(
        "INSERT OR IGNORE INTO config (key, value_json, updated_at) VALUES (?, ?, ?)",
        key,
        JSON.stringify(value),
        timestamp,
      );
    }
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const url = new URL(request.url);
      if (url.pathname === "/app.css") return new Response(APP_CSS, { headers: { "content-type": "text/css; charset=utf-8", "cache-control": "public, max-age=3600" } });
      if (url.pathname === "/app.js") return new Response(APP_JS, { headers: { "content-type": "text/javascript; charset=utf-8", "cache-control": "public, max-age=3600" } });
      if (url.pathname === "/client-metadata.json") return this.clientMetadata(url);
      if (url.pathname === "/auth/login" && request.method === "GET") return this.login(request);
      if (url.pathname === "/auth/callback" && request.method === "GET") return this.callback(request);
      if (url.pathname === "/auth/logout" && request.method === "POST") return this.logout(request);
      if (url.pathname === "/api/v1/health" && request.method === "GET") return json({ ok: true, service: "blobforge-coordinator" });
      if (url.pathname === "/api/v1/migration/import" && request.method === "POST") return this.migrationImport(request);
      if (url.pathname.startsWith("/api/v1/admin/")) return this.adminApi(request, url);
      if (url.pathname.startsWith("/api/v1/")) return this.workerApi(request, url);
      if (url.pathname === "/" && request.method === "GET") {
        const session = await this.session(request);
        return html(renderHome(Boolean(session), this.adminMe(), `${url.origin}/auth/callback`));
      }
      return error("Not found", 404);
    } catch (cause) {
      console.error(cause);
      return error(cause instanceof Error ? cause.message : "Internal error", 500);
    }
  }

  async alarm(): Promise<void> {
    this.recoverExpiredLeases();
    await this.scheduleNextAlarm();
  }

  private adminMe(): string {
    return canonicalUrl(this.env.ADMIN_ME || "https://eric.wendland.dev/");
  }

  private clientMetadata(url: URL): Response {
    return json({
      client_id: `${url.origin}/client-metadata.json`,
      client_name: "BlobForge",
      client_uri: `${url.origin}/`,
      redirect_uris: [`${url.origin}/auth/callback`],
    });
  }

  private async login(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const me = this.adminMe();
    const discovered = await discoverIndieAuth(me);
    const state = randomToken();
    const verifier = randomToken(48);
    const challenge = await sha256(verifier);
    const expires = now() + 10 * 60_000;
    this.sql.exec("DELETE FROM auth_attempts WHERE expires_at < ?", now());
    this.sql.exec(
      "INSERT INTO auth_attempts (state, verifier, token_endpoint, issuer, expires_at) VALUES (?, ?, ?, ?, ?)",
      state,
      verifier,
      discovered.token,
      discovered.issuer,
      expires,
    );
    const auth = new URL(discovered.authorization);
    auth.searchParams.set("response_type", "code");
    auth.searchParams.set("client_id", `${url.origin}/client-metadata.json`);
    auth.searchParams.set("redirect_uri", `${url.origin}/auth/callback`);
    auth.searchParams.set("state", state);
    auth.searchParams.set("code_challenge", challenge);
    auth.searchParams.set("code_challenge_method", "S256");
    auth.searchParams.set("scope", "profile");
    auth.searchParams.set("me", me);
    return Response.redirect(auth.toString(), 302);
  }

  private async callback(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const state = url.searchParams.get("state") || "";
    const code = url.searchParams.get("code") || "";
    if (!state || !code) return error(url.searchParams.get("error") || "Missing IndieAuth response", 400);
    const attempt = Array.from(this.sql.exec<AuthAttemptRow>("SELECT * FROM auth_attempts WHERE state = ?", state))[0];
    this.sql.exec("DELETE FROM auth_attempts WHERE state = ?", state);
    if (!attempt || attempt.expires_at < now()) return error("Expired or invalid IndieAuth state", 400);
    const form = new URLSearchParams({
      grant_type: "authorization_code",
      code,
      client_id: `${url.origin}/client-metadata.json`,
      redirect_uri: `${url.origin}/auth/callback`,
      code_verifier: attempt.verifier,
    });
    const tokenResponse = await fetch(attempt.token_endpoint, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded", accept: "application/json" },
      body: form,
    });
    const contentType = tokenResponse.headers.get("content-type") || "";
    let result: Record<string, unknown>;
    if (contentType.includes("json")) {
      result = (await tokenResponse.json()) as Record<string, unknown>;
    } else {
      result = {};
      new URLSearchParams(await tokenResponse.text()).forEach((value, key) => { result[key] = value; });
    }
    if (!tokenResponse.ok) return error(String(result.error_description || result.error || "IndieAuth exchange failed"), 401);
    if (typeof result.me !== "string" || !result.me) return error("IndieAuth response did not include an identity", 401);
    const returnedMe = canonicalUrl(result.me);
    if (returnedMe !== this.adminMe()) return error("Authenticated identity is not a BlobForge administrator", 403);
    const token = randomToken();
    const tokenHash = await sha256(token);
    const ttl = Math.max(300, Number(this.env.SESSION_TTL_SECONDS || 43_200));
    this.sql.exec("DELETE FROM sessions WHERE expires_at < ?", now());
    this.sql.exec(
      "INSERT INTO sessions (token_hash, me, created_at, expires_at) VALUES (?, ?, ?, ?)",
      tokenHash,
      returnedMe,
      now(),
      now() + ttl * 1000,
    );
    this.audit(returnedMe, "login", returnedMe, {});
    return new Response(null, { status: 302, headers: { location: "/", "set-cookie": sessionCookie(url, token, ttl) } });
  }

  private async logout(request: Request): Promise<Response> {
    if (!this.sameOrigin(request)) return error("Invalid origin", 403);
    const url = new URL(request.url);
    const raw = parseCookies(request).get(cookieName(url));
    if (raw) this.sql.exec("DELETE FROM sessions WHERE token_hash = ?", await sha256(raw));
    return new Response(null, { status: 302, headers: { location: "/", "set-cookie": clearSessionCookie(url) } });
  }

  private async session(request: Request): Promise<{ me: string } | null> {
    const url = new URL(request.url);
    const token = parseCookies(request).get(cookieName(url));
    if (!token) return null;
    const row = Array.from(this.sql.exec<{ me: string; expires_at: number }>(
      "SELECT me, expires_at FROM sessions WHERE token_hash = ?",
      await sha256(token),
    ))[0];
    if (!row || row.expires_at < now()) return null;
    return { me: row.me };
  }

  private sameOrigin(request: Request): boolean {
    if (["GET", "HEAD", "OPTIONS"].includes(request.method)) return true;
    const origin = request.headers.get("origin");
    return origin === new URL(request.url).origin;
  }

  private async workerAuthorized(request: Request): Promise<boolean> {
    const auth = request.headers.get("authorization") || "";
    if (!auth.startsWith("Bearer ") || !this.env.WORKER_API_TOKEN) return false;
    return secureEqual(auth.slice(7), this.env.WORKER_API_TOKEN);
  }

  private async migrationAuthorized(request: Request): Promise<boolean> {
    const auth = request.headers.get("authorization") || "";
    if (!auth.startsWith("Bearer ") || !this.env.MIGRATION_API_TOKEN) return false;
    return secureEqual(auth.slice(7), this.env.MIGRATION_API_TOKEN);
  }

  private async migrationImport(request: Request): Promise<Response> {
    if (!(await this.migrationAuthorized(request))) return error("Unauthorized", 401);
    const body = await this.body(request);
    const items = body.items;
    if (!Array.isArray(items) || items.length < 1 || items.length > 250) {
      return error("items must contain between 1 and 250 records");
    }
    const timestamp = now();
    let imported = 0;
    this.ctx.storage.transactionSync(() => {
      for (const value of items) {
        if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid migration record");
        const item = value as Record<string, unknown>;
        const hash = String(item.hash || "");
        if (!validHash(hash)) throw new Error(`Invalid migration hash: ${hash}`);
        const status = item.status === "processing" ? "todo" : String(item.status || "todo");
        if (!["todo", "failed", "dead", "done"].includes(status)) throw new Error(`Invalid migration status: ${status}`);
        const priority = item.priority || "3_normal";
        if (!validPriority(priority)) throw new Error(`Invalid migration priority: ${String(priority)}`);
        const paths = Array.isArray(item.paths) ? item.paths.filter((entry): entry is string => typeof entry === "string") : [];
        const tags = Array.isArray(item.tags) ? item.tags.filter((entry): entry is string => typeof entry === "string") : [];
        this.sql.exec(
          `INSERT INTO files (hash, original_name, size_bytes, source, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(hash) DO UPDATE SET original_name=COALESCE(excluded.original_name, files.original_name),
             size_bytes=CASE WHEN excluded.size_bytes > 0 THEN excluded.size_bytes ELSE files.size_bytes END,
             source=COALESCE(excluded.source, files.source), updated_at=excluded.updated_at`,
          hash,
          typeof item.original_name === "string" ? item.original_name : null,
          Math.max(0, Number(item.size_bytes || 0)),
          typeof item.source === "string" ? item.source : null,
          timestamp,
          timestamp,
        );
        for (const path of paths) this.sql.exec("INSERT OR IGNORE INTO file_paths (file_hash, path) VALUES (?, ?)", hash, path);
        for (const tag of tags) this.sql.exec("INSERT OR IGNORE INTO file_tags (file_hash, tag) VALUES (?, ?)", hash, tag);
        const retryCount = Math.max(0, Math.floor(Number(item.retry_count || 0)));
        const maxRetries = Math.max(retryCount, Math.floor(Number(item.max_retries || this.getConfig().max_retries || 3)));
        this.sql.exec(
          `INSERT INTO jobs (file_hash, status, priority, retry_count, max_retries, created_at, updated_at, completed_at, available_at, error_message)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(file_hash) DO UPDATE SET status=excluded.status, priority=excluded.priority,
             retry_count=excluded.retry_count, max_retries=excluded.max_retries, updated_at=excluded.updated_at,
             completed_at=excluded.completed_at, available_at=excluded.available_at, error_message=excluded.error_message,
             worker_id=NULL, lease_token=NULL, lease_expires_at=NULL, progress_json='{}'`,
          hash,
          status,
          priority,
          retryCount,
          maxRetries,
          timestamp,
          timestamp,
          status === "done" ? timestamp : null,
          timestamp,
          typeof item.error === "string" ? item.error : null,
        );
        imported += 1;
      }
    });
    this.audit("migration-api", "import", null, { imported });
    return json({ ok: true, imported });
  }

  private async workerApi(request: Request, url: URL): Promise<Response> {
    if (!(await this.workerAuthorized(request))) return error("Unauthorized", 401);
    if (url.pathname === "/api/v1/config" && request.method === "GET") return json(this.getConfig());
    if (url.pathname === "/api/v1/snapshot" && request.method === "GET") return json(this.snapshot(false));
    if (url.pathname === "/api/v1/workers/register" && request.method === "POST") return this.registerWorker(request);
    if (url.pathname === "/api/v1/workers/heartbeat" && request.method === "POST") return this.workerHeartbeat(request);
    if (url.pathname === "/api/v1/workers/deregister" && request.method === "POST") return this.deregisterWorker(request);
    if (url.pathname === "/api/v1/jobs/claim" && request.method === "POST") return this.claim(request);
    const match = url.pathname.match(/^\/api\/v1\/jobs\/([a-f0-9]{64})(?:\/(heartbeat|complete|fail|release))?$/);
    if (!match) return error("Not found", 404);
    const hash = match[1]!;
    const action = match[2];
    if (!action && request.method === "PUT") return this.enqueue(request, hash);
    if (!action && request.method === "GET") return this.getJob(hash);
    if (request.method !== "POST") return error("Method not allowed", 405);
    if (action === "heartbeat") return this.jobHeartbeat(request, hash);
    if (action === "complete") return this.complete(request, hash);
    if (action === "fail") return this.fail(request, hash);
    if (action === "release") return this.release(request, hash);
    return error("Not found", 404);
  }

  private async adminApi(request: Request, url: URL): Promise<Response> {
    const session = await this.session(request);
    if (!session) return error("Unauthorized", 401);
    if (!this.sameOrigin(request)) return error("Invalid origin", 403);
    if (url.pathname === "/api/v1/admin/snapshot" && request.method === "GET") return json(this.snapshot());
    if (url.pathname === "/api/v1/admin/config" && request.method === "PUT") return this.updateConfig(request, session.me);
    if (url.pathname === "/api/v1/admin/recover" && request.method === "POST") {
      const recovered = this.recoverExpiredLeases();
      this.audit(session.me, "recover", null, { recovered });
      return json({ recovered });
    }
    const match = url.pathname.match(/^\/api\/v1\/admin\/jobs\/([a-f0-9]{64})\/(retry|cancel|priority)$/);
    if (!match || request.method !== "POST") return error("Not found", 404);
    const hash = match[1]!;
    const action = match[2]!;
    if (action === "retry") return this.adminRetry(hash, session.me);
    if (action === "cancel") return this.adminCancel(hash, session.me);
    return this.adminPriority(request, hash, session.me);
  }

  private async body(request: Request): Promise<Record<string, unknown>> {
    if (!(request.headers.get("content-type") || "").includes("application/json")) throw new Error("Expected application/json");
    const value = await request.json();
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Expected a JSON object");
    return value as Record<string, unknown>;
  }

  private async enqueue(request: Request, hash: string): Promise<Response> {
    if (!validHash(hash)) return error("Invalid hash");
    const body = await this.body(request);
    const priority = body.priority ?? "3_normal";
    if (!validPriority(priority)) return error("Invalid priority");
    const timestamp = now();
    const paths = Array.isArray(body.paths) ? body.paths.filter((v): v is string => typeof v === "string") : [];
    const tags = Array.isArray(body.tags) ? body.tags.filter((v): v is string => typeof v === "string") : [];
    this.ctx.storage.transactionSync(() => {
      this.sql.exec(
        `INSERT INTO files (hash, original_name, size_bytes, source, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(hash) DO UPDATE SET
           original_name = COALESCE(excluded.original_name, files.original_name),
           size_bytes = CASE WHEN excluded.size_bytes > 0 THEN excluded.size_bytes ELSE files.size_bytes END,
           source = COALESCE(excluded.source, files.source), updated_at = excluded.updated_at`,
        hash,
        typeof body.original_name === "string" ? body.original_name : null,
        Number(body.size_bytes || 0),
        typeof body.source === "string" ? body.source : null,
        timestamp,
        timestamp,
      );
      for (const path of paths) this.sql.exec("INSERT OR IGNORE INTO file_paths (file_hash, path) VALUES (?, ?)", hash, path);
      for (const tag of tags) this.sql.exec("INSERT OR IGNORE INTO file_tags (file_hash, tag) VALUES (?, ?)", hash, tag);
      this.sql.exec(
        `INSERT INTO jobs (file_hash, status, priority, retry_count, max_retries, created_at, updated_at, available_at)
         VALUES (?, 'todo', ?, 0, ?, ?, ?, ?)
         ON CONFLICT(file_hash) DO UPDATE SET
           priority = CASE WHEN jobs.status IN ('todo','failed') THEN excluded.priority ELSE jobs.priority END,
           updated_at = excluded.updated_at`,
        hash,
        priority,
        Number(this.getConfig().max_retries || 3),
        timestamp,
        timestamp,
        timestamp,
      );
    });
    this.audit("worker-api", "enqueue", hash, { priority });
    return this.getJob(hash, 200);
  }

  private getJob(hash: string, status = 200): Response {
    const row = Array.from(this.sql.exec<JobRow>(
      `SELECT j.*, f.original_name, f.size_bytes FROM jobs j JOIN files f ON f.hash = j.file_hash WHERE j.file_hash = ?`,
      hash,
    ))[0];
    if (!row) return error("Job not found", 404);
    return json(this.jobJson(row), { status });
  }

  private async registerWorker(request: Request): Promise<Response> {
    const body = await this.body(request);
    const workerId = String(body.worker_id || "");
    if (!/^[A-Za-z0-9._:-]{1,128}$/.test(workerId)) return error("Invalid worker_id");
    const timestamp = now();
    this.sql.exec(
      `INSERT INTO workers (worker_id, hostname, status, current_job, last_heartbeat, registered_at, metadata_json, metrics_json)
       VALUES (?, ?, 'idle', NULL, ?, ?, ?, '{}')
       ON CONFLICT(worker_id) DO UPDATE SET hostname=excluded.hostname, status='idle', last_heartbeat=excluded.last_heartbeat, metadata_json=excluded.metadata_json`,
      workerId,
      String(body.hostname || workerId),
      timestamp,
      timestamp,
      JSON.stringify(body),
    );
    return json({ ok: true, worker_id: workerId });
  }

  private async workerHeartbeat(request: Request): Promise<Response> {
    const body = await this.body(request);
    const workerId = String(body.worker_id || "");
    const result = this.sql.exec(
      "UPDATE workers SET status=?, current_job=?, last_heartbeat=?, metrics_json=? WHERE worker_id=?",
      body.current_job ? "processing" : "idle",
      body.current_job ? String(body.current_job) : null,
      now(),
      JSON.stringify(body.metrics || {}),
      workerId,
    );
    return json({ ok: result.rowsWritten > 0 });
  }

  private async deregisterWorker(request: Request): Promise<Response> {
    const body = await this.body(request);
    this.sql.exec("UPDATE workers SET status='stopped', current_job=NULL, last_heartbeat=? WHERE worker_id=?", now(), String(body.worker_id || ""));
    return json({ ok: true });
  }

  private async claim(request: Request): Promise<Response> {
    const body = await this.body(request);
    const workerId = String(body.worker_id || "");
    if (!/^[A-Za-z0-9._:-]{1,128}$/.test(workerId)) return error("Invalid worker_id");
    const allowed = Array.isArray(body.priorities)
      ? body.priorities.filter(validPriority)
      : [...PRIORITIES];
    if (!allowed.length) return error("No valid priorities");
    const timestamp = now();
    const leaseSeconds = Math.max(60, Number(this.getConfig().lease_seconds || this.env.LEASE_SECONDS || 900));
    const leaseToken = randomToken();
    let claimed: JobRow | undefined;
    this.ctx.storage.transactionSync(() => {
      this.recoverExpiredLeases(timestamp);
      const existing = Array.from(this.sql.exec<JobRow>(
        `SELECT j.*, f.original_name, f.size_bytes FROM jobs j JOIN files f ON f.hash=j.file_hash
         WHERE j.status='processing' AND j.worker_id=? AND j.lease_expires_at>=? LIMIT 1`,
        workerId,
        timestamp,
      ))[0];
      if (existing) {
        claimed = existing;
        return;
      }
      const placeholders = allowed.map(() => "?").join(",");
      const row = Array.from(this.sql.exec<JobRow>(
        `SELECT j.*, f.original_name, f.size_bytes FROM jobs j JOIN files f ON f.hash=j.file_hash
         WHERE j.status IN ('todo','failed') AND j.available_at <= ? AND j.priority IN (${placeholders})
         ORDER BY CASE j.priority WHEN '1_critical' THEN 1 WHEN '2_high' THEN 2 WHEN '3_normal' THEN 3 WHEN '4_low' THEN 4 ELSE 5 END, j.created_at
         LIMIT 1`,
        timestamp,
        ...allowed,
      ))[0];
      if (!row) return;
      this.sql.exec(
        `UPDATE jobs SET status='processing', worker_id=?, lease_token=?, lease_expires_at=?, started_at=COALESCE(started_at, ?), updated_at=?, progress_json='{}' WHERE file_hash=?`,
        workerId,
        leaseToken,
        timestamp + leaseSeconds * 1000,
        timestamp,
        timestamp,
        row.file_hash,
      );
      this.sql.exec("UPDATE workers SET status='processing', current_job=?, last_heartbeat=? WHERE worker_id=?", row.file_hash, timestamp, workerId);
      claimed = { ...row, status: "processing", worker_id: workerId, lease_token: leaseToken, lease_expires_at: timestamp + leaseSeconds * 1000 };
    });
    if (!claimed) return new Response(null, { status: 204 });
    await this.scheduleAlarmAt(claimed.lease_expires_at!);
    this.audit(workerId, "claim", claimed.file_hash, {});
    return json(this.jobJson(claimed));
  }

  private async jobHeartbeat(request: Request, hash: string): Promise<Response> {
    const body = await this.body(request);
    const workerId = String(body.worker_id || "");
    const leaseToken = String(body.lease_token || "");
    const timestamp = now();
    const leaseSeconds = Math.max(60, Number(this.getConfig().lease_seconds || this.env.LEASE_SECONDS || 900));
    const result = this.sql.exec(
      `UPDATE jobs SET lease_expires_at=?, updated_at=?, progress_json=?
       WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=? AND lease_expires_at>=?`,
      timestamp + leaseSeconds * 1000,
      timestamp,
      JSON.stringify(body.progress || {}),
      hash,
      workerId,
      leaseToken,
      timestamp,
    );
    if (!result.rowsWritten) return error("Lease is no longer valid", 409);
    this.sql.exec("UPDATE workers SET status='processing', current_job=?, last_heartbeat=?, metrics_json=? WHERE worker_id=?", hash, timestamp, JSON.stringify(body.metrics || {}), workerId);
    await this.scheduleAlarmAt(timestamp + leaseSeconds * 1000);
    return json({ ok: true, lease_expires_at: timestamp + leaseSeconds * 1000 });
  }

  private async complete(request: Request, hash: string): Promise<Response> {
    const body = await this.body(request);
    const timestamp = now();
    const workerId = String(body.worker_id || "");
    const current = Array.from(this.sql.exec<{ status: string }>("SELECT status FROM jobs WHERE file_hash=?", hash))[0];
    if (current?.status === "done") return json({ ok: true, already_completed: true });
    const result = this.sql.exec(
      `UPDATE jobs SET status='done', completed_at=?, updated_at=?, lease_token=NULL, lease_expires_at=NULL, worker_id=NULL, progress_json=?
       WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=?`,
      timestamp,
      timestamp,
      JSON.stringify(body.result || {}),
      hash,
      workerId,
      String(body.lease_token || ""),
    );
    if (!result.rowsWritten) return error("Lease is no longer valid", 409);
    this.sql.exec("UPDATE workers SET status='idle', current_job=NULL, last_heartbeat=?, metrics_json=? WHERE worker_id=?", timestamp, JSON.stringify(body.metrics || {}), workerId);
    this.audit(workerId, "complete", hash, body.result && typeof body.result === "object" ? body.result as Record<string, Json> : {});
    return json({ ok: true });
  }

  private async fail(request: Request, hash: string): Promise<Response> {
    const body = await this.body(request);
    const timestamp = now();
    const workerId = String(body.worker_id || "");
    const row = Array.from(this.sql.exec<JobRow>(
      "SELECT * FROM jobs WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=?",
      hash,
      workerId,
      String(body.lease_token || ""),
    ))[0];
    if (!row) {
      const current = Array.from(this.sql.exec<{ status: string; retry_count: number }>("SELECT status, retry_count FROM jobs WHERE file_hash=?", hash))[0];
      if (current && ["failed", "dead"].includes(current.status)) return json({ ok: true, already_failed: true, status: current.status, retry_count: current.retry_count });
      return error("Lease is no longer valid", 409);
    }
    const retries = row.retry_count + 1;
    const terminal = retries > row.max_retries;
    const delaySeconds = Math.min(3600, 60 * 2 ** Math.max(0, retries - 1));
    this.sql.exec(
      `UPDATE jobs SET status=?, retry_count=?, available_at=?, error_message=?, updated_at=?, worker_id=NULL, lease_token=NULL, lease_expires_at=NULL, progress_json='{}' WHERE file_hash=?`,
      terminal ? "dead" : "failed",
      retries,
      terminal ? timestamp : timestamp + delaySeconds * 1000,
      String(body.error || "Unknown worker error").slice(0, 4000),
      timestamp,
      hash,
    );
    this.sql.exec("UPDATE workers SET status='idle', current_job=NULL, last_heartbeat=?, metrics_json=? WHERE worker_id=?", timestamp, JSON.stringify(body.metrics || {}), workerId);
    this.sql.exec("INSERT INTO job_logs (file_hash, level, message, detail_json, created_at) VALUES (?, 'error', ?, ?, ?)", hash, String(body.error || "Unknown worker error").slice(0, 4000), JSON.stringify({ traceback: body.traceback || null, context: body.context || {} }), timestamp);
    this.audit(workerId, terminal ? "dead" : "fail", hash, { retries });
    return json({ ok: true, status: terminal ? "dead" : "failed", retry_count: retries, available_at: terminal ? null : timestamp + delaySeconds * 1000 });
  }

  private async release(request: Request, hash: string): Promise<Response> {
    const body = await this.body(request);
    const workerId = String(body.worker_id || "");
    const result = this.sql.exec(
      `UPDATE jobs SET status='todo', available_at=?, updated_at=?, worker_id=NULL, lease_token=NULL, lease_expires_at=NULL, progress_json='{}'
       WHERE file_hash=? AND status='processing' AND worker_id=? AND lease_token=?`,
      now(),
      now(),
      hash,
      workerId,
      String(body.lease_token || ""),
    );
    if (!result.rowsWritten) {
      const current = Array.from(this.sql.exec<{ status: string }>("SELECT status FROM jobs WHERE file_hash=?", hash))[0];
      if (current?.status === "todo") return json({ ok: true, already_released: true });
      return error("Lease is no longer valid", 409);
    }
    this.sql.exec("UPDATE workers SET status='idle', current_job=NULL, last_heartbeat=? WHERE worker_id=?", now(), workerId);
    this.audit(workerId, "release", hash, { reason: String(body.reason || "worker_release") });
    return json({ ok: true });
  }

  private adminRetry(hash: string, actor: string): Response {
    const result = this.sql.exec(
      `UPDATE jobs SET status='todo', retry_count=0, available_at=?, error_message=NULL, updated_at=?, worker_id=NULL, lease_token=NULL, lease_expires_at=NULL WHERE file_hash=? AND status IN ('failed','dead')`,
      now(), now(), hash,
    );
    if (!result.rowsWritten) return error("Job is not failed or dead", 409);
    this.audit(actor, "retry", hash, {});
    return json({ ok: true });
  }

  private adminCancel(hash: string, actor: string): Response {
    const result = this.sql.exec(
      `UPDATE jobs SET status='todo', available_at=?, updated_at=?, worker_id=NULL, lease_token=NULL, lease_expires_at=NULL, progress_json='{}' WHERE file_hash=? AND status='processing'`,
      now(), now(), hash,
    );
    if (!result.rowsWritten) return error("Job is not processing", 409);
    this.sql.exec("UPDATE workers SET status='idle', current_job=NULL, last_heartbeat=? WHERE current_job=?", now(), hash);
    this.audit(actor, "cancel", hash, {});
    return json({ ok: true });
  }

  private async adminPriority(request: Request, hash: string, actor: string): Promise<Response> {
    const body = await this.body(request);
    if (!validPriority(body.priority)) return error("Invalid priority");
    const result = this.sql.exec("UPDATE jobs SET priority=?, updated_at=? WHERE file_hash=? AND status IN ('todo','failed')", body.priority, now(), hash);
    if (!result.rowsWritten) return error("Only queued jobs can be reprioritized", 409);
    this.audit(actor, "priority", hash, { priority: body.priority });
    return json({ ok: true });
  }

  private async updateConfig(request: Request, actor: string): Promise<Response> {
    const body = await this.body(request);
    const allowed = new Set(Object.keys(DEFAULT_CONFIG));
    for (const [key, value] of Object.entries(body)) {
      if (!allowed.has(key) || typeof value !== "number" || !Number.isFinite(value) || value < 0) return error(`Invalid config value: ${key}`);
      this.sql.exec("INSERT INTO config (key, value_json, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at", key, JSON.stringify(value), now());
    }
    this.audit(actor, "config", null, body as Record<string, Json>);
    return json(this.getConfig());
  }

  private getConfig(): Record<string, Json> {
    const result: Record<string, Json> = {};
    for (const row of this.sql.exec<{ key: string; value_json: string }>("SELECT key, value_json FROM config")) {
      try { result[row.key] = JSON.parse(row.value_json) as Json; } catch { result[row.key] = row.value_json; }
    }
    return result;
  }

  private recoverExpiredLeases(timestamp = now()): number {
    const expired = Array.from(this.sql.exec<JobRow>("SELECT * FROM jobs WHERE status='processing' AND lease_expires_at < ?", timestamp));
    for (const row of expired) {
      const retries = row.retry_count + 1;
      const terminal = retries > row.max_retries;
      this.sql.exec(
        `UPDATE jobs SET status=?, retry_count=?, available_at=?, error_message='Worker lease expired', updated_at=?, worker_id=NULL, lease_token=NULL, lease_expires_at=NULL, progress_json='{}' WHERE file_hash=? AND status='processing' AND lease_expires_at < ?`,
        terminal ? "dead" : "failed", retries, timestamp, timestamp, row.file_hash, timestamp,
      );
      if (row.worker_id) this.sql.exec("UPDATE workers SET status='stale', current_job=NULL WHERE worker_id=?", row.worker_id);
      this.audit("system", terminal ? "lease_dead" : "lease_expired", row.file_hash, { retries });
    }
    return expired.length;
  }

  private async scheduleAlarmAt(timestamp: number): Promise<void> {
    const current = await this.ctx.storage.getAlarm();
    if (current === null || timestamp < current) await this.ctx.storage.setAlarm(timestamp);
  }

  private async scheduleNextAlarm(): Promise<void> {
    const row = Array.from(this.sql.exec<{ lease_expires_at: number }>("SELECT MIN(lease_expires_at) AS lease_expires_at FROM jobs WHERE status='processing'"))[0];
    if (row?.lease_expires_at) await this.ctx.storage.setAlarm(Math.max(now() + 1000, row.lease_expires_at));
  }

  private snapshot(includeLeases = true): Record<string, Json> {
    const counts: Record<string, number> = { todo: 0, processing: 0, failed: 0, dead: 0, done: 0 };
    for (const row of this.sql.exec<{ status: string; count: number }>("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status")) counts[row.status] = row.count;
    const priority: Record<string, number> = Object.fromEntries(PRIORITIES.map((p) => [p, 0]));
    for (const row of this.sql.exec<{ priority: string; count: number }>("SELECT priority, COUNT(*) AS count FROM jobs WHERE status IN ('todo','failed') GROUP BY priority")) priority[row.priority] = row.count;
    const jobs = Array.from(this.sql.exec<JobRow>(
      `SELECT j.*, f.original_name, f.size_bytes FROM jobs j JOIN files f ON f.hash=j.file_hash
       ORDER BY CASE j.status WHEN 'processing' THEN 1 WHEN 'dead' THEN 2 WHEN 'failed' THEN 3 WHEN 'todo' THEN 4 ELSE 5 END, j.updated_at DESC LIMIT 250`,
    )).map((row) => this.jobJson(row, includeLeases));
    const workers = Array.from(this.sql.exec<WorkerRow>("SELECT * FROM workers ORDER BY last_heartbeat DESC LIMIT 100")).map((row) => ({
      ...row,
      metadata: parseJsonObject(row.metadata_json),
      metrics: parseJsonObject(row.metrics_json),
      metadata_json: undefined,
      metrics_json: undefined,
    }));
    return { counts, priority, jobs, workers, config: this.getConfig(), server_time: now() } as unknown as Record<string, Json>;
  }

  private jobJson(row: JobRow, includeLease = true): Record<string, Json> {
    return {
      hash: row.file_hash,
      status: row.status,
      priority: row.priority,
      retry_count: row.retry_count,
      max_retries: row.max_retries,
      worker_id: row.worker_id,
      lease_token: includeLease ? row.lease_token : null,
      lease_expires_at: row.lease_expires_at,
      created_at: row.created_at,
      updated_at: row.updated_at,
      started_at: row.started_at,
      completed_at: row.completed_at,
      available_at: row.available_at,
      error_message: row.error_message,
      progress: parseJsonObject(row.progress_json),
      original_name: row.original_name,
      size_bytes: row.size_bytes,
    };
  }

  private audit(actor: string, action: string, subject: string | null, detail: Record<string, Json>): void {
    this.sql.exec("INSERT INTO audit_log (actor, action, subject, detail_json, created_at) VALUES (?, ?, ?, ?, ?)", actor, action, subject, JSON.stringify(detail), now());
  }
}
