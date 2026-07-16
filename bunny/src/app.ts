import { CoordinatorDatabase, PRIORITIES, type JobRecord, type JobStatus, type Priority } from "./database";
import type { ObjectTransferStore } from "./object_store";
import { APP_CSS, LOGIN_JS, VIEWER_CSS, renderHome } from "./ui";
import { APP_JS } from "./management_ui";
import { MARKDOWN_JS } from "./generated/markdown_bundle";

export interface AppConfig {
  clientApiToken: string;
  sessionSigningSecret: string;
  adminMes?: string[];
  adminMe?: string;
  sessionTtlSeconds?: number;
  leaseSeconds?: number;
  objectStore: ObjectTransferStore;
}

function json(data: unknown, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers);
  headers.set("content-type", "application/json; charset=utf-8");
  setPrivateNoCache(headers);
  return new Response(JSON.stringify(data), { ...init, headers });
}

function error(message: string, status = 400): Response {
  return json({ error: message }, { status });
}

function html(body: string, status = 200): Response {
  const headers = new Headers({
    "content-type": "text/html; charset=utf-8",
    "content-security-policy": "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self' https:; img-src 'self' data: blob:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
    "referrer-policy": "no-referrer", "x-content-type-options": "nosniff",
  });
  setPrivateNoCache(headers);
  headers.set("vary", "Cookie");
  return new Response(body, { status, headers });
}

function setPrivateNoCache(headers: Headers): void {
  headers.set("cache-control", "private, no-store, no-cache, max-age=0, must-revalidate");
  headers.set("cdn-cache-control", "no-store");
  headers.set("surrogate-control", "no-store");
  headers.set("pragma", "no-cache");
}

function canonicalUrl(value: string): string {
  const url = new URL(value);
  url.hash = "";
  if (!url.pathname) url.pathname = "/";
  return url.toString();
}

export function normalizeProfileUrl(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) throw new Error("Enter your IndieAuth profile URL");
  const withProtocol = /^[a-z][a-z0-9+.-]*:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`;
  const normalized = canonicalUrl(withProtocol);
  if (new URL(normalized).protocol !== "https:") throw new Error("IndieAuth profile URLs must use HTTPS");
  return normalized;
}

function validHash(value: string): boolean {
  return /^[a-f0-9]{64}$/.test(value);
}

function validPriority(value: unknown): value is Priority {
  return typeof value === "string" && (PRIORITIES as readonly string[]).includes(value);
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

function decodeBase64url(value: string): Uint8Array {
  const padded = value.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - value.length % 4) % 4);
  const binary = atob(padded);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
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
  for (let index = 0; index < Math.max(a.length, b.length); index++) {
    diff |= (a.charCodeAt(index % a.length) || 0) ^ (b.charCodeAt(index % b.length) || 0);
  }
  return diff === 0;
}

const SESSION_COOKIE = "__Host-blobforge_session";

function clearSessionCookie(): string {
  return `${SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Secure; Max-Age=0`;
}

function linkRelations(htmlText: string, rel: string): string[] {
  const results: string[] = [];
  for (const match of htmlText.matchAll(/<link\s+[^>]*>/gi)) {
    const attrs = new Map<string, string>();
    for (const attr of match[0].matchAll(/([\w:-]+)\s*=\s*["']([^"']*)["']/g)) attrs.set(attr[1]!.toLowerCase(), attr[2]!);
    if ((attrs.get("rel") || "").split(/\s+/).includes(rel) && attrs.get("href")) results.push(attrs.get("href")!);
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
    const metadata = await metadataResponse.json() as Record<string, unknown>;
    const discovered = {
      authorization: String(metadata.authorization_endpoint || ""),
      token: String(metadata.token_endpoint || ""),
      issuer: String(metadata.issuer || ""),
    };
    if (!discovered.authorization || !discovered.token || !discovered.issuer) throw new Error("Incomplete IndieAuth metadata");
    for (const endpoint of Object.values(discovered)) if (new URL(endpoint).protocol !== "https:") throw new Error("IndieAuth endpoints must use HTTPS");
    return discovered;
  }
  const authorization = headerLink(response, "authorization_endpoint") || linkRelations(body, "authorization_endpoint")[0];
  const token = headerLink(response, "token_endpoint") || linkRelations(body, "token_endpoint")[0];
  if (!authorization || !token) throw new Error("No IndieAuth endpoints found on admin profile");
  const resolvedAuthorization = new URL(authorization, base);
  return { authorization: resolvedAuthorization.toString(), token: new URL(token, base).toString(), issuer: resolvedAuthorization.origin + "/" };
}

function jobJson(job: JobRecord, includeLease = true): Record<string, unknown> {
  return {
    hash: job.file_hash, status: job.status, priority: job.priority, retry_count: job.retry_count,
    max_retries: job.max_retries, worker_id: job.worker_id, lease_token: includeLease ? job.lease_token : null,
    lease_expires_at: job.lease_expires_at, created_at: job.created_at, updated_at: job.updated_at,
    started_at: job.started_at, completed_at: job.completed_at, available_at: job.available_at,
    error_message: job.error_message, progress: JSON.parse(job.progress_json || "{}"),
    original_name: job.original_name, size_bytes: job.size_bytes,
  };
}

export class BlobForgeApp {
  constructor(private readonly db: CoordinatorDatabase, private readonly config: AppConfig) {}

  async fetch(request: Request): Promise<Response> {
    try {
      await this.db.ensureSchema();
      const url = new URL(request.url);
      if (url.pathname === "/app.css") return new Response(`${APP_CSS}${VIEWER_CSS}`, { headers: { "content-type": "text/css; charset=utf-8", "cache-control": "public, max-age=3600" } });
      if (url.pathname === "/app.js") return new Response(APP_JS, { headers: { "content-type": "text/javascript; charset=utf-8", "cache-control": "public, max-age=3600" } });
      if (url.pathname === "/markdown.js") return new Response(MARKDOWN_JS, { headers: { "content-type": "text/javascript; charset=utf-8", "cache-control": "public, max-age=3600" } });
      if (url.pathname === "/login.js") return new Response(LOGIN_JS, { headers: { "content-type": "text/javascript; charset=utf-8", "cache-control": "public, max-age=3600" } });
      if (url.pathname === "/client-metadata.json") return this.clientMetadata(url);
      if (url.pathname === "/auth/login" && request.method === "GET") return this.login(request);
      if (url.pathname === "/auth/callback" && request.method === "GET") return this.callback(request);
      if (url.pathname === "/auth/logout" && request.method === "POST") return this.logout(request);
      if (url.pathname === "/auth/status" && request.method === "GET") return this.authStatus(request);
      if (url.pathname === "/api/v1/health" && request.method === "GET") return json({ ok: true, service: "blobforge-bunny-coordinator", database: "connected" });
      if (url.pathname.startsWith("/api/v1/admin/")) return this.adminApi(request, url);
      if (url.pathname.startsWith("/api/v1/")) return this.workerApi(request, url);
      if (url.pathname === "/console" && request.method === "GET") {
        return html(renderHome(true, "", `${url.origin}/auth/callback`));
      }
      if (url.pathname === "/" && request.method === "GET") {
        const session = await this.session(request);
        return html(renderHome(Boolean(session), session?.me || "", `${url.origin}/auth/callback`));
      }
      return error("Not found", 404);
    } catch (cause) {
      console.error(cause);
      return error(cause instanceof Error ? cause.message : "Internal error", 500);
    }
  }

  private allowedAdmins(): string[] {
    const configured = this.config.adminMes?.length ? this.config.adminMes : [this.config.adminMe || "https://eric.wendland.dev/"];
    return [...new Set(configured.map(normalizeProfileUrl))];
  }
  private isAdmin(me: string): boolean { return this.allowedAdmins().includes(me); }
  private leaseSeconds(runtime?: Record<string, unknown>): number { return Math.max(60, Number(runtime?.lease_seconds || this.config.leaseSeconds || 900)); }
  private clientMetadata(url: URL): Response { return json({ client_id: `${url.origin}/client-metadata.json`, client_name: "BlobForge", client_uri: `${url.origin}/`, redirect_uris: [`${url.origin}/auth/callback`] }); }

  private async body(request: Request): Promise<Record<string, unknown>> {
    if (!(request.headers.get("content-type") || "").includes("application/json")) throw new Error("Expected application/json");
    const value = await request.json();
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Expected a JSON object");
    return value as Record<string, unknown>;
  }

  private async bearerAuthorized(request: Request, expected?: string): Promise<boolean> {
    const auth = request.headers.get("authorization") || "";
    return Boolean(expected && auth.startsWith("Bearer ") && await secureEqual(auth.slice(7), expected));
  }

  private async workerIdentity(request: Request): Promise<string | null> {
    const authorization = request.headers.get("authorization") || "";
    if (!authorization.startsWith("Bearer ")) return null;
    return this.db.authenticateWorkerToken(await sha256(authorization.slice(7)));
  }

  private sameOrigin(request: Request): boolean {
    if (["GET", "HEAD", "OPTIONS"].includes(request.method)) return true;
    return request.headers.get("origin") === new URL(request.url).origin;
  }

  private async signature(payload: string): Promise<string> {
    const key = await crypto.subtle.importKey(
      "raw", new TextEncoder().encode(this.config.sessionSigningSecret),
      { name: "HMAC", hash: "SHA-256" }, false, ["sign"],
    );
    return base64url(new Uint8Array(await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload))));
  }

  private async signPayload(value: Record<string, unknown>): Promise<string> {
    const payload = base64url(new TextEncoder().encode(JSON.stringify(value)));
    return `${payload}.${await this.signature(payload)}`;
  }

  private async verifyPayload(value: string): Promise<Record<string, unknown> | null> {
    const parts = value.split(".");
    if (parts.length !== 2 || !(await secureEqual(parts[1]!, await this.signature(parts[0]!)))) return null;
    try {
      const parsed = JSON.parse(new TextDecoder().decode(decodeBase64url(parts[0]!)));
      return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : null;
    } catch { return null; }
  }

  private async workerApi(request: Request, url: URL): Promise<Response> {
    const clientAuthorized = await this.bearerAuthorized(request, this.config.clientApiToken);
    const workerId = clientAuthorized ? null : await this.workerIdentity(request);
    if (url.pathname === "/api/v1/config" && request.method === "GET") {
      return clientAuthorized || workerId ? json(await this.db.getConfig()) : error("Unauthorized", 401);
    }
    if (url.pathname === "/api/v1/snapshot" && request.method === "GET") {
      return clientAuthorized || workerId ? json(await this.db.snapshot(false)) : error("Unauthorized", 401);
    }
    const jobMatch = url.pathname.match(/^\/api\/v1\/jobs\/([a-f0-9]{64})(?:\/(heartbeat|complete|fail|release|upload-url))?$/);
    if (jobMatch && !jobMatch[2]) {
      if (!clientAuthorized) return error("Unauthorized", 401);
      const hash = jobMatch[1]!;
      if (request.method === "PUT") {
        const body = await this.body(request);
        if (!validPriority(body.priority ?? "3_normal")) return error("Invalid priority");
        return json(jobJson(await this.db.enqueue(hash, body)));
      }
      if (request.method === "GET") { const job = await this.db.getJob(hash); return job ? json(jobJson(job)) : error("Job not found", 404); }
      return error("Method not allowed", 405);
    }
    if (!workerId) return error("Unauthorized", 401);
    if (url.pathname === "/api/v1/workers/me" && request.method === "GET") return json({ worker_id: workerId });
    if (url.pathname === "/api/v1/workers/register" && request.method === "POST") {
      const body = await this.body(request);
      if (body.worker_id && body.worker_id !== workerId) return error("Worker token does not match worker_id", 403);
      await this.db.registerWorker({ ...body, worker_id: workerId }); return json({ ok: true, worker_id: workerId });
    }
    if (url.pathname === "/api/v1/workers/heartbeat" && request.method === "POST") {
      const body = await this.body(request); return json({ ok: await this.db.workerHeartbeat({ ...body, worker_id: workerId }) });
    }
    if (url.pathname === "/api/v1/workers/deregister" && request.method === "POST") { await this.db.deregisterWorker(workerId); return json({ ok: true }); }
    if (url.pathname === "/api/v1/jobs/claim" && request.method === "POST") {
      const body = await this.body(request);
      if (body.worker_id && body.worker_id !== workerId) return error("Worker token does not match worker_id", 403);
      const priorities = Array.isArray(body.priorities) ? body.priorities.filter(validPriority) : [...PRIORITIES];
      if (!priorities.length) return error("No valid priorities");
      const runtime = await this.db.getConfig();
      const job = await this.db.claim(workerId, priorities, randomToken(), this.leaseSeconds(runtime));
      if (!job) return new Response(null, { status: 204 });
      const [metadata, input, outputExists] = await Promise.all([
        this.db.getFileMetadata(job.file_hash), this.config.objectStore.download(job.file_hash), this.config.objectStore.outputExists(job.file_hash),
      ]);
      return json({ ...jobJson(job), ...metadata, input: { url: input.url, expires_at: input.expiresAt }, output_exists: outputExists });
    }
    if (!jobMatch) return error("Not found", 404);
    const hash = jobMatch[1]!; const action = jobMatch[2];
    if (request.method !== "POST") return error("Method not allowed", 405);
    const body = await this.body(request);
    if (body.worker_id && body.worker_id !== workerId) return error("Worker token does not match worker_id", 403);
    const leaseToken = String(body.lease_token || "");
    if (action === "upload-url") {
      if (!(await this.db.validLease(hash, workerId, leaseToken))) return error("Lease is no longer valid", 409);
      const upload = await this.config.objectStore.upload(hash);
      return json({ url: upload.url, method: "PUT", expires_at: upload.expiresAt, headers: { "content-type": "application/zip" } });
    }
    if (action === "heartbeat") {
      const ok = await this.db.jobHeartbeat(hash, workerId, leaseToken, body.progress, body.metrics, this.leaseSeconds(await this.db.getConfig()));
      return ok ? json({ ok: true }) : error("Lease is no longer valid", 409);
    }
    if (action === "complete") {
      const current = await this.db.getJob(hash);
      if (current?.status === "done") return json({ ok: true, already_completed: true });
      if (!(await this.db.validLease(hash, workerId, leaseToken))) return error("Lease is no longer valid", 409);
      if (!(await this.config.objectStore.outputExists(hash))) return error("Output object does not exist", 409);
      const result = body.result && typeof body.result === "object" && !Array.isArray(body.result) ? body.result as Record<string, unknown> : {};
      const outcome = await this.db.complete(hash, workerId, leaseToken, { ...result, output_key: this.config.objectStore.outputKey(hash) }, body.metrics);
      return outcome === "conflict" ? error("Lease is no longer valid", 409) : json({ ok: true, already_completed: outcome === "done" });
    }
    if (action === "fail") {
      const job = await this.db.fail(hash, workerId, leaseToken, String(body.error || "Unknown worker error").slice(0, 4000), { traceback: body.traceback || null, context: body.context || {} }, body.metrics);
      if (!job) { const current = await this.db.getJob(hash); return current && ["failed", "dead"].includes(current.status) ? json({ ok: true, already_failed: true, status: current.status, retry_count: current.retry_count }) : error("Lease is no longer valid", 409); }
      return json({ ok: true, status: job.status, retry_count: job.retry_count, available_at: job.status === "dead" ? null : job.available_at });
    }
    if (action === "release") {
      const outcome = await this.db.release(hash, workerId, leaseToken);
      return outcome === "conflict" ? error("Lease is no longer valid", 409) : json({ ok: true, already_released: outcome === "todo" });
    }
    return error("Not found", 404);
  }

  private async adminApi(request: Request, url: URL): Promise<Response> {
    const session = await this.session(request);
    if (!session) return error("Unauthorized", 401);
    if (!this.sameOrigin(request)) return error("Invalid origin", 403);
    if (url.pathname === "/api/v1/admin/snapshot" && request.method === "GET") {
      return json({ ...await this.db.snapshot(false, false), worker_enrollments: await this.db.listWorkerCredentials(), identity: session.me });
    }
    if (url.pathname === "/api/v1/admin/files" && request.method === "GET") {
      const status = url.searchParams.get("status") || undefined;
      const priority = url.searchParams.get("priority") || undefined;
      if (status && !["todo", "processing", "failed", "dead", "done"].includes(status)) return error("Invalid status");
      if (priority && !validPriority(priority)) return error("Invalid priority");
      return json(await this.db.listJobs({
        query: url.searchParams.get("q") || undefined,
        status: status as JobStatus | undefined,
        priority: priority as Priority | undefined,
        limit: Number(url.searchParams.get("limit") || 50),
        offset: Number(url.searchParams.get("offset") || 0),
      }));
    }
    if (url.pathname === "/api/v1/admin/uploads" && request.method === "POST") {
      const body = await this.body(request); const hash = String(body.hash || "");
      if (!validHash(hash)) return error("Invalid PDF hash");
      const transfer = await this.config.objectStore.rawUpload(hash);
      return json({ hash, url: transfer.url, method: "PUT", expires_at: transfer.expiresAt, already_exists: await this.config.objectStore.rawExists(hash), headers: { "content-type": "application/pdf" } });
    }
    const uploadComplete = url.pathname.match(/^\/api\/v1\/admin\/uploads\/([a-f0-9]{64})\/complete$/);
    if (uploadComplete && request.method === "POST") {
      const hash = uploadComplete[1]!; const body = await this.body(request);
      if (!(await this.config.objectStore.rawExists(hash))) return error("Uploaded PDF was not found", 409);
      if (!validPriority(body.priority ?? "3_normal")) return error("Invalid priority");
      const originalName = String(body.original_name || "").trim().slice(0, 500);
      if (!originalName.toLowerCase().endsWith(".pdf")) return error("A PDF filename is required");
      const job = await this.db.enqueue(hash, {
        original_name: originalName, size_bytes: Math.max(0, Number(body.size_bytes || 0)),
        paths: [String(body.path || originalName).slice(0, 2000)],
        tags: Array.isArray(body.tags) ? body.tags.filter((tag): tag is string => typeof tag === "string").slice(0, 50) : [],
        source: "management-ui", priority: body.priority || "3_normal",
      });
      await this.db.audit(session.me, "upload.ingest", hash, { original_name: originalName, size_bytes: body.size_bytes || 0 });
      return json(jobJson(job), { status: 201 });
    }
    const downloadMatch = url.pathname.match(/^\/api\/v1\/admin\/files\/([a-f0-9]{64})\/download$/);
    if (downloadMatch && request.method === "GET") {
      const hash = downloadMatch[1]!; const job = await this.db.getJob(hash);
      if (!job) return error("File not found", 404);
      const kind = url.searchParams.get("kind") || "raw";
      if (kind === "raw") {
        if (!(await this.config.objectStore.rawExists(hash))) return error("Source PDF not found", 404);
        return json({ kind, ...await this.config.objectStore.download(hash) });
      }
      if (kind === "output") {
        if (job.status !== "done" || !(await this.config.objectStore.outputExists(hash))) return error("Completed output not found", 404);
        return json({ kind, ...await this.config.objectStore.outputDownload(hash) });
      }
      return error("Invalid download kind");
    }
    if (url.pathname === "/api/v1/admin/backups" && request.method === "POST") {
      const backup = await this.db.exportBackup();
      const body = JSON.stringify(backup);
      const checksum = await sha256(body);
      const name = `${new Date().toISOString().replace(/[:.]/g, "-")}-${randomToken(6)}`;
      const stored = await this.config.objectStore.backup(name, body);
      const tables = backup.tables as Record<string, unknown[]>;
      const rows = Object.fromEntries(Object.entries(tables).map(([table, values]) => [table, values.length]));
      await this.db.audit(session.me, "backup.create", stored.key, { bytes: new TextEncoder().encode(body).byteLength, checksum });
      return json({ ok: true, key: stored.key, bytes: new TextEncoder().encode(body).byteLength, checksum_sha256_base64url: checksum, rows }, { status: 201 });
    }
    if (url.pathname === "/api/v1/admin/workers" && request.method === "POST") {
      const body = await this.body(request); const label = String(body.label || "").trim();
      if (!label || label.length > 80) return error("Worker label must contain 1 to 80 characters");
      const slug = label.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 48) || "worker";
      const workerId = `${slug}-${randomToken(6)}`; const token = `bfw_${randomToken(32)}`;
      await this.db.createWorkerCredential(workerId, label, await sha256(token), session.me);
      return json({ worker_id: workerId, label, token, coordinator_url: new URL(request.url).origin }, { status: 201 });
    }
    const workerMatch = url.pathname.match(/^\/api\/v1\/admin\/workers\/([A-Za-z0-9._:-]{1,128})\/revoke$/);
    if (workerMatch && request.method === "POST") {
      return await this.db.revokeWorkerCredential(workerMatch[1]!, session.me) ? json({ ok: true }) : error("Worker is already revoked or does not exist", 409);
    }
    if (url.pathname === "/api/v1/admin/config" && request.method === "PUT") {
      const body = await this.body(request);
      const allowed = new Set(["max_retries", "heartbeat_interval", "lease_seconds", "conversion_timeout"]);
      for (const [key, value] of Object.entries(body)) if (!allowed.has(key) || typeof value !== "number" || !Number.isFinite(value) || value < 0) return error(`Invalid config value: ${key}`);
      return json(await this.db.updateConfig(body, session.me));
    }
    if (url.pathname === "/api/v1/admin/recover" && request.method === "POST") { const recovered = await this.db.recoverExpiredLeases(); await this.db.audit(session.me, "recover", null, { recovered }); return json({ recovered }); }
    const match = url.pathname.match(/^\/api\/v1\/admin\/jobs\/([a-f0-9]{64})\/(retry|cancel|priority)$/);
    if (!match || request.method !== "POST") return error("Not found", 404);
    const hash = match[1]!; const action = match[2]!; let ok = false; let detail: Record<string, unknown> = {};
    if (action === "retry") ok = await this.db.adminRetry(hash);
    else if (action === "cancel") ok = await this.db.adminCancel(hash);
    else { const body = await this.body(request); if (!validPriority(body.priority)) return error("Invalid priority"); ok = await this.db.adminPriority(hash, body.priority); detail = { priority: body.priority }; }
    if (!ok) return error(action === "priority" ? "Only queued jobs can be reprioritized" : `Job cannot be ${action === "retry" ? "retried" : "cancelled"}`, 409);
    await this.db.audit(session.me, action, hash, detail);
    return json({ ok: true });
  }

  private async login(request: Request): Promise<Response> {
    const url = new URL(request.url);
    let me: string;
    try { me = normalizeProfileUrl(url.searchParams.get("me") || ""); }
    catch (cause) { return error(cause instanceof Error ? cause.message : "Invalid profile URL"); }
    if (!this.isAdmin(me)) return error("This IndieAuth identity is not a BlobForge administrator", 403);
    const discovered = await discoverIndieAuth(me);
    const verifier = randomToken(48); const challenge = await sha256(verifier);
    const state = await this.signPayload({ me, verifier, token_endpoint: discovered.token, issuer: discovered.issuer, exp: Date.now() + 600_000, nonce: randomToken() });
    const auth = new URL(discovered.authorization);
    for (const [key, value] of Object.entries({ response_type: "code", client_id: `${url.origin}/client-metadata.json`, redirect_uri: `${url.origin}/auth/callback`, state, code_challenge: challenge, code_challenge_method: "S256", scope: "profile", me })) auth.searchParams.set(key, value);
    return Response.redirect(auth.toString(), 302);
  }

  private async callback(request: Request): Promise<Response> {
    const url = new URL(request.url); const state = url.searchParams.get("state") || ""; const code = url.searchParams.get("code") || "";
    if (!state || !code) return error(url.searchParams.get("error") || "Missing IndieAuth response", 400);
    const attempt = await this.verifyPayload(state);
    if (!attempt || Number(attempt.exp) < Date.now() || typeof attempt.me !== "string" || !this.isAdmin(attempt.me)) return error("Expired or invalid IndieAuth state", 400);
    const form = new URLSearchParams({ grant_type: "authorization_code", code, client_id: `${url.origin}/client-metadata.json`, redirect_uri: `${url.origin}/auth/callback`, code_verifier: String(attempt.verifier) });
    const tokenResponse = await fetch(String(attempt.token_endpoint), { method: "POST", headers: { "content-type": "application/x-www-form-urlencoded", accept: "application/json" }, body: form });
    const result: Record<string, unknown> = (tokenResponse.headers.get("content-type") || "").includes("json") ? await tokenResponse.json() as Record<string, unknown> : Object.fromEntries(new URLSearchParams(await tokenResponse.text()));
    if (!tokenResponse.ok) return error(String(result.error_description || result.error || "IndieAuth exchange failed"), 401);
    if (typeof result.me !== "string") return error("IndieAuth response did not include an identity", 401);
    const returnedMe = normalizeProfileUrl(result.me);
    if (returnedMe !== attempt.me || !this.isAdmin(returnedMe)) return error("Authenticated identity is not a BlobForge administrator", 403);
    const ttl = Math.max(300, this.config.sessionTtlSeconds || 43_200);
    const token = await this.signPayload({ me: returnedMe, exp: Date.now() + ttl * 1000 });
    await this.db.audit(returnedMe, "login", returnedMe, {});
    const headers = new Headers({ location: `/console#session=${encodeURIComponent(token)}` });
    setPrivateNoCache(headers);
    return new Response(null, { status: 302, headers });
  }

  private async logout(request: Request): Promise<Response> {
    if (!this.sameOrigin(request)) return error("Invalid origin", 403);
    const headers = new Headers({ location: "/", "set-cookie": clearSessionCookie() });
    setPrivateNoCache(headers);
    return new Response(null, { status: 302, headers });
  }

  private async session(request: Request): Promise<{ me: string } | null> {
    const authorization = request.headers.get("authorization") || "";
    const token = authorization.startsWith("BlobForge-Session ")
      ? authorization.slice("BlobForge-Session ".length)
      : parseCookies(request).get(SESSION_COOKIE);
    if (!token) return null;
    const session = await this.verifyPayload(token);
    return session && typeof session.me === "string" && Number(session.exp) >= Date.now() && this.isAdmin(session.me) ? { me: session.me } : null;
  }

  private async authStatus(request: Request): Promise<Response> {
    const cookiePresent = parseCookies(request).has(SESSION_COOKIE);
    const sessionHeaderPresent = (request.headers.get("authorization") || "").startsWith("BlobForge-Session ");
    const session = await this.session(request);
    return json({
      authenticated: Boolean(session),
      cookie_present: cookiePresent,
      session_header_present: sessionHeaderPresent,
      identity: session?.me || null,
      request_protocol: new URL(request.url).protocol,
      forwarded_protocol: request.headers.get("x-forwarded-proto"),
    });
  }
}
