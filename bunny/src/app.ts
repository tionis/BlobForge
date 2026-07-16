import { CoordinatorDatabase, PRIORITIES, type ImportItem, type JobRecord, type Priority } from "./database";
import { APP_CSS, APP_JS, renderHome } from "./ui";

export interface AppConfig {
  workerApiToken: string;
  migrationApiToken?: string;
  adminMe?: string;
  sessionTtlSeconds?: number;
  leaseSeconds?: number;
}

function json(data: unknown, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers);
  headers.set("content-type", "application/json; charset=utf-8");
  headers.set("cache-control", "no-store");
  return new Response(JSON.stringify(data), { ...init, headers });
}

function error(message: string, status = 400): Response {
  return json({ error: message }, { status });
}

function html(body: string, status = 200): Response {
  return new Response(body, { status, headers: {
    "content-type": "text/html; charset=utf-8", "cache-control": "no-store",
    "content-security-policy": "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
    "referrer-policy": "no-referrer", "x-content-type-options": "nosniff",
  } });
}

function canonicalUrl(value: string): string {
  const url = new URL(value);
  url.hash = "";
  if (!url.pathname) url.pathname = "/";
  return url.toString();
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

function cookieName(url: URL): string {
  return url.protocol === "https:" ? "__Host-blobforge_session" : "blobforge_session";
}

function sessionCookie(url: URL, token: string, maxAge: number): string {
  return `${cookieName(url)}=${token}; Path=/; HttpOnly; SameSite=Lax${url.protocol === "https:" ? "; Secure" : ""}; Max-Age=${maxAge}`;
}

function clearSessionCookie(url: URL): string {
  return `${cookieName(url)}=; Path=/; HttpOnly; SameSite=Lax${url.protocol === "https:" ? "; Secure" : ""}; Max-Age=0`;
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
      if (url.pathname === "/app.css") return new Response(APP_CSS, { headers: { "content-type": "text/css; charset=utf-8", "cache-control": "public, max-age=3600" } });
      if (url.pathname === "/app.js") return new Response(APP_JS, { headers: { "content-type": "text/javascript; charset=utf-8", "cache-control": "public, max-age=3600" } });
      if (url.pathname === "/client-metadata.json") return this.clientMetadata(url);
      if (url.pathname === "/auth/login" && request.method === "GET") return this.login(request);
      if (url.pathname === "/auth/callback" && request.method === "GET") return this.callback(request);
      if (url.pathname === "/auth/logout" && request.method === "POST") return this.logout(request);
      if (url.pathname === "/api/v1/health" && request.method === "GET") return json({ ok: true, service: "blobforge-bunny-coordinator", database: "connected" });
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

  private adminMe(): string { return canonicalUrl(this.config.adminMe || "https://eric.wendland.dev/"); }
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

  private sameOrigin(request: Request): boolean {
    if (["GET", "HEAD", "OPTIONS"].includes(request.method)) return true;
    return request.headers.get("origin") === new URL(request.url).origin;
  }

  private async workerApi(request: Request, url: URL): Promise<Response> {
    if (!(await this.bearerAuthorized(request, this.config.workerApiToken))) return error("Unauthorized", 401);
    if (url.pathname === "/api/v1/config" && request.method === "GET") return json(await this.db.getConfig());
    if (url.pathname === "/api/v1/snapshot" && request.method === "GET") return json(await this.db.snapshot(false));
    if (url.pathname === "/api/v1/workers/register" && request.method === "POST") {
      const body = await this.body(request); const workerId = String(body.worker_id || "");
      if (!/^[A-Za-z0-9._:-]{1,128}$/.test(workerId)) return error("Invalid worker_id");
      await this.db.registerWorker(body); return json({ ok: true, worker_id: workerId });
    }
    if (url.pathname === "/api/v1/workers/heartbeat" && request.method === "POST") return json({ ok: await this.db.workerHeartbeat(await this.body(request)) });
    if (url.pathname === "/api/v1/workers/deregister" && request.method === "POST") { const body = await this.body(request); await this.db.deregisterWorker(String(body.worker_id || "")); return json({ ok: true }); }
    if (url.pathname === "/api/v1/jobs/claim" && request.method === "POST") {
      const body = await this.body(request); const workerId = String(body.worker_id || "");
      if (!/^[A-Za-z0-9._:-]{1,128}$/.test(workerId)) return error("Invalid worker_id");
      const priorities = Array.isArray(body.priorities) ? body.priorities.filter(validPriority) : [...PRIORITIES];
      if (!priorities.length) return error("No valid priorities");
      const runtime = await this.db.getConfig();
      const job = await this.db.claim(workerId, priorities, randomToken(), this.leaseSeconds(runtime));
      return job ? json(jobJson(job)) : new Response(null, { status: 204 });
    }
    const match = url.pathname.match(/^\/api\/v1\/jobs\/([a-f0-9]{64})(?:\/(heartbeat|complete|fail|release))?$/);
    if (!match) return error("Not found", 404);
    const hash = match[1]!; const action = match[2];
    if (!action && request.method === "PUT") {
      const body = await this.body(request);
      if (!validPriority(body.priority ?? "3_normal")) return error("Invalid priority");
      return json(jobJson(await this.db.enqueue(hash, body)));
    }
    if (!action && request.method === "GET") { const job = await this.db.getJob(hash); return job ? json(jobJson(job)) : error("Job not found", 404); }
    if (request.method !== "POST") return error("Method not allowed", 405);
    const body = await this.body(request); const workerId = String(body.worker_id || ""); const leaseToken = String(body.lease_token || "");
    if (action === "heartbeat") {
      const ok = await this.db.jobHeartbeat(hash, workerId, leaseToken, body.progress, body.metrics, this.leaseSeconds(await this.db.getConfig()));
      return ok ? json({ ok: true }) : error("Lease is no longer valid", 409);
    }
    if (action === "complete") {
      const outcome = await this.db.complete(hash, workerId, leaseToken, body.result, body.metrics);
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

  private async migrationImport(request: Request): Promise<Response> {
    if (!(await this.bearerAuthorized(request, this.config.migrationApiToken))) return error("Unauthorized", 401);
    const items = (await this.body(request)).items;
    if (!Array.isArray(items) || items.length < 1 || items.length > 250) return error("items must contain between 1 and 250 records");
    for (const item of items) {
      if (!item || typeof item !== "object" || Array.isArray(item)) return error("Invalid migration record");
      const record = item as Record<string, unknown>;
      if (!validHash(String(record.hash || "")) || !validPriority(record.priority || "3_normal")) return error("Invalid migration record");
      if (record.status && !["todo", "processing", "failed", "dead", "done"].includes(String(record.status))) return error("Invalid migration status");
    }
    return json({ ok: true, imported: await this.db.importItems(items as ImportItem[]) });
  }

  private async adminApi(request: Request, url: URL): Promise<Response> {
    const session = await this.session(request);
    if (!session) return error("Unauthorized", 401);
    if (!this.sameOrigin(request)) return error("Invalid origin", 403);
    if (url.pathname === "/api/v1/admin/snapshot" && request.method === "GET") return json(await this.db.snapshot());
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
    const url = new URL(request.url); const me = this.adminMe(); const discovered = await discoverIndieAuth(me);
    const state = randomToken(); const verifier = randomToken(48); const challenge = await sha256(verifier);
    await this.db.saveAuthAttempt(state, verifier, discovered.token, discovered.issuer, Date.now() + 600_000);
    const auth = new URL(discovered.authorization);
    for (const [key, value] of Object.entries({ response_type: "code", client_id: `${url.origin}/client-metadata.json`, redirect_uri: `${url.origin}/auth/callback`, state, code_challenge: challenge, code_challenge_method: "S256", scope: "profile", me })) auth.searchParams.set(key, value);
    return Response.redirect(auth.toString(), 302);
  }

  private async callback(request: Request): Promise<Response> {
    const url = new URL(request.url); const state = url.searchParams.get("state") || ""; const code = url.searchParams.get("code") || "";
    if (!state || !code) return error(url.searchParams.get("error") || "Missing IndieAuth response", 400);
    const attempt = await this.db.takeAuthAttempt(state);
    if (!attempt || Number(attempt.expires_at) < Date.now()) return error("Expired or invalid IndieAuth state", 400);
    const form = new URLSearchParams({ grant_type: "authorization_code", code, client_id: `${url.origin}/client-metadata.json`, redirect_uri: `${url.origin}/auth/callback`, code_verifier: String(attempt.verifier) });
    const tokenResponse = await fetch(String(attempt.token_endpoint), { method: "POST", headers: { "content-type": "application/x-www-form-urlencoded", accept: "application/json" }, body: form });
    const result: Record<string, unknown> = (tokenResponse.headers.get("content-type") || "").includes("json") ? await tokenResponse.json() as Record<string, unknown> : Object.fromEntries(new URLSearchParams(await tokenResponse.text()));
    if (!tokenResponse.ok) return error(String(result.error_description || result.error || "IndieAuth exchange failed"), 401);
    if (typeof result.me !== "string" || canonicalUrl(result.me) !== this.adminMe()) return error("Authenticated identity is not a BlobForge administrator", 403);
    const token = randomToken(); const ttl = Math.max(300, this.config.sessionTtlSeconds || 43_200);
    await this.db.createSession(await sha256(token), canonicalUrl(result.me), ttl);
    await this.db.audit(canonicalUrl(result.me), "login", canonicalUrl(result.me), {});
    return new Response(null, { status: 302, headers: { location: "/", "set-cookie": sessionCookie(url, token, ttl) } });
  }

  private async logout(request: Request): Promise<Response> {
    if (!this.sameOrigin(request)) return error("Invalid origin", 403);
    const url = new URL(request.url); const token = parseCookies(request).get(cookieName(url));
    if (token) await this.db.deleteSession(await sha256(token));
    return new Response(null, { status: 302, headers: { location: "/", "set-cookie": clearSessionCookie(url) } });
  }

  private async session(request: Request): Promise<{ me: string } | null> {
    const url = new URL(request.url); const token = parseCookies(request).get(cookieName(url));
    if (!token) return null;
    const session = await this.db.getSession(await sha256(token));
    return session && session.expires_at >= Date.now() ? { me: session.me } : null;
  }
}
