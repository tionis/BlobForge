import { createClient, type Client } from "@libsql/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { BlobForgeApp, normalizeProfileUrl, workerIdFromLabel } from "../src/app";
import { CoordinatorDatabase } from "../src/database";

let client: Client;
let app: BlobForgeApp;
let database: CoordinatorDatabase;
let outputExists = false;
let rawExists = false;
let backupBody = "";
const workerToken = "bfw_test-worker-token";
const workerHeaders = { authorization: `Bearer ${workerToken}`, "content-type": "application/json" };
const clientHeaders = { authorization: "Bearer client-secret", "content-type": "application/json" };

async function tokenHash(token: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(token));
  return btoa(String.fromCharCode(...new Uint8Array(digest))).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

beforeEach(async () => {
  client = createClient({ url: "file::memory:" });
  database = new CoordinatorDatabase(client);
  app = new BlobForgeApp(database, {
    clientApiToken: "client-secret",
    sessionSigningSecret: "session-secret-that-is-different-from-worker-secret",
    adminMes: ["https://eric.wendland.dev/", "https://alice.example/"],
    objectStore: {
      rawKey: (hash) => `pdf/store/raw/${hash}.pdf`,
      outputKey: (hash) => `pdf/store/out/${hash}.zip`,
      download: async (hash) => ({ url: `https://s3.example/raw/${hash}`, expiresAt: Date.now() + 3600_000 }),
      upload: async (hash) => ({ url: `https://s3.example/out/${hash}`, expiresAt: Date.now() + 900_000 }),
      outputExists: async () => outputExists,
      rawExists: async () => rawExists,
      rawUpload: async (hash) => ({ url: `https://s3.example/raw-upload/${hash}`, expiresAt: Date.now() + 900_000 }),
      outputDownload: async (hash) => ({ url: `https://s3.example/out-download/${hash}`, expiresAt: Date.now() + 3600_000 }),
      backup: async (name, body) => { backupBody = body; return { key: `pdf/backups/coordinator/${name}.json` }; },
    },
  });
  await database.ensureSchema();
  await database.createWorkerCredential("worker-1", "Test worker", await tokenHash(workerToken), "test-admin");
  outputExists = false;
  rawExists = false;
  backupBody = "";
});

afterEach(() => { vi.restoreAllMocks(); client.close(); });

function call(path: string, method = "GET", body?: unknown, headers?: Record<string, string>): Promise<Response> {
  const workerRoute = path.includes("/workers/") || path.endsWith("/jobs/claim") || /\/(heartbeat|complete|fail|release|upload-url)$/.test(path);
  return app.fetch(new Request(`https://blobforge.example${path}`, {
    method, headers: headers || (workerRoute ? workerHeaders : clientHeaders), body: body === undefined ? undefined : JSON.stringify(body),
  }));
}

describe("Bunny BlobForge coordinator", () => {
  it("checks Bunny Database connectivity and rejects unauthenticated workers", async () => {
    const health = await app.fetch(new Request("https://blobforge.example/api/v1/health"));
    expect(health.status).toBe(200);
    await expect(health.json()).resolves.toMatchObject({ ok: true, database: "connected" });

    const denied = await app.fetch(new Request("https://blobforge.example/api/v1/jobs/claim", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ worker_id: "worker-1" }),
    }));
    expect(denied.status).toBe(401);
    expect((await call("/api/v1/workers/register", "POST", { worker_id: "worker-1" }, clientHeaders)).status).toBe(401);
    expect((await call("/api/v1/workers/register", "POST", { worker_id: "another-worker" }, workerHeaders)).status).toBe(403);
  });

  it("enqueues, claims with a fenced lease, heartbeats, and completes", async () => {
    const hash = "a".repeat(64);
    expect((await call("/api/v1/workers/register", "POST", { worker_id: "worker-1", hostname: "test" })).status).toBe(200);
    const queued = await call(`/api/v1/jobs/${hash}`, "PUT", {
      original_name: "book.pdf", size_bytes: 123, paths: ["books/book.pdf"], tags: ["books"], priority: "2_high",
    });
    await expect(queued.json()).resolves.toMatchObject({ hash, status: "todo", priority: "2_high" });

    const claimed = await call("/api/v1/jobs/claim", "POST", { worker_id: "worker-1" });
    const claimBody = await claimed.json() as { job: Record<string, unknown> };
    const job = claimBody.job;
    expect(job).toMatchObject({ hash, status: "processing", worker_id: "worker-1" });
    expect(job.lease_token).toEqual(expect.any(String));
    expect(job).toMatchObject({ input: { url: `https://s3.example/raw/${hash}` }, output_exists: false, tags: ["books"] });

    const repeated = await call("/api/v1/jobs/claim", "POST", { worker_id: "worker-1" });
    await expect(repeated.json()).resolves.toMatchObject({ job: { hash, lease_token: job.lease_token } });

    expect((await call(`/api/v1/jobs/${hash}/heartbeat`, "POST", {
      worker_id: "worker-1", lease_token: job.lease_token, progress: { stage: "converting" },
    })).status).toBe(200);
    expect((await call(`/api/v1/jobs/${hash}/complete`, "POST", {
      worker_id: "worker-1", lease_token: "wrong",
    })).status).toBe(409);
    const upload = await call(`/api/v1/jobs/${hash}/upload-url`, "POST", {
      worker_id: "worker-1", lease_token: job.lease_token,
    });
    await expect(upload.json()).resolves.toMatchObject({ method: "PUT", url: `https://s3.example/out/${hash}` });
    outputExists = true;
    expect((await call(`/api/v1/jobs/${hash}/complete`, "POST", {
      worker_id: "worker-1", lease_token: job.lease_token, result: { output_key: `store/out/${hash}.zip` },
    })).status).toBe(200);
    await expect((await call(`/api/v1/jobs/${hash}`)).json()).resolves.toMatchObject({ status: "done", lease_token: null });

  });

  it("publishes suspension once and returns live heartbeat policy", async () => {
    await database.updateConfig({ heartbeat_enabled: false, heartbeat_interval: 180 }, "test-admin");
    await call("/api/v1/workers/register", "POST", { worker_id: "worker-1", hostname: "test" });
    const before = await database.snapshot(false, false) as { workers: Record<string, unknown>[] };
    const lastHeartbeat = before.workers[0]!.last_heartbeat;

    const suspended = await call("/api/v1/workers/state", "POST", {
      worker_id: "worker-1",
      status: "suspended",
      detail: { reason: "run_window", until: 1_800_000_000_000 },
    });
    await expect(suspended.json()).resolves.toMatchObject({
      ok: true,
      config: { heartbeat_enabled: false, heartbeat_interval: 180 },
    });
    const during = await database.snapshot(false, false) as { workers: Record<string, unknown>[] };
    expect(during.workers[0]).toMatchObject({
      status: "suspended",
      last_heartbeat: lastHeartbeat,
      state: { reason: "run_window", until: 1_800_000_000_000 },
    });

    const idleClaim = await call("/api/v1/jobs/claim", "POST", { worker_id: "worker-1" });
    await expect(idleClaim.json()).resolves.toMatchObject({
      job: null,
      config: { heartbeat_enabled: false, heartbeat_interval: 180 },
    });
  });

  it("recovers an expired lease lazily on the next claim", async () => {
    const hash = "c".repeat(64);
    const newToken = "bfw_new-worker-token";
    const newHeaders = { authorization: `Bearer ${newToken}`, "content-type": "application/json" };
    await database.createWorkerCredential("worker-new", "New worker", await tokenHash(newToken), "test-admin");
    await call("/api/v1/workers/register", "POST", { worker_id: "worker-1", hostname: "old" });
    await call("/api/v1/workers/register", "POST", { worker_id: "worker-new", hostname: "new" }, newHeaders);
    await call(`/api/v1/jobs/${hash}`, "PUT", { original_name: "recover.pdf", priority: "3_normal" });
    await call("/api/v1/jobs/claim", "POST", { worker_id: "worker-1" });
    await client.execute({ sql: "UPDATE jobs SET lease_expires_at=0 WHERE file_hash=?", args: [hash] });

    const recovered = await call("/api/v1/jobs/claim", "POST", { worker_id: "worker-new" }, newHeaders);
    await expect(recovered.json()).resolves.toMatchObject({
      job: { hash, status: "processing", worker_id: "worker-new", retry_count: 1 },
    });
  });

  it("retains structured failure history and the last progress snapshot", async () => {
    const hash = "f".repeat(64);
    await call("/api/v1/workers/register", "POST", { worker_id: "worker-1", hostname: "test" });
    await call(`/api/v1/jobs/${hash}`, "PUT", { original_name: "broken.pdf", priority: "3_normal" });
    const claimedBody = await (await call("/api/v1/jobs/claim", "POST", { worker_id: "worker-1" })).json() as { job: Record<string, unknown> };
    const claimed = claimedBody.job;
    await call(`/api/v1/jobs/${hash}/heartbeat`, "POST", {
      worker_id: "worker-1", lease_token: claimed.lease_token,
      progress: { stage: "converting", percent: 15, elapsed_seconds: 42 },
    });

    const failed = await call(`/api/v1/jobs/${hash}/fail`, "POST", {
      worker_id: "worker-1", lease_token: claimed.lease_token,
      error: "PDFium data format error", traceback: "Traceback: conversion exploded",
      context: { stage: "conversion", exception_type: "RuntimeError" },
    });
    await expect(failed.json()).resolves.toMatchObject({ status: "failed", retry_count: 1 });
    await expect(database.listJobFailures(hash)).resolves.toMatchObject([{
      attempt: 1, worker_id: "worker-1", error_message: "PDFium data format error",
      traceback: "Traceback: conversion exploded",
      context: { stage: "conversion", exception_type: "RuntimeError" },
      progress: { stage: "converting", percent: 15, elapsed_seconds: 42 },
    }]);
    await expect(database.listJobs({ status: "failed" })).resolves.toMatchObject({
      jobs: [expect.objectContaining({ latest_failure_context: { stage: "conversion", exception_type: "RuntimeError" } })],
    });
  });

  it("publishes IndieAuth client metadata", async () => {
    const response = await app.fetch(new Request("https://blobforge.example/client-metadata.json"));
    await expect(response.json()).resolves.toMatchObject({
      client_id: "https://blobforge.example/client-metadata.json",
      redirect_uris: ["https://blobforge.example/auth/callback"],
    });
    expect(response.headers.get("cache-control")).toContain("public, max-age=300");
    expect(response.headers.get("cdn-cache-control")).toContain("max-age=86400");
  });

  it("serves cacheable public documentation and a private administrator login", async () => {
    const schema = vi.spyOn(database, "ensureSchema");
    const page = await app.fetch(new Request("https://blobforge.example/"));
    const body = await page.text();
    expect(schema).not.toHaveBeenCalled();
    expect(body).toContain("Self-hosted document infrastructure");
    expect(body).toContain("Bring a Linux machine online");
    expect(body).toContain('href="/console"');
    expect(body).toContain('href="/static/docs-v1.css"');
    expect(page.headers.get("cache-control")).toContain("public, max-age=300");
    expect(page.headers.get("cdn-cache-control")).toContain("max-age=86400");
    expect(page.headers.get("surrogate-control")).toContain("max-age=86400");
    expect(page.headers.get("content-security-policy")).toContain("default-src 'self'");
    expect(page.headers.get("vary")).toBeNull();

    const revalidated = await app.fetch(new Request("https://blobforge.example/", {
      headers: { "if-none-match": page.headers.get("etag")! },
    }));
    expect(revalidated.status).toBe(304);
    expect(await revalidated.text()).toBe("");

    for (const asset of [
      ["/static/docs-v1.css", "text/css"],
      ["/static/blobforge-v1.svg", "image/svg+xml"],
      ["/static/app-v7.css", "text/css"],
      ["/static/app-v7.js", "text/javascript"],
      ["/static/markdown-v1.js", "text/javascript"],
      ["/static/login-v4.js", "text/javascript"],
    ]) {
      const response = await app.fetch(new Request(`https://blobforge.example${asset[0]}`));
      expect(response.headers.get("content-type")).toContain(asset[1]);
      expect(response.headers.get("cache-control")).toBe("public, max-age=31536000, immutable");
      expect(response.headers.get("cdn-cache-control")).toBe("public, max-age=31536000, immutable");
      expect(response.headers.get("surrogate-control")).toBe("max-age=31536000, immutable");
      expect(response.headers.get("etag")).toBeTruthy();
    }

    const robots = await app.fetch(new Request("https://blobforge.example/robots.txt"));
    expect(await robots.text()).toContain("Disallow: /api/");
    expect(robots.headers.get("cdn-cache-control")).toContain("max-age=86400");

    const head = await app.fetch(new Request("https://blobforge.example/static/docs-v1.css", { method: "HEAD" }));
    expect(head.status).toBe(200);
    expect(await head.text()).toBe("");
    expect(head.headers.get("cache-control")).toContain("immutable");

    const missing = await app.fetch(new Request("https://blobforge.example/wp-login.php"));
    expect(missing.status).toBe(404);
    expect(missing.headers.get("cache-control")).toContain("no-store");
    expect(schema).not.toHaveBeenCalled();

    const loginPage = await app.fetch(new Request("https://blobforge.example/login"));
    const loginBody = await loginPage.text();
    expect(loginBody).toContain('name="me"');
    expect(loginBody).toContain('src="/static/login-v4.js"');
    expect(loginPage.headers.get("cache-control")).toContain("no-store");
    expect(normalizeProfileUrl("alice.example")).toBe("https://alice.example/");
    expect(() => normalizeProfileUrl("http://alice.example")).toThrow("must use HTTPS");
    expect(workerIdFromLabel("GPU Workstation #2")).toBe("gpu-workstation-2");

    const loginScript = await app.fetch(new Request("https://blobforge.example/static/login-v4.js"));
    expect(loginScript.headers.get("content-type")).toContain("text/javascript");
    expect(await loginScript.text()).toContain("window.location.assign");

    const consolePage = await app.fetch(new Request("https://blobforge.example/console"));
    const consoleBody = await consolePage.text();
    expect(consoleBody).toContain("Coordination console");
    expect(consoleBody).toContain('id="viewer-toc"');
    expect(consoleBody).toContain('id="toc-toggle"');
    expect(consoleBody).toContain('id="failure-viewer"');
    expect(consolePage.headers.get("cache-control")).toContain("no-store");
    const appScript = await app.fetch(new Request("https://blobforge.example/static/app-v7.js"));
    const appBody = await appScript.text();
    expect(() => new Function(appBody)).not.toThrow();
    expect(appBody).toContain("localStorage.setItem");
    expect(appBody).toContain("history.replaceState");
    expect(appBody).toContain("BlobForge-Session");
    expect(appBody).toContain("/api/v1/admin/workers");
    expect(appBody).toContain("/api/v1/admin/files");
    expect(appBody).toContain("/api/v1/admin/uploads");
    expect(appBody).toContain("DecompressionStream");
    expect(appBody).toContain("BlobForgeMarkdown.render");
    expect(appBody).toContain("renderToc");
    expect(appBody).toContain("showFailures");
    expect(appBody).not.toContain("markdownToHtml");
    expect(appBody).toContain("location.replace('/login')");
    const markdownScript = await app.fetch(new Request("https://blobforge.example/static/markdown-v1.js"));
    expect(markdownScript.headers.get("content-type")).toContain("text/javascript");
    expect((await markdownScript.text()).length).toBeGreaterThan(10_000);
    const stylesheet = await app.fetch(new Request("https://blobforge.example/static/app-v7.css"));
    const css = await stylesheet.text();
    expect(css).toContain(".viewer-toc");
    expect(css).toContain(".viewer-toc.open");
  });

  it("rejects identities outside the multi-admin allowlist before discovery", async () => {
    const discovery = vi.spyOn(globalThis, "fetch");
    const response = await app.fetch(new Request("https://blobforge.example/auth/login?me=mallory.example"));
    expect(response.status).toBe(403);
    expect(discovery).not.toHaveBeenCalled();
  });

  it("completes IndieAuth with a signed header session that is immediately readable", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
      if (url === "https://alice.example/") return new Response(
        '<link rel="indieauth-metadata" href="https://auth.example/metadata">',
        { status: 200, headers: { "content-type": "text/html" } },
      );
      if (url === "https://auth.example/metadata") return Response.json({
        issuer: "https://auth.example/",
        authorization_endpoint: "https://auth.example/authorize",
        token_endpoint: "https://auth.example/token",
      });
      if (url === "https://auth.example/token") return Response.json({ me: "https://alice.example/" });
      throw new Error(`Unexpected fetch: ${url}`);
    });

    const login = await app.fetch(new Request("https://blobforge.example/auth/login?me=alice.example"));
    expect(login.status).toBe(302);
    const authorization = new URL(login.headers.get("location")!);
    expect(authorization.searchParams.get("me")).toBe("https://alice.example/");

    const callback = await app.fetch(new Request(
      `https://blobforge.example/auth/callback?code=test-code&state=${encodeURIComponent(authorization.searchParams.get("state")!)}`,
    ));
    expect(callback.status).toBe(302);
    expect(callback.headers.get("cache-control")).toContain("no-store");
    expect(callback.headers.get("set-cookie")).toBeNull();
    const destination = new URL(callback.headers.get("location")!, "https://blobforge.example");
    expect(destination.pathname).toBe("/console");
    const token = new URLSearchParams(destination.hash.slice(1)).get("session");
    expect(token).toBeTruthy();
    const authorizationHeader = { authorization: `BlobForge-Session ${token}` };

    const dashboard = await app.fetch(new Request("https://blobforge.example/console"));
    const body = await dashboard.text();
    expect(body).toContain("Coordination console");

    const snapshot = await app.fetch(new Request("https://blobforge.example/api/v1/admin/snapshot", { headers: authorizationHeader }));
    expect(snapshot.status).toBe(200);
    await expect(snapshot.json()).resolves.toMatchObject({ identity: "https://alice.example/" });

    const uploadedHash = "d".repeat(64);
    const prepare = await app.fetch(new Request("https://blobforge.example/api/v1/admin/uploads", {
      method: "POST", headers: { ...authorizationHeader, origin: "https://blobforge.example", "content-type": "application/json" },
      body: JSON.stringify({ hash: uploadedHash }),
    }));
    await expect(prepare.json()).resolves.toMatchObject({ hash: uploadedHash, already_exists: false, url: `https://s3.example/raw-upload/${uploadedHash}` });
    rawExists = true;
    const ingested = await app.fetch(new Request(`https://blobforge.example/api/v1/admin/uploads/${uploadedHash}/complete`, {
      method: "POST", headers: { ...authorizationHeader, origin: "https://blobforge.example", "content-type": "application/json" },
      body: JSON.stringify({ original_name: "Uploaded Rulebook.pdf", size_bytes: 456, path: "library/Uploaded Rulebook.pdf", tags: ["rulebook"], priority: "3_normal" }),
    }));
    expect(ingested.status).toBe(201);
    const searched = await app.fetch(new Request("https://blobforge.example/api/v1/admin/files?q=rulebook&status=todo", { headers: authorizationHeader }));
    await expect(searched.json()).resolves.toMatchObject({ total: 1, jobs: [expect.objectContaining({ hash: uploadedHash, original_name: "Uploaded Rulebook.pdf", tags: ["rulebook"] })] });
    const rawDownload = await app.fetch(new Request(`https://blobforge.example/api/v1/admin/files/${uploadedHash}/download?kind=raw`, { headers: authorizationHeader }));
    await expect(rawDownload.json()).resolves.toMatchObject({ kind: "raw", url: `https://s3.example/raw/${uploadedHash}` });
    await client.execute({ sql: "UPDATE jobs SET status='done',completed_at=?,updated_at=? WHERE file_hash=?", args: [Date.now(), Date.now(), uploadedHash] });
    outputExists = true;
    const doneFiles = await app.fetch(new Request("https://blobforge.example/api/v1/admin/files?status=done", { headers: authorizationHeader }));
    await expect(doneFiles.json()).resolves.toMatchObject({ total: 1, jobs: [expect.objectContaining({ hash: uploadedHash, status: "done" })] });
    const zipDownload = await app.fetch(new Request(`https://blobforge.example/api/v1/admin/files/${uploadedHash}/download?kind=output`, { headers: authorizationHeader }));
    await expect(zipDownload.json()).resolves.toMatchObject({ kind: "output", url: `https://s3.example/out-download/${uploadedHash}` });

    const backup = await app.fetch(new Request("https://blobforge.example/api/v1/admin/backups", {
      method: "POST",
      headers: { ...authorizationHeader, origin: "https://blobforge.example", "content-type": "application/json" },
      body: "{}",
    }));
    expect(backup.status).toBe(201);
    await expect(backup.json()).resolves.toMatchObject({ ok: true, key: expect.stringContaining("backups/coordinator/") });
    expect(JSON.parse(backupBody)).toMatchObject({ format: "blobforge-coordinator-backup", version: 1 });

    const status = await app.fetch(new Request("https://blobforge.example/auth/status", { headers: authorizationHeader }));
    await expect(status.json()).resolves.toMatchObject({
      authenticated: true,
      cookie_present: false,
      session_header_present: true,
      identity: "https://alice.example/",
    });

    const enrollment = await app.fetch(new Request("https://blobforge.example/api/v1/admin/workers", {
      method: "POST",
      headers: { ...authorizationHeader, origin: "https://blobforge.example", "content-type": "application/json" },
      body: JSON.stringify({ label: "GPU workstation" }),
    }));
    expect(enrollment.status).toBe(201);
    const created = await enrollment.json() as Record<string, unknown>;
    expect(created).toMatchObject({ worker_id: "gpu-workstation", label: "GPU workstation", coordinator_url: "https://blobforge.example" });
    expect(created.token).toMatch(/^bfw_/);

    const duplicate = await app.fetch(new Request("https://blobforge.example/api/v1/admin/workers", {
      method: "POST",
      headers: { ...authorizationHeader, origin: "https://blobforge.example", "content-type": "application/json" },
      body: JSON.stringify({ label: "GPU--workstation" }),
    }));
    expect(duplicate.status).toBe(409);
    await expect(duplicate.json()).resolves.toMatchObject({ error: expect.stringContaining("gpu-workstation") });

    const identity = await app.fetch(new Request("https://blobforge.example/api/v1/workers/me", {
      headers: { authorization: `Bearer ${created.token}` },
    }));
    await expect(identity.json()).resolves.toMatchObject({ worker_id: created.worker_id });

    const revoked = await app.fetch(new Request(`https://blobforge.example/api/v1/admin/workers/${created.worker_id}/revoke`, {
      method: "POST",
      headers: { ...authorizationHeader, origin: "https://blobforge.example", "content-type": "application/json" },
      body: "{}",
    }));
    expect(revoked.status).toBe(200);
    const activeWorkers = await app.fetch(new Request("https://blobforge.example/api/v1/admin/snapshot", { headers: authorizationHeader }));
    const activeWorkerBody = await activeWorkers.json() as { worker_enrollments: Record<string, unknown>[] };
    expect(activeWorkerBody.worker_enrollments.some((worker) => worker.worker_id === created.worker_id)).toBe(false);
    const revokedWorkers = await app.fetch(new Request("https://blobforge.example/api/v1/admin/workers?revoked=true", { headers: authorizationHeader }));
    await expect(revokedWorkers.json()).resolves.toMatchObject({
      workers: [expect.objectContaining({ worker_id: created.worker_id, revoked_at: expect.any(Number) })],
    });
    const deniedAfterRevoke = await app.fetch(new Request("https://blobforge.example/api/v1/workers/me", {
      headers: { authorization: `Bearer ${created.token}` },
    }));
    expect(deniedAfterRevoke.status).toBe(401);
  });
});
