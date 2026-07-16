import { createClient, type Client } from "@libsql/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { BlobForgeApp, normalizeProfileUrl } from "../src/app";
import { CoordinatorDatabase } from "../src/database";

let client: Client;
let app: BlobForgeApp;
let database: CoordinatorDatabase;
let outputExists = false;
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
      backup: async (name, body) => { backupBody = body; return { key: `pdf/backups/coordinator/${name}.json` }; },
    },
  });
  await database.ensureSchema();
  await database.createWorkerCredential("worker-1", "Test worker", await tokenHash(workerToken), "test-admin");
  outputExists = false;
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
    const job = await claimed.json() as Record<string, unknown>;
    expect(job).toMatchObject({ hash, status: "processing", worker_id: "worker-1" });
    expect(job.lease_token).toEqual(expect.any(String));
    expect(job).toMatchObject({ input: { url: `https://s3.example/raw/${hash}` }, output_exists: false, tags: ["books"] });

    const repeated = await call("/api/v1/jobs/claim", "POST", { worker_id: "worker-1" });
    await expect(repeated.json()).resolves.toMatchObject({ hash, lease_token: job.lease_token });

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
      hash, status: "processing", worker_id: "worker-new", retry_count: 1,
    });
  });

  it("publishes IndieAuth client metadata", async () => {
    const response = await app.fetch(new Request("https://blobforge.example/client-metadata.json"));
    await expect(response.json()).resolves.toMatchObject({
      client_id: "https://blobforge.example/client-metadata.json",
      redirect_uris: ["https://blobforge.example/auth/callback"],
    });
  });

  it("shows a profile URL field and normalizes bare domains to HTTPS", async () => {
    const page = await app.fetch(new Request("https://blobforge.example/"));
    const body = await page.text();
    expect(body).toContain('name="me"');
    expect(body).toContain('src="/login.js?v=3"');
    expect(normalizeProfileUrl("alice.example")).toBe("https://alice.example/");
    expect(() => normalizeProfileUrl("http://alice.example")).toThrow("must use HTTPS");

    const loginScript = await app.fetch(new Request("https://blobforge.example/login.js"));
    expect(loginScript.headers.get("content-type")).toContain("text/javascript");
    expect(await loginScript.text()).toContain("window.location.assign");

    const consolePage = await app.fetch(new Request("https://blobforge.example/console"));
    expect(await consolePage.text()).toContain("Coordination console");
    const appScript = await app.fetch(new Request("https://blobforge.example/app.js"));
    const appBody = await appScript.text();
    expect(() => new Function(appBody)).not.toThrow();
    expect(appBody).toContain("localStorage.setItem");
    expect(appBody).toContain("history.replaceState");
    expect(appBody).toContain("BlobForge-Session");
    expect(appBody).toContain("/api/v1/admin/workers");
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
    expect(created).toMatchObject({ label: "GPU workstation", coordinator_url: "https://blobforge.example" });
    expect(created.token).toMatch(/^bfw_/);

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
    const deniedAfterRevoke = await app.fetch(new Request("https://blobforge.example/api/v1/workers/me", {
      headers: { authorization: `Bearer ${created.token}` },
    }));
    expect(deniedAfterRevoke.status).toBe(401);
  });
});
