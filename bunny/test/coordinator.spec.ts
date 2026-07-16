import { createClient, type Client } from "@libsql/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { BlobForgeApp, normalizeProfileUrl } from "../src/app";
import { CoordinatorDatabase } from "../src/database";

let client: Client;
let app: BlobForgeApp;
let database: CoordinatorDatabase;
const workerHeaders = { authorization: "Bearer worker-secret", "content-type": "application/json" };

beforeEach(() => {
  client = createClient({ url: "file::memory:" });
  database = new CoordinatorDatabase(client);
  app = new BlobForgeApp(database, {
    workerApiToken: "worker-secret",
    migrationApiToken: "migration-secret",
    sessionSigningSecret: "session-secret-that-is-different-from-worker-secret",
    adminMes: ["https://eric.wendland.dev/", "https://alice.example/"],
  });
});

afterEach(() => { vi.restoreAllMocks(); client.close(); });

function call(path: string, method = "GET", body?: unknown, headers = workerHeaders): Promise<Response> {
  return app.fetch(new Request(`https://blobforge.example${path}`, {
    method, headers, body: body === undefined ? undefined : JSON.stringify(body),
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

    const repeated = await call("/api/v1/jobs/claim", "POST", { worker_id: "worker-1" });
    await expect(repeated.json()).resolves.toMatchObject({ hash, lease_token: job.lease_token });

    expect((await call(`/api/v1/jobs/${hash}/heartbeat`, "POST", {
      worker_id: "worker-1", lease_token: job.lease_token, progress: { stage: "converting" },
    })).status).toBe(200);
    expect((await call(`/api/v1/jobs/${hash}/complete`, "POST", {
      worker_id: "worker-1", lease_token: "wrong",
    })).status).toBe(409);
    expect((await call(`/api/v1/jobs/${hash}/complete`, "POST", {
      worker_id: "worker-1", lease_token: job.lease_token, result: { output_key: `store/out/${hash}.zip` },
    })).status).toBe(200);
    await expect((await call(`/api/v1/jobs/${hash}`)).json()).resolves.toMatchObject({ status: "done", lease_token: null });
  });

  it("imports terminal state only with the separate migration token", async () => {
    const hash = "b".repeat(64);
    const payload = { items: [{ hash, status: "dead", priority: "4_low", retry_count: 5, max_retries: 5, original_name: "failed.pdf" }] };
    expect((await call("/api/v1/migration/import", "POST", payload)).status).toBe(401);
    const imported = await call("/api/v1/migration/import", "POST", payload, {
      authorization: "Bearer migration-secret", "content-type": "application/json",
    });
    await expect(imported.json()).resolves.toMatchObject({ imported: 1 });
    await expect((await call(`/api/v1/jobs/${hash}`)).json()).resolves.toMatchObject({ status: "dead", retry_count: 5 });
  });

  it("recovers an expired lease lazily on the next claim", async () => {
    const hash = "c".repeat(64);
    await call("/api/v1/workers/register", "POST", { worker_id: "worker-old", hostname: "old" });
    await call("/api/v1/workers/register", "POST", { worker_id: "worker-new", hostname: "new" });
    await call(`/api/v1/jobs/${hash}`, "PUT", { original_name: "recover.pdf", priority: "3_normal" });
    await call("/api/v1/jobs/claim", "POST", { worker_id: "worker-old" });
    await client.execute({ sql: "UPDATE jobs SET lease_expires_at=0 WHERE file_hash=?", args: [hash] });

    const recovered = await call("/api/v1/jobs/claim", "POST", { worker_id: "worker-new" });
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
    expect(normalizeProfileUrl("alice.example")).toBe("https://alice.example/");
    expect(() => normalizeProfileUrl("http://alice.example")).toThrow("must use HTTPS");
  });

  it("rejects identities outside the multi-admin allowlist before discovery", async () => {
    const discovery = vi.spyOn(globalThis, "fetch");
    const response = await app.fetch(new Request("https://blobforge.example/auth/login?me=mallory.example"));
    expect(response.status).toBe(403);
    expect(discovery).not.toHaveBeenCalled();
  });

  it("completes IndieAuth with a signed session that is immediately readable", async () => {
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
    const cookie = callback.headers.get("set-cookie")!.split(";", 1)[0]!;

    const dashboard = await app.fetch(new Request("https://blobforge.example/", { headers: { cookie } }));
    const body = await dashboard.text();
    expect(body).toContain("Coordination console");
    expect(body).toContain("https://alice.example/");
  });
});
