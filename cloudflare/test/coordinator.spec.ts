import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";

const auth = { authorization: "Bearer test-worker-token", "content-type": "application/json" };
const hash = "a".repeat(64);

async function call(path: string, method = "GET", body?: unknown): Promise<Response> {
  return SELF.fetch(`https://blobforge.test${path}`, {
    method,
    headers: auth,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

describe("BlobForge coordinator", () => {
  it("serves health and rejects unauthenticated worker calls", async () => {
    const health = await SELF.fetch("https://blobforge.test/api/v1/health");
    expect(health.status).toBe(200);
    await expect(health.json()).resolves.toMatchObject({ ok: true });

    const denied = await SELF.fetch("https://blobforge.test/api/v1/jobs/claim", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ worker_id: "worker-1" }),
    });
    expect(denied.status).toBe(401);
  });

  it("retains, claims, heartbeats, and completes a job with a fenced lease", async () => {
    expect((await call("/api/v1/workers/register", "POST", { worker_id: "worker-1", hostname: "test" })).status).toBe(200);
    const enqueue = await call(`/api/v1/jobs/${hash}`, "PUT", {
      original_name: "book.pdf",
      size_bytes: 123,
      paths: ["books/book.pdf"],
      tags: ["books"],
      priority: "2_high",
    });
    expect(enqueue.status).toBe(200);
    await expect(enqueue.json()).resolves.toMatchObject({ hash, status: "todo", priority: "2_high" });

    const claim = await call("/api/v1/jobs/claim", "POST", { worker_id: "worker-1" });
    expect(claim.status).toBe(200);
    const job = await claim.json() as Record<string, unknown>;
    expect(job).toMatchObject({ hash, status: "processing", worker_id: "worker-1" });
    expect(job.lease_token).toEqual(expect.any(String));

    const heartbeat = await call(`/api/v1/jobs/${hash}/heartbeat`, "POST", {
      worker_id: "worker-1",
      lease_token: job.lease_token,
      progress: { stage: "converting" },
    });
    expect(heartbeat.status).toBe(200);

    const rejected = await call(`/api/v1/jobs/${hash}/complete`, "POST", {
      worker_id: "worker-1",
      lease_token: "wrong-token",
    });
    expect(rejected.status).toBe(409);

    const completed = await call(`/api/v1/jobs/${hash}/complete`, "POST", {
      worker_id: "worker-1",
      lease_token: job.lease_token,
      result: { output_key: `store/out/${hash}.zip` },
    });
    expect(completed.status).toBe(200);
    const stored = await call(`/api/v1/jobs/${hash}`);
    await expect(stored.json()).resolves.toMatchObject({ status: "done", lease_token: null });
  });

  it("publishes IndieAuth client metadata for the callback", async () => {
    const response = await SELF.fetch("https://blobforge.test/client-metadata.json");
    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      client_id: "https://blobforge.test/client-metadata.json",
      redirect_uris: ["https://blobforge.test/auth/callback"],
    });
  });

  it("imports legacy terminal state only with the migration secret", async () => {
    const migratedHash = "b".repeat(64);
    const payload = JSON.stringify({
      items: [{
        hash: migratedHash,
        status: "dead",
        priority: "4_low",
        retry_count: 5,
        max_retries: 5,
        paths: ["archive/failed.pdf"],
        original_name: "failed.pdf",
        error: "legacy failure",
      }],
    });
    const denied = await SELF.fetch("https://blobforge.test/api/v1/migration/import", {
      method: "POST",
      headers: auth,
      body: payload,
    });
    expect(denied.status).toBe(401);

    const imported = await SELF.fetch("https://blobforge.test/api/v1/migration/import", {
      method: "POST",
      headers: { authorization: "Bearer test-migration-token", "content-type": "application/json" },
      body: payload,
    });
    expect(imported.status).toBe(200);
    await expect(imported.json()).resolves.toMatchObject({ imported: 1 });
    await expect((await call(`/api/v1/jobs/${migratedHash}`)).json()).resolves.toMatchObject({
      status: "dead",
      retry_count: 5,
      error_message: "legacy failure",
    });
  });
});
