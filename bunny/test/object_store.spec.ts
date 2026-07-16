import { afterEach, describe, expect, it, vi } from "vitest";
import { S3ObjectStore } from "../src/object_store";

describe("S3ObjectStore", () => {
  afterEach(() => vi.restoreAllMocks());
  it("creates deterministic path-style SigV4 URLs without exposing the secret", async () => {
    const store = new S3ObjectStore({
      endpointUrl: "https://s3.example.test/base/",
      bucket: "blob bucket",
      region: "eu-central-1",
      accessKeyId: "ACCESS123",
      secretAccessKey: "never-put-this-in-the-url",
      prefix: "/tenant/pdf/",
      forcePathStyle: true,
    });

    const url = await store.presign("GET", store.rawKey("a".repeat(64)), 600, new Date("2026-07-16T12:34:56Z"));
    const parsed = new URL(url);
    expect(parsed.pathname).toBe(`/base/blob%20bucket/tenant/pdf/store/raw/${"a".repeat(64)}.pdf`);
    expect(parsed.searchParams.get("X-Amz-Algorithm")).toBe("AWS4-HMAC-SHA256");
    expect(parsed.searchParams.get("X-Amz-Credential")).toBe("ACCESS123/20260716/eu-central-1/s3/aws4_request");
    expect(parsed.searchParams.get("X-Amz-Date")).toBe("20260716T123456Z");
    expect(parsed.searchParams.get("X-Amz-Expires")).toBe("600");
    expect(parsed.searchParams.get("X-Amz-Signature")).toMatch(/^[a-f0-9]{64}$/);
    expect(url).not.toContain("never-put-this-in-the-url");
  });

  it("supports virtual-hosted bucket URLs", async () => {
    const store = new S3ObjectStore({
      endpointUrl: "https://s3.example.test",
      bucket: "blobforge",
      region: "us-east-1",
      accessKeyId: "ACCESS123",
      secretAccessKey: "secret",
      forcePathStyle: false,
    });
    const url = new URL(await store.presign("PUT", "store/out/result.zip", 60, new Date("2026-07-16T00:00:00Z")));
    expect(url.host).toBe("blobforge.s3.example.test");
    expect(url.pathname).toBe("/store/out/result.zip");
  });

  it("uploads coordinator backups outside the legacy registry prefix", async () => {
    const upload = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 200 }));
    const store = new S3ObjectStore({
      endpointUrl: "https://s3.example.test",
      bucket: "blobforge",
      region: "us-east-1",
      accessKeyId: "ACCESS123",
      secretAccessKey: "secret",
      prefix: "pdf/",
    });
    const stored = await store.backup("2026-07-16-test", '{"version":1}');
    expect(stored.key).toBe("pdf/backups/coordinator/2026-07-16-test.json");
    expect(upload).toHaveBeenCalledWith(expect.stringContaining("pdf/backups/coordinator/2026-07-16-test.json"), expect.objectContaining({ method: "PUT" }));
  });
});
