import * as BunnySDK from "@bunny.net/edgescript-sdk";
import { createClient } from "@libsql/client/web";
import { BlobForgeApp } from "./app";
import { CoordinatorDatabase } from "./database";
import { S3ObjectStore } from "./object_store";

declare const Deno: { env: { get(name: string): string | undefined } };

function required(name: string): string {
  const value = Deno.env.get(name);
  if (!value) throw new Error(`Missing required environment secret: ${name}`);
  return value;
}

const client = createClient({
  url: required("BUNNY_DATABASE_URL"),
  authToken: required("BUNNY_DATABASE_AUTH_TOKEN"),
});

const objectStore = new S3ObjectStore({
  endpointUrl: required("S3_ENDPOINT_URL"),
  bucket: required("S3_BUCKET"),
  region: Deno.env.get("S3_REGION") || "us-east-1",
  accessKeyId: required("S3_ACCESS_KEY_ID"),
  secretAccessKey: required("S3_SECRET_ACCESS_KEY"),
  prefix: Deno.env.get("S3_PREFIX") || "pdf/",
  forcePathStyle: (Deno.env.get("S3_FORCE_PATH_STYLE") || "true").toLowerCase() !== "false",
  downloadTtlSeconds: Number(Deno.env.get("DOWNLOAD_URL_TTL_SECONDS") || 3600),
  uploadTtlSeconds: Number(Deno.env.get("UPLOAD_URL_TTL_SECONDS") || 900),
});

const app = new BlobForgeApp(new CoordinatorDatabase(client), {
  clientApiToken: Deno.env.get("CLIENT_API_TOKEN") || required("WORKER_API_TOKEN"),
  sessionSigningSecret: required("SESSION_SIGNING_SECRET"),
  adminMes: (Deno.env.get("ADMIN_MES") || Deno.env.get("ADMIN_ME") || "https://eric.wendland.dev/").split(",").map((value) => value.trim()).filter(Boolean),
  sessionTtlSeconds: Number(Deno.env.get("SESSION_TTL_SECONDS") || 43_200),
  leaseSeconds: Number(Deno.env.get("LEASE_SECONDS") || 900),
  objectStore,
});

BunnySDK.net.http.serve((request: Request) => app.fetch(request));
