import * as BunnySDK from "@bunny.net/edgescript-sdk";
import { createClient } from "@libsql/client/web";
import { BlobForgeApp } from "./app";
import { CoordinatorDatabase } from "./database";

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

const app = new BlobForgeApp(new CoordinatorDatabase(client), {
  workerApiToken: required("WORKER_API_TOKEN"),
  migrationApiToken: Deno.env.get("MIGRATION_API_TOKEN"),
  adminMe: Deno.env.get("ADMIN_ME") || "https://eric.wendland.dev/",
  sessionTtlSeconds: Number(Deno.env.get("SESSION_TTL_SECONDS") || 43_200),
  leaseSeconds: Number(Deno.env.get("LEASE_SECONDS") || 900),
});

BunnySDK.net.http.serve((request: Request) => app.fetch(request));
