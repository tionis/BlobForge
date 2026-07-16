export interface ObjectStoreConfig {
  endpointUrl: string;
  bucket: string;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
  prefix?: string;
  forcePathStyle?: boolean;
  downloadTtlSeconds?: number;
  uploadTtlSeconds?: number;
}

export interface PresignedTransfer {
  url: string;
  expiresAt: number;
}

export interface ObjectTransferStore {
  rawKey(hash: string): string;
  outputKey(hash: string): string;
  download(hash: string): Promise<PresignedTransfer>;
  upload(hash: string): Promise<PresignedTransfer>;
  outputExists(hash: string): Promise<boolean>;
  rawExists(hash: string): Promise<boolean>;
  rawUpload(hash: string): Promise<PresignedTransfer>;
  outputDownload(hash: string): Promise<PresignedTransfer>;
  backup(name: string, body: string): Promise<{ key: string }>;
}

function encodeRfc3986(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`);
}

function encodePath(value: string): string {
  return value.split("/").map(encodeRfc3986).join("/");
}

function hex(bytes: Uint8Array): string {
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function sha256Hex(value: string): Promise<string> {
  return hex(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value))));
}

async function hmac(key: Uint8Array, value: string): Promise<Uint8Array> {
  const cryptoKey = await crypto.subtle.importKey("raw", Uint8Array.from(key).buffer, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await crypto.subtle.sign("HMAC", cryptoKey, new TextEncoder().encode(value)));
}

function normalizedPrefix(value = ""): string {
  const trimmed = value.replace(/^\/+|\/+$/g, "");
  return trimmed ? `${trimmed}/` : "";
}

export class S3ObjectStore implements ObjectTransferStore {
  private readonly endpoint: URL;
  private readonly prefix: string;

  constructor(private readonly config: ObjectStoreConfig) {
    this.endpoint = new URL(config.endpointUrl);
    if (this.endpoint.protocol !== "https:") throw new Error("S3_ENDPOINT_URL must use HTTPS");
    this.prefix = normalizedPrefix(config.prefix);
  }

  rawKey(hash: string): string { return `${this.prefix}store/raw/${hash}.pdf`; }
  outputKey(hash: string): string { return `${this.prefix}store/out/${hash}.zip`; }

  async download(hash: string): Promise<PresignedTransfer> {
    const ttl = Math.min(604_800, Math.max(60, this.config.downloadTtlSeconds || 3600));
    return { url: await this.presign("GET", this.rawKey(hash), ttl), expiresAt: Date.now() + ttl * 1000 };
  }

  async upload(hash: string): Promise<PresignedTransfer> {
    const ttl = Math.min(604_800, Math.max(60, this.config.uploadTtlSeconds || 900));
    return { url: await this.presign("PUT", this.outputKey(hash), ttl), expiresAt: Date.now() + ttl * 1000 };
  }

  async outputExists(hash: string): Promise<boolean> {
    const response = await fetch(await this.presign("HEAD", this.outputKey(hash), 60), { method: "HEAD" });
    if (response.ok) return true;
    if (response.status === 404) return false;
    throw new Error(`Object-store HEAD failed (${response.status})`);
  }

  async rawExists(hash: string): Promise<boolean> {
    const response = await fetch(await this.presign("HEAD", this.rawKey(hash), 60), { method: "HEAD" });
    if (response.ok) return true;
    if (response.status === 404) return false;
    throw new Error(`Raw object HEAD failed (${response.status})`);
  }

  async rawUpload(hash: string): Promise<PresignedTransfer> {
    const ttl = Math.min(3600, Math.max(60, this.config.uploadTtlSeconds || 900));
    return { url: await this.presign("PUT", this.rawKey(hash), ttl), expiresAt: Date.now() + ttl * 1000 };
  }

  async outputDownload(hash: string): Promise<PresignedTransfer> {
    const ttl = Math.min(604_800, Math.max(60, this.config.downloadTtlSeconds || 3600));
    return { url: await this.presign("GET", this.outputKey(hash), ttl), expiresAt: Date.now() + ttl * 1000 };
  }


  async backup(name: string, body: string): Promise<{ key: string }> {
    const key = `${this.prefix}backups/coordinator/${name}.json`;
    const response = await fetch(await this.presign("PUT", key, 900), {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body,
    });
    if (!response.ok) throw new Error(`Object-store backup upload failed (${response.status})`);
    return { key };
  }

  async presign(method: "GET" | "PUT" | "HEAD", key: string, expiresSeconds: number, now = new Date()): Promise<string> {
    const timestamp = now.toISOString().replace(/[:-]|\.\d{3}/g, "");
    const date = timestamp.slice(0, 8);
    const scope = `${date}/${this.config.region}/s3/aws4_request`;
    const forcePathStyle = this.config.forcePathStyle !== false;
    const endpointPath = this.endpoint.pathname.replace(/\/$/, "");
    const host = forcePathStyle ? this.endpoint.host : `${this.config.bucket}.${this.endpoint.host}`;
    const canonicalUri = forcePathStyle
      ? `${endpointPath}/${encodePath(this.config.bucket)}/${encodePath(key)}`.replace(/^$/, "/")
      : `${endpointPath}/${encodePath(key)}`.replace(/^$/, "/");
    const queryEntries = [
      ["X-Amz-Algorithm", "AWS4-HMAC-SHA256"],
      ["X-Amz-Credential", `${this.config.accessKeyId}/${scope}`],
      ["X-Amz-Date", timestamp],
      ["X-Amz-Expires", String(expiresSeconds)],
      ["X-Amz-SignedHeaders", "host"],
    ].map(([name, value]) => [encodeRfc3986(name!), encodeRfc3986(value!)] as const)
      .sort(([left], [right]) => left.localeCompare(right));
    const canonicalQuery = queryEntries.map(([name, value]) => `${name}=${value}`).join("&");
    const canonicalHeaders = `host:${host}\n`;
    const canonicalRequest = `${method}\n${canonicalUri}\n${canonicalQuery}\n${canonicalHeaders}\nhost\nUNSIGNED-PAYLOAD`;
    const stringToSign = `AWS4-HMAC-SHA256\n${timestamp}\n${scope}\n${await sha256Hex(canonicalRequest)}`;
    const secret = new TextEncoder().encode(`AWS4${this.config.secretAccessKey}`);
    const signingKey = await hmac(await hmac(await hmac(await hmac(secret, date), this.config.region), "s3"), "aws4_request");
    const signature = hex(await hmac(signingKey, stringToSign));
    return `${this.endpoint.protocol}//${host}${canonicalUri}?${canonicalQuery}&X-Amz-Signature=${signature}`;
  }
}
