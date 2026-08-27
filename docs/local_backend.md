# Self-hosted SQLite and Filesystem Backend

## Decision

BlobForge's primary backend is moving from Bunny Edge Scripting, Bunny Database,
and S3 to one conventional Python HTTP service. SQLite is the authoritative
metadata/queue database and a durable local directory contains immutable source
and artifact objects. Workers still use the existing coordinator protocol and
never receive direct filesystem access.

This is deliberately a single-writer-service architecture. Run exactly one
BlobForge server against a data directory. Multiple conversion workers may use
that server concurrently; multiple server replicas must not share the SQLite
file. If availability later requires replicas, the database boundary can move
to PostgreSQL without changing transfer capabilities or worker leases.

## Components and trust boundaries

- `blobforge serve` runs the FastAPI application.
- `blobforge.sqlite3` owns sources, digest aliases, jobs, fenced leases,
  workers, failures, recipes selected for jobs, and immutable artifacts.
- `objects/sources/` stores source bytes by their declared canonical digest.
- `objects/artifacts/` stores immutable outputs by source, recipe, and artifact
  identity. MDAF v1 is an artifact type, not a PDF-specific special case.
- `pending/` contains lease-scoped uploads until completion commits them.
- `capability.key` signs short-lived transfer URLs and is generated with mode
  `0600`. It must be included in backups so outstanding URLs remain valid after
  a restore; rotating it intentionally revokes them.

Client and worker bearer tokens authorize control-plane calls. Transfer URLs
are separately HMAC signed, expire, and are scoped to one HTTP method and one
object. Output transfers are additionally tied to a current worker/lease pair,
which is revalidated when bytes arrive. Uploads stream to a temporary file,
`fsync`, verify digests where applicable, and atomically rename into place.

## Media-neutral identity model

`sources` separates an opaque compatibility key from `(digest_algorithm,
digest)` and `media_type`. `source_aliases` retains additional identities such
as the historical SHA-256 while migrated sources use BLAKE3 canonically. Jobs
refer to sources rather than PDFs. Workers advertise versioned capabilities
when registering and claiming work. Each capability binds its recipe to
accepted media and artifact types. The current Marker worker can only claim
`application/pdf`; a future multipurpose supervisor can advertise audio, image,
ebook, web, and video adapters and interleave jobs on one constrained host.

Artifacts have a recipe, byte digests, media type, artifact type, and immutable
identity. Current Marker workers still publish `legacy-archive`; the verified
legacy migration imports `mdaf/v1`. A future staged worker will publish MDAF
directly after source-map and package validation.

## On-disk layout

```text
/var/lib/blobforge/
├── blobforge.sqlite3
├── blobforge.sqlite3-wal
├── blobforge.sqlite3-shm
├── capability.key
├── objects/
│   ├── sources/<algorithm>/<shard>/<digest>
│   └── artifacts/<source-shard>/<source>/<recipe>/<identity>
└── pending/<source>/<lease-token>
```

Do not place the SQLite database on NFS or a storage system with unreliable
POSIX locking/rename semantics. The object tree and database must be restored
as one backup generation.

## Local development

```bash
uv sync --extra dev --extra server
export BLOBFORGE_SERVER_DATA_DIR="$PWD/.local-server"
export BLOBFORGE_SERVER_CLIENT_TOKEN="$(openssl rand -hex 32)"
export BLOBFORGE_SERVER_WORKER_TOKENS='{"pdf-worker-1":"replace-me"}'
uv run blobforge serve --host 127.0.0.1 --port 8080
```

Set clients and workers to `http://127.0.0.1:8080` with their respective
tokens. TLS belongs at a local reverse proxy for non-loopback deployments.
OIDC/SCIM settings and the authorization model are documented in
`docs/coordinator_identity_and_routing.md`.

## Podman Quadlet deployment

The server image is `ghcr.io/tionis/blobforge:latest`. Heavy worker images are
tagged `:worker` and `:worker-cuda`. Copy the examples from `deploy/quadlet/`
to `/etc/containers/systemd/` for a system service or
`~/.config/containers/systemd/` for a rootless user. Copy the environment
example to `/etc/blobforge/blobforge.env`, replace every token, and restrict it
to the service account.

```bash
sudo install -d -m 0750 /etc/blobforge /etc/containers/systemd
sudo install -m 0644 deploy/quadlet/blobforge.container \
  deploy/quadlet/blobforge-data.volume /etc/containers/systemd/
sudo install -m 0600 deploy/quadlet/blobforge.env.example \
  /etc/blobforge/blobforge.env
sudo systemctl daemon-reload
sudo systemctl start blobforge.service
curl --fail http://127.0.0.1:8080/api/v1/health
```

The example binds only to loopback, drops capabilities, uses a read-only
container root, and persists `/var/lib/blobforge` in a named volume. The `:U`
volume option adjusts a new volume for the image's non-root UID; do not remove
it unless ownership is provisioned separately. Quadlet's
`AutoUpdate=registry` requires a fully qualified image reference. Decide
explicitly whether to enable a `podman-auto-update.timer`; pin a digest instead
when deployments must be manually promoted.

## Backup and restore

The first release does not claim online snapshot consistency. For a guaranteed
backup, stop `blobforge.service`, archive the entire named volume, then restart
it. A future admin backup endpoint should use SQLite's online backup API and
record a manifest of every referenced object before online backups are called
production-ready. Test restores independently before retiring Bunny/S3.

## Legacy MDAF import and cutover

The existing verified stage imports without remote writes:

```bash
uv run blobforge migrate import-local \
  --stage .blobforge-migration/staged-v2 \
  --data-dir /path/to/blobforge-data

# After reviewing the dry run:
uv run blobforge migrate import-local \
  --stage .blobforge-migration/staged-v2 \
  --data-dir /path/to/blobforge-data --execute

# Then recover raw sources which had no completed legacy artifact:
uv run blobforge migrate import-legacy-sources \
  --workspace .blobforge-migration \
  --data-dir /path/to/blobforge-data
uv run blobforge migrate import-legacy-sources \
  --workspace .blobforge-migration \
  --data-dir /path/to/blobforge-data --execute
```

The importer checks the canonical manifest digest, source BLAKE3, each MDAF
logical identity, and path containment before any write. It keeps the legacy
SHA-256 as the compatibility key/alias, records BLAKE3 as canonical, marks the
job done, and is safe to repeat.

The companion raw-source importer skips those completed records, verifies each
remaining source against its filename SHA-256 while deriving BLAKE3, and queues
it at normal priority. The legacy catalog cannot reconstruct all coordinator
paths/tags, so these jobs are labeled `metadata-unavailable` rather than
inventing provenance. For the current mirror this second phase accounts for the
431 sources without a legacy artifact, bringing the planned local source total
to 1,808.

Cutover remains gated on a restored-backup drill, a full imported-count and
object audit, client/worker canaries, and a read-only retention period for the
old bucket. Nothing in the importer deletes or changes Bunny/S3.

### Completed local migration and Citadel handoff

The 2026-08-27 local import target is
`.blobforge-migration/local-server-data`. The completed state contains 1,808
sources/jobs, 1,377 done legacy MDAFs, 431 queued raw-only sources, 3,616 digest
aliases, and no pending objects. Its SQLite quick check passes. This directory
is ignored by Git and must never enter the container build context.

Treat the entire directory as one recovery unit: `blobforge.sqlite3`, its
checkpoint state, `capability.key`, and `objects/` must travel together. Before
copying it to Citadel, stop any local coordinator using it and create/verify a
whole-directory BLAKE3 manifest. On Citadel, stop BlobForge, copy into a fresh
`/srv/blobforge`, set ownership for container UID/GID 10001, run SQLite
`PRAGMA quick_check`, verify the expected counts and object manifest, then start
the pinned server image. Do not initialize an empty production database first
and copy over a running SQLite service.

The frozen recovery unit has 3,188 files and occupies approximately 33 GB.
Its relative-path checksum list is
`.blobforge-migration/local-server-data.blake3` (3,188 entries), whose BLAKE3 is
`b654923b59e24bd5709aab3e8a9803b351f5c03cba48596baf3df876c36ddf23`.
`MIGRATION.json` inside the recovery unit records the accepted counts, byte
totals, legacy recipe, and provenance limitations. Verify a copied unit from
its root with:

```bash
b3sum --check --quiet /secure/path/local-server-data.blake3
```

The manifest includes the persistent `capability.key`; both files are sensitive
deployment material and must remain outside Git and container build contexts.

After start, verify health, the 1,377/431 done/todo split, one legacy artifact
download, one source download, OIDC login, SCIM readiness, worker registration,
and one low-risk conversion. Keep Bunny/S3 read-only and available through a
rollback window; DNS cutover is reversible, whereas deleting the old objects is
not part of migration.

## CI and publication

`.github/workflows/container.yml` first installs the locked development/server
extras, runs the full suite, and builds Python distributions. Only after that
gate does it build multi-architecture server and CPU worker images plus an
amd64 CUDA worker image; it publishes to GHCR only for non-pull-request events.
The former Bunny deployment workflow is manual-only during the migration
window.

## Known first-release gaps

- No replacement management Web UI or token CRUD yet; worker tokens are
  bootstrapped from the authoritative environment mapping and stored hashed in
  SQLite. Removing a worker from the mapping revokes it on the next restart.
- No online backup endpoint, metrics endpoint, rate limiting, or reverse proxy.
- The generic persistence and multipurpose claim contract is implemented, but
  filesystem ingestion still defaults to PDFs and the production worker still
  emits a legacy ZIP. New media requires adapter dispatch, its runtime, and
  output validation.
- Alias lookup is stored but the public lookup/ingestion path still uses the
  historical 64-character compatibility key.
- SQLite supports this single-server design, not active-active replicas.
