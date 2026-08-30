# Self-hosted management console

## Purpose and navigation

The authenticated root is the administration console, not a diagnostic
landing page. It is limited to the `admin` role and is organized around the
objects administrators operate:

- **Overview** shows queue state, priority distribution, worker availability,
  and recent administrative audit events.
- **Jobs** is a paginated library with name/hash/path/tag search and state or
  priority filters. Administrators can stream-upload a source, change its
  priority, inspect failure history and artifacts, download source/output
  bytes, release and requeue active work, retry failures with or without
  resetting attempts, request an exact conversion recipe, and delete a job.
- **Workers** creates one credential per worker identity and lists runtime
  state. Dynamically enrolled credentials can be rotated or revoked.
- **Recipes** explains and manages converter output identities. Administrators
  may attach a display name and notes or retire/reactivate a recipe.
- **Access** creates and revokes automation administrator tokens.

OpenAPI remains available from the sidebar for integration work. The old
**Snapshot JSON** link was a raw diagnostics endpoint and is intentionally not
an administrative control. The old **Conversion recipes** link merely exposed
the worker-advertised recipe registry as JSON; the Recipes section now gives
that registry an explicit operating model.

## Job mutation semantics

Management uploads stream through the coordinator rather than being buffered
in browser memory. One pass derives SHA-256 and BLAKE3. The current compatibility
job key remains SHA-256 while BLAKE3 is stored as an alias; this does not yet
complete the planned canonical-key cutover.

The equivalent CLI intake is intended for bulk rulebook uploads. Create a
revocable administrator token in the management console, keep it in the
environment rather than shell history, and preview the exact batch first:

```bash
export BLOBFORGE_COORDINATOR_URL=https://blobforge.tionis.dev
read -rsp 'BlobForge admin token: ' BLOBFORGE_COORDINATOR_TOKEN
export BLOBFORGE_COORDINATOR_TOKEN
printf '\n'
uv run blobforge upload ~/rulebooks \
  --recipe mistral-ocr-wiki \
  --priority 2_high \
  --tag rulebook \
  --dry-run
```

Remove `--dry-run` to stream every PDF recursively. The recipe selector accepts
an exact digest or an unambiguous active backend/display name. Use
`--recipe datalab-convert-wiki` for the Datalab challenger. `--unassigned` is
an explicit alternative, but hosted workers deliberately cannot claim those
jobs. Upload separate directory/file groups when they need different
priorities; `--json` produces machine-readable per-file results.

Requeueing an active job clears its lease token, so a late completion from the
old worker is rejected by lease fencing. A completed job cannot be generically
requeued: requesting a different immutable recipe is the safe way to produce
another output. Reset-and-retry clears the attempt counter; ordinary retry
preserves it.

Deletion first refuses actively processing jobs. It removes catalog rows and
moves existing source/artifact files into the server's `trash/` tree instead
of unlinking them. Trash retention and automated orphan cleanup remain an
operations follow-up; recovery currently requires an administrator with host
access.

## Worker credentials

The console creates credentials prefixed `bfw_`. Plaintext is displayed once;
only a SHA-256 hash is persisted. Each credential is bound to exactly one
worker ID and survives coordinator restarts. Rotation invalidates the prior
secret immediately. Revocation prevents registration, claiming, heartbeats,
and completion.

Workers declared through `BLOBFORGE_SERVER_WORKER_TOKENS` remain owned by
deployment configuration. The console identifies them as `environment` and
does not offer rotate/revoke controls because a restart would otherwise restore
the configured value. Remove or rotate those credentials through Gandalf.

One worker process may advertise several media/backend capabilities and still
claims one job at a time. The console manages the identity; converter adapter
installation and runtime isolation remain worker-host responsibilities.

## Recipes are immutable output identities

A recipe is created when a worker advertises a capability. Its digest binds the
canonical output-affecting converter configuration. Editing that canonical
configuration in the console would break reproducibility, so the UI only edits
operator metadata (`display_name`, `notes`) and `enabled` state. Retiring a
recipe prevents backend auto-selection and hides it from new interactive
conversion requests; it does not delete existing artifacts or rewrite history.

If more than one enabled recipe is available for the same backend/media pair,
the administrator must choose an exact digest. This is intentional for Marker
1/Marker 2/Docling evaluation and future reproducible conversions.

## Administrator tokens and browser security

Interactive access is OIDC-authenticated and re-authorized from current SCIM
state on every request. Console mutations require an exact same-origin
`Origin` header, while its document is private/no-store and uses same-origin
script/style/connect CSP directives.

Automation credentials are prefixed `bfa_`, shown once, stored only as a
SHA-256 hash, optionally expire, record last use, and can be revoked. They
currently carry the global `admin` role. The deployment bootstrap client token
also remains global admin for recovery and should be kept out of routine use.
Collection-scoped service accounts are still part of the access-control
roadmap; neither tags nor token labels are authorization boundaries.

Worker credentials are rejected by admin-role endpoints. Administrative
mutations record principal, action, target, detail, and timestamp in the local
append-only audit table. This audit is operational history, not yet a complete
tamper-evident security log.
