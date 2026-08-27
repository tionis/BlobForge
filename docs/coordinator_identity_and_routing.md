# Coordinator identity and converter routing

## Decisions

The self-hosted coordinator treats a converter as a versioned capability, not
as a property of a worker process. A capability contains a backend name, exact
recipe digest and canonical recipe, accepted media types, and output artifact
type. A worker may advertise any number of capabilities. Claiming remains one
job at a time, so a constrained host can run a PDF conversion, then an audio
conversion, then another PDF conversion without dedicating one coordinator
identity per media type.

The current production worker advertises one capability: its exact Marker
recipe for `application/pdf`. This protocol work does not make the Marker
implementation capable of processing audio or video. New backends still need a
converter adapter and an isolated runtime/dispatcher. Isolation is important:
large ML stacks with incompatible dependencies must not be imported together,
and a native crash in one backend must not kill the worker supervisor.

## Selecting a conversion

`GET /api/v1/recipes?media_type=application/pdf` lists recipes advertised by
registered workers. A client can request an exact reproducible conversion:

```json
POST /api/v1/jobs/{source-key}/convert
{"recipe_digest":"<canonical recipe digest>"}
```

For interactive use it may instead name a backend:

```json
{"backend":"docling"}
```

Backend selection succeeds only when exactly one active recipe for that
backend and source media type exists. If Marker 1 and Marker 2 are both active,
the request is rejected as ambiguous and the caller must select the recipe
digest. The CLI equivalents are:

```bash
blobforge request-conversion SOURCE_HASH RECIPE_DIGEST
blobforge request-conversion SOURCE_HASH --backend docling
```

Jobs bind to the selected recipe when leased. The claim response identifies the
selected capability, preventing a multipurpose worker from dispatching a job
to the wrong adapter.

## Legacy provenance

All 1,377 staged legacy MDAFs preserve the old `content.md` and `info.json` in
the `dev.tionis.blobforge.legacy` rendition namespace. Their provenance names
the historical format (`blobforge-zip-v0`), recovered Marker conversion
activity, unavailable historical Marker/model version, exact BlobForge
migration producer version, BLAKE3 source identity plus verified SHA-256 alias,
and the `page-anchors-and-exact-toc-heading-alignment` mapping strategy. The
canonical migration recipe is
`blake3:8822289b4860301f73b64a2139a3559f2026793a48135fc13b83bc84a67b0c39`.

The local artifact catalog now also exposes `legacy=true`,
`converter_backend=marker`, `converter_version=unavailable`, and the recovered
provenance summary. This makes the distinction visible without opening each
MDAF. “Enriched with spans” is intentionally not a claim of complete mapping:
page anchors produce page spans where they existed, and otherwise only exact
TOC-to-heading evidence is recorded. Missing historical version or location
evidence remains `unavailable`; it is never guessed.

## OIDC and SCIM authorization

OIDC authenticates browser users. SCIM is authoritative for account lifecycle
and group membership. A successful OIDC callback is accepted only when the ID
token's validated `sub` equals the `externalId` of an active SCIM user that is
in a configured BlobForge role group. Every authenticated API request reloads
that SCIM record, so disabling or deleting the user immediately invalidates an
otherwise valid browser session. Access and refresh tokens are not persisted.

| SCIM group | BlobForge role |
|---|---|
| `blobforge-viewer` | read metadata and artifacts |
| `blobforge-operator` | viewer plus ingest and conversion requests |
| `blobforge-admin` | operator plus administration |

The mapping is configurable with `BLOBFORGE_SERVER_ROLE_GROUPS`. The static
client token remains a recovery/bootstrap admin mechanism. Revocable `bfa_`
admin tokens support automation, while per-worker `bfw_` credentials are bound
to one worker identity and cannot call admin-role routes. Cookie-authenticated
mutations additionally require a same-origin `Origin` header. The complete UI
and credential lifecycle is documented in `docs/management_console.md`.

OIDC uses discovery, Authorization Code flow, issuer/signature/audience/nonce
validation through Authlib, a Secure HttpOnly SameSite=Lax host-only session
cookie, and `/auth/login`, `/auth/callback`, `/auth/logout`, and `/api/v1/me`.
Browser navigation errors outside `/api/` and `/scim/` use private, no-store
HTML recovery pages with a fresh-login action. Machine-facing routes retain
structured JSON errors. Reused or expired OIDC authorization codes are handled
as a recoverable 400 response rather than leaking an exception or returning a
generic 500.

SCIM 2.0 is served below `/scim/v2` with a separate bearer token. It supports
service discovery and Users/Groups list, filter, create, replace, patch, and
delete operations. SCIM must be reachable from Authentik over the private
Podman network and blocked at public Caddy ingress.

Required runtime settings are shown in
`deploy/quadlet/blobforge.env.example`. The OIDC `sub`/SCIM `externalId`
contract is fail-closed. Authentik documents that its default SCIM
`externalId` matches the OIDC provider's default `hashed_user_id` subject mode,
so Gandalf should retain the existing Todo pattern for both providers. If the
subject mode or SCIM mappings are customized later, they must still produce the
same identifier.

## Citadel/Gandalf deployment contract

The Gandalf repository's `todo` role is the closest current pattern: it owns a
service instance, rootful Quadlet, dedicated network also joined by Caddy and
Authentik, exact Authentik OIDC/SCIM reconciliation, SCIM backchannel readiness,
and quiesced SQLite backups. BlobForge should follow that pattern with these
differences:

- digest-pinned server image, internal port 8080;
- state `/srv/blobforge` mounted at `/var/lib/blobforge`;
- public URL `https://blobforge.tionis.dev`;
- callback `https://blobforge.tionis.dev/auth/callback`;
- Authentik SCIM URL `http://blobforge:8080/scim/v2`;
- Caddy must reject `/scim/v2*` publicly and permit large streaming uploads;
- backup is one quiesced recovery unit containing SQLite, capability key, and
  every source/artifact object under `/srv/blobforge`;
- initial image deployment must pin an immutable digest even if later releases
  deliberately opt into Quadlet auto-update.

The canonical Gandalf changes belong in a new `blobforge` role,
`inventory/services/blobforge/{service,secrets}.yml`, Citadel playbook role
ordering before Caddy, the infrastructure endpoint/network attachments, and
Citadel backup policy. Generated host-var views must be produced with
`scripts/infra compile`, not edited by hand. The initial deployment uses
`ghcr.io/tionis/blobforge@sha256:97f764f71d329c25c0783617595d7ee4b3ec5c586a2e3d481d7612c0ab56f330`
(revision `6b8aa75`).
The coordinator, OIDC provider, and private SCIM integration are live and
healthy on Citadel. The only configured interactive role group is
`blobforge-admin`. Public DNS and TLS now terminate on Citadel; API health, the
OIDC redirect, public SCIM denial, and an existing ingress endpoint passed after
the approved Caddy restart. The first quiesced backup completed in 55 seconds;
the isolated restore recovered 32.165 GiB and passed SQLite verification in 201
seconds. Daily backup and weekly restore-test timers are enabled.

### Authentik filtered-SCIM membership caveat

Authentik 2026.8 dispatches membership changes immediately, but its outgoing
SCIM membership task assumes the affected user already has a provider-specific
SCIM mapping. A user's first addition to the application access group also
makes that user newly eligible for the filtered backchannel provider. In that
case the real-time task can finish successfully without first creating the
user, so it has no remote member ID to add. Authentik's full-sync safety net is
scheduled only every four hours; the Gandalf targeted reconciliation with
`blobforge_scim_force_sync=true` is the current immediate recovery operation.

Do not work around this by widening BlobForge's SCIM scope to every Authentik
user. The durable fix should preserve the access-group filter and order an
in-scope user sync before the membership update (preferably upstream in
Authentik), with a short bounded full-sync reconciliation only as a safety net.
