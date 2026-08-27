"""FastAPI application for the self-hosted BlobForge backend."""

from __future__ import annotations

import hashlib
import html
import json
import os
import secrets
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from blake3 import blake3
from authlib.integrations.base_client.errors import OAuthError
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from authlib.integrations.starlette_client import OAuth
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.sessions import SessionMiddleware

from .config import ServerSettings
from .database import Conflict, Database, token_hash
from .storage import CapabilitySigner, LocalStorage
from .scim import create_scim_router


def _bearer(request: Request) -> str:
    value = request.headers.get("authorization", "")
    return value[7:] if value.startswith("Bearer ") else ""


def _digest(value: str, field: str = "digest") -> str:
    value = value.lower()
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise HTTPException(400, f"{field} must be 64 hexadecimal characters")
    return value


def _browser_request(request: Request) -> bool:
    """Return true only for browser navigation outside machine-facing APIs."""
    return (
        request.method in {"GET", "HEAD"}
        and "text/html" in request.headers.get("accept", "").lower()
        and not request.url.path.startswith(("/api/", "/scim/"))
    )


def _browser_error(status: int, detail: str) -> HTMLResponse:
    titles = {
        400: "Sign-in could not be completed",
        401: "Sign-in required",
        403: "Access is not provisioned",
        404: "Page not found",
    }
    title = titles.get(status, "Something went wrong")
    retry = status in {400, 401, 403}
    action_href = "/auth/login" if retry else "/"
    action_label = "Start a new sign-in" if retry else "Return to BlobForge"
    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{status} · BlobForge</title><style>
:root{{color-scheme:dark;background:#101316;color:#f3f5f7;font:16px/1.5 system-ui,sans-serif}}body{{margin:0}}
main{{max-width:46rem;margin:auto;padding:clamp(2rem,8vw,7rem)}}.eyebrow{{color:#8be0bd;text-transform:uppercase;letter-spacing:.16em;font-weight:700}}
h1{{font-size:clamp(2.4rem,7vw,5rem);line-height:1;margin:.2em 0;letter-spacing:-.055em}}p{{color:#b8c1ca;font-size:1.08rem;max-width:40rem}}
a{{display:inline-block;margin-top:1.5rem;color:#101316;background:#8be0bd;padding:.75rem 1rem;border-radius:.65rem;text-decoration:none;font-weight:700}}
code{{color:#8be0bd}}</style></head><body><main><p class="eyebrow">BlobForge · HTTP {status}</p>
<h1>{html.escape(title)}</h1><p>{html.escape(detail)}</p><a href="{action_href}">{action_label}</a>
</main></body></html>"""
    return HTMLResponse(page, status_code=status, headers={
        "Cache-Control": "private, no-store",
        "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
        "X-Content-Type-Options": "nosniff",
    })


def create_app(settings: ServerSettings | None = None) -> FastAPI:
    settings = settings or ServerSettings.from_env()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    database = Database(
        settings.data_dir / "blobforge.sqlite3",
        lease_seconds=settings.lease_seconds,
        max_retries=settings.max_retries,
    )
    database.bootstrap_workers(settings.worker_tokens)
    storage = LocalStorage(settings.data_dir)
    signer = CapabilitySigner(settings.data_dir / "capability.key", settings.capability_ttl_seconds)
    client_hash = token_hash(settings.client_token)
    app = FastAPI(title="BlobForge", version="1")
    app.state.settings = settings
    app.state.database = database
    app.state.storage = storage
    oauth: OAuth | None = None
    if settings.oidc_enabled:
        app.add_middleware(
            SessionMiddleware,
            secret_key=str(settings.session_secret),
            session_cookie="__Host-blobforge_session",
            same_site="lax",
            https_only=True,
        )
        oauth = OAuth()
        oauth.register(
            name="oidc",
            server_metadata_url=f"{settings.oidc_issuer.rstrip('/')}/.well-known/openid-configuration",
            client_id=settings.oidc_client_id,
            client_secret=settings.oidc_client_secret,
            client_kwargs={"scope": "openid profile email"},
        )
    if settings.scim_token:
        app.include_router(create_scim_router(database, settings.scim_token))

    @app.exception_handler(StarletteHTTPException)
    async def friendly_http_error(request: Request, exc: StarletteHTTPException) -> Response:
        if _browser_request(request):
            detail = str(exc.detail) if isinstance(exc.detail, str) else "The request could not be completed."
            return _browser_error(exc.status_code, detail)
        return await http_exception_handler(request, exc)

    def authorize(request: Request, *, worker: bool = False, roles: set[str] | None = None) -> str | None:
        token = _bearer(request)
        worker_id = database.worker_for_token(token) if token else None
        if worker and not worker_id:
            raise HTTPException(401, "valid worker token required")
        if not worker:
            if worker_id or (token and secrets.compare_digest(token_hash(token), client_hash)):
                return worker_id
            subject = request.session.get("sub") if settings.oidc_enabled else None
            principal = database.oidc_principal(str(subject), settings.role_groups) if subject else None
            if not principal:
                raise HTTPException(401, "valid client token or provisioned OIDC session required")
            hierarchy = {"viewer": 1, "operator": 2, "admin": 3}
            required = min((hierarchy[role] for role in (roles or {"viewer"})), default=1)
            granted = max((hierarchy[role] for role in principal["roles"]), default=0)
            if granted < required:
                raise HTTPException(403, "insufficient BlobForge role")
            if roles and request.method not in {"GET", "HEAD", "OPTIONS"}:
                expected_origin = (settings.public_url or str(request.base_url)).rstrip("/")
                if request.headers.get("origin", "").rstrip("/") != expected_origin:
                    raise HTTPException(403, "session-authenticated mutation requires the configured same origin")
            request.state.principal = principal
        return worker_id

    def base_url(request: Request) -> str:
        return (settings.public_url or str(request.base_url)).rstrip("/")

    def capability_url(request: Request, method: str, scope: str, subject: str, path: str, extra: dict[str, str] | None = None) -> str:
        expires, signature = signer.issue(method, scope, subject)
        query = {"expires": str(expires), "signature": signature}
        query.update(extra or {})
        return f"{base_url(request)}{path}?{urlencode(query)}"

    def verify_capability(request: Request, method: str, scope: str, subject: str) -> None:
        try:
            expires = int(request.query_params.get("expires", "0"))
        except ValueError:
            raise HTTPException(403, "invalid transfer capability")
        if not signer.verify(method, scope, subject, expires, request.query_params.get("signature", "")):
            raise HTTPException(403, "invalid or expired transfer capability")

    async def atomic_request_body(request: Request, destination: Path) -> dict[str, Any]:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{secrets.token_hex(8)}.tmp")
        sha = hashlib.sha256(); b3 = blake3(); size = 0
        try:
            with temporary.open("xb") as target:
                async for chunk in request.stream():
                    target.write(chunk); sha.update(chunk); b3.update(chunk); size += len(chunk)
                target.flush(); os.fsync(target.fileno())
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return {"storage_path": str(destination), "size_bytes": size, "sha256": sha.hexdigest(), "blake3": b3.hexdigest()}

    def local_file_response(path: Path, media_type: str, filename: str | None = None) -> StreamingResponse:
        async def body():
            with path.open("rb") as stream:
                while chunk := stream.read(1024 * 1024):
                    yield chunk
        headers = {"Content-Length": str(path.stat().st_size)}
        if filename:
            headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return StreamingResponse(body(), media_type=media_type, headers=headers)

    @app.exception_handler(Conflict)
    async def conflict_handler(_request: Request, exc: Conflict) -> Response:
        return Response(json.dumps({"error": str(exc)}), status_code=409, media_type="application/json")

    def runtime_config() -> dict[str, Any]:
        return {"lease_timeout": settings.lease_seconds, "max_retries": settings.max_retries, "heartbeat_interval": max(10, settings.lease_seconds // 3)}

    @app.get("/api/v1/health")
    async def health() -> dict[str, Any]:
        return {"ok": True, "backend": "sqlite-filesystem", "schema": 1}

    @app.get("/", response_class=HTMLResponse)
    async def landing(request: Request) -> Response:
        try:
            authorize(request)
        except HTTPException as exc:
            if exc.status_code == 401 and settings.oidc_enabled:
                return RedirectResponse("/auth/login")
            raise
        principal = getattr(request.state, "principal", None)
        identity = (
            str(principal.get("display_name") or principal.get("user_name") or "OIDC user")
            if principal
            else "API token"
        )
        roles = ", ".join(str(role) for role in principal.get("roles", [])) if principal else "service administrator"
        counts = database.snapshot()["counts"]
        count_cards = "".join(
            f"<li><strong>{int(value):,}</strong><span>{html.escape(str(label).replace('_', ' '))}</span></li>"
            for label, value in sorted(counts.items())
        )
        page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BlobForge</title><style>
:root{{color-scheme:dark;background:#101316;color:#f3f5f7;font:16px/1.5 system-ui,sans-serif}}body{{margin:0}}
main{{max-width:72rem;margin:auto;padding:clamp(2rem,6vw,6rem)}}h1{{font-size:clamp(2.5rem,8vw,6rem);margin:0;letter-spacing:-.06em}}
.eyebrow{{color:#8be0bd;text-transform:uppercase;letter-spacing:.16em;font-weight:700}}.muted{{color:#a9b2bc}}
ul{{display:grid;grid-template-columns:repeat(auto-fit,minmax(9rem,1fr));gap:1rem;padding:0;margin:2.5rem 0;list-style:none}}
li{{background:#1a2026;border:1px solid #303942;border-radius:1rem;padding:1.25rem}}li strong{{display:block;font-size:1.8rem}}li span{{color:#a9b2bc}}
nav{{display:flex;flex-wrap:wrap;gap:.75rem}}a{{color:#101316;background:#8be0bd;padding:.7rem 1rem;border-radius:.65rem;text-decoration:none;font-weight:700}}
a.secondary{{color:#dce3e9;background:#242c33}}code{{color:#8be0bd}}</style></head>
<body><main><p class="eyebrow">Coordinator online</p><h1>BlobForge</h1>
<p>Signed in as <strong>{html.escape(identity)}</strong> · {html.escape(roles)}</p>
<p class="muted">Self-hosted media conversion coordination and immutable MDAF artifact storage.</p>
<ul>{count_cards}</ul><nav><a href="/docs">API documentation</a><a class="secondary" href="/api/v1/snapshot">Snapshot JSON</a><a class="secondary" href="/api/v1/recipes">Conversion recipes</a></nav>
<p class="muted">The full browser file library and token-management console are not implemented yet.</p>
</main></body></html>"""
        return HTMLResponse(page, headers={
            "Cache-Control": "private, no-store",
            "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
            "X-Content-Type-Options": "nosniff",
        })

    @app.get("/auth/login")
    async def oidc_login(request: Request) -> Response:
        if not oauth:
            raise HTTPException(404, "OIDC is not configured")
        redirect_uri = f"{base_url(request)}/auth/callback"
        return await oauth.oidc.authorize_redirect(request, redirect_uri)  # type: ignore[union-attr]

    @app.get("/auth/callback")
    async def oidc_callback(request: Request) -> Response:
        if not oauth:
            raise HTTPException(404, "OIDC is not configured")
        try:
            token = await oauth.oidc.authorize_access_token(request)  # type: ignore[union-attr]
        except OAuthError as exc:
            request.session.clear()
            raise HTTPException(
                400,
                "This sign-in request expired or was already used. Start a new sign-in.",
            ) from exc
        userinfo = token.get("userinfo") or {}
        subject = str(userinfo.get("sub") or "")
        if not subject or not database.oidc_principal(subject, settings.role_groups):
            request.session.clear()
            raise HTTPException(
                403,
                "Your identity was verified, but BlobForge has not received an active authorized membership from SCIM. If access was just granted, start a new sign-in shortly; otherwise contact an administrator.",
            )
        request.session.clear(); request.session["sub"] = subject
        return RedirectResponse("/")

    @app.post("/auth/logout")
    async def oidc_logout(request: Request) -> dict[str, bool]:
        if not settings.oidc_enabled:
            raise HTTPException(404, "OIDC is not configured")
        request.session.clear()
        return {"ok": True}

    @app.get("/api/v1/me")
    async def me(request: Request) -> dict[str, Any]:
        authorize(request)
        principal = getattr(request.state, "principal", None)
        return principal or {"authentication": "token"}

    @app.get("/api/v1/config")
    async def config(request: Request) -> dict[str, Any]:
        authorize(request)
        return runtime_config()

    @app.get("/api/v1/recipes")
    async def recipes(request: Request, media_type: str | None = None) -> dict[str, Any]:
        authorize(request)
        return {"recipes": database.recipes(media_type)}

    @app.get("/api/v1/snapshot")
    async def snapshot(request: Request) -> dict[str, Any]:
        authorize(request)
        return database.snapshot()

    @app.post("/api/v1/jobs/status")
    async def statuses(request: Request) -> dict[str, Any]:
        authorize(request); body = await request.json(); keys = body.get("hashes") or []
        if not isinstance(keys, list) or len(keys) > 5000: raise HTTPException(400, "hashes must contain at most 5000 entries")
        return {"results": database.statuses([str(key) for key in keys])}

    @app.get("/api/v1/jobs/done-since")
    async def done_since(request: Request, since: int = 0, cursor: str = "", limit: int = 5000) -> dict[str, Any]:
        authorize(request); limit = max(1, min(limit, 20000)); cursor_value = int(cursor or 0)
        with database.connect() as db:
            rows = list(db.execute("SELECT source_key,completed_at,done_seq FROM jobs WHERE status='done' AND done_seq>? ORDER BY done_seq LIMIT ?", (cursor_value, limit)))
        next_cursor = str(rows[-1]["done_seq"] if rows else cursor_value)
        next_since = int(rows[-1]["completed_at"] if rows else since)
        return {"hashes": [row["source_key"] for row in rows], "next_since": next_since, "next_cursor": next_cursor, "complete": len(rows) < limit}

    @app.put("/api/v1/jobs/{key}")
    async def enqueue(key: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"operator"}); body = await request.json(); key = _digest(key, "source key")
        algorithm = str(body.get("digest_algorithm") or "sha256")
        if algorithm not in {"sha256", "blake3"}: raise HTTPException(400, "unsupported digest algorithm")
        digest = _digest(str(body.get("digest") or key))
        body["digest_algorithm"] = algorithm; body["digest"] = digest
        if not storage.source_path(algorithm, digest).is_file():
            raise HTTPException(409, "source object must be uploaded before enqueue")
        return database.enqueue(key, body)

    @app.get("/api/v1/jobs/{key}")
    async def get_job(key: str, request: Request) -> dict[str, Any]:
        authorize(request)
        try: return database.get_job(key)
        except KeyError: raise HTTPException(404, "source not found")

    @app.post("/api/v1/jobs/{key}/raw-upload-url")
    async def raw_upload_url(key: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"operator"}); body = await request.json(); key = _digest(key, "source key")
        algorithm = str(body.get("digest_algorithm") or "sha256")
        if algorithm not in {"sha256", "blake3"}: raise HTTPException(400, "unsupported digest algorithm")
        digest = _digest(str(body.get("digest") or key))
        path = storage.source_path(algorithm, digest); subject = f"{key}|{algorithm}|{digest}"
        return {"url": capability_url(request, "PUT", "source", subject, f"/api/v1/transfers/sources/{key}", {"algorithm": algorithm, "digest": digest}), "already_exists": path.is_file(), "headers": {"Content-Type": str(body.get("media_type") or "application/octet-stream")}}

    @app.put("/api/v1/transfers/sources/{key}")
    async def put_source(key: str, request: Request) -> dict[str, Any]:
        algorithm = request.query_params.get("algorithm", "sha256"); digest = request.query_params.get("digest", key); subject = f"{key}|{algorithm}|{digest}"
        verify_capability(request, "PUT", "source", subject)
        result = await atomic_request_body(request, storage.source_path(algorithm, digest))
        actual = result.get(algorithm)
        if actual != digest:
            Path(result["storage_path"]).unlink(missing_ok=True)
            raise HTTPException(422, f"{algorithm} digest mismatch")
        return {"ok": True, "size_bytes": result["size_bytes"]}

    @app.get("/api/v1/transfers/sources/{key}")
    async def get_source(key: str, request: Request) -> StreamingResponse:
        algorithm = request.query_params.get("algorithm", "sha256"); digest = request.query_params.get("digest", key); subject = f"{key}|{algorithm}|{digest}"
        verify_capability(request, "GET", "source", subject); path = storage.source_path(algorithm, digest)
        if not path.is_file(): raise HTTPException(404, "source object missing")
        return local_file_response(path, request.query_params.get("media_type", "application/octet-stream"))

    @app.get("/api/v1/workers/me")
    async def worker_me(request: Request) -> dict[str, str]:
        return {"worker_id": str(authorize(request, worker=True))}

    @app.post("/api/v1/workers/register")
    async def register(request: Request) -> dict[str, Any]:
        worker_id = str(authorize(request, worker=True)); body = await request.json()
        if body.get("worker_id") != worker_id: raise HTTPException(403, "token is bound to another worker")
        timestamp = int(__import__("time").time() * 1000)
        with database.transaction() as db:
            db.execute("UPDATE workers SET hostname=?,status='idle',metadata_json=?,last_seen=? WHERE worker_id=?", (body.get("hostname"), json.dumps(body), timestamp, worker_id))
        capabilities = body.get("capabilities")
        if capabilities is None and body.get("conversion_recipe_digest"):
            capabilities = [{
                "backend": (body.get("conversion_recipe") or {}).get("engine", "marker"),
                "recipe_digest": body["conversion_recipe_digest"],
                "recipe": body.get("conversion_recipe") or {},
                "media_types": body.get("accepted_media_types") or ["application/pdf"],
                "artifact_type": "legacy-archive",
            }]
        try:
            registered = database.register_capabilities(worker_id, list(capabilities or []))
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, str(exc))
        return {"worker_id": worker_id, "capabilities": registered, "config": runtime_config()}

    @app.post("/api/v1/workers/heartbeat")
    @app.post("/api/v1/workers/state")
    async def worker_update(request: Request) -> dict[str, Any]:
        worker_id = str(authorize(request, worker=True)); body = await request.json()
        if body.get("worker_id") != worker_id: raise HTTPException(403, "token is bound to another worker")
        status = body.get("status") or ("working" if body.get("current_job") else "idle")
        with database.transaction() as db:
            db.execute("UPDATE workers SET status=?,current_job=?,last_seen=?,metadata_json=? WHERE worker_id=?", (status, body.get("current_job"), int(__import__("time").time()*1000), json.dumps(body), worker_id))
        return {"ok": True, "config": runtime_config()}

    @app.post("/api/v1/workers/deregister")
    async def deregister(request: Request) -> dict[str, bool]:
        worker_id = str(authorize(request, worker=True)); body = await request.json()
        if body.get("worker_id") != worker_id: raise HTTPException(403, "token is bound to another worker")
        with database.transaction() as db: db.execute("UPDATE workers SET status='offline',current_job=NULL,last_seen=? WHERE worker_id=?", (int(__import__("time").time()*1000), worker_id))
        return {"ok": True}

    @app.post("/api/v1/jobs/claim")
    async def claim(request: Request) -> dict[str, Any]:
        worker_id = str(authorize(request, worker=True)); body = await request.json()
        if body.get("worker_id") != worker_id: raise HTTPException(403, "token is bound to another worker")
        capabilities = body.get("capabilities")
        if capabilities is None:
            capabilities = [{
                "backend": (body.get("recipe") or {}).get("engine", "marker"),
                "recipe_digest": body.get("recipe_digest") or "unversioned-marker",
                "recipe": body.get("recipe") or {},
                "media_types": body.get("accepted_media_types") or ["application/pdf"],
                "artifact_type": "legacy-archive",
            }]
        try:
            job = database.claim(worker_id, list(body.get("priorities") or []), list(capabilities))
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, str(exc))
        if job:
            subject = f"{job['hash']}|{job['digest_algorithm']}|{job['digest']}"
            job["input"] = {"url": capability_url(request, "GET", "source", subject, f"/api/v1/transfers/sources/{job['hash']}", {"algorithm": job["digest_algorithm"], "digest": job["digest"], "media_type": job["media_type"]})}
        return {"job": job, "config": runtime_config()}

    @app.post("/api/v1/jobs/{key}/heartbeat")
    async def job_heartbeat(key: str, request: Request) -> dict[str, Any]:
        worker_id = str(authorize(request, worker=True)); body = await request.json()
        database.heartbeat(key, worker_id, str(body.get("lease_token")), body.get("progress")); return {"ok": True, "config": runtime_config()}

    @app.post("/api/v1/jobs/{key}/upload-url")
    async def output_upload_url(key: str, request: Request) -> dict[str, Any]:
        worker_id = str(authorize(request, worker=True)); body = await request.json(); lease = str(body.get("lease_token"))
        if not database.lease_valid(key, worker_id, lease): raise Conflict("lease is missing, expired, or owned by another worker")
        subject = f"{key}|{worker_id}|{lease}"
        return {"url": capability_url(request, "PUT", "output", subject, f"/api/v1/transfers/outputs/{key}", {"worker_id": worker_id, "lease_token": lease}), "headers": {"Content-Type": "application/zip"}}

    @app.put("/api/v1/transfers/outputs/{key}")
    async def put_output(key: str, request: Request) -> dict[str, Any]:
        worker_id = request.query_params.get("worker_id", ""); lease = request.query_params.get("lease_token", ""); subject = f"{key}|{worker_id}|{lease}"
        verify_capability(request, "PUT", "output", subject)
        if not database.lease_valid(key, worker_id, lease): raise Conflict("lease is missing, expired, or owned by another worker")
        result = await atomic_request_body(request, storage.pending_output_path(key, lease)); return {"ok": True, **result}

    @app.post("/api/v1/jobs/{key}/complete")
    async def complete(key: str, request: Request) -> dict[str, bool]:
        worker_id = str(authorize(request, worker=True)); body = await request.json(); lease = str(body.get("lease_token")); pending = storage.pending_output_path(key, lease)
        if not pending.is_file(): raise HTTPException(409, "uploaded output is missing")
        inspected = storage.inspect(pending); result = body.get("result") or {}; recipe = str(result.get("recipe_digest") or database.get_job(key).get("recipe_digest") or "legacy")
        identity = f"blake3:{inspected.blake3}"; destination = storage.artifact_path(key, recipe, identity); destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(pending, destination)
        artifact = {"identity": identity, "storage_path": str(destination.relative_to(settings.data_dir)), "media_type": str(result.get("media_type") or "application/zip"), "artifact_type": str(result.get("artifact_type") or "legacy-archive"), "size_bytes": inspected.size, "sha256": inspected.sha256, "blake3": inspected.blake3}
        try: database.complete(key, worker_id, lease, artifact, result)
        except Exception:
            if destination.exists() and not pending.exists(): os.replace(destination, pending)
            raise
        return {"ok": True}

    @app.post("/api/v1/jobs/{key}/fail")
    async def fail(key: str, request: Request) -> dict[str, str]:
        worker_id = str(authorize(request, worker=True)); body = await request.json(); return {"status": database.fail(key, worker_id, str(body.get("lease_token")), body)}

    @app.post("/api/v1/jobs/{key}/release")
    async def release(key: str, request: Request) -> dict[str, bool]:
        worker_id = str(authorize(request, worker=True)); body = await request.json(); database.release(key, worker_id, str(body.get("lease_token"))); return {"ok": True}

    @app.get("/api/v1/jobs/{key}/artifacts")
    async def artifacts(key: str, request: Request) -> dict[str, Any]:
        authorize(request); return {"artifacts": database.artifacts(key)}

    @app.post("/api/v1/jobs/{key}/convert")
    async def request_conversion(key: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"operator"}); body = await request.json(); recipe = str(body.get("recipe_digest") or "")
        try:
            job = database.get_job(key)
        except KeyError:
            raise HTTPException(404, "source not found")
        if not recipe and body.get("backend"):
            try:
                recipe = database.resolve_backend(str(body["backend"]), str(job["media_type"]))
            except KeyError:
                raise HTTPException(404, "no active recipe for that backend and media type")
        if not recipe:
            raise HTTPException(400, "recipe_digest or backend is required")
        return database.request_conversion(key, recipe)

    @app.post("/api/v1/jobs/{key}/download-url")
    async def download_url(key: str, request: Request) -> dict[str, str]:
        authorize(request); body = await request.json(); artifact = database.artifact(key, body.get("recipe_digest"))
        if not artifact: raise HTTPException(404, "artifact not found")
        subject = str(artifact["id"]); return {"url": capability_url(request, "GET", "artifact", subject, f"/api/v1/transfers/artifacts/{artifact['id']}")}

    @app.get("/api/v1/transfers/artifacts/{artifact_id}")
    async def get_artifact(artifact_id: int, request: Request) -> StreamingResponse:
        verify_capability(request, "GET", "artifact", str(artifact_id))
        with database.connect() as db: row = db.execute("SELECT * FROM artifacts WHERE id=?", (artifact_id,)).fetchone()
        if not row: raise HTTPException(404, "artifact not found")
        path = settings.data_dir / row["storage_path"]
        if not path.is_file(): raise HTTPException(404, "artifact object missing")
        return local_file_response(path, row["media_type"], Path(path).name)

    return app
