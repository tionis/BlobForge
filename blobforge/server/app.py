"""FastAPI application for the self-hosted BlobForge backend."""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import secrets
import zipfile
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
from .database import Conflict, Database, now_ms, token_hash
from .management_ui import ASSET_VERSION, CSS as MANAGEMENT_CSS, JS as MANAGEMENT_JS, console_html
from .storage import CapabilitySigner, LocalStorage
from .scim import create_scim_router
from ..routing import RoutingFeatures, route_pdf
from ..mdaf import canonical_json_bytes, validate_mdaf
from ..recipe_lifecycle import RECIPE_MEMBER_PATH, recipe_digest


PRIORITIES = {"1_urgent", "2_high", "3_normal", "4_low"}
WORKER_ID = re.compile(r"^[a-z0-9][a-z0-9-]{1,62}$")


def _bearer(request: Request) -> str:
    value = request.headers.get("authorization", "")
    return value[7:] if value.startswith("Bearer ") else ""


def _digest(value: str, field: str = "digest") -> str:
    value = value.lower()
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise HTTPException(400, f"{field} must be 64 hexadecimal characters")
    return value


def _recipe_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}", value):
        raise HTTPException(400, "invalid recipe identifier")
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
        if worker:
            request.state.principal = {"id": f"worker:{worker_id}", "authentication": "worker", "roles": []}
            return worker_id
        principal: dict[str, Any] | None = None
        if token and secrets.compare_digest(token_hash(token), client_hash):
            principal = {"id": "token:bootstrap", "display_name": "Bootstrap token",
                         "authentication": "bootstrap_token", "roles": ["admin"], "groups": []}
        elif token:
            admin_token = database.admin_token(token)
            if admin_token:
                principal = {"id": f"token:{admin_token['id']}", "display_name": admin_token["label"],
                             "authentication": "admin_token", "roles": ["admin"], "groups": []}
            elif worker_id:
                if roles:
                    raise HTTPException(403, "worker credentials cannot access management operations")
                return worker_id
        if principal is None:
            subject = request.session.get("sub") if settings.oidc_enabled else None
            principal = database.oidc_principal(str(subject), settings.role_groups) if subject else None
            if not principal:
                raise HTTPException(401, "valid client token or provisioned OIDC session required")
            principal["authentication"] = "oidc"
        hierarchy = {"viewer": 1, "operator": 2, "admin": 3}
        required = min((hierarchy[role] for role in (roles or {"viewer"})), default=1)
        granted = max((hierarchy[role] for role in principal["roles"]), default=0)
        if granted < required:
            raise HTTPException(403, "insufficient BlobForge role")
        if (principal.get("authentication") == "oidc" and roles
                and request.method not in {"GET", "HEAD", "OPTIONS"}):
            expected_origin = (settings.public_url or str(request.base_url)).rstrip("/")
            if request.headers.get("origin", "").rstrip("/") != expected_origin:
                raise HTTPException(403, "session-authenticated mutation requires the configured same origin")
        request.state.principal = principal
        return worker_id

    def principal_id(request: Request) -> str:
        principal = getattr(request.state, "principal", None) or {}
        return str(principal.get("id") or "unknown")

    def base_url(request: Request) -> str:
        return (settings.public_url or str(request.base_url)).rstrip("/")

    def validate_stored_mdaf(path: Path):
        """Validate an extensionless content-addressed MDAF through a hard link."""
        alias = path.with_name(f".{path.name}.{secrets.token_hex(8)}.mdaf")
        os.link(path, alias)
        try:
            return validate_mdaf(alias)
        finally:
            alias.unlink(missing_ok=True)

    def capability_url(
        request: Request,
        method: str,
        scope: str,
        subject: str,
        path: str,
        extra: dict[str, str] | None = None,
        *,
        internal: bool = False,
    ) -> str:
        expires, signature = signer.issue(method, scope, subject)
        query = {"expires": str(expires), "signature": signature}
        query.update(extra or {})
        origin = str(request.base_url).rstrip("/") if internal else base_url(request)
        return f"{origin}{path}?{urlencode(query)}"

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
            authorize(request, roles={"admin"})
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
        return HTMLResponse(console_html(identity, list(principal.get("roles", [])) if principal else ["admin"]), headers={
            "Cache-Control": "private, no-store",
            "Content-Security-Policy": "default-src 'none'; style-src 'self'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
            "X-Content-Type-Options": "nosniff",
        })

    @app.get(f"/static/management-{ASSET_VERSION}.css")
    async def management_css() -> Response:
        return Response(MANAGEMENT_CSS, media_type="text/css", headers={"Cache-Control": "public, max-age=31536000, immutable"})

    @app.get(f"/static/management-{ASSET_VERSION}.js")
    async def management_js() -> Response:
        return Response(MANAGEMENT_JS, media_type="text/javascript", headers={"Cache-Control": "public, max-age=31536000, immutable"})

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

    @app.get("/api/v1/admin/overview")
    async def admin_overview(request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"})
        snap = database.snapshot()
        workers = database.workers()
        return {"counts": snap["counts"], "priority": snap["priority"],
                "workers": {"total": len(workers), "online": sum(1 for value in workers if not value["revoked"] and value["status"] != "offline")},
                "audit": database.audit_events(25), "generated_at": snap["generated_at"]}

    @app.get("/api/v1/admin/jobs")
    async def admin_jobs(request: Request, search: str = "", status: str = "", priority: str = "",
                         media_type: str = "", limit: int = 50, offset: int = 0) -> dict[str, Any]:
        authorize(request, roles={"admin"})
        if status and status not in {"todo", "processing", "failed", "dead", "done"}:
            raise HTTPException(400, "unsupported job status")
        if priority and priority not in PRIORITIES:
            raise HTTPException(400, "unsupported priority")
        return database.list_jobs(search=search[:200], status=status, priority=priority,
                                  media_type=media_type[:120], limit=max(1, min(limit, 200)), offset=max(0, offset))

    @app.post("/api/v1/admin/uploads")
    async def admin_upload(request: Request, filename: str, media_type: str = "application/octet-stream",
                           priority: str = "3_normal", tags: str = "",
                           recipe_digest: str = "") -> dict[str, Any]:
        authorize(request, roles={"admin"})
        filename = filename.strip()
        if not filename or len(filename) > 512:
            raise HTTPException(400, "filename must contain 1-512 characters")
        if priority not in PRIORITIES:
            raise HTTPException(400, "unsupported priority")
        temporary = storage.pending / "admin-upload" / secrets.token_urlsafe(18)
        result = await atomic_request_body(request, temporary)
        key = str(result["sha256"])
        destination = storage.source_path("sha256", key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            temporary.unlink(missing_ok=True)
        else:
            os.replace(temporary, destination)
        job = database.enqueue(key, {"digest_algorithm": "sha256", "digest": key,
            "media_type": media_type[:120], "original_name": filename,
            "size_bytes": int(result["size_bytes"]), "paths": [filename],
            "tags": [value.strip() for value in tags.split(",") if value.strip()][:100],
            "priority": priority, "source": "management-ui",
            "aliases": {"blake3": str(result["blake3"])}})
        if recipe_digest:
            job = database.request_conversion(
                key, _recipe_identifier(recipe_digest)
            )["job"]
        database.audit(principal_id(request), "job.upload", key,
                       {"filename": filename, "size_bytes": result["size_bytes"],
                        **({"recipe_digest": recipe_digest} if recipe_digest else {})})
        return job

    @app.get("/api/v1/admin/jobs/{key}")
    async def admin_job(key: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"})
        try:
            return {"job": database.get_job(key), "artifacts": database.artifacts(key),
                    "failures": database.job_failures(key),
                    "quota": database.quota_records(key)}
        except KeyError:
            raise HTTPException(404, "job not found") from None

    @app.get("/api/v1/admin/jobs/{key}/source-url")
    async def admin_source_url(key: str, request: Request) -> dict[str, str]:
        authorize(request, roles={"admin"})
        try: job = database.get_job(key)
        except KeyError: raise HTTPException(404, "job not found") from None
        algorithm = str(job["digest_algorithm"])
        digest = str(job["digest"])
        path = storage.source_path(algorithm, digest)
        if not path.is_file(): raise HTTPException(404, "source object missing")
        subject = f"{key}|{algorithm}|{digest}"
        return {"url": capability_url(
            request, "GET", "source", subject, f"/api/v1/transfers/sources/{key}",
            {"algorithm": algorithm, "digest": digest, "media_type": str(job["media_type"])},
        )}

    @app.patch("/api/v1/admin/jobs/{key}/priority")
    async def admin_priority(key: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"}); body = await request.json(); priority = str(body.get("priority") or "")
        if priority not in PRIORITIES:
            raise HTTPException(400, "priority must be 1_urgent, 2_high, 3_normal, or 4_low")
        try: job = database.set_priority(key, priority)
        except KeyError: raise HTTPException(404, "job not found") from None
        database.audit(principal_id(request), "job.priority", key, {"priority": priority})
        return job

    @app.post("/api/v1/admin/jobs/{key}/requeue")
    @app.post("/api/v1/admin/jobs/{key}/retry")
    async def admin_requeue(key: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"}); body = await request.json()
        try: job = database.requeue_job(key, reset_retries=bool(body.get("reset_retries")))
        except KeyError: raise HTTPException(404, "job not found") from None
        database.audit(principal_id(request), "job.requeue", key, {"reset_retries": bool(body.get("reset_retries"))})
        return job

    @app.delete("/api/v1/admin/jobs/{key}")
    async def admin_delete_job(key: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"})
        try: removed = database.delete_job(key)
        except KeyError: raise HTTPException(404, "job not found") from None
        source = removed["source"]
        paths = [storage.source_path(str(source["digest_algorithm"]), str(source["digest"]))]
        paths.extend(storage.root / str(value["storage_path"]) for value in removed["artifacts"])
        moved = storage.trash(paths, f"job-{key[:16]}")
        database.audit(principal_id(request), "job.delete", key, {"trash": moved})
        return {"deleted": True, "trash": moved}

    @app.get("/api/v1/admin/workers")
    async def admin_workers(request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"}); return {"workers": database.workers()}

    @app.post("/api/v1/admin/workers")
    async def admin_create_worker(request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"}); body = await request.json(); worker_id = str(body.get("worker_id") or "")
        if not WORKER_ID.fullmatch(worker_id):
            raise HTTPException(400, "worker_id must be 2-63 lowercase letters, digits, or hyphens")
        value = database.create_worker(worker_id); database.audit(principal_id(request), "worker.create", worker_id)
        return value

    @app.post("/api/v1/admin/workers/{worker_id}/token")
    async def admin_rotate_worker(worker_id: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"})
        try: value = database.rotate_worker_token(worker_id)
        except KeyError: raise HTTPException(404, "worker not found") from None
        database.audit(principal_id(request), "worker.rotate", worker_id); return value

    @app.post("/api/v1/admin/workers/{worker_id}/revoke")
    async def admin_revoke_worker(worker_id: str, request: Request) -> dict[str, bool]:
        authorize(request, roles={"admin"})
        try: database.revoke_worker(worker_id)
        except KeyError: raise HTTPException(404, "worker not found") from None
        database.audit(principal_id(request), "worker.revoke", worker_id); return {"revoked": True}

    @app.get("/api/v1/admin/tokens")
    async def admin_tokens(request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"}); return {"tokens": database.admin_tokens()}

    @app.post("/api/v1/admin/tokens")
    async def admin_create_token(request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"}); body = await request.json(); label = str(body.get("label") or "").strip()
        if not label or len(label) > 120: raise HTTPException(400, "label must contain 1-120 characters")
        days = body.get("expires_in_days")
        if days is not None and (not isinstance(days, int) or days < 1 or days > 3650):
            raise HTTPException(400, "expires_in_days must be between 1 and 3650")
        expires = now_ms() + days * 86400000 if days else None
        value = database.create_admin_token(label, expires); database.audit(principal_id(request), "token.create", value["id"], {"label": label})
        return value

    @app.post("/api/v1/admin/tokens/{identifier}/revoke")
    async def admin_revoke_token(identifier: str, request: Request) -> dict[str, bool]:
        authorize(request, roles={"admin"})
        try: database.revoke_admin_token(identifier)
        except KeyError: raise HTTPException(404, "active token not found") from None
        database.audit(principal_id(request), "token.revoke", identifier); return {"revoked": True}

    @app.patch("/api/v1/admin/recipes/{digest}")
    async def admin_update_recipe(digest: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"}); digest = _recipe_identifier(digest); body = await request.json()
        try: value = database.update_recipe(digest, body)
        except KeyError: raise HTTPException(404, "recipe not found") from None
        except ValueError as exc: raise HTTPException(400, str(exc)) from exc
        database.audit(principal_id(request), "recipe.update", digest,
                       {key: body[key] for key in ("enabled", "display_name") if key in body})
        return value

    @app.get("/api/v1/admin/quotas")
    async def admin_quotas(request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"})
        return database.quota_summary()

    @app.put("/api/v1/admin/provider-accounts/{account_key:path}")
    async def admin_provider_account(account_key: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"})
        body = await request.json()
        if "enabled" in body and not isinstance(body["enabled"], bool):
            raise HTTPException(400, "enabled must be a boolean")
        try:
            value = database.configure_provider_account(
                account_key,
                str(body.get("provider") or ""),
                enabled=bool(body.get("enabled", True)),
                concurrency_limit=body.get("concurrency_limit", 1),
                currency=str(body.get("currency") or "USD"),
            )
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        database.audit(principal_id(request), "quota.account.configure", account_key,
                       {"provider": value["provider"], "currency": value["currency"],
                        "enabled": value["enabled"],
                        "concurrency_limit": value["concurrency_limit"]})
        return value

    @app.put("/api/v1/admin/quota-schedules/{account_key:path}")
    async def admin_quota_schedule(account_key: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"})
        body = await request.json()
        if "enabled" in body and not isinstance(body["enabled"], bool):
            raise HTTPException(400, "enabled must be a boolean")
        try:
            value = database.configure_quota_schedule(
                account_key,
                timezone_name=str(body.get("timezone") or "Europe/Berlin"),
                reset_day=body.get("reset_day"),
                label=str(body.get("label") or ""),
                enabled=bool(body.get("enabled", True)),
                limit_requests=body.get("limit_requests"),
                limit_pages=body.get("limit_pages"),
                limit_estimated_micro_usd=body.get("limit_estimated_micro_usd"),
                limit_billed_micro_usd=body.get("limit_billed_micro_usd"),
            )
        except KeyError:
            raise HTTPException(404, "provider account not found") from None
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        database.audit(
            principal_id(request),
            "quota.schedule.configure",
            account_key,
            {key: value[key] for key in value if key not in {"created_at", "updated_at"}},
        )
        return value

    @app.post("/api/v1/admin/quota-policies")
    async def admin_quota_policy(request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"})
        body = await request.json()
        try:
            value = database.create_quota_policy(
                str(body.get("account_key") or ""),
                window_start=body.get("window_start"),
                window_end=body.get("window_end"),
                label=str(body.get("label") or ""),
                limit_requests=body.get("limit_requests"),
                limit_pages=body.get("limit_pages"),
                limit_estimated_micro_usd=body.get("limit_estimated_micro_usd"),
                limit_billed_micro_usd=body.get("limit_billed_micro_usd"),
            )
        except KeyError:
            raise HTTPException(404, "provider account not found") from None
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        database.audit(principal_id(request), "quota.policy.create", value["id"],
                       {key: value[key] for key in value if key != "created_at"})
        return value

    @app.post("/api/v1/admin/jobs/{key}/quota-overrides")
    async def admin_quota_override(key: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"})
        body = await request.json()
        if body.get("confirm") is not True:
            raise HTTPException(400, "confirm=true is required for a quota overage")
        try:
            value = database.create_quota_override(
                key,
                str(body.get("recipe_digest") or ""),
                extra_requests=body.get("extra_requests", 0),
                extra_pages=body.get("extra_pages", 0),
                extra_micro_usd=body.get("extra_micro_usd", 0),
                reason=str(body.get("reason") or ""),
                actor=principal_id(request),
                expires_at=body.get("expires_at"),
            )
        except KeyError:
            raise HTTPException(404, "job not found") from None
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        database.audit(principal_id(request), "quota.override.create", value["id"],
                       {"source_key": key, "recipe_digest": value["recipe_digest"],
                        "reason": value["reason"], "expires_at": value["expires_at"],
                        "extra_requests": value["extra_requests"],
                        "extra_pages": value["extra_pages"],
                        "extra_micro_usd": value["extra_micro_usd"]})
        return value

    @app.post("/api/v1/admin/quota-overrides/{identifier}/revoke")
    async def admin_revoke_quota_override(identifier: str, request: Request) -> dict[str, bool]:
        authorize(request, roles={"admin"})
        try:
            database.revoke_quota_override(identifier)
        except KeyError:
            raise HTTPException(404, "unused quota override not found") from None
        database.audit(principal_id(request), "quota.override.revoke", identifier)
        return {"revoked": True}

    @app.post("/api/v1/admin/quota-reservations/{identifier}/reconcile")
    async def admin_reconcile_quota(identifier: str, request: Request) -> dict[str, Any]:
        authorize(request, roles={"admin"})
        body = await request.json()
        suffix = f" [reconciled by {principal_id(request)}]"
        detail = f"{str(body.get('detail') or '')[:1000 - len(suffix)]}{suffix}"
        try:
            value = database.reconcile_quota(
                identifier,
                state=str(body.get("state") or ""),
                detail=detail,
                billed_micro_usd=body.get("billed_micro_usd"),
                credits_micro_usd=body.get("credits_micro_usd"),
            )
        except KeyError:
            raise HTTPException(404, "quota reservation not found") from None
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        database.audit(principal_id(request), "quota.reservation.reconcile", identifier,
                       {"state": value["state"], "detail": detail})
        return value

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
                "input_kinds": ["source"],
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
                "input_kinds": ["source"],
            }]
        try:
            job = database.claim(worker_id, list(body.get("priorities") or []), list(capabilities))
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, str(exc))
        if job:
            if job.get("input_kind") == "artifact":
                artifact_id = job.get("input_artifact_id")
                artifact = (
                    database.artifact_by_id(int(artifact_id))
                    if artifact_id is not None
                    else None
                )
                if not artifact or artifact["source_key"] != job["hash"]:
                    database.release(job["hash"], worker_id, job["lease_token"])
                    raise HTTPException(409, "reprocessing parent artifact is missing")
                subject = str(artifact["id"])
                job["input"] = {
                    "kind": "artifact",
                    "artifact_id": artifact["id"],
                    "artifact_identity": artifact["identity"],
                    "recipe_digest": artifact["recipe_digest"],
                    "media_type": artifact["media_type"],
                    "url": capability_url(
                        request,
                        "GET",
                        "artifact",
                        subject,
                        f"/api/v1/transfers/artifacts/{artifact['id']}",
                        internal=True,
                    ),
                }
            else:
                subject = f"{job['hash']}|{job['digest_algorithm']}|{job['digest']}"
                job["input"] = {
                    "kind": "source",
                    "media_type": job["media_type"],
                    "url": capability_url(
                        request,
                        "GET",
                        "source",
                        subject,
                        f"/api/v1/transfers/sources/{job['hash']}",
                        {
                            "algorithm": job["digest_algorithm"],
                            "digest": job["digest"],
                            "media_type": job["media_type"],
                        },
                        internal=True,
                    ),
                }
        return {"job": job, "config": runtime_config()}

    @app.post("/api/v1/jobs/{key}/heartbeat")
    async def job_heartbeat(key: str, request: Request) -> dict[str, Any]:
        worker_id = str(authorize(request, worker=True)); body = await request.json()
        database.heartbeat(key, worker_id, str(body.get("lease_token")), body.get("progress")); return {"ok": True, "config": runtime_config()}

    @app.post("/api/v1/jobs/{key}/quota-reservation")
    async def reserve_job_quota(key: str, request: Request) -> dict[str, Any]:
        worker_id = str(authorize(request, worker=True))
        body = await request.json()
        try:
            return database.reserve_quota(
                key, worker_id, str(body.get("lease_token") or ""), body
            )
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.post("/api/v1/quota-reservations/{identifier}/settle")
    async def settle_job_quota(identifier: str, request: Request) -> dict[str, Any]:
        worker_id = str(authorize(request, worker=True))
        body = await request.json()
        try:
            return database.settle_quota(identifier, worker_id, body)
        except KeyError:
            raise HTTPException(404, "quota reservation not found") from None
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.post("/api/v1/jobs/{key}/upload-url")
    async def output_upload_url(key: str, request: Request) -> dict[str, Any]:
        worker_id = str(authorize(request, worker=True)); body = await request.json(); lease = str(body.get("lease_token"))
        if not database.lease_valid(key, worker_id, lease): raise Conflict("lease is missing, expired, or owned by another worker")
        subject = f"{key}|{worker_id}|{lease}"
        return {"url": capability_url(request, "PUT", "output", subject, f"/api/v1/transfers/outputs/{key}", {"worker_id": worker_id, "lease_token": lease}, internal=True), "headers": {"Content-Type": "application/zip"}}

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
        result = body.get("result") or {}; job = database.get_job(key)
        recipe = str(job.get("recipe_digest") or "legacy")
        logical_identity = None
        if result.get("recipe_digest") and str(result["recipe_digest"]) != recipe:
            pending.unlink(missing_ok=True)
            raise HTTPException(422, "worker result recipe does not match the leased job")
        if str(result.get("artifact_type") or "legacy-archive") == "mdaf/v1":
            try:
                validated = validate_stored_mdaf(pending)
                if result.get("logical_identity") != validated.identity:
                    raise ValueError("reported logical identity does not match MDAF")
                logical_identity = validated.identity
                target_definition = job.get("recipe") or {}
                if target_definition.get("schema") == "dev.tionis.blobforge.recipe/v3":
                    with zipfile.ZipFile(pending) as archive:
                        embedded = json.loads(archive.read(RECIPE_MEMBER_PATH))
                    if recipe_digest(embedded) != recipe or canonical_json_bytes(
                        embedded
                    ) != canonical_json_bytes(target_definition):
                        raise ValueError("embedded lifecycle recipe does not match the lease")
                if job.get("input_kind") == "artifact":
                    parent = database.artifact_by_id(int(job["input_artifact_id"]))
                    if not parent or parent["source_key"] != key:
                        raise ValueError("reprocessing parent artifact is missing")
                    parent_path = settings.data_dir / parent["storage_path"]
                    parent_identity = validate_stored_mdaf(parent_path).identity
                    if parent_identity not in validated.manifest.get("derived_from", []):
                        raise ValueError("derivative does not declare its exact parent")
            except (OSError, KeyError, ValueError, zipfile.BadZipFile) as exc:
                pending.unlink(missing_ok=True)
                raise HTTPException(422, f"invalid MDAF output: {exc}") from exc
        inspected = storage.inspect(pending)
        identity = logical_identity or f"blake3:{inspected.blake3}"; destination = storage.artifact_path(key, recipe, identity); destination.parent.mkdir(parents=True, exist_ok=True)
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
        result = database.request_conversion(key, recipe)
        database.audit(principal_id(request), "job.convert", key, {"recipe_digest": recipe})
        return result

    @app.post("/api/v1/admin/reprocessing")
    async def plan_reprocessing(request: Request) -> dict[str, Any]:
        """Plan or atomically queue derivatives from immutable parent MDAFs."""
        authorize(request, roles={"operator"})
        body = await request.json()
        target = _recipe_identifier(str(body.get("target_recipe_digest") or ""))
        source = _recipe_identifier(str(body.get("source_recipe_digest") or ""))
        execute = body.get("execute", False)
        if not isinstance(execute, bool):
            raise HTTPException(400, "execute must be a boolean")
        keys = body.get("source_keys")
        if keys is not None and (
            not isinstance(keys, list)
            or len(keys) > 10_000
            or any(not isinstance(key, str) or not key for key in keys)
        ):
            raise HTTPException(400, "source_keys must be at most 10,000 non-empty strings")
        priority = body.get("priority")
        if priority is not None and priority not in PRIORITIES:
            raise HTTPException(400, "unsupported priority")
        try:
            result = database.plan_reprocessing(
                target,
                source,
                source_keys=keys,
                execute=execute,
                priority=priority,
            )
        except KeyError:
            raise HTTPException(404, "target recipe is not registered") from None
        except (Conflict, ValueError) as exc:
            raise HTTPException(409, str(exc)) from exc
        database.audit(
            principal_id(request),
            "artifact.reprocess.bulk" if execute else "artifact.reprocess.plan",
            target,
            {
                key: value
                for key, value in result.items()
                if key != "eligible_source_keys"
            },
        )
        return result

    @app.post("/api/v1/jobs/{key}/route")
    async def route_conversion(key: str, request: Request) -> dict[str, Any]:
        """Recompute, apply, and fully audit one versioned routing decision."""
        authorize(request, roles={"operator"})
        body = await request.json()
        try:
            job = database.get_job(key)
        except KeyError:
            raise HTTPException(404, "source not found") from None
        try:
            for field in (
                "allow_canary",
                "complex_tables",
                "equations",
                "external_processing_allowed",
            ):
                if field in body and not isinstance(body[field], bool):
                    raise ValueError(f"{field} must be a boolean")
            if isinstance(body.get("page_count"), bool) or not isinstance(
                body.get("page_count"), int
            ):
                raise ValueError("page_count must be an integer")
            features = RoutingFeatures(
                media_type=str(job["media_type"]),
                source_class=str(
                    body.get("source_class") or "born-digital-pnp-rulebook"
                ),
                page_count=body["page_count"],
                native_text_ratio=float(body["native_text_ratio"]),
                language=str(body.get("language") or "und"),
                quality_tier=str(body.get("quality_tier") or "quality"),
                layout_class=str(body.get("layout_class") or "standard"),
                complex_tables=bool(body.get("complex_tables")),
                equations=bool(body.get("equations")),
                external_processing_allowed=bool(
                    body.get("external_processing_allowed")
                ),
                max_cost_usd=(
                    float(body["max_cost_usd"])
                    if body.get("max_cost_usd") is not None
                    else None
                ),
            )
            decision = route_pdf(
                features,
                allow_canary=bool(body.get("allow_canary")),
                recipe_override=(
                    str(body["recipe_override"])
                    if body.get("recipe_override")
                    else None
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(400, f"invalid routing features: {exc}") from exc
        decision_json = decision.as_json()
        if not decision.eligible or not decision.recipe_digest:
            raise HTTPException(409, detail=decision_json)
        active = [
            recipe
            for recipe in database.recipes(str(job["media_type"]))
            if recipe["recipe_digest"] == decision.recipe_digest
            and recipe["enabled"]
            and recipe["worker_count"]
        ]
        if not active:
            decision_json["rationale"].append(
                "no active worker advertises the selected exact recipe"
            )
            decision_json["eligible"] = False
            raise HTTPException(409, detail=decision_json)
        result = database.request_conversion(key, decision.recipe_digest)
        database.audit(principal_id(request), "job.route", key, decision_json)
        return {"decision": decision_json, **result}

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
