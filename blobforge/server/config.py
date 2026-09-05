"""Configuration for the self-hosted BlobForge service."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class ServerSettings:
    data_dir: Path
    client_token: str
    worker_tokens: Mapping[str, str]
    public_url: str | None = None
    capability_ttl_seconds: int = 900
    lease_seconds: int = 900
    max_retries: int = 3
    fx_refresh_enabled: bool = True
    oidc_issuer: str | None = None
    oidc_client_id: str | None = None
    oidc_client_secret: str | None = None
    session_secret: str | None = None
    scim_token: str | None = None
    role_groups: Mapping[str, str] = field(default_factory=lambda: {
        "blobforge-admin": "admin",
        "blobforge-operator": "operator",
        "blobforge-viewer": "viewer",
    })

    @property
    def oidc_enabled(self) -> bool:
        return bool(self.oidc_issuer and self.oidc_client_id and self.oidc_client_secret)

    @classmethod
    def from_env(cls) -> "ServerSettings":
        raw_workers = os.getenv("BLOBFORGE_SERVER_WORKER_TOKENS", "{}")
        try:
            worker_tokens = json.loads(raw_workers)
        except json.JSONDecodeError as exc:
            raise ValueError("BLOBFORGE_SERVER_WORKER_TOKENS must be a JSON object") from exc
        if not isinstance(worker_tokens, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in worker_tokens.items()
        ):
            raise ValueError("BLOBFORGE_SERVER_WORKER_TOKENS must map worker IDs to tokens")
        client_token = os.getenv("BLOBFORGE_SERVER_CLIENT_TOKEN", "")
        if not client_token:
            raise ValueError("BLOBFORGE_SERVER_CLIENT_TOKEN is required")
        raw_role_groups = os.getenv("BLOBFORGE_SERVER_ROLE_GROUPS", "")
        role_groups = json.loads(raw_role_groups) if raw_role_groups else None
        if role_groups is not None and (not isinstance(role_groups, dict) or not all(
            isinstance(key, str) and value in {"viewer", "operator", "admin"}
            for key, value in role_groups.items()
        )):
            raise ValueError("BLOBFORGE_SERVER_ROLE_GROUPS must map group names to viewer, operator, or admin")
        settings = cls(
            data_dir=Path(os.getenv("BLOBFORGE_SERVER_DATA_DIR", "/var/lib/blobforge")),
            client_token=client_token,
            worker_tokens=worker_tokens,
            public_url=os.getenv("BLOBFORGE_SERVER_PUBLIC_URL") or None,
            capability_ttl_seconds=int(os.getenv("BLOBFORGE_SERVER_CAPABILITY_TTL", "900")),
            lease_seconds=int(os.getenv("BLOBFORGE_SERVER_LEASE_SECONDS", "900")),
            max_retries=int(os.getenv("BLOBFORGE_SERVER_MAX_RETRIES", "3")),
            fx_refresh_enabled=os.getenv("BLOBFORGE_SERVER_FX_REFRESH", "true").lower() not in {"false", "0", "no"},
            oidc_issuer=os.getenv("BLOBFORGE_SERVER_OIDC_ISSUER") or None,
            oidc_client_id=os.getenv("BLOBFORGE_SERVER_OIDC_CLIENT_ID") or None,
            oidc_client_secret=os.getenv("BLOBFORGE_SERVER_OIDC_CLIENT_SECRET") or None,
            session_secret=os.getenv("BLOBFORGE_SERVER_SESSION_SECRET") or None,
            scim_token=os.getenv("BLOBFORGE_SERVER_SCIM_TOKEN") or None,
            role_groups=role_groups or {"blobforge-admin": "admin", "blobforge-operator": "operator", "blobforge-viewer": "viewer"},
        )
        oidc_values = (settings.oidc_issuer, settings.oidc_client_id, settings.oidc_client_secret)
        if any(oidc_values) and not all(oidc_values):
            raise ValueError("OIDC issuer, client ID, and client secret must be configured together")
        if settings.oidc_enabled and not settings.session_secret:
            raise ValueError("BLOBFORGE_SERVER_SESSION_SECRET is required when OIDC is enabled")
        return settings
