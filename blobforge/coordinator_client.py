"""HTTP client for the Cloudflare BlobForge coordination backend.

The coordinator owns manifests, persistent job state, leases, retries, worker
registration, progress, and operational configuration. S3 remains responsible
only for raw PDFs and converted output archives when this client is enabled.
"""

from __future__ import annotations

import json
import os
import socket
import urllib.error
import urllib.request
from typing import Any, Dict, Iterable, Optional


COORDINATOR_URL = os.getenv("BLOBFORGE_COORDINATOR_URL", "").rstrip("/")
COORDINATOR_TOKEN = os.getenv("BLOBFORGE_COORDINATOR_TOKEN", "")


class CoordinatorError(RuntimeError):
    """Raised when the coordination API rejects or cannot complete a request."""

    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class CoordinatorClient:
    """Small dependency-free client for workers, ingestion, and CLI reads."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.base_url = (base_url if base_url is not None else COORDINATOR_URL).rstrip("/")
        self.token = token if token is not None else COORDINATOR_TOKEN
        self.timeout = timeout

    @property
    def available(self) -> bool:
        return bool(self.base_url and self.token)

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        *,
        allow_empty: bool = False,
        token: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        request_token = token if token is not None else self.token
        if not self.base_url or not request_token:
            raise CoordinatorError(
                "Cloudflare coordinator is not configured; set "
                "BLOBFORGE_COORDINATOR_URL and BLOBFORGE_COORDINATOR_TOKEN"
            )
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {
            "Authorization": f"Bearer {request_token}",
            "Accept": "application/json",
        }
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                if response.status == 204 or allow_empty and not response.length:
                    return None
                payload = response.read()
                return json.loads(payload.decode("utf-8")) if payload else None
        except urllib.error.HTTPError as exc:
            payload = exc.read().decode("utf-8", errors="replace")
            try:
                message = json.loads(payload).get("error", payload)
            except (json.JSONDecodeError, AttributeError):
                message = payload or exc.reason
            raise CoordinatorError(str(message), status=exc.code) from exc
        except urllib.error.URLError as exc:
            raise CoordinatorError(f"Coordinator request failed: {exc.reason}") from exc

    def health(self) -> Dict[str, Any]:
        return self._request("GET", "/api/v1/health") or {}

    def get_config(self) -> Dict[str, Any]:
        return self._request("GET", "/api/v1/config") or {}

    def snapshot(self) -> Dict[str, Any]:
        return self._request("GET", "/api/v1/snapshot") or {}

    def import_items(
        self, items: Iterable[Dict[str, Any]], migration_token: str
    ) -> Dict[str, Any]:
        """Import a cutover batch using the separately scoped migration secret."""
        return self._request(
            "POST",
            "/api/v1/migration/import",
            {"items": list(items)},
            token=migration_token,
        ) or {}

    def enqueue(
        self,
        file_hash: str,
        *,
        priority: str,
        original_name: str,
        size_bytes: int,
        paths: Iterable[str],
        tags: Iterable[str],
        source: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self._request(
            "PUT",
            f"/api/v1/jobs/{file_hash}",
            {
                "priority": priority,
                "original_name": original_name,
                "size_bytes": size_bytes,
                "paths": list(paths),
                "tags": list(tags),
                "source": source,
            },
        ) or {}

    def get_job(self, file_hash: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v1/jobs/{file_hash}") or {}

    def register_worker(self, worker_id: str, metadata: Dict[str, Any]) -> None:
        payload = dict(metadata)
        payload.update({"worker_id": worker_id, "hostname": payload.get("hostname") or socket.gethostname()})
        self._request("POST", "/api/v1/workers/register", payload)

    def worker_heartbeat(
        self,
        worker_id: str,
        *,
        current_job: Optional[str],
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._request(
            "POST",
            "/api/v1/workers/heartbeat",
            {"worker_id": worker_id, "current_job": current_job, "metrics": metrics or {}},
        )

    def deregister_worker(self, worker_id: str) -> None:
        self._request("POST", "/api/v1/workers/deregister", {"worker_id": worker_id})

    def claim_job(self, worker_id: str, priorities: Iterable[str]) -> Optional[Dict[str, Any]]:
        return self._request(
            "POST",
            "/api/v1/jobs/claim",
            {"worker_id": worker_id, "priorities": list(priorities)},
            allow_empty=True,
        )

    def heartbeat(
        self,
        file_hash: str,
        *,
        worker_id: str,
        lease_token: str,
        progress: Dict[str, Any],
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._request(
            "POST",
            f"/api/v1/jobs/{file_hash}/heartbeat",
            {
                "worker_id": worker_id,
                "lease_token": lease_token,
                "progress": progress,
                "metrics": metrics or {},
            },
        )

    def complete(
        self,
        file_hash: str,
        *,
        worker_id: str,
        lease_token: str,
        result: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._request(
            "POST",
            f"/api/v1/jobs/{file_hash}/complete",
            {
                "worker_id": worker_id,
                "lease_token": lease_token,
                "result": result or {},
                "metrics": metrics or {},
            },
        )

    def fail(
        self,
        file_hash: str,
        *,
        worker_id: str,
        lease_token: str,
        error: str,
        traceback: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/jobs/{file_hash}/fail",
            {
                "worker_id": worker_id,
                "lease_token": lease_token,
                "error": error,
                "traceback": traceback,
                "context": context or {},
                "metrics": metrics or {},
            },
        ) or {}

    def release(
        self,
        file_hash: str,
        *,
        worker_id: str,
        lease_token: str,
        reason: str,
    ) -> None:
        self._request(
            "POST",
            f"/api/v1/jobs/{file_hash}/release",
            {"worker_id": worker_id, "lease_token": lease_token, "reason": reason},
        )
