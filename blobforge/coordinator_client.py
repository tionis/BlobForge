"""HTTP client for the Bunny BlobForge coordination backend.

The coordinator owns file metadata, persistent job state, leases, retries, worker
registration, progress, and operational configuration. S3 remains responsible
only for raw PDFs and converted output archives when this client is enabled.
"""

from __future__ import annotations

import json
import http.client
import os
import shutil
import socket
import urllib.error
import urllib.request
from urllib.parse import urlsplit
from typing import Any, Dict, Iterable, Optional


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
        self.base_url = (base_url if base_url is not None else os.getenv("BLOBFORGE_COORDINATOR_URL", "")).rstrip("/")
        self.token = token if token is not None else os.getenv("BLOBFORGE_COORDINATOR_TOKEN", "")
        self.timeout = timeout
        self.runtime_config: Dict[str, Any] = {}

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
                "Bunny coordinator is not configured; set "
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
                result = json.loads(payload.decode("utf-8")) if payload else None
                if isinstance(result, dict) and isinstance(result.get("config"), dict):
                    self.runtime_config = dict(result["config"])
                return result
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

    def worker_identity(self) -> str:
        payload = self._request("GET", "/api/v1/workers/me") or {}
        worker_id = str(payload.get("worker_id") or "")
        if not worker_id:
            raise CoordinatorError("Coordinator did not return a worker identity")
        return worker_id

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

    def register_worker(self, worker_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(metadata)
        payload.update({"worker_id": worker_id, "hostname": payload.get("hostname") or socket.gethostname()})
        return self._request("POST", "/api/v1/workers/register", payload) or {}

    def worker_heartbeat(
        self,
        worker_id: str,
        *,
        current_job: Optional[str],
        metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/api/v1/workers/heartbeat",
            {"worker_id": worker_id, "current_job": current_job, "metrics": metrics or {}},
        ) or {}

    def worker_state(
        self,
        worker_id: str,
        *,
        status: str,
        detail: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Publish a one-shot worker lifecycle state transition."""
        return self._request(
            "POST",
            "/api/v1/workers/state",
            {"worker_id": worker_id, "status": status, "detail": detail or {}},
        ) or {}

    def deregister_worker(self, worker_id: str) -> None:
        self._request("POST", "/api/v1/workers/deregister", {"worker_id": worker_id})

    def claim_job(self, worker_id: str, priorities: Iterable[str]) -> Optional[Dict[str, Any]]:
        payload = self._request(
            "POST",
            "/api/v1/jobs/claim",
            {"worker_id": worker_id, "priorities": list(priorities)},
        )
        if not isinstance(payload, dict) or "job" not in payload:
            raise CoordinatorError("Coordinator returned an invalid claim response")
        job = payload.get("job")
        return job if isinstance(job, dict) else None

    def download_job_input(self, job: Dict[str, Any], local_path: str) -> None:
        """Download the claimed PDF through its coordinator-issued signed URL."""
        transfer = job.get("input") or {}
        url = str(transfer.get("url") or "")
        if not url:
            raise CoordinatorError("Coordinator claim did not include an input URL")
        try:
            request = urllib.request.Request(url, headers={"Accept": "application/pdf"})
            with urllib.request.urlopen(request, timeout=self.timeout) as response, open(local_path, "wb") as target:
                shutil.copyfileobj(response, target, length=1024 * 1024)
        except (urllib.error.URLError, OSError) as exc:
            raise CoordinatorError(f"Input download failed: {exc}") from exc

    def upload_job_output(
        self,
        file_hash: str,
        local_path: str,
        *,
        worker_id: str,
        lease_token: str,
    ) -> None:
        """Request a fresh lease-bound signed URL and stream the result archive."""
        transfer = self._request(
            "POST",
            f"/api/v1/jobs/{file_hash}/upload-url",
            {"worker_id": worker_id, "lease_token": lease_token},
        ) or {}
        url = str(transfer.get("url") or "")
        if not url:
            raise CoordinatorError("Coordinator did not return an output upload URL")
        self._stream_put(url, local_path, transfer.get("headers") or {})

    def _stream_put(self, url: str, local_path: str, headers: Dict[str, Any]) -> None:
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise CoordinatorError("Coordinator returned an invalid output upload URL")
        connection_class = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        connection = connection_class(parsed.hostname, parsed.port, timeout=self.timeout)
        path = parsed.path + (f"?{parsed.query}" if parsed.query else "")
        size = os.path.getsize(local_path)
        try:
            connection.putrequest("PUT", path)
            connection.putheader("Content-Length", str(size))
            for name, value in headers.items():
                connection.putheader(str(name), str(value))
            connection.endheaders()
            with open(local_path, "rb") as source:
                while chunk := source.read(1024 * 1024):
                    connection.send(chunk)
            response = connection.getresponse()
            detail = response.read(4096).decode("utf-8", errors="replace")
            if response.status < 200 or response.status >= 300:
                raise CoordinatorError(f"Output upload failed ({response.status}): {detail or response.reason}", status=response.status)
        except (OSError, http.client.HTTPException) as exc:
            raise CoordinatorError(f"Output upload failed: {exc}") from exc
        finally:
            connection.close()

    def heartbeat(
        self,
        file_hash: str,
        *,
        worker_id: str,
        lease_token: str,
        progress: Dict[str, Any],
        metrics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._request(
            "POST",
            f"/api/v1/jobs/{file_hash}/heartbeat",
            {
                "worker_id": worker_id,
                "lease_token": lease_token,
                "progress": progress,
                "metrics": metrics or {},
            },
        ) or {}

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
