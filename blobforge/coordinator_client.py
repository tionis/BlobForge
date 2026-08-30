"""HTTP client for a BlobForge coordination backend.

The coordinator owns file metadata, persistent job state, leases, retries, worker
registration, progress, operational configuration, and signed byte transfers.
"""

from __future__ import annotations

import json
import http.client
import os
import shutil
import socket
import urllib.error
import urllib.request
from urllib.parse import urlencode, urlsplit
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


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
                "BlobForge coordinator is not configured; set "
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
        digest_algorithm: str = "sha256",
        digest: Optional[str] = None,
        media_type: str = "application/pdf",
        aliases: Optional[Dict[str, str]] = None,
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
                "digest_algorithm": digest_algorithm,
                "digest": digest or file_hash,
                "media_type": media_type,
                "aliases": aliases or {},
            },
        ) or {}

    def get_job(self, file_hash: str) -> Dict[str, Any]:
        return self._request("GET", f"/api/v1/jobs/{file_hash}") or {}

    def sync_done_hashes(
        self,
        since_ms: int = 0,
        cursor: str = "",
        *,
        progress: Optional[Any] = None,
        chunk_size: int = 5000,
    ) -> Tuple[List[str], int, str]:
        """Page the coordinator's done-set since a watermark, returning new hashes.

        Pages the server's `done-since` endpoint, which orders done jobs by a
        strictly monotonic ``done_seq`` and resumes strictly after the previous
        ``since``, so same-millisecond completions can never be skipped. Returns
        ``(hashes, next_since, next_cursor)``; the caller stores the watermark and
        only re-queries the delta on subsequent runs.
        """
        collected: List[str] = []
        current_since = int(since_ms)
        current_cursor = cursor
        while True:
            path = (
                f"/api/v1/jobs/done-since?since={current_since}"
                f"&cursor={current_cursor}&limit={chunk_size}"
            )
            payload = self._request("GET", path) or {}
            batch = payload.get("hashes")
            if isinstance(batch, list):
                collected.extend(str(h) for h in batch if isinstance(h, str))
            if progress is not None:
                progress(len(collected))
            if payload.get("complete"):
                next_since = int(payload.get("next_since", current_since))
                next_cursor = str(payload.get("next_cursor", current_cursor))
                return collected, next_since, next_cursor
            next_since = int(payload.get("next_since", current_since))
            next_cursor = str(payload.get("next_cursor", current_cursor))
            # Keyset pagination must always advance; a page that neither completes
            # nor advances the watermark indicates a coordinator protocol bug.
            if (next_since, next_cursor) == (current_since, current_cursor):
                raise CoordinatorError(
                    "Done-sync pagination did not advance; refusing to loop"
                )
            current_since, current_cursor = next_since, next_cursor

    def check_statuses(
        self,
        hashes: Iterable[str],
        *,
        progress: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Bulk-check completion state, chunking to the server's per-request limit.

        The coordinator answers up to 5,000 hashes per request, so large
        candidate sets are split automatically. An optional ``progress``
        callback receives ``(checked, total)`` after each chunk.
        """
        values = list(dict.fromkeys(hashes))
        if not values:
            return {}
        results: Dict[str, Any] = {}
        chunk_size = 5000
        total = len(values)
        for start in range(0, total, chunk_size):
            batch = values[start:start + chunk_size]
            payload = self._request("POST", "/api/v1/jobs/status", {"hashes": batch}) or {}
            chunk_results = payload.get("results")
            if isinstance(chunk_results, dict):
                results.update(chunk_results)
            if progress is not None:
                progress(min(start + chunk_size, total), total)
        return results

    def output_download_url(
        self, file_hash: str, recipe_digest: Optional[str] = None
    ) -> str:
        """Return a coordinator-issued signed URL for a completed result archive."""
        body = {"recipe_digest": recipe_digest} if recipe_digest else {}
        transfer = self._request(
            "POST", f"/api/v1/jobs/{file_hash}/download-url", body
        ) or {}
        url = str(transfer.get("url") or "")
        if not url:
            raise CoordinatorError("Coordinator did not return an output download URL")
        return url

    def list_artifacts(self, file_hash: str) -> List[Dict[str, Any]]:
        """List every retained conversion artifact for a source document."""
        payload = self._request("GET", f"/api/v1/jobs/{file_hash}/artifacts") or {}
        artifacts = payload.get("artifacts")
        return artifacts if isinstance(artifacts, list) else []

    def list_recipes(self, media_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """List conversion recipes currently advertised by workers."""
        suffix = f"?{urlencode({'media_type': media_type})}" if media_type else ""
        payload = self._request("GET", f"/api/v1/recipes{suffix}") or {}
        recipes = payload.get("recipes")
        return recipes if isinstance(recipes, list) else []

    def request_conversion(
        self, file_hash: str, recipe_digest: Optional[str] = None, *, backend: Optional[str] = None
    ) -> Dict[str, Any]:
        """Queue or select an exact recipe, or an unambiguous active backend."""
        body = {"recipe_digest": recipe_digest} if recipe_digest else {"backend": backend}
        return self._request(
            "POST",
            f"/api/v1/jobs/{file_hash}/convert",
            body,
        ) or {}

    def route_conversion(
        self, file_hash: str, routing_features: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """Recompute, apply, and audit a versioned exact-recipe decision."""
        return self._request(
            "POST",
            f"/api/v1/jobs/{file_hash}/route",
            dict(routing_features),
        ) or {}

    def plan_reprocessing(
        self,
        *,
        target_recipe_digest: str,
        source_recipe_digest: str,
        source_keys: Optional[Iterable[str]] = None,
        execute: bool = False,
        priority: Optional[str] = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "target_recipe_digest": target_recipe_digest,
            "source_recipe_digest": source_recipe_digest,
            "execute": execute,
        }
        if source_keys is not None:
            body["source_keys"] = list(source_keys)
        if priority is not None:
            body["priority"] = priority
        return self._request("POST", "/api/v1/admin/reprocessing", body) or {}

    def raw_upload_url(
        self,
        file_hash: str,
        *,
        digest_algorithm: str = "sha256",
        digest: Optional[str] = None,
        media_type: str = "application/pdf",
    ) -> Dict[str, Any]:
        """Return a signed raw-object upload URL plus whether the object already exists."""
        transfer = self._request(
            "POST",
            f"/api/v1/jobs/{file_hash}/raw-upload-url",
            {
                "digest_algorithm": digest_algorithm,
                "digest": digest or file_hash,
                "media_type": media_type,
            },
        ) or {}
        url = str(transfer.get("url") or "")
        if not url:
            raise CoordinatorError("Coordinator did not return a raw upload URL")
        return {
            "url": url,
            "already_exists": bool(transfer.get("already_exists")),
            "headers": transfer.get("headers") or {},
        }

    def download_output(
        self,
        file_hash: str,
        local_path: str,
        recipe_digest: Optional[str] = None,
    ) -> None:
        """Download a completed result archive through its coordinator-issued signed URL."""
        url = self.output_download_url(file_hash, recipe_digest)
        try:
            request = urllib.request.Request(url, headers={"Accept": "application/zip"})
            with urllib.request.urlopen(request, timeout=self.timeout) as response, open(local_path, "wb") as target:
                shutil.copyfileobj(response, target, length=1024 * 1024)
        except (urllib.error.URLError, OSError) as exc:
            raise CoordinatorError(f"Output download failed: {exc}") from exc

    def upload_raw(
        self,
        file_hash: str,
        local_path: str,
        *,
        transfer: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Stream a raw PDF through an existing or newly issued signed URL."""
        if transfer is None:
            transfer = self.raw_upload_url(file_hash)
        self._stream_put(transfer["url"], local_path, transfer["headers"])

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

    def claim_job(
        self,
        worker_id: str,
        priorities: Iterable[str],
        *,
        recipe_digest: Optional[str] = None,
        recipe: Optional[Dict[str, Any]] = None,
        accepted_media_types: Optional[Iterable[str]] = None,
        capabilities: Optional[Iterable[Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        body: Dict[str, Any] = {
            "worker_id": worker_id,
            "priorities": list(priorities),
        }
        if capabilities is not None:
            body["capabilities"] = list(capabilities)
        if recipe_digest:
            body["recipe_digest"] = recipe_digest
            body["recipe"] = recipe or {}
        if accepted_media_types is not None:
            body["accepted_media_types"] = list(accepted_media_types)
        payload = self._request(
            "POST",
            "/api/v1/jobs/claim",
            body,
        )
        if not isinstance(payload, dict) or "job" not in payload:
            raise CoordinatorError("Coordinator returned an invalid claim response")
        job = payload.get("job")
        return job if isinstance(job, dict) else None

    def download_job_input(self, job: Dict[str, Any], local_path: str) -> None:
        """Download the claimed source or parent artifact through its signed URL."""
        transfer = job.get("input") or {}
        url = str(transfer.get("url") or "")
        if not url:
            raise CoordinatorError("Coordinator claim did not include an input URL")
        try:
            request = urllib.request.Request(
                url,
                headers={"Accept": str(transfer.get("media_type") or "application/octet-stream")},
            )
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
