"""Coordinator worker for isolated, exact-recipe MDAF adapters."""

from __future__ import annotations

import logging
import socket
import tempfile
import threading
import time
import traceback as traceback_module
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from .config import PRIORITIES
from .converters import ConverterRunResult, run_converter
from .coordinator_client import CoordinatorClient
from .recipe_runtime import AdapterRecipe

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobOutcome:
    claimed: bool
    source_key: str | None = None
    recipe_digest: str | None = None
    success: bool | None = None
    error: str | None = None


class _LeaseHeartbeat:
    def __init__(self, client, job: dict, worker_id: str, interval: float):
        self.client = client
        self.job = job
        self.worker_id = worker_id
        self.interval = interval
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self.stop_event.wait(self.interval):
            try:
                self.client.worker_heartbeat(
                    self.worker_id,
                    current_job=self.job["hash"],
                    metrics={"stage": "converting"},
                )
                self.client.heartbeat(
                    self.job["hash"],
                    worker_id=self.worker_id,
                    lease_token=self.job["lease_token"],
                    progress={"stage": "converting"},
                )
            except Exception as exc:  # The fenced upload remains authoritative.
                logger.warning("Lease heartbeat failed: %s", exc)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop_event.set()
        self.thread.join(timeout=max(1.0, min(self.interval, 5.0)))


class RecipeWorker:
    """Advertise many recipe/media capabilities and dispatch each lease exactly."""

    def __init__(
        self,
        coordinator: CoordinatorClient,
        recipes: Sequence[AdapterRecipe],
        *,
        timeout_seconds: int = 86_400,
        heartbeat_interval: float = 30.0,
        converter: Callable[..., ConverterRunResult] = run_converter,
    ):
        if not recipes:
            raise ValueError("at least one recipe is required")
        if heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        by_digest = {recipe.recipe_digest: recipe for recipe in recipes}
        if len(by_digest) != len(recipes):
            raise ValueError("recipe digests must be unique")
        self.coordinator = coordinator
        self.recipes = by_digest
        self.timeout_seconds = timeout_seconds
        self.heartbeat_interval = heartbeat_interval
        self.converter = converter
        self.worker_id: str | None = None

    @property
    def capabilities(self) -> list[dict]:
        return [recipe.capability() for recipe in self.recipes.values()]

    def register(self) -> str:
        worker_id = self.coordinator.worker_identity()
        self.coordinator.register_worker(
            worker_id,
            {
                "hostname": socket.gethostname(),
                "worker_kind": "isolated-recipe-worker/v1",
                "capabilities": self.capabilities,
                "recipe_statuses": {
                    digest: recipe.deployment_status
                    for digest, recipe in self.recipes.items()
                },
            },
        )
        self.worker_id = worker_id
        return worker_id

    def process_once(self) -> JobOutcome:
        if self.worker_id is None:
            raise RuntimeError("worker must be registered before claiming jobs")
        job = self.coordinator.claim_job(
            self.worker_id,
            PRIORITIES,
            capabilities=self.capabilities,
        )
        if job is None:
            return JobOutcome(claimed=False)
        source_key = str(job["hash"])
        lease = str(job.get("lease_token") or "")
        capability = job.get("capability") or {}
        digest = str(capability.get("recipe_digest") or job.get("recipe_digest") or "")
        recipe = self.recipes.get(digest)
        if recipe is None or not lease:
            if lease:
                self.coordinator.release(
                    source_key,
                    worker_id=self.worker_id,
                    lease_token=lease,
                    reason="claimed capability is not executable by this worker",
                )
            return JobOutcome(
                claimed=True,
                source_key=source_key,
                recipe_digest=digest or None,
                success=False,
                error="unknown or malformed claimed capability",
            )
        if str(job.get("media_type")) not in recipe.media_types:
            self.coordinator.release(
                source_key,
                worker_id=self.worker_id,
                lease_token=lease,
                reason="claimed media type does not match exact recipe",
            )
            return JobOutcome(
                claimed=True,
                source_key=source_key,
                recipe_digest=digest,
                success=False,
                error="media type mismatch",
            )

        try:
            self.coordinator.worker_heartbeat(
                self.worker_id,
                current_job=source_key,
                metrics={"stage": "downloading"},
            )
            self.coordinator.heartbeat(
                source_key,
                worker_id=self.worker_id,
                lease_token=lease,
                progress={"stage": "downloading"},
            )
            with tempfile.TemporaryDirectory(prefix="blobforge-recipe-worker-") as temporary:
                root = Path(temporary)
                source = root / f"source{recipe.input_suffix}"
                artifact = root / "artifact.mdaf"
                self.coordinator.download_job_input(job, str(source))
                self.coordinator.heartbeat(
                    source_key,
                    worker_id=self.worker_id,
                    lease_token=lease,
                    progress={"stage": "converting"},
                )
                with _LeaseHeartbeat(
                    self.coordinator,
                    job,
                    self.worker_id,
                    self.heartbeat_interval,
                ):
                    result = self.converter(
                        recipe.command,
                        source,
                        artifact,
                        parameters=recipe.parameters,
                        timeout_seconds=self.timeout_seconds,
                        environment=recipe.environment,
                    )
                self.coordinator.heartbeat(
                    source_key,
                    worker_id=self.worker_id,
                    lease_token=lease,
                    progress={"stage": "uploading"},
                )
                self.coordinator.upload_job_output(
                    source_key,
                    str(result.artifact_path),
                    worker_id=self.worker_id,
                    lease_token=lease,
                )
                self.coordinator.complete(
                    source_key,
                    worker_id=self.worker_id,
                    lease_token=lease,
                    result={
                        "artifact_type": recipe.artifact_type,
                        "converter_backend": recipe.backend,
                        "converter_version": str(
                            (recipe.recipe.get("adapter") or {}).get("version")
                            or "unavailable"
                        ),
                        "deployment_status": recipe.deployment_status,
                        "diagnostics": list(result.diagnostics),
                        "legacy": False,
                        "logical_identity": result.identity,
                        "media_type": "application/zip",
                        "recipe_digest": digest,
                    },
                    metrics={"elapsed_seconds": result.elapsed_seconds},
                )
            return JobOutcome(True, source_key, digest, True)
        except Exception as exc:
            try:
                self.coordinator.fail(
                    source_key,
                    worker_id=self.worker_id,
                    lease_token=lease,
                    error=str(exc),
                    traceback=traceback_module.format_exc(),
                    context={"recipe_digest": digest, "backend": recipe.backend},
                )
            except Exception as report_error:
                logger.error(
                    "Could not report failed lease %s: %s", source_key, report_error
                )
            return JobOutcome(True, source_key, digest, False, str(exc))

    def run(self, *, run_once: bool = False, idle_sleep: float = 10.0) -> int:
        worker_id = self.register()
        try:
            while True:
                self.coordinator.worker_heartbeat(worker_id, current_job=None)
                outcome = self.process_once()
                if run_once and outcome.claimed:
                    return 0 if outcome.success else 1
                if not outcome.claimed:
                    if run_once:
                        return 0
                    time.sleep(idle_sleep)
        finally:
            self.coordinator.deregister_worker(worker_id)
