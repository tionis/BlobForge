"""Coordinator worker for isolated, exact-recipe MDAF adapters."""

from __future__ import annotations

import json
import logging
import signal
import socket
import tempfile
import threading
import time
import traceback as traceback_module
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from .converters import (
    AdapterCancelled,
    ConverterExecutionError,
    ConverterRunResult,
    ProviderProbe,
    probe_provider,
    run_converter,
)
from .coordinator_client import CoordinatorClient, CoordinatorTransferUnavailable
from .recipe_runtime import AdapterRecipe
from .reprocessing import ReprocessResult, reprocess_mdaf

logger = logging.getLogger(__name__)
RECIPE_PRIORITIES = ("1_urgent", "2_high", "3_normal", "4_low")


@dataclass(frozen=True)
class JobOutcome:
    claimed: bool
    source_key: str | None = None
    recipe_digest: str | None = None
    success: bool | None = None
    error: str | None = None
    deferred: bool = False


class _LeaseHeartbeat:
    def __init__(self, client, job: dict, worker_id: str, interval: float, stage: str):
        self.client = client
        self.job = job
        self.worker_id = worker_id
        self.interval = interval
        self.stage = stage
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self.stop_event.wait(self.interval):
            try:
                self.client.worker_heartbeat(
                    self.worker_id,
                    current_job=self.job["hash"],
                    metrics={"stage": self.stage},
                )
                self.client.heartbeat(
                    self.job["hash"],
                    worker_id=self.worker_id,
                    lease_token=self.job["lease_token"],
                    progress={"stage": self.stage},
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
        prober: Callable[..., ProviderProbe] = probe_provider,
        reprocessor: Callable[..., ReprocessResult] = reprocess_mdaf,
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
        self.prober = prober
        self.reprocessor = reprocessor
        self.worker_id: str | None = None
        self.stop_event = threading.Event()

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
            RECIPE_PRIORITIES,
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
        input_kind = str(job.get("input_kind") or (job.get("input") or {}).get("kind") or "source")
        if input_kind not in recipe.input_kinds:
            self.coordinator.release(
                source_key,
                worker_id=self.worker_id,
                lease_token=lease,
                reason="claimed input kind does not match exact recipe capability",
            )
            return JobOutcome(
                claimed=True,
                source_key=source_key,
                recipe_digest=digest,
                success=False,
                error="input kind mismatch",
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

        quota_reservation_id: str | None = None
        quota_report_path: Path | None = None
        quota_probe: ProviderProbe | None = None
        quota_settled = False
        quota_report_state: str | None = None
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
                source = root / (
                    "parent.mdaf" if input_kind == "artifact" else f"source{recipe.input_suffix}"
                )
                artifact = root / "artifact.mdaf"
                try:
                    self.coordinator.download_job_input(job, str(source))
                except CoordinatorTransferUnavailable as exc:
                    self.coordinator.release(
                        source_key,
                        worker_id=self.worker_id,
                        lease_token=lease,
                        reason="source transfer network unavailable",
                    )
                    return JobOutcome(
                        True, source_key, digest, None, str(exc), True
                    )
                if input_kind == "source" and recipe.provider_account is not None:
                    self.coordinator.heartbeat(
                        source_key,
                        worker_id=self.worker_id,
                        lease_token=lease,
                        progress={"stage": "provider-preflight"},
                    )
                    quota_probe = self.prober(
                        recipe.command,
                        source,
                        parameters=recipe.parameters,
                        timeout_seconds=min(self.timeout_seconds, 300),
                        environment=recipe.environment,
                        cancel_event=self.stop_event,
                    )
                    if (
                        quota_probe.account_key != recipe.provider_account
                        or quota_probe.provider != recipe.provider
                    ):
                        raise ValueError(
                            "provider probe does not match the exact recipe capability"
                        )
                    authorization = self.coordinator.reserve_quota(
                        source_key,
                        lease_token=lease,
                        probe=quota_probe.raw,
                    )
                    if not authorization.get("authorized"):
                        return JobOutcome(
                            True,
                            source_key,
                            digest,
                            None,
                            str(authorization.get("reason") or "quota deferred"),
                            True,
                        )
                    reservation = authorization.get("reservation") or {}
                    quota_reservation_id = str(reservation.get("id") or "")
                    if not quota_reservation_id:
                        raise RuntimeError("coordinator did not return a quota reservation")
                    quota_report_path = root / "provider-attempt.json"
                work_stage = "reprocessing" if input_kind == "artifact" else "converting"
                self.coordinator.heartbeat(
                    source_key,
                    worker_id=self.worker_id,
                    lease_token=lease,
                    progress={"stage": work_stage},
                )
                with _LeaseHeartbeat(
                    self.coordinator,
                    job,
                    self.worker_id,
                    self.heartbeat_interval,
                    work_stage,
                ):
                    started = time.monotonic()
                    if input_kind == "artifact":
                        metadata = {}
                        if recipe.recipe.get("normalization", {}).get("profile") == "wiki-v3" and job.get("original_name"):
                            metadata["source_name"] = job["original_name"]
                        result = self.reprocessor(source, recipe.recipe, artifact, **metadata)
                        elapsed_seconds = time.monotonic() - started
                        diagnostics: list[dict] = []
                        identity = str(result.identity)
                    else:
                        try:
                            result = self.converter(
                                recipe.command,
                                source,
                                artifact,
                                parameters=recipe.parameters,
                                recipe=recipe.recipe,
                                original_name=job.get("original_name"),
                                timeout_seconds=self.timeout_seconds,
                                environment=recipe.environment,
                                attempt_report_path=quota_report_path,
                                reservation_id=quota_reservation_id,
                                cancel_event=self.stop_event,
                            )
                        except Exception:
                            # Settle durable provider evidence before leaving
                            # the temporary-directory context that owns it.
                            if (
                                quota_reservation_id
                                and quota_report_path
                                and quota_report_path.is_file()
                            ):
                                report = json.loads(
                                    quota_report_path.read_text(encoding="utf-8")
                                )
                                quota_report_state = str(report.get("state") or "")
                                self.coordinator.settle_quota(
                                    quota_reservation_id, report
                                )
                                quota_settled = True
                            raise
                        elapsed_seconds = result.elapsed_seconds
                        diagnostics = list(result.diagnostics)
                        identity = result.identity
                        if quota_reservation_id:
                            if result.provider_attempt is None:
                                raise RuntimeError(
                                    "quota-managed adapter omitted its attempt report"
                                )
                            quota_report_state = str(
                                result.provider_attempt.get("state") or ""
                            )
                            self.coordinator.settle_quota(
                                quota_reservation_id, result.provider_attempt
                            )
                            quota_settled = True
                self.coordinator.heartbeat(
                    source_key,
                    worker_id=self.worker_id,
                    lease_token=lease,
                    progress={"stage": "uploading"},
                )
                self.coordinator.upload_job_output(
                    source_key,
                    str(artifact),
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
                        "diagnostics": diagnostics,
                        "execution_mode": input_kind,
                        "legacy": False,
                        "logical_identity": identity,
                        "media_type": "application/zip",
                        "recipe_digest": digest,
                        **(
                            {"quota_reservation_id": quota_reservation_id}
                            if quota_reservation_id
                            else {}
                        ),
                    },
                    metrics={"elapsed_seconds": elapsed_seconds},
                )
            return JobOutcome(True, source_key, digest, True)
        except Exception as exc:
            if quota_reservation_id and not quota_settled:
                try:
                    if (
                        isinstance(exc, ConverterExecutionError)
                        and exc.provider_attempt is not None
                    ):
                        report = exc.provider_attempt
                    elif quota_report_path and quota_report_path.is_file():
                        report = json.loads(
                            quota_report_path.read_text(encoding="utf-8")
                        )
                    elif quota_probe is not None:
                        report = {
                            "contract": "dev.tionis.blobforge.provider-attempt/v1",
                            "reservation_id": quota_reservation_id,
                            "provider": quota_probe.provider,
                            "account_key": quota_probe.account_key,
                            "currency": quota_probe.currency,
                            "list_currency": quota_probe.estimate_currency,
                            "checkpoint_key": quota_probe.checkpoint_key,
                            "state": "cache_hit" if quota_probe.cache_hit else "ambiguous",
                            "cache_hit": quota_probe.cache_hit,
                            "requests": 0 if quota_probe.cache_hit else quota_probe.requests,
                            "pages": 0 if quota_probe.cache_hit else quota_probe.pages,
                            "estimated_micro_usd": (
                                0 if quota_probe.cache_hit else quota_probe.estimated_micro_usd
                            ),
                            "list_micro_usd": 0 if quota_probe.cache_hit else None,
                            "billed_micro_usd": None,
                            "credits_micro_usd": None,
                            "detail": "worker lost adapter report; purchase outcome is ambiguous",
                        }
                    else:
                        report = None
                    if report is not None:
                        quota_report_state = str(report.get("state") or "")
                        self.coordinator.settle_quota(quota_reservation_id, report)
                        quota_settled = True
                except Exception as settlement_error:
                    logger.error(
                        "Could not settle provider reservation %s: %s",
                        quota_reservation_id,
                        settlement_error,
                    )
            shutdown_requested = (
                isinstance(exc, AdapterCancelled) or self.stop_event.is_set()
            )
            if shutdown_requested and quota_report_state != "ambiguous":
                try:
                    self.coordinator.release(
                        source_key,
                        worker_id=self.worker_id,
                        lease_token=lease,
                        reason="worker shutdown requested",
                    )
                except Exception as release_error:
                    logger.error(
                        "Could not release shutdown lease %s: %s",
                        source_key,
                        release_error,
                    )
                return JobOutcome(True, source_key, digest, None, str(exc), True)
            if quota_report_state == "rate_limited":
                return JobOutcome(True, source_key, digest, None, str(exc), True)
            try:
                self.coordinator.fail(
                    source_key,
                    worker_id=self.worker_id,
                    lease_token=lease,
                    error=str(exc),
                    traceback=traceback_module.format_exc(),
                    context={
                        "recipe_digest": digest,
                        "backend": recipe.backend,
                        "input_kind": input_kind,
                        "parent_recipe_digest": job.get("parent_recipe_digest"),
                    },
                )
            except Exception as report_error:
                logger.error(
                    "Could not report failed lease %s: %s", source_key, report_error
                )
            return JobOutcome(True, source_key, digest, False, str(exc))

    def run(self, *, run_once: bool = False, idle_sleep: float = 10.0) -> int:
        worker_id = self.register()
        previous_handlers: dict[int, object] = {}
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, lambda *_args: self.stop_event.set())
        try:
            while not self.stop_event.is_set():
                self.coordinator.worker_heartbeat(worker_id, current_job=None)
                outcome = self.process_once()
                if run_once and outcome.claimed:
                    return 0 if outcome.success or outcome.deferred else 1
                if outcome.deferred:
                    if run_once:
                        return 0
                    self.stop_event.wait(idle_sleep)
                elif not outcome.claimed:
                    if run_once:
                        return 0
                    self.stop_event.wait(idle_sleep)
        finally:
            try:
                self.coordinator.deregister_worker(worker_id)
            finally:
                for signum, handler in previous_handlers.items():
                    signal.signal(signum, handler)
        return 0
