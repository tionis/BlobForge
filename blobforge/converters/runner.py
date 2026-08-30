"""Subprocess runner and shared MDAF packager for converter adapters."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..mdaf import (
    MdafMemberInput,
    MdafSource,
    blake3_file,
    build_mdaf,
    canonical_json_bytes,
    validate_mdaf,
)
from ..mdaf.builder import activity, markdown_outline
from ..recipe_lifecycle import (
    RECIPE_MEMBER_PATH,
    parse_recipe_lifecycle,
    recipe_digest,
)
from .contract import (
    PROVIDER_ATTEMPT_CONTRACT,
    PROVIDER_PROBE_CONTRACT,
    ConversionRequest,
    load_bundle,
)


@dataclass(frozen=True)
class ConverterRunResult:
    artifact_path: Path
    identity: str
    elapsed_seconds: float
    diagnostics: tuple[Mapping[str, Any], ...]
    provider_attempt: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class ProviderProbe:
    provider: str
    account_key: str
    checkpoint_key: str
    cache_hit: bool
    requests: int
    pages: int
    estimated_micro_usd: int
    raw: Mapping[str, Any]
    currency: str = "USD"


class AdapterCancelled(RuntimeError):
    """The worker requested termination of an isolated adapter subprocess."""


class ConverterExecutionError(RuntimeError):
    """An adapter failed after it may have recorded a provider attempt."""

    def __init__(
        self,
        message: str,
        *,
        provider_attempt: Mapping[str, Any] | None = None,
    ):
        super().__init__(message)
        self.provider_attempt = provider_attempt


def _load_provider_attempt(
    report_path: Path | None,
    reservation_id: str | None,
    *,
    required: bool,
) -> Mapping[str, Any] | None:
    if report_path is None:
        return None
    if not report_path.is_file():
        if required:
            raise FileNotFoundError(report_path)
        return None
    provider_attempt = json.loads(report_path.read_text(encoding="utf-8"))
    if provider_attempt.get("contract") != PROVIDER_ATTEMPT_CONTRACT:
        raise ValueError("unsupported provider attempt contract")
    if provider_attempt.get("reservation_id") != reservation_id:
        raise ValueError("provider attempt reservation does not match")
    return provider_attempt


def _adapter_environment(environment: Mapping[str, str] | None) -> dict[str, str]:
    value = os.environ.copy()
    if environment:
        value.update(environment)
    return value


def _terminate_adapter(process: subprocess.Popen[str]) -> tuple[str, str]:
    if process.poll() is None:
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:  # pragma: no cover - exercised by Windows workers.
                process.terminate()
        except ProcessLookupError:
            pass
    try:
        return process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:  # pragma: no cover - exercised by Windows workers.
            process.kill()
        return process.communicate()


def _run_adapter(
    command: Sequence[str],
    *,
    timeout_seconds: int,
    environment: Mapping[str, str] | None,
    cancel_event: threading.Event | None,
) -> subprocess.CompletedProcess[str]:
    started = time.monotonic()
    process = subprocess.Popen(
        list(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=_adapter_environment(environment),
        start_new_session=os.name == "posix",
    )
    while True:
        if cancel_event is not None and cancel_event.is_set():
            stdout, stderr = _terminate_adapter(process)
            raise AdapterCancelled(
                f"adapter cancelled during worker shutdown: {stderr[-4000:]}"
            )
        remaining = timeout_seconds - (time.monotonic() - started)
        if remaining <= 0:
            stdout, stderr = _terminate_adapter(process)
            raise subprocess.TimeoutExpired(
                list(command), timeout_seconds, output=stdout, stderr=stderr
            )
        try:
            stdout, stderr = process.communicate(timeout=min(0.25, remaining))
            return subprocess.CompletedProcess(
                list(command), process.returncode, stdout, stderr
            )
        except subprocess.TimeoutExpired:
            continue


def probe_provider(
    command: Sequence[str],
    source_path: str | Path,
    *,
    parameters: Mapping[str, Any] | None = None,
    timeout_seconds: int = 300,
    environment: Mapping[str, str] | None = None,
    cancel_event: threading.Event | None = None,
) -> ProviderProbe:
    """Run the adapter's network-free purchase probe."""
    source = Path(source_path).resolve()
    if not source.is_file():
        raise ValueError(f"source is not a file: {source}")
    with tempfile.TemporaryDirectory(prefix="blobforge-provider-probe-") as temporary:
        root = Path(temporary)
        output = root / "output"
        output.mkdir()
        request_path = root / "request.json"
        request_path.write_text(
            json.dumps(
                ConversionRequest(
                    source,
                    output,
                    parameters or {},
                    operation="probe",
                ).as_json()
            ),
            encoding="utf-8",
        )
        completed = _run_adapter(
            [*command, str(request_path)],
            timeout_seconds=timeout_seconds,
            environment=environment,
            cancel_event=cancel_event,
        )
        if completed.returncode:
            raise RuntimeError(
                f"provider probe exited {completed.returncode}: {completed.stderr[-4000:]}"
            )
        probe_path = output / "probe.json"
        value = json.loads(probe_path.read_text(encoding="utf-8"))
        if value.get("contract") != PROVIDER_PROBE_CONTRACT:
            raise ValueError("unsupported provider probe contract")
        for key in ("provider", "account_key", "checkpoint_key"):
            if not isinstance(value.get(key), str) or not value[key]:
                raise ValueError(f"provider probe {key} must be a non-empty string")
        for key in ("requests", "pages", "estimated_micro_usd"):
            if isinstance(value.get(key), bool) or not isinstance(value.get(key), int):
                raise ValueError(f"provider probe {key} must be an integer")
            if value[key] < 0:
                raise ValueError(f"provider probe {key} cannot be negative")
        if not isinstance(value.get("cache_hit"), bool):
            raise ValueError("provider probe cache_hit must be a boolean")
        currency = str(value.get("currency") or "USD").upper()
        if len(currency) != 3 or not currency.isalpha():
            raise ValueError("provider probe currency must be a three-letter ISO 4217 code")
        if value["cache_hit"] and any(
            value[key] for key in ("requests", "estimated_micro_usd")
        ):
            raise ValueError("a cache hit cannot reserve a request or estimated spend")
        return ProviderProbe(
            provider=value["provider"],
            account_key=value["account_key"],
            checkpoint_key=value["checkpoint_key"],
            cache_hit=value["cache_hit"],
            requests=value["requests"],
            pages=value["pages"],
            estimated_micro_usd=value["estimated_micro_usd"],
            raw=value,
            currency=currency,
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def run_converter(
    command: Sequence[str],
    source_path: str | Path,
    output_path: str | Path,
    *,
    parameters: Mapping[str, Any] | None = None,
    recipe: Mapping[str, Any] | None = None,
    timeout_seconds: int = 86_400,
    environment: Mapping[str, str] | None = None,
    attempt_report_path: str | Path | None = None,
    reservation_id: str | None = None,
    cancel_event: threading.Event | None = None,
) -> ConverterRunResult:
    """Run one isolated adapter and package its validated bundle as MDAF."""
    source = Path(source_path).resolve()
    if not source.is_file():
        raise ValueError(f"source is not a file: {source}")
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="blobforge-converter-") as temporary:
        root = Path(temporary)
        bundle_root = root / "bundle"
        bundle_root.mkdir()
        report_path = Path(attempt_report_path).resolve() if attempt_report_path else None
        request = ConversionRequest(
            source,
            bundle_root,
            parameters or {},
            attempt_report_path=report_path,
            reservation_id=reservation_id,
        )
        request_path = root / "request.json"
        request_path.write_text(json.dumps(request.as_json()), encoding="utf-8")
        completed = _run_adapter(
            [*command, str(request_path)],
            timeout_seconds=timeout_seconds,
            environment=environment,
            cancel_event=cancel_event,
        )
        if completed.returncode:
            raise ConverterExecutionError(
                f"converter exited {completed.returncode}: {completed.stderr[-4000:]}",
                provider_attempt=_load_provider_attempt(
                    report_path, reservation_id, required=False
                ),
            )
        bundle = load_bundle(bundle_root)
        text = bundle.text_path.read_text(encoding="utf-8")
        effective_outline = bundle.outline or markdown_outline(text)
        lifecycle = None
        recipe_member = None
        if recipe is not None and recipe.get("schema") == "dev.tionis.blobforge.recipe/v3":
            lifecycle = parse_recipe_lifecycle(recipe)
            if bundle.parameters.get("recipe_digest") != lifecycle.digest:
                raise ValueError("adapter bundle recipe digest does not match embedded recipe")
            recipe_member = MdafMemberInput(
                RECIPE_MEMBER_PATH,
                canonical_json_bytes(recipe),
                "extension",
                "activity:postprocess",
                "application/json",
                namespace="dev.tionis.blobforge",
            )

        generated_paths = [member.artifact_path for member in bundle.members]
        generated_paths += ["source-map.json"] if bundle.source_map is not None else []
        generated_paths += ["outline.json"]
        if lifecycle is None:
            activities = [
                activity(
                    activity_id="activity:convert",
                    kind="document-extraction",
                    tools=list(bundle.tools),
                    models=list(bundle.models),
                    inputs=["source:document"],
                    outputs=["text.md", "provenance.json", *generated_paths],
                    parameters=bundle.parameters,
                )
            ]
            primary_activity = "activity:convert"
        else:
            if len(bundle.tools) < 2:
                raise ValueError(
                    "lifecycle recipe requires distinct extraction and post-processing tools"
                )
            native_paths = set(lifecycle.native_members)
            observed_paths = {member.artifact_path for member in bundle.members}
            missing_native = native_paths - observed_paths
            if missing_native:
                raise ValueError(
                    f"adapter omitted lifecycle native evidence: {sorted(missing_native)}"
                )
            extraction_parameters = {
                key: value
                for key, value in bundle.parameters.items()
                if key not in {"normalization_profile", "recipe_digest"}
            }
            extraction_parameters["recipe_digest"] = (
                lifecycle.extraction_recipe_digest
            )
            postprocess_paths = [
                path for path in generated_paths if path not in native_paths
            ]
            postprocess_paths.extend([RECIPE_MEMBER_PATH, "text.md", "provenance.json"])
            activities = [
                activity(
                    activity_id="activity:extract",
                    kind="document-extraction",
                    tools=[dict(bundle.tools[0])],
                    models=list(bundle.models),
                    inputs=["source:document"],
                    outputs=sorted(native_paths),
                    parameters=extraction_parameters,
                ),
                activity(
                    activity_id="activity:postprocess",
                    kind="document-normalization",
                    tools=[dict(tool) for tool in bundle.tools[1:]],
                    inputs=sorted(native_paths),
                    outputs=sorted(set(postprocess_paths)),
                    parameters={
                        "recipe_digest": recipe_digest(recipe),
                        "recipe_version": (
                            f"{lifecycle.version.major}."
                            f"{lifecycle.version.minor}."
                            f"{lifecycle.version.patch}"
                        ),
                        "profile": lifecycle.postprocessing_profile,
                        "postprocessing_version": (
                            f"{lifecycle.postprocessing_version.major}."
                            f"{lifecycle.postprocessing_version.minor}."
                            f"{lifecycle.postprocessing_version.patch}"
                        ),
                    },
                    depends_on=["activity:extract"],
                ),
            ]
            primary_activity = "activity:postprocess"
        extra_members = [
            MdafMemberInput(
                member.artifact_path,
                member.file_path.read_bytes(),
                member.role,
                (
                    "activity:extract"
                    if lifecycle is not None
                    and member.artifact_path in set(lifecycle.native_members)
                    else primary_activity
                ),
                member.media_type,
                member.schema,
                member.namespace,
            )
            for member in bundle.members
        ]
        if recipe_member is not None:
            extra_members.append(recipe_member)
        result = build_mdaf(
            output_path,
            text=text,
            title=source.stem,
            sources=[
                MdafSource(
                    "document",
                    "application/pdf",
                    blake3_file(source),
                    (f"sha256:{_sha256_file(source)}",),
                    source.name,
                )
            ],
            activities=activities,
            producer={"name": "blobforge", "version": version("blobforge")},
            extra_members=extra_members,
            source_map=bundle.source_map,
            outline=effective_outline,
            markdown_variant=bundle.markdown_variant,
            markdown_features=bundle.markdown_features,
            primary_created_by=primary_activity,
        )
        validated = validate_mdaf(result.path)
        if validated.identity != result.identity:
            raise RuntimeError("MDAF changed during post-build validation")
        provider_attempt = _load_provider_attempt(
            report_path, reservation_id, required=True
        )
        return ConverterRunResult(
            artifact_path=result.path,
            identity=result.identity,
            elapsed_seconds=time.monotonic() - started,
            diagnostics=bundle.diagnostics,
            provider_attempt=provider_attempt,
        )
