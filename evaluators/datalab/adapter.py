"""Datalab Convert API adapter with resumable, durable response capture."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import mimetypes
import os
import re
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

REPOSITORY = Path(__file__).resolve().parents[2]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from blobforge.normalization import (
    normalize_datalab_pages,
    raster_dimensions,
    referenced_asset_names,
)

CONTRACT = "dev.tionis.blobforge.converter-bundle/v1"
CACHE_CONTRACT = "dev.tionis.blobforge.datalab-response/v1"
PROBE_CONTRACT = "dev.tionis.blobforge.provider-probe/v1"
ATTEMPT_CONTRACT = "dev.tionis.blobforge.provider-attempt/v1"
ADAPTER_VERSION = "0.1.0"
API_URL = "https://www.datalab.to/api/v1/convert"
MAX_FILE_BYTES = 200_000_000
MAX_API_PAGES = 7_000
PAGE_RE = re.compile(r"(?:\r?\n){0,2}\{(\d+)\}-{48}(?:\r?\n){2}")
LINK_RE = re.compile(r"(!?\[[^\]]*\]\()([^\)\s]+)(\))")
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _request_identity(
    source_sha256: str, recipe_digest: str, mode: str
) -> tuple[str, dict[str, Any]]:
    value = {
        "source_sha256": f"sha256:{source_sha256}",
        "recipe_digest": recipe_digest,
        "endpoint": API_URL,
        "output_format": "markdown",
        "mode": mode,
        "paginate": True,
        "disable_image_extraction": False,
        "disable_image_captions": False,
        "skip_cache": False,
    }
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest(), value


def _cache_path(root: Path, identity: str) -> Path:
    return root / identity[:2] / f"{identity}.json"


@contextmanager
def _response_lock(path: Path):
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = path.with_suffix(".lock")
    with lock_path.open("a", encoding="utf-8") as handle:
        os.chmod(lock_path, 0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _write_cache(path: Path, envelope: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(envelope, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_name, 0o600)
        os.replace(temporary_name, path)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def _attempt_report(
    request: dict[str, Any],
    *,
    state: str,
    account_key: str,
    checkpoint_key: str,
    requests: int,
    pages: int,
    estimated_micro_usd: int,
    list_micro_usd: int | None = None,
    billed_micro_usd: int | None = None,
    credits_micro_usd: int | None = None,
    detail: str | None = None,
    retry_after_ms: int | None = None,
) -> None:
    raw_path = request.get("attempt_report_path")
    if not raw_path:
        return
    reservation_id = request.get("reservation_id")
    if not isinstance(reservation_id, str) or not reservation_id:
        raise ValueError("quota-managed conversion requires a reservation_id")
    _write_cache(
        Path(raw_path),
        {
            "contract": ATTEMPT_CONTRACT,
            "reservation_id": reservation_id,
            "provider": "datalab",
            "account_key": account_key,
            "checkpoint_key": checkpoint_key,
            "state": state,
            "cache_hit": state == "cache_hit",
            "requests": requests,
            "pages": pages,
            "estimated_micro_usd": estimated_micro_usd,
            "list_micro_usd": list_micro_usd,
            "billed_micro_usd": billed_micro_usd,
            "credits_micro_usd": credits_micro_usd,
            **({"detail": detail[:1000]} if detail else {}),
            **({"retry_after_ms": retry_after_ms} if retry_after_ms is not None else {}),
        },
    )


def _rate_limited(error: Exception) -> bool:
    response = getattr(error, "response", None)
    return getattr(response, "status_code", None) == 429


def _retry_after_ms(error: Exception) -> int | None:
    response = getattr(error, "response", None)
    headers = getattr(response, "headers", {}) or {}
    value = headers.get("retry-after") or headers.get("Retry-After")
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return max(1_000, min(round(seconds * 1000), 86_400_000))


def _read_cache(
    path: Path, identity: str, request_value: dict[str, Any]
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"invalid Datalab response cache entry {path}: {error}") from error
    if (
        not isinstance(value, dict)
        or value.get("contract") != CACHE_CONTRACT
        or value.get("request_identity") != f"sha256:{identity}"
        or value.get("request") != request_value
        or value.get("state") not in {"submitted", "complete", "failed"}
    ):
        raise ValueError(f"Datalab response cache entry does not match request: {path}")
    return value


def _safe_check_url(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("Datalab submission did not return request_check_url")
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.datalab.to"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port not in {None, 443}
        or not parsed.path.startswith("/api/v1/convert/")
    ):
        raise ValueError("Datalab returned an unsafe request_check_url")
    return value


def _json_response(response: Any, action: str) -> dict[str, Any]:
    response.raise_for_status()
    value = response.json()
    if not isinstance(value, dict):
        raise ValueError(f"Datalab {action} response must be an object")
    return value


def _submit(source: Path, api_key: str, page_count: int, mode: str) -> dict[str, Any]:
    import requests

    with source.open("rb") as handle:
        response = requests.post(
            API_URL,
            headers={"X-API-Key": api_key},
            files={"file": (source.name, handle, "application/pdf")},
            data={
                "output_format": "markdown",
                "mode": mode,
                "paginate": "true",
                "max_pages": str(page_count),
                "disable_image_extraction": "false",
                "disable_image_captions": "false",
                "skip_cache": "false",
            },
            timeout=(30, 180),
        )
    return _json_response(response, "submission")


def _poll(check_url: str, api_key: str, *, attempts: int = 900) -> dict[str, Any]:
    import requests

    check_url = _safe_check_url(check_url)
    for attempt in range(attempts):
        result = _json_response(
            requests.get(check_url, headers={"X-API-Key": api_key}, timeout=(15, 60)),
            "poll",
        )
        status = result.get("status")
        if status == "complete":
            return result
        if status == "failed" or result.get("success") is False:
            raise ValueError(f"Datalab conversion failed: {result.get('error', 'unknown error')}")
        if status not in {"processing", "pending", "queued"}:
            raise ValueError(f"Datalab returned unknown conversion status {status!r}")
        if attempt + 1 < attempts:
            time.sleep(2)
    raise TimeoutError("Datalab conversion did not complete within the polling limit")


def _page_count(source: Path) -> int:
    from pypdf import PdfReader

    return len(PdfReader(source).pages)


def _split_pages(markdown: str, expected_pages: int) -> list[str]:
    matches = list(PAGE_RE.finditer(markdown))
    indices = [int(match.group(1)) for match in matches]
    if indices != list(range(expected_pages)):
        raise ValueError(
            "Datalab page delimiters do not exactly cover the source: "
            f"expected 0..{expected_pages - 1}, got {indices[:12]}"
        )
    if markdown[: matches[0].start()].strip():
        raise ValueError("Datalab returned content before the first page delimiter")
    return [
        markdown[match.end() : matches[index + 1].start() if index + 1 < len(matches) else None]
        for index, match in enumerate(matches)
    ]


def _money_cents(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"Datalab cost_breakdown.{field} is invalid")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Datalab cost_breakdown.{field} is missing") from error
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"Datalab cost_breakdown.{field} is invalid")
    return number


def _validate_response(
    response: dict[str, Any], source_pages: int, max_cost_usd: float
) -> tuple[str, float | None, float]:
    if response.get("status") != "complete" or response.get("success") is not True:
        raise ValueError("Datalab response is not a successful completed conversion")
    markdown = response.get("markdown")
    if not isinstance(markdown, str):
        raise ValueError("Datalab response is missing Markdown")
    page_count = response.get("page_count")
    if isinstance(page_count, bool) or page_count != source_pages:
        raise ValueError("Datalab response page_count does not match the source")
    costs = response.get("cost_breakdown")
    if not isinstance(costs, dict):
        raise ValueError("Datalab response is missing cost_breakdown")
    list_cents = (
        _money_cents(costs.get("list_cost_cents"), "list_cost_cents")
        if costs.get("list_cost_cents") is not None
        else None
    )
    final_cents = _money_cents(costs.get("final_cost_cents"), "final_cost_cents")
    if final_cents / 100 > max_cost_usd:
        raise ValueError(
            f"Datalab returned cost ${final_cents / 100:.4f} above ceiling ${max_cost_usd:.4f}"
        )
    _split_pages(markdown, source_pages)
    return markdown, list_cents, final_cents


def _decode_image(value: str) -> tuple[bytes, str | None]:
    media_type = None
    encoded = value
    if value.startswith("data:"):
        header, encoded = value.split(",", 1)
        media_type = header[5:].split(";", 1)[0] or None
    return base64.b64decode(encoded, validate=True), media_type


def _image_media_type(data: bytes, declared: str | None) -> str:
    signatures = (
        (b"\x89PNG\r\n\x1a\n", "image/png"),
        (b"\xff\xd8\xff", "image/jpeg"),
        (b"GIF87a", "image/gif"),
        (b"GIF89a", "image/gif"),
    )
    detected = next((media for prefix, media in signatures if data.startswith(prefix)), None)
    if detected is None and data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        detected = "image/webp"
    if detected is None:
        raise ValueError("Datalab image payload is not a supported raster image")
    if declared and declared != detected:
        raise ValueError("Datalab image media type does not match its bytes")
    return detected


def _asset_name(index: int, original: str, media_type: str) -> str:
    safe = SAFE_NAME_RE.sub("-", Path(original).name).strip(".-") or "image"
    suffix = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
    }.get(media_type) or mimetypes.guess_extension(media_type) or ".bin"
    return f"image-{index:04d}-{Path(safe).stem[:80]}{suffix.lower()}"


def _mapping(page: int, start: int, end: int) -> dict[str, Any]:
    return {
        "document": {"start": start, "end": end},
        "source": {
            "source_id": "document",
            "selectors": [
                {"type": "interval", "unit": "page", "start": page, "end": page + 1}
            ],
        },
        "confidence": 1,
        "method": "dev.tionis.blobforge/datalab-pagination",
    }


def main() -> int:
    request = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    if request.get("contract") != CONTRACT:
        raise ValueError("unsupported converter request contract")
    source = Path(request["source_path"])
    output = Path(request["output_dir"])
    data = output / "data"
    assets = data / "assets"
    native_dir = data / "native"
    assets.mkdir(parents=True)
    native_dir.mkdir()
    parameters = request.get("parameters", {})
    source_pages = _page_count(source)
    max_pages = int(parameters.get("max_pages") or 0)
    max_cost = float(parameters.get("max_cost_usd") or 0)
    if source_pages <= 0:
        raise ValueError("source PDF has no pages")
    if source_pages > MAX_API_PAGES:
        raise ValueError(f"Datalab accepts at most {MAX_API_PAGES} pages per request")
    if source.stat().st_size > MAX_FILE_BYTES:
        raise ValueError(f"Datalab accepts files no larger than {MAX_FILE_BYTES} bytes")
    if max_pages <= 0 or source_pages > max_pages:
        raise ValueError(f"page ceiling rejected {source_pages} pages (limit {max_pages})")
    if not math.isfinite(max_cost) or max_cost <= 0:
        raise ValueError("a positive finite returned-cost ceiling is required")
    if parameters.get("do_table_structure") is False:
        raise ValueError("the frozen Datalab recipe requires table extraction")
    if parameters.get("extract_images") is False:
        raise ValueError("the frozen Datalab recipe requires image extraction")
    mode = str(parameters.get("mode") or "accurate")
    if mode != "accurate":
        raise ValueError("this frozen evaluator only permits accurate mode")
    recipe_digest = str(parameters.get("recipe_digest") or "")
    if not re.fullmatch(r"blake3:[0-9a-f]{64}", recipe_digest):
        raise ValueError("a canonical tagged recipe_digest is required")
    provider_request_digest = str(
        parameters.get("provider_request_digest") or recipe_digest
    )
    if not re.fullmatch(r"blake3:[0-9a-f]{64}", provider_request_digest):
        raise ValueError("a canonical tagged provider_request_digest is required")
    normalization_profile = parameters.get("normalization_profile")
    if normalization_profile not in {None, "wiki-v1"}:
        raise ValueError("unsupported normalization_profile")
    cache_root = os.environ.get("BLOBFORGE_DATALAB_RESPONSE_CACHE")
    if not cache_root:
        raise ValueError("BLOBFORGE_DATALAB_RESPONSE_CACHE is required")

    request_id, request_value = _request_identity(
        _sha256_file(source), provider_request_digest, mode
    )
    response_path = _cache_path(Path(cache_root).expanduser(), request_id)
    account_key = str(parameters.get("provider_account") or "datalab:primary")
    maximum_micro_usd = round(max_cost * 1_000_000)
    checkpoint_key = f"sha256:{request_id}"
    if request.get("operation") == "probe":
        with _response_lock(response_path):
            existing = _read_cache(response_path, request_id, request_value)
        if existing is not None and existing["state"] == "failed":
            raise ValueError("cached Datalab request failed; refusing automatic repurchase")
        complete = existing is not None and existing["state"] == "complete"
        submitted = existing is not None and existing["state"] == "submitted"
        _write_cache(
            output / "probe.json",
            {
                "contract": PROBE_CONTRACT,
                "provider": "datalab",
                "account_key": account_key,
                "checkpoint_key": checkpoint_key,
                "cache_hit": complete,
                "checkpoint_state": (
                    "complete" if complete else "submitted" if submitted else "missing"
                ),
                "requests": 0 if existing is not None else 1,
                "pages": 0 if existing is not None else source_pages,
                "source_pages": source_pages,
                "estimated_micro_usd": 0 if existing is not None else maximum_micro_usd,
                "estimate_basis": (
                    "configured-per-job-ceiling" if existing is None else "existing-submission"
                ),
                **(
                    {"resume_reservation_id": existing["reservation_id"]}
                    if existing is not None
                    and isinstance(existing.get("reservation_id"), str)
                    and existing["reservation_id"]
                    else {}
                ),
            },
        )
        return 0
    if request.get("operation", "convert") != "convert":
        raise ValueError("unsupported adapter operation")
    if parameters.get("quota_managed") is True and not request.get("reservation_id"):
        raise ValueError("quota-managed conversion requires coordinator reservation")
    submitted_here = False
    completed_cache_hit = False
    with _response_lock(response_path):
        envelope = _read_cache(response_path, request_id, request_value)
        if envelope is not None and envelope["state"] == "failed":
            raise ValueError("cached Datalab request failed; refusing automatic repurchase")
        cache_status = "hit"
        if envelope is None or envelope["state"] == "submitted":
            if parameters.get("api_rights_confirmed") is not True:
                raise ValueError("api_rights_confirmed=true is required before provider access")
            api_key = os.environ.get("DATALAB_API_KEY")
            if not api_key:
                _attempt_report(
                    request, state="released", account_key=account_key,
                    checkpoint_key=checkpoint_key, requests=0, pages=0,
                    estimated_micro_usd=0, detail="API key missing before provider access",
                )
                raise ValueError("DATALAB_API_KEY is required before provider access")
            if envelope is None:
                try:
                    submission = _submit(source, api_key, source_pages, mode)
                except Exception as exc:
                    _attempt_report(
                        request,
                        state="rate_limited" if _rate_limited(exc) else "ambiguous",
                        account_key=account_key,
                        checkpoint_key=checkpoint_key,
                        requests=1,
                        pages=source_pages,
                        estimated_micro_usd=maximum_micro_usd,
                        detail=str(exc),
                        retry_after_ms=_retry_after_ms(exc),
                    )
                    raise
                check_url = _safe_check_url(submission.get("request_check_url"))
                envelope = {
                    "contract": CACHE_CONTRACT,
                    "request_identity": f"sha256:{request_id}",
                    "request": request_value,
                    "state": "submitted",
                    "request_check_url": check_url,
                    "submission": submission,
                    **(
                        {"reservation_id": request["reservation_id"]}
                        if request.get("reservation_id")
                        else {}
                    ),
                }
                _write_cache(response_path, envelope)
                cache_status = "captured submission and result"
                submitted_here = True
            try:
                response = _poll(envelope["request_check_url"], api_key)
            except Exception as exc:
                _attempt_report(
                    request, state="ambiguous", account_key=account_key,
                    checkpoint_key=checkpoint_key,
                    requests=1 if submitted_here else 0,
                    pages=source_pages if submitted_here else 0,
                    estimated_micro_usd=maximum_micro_usd if submitted_here else 0,
                    detail=str(exc),
                )
                raise
            envelope["state"] = "complete"
            envelope["response"] = response
            _write_cache(response_path, envelope)
        else:
            completed_cache_hit = True
            response = envelope.get("response")
            if not isinstance(response, dict):
                raise ValueError("complete Datalab cache entry has no response")

    markdown, list_cents, final_cents = _validate_response(
        response, source_pages, max_cost
    )
    list_micro_usd = round(list_cents * 10_000) if list_cents is not None else None
    billed_micro_usd = round(final_cents * 10_000)
    credits_micro_usd = (
        max(0, list_micro_usd - billed_micro_usd)
        if list_micro_usd is not None
        else None
    )
    _attempt_report(
        request,
        state=(
            "cache_hit"
            if completed_cache_hit
            and request.get("reservation_id") != envelope.get("reservation_id")
            else "committed"
        ),
        account_key=account_key,
        checkpoint_key=checkpoint_key,
        requests=(
            0
            if completed_cache_hit
            and request.get("reservation_id") != envelope.get("reservation_id")
            else 1
        ),
        pages=(
            0
            if completed_cache_hit
            and request.get("reservation_id") != envelope.get("reservation_id")
            else source_pages
        ),
        estimated_micro_usd=(
            0
            if completed_cache_hit
            and request.get("reservation_id") != envelope.get("reservation_id")
            else maximum_micro_usd
        ),
        list_micro_usd=(
            0
            if completed_cache_hit
            and request.get("reservation_id") != envelope.get("reservation_id")
            else list_micro_usd
        ),
        billed_micro_usd=(
            0
            if completed_cache_hit
            and request.get("reservation_id") != envelope.get("reservation_id")
            else billed_micro_usd
        ),
        credits_micro_usd=(
            0
            if completed_cache_hit
            and request.get("reservation_id") != envelope.get("reservation_id")
            else credits_micro_usd
        ),
    )
    (native_dir / "response.json").write_text(
        json.dumps(response, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    replacements: dict[str, str] = {}
    asset_media_types: dict[str, str] = {}
    asset_dimensions: dict[str, tuple[int, int] | None] = {}
    images = response.get("images") or {}
    if not isinstance(images, dict):
        raise ValueError("Datalab response images must be an object")
    for index, (original, value) in enumerate(sorted(images.items())):
        if not isinstance(value, str):
            raise ValueError("Datalab image payload must be base64 text")
        decoded, declared = _decode_image(value)
        media_type = _image_media_type(decoded, declared)
        name = _asset_name(index, original, media_type)
        if Path(original).name in replacements:
            raise ValueError("Datalab response repeats an image basename")
        (assets / name).write_bytes(decoded)
        replacements[Path(original).name] = name
        asset_media_types[name] = media_type
        asset_dimensions[name] = raster_dimensions(decoded)

    page_markdowns = []
    for page_markdown in _split_pages(markdown, source_pages):
        page_markdowns.append(
            LINK_RE.sub(
                lambda match: (
                    f"{match.group(1)}assets/{replacements[Path(match.group(2)).name]}{match.group(3)}"
                    if Path(match.group(2)).name in replacements
                    else match.group(0)
                ),
                page_markdown,
            )
        )
    normalization_stats = None
    if normalization_profile == "wiki-v1":
        page_markdowns, normalization_stats = normalize_datalab_pages(
            page_markdowns, asset_dimensions
        )

    output_markdown = ""
    mappings = []
    for page, page_markdown in enumerate(page_markdowns):
        if output_markdown:
            output_markdown += "\n\n"
        start = len(output_markdown.encode("utf-8"))
        output_markdown += page_markdown
        end = len(output_markdown.encode("utf-8"))
        if end > start:
            mappings.append(_mapping(page, start, end))
    (data / "text.md").write_text(output_markdown, encoding="utf-8")
    (data / "source-map.json").write_text(
        json.dumps({"mappings": mappings, "references": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    members = [
        {
            "path": "renditions/com.datalab/convert-response.json",
            "file": "data/native/response.json",
            "role": "rendition",
            "media_type": "application/json",
            "namespace": "com.datalab",
        }
    ]
    referenced_assets = referenced_asset_names(output_markdown)
    for path in sorted(assets.iterdir()):
        if normalization_profile == "wiki-v1" and path.name not in referenced_assets:
            continue
        members.append(
            {
                "path": f"assets/{path.name}",
                "file": f"data/assets/{path.name}",
                "role": "asset",
                "media_type": asset_media_types[path.name],
            }
        )
    credits_cents = (
        max(0.0, list_cents - final_cents) if list_cents is not None else None
    )
    billing_message = f"billed=${final_cents / 100:.4f}"
    if list_cents is None:
        billing_message += "; list price and credits/discount unavailable"
    else:
        billing_message += (
            f"; list=${list_cents / 100:.4f}; "
            f"credits/discount=${credits_cents / 100:.4f}"
        )
    bundle = {
        "contract": CONTRACT,
        "text_path": "data/text.md",
        "source_map": "data/source-map.json",
        "members": members,
        "tool": {"name": "blobforge-datalab-adapter", "version": ADAPTER_VERSION},
        **(
            {
                "additional_tools": [
                    {"name": "blobforge-wiki-normalizer", "version": "1.0.0"}
                ],
                "markdown_features": ["raw-html", "semantic-html-table-v1"],
            }
            if normalization_profile == "wiki-v1"
            else {}
        ),
        "models": [
            {
                "provider": "datalab",
                "identifier": "datalab-convert-accurate",
                "resolution": "mutable-alias",
            }
        ],
        "parameters": {
            "endpoint": API_URL,
            "output_format": "markdown",
            "mode": mode,
            "paginate": True,
            "disable_image_extraction": False,
            "disable_image_captions": False,
            "skip_cache": False,
            "recipe_digest": recipe_digest,
            **(
                {"provider_request_digest": provider_request_digest}
                if "provider_request_digest" in parameters
                else {}
            ),
            **(
                {"normalization_profile": normalization_profile}
                if normalization_profile is not None
                else {}
            ),
        },
        "diagnostics": [
            {
                "severity": "warning",
                "message": "Datalab does not expose immutable model checkpoint identities for this managed recipe.",
            },
            {
                "severity": "info",
                "message": (
                    f"Provider response cache {cache_status}; pages={source_pages}; "
                    f"{billing_message}."
                ),
            },
            *(
                [
                    {
                        "severity": "info",
                        "message": f"Wiki normalization applied: {normalization_stats}.",
                    }
                ]
                if normalization_stats is not None
                else []
            ),
        ],
    }
    (output / "bundle.json").write_text(
        json.dumps(bundle, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
