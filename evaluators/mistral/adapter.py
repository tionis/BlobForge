"""Mistral OCR 4.1 API adapter with durable response capture.

The provider response is atomically persisted outside the disposable converter
workspace before any MDAF packaging work starts. A retry with the same source
and output-affecting request therefore replays it instead of spending quota.
"""

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
from contextlib import contextmanager
from importlib.metadata import version
from pathlib import Path
from typing import Any

REPOSITORY = Path(__file__).resolve().parents[2]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from blobforge.normalization import normalize_mistral_pages, referenced_asset_names

CONTRACT = "dev.tionis.blobforge.converter-bundle/v1"
CACHE_CONTRACT = "dev.tionis.blobforge.mistral-response/v1"
PRICE_PER_PAGE_USD = 0.004
LINK_RE = re.compile(r"(!?\[[^\]]*\]\()([^\)\s]+)(\))")
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _model_dump(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        result = value.model_dump(mode="json", exclude_none=True)
    elif isinstance(value, dict):
        result = value
    else:
        raise TypeError(f"cannot serialize {type(value).__name__}")
    if not isinstance(result, dict):
        raise TypeError("Mistral response must serialize to an object")
    return result


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _request_identity(
    source_sha256: str, recipe_digest: str, model: str
) -> tuple[str, dict[str, Any]]:
    request = {
        "source_sha256": f"sha256:{source_sha256}",
        "recipe_digest": recipe_digest,
        "model": model,
        "include_image_base64": True,
        "include_blocks": True,
        "confidence_scores_granularity": "block",
    }
    encoded = json.dumps(
        request, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest(), request


def _cache_path(cache_root: Path, request_identity: str) -> Path:
    return cache_root / request_identity[:2] / f"{request_identity}.json"


@contextmanager
def _response_lock(response_path: Path):
    """Serialize identical paid requests; kernel locks recover after crashes."""
    import fcntl

    response_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = response_path.with_suffix(".lock")
    with lock_path.open("a", encoding="utf-8") as handle:
        os.chmod(lock_path, 0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _read_cached_response(
    path: Path, request_identity: str, expected_request: dict[str, Any]
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        envelope = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"invalid Mistral response cache entry {path}: {error}") from error
    if (
        not isinstance(envelope, dict)
        or envelope.get("contract") != CACHE_CONTRACT
        or envelope.get("request_identity") != f"sha256:{request_identity}"
        or envelope.get("request") != expected_request
        or not isinstance(envelope.get("response"), dict)
    ):
        raise ValueError(f"Mistral response cache entry does not match request: {path}")
    return envelope["response"]


def _write_cached_response(
    path: Path,
    request_identity: str,
    request_value: dict[str, Any],
    response: dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    envelope = {
        "contract": CACHE_CONTRACT,
        "request_identity": f"sha256:{request_identity}",
        "request": request_value,
        "response": response,
    }
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


def _perform_request(source: Path, model: str, api_key: str) -> dict[str, Any]:
    # Keep the SDK out of offline unit tests and the BlobForge parent process.
    from mistralai.client import Mistral

    uploaded_id = None
    with Mistral(api_key=api_key) as client:
        try:
            uploaded = client.files.upload(
                file={"file_name": source.name, "content": source.read_bytes()},
                purpose="ocr",
            )
            uploaded_id = uploaded.id
            signed = client.files.get_signed_url(file_id=uploaded.id, expiry=60)
            response = client.ocr.process(
                model=model,
                document={"type": "document_url", "document_url": signed.url},
                include_image_base64=True,
                include_blocks=True,
                confidence_scores_granularity="block",
            )
            return _model_dump(response)
        finally:
            if uploaded_id is not None:
                client.files.delete(file_id=uploaded_id)


def _page_count(source: Path) -> int:
    from pypdf import PdfReader

    return len(PdfReader(source).pages)


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
        raise ValueError("Mistral image payload is not a supported raster image")
    if declared and declared != detected:
        raise ValueError(
            f"Mistral image media type mismatch: declared {declared}, detected {detected}"
        )
    return detected


def _asset_name(
    page_number: int, image_index: int, original_id: str, media_type: str | None
) -> str:
    original = SAFE_NAME_RE.sub("-", Path(original_id).name).strip(".-") or "image"
    suffix = mimetypes.guess_extension(media_type) if media_type else None
    suffix = suffix or Path(original).suffix
    if not suffix:
        suffix = ".bin"
    stem = Path(original).stem[:80] or "image"
    return f"page-{page_number:04d}-{image_index:03d}-{stem}{suffix.lower()}"


def _page_confidence(page: dict[str, Any]) -> float | None:
    scores = page.get("confidence_scores")
    if not isinstance(scores, dict):
        return None
    confidence = scores.get("average_page_confidence_score")
    if isinstance(confidence, (int, float)) and not isinstance(confidence, bool):
        value = float(confidence)
        if 0 <= value <= 1:
            return value
    return None


def _validate_response(native: dict[str, Any], source_pages: int) -> list[dict[str, Any]]:
    if not isinstance(native.get("model"), str) or not native["model"]:
        raise ValueError("Mistral response is missing the returned model identity")
    pages = native.get("pages")
    if not isinstance(pages, list):
        raise ValueError("Mistral response pages must be an array")
    indices = []
    for fallback_page, page in enumerate(pages):
        if not isinstance(page, dict) or not isinstance(page.get("markdown"), str):
            raise ValueError(f"Mistral page {fallback_page} is malformed")
        index = page.get("index", fallback_page)
        if isinstance(index, bool) or not isinstance(index, int):
            raise ValueError(f"Mistral page {fallback_page} has an invalid index")
        indices.append(index)
    if indices != list(range(source_pages)):
        raise ValueError(
            "Mistral response page indices do not exactly cover the source: "
            f"expected 0..{source_pages - 1}, got {indices[:12]}"
        )
    usage = native.get("usage_info")
    processed = usage.get("pages_processed") if isinstance(usage, dict) else None
    if (
        isinstance(processed, bool)
        or not isinstance(processed, int)
        or processed != source_pages
    ):
        raise ValueError("Mistral usage_info.pages_processed does not match the source")
    return pages


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
    page_count = _page_count(source)
    if page_count <= 0:
        raise ValueError("source PDF has no pages")
    max_pages = int(parameters.get("max_pages") or 0)
    max_cost = float(parameters.get("max_cost_usd") or 0)
    expected_cost = page_count * PRICE_PER_PAGE_USD
    if max_pages <= 0 or page_count > max_pages:
        raise ValueError(f"page ceiling rejected {page_count} pages (limit {max_pages})")
    if not math.isfinite(max_cost) or max_cost <= 0 or expected_cost > max_cost:
        raise ValueError(
            f"spend ceiling rejected estimated ${expected_cost:.4f} (limit ${max_cost:.4f})"
        )
    model = str(parameters.get("model") or "mistral-ocr-4-1")
    if model != "mistral-ocr-4-1":
        raise ValueError("this frozen evaluator only permits model mistral-ocr-4-1")
    if parameters.get("do_ocr") is False:
        raise ValueError("the frozen Mistral recipe requires OCR")
    if parameters.get("do_table_structure") is False:
        raise ValueError("the frozen Mistral recipe requires table extraction")
    if parameters.get("extract_images") is False:
        raise ValueError("the frozen Mistral recipe requires image extraction")
    recipe_digest = str(parameters.get("recipe_digest") or "")
    if not re.fullmatch(r"blake3:[0-9a-f]{64}", recipe_digest):
        raise ValueError("a canonical tagged recipe_digest is required")
    provider_request_digest = str(
        parameters.get("provider_request_digest") or recipe_digest
    )
    if not re.fullmatch(r"blake3:[0-9a-f]{64}", provider_request_digest):
        raise ValueError("a canonical tagged provider_request_digest is required")
    normalization_profile = parameters.get("normalization_profile")
    if normalization_profile not in {None, "wiki-v1", "wiki-v2"}:
        raise ValueError("unsupported normalization_profile")

    cache_root_value = os.environ.get("BLOBFORGE_MISTRAL_RESPONSE_CACHE")
    if not cache_root_value:
        raise ValueError("BLOBFORGE_MISTRAL_RESPONSE_CACHE is required")
    source_sha256 = _sha256_file(source)
    request_id, request_value = _request_identity(
        source_sha256, provider_request_digest, model
    )
    response_path = _cache_path(Path(cache_root_value).expanduser(), request_id)
    with _response_lock(response_path):
        native = _read_cached_response(response_path, request_id, request_value)
        cache_status = "hit"
        if native is None:
            if parameters.get("api_rights_confirmed") is not True:
                raise ValueError("api_rights_confirmed=true is required for a cache miss")
            api_key = os.environ.get("MISTRAL_API_KEY")
            if not api_key:
                raise ValueError("MISTRAL_API_KEY is required for a cache miss")
            native = _perform_request(source, model, api_key)
            # Persist provider success before validation, rendition creation,
            # or packaging so a local bug fix can replay it without another
            # API call.
            _write_cached_response(response_path, request_id, request_value, native)
            cache_status = "captured"

    pages = _validate_response(native, page_count)
    (native_dir / "response.json").write_text(
        json.dumps(native, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    normalization_stats = None
    if normalization_profile in {"wiki-v1", "wiki-v2"}:
        normalized_pages, normalization_stats = normalize_mistral_pages(
            pages,
            normalize_lists=normalization_profile == "wiki-v2",
        )
    else:
        normalized_pages = [page["markdown"] for page in pages]

    markdown = ""
    mappings = []
    asset_media_types: dict[str, str] = {}
    for page_number, page in enumerate(pages):
        page_markdown = normalized_pages[page_number]
        replacements: dict[str, str] = {}
        for image_index, image in enumerate(page.get("images", []) or []):
            if not isinstance(image, dict):
                raise ValueError(f"Mistral page {page_number} contains a malformed image")
            original_id = str(image.get("id") or f"page-{page_number}-image-{image_index}")
            image_data = image.get("image_base64")
            if not image_data:
                continue
            decoded, declared_media_type = _decode_image(str(image_data))
            media_type = _image_media_type(decoded, declared_media_type)
            name = _asset_name(page_number, image_index, original_id, media_type)
            (assets / name).write_bytes(decoded)
            asset_media_types[name] = media_type
            link_name = Path(original_id).name
            if link_name in replacements:
                raise ValueError(
                    f"Mistral page {page_number} repeats image id basename {link_name!r}"
                )
            replacements[link_name] = name
        page_markdown = LINK_RE.sub(
            lambda match: (
                f"{match.group(1)}assets/{replacements[Path(match.group(2)).name]}{match.group(3)}"
                if Path(match.group(2)).name in replacements
                else match.group(0)
            ),
            page_markdown,
        )
        if markdown:
            markdown += "\n\n"
        start = len(markdown.encode("utf-8"))
        markdown += page_markdown
        end = len(markdown.encode("utf-8"))
        if end > start:
            mapping: dict[str, Any] = {
                "document": {"start": start, "end": end},
                "source": {
                    "source_id": "document",
                    "selectors": [
                        {
                            "type": "interval",
                            "unit": "page",
                            "start": page_number,
                            "end": page_number + 1,
                        }
                    ],
                },
                "method": "dev.tionis.blobforge/mistral-ocr-page",
            }
            confidence = _page_confidence(page)
            if confidence is not None:
                mapping["confidence"] = confidence
            mappings.append(mapping)
    (data / "text.md").write_text(markdown, encoding="utf-8")
    (data / "source-map.json").write_text(
        json.dumps({"mappings": mappings, "references": []}, indent=2) + "\n",
        encoding="utf-8",
    )
    members = [
        {
            "path": "renditions/ai.mistral/ocr-response.json",
            "file": "data/native/response.json",
            "role": "rendition",
            "media_type": "application/json",
            "namespace": "ai.mistral",
        }
    ]
    referenced_assets = referenced_asset_names(markdown)
    for path in sorted(assets.iterdir()):
        if (
            normalization_profile in {"wiki-v1", "wiki-v2"}
            and path.name not in referenced_assets
        ):
            continue
        if path.is_file():
            members.append(
                {
                    "path": f"assets/{path.name}",
                    "file": f"data/assets/{path.name}",
                    "role": "asset",
                    "media_type": asset_media_types[path.name],
                }
            )
    returned_model = native.get("model")
    bundle = {
        "contract": CONTRACT,
        "text_path": "data/text.md",
        "source_map": "data/source-map.json",
        "members": members,
        "tool": {"name": "mistralai", "version": version("mistralai")},
        **(
            {
                "additional_tools": [
                    {
                        "name": "blobforge-wiki-normalizer",
                        "version": (
                            "2.0.0"
                            if normalization_profile == "wiki-v2"
                            else "1.0.0"
                        ),
                    }
                ],
                "markdown_features": ["raw-html", "semantic-html-table-v1"],
            }
            if normalization_profile in {"wiki-v1", "wiki-v2"}
            else {}
        ),
        "models": [
            {
                "provider": "mistral-ai",
                "identifier": model,
                **(
                    {"returned_identifier": returned_model}
                    if isinstance(returned_model, str) and returned_model
                    else {}
                ),
                "resolution": "mutable-alias",
            }
        ],
        "parameters": {
            "model": model,
            "include_blocks": True,
            "confidence_scores_granularity": "block",
            "include_image_base64": True,
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
                "message": "Mistral does not expose an immutable OCR checkpoint digest in this response.",
            },
            {
                "severity": "info",
                "message": (
                    f"Provider response cache {cache_status}; usage="
                    f"{native.get('usage_info', {})}; list-price estimate=${expected_cost:.6f}."
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
