"""Portable download names; storage identities and artifact bytes stay unchanged."""

import re
import unicodedata
from urllib.parse import quote


def safe_basename(value: str, fallback: str = "artifact", *, max_bytes: int = 240) -> str:
    name = str(value or "").replace("\\", "/").rsplit("/", 1)[-1]
    name = "".join(c for c in name if not unicodedata.category(c).startswith("C"))
    name = re.sub(r'[<>:"/\\|?*]', "_", name).strip(" .")
    # Stay below common 255-byte filesystem limits, including the extension.
    name = name.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore").rstrip(" .")
    if not name:
        name = fallback
    if re.fullmatch(r"(?i:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])", name.split(".")[0]):
        name = "_" + name
    return name


def artifact_filename(original_name: str, source_key: str, artifact_type: str) -> str:
    name = safe_basename(original_name, safe_basename(source_key))
    stem = name.rsplit(".", 1)[0] if "." in name else name
    extension = ".mdaf" if artifact_type == "mdaf/v1" else ".zip"
    return safe_basename(stem, max_bytes=220) + extension


def content_disposition(filename: str) -> str:
    name = safe_basename(filename)
    fallback = name.encode("ascii", errors="replace").decode().replace("?", "_")
    return f'attachment; filename="{fallback}"; filename*=UTF-8\'\'{quote(name, safe="")}'
