"""Read-only artifact selection and collision-safe local downloads."""

import os
import re
import tempfile
import unicodedata
from pathlib import Path

from .download_names import artifact_filename
from .hydrator import select_artifact


def resolve_sources(client, source=None, search=None):
    if bool(source) == (search is not None):
        raise ValueError("Specify either a source key/PDF name or --search QUERY")
    if source and re.fullmatch(r"(?:sha256:|blake3:)?[0-9a-fA-F]{64}", source):
        job = client.get_job(source)
        if not job:
            raise ValueError(f"Source {source} does not exist")
        return [job]
    query = search if search is not None else source
    if not query or not query.strip() or len(query) > 200:
        raise ValueError("Search must contain 1–200 characters")
    jobs = {}
    offset = 0
    while True:
        page = client.list_jobs(search=query, limit=200, offset=offset)
        rows = page.get("jobs", [])
        for job in rows:
            jobs[job["hash"]] = job
        offset += len(rows)
        if not rows or offset >= page.get("total", 0):
            break
    matches = list(jobs.values())
    if search is None:
        def normalized(name):
            return unicodedata.normalize("NFKC", name).casefold()
        exact = [job for job in matches if normalized(job.get("original_name", "")) == normalized(source)]
        matches = exact or matches
        if len(matches) > 1:
            choices = "; ".join(f"{j.get('original_name') or 'Unnamed'} [{j['hash']}]" for j in matches[:10])
            raise ValueError(f"Ambiguous source name; use a source key: {choices}")
    if not matches:
        raise ValueError(f"No sources match {query!r}")
    return matches


def plan_downloads(client, jobs, *, output=None, recipe_digest=None, mdaf=False, bulk=False, force=False):
    destination = Path(output) if output else Path.cwd()
    directory = output is None or destination.is_dir() or bulk
    if directory and not destination.is_dir():
        raise ValueError("The output directory must already exist")
    plans, skipped, names = [], [], set()
    for job in jobs:
        key = job["hash"]
        artifact = select_artifact({**job, "artifacts": client.list_artifacts(key)}, recipe_digest)
        if artifact is None or (mdaf and artifact.get("artifact_type") != "mdaf/v1"):
            reason = "No retained MDAF for the selected recipe" if mdaf else "No retained artifact for the selected recipe"
            if not bulk:
                raise ValueError(reason)
            skipped.append({"hash": key, "reason": reason})
            continue
        artifact_type = artifact.get("artifact_type") or "legacy-archive"
        name = artifact_filename(job.get("original_name", ""), key, artifact_type)
        path = destination / name if directory else destination
        identity = unicodedata.normalize("NFKC", str(path.absolute())).casefold()
        if identity in names:
            raise ValueError(f"Multiple sources would write {path}; download separately with explicit -o filenames")
        names.add(identity)
        if path.is_symlink() or (path.exists() and (not force or not path.is_file())):
            raise ValueError(f"Output already exists: {path}; use --force to replace a regular file")
        if not path.parent.is_dir():
            raise ValueError(f"Output directory does not exist: {path.parent}")
        plans.append({"hash": key, "original_name": job.get("original_name", ""),
                      "recipe_digest": artifact.get("recipe_digest"),
                      "artifact_type": artifact_type, "output": str(path)})
        if isinstance(artifact.get("size_bytes"), int):
            plans[-1]["size_bytes"] = artifact["size_bytes"]
    if not plans:
        raise ValueError("No matching sources have downloadable artifacts")
    return plans, skipped


def download_one(client, plan, *, force=False):
    """Publish only complete files, without overwriting by default (including races)."""
    path = Path(plan["output"])
    fd, temporary = tempfile.mkstemp(prefix=".blobforge-download-", dir=path.parent)
    os.close(fd)
    try:
        client.download_output(plan["hash"], temporary, plan["recipe_digest"])
        expected = plan.get("size_bytes")
        if expected is not None and Path(temporary).stat().st_size != expected:
            raise ValueError("Downloaded size does not match the retained artifact")
        if force:
            if path.is_symlink():
                raise ValueError(f"Refusing to replace a symlink: {path}")
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)
