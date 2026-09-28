"""Publish locally built legacy enrichment derivatives to the coordinator."""

from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .coordinator_client import CoordinatorClient, CoordinatorError
from .enrichment.legacy import enrichment_recipe, enrichment_recipe_digest
from .mdaf import validate_mdaf


@dataclass
class PublishSummary:
    recipe_digest: str
    execute: bool
    checked: int = 0
    counts: dict[str, int] = field(default_factory=dict)
    errors: list[dict[str, str]] = field(default_factory=list)

    def add(self, outcome: str) -> None:
        self.counts[outcome] = self.counts.get(outcome, 0) + 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "recipe_digest": self.recipe_digest,
            "execute": self.execute,
            "checked": self.checked,
            "counts": dict(sorted(self.counts.items())),
            "errors": self.errors,
        }


def _rows(workspace: Path, recipe: str, hashes: Iterable[str], limit: int | None) -> list[sqlite3.Row]:
    connection = sqlite3.connect(f"file:{workspace / 'catalog.sqlite3'}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        query = """SELECT legacy_sha256,base_mdaf_identity,output_path,mdaf_identity
            FROM legacy_enrichments WHERE recipe_digest=? AND status='converted'"""
        params: list[Any] = [recipe]
        selected = list(dict.fromkeys(hashes))
        if selected:
            query += f" AND legacy_sha256 IN ({','.join('?' for _ in selected)})"
            params.extend(selected)
        query += " ORDER BY legacy_sha256"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = list(connection.execute(query, params))
    finally:
        connection.close()
    if selected and len(rows) != len(selected):
        found = {row["legacy_sha256"] for row in rows}
        missing = ", ".join(sorted(set(selected) - found))
        raise ValueError(f"no converted enrichment for: {missing}")
    return rows


def _plan(client: CoordinatorClient, row: sqlite3.Row, recipe: str, select: bool) -> str:
    """Classify one derivative; raises ValueError for anything unsafe to publish."""
    path = Path(row["output_path"])
    if not path.is_file():
        raise ValueError(f"derivative is missing: {path}")
    validated = validate_mdaf(path)
    if validated.identity != row["mdaf_identity"]:
        raise ValueError("derivative identity does not match the catalog")
    if row["base_mdaf_identity"] not in validated.manifest.get("derived_from", []):
        raise ValueError("derivative does not declare its catalogued base artifact")
    key = row["legacy_sha256"]
    artifacts = client.list_artifacts(key)
    parents = [item for item in artifacts if item.get("identity") == row["base_mdaf_identity"]]
    if len(parents) != 1:
        raise ValueError("production does not retain the base artifact")
    targets = [item for item in artifacts if item.get("recipe_digest") == recipe]
    if targets and targets[0].get("identity") != validated.identity:
        raise ValueError("production holds a different artifact for this recipe")
    if not targets:
        return "import"
    job = client.get_job(key)
    if (
        select
        and job.get("status") == "done"
        and job.get("recipe_digest") == parents[0].get("recipe_digest")
    ):
        return "select"
    return "present"


def publish_enrichments(
    workspace: str | Path,
    client: CoordinatorClient,
    *,
    hashes: Iterable[str] = (),
    limit: int | None = None,
    execute: bool = False,
    select: bool = True,
) -> PublishSummary:
    """Plan, or idempotently publish, catalogued derivatives of retained artifacts.

    Every item is validated locally and checked against the coordinator's
    retained base artifact before anything is sent. Re-runs skip items that
    are already present, so an interrupted publication can simply be resumed.
    """
    recipe = enrichment_recipe_digest(enrichment_recipe(validate_runtime=False))
    rows = _rows(Path(workspace), recipe, hashes, limit)
    summary = PublishSummary(recipe_digest=recipe, execute=execute)
    for index, row in enumerate(rows, 1):
        key = row["legacy_sha256"]
        summary.checked += 1
        try:
            outcome = _plan(client, row, recipe, select)
            if execute and outcome in {"import", "select"}:
                result = client.import_artifact(key, recipe, row["output_path"], select=select)
                if result.get("identity") != row["mdaf_identity"]:
                    raise ValueError("coordinator recorded a different identity")
                outcome = "imported" if result.get("action") == "imported" else "already-present"
                if result.get("selected"):
                    summary.add("selected")
        except (CoordinatorError, OSError, ValueError) as exc:
            summary.add("error")
            summary.errors.append({"source_key": key, "error": str(exc)})
            continue
        summary.add(outcome)
        if index % 50 == 0 or index == len(rows):
            print(f"{index:,}/{len(rows):,} checked", file=sys.stderr, flush=True)
    return summary
