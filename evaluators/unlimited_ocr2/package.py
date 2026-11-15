#!/usr/bin/env python3
"""Package one cached Unlimited-OCR-2 run as a validated MDAF.

Runs locally in the BlobForge environment plus Pillow, with no GPU:

    uv run --with pillow==13.0.0 python evaluators/unlimited_ocr2/package.py \\
        --evidence .blobforge-migration/evaluations/unlimited-ocr2-colab-canary \\
        --run storypath-page-local -o storypath.uocr2-page-local.mdaf
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from blobforge.converters.runner import run_converter

ADAPTER = Path(__file__).resolve().parent / "adapter.py"


def select_units(units_root: Path) -> list[Path]:
    """Top-level units in page order, descending into halves of a failed unit."""
    records = {}
    for path in units_root.glob("*/result.json"):
        records[path.parent.name] = json.loads(path.read_text(encoding="utf-8"))

    def expand(key: str) -> list[str]:
        children = sorted(k for k, r in records.items() if r.get("fallback_of") == key)
        if children and not records[key]["analysis"]["ok"]:
            return [item for child in children for item in expand(child)]
        return [key]

    tops = [k for k, r in records.items() if r.get("fallback_of") is None]
    selected = [item for key in tops for item in expand(key)]
    return [units_root / key for key in sorted(selected, key=lambda k: records[k]["pages"][0])]


def latest_environment(out: Path) -> dict[str, Any]:
    paths = sorted((out / "environment").glob("*.json"))
    if not paths:
        raise SystemExit(f"no environment record under {out}")
    return json.loads(paths[-1].read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--evidence", type=Path, required=True,
                        help="canary directory holding inputs/ and out/")
    parser.add_argument("--run", required=True)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--output", "-o", type=Path, required=True)
    args = parser.parse_args()

    out = args.evidence / "out"
    plan = json.loads((out / "plan.json").read_text(encoding="utf-8"))
    entry = next(run for run in plan["runs"] if run["id"] == args.run)
    source = plan["sources"][entry["source"]]
    if source.get("kind", "pdf") != "pdf":
        raise SystemExit("only PDF sources can be packaged")
    units = select_units(out / "units" / args.run / f"r{args.repeat}")
    if not units:
        raise SystemExit(f"no units for {args.run} r{args.repeat}")
    recipe_path = out / "recipes" / f"{entry['mode']}.json"
    recipe_id = json.loads(recipe_path.read_text(encoding="utf-8"))["recipe_id"]
    result = run_converter(
        [sys.executable, str(ADAPTER)],
        args.evidence / "inputs" / source["file"],
        args.output,
        parameters={
            "run": args.run,
            "mode": entry["mode"],
            "unit_dirs": [str(unit.resolve()) for unit in units],
            "rendered_root": str((out / "rendered" / entry["source"] / f"{plan['dpi']}dpi").resolve()),
            "recipe_id": recipe_id,
            "recipe_path": str(recipe_path.resolve()),
            "source_sha256": source["sha256"],
            "environment": latest_environment(out),
        },
        original_name=source.get("display_name", source["file"]),
        timeout_seconds=600,
    )
    print(json.dumps({"output": str(args.output), "identity": result.identity,
                      "units": [unit.name for unit in units],
                      "diagnostics": list(result.diagnostics)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
