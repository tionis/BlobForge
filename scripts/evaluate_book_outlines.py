"""Compare frozen and candidate outlines locally; never contacts a provider.

Run with uv run python scripts/evaluate_book_outlines.py /path/to/mdafs.
JSONL output is evaluation data, not an independent semantic-quality score.
"""

import argparse
import hashlib
import json
import zipfile
from pathlib import Path

from blobforge.normalization.book_structure import recover_book_structure
from blobforge.normalization.hierarchy import book_outline
from blobforge.normalization.mistral import render_mistral_response


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--ratios", nargs="+", type=float, default=[0.55, 0.65, 0.75])
    args = parser.parse_args()
    seen = {}
    for path in sorted(args.directory.glob("*.mdaf")):
        with zipfile.ZipFile(path) as archive:
            data = archive.read("renditions/ai.mistral/ocr-response.json")
        key = hashlib.sha256(data).digest()
        if key in seen:
            print(
                json.dumps({"file": path.name, "same_native_as": seen[key]}), flush=True
            )
            continue
        seen[key] = path.name
        native = json.loads(data)
        rendered = render_mistral_response(native, normalization_profile="wiki-v2")
        _, baseline = book_outline(rendered.text, native["pages"], rendered.source_map)
        experiments = []
        for ratio in args.ratios:
            _, report = recover_book_structure(
                rendered.text,
                native["pages"],
                rendered.source_map,
                geometry_ratio=ratio,
            )
            experiments.append({"geometry_ratio": ratio, **report})
        print(
            json.dumps(
                {
                    "file": path.name,
                    "pages": len(native["pages"]),
                    "baseline": baseline,
                    "experiments": experiments,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
