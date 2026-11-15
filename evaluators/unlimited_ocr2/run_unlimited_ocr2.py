#!/usr/bin/env python3
"""Resumable Unlimited-OCR-2 evidence runner for a temporary GPU host.

The runner renders pinned PDF pages, verifies the pinned model snapshot, runs
page-local (gundam) or bounded multi-page (base) generation through the
upstream Transformers path, and caches every unit's raw generation and
diagnostics atomically. A disconnected session resumes without repeating
completed units. Packaging into ConversionBundle/MDAF happens later, locally,
from the cached evidence; this script never builds an artifact.

Subcommands:
  run        execute a plan (needs CUDA, torch and the pinned model)
  summarize  aggregate cached unit results into summary.json
  self-test  exercise rendering, caching, parsing and checks with a fake model
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import tempfile
import time
from importlib import metadata
from pathlib import Path
from typing import Any, Callable

MODEL_REPO = "baidu/Unlimited-OCR-2"
MODEL_REVISION = "9cf077c4b206e0e2a31879df937b0b0bcba3661c"

# SHA-256 of every snapshot file the model loads. Code files were reviewed for
# network, process and deserialization calls before pinning; see README.md.
MODEL_FILES = {
    "_sparse_base_impl.py": "86da9c5638704bb9338575040568a97b3e10820bbeb426f51c07e866f3423ccd",
    "config.json": "607d130a70d5af5a35329aa237acd68651f45504c62cc7ced981066f4e781126",
    "configuration_deepseek_v2.py": "b8470dd616ba8745fce6e27b093aef73a098863cc891b2477dcf9326a36000f7",
    "conversation.py": "ec7b6ce89bcda643de1f43269ffa66a7b2e65dc3ed30e427958f776546b4ba03",
    "dash_attention.py": "960b460acd816df8e561e73dccd5683acb95445eddc723c4f0d6ed1f462b2f77",
    "deepencoder.py": "419b63388c1645b9b72c716070cdca36cd0be6047049424dc75941d3a00aaf1f",
    "generation_config.json": "dcd0479f3a12e5e4413d956197f8a4074408e4901834b38c7e036519134e05af",
    "model-00001-of-00002.safetensors": "4c40b740f209fd7682040d4ebf19409208acde5c401ccd2dde426d0f41542f18",
    "model-00002-of-00002.safetensors": "9490b82fa47f89e5110eb5577f8ab1b6694c12121905dbc086ddb10207a4ba0c",
    "model.safetensors.index.json": "dc60a31ecb77eb03e166345d18cbe25bc71d2a19cc6fc089d8a9ac1c7f209671",
    "modeling_deepseekv2.py": "24a15dfd47578ab24a7a4d4e977dc5d7a23dfef0a2a4c4bf73fcf3e2ce7acaf2",
    "modeling_unlimitedocr.py": "f6df813cef23cc28b1daca40fd8cb20740d05a238e7b9dfa2334531fdcf39f1f",
    "packing_patch.py": "e60d842c210c9f1d6cd2ae328cf9127a75f7941a0cea2a8e9abc14662f770c6c",
    "sparse_gather_e2e_triton.py": "58f1bc46093d47b00744970677e66bfbd7485a2d13810f46d13d867ceda13dd4",
    "sparse_gather_triton.py": "c3de717ffaca40240c665b8d645f15a9d1999feeabaa94beb35caad877c2f0f4",
    "special_tokens_map.json": "9e89493d7938e7710c524dbaff613c3648f0a018262417af78505128004e5cb4",
    "tokenizer.json": "c55c165e32c5a67830362a53e1f548c8930dcc8469efa30ff3252a982799c589",
    "tokenizer_config.json": "ceba7d221f265cca4c98b5e0a1d48c10107270b7b29c12396610d6d0fd64b126",
}

PAGE_TOKEN = "<PAGE>"
ANALYSIS_PARSER = 2  # 1: v1 det tags only; 2: adds v2 BLOCK/LEVEL/FORK/MERGE syntax
PAGE_TOKEN_ID = 128827

# Generation settings follow the authors' published SGLang evaluation request
# (temperature 0, 8,000 tokens per page, 20,000-token page guard, 35-gram
# no-repeat window 128 for one page and 1,024 for several).
GENERATION = {
    "prompt": "Multi page parsing.",
    "temperature": 0.0,
    "tokens_per_page": 8000,
    "max_page_tokens": 20000,
    "page_count_guard": True,
    "no_repeat_ngram_size": 35,
    "ngram_window_single": 128,
    "ngram_window_multi": 1024,
    "base_size": 1024,
    "image_size": 640,
    "max_crops": 16,
    "dash_topm": 16,
}

MODES = {
    "page-local": {"crop_mode": True, "image_mode": "gundam"},
    "multi-page": {"crop_mode": False, "image_mode": "base"},
}

RUNTIME_PACKAGES = (
    "torch", "torchvision", "triton", "transformers", "tokenizers",
    "huggingface-hub", "safetensors", "pymupdf", "pillow", "numpy",
)

MERGE_TOKEN = "<MERGE>"
BOX_COORDS_RE = re.compile(r"\[\s*-?\d+\s*,\s*-?\d+\s*,\s*-?\d+\s*,\s*-?\d+\s*\]")
BLOCK_V2_RE = re.compile(r"(?P<label>[a-z_]+)?(?:<LEVEL(?P<level>\d+)>)?(?P<box>\[[^\]<\n]*\])?(?P<fork><FORK>)?")
BLOCK_RE = re.compile(
    r"(?:<\|ref\|>(?P<ref>.*?)<\|/ref\|>)?<\|det\|>\s*(?P<label>[A-Za-z_][\w-]*)?\s*"
    r"(?P<box>\[(?:[^\[\]]|\[[^\[\]]*\])*\])\s*<\|/det\|>",
    re.DOTALL,
)


# ---------------------------------------------------------------- utilities

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def write_json(path: Path, value: Any) -> None:
    write_atomic(path, json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False).encode() + b"\n")


def package_versions() -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for name in RUNTIME_PACKAGES:
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            versions[name] = None
    return versions


# ---------------------------------------------------------------- plan

def load_plan(path: Path) -> dict[str, Any]:
    plan = json.loads(path.read_text(encoding="utf-8"))
    sources = plan["sources"]
    seen: set[tuple[str, int]] = set()
    for run in plan["runs"]:
        for repeat in repeats_for(run):
            if (run["id"], repeat) in seen:
                raise ValueError(f"duplicate run id {run['id']} repeat {repeat}")
            seen.add((run["id"], repeat))
        if run["mode"] not in MODES:
            raise ValueError(f"run {run['id']}: unknown mode {run['mode']}")
        if run["source"] not in sources:
            raise ValueError(f"run {run['id']}: unknown source {run['source']}")
        if run["mode"] == "multi-page" and int(run.get("chunk", 0)) < 2:
            raise ValueError(f"run {run['id']}: multi-page needs chunk >= 2")
    return plan


def repeats_for(run: dict[str, Any]) -> list[int]:
    # "repeat" schedules one later repeat of an earlier entry with the same id.
    return [int(run["repeat"])] if "repeat" in run else list(range(1, int(run.get("repeats", 1)) + 1))


def recipe_for(plan: dict[str, Any], mode: str) -> dict[str, Any]:
    """Every output-affecting setting for one extraction mode."""
    return {
        "evaluator": "blobforge-unlimited-ocr2-evaluator/0.1.0",
        "model": {"repo": MODEL_REPO, "revision": MODEL_REVISION, "files": MODEL_FILES},
        "runtime": "transformers-native",
        "dtype": "bfloat16",
        "mode": mode,
        **MODES[mode],
        "generation": GENERATION,
        "render": {"renderer": "pymupdf", "dpi": plan["dpi"], "colorspace": "rgb", "alpha": False},
    }


def recipe_id(recipe: dict[str, Any]) -> str:
    # Evaluation identity only, not a registered BlobForge recipe digest.
    return "sha256:" + sha256_bytes(canonical_json(recipe))


def units_for(run: dict[str, Any], pages: list[int]) -> list[list[int]]:
    if run["mode"] == "page-local":
        return [[page] for page in pages]
    chunk = int(run["chunk"])
    return [pages[i:i + chunk] for i in range(0, len(pages), chunk)]


def unit_key(pages: list[int]) -> str:
    return f"p{pages[0]:04d}" if len(pages) == 1 else f"p{pages[0]:04d}-p{pages[-1]:04d}"


# ---------------------------------------------------------------- rendering

def render_source(source_id: str, source: dict[str, Any], inputs: Path, cache: Path,
                  dpi: int, pages: list[int]) -> dict[int, dict[str, Any]]:
    """Render (or load) the requested 1-based pages and return their evidence."""
    path = inputs / source["file"]
    actual = sha256_file(path)
    if actual != source["sha256"]:
        raise SystemExit(f"{path}: SHA-256 {actual} does not match plan {source['sha256']}")
    out: dict[int, dict[str, Any]] = {}
    if source.get("kind", "pdf") == "image":
        if pages != [1]:
            raise ValueError(f"image source {source_id} has exactly one page")
        from PIL import Image

        with Image.open(path) as image:
            width, height = image.size
        out[1] = {"page": 1, "image": str(path), "png_sha256": actual,
                  "width": width, "height": height, "kind": "retained-raster",
                  "note": source.get("note")}
        return out

    import fitz  # PyMuPDF

    scale = dpi / 72
    with fitz.open(path) as doc:
        for page_no in pages:
            target = cache / source_id / f"{dpi}dpi" / f"page-{page_no:04d}.png"
            meta_path = target.with_suffix(".json")
            if target.exists() and meta_path.exists():
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if sha256_file(target) == meta["png_sha256"]:
                    out[page_no] = meta
                    continue
            page = doc[page_no - 1]
            pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), colorspace=fitz.csRGB, alpha=False)
            png = pix.tobytes("png")
            write_atomic(target, png)
            meta = {
                "page": page_no,
                "image": str(target),
                "png_sha256": sha256_bytes(png),
                "pixels_sha256": sha256_bytes(pix.samples),
                "width": pix.width,
                "height": pix.height,
                "dpi": dpi,
                "pixmap_matrix": [scale, 0, 0, scale, 0, 0],
                "page_rect": list(page.rect),
                "mediabox": list(page.mediabox),
                "cropbox": list(page.cropbox),
                "rotation": page.rotation,
                "derotation_matrix": list(page.derotation_matrix),
                "renderer": f"pymupdf {fitz.VersionBind}",
            }
            write_json(meta_path, meta)
            out[page_no] = meta
    return out


# ---------------------------------------------------------------- analysis

def box_list(raw: str | None) -> list[list[int]] | None:
    """Validate one box, or (as upstream's v1 parser allows) a list of boxes."""
    try:
        values = json.loads(raw) if raw else None
    except json.JSONDecodeError:
        return None
    boxes = values if isinstance(values, list) and values and isinstance(values[0], list) else [values]
    valid = all(
        isinstance(box, list) and len(box) == 4
        and all(isinstance(v, int) and 0 <= v <= 999 for v in box)
        and box[0] < box[2] and box[1] < box[3]
        for box in boxes
    )
    return boxes if valid else None


def make_block(label: str, box_raw: str | None, text: str, width: int, height: int,
               well_formed: bool = True, **extra: Any) -> dict[str, Any]:
    boxes = box_list(box_raw) if well_formed else None
    block: dict[str, Any] = {"label": label, "box_raw": box_raw, "text": text, "valid_box": boxes is not None,
                             **extra}
    if boxes is not None:
        block["boxes_norm"] = boxes
        block["boxes_px"] = [[x1 / 999 * width, y1 / 999 * height, x2 / 999 * width, y2 / 999 * height]
                             for x1, y1, x2, y2 in boxes]
    return block


def parse_blocks(text: str, width: int, height: int) -> tuple[list[dict[str, Any]], int]:
    """Parse grounding blocks; boxes are 0..999 normalized to the rendered page.

    v2 emits ``<BLOCK>label<LEVELn>[box]<FORK>text``; ``<MERGE>`` after a block
    joins it to the next across a column break, and multi-page output labels
    page-break continuations ``cross_text``/``append_text``. v1 emitted
    ``<|det|>label [box]<|/det|>text``.
    """
    if "<BLOCK>" not in text:
        blocks = []
        matches = list(BLOCK_RE.finditer(text))
        for index, match in enumerate(matches):
            end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            label = (match.group("label") or match.group("ref") or "").strip()
            blocks.append(make_block(label, match.group("box"), text[match.end():end].strip(), width, height))
        return blocks, sum(1 for b in blocks if not b["valid_box"])
    blocks = []
    merge_pending = False
    for segment in text.split("<BLOCK>")[1:]:
        match = BLOCK_V2_RE.match(segment)
        body = segment[match.end():]
        merges_next = body.rstrip().endswith(MERGE_TOKEN)
        if merges_next:
            body = body.rstrip()[:-len(MERGE_TOKEN)]
        blocks.append(make_block(
            match.group("label") or "", match.group("box"), body.strip(), width, height,
            well_formed=bool(match.group("label") and match.group("fork")),
            level=int(match.group("level")) if match.group("level") else None,
            merged_from_previous=merge_pending,
        ))
        merge_pending = merges_next
    return blocks, sum(1 for b in blocks if not b["valid_box"])


def text_sha256(text: str) -> str:
    """Digest of the output with box coordinates blanked, for repeat comparison."""
    return sha256_bytes(BOX_COORDS_RE.sub("[]", text).encode("utf-8"))


def split_pages(text: str) -> tuple[str, list[str]]:
    parts = text.split(PAGE_TOKEN)
    return parts[0], parts[1:]


def page_token_counts(token_ids: list[int]) -> list[int]:
    counts: list[int] = []
    for token in token_ids:
        if token == PAGE_TOKEN_ID:
            counts.append(0)
        elif counts:
            counts[-1] += 1
    return counts


def analyze(result: dict[str, Any], pages: list[dict[str, Any]], events: list[dict[str, Any]]) -> dict[str, Any]:
    preamble, page_texts = split_pages(result["text"])
    counts = page_token_counts(result["token_ids"])
    per_page = []
    for index, page_text in enumerate(page_texts):
        meta = pages[index] if index < len(pages) else {"page": None, "width": 1, "height": 1}
        blocks, invalid = parse_blocks(page_text, meta["width"], meta["height"])
        per_page.append({
            "page": meta["page"],
            "generated_tokens": counts[index] if index < len(counts) else None,
            "blocks": len(blocks),
            "invalid_boxes": invalid,
            "labels": sorted({b["label"] for b in blocks}),
            "heading_levels": sorted({b["level"] for b in blocks if b.get("level")}),
            "merges": sum(1 for b in blocks if b.get("merged_from_previous")),
            "continuations": sum(1 for b in blocks if b["label"] in ("cross_text", "append_text")),
            "empty": not page_text.strip(),
        })
    cap = GENERATION["max_page_tokens"]
    failures = []
    if result["finish_reason"] != "stop":
        failures.append(f"finish_reason={result['finish_reason']}")
    if len(page_texts) != len(pages):
        failures.append(f"pages_out={len(page_texts)} expected={len(pages)}")
    if preamble.strip():
        failures.append("text_before_first_page_marker")
    guard = [e for e in events if e["source"] == "page_guard"]
    kinds = {kind: sum(1 for e in guard if e["kind"] == kind)
             for kind in ("page_break_forced", "eos_forced", "eos_suppressed", "other")}
    if kinds["page_break_forced"] or any(
            p["generated_tokens"] is not None and p["generated_tokens"] >= cap for p in per_page):
        failures.append("page_token_cap_forced")
    if kinds["eos_forced"]:
        failures.append(f"eos_forced x{kinds['eos_forced']}")
    if kinds["eos_suppressed"]:
        failures.append(f"early_stop_suppressed x{kinds['eos_suppressed']}")
    if kinds["other"]:
        failures.append(f"guard_intervention x{kinds['other']}")
    if any(p["empty"] for p in per_page):
        failures.append("empty_page")
    invalid = sum(p["invalid_boxes"] for p in per_page)
    if invalid:
        failures.append(f"malformed_blocks x{invalid}")
    return {
        "parser": ANALYSIS_PARSER,
        "text_sha256": text_sha256(result["text"]),
        "ok": not failures,
        "failures": failures,
        "pages": per_page,
        "ngram_interventions": sum(1 for e in events if e["source"] == "ngram"),
        "guard_events": guard,
    }


# ---------------------------------------------------------------- model

class ModelRunner:
    """Upstream Transformers inference with observers on the logit guards."""

    def __init__(self, model_dir: Path, allow_emulated_bf16: bool) -> None:
        import torch

        if not torch.cuda.is_available():
            raise SystemExit("CUDA is required; select a GPU runtime")
        major, minor = torch.cuda.get_device_capability()
        if (major, minor) < (8, 0) and not allow_emulated_bf16:
            raise SystemExit(
                f"GPU compute capability {major}.{minor} lacks native BF16; results would be a "
                "different recipe. Use an L4/A100/H100, or pass --allow-emulated-bf16 for a smoke test."
            )
        verify_model(model_dir)
        os.environ["HF_HUB_OFFLINE"] = "1"
        from transformers import AutoModel, AutoTokenizer

        self.torch = torch
        self.tokenizer = AutoTokenizer.from_pretrained(model_dir, trust_remote_code=True, local_files_only=True)
        self.model = AutoModel.from_pretrained(
            model_dir, trust_remote_code=True, use_safetensors=True, local_files_only=True,
            torch_dtype=torch.bfloat16,
        ).eval().cuda()
        self.events: list[dict[str, Any]] = []
        self._install_observers()

    def _install_observers(self) -> None:
        # Observe, never alter: record when a guard changes the greedy choice.
        module = sys.modules[type(self.model).__module__]
        torch = self.torch
        eos = int(self.tokenizer.eos_token_id)
        guard, ngram = module._uocr_page_count_guard, module._uocr_no_repeat_ngram

        def observed_guard(logits, gen_ids, *args, **kwargs):
            before = int(torch.argmax(logits))
            out = guard(logits, gen_ids, *args, **kwargs)
            after = int(torch.argmax(out))
            if after != before:
                kind = ("eos_suppressed" if before == eos else
                        "page_break_forced" if after == PAGE_TOKEN_ID else
                        "eos_forced" if after == eos else "other")
                self.events.append({"source": "page_guard", "kind": kind, "step": len(gen_ids)})
            return out

        def observed_ngram(logits, gen_ids, *args, **kwargs):
            before = int(torch.argmax(logits))
            out = ngram(logits, gen_ids, *args, **kwargs)
            if int(torch.argmax(out)) != before:
                self.events.append({"source": "ngram", "kind": "banned_greedy", "step": len(gen_ids)})
            return out

        module._uocr_page_count_guard = observed_guard
        module._uocr_no_repeat_ngram = observed_ngram

    def __call__(self, images: list[str], mode: str) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
        from PIL import Image

        torch = self.torch
        self.events = []
        pil = [Image.open(path).convert("RGB") for path in images]
        multi = len(pil) > 1
        torch.cuda.synchronize()
        torch.cuda.reset_peak_memory_stats()
        started = time.monotonic()
        result = self.model.infer(
            self.tokenizer,
            prompt=GENERATION["prompt"],
            image_file=pil,
            crop_mode=MODES[mode]["crop_mode"],
            base_size=GENERATION["base_size"],
            image_size=GENERATION["image_size"],
            max_crops=GENERATION["max_crops"],
            max_new_tokens=GENERATION["tokens_per_page"] * len(pil),
            temperature=GENERATION["temperature"],
            page_count_guard=GENERATION["page_count_guard"],
            max_page_tokens=GENERATION["max_page_tokens"],
            no_repeat_ngram_size=GENERATION["no_repeat_ngram_size"],
            ngram_window=GENERATION["ngram_window_multi" if multi else "ngram_window_single"],
            dash_topm=GENERATION["dash_topm"],
            eval_mode=True,
            return_dict=True,
        )
        torch.cuda.synchronize()
        wall = time.monotonic() - started
        metrics = {
            "wall_seconds": wall,
            "model_seconds": result.get("time"),
            "prefill_seconds": result.get("prefill_time"),
            "prompt_tokens": len(result["prompt_token_ids"]),
            "generated_tokens": len(result["token_ids"]),
            "tokens_per_second": len(result["token_ids"]) / wall if wall else None,
            "peak_vram_allocated_bytes": torch.cuda.max_memory_allocated(),
            "peak_vram_reserved_bytes": torch.cuda.max_memory_reserved(),
        }
        return result, list(self.events), metrics


def verify_model(model_dir: Path) -> None:
    for name, expected in MODEL_FILES.items():
        path = model_dir / name
        if not path.is_file():
            raise SystemExit(f"model file missing: {path}")
        actual = sha256_file(path)
        if actual != expected:
            raise SystemExit(f"{path}: SHA-256 {actual} does not match pinned {expected}")
    extra = sorted(p.name for p in model_dir.glob("*.py") if p.name not in MODEL_FILES)
    if extra:
        raise SystemExit(f"unreviewed code files in model directory: {extra}")


def environment_record() -> dict[str, Any]:
    record: dict[str, Any] = {
        "python": sys.version,
        "platform": platform.platform(),
        "packages": package_versions(),
        "model": {"repo": MODEL_REPO, "revision": MODEL_REVISION},
    }
    try:
        import torch

        record["torch_cuda"] = torch.version.cuda
        if torch.cuda.is_available():
            props = torch.cuda.get_device_properties(0)
            record["gpu"] = {"name": props.name, "capability": list(torch.cuda.get_device_capability()),
                             "total_memory_bytes": props.total_memory}
    except ImportError:
        pass
    try:
        record["nvidia_smi"] = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,driver_version,memory.total", "--format=csv,noheader"],
            capture_output=True, text=True, check=True, timeout=30,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        record["nvidia_smi"] = None
    return record


# ---------------------------------------------------------------- execution

Generate = Callable[[list[str], str], tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]]


def run_unit(out: Path, run: dict[str, Any], repeat: int, pages: list[int], rendered: dict[int, dict[str, Any]],
             recipe: dict[str, Any], rid: str, generate: Generate, fallback_of: str | None = None) -> dict[str, Any]:
    unit_dir = out / "units" / run["id"] / f"r{repeat}" / unit_key(pages)
    result_path = unit_dir / "result.json"
    if result_path.exists():
        cached = json.loads(result_path.read_text(encoding="utf-8"))
        if cached["recipe_id"] == rid:
            return cached
        raise SystemExit(f"{result_path} was produced by recipe {cached['recipe_id']}, not {rid}")
    page_meta = [rendered[p] for p in pages]
    print(f"[{run['id']} r{repeat}] {unit_key(pages)} ({len(pages)} page(s), {run['mode']})", flush=True)
    result, events, metrics = generate([m["image"] for m in page_meta], run["mode"])
    raw = result["text"].encode("utf-8")
    write_atomic(unit_dir / "raw.txt", raw)
    write_json(unit_dir / "token_ids.json", result["token_ids"])
    analysis = analyze(result, page_meta, events)
    record = {
        "run": run["id"],
        "repeat": repeat,
        "source": run["source"],
        "mode": run["mode"],
        "pages": pages,
        "rendered": page_meta,
        "recipe_id": rid,
        "fallback_of": fallback_of,
        "raw_sha256": sha256_bytes(raw),
        "finish_reason": result["finish_reason"],
        "expected_pages": result.get("expected_pages"),
        "num_pages_out": result.get("num_pages_out"),
        "metrics": metrics,
        "analysis": analysis,
        "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    write_json(result_path, record)
    status = "ok" if analysis["ok"] else "FAILED " + ", ".join(analysis["failures"])
    print(f"    {metrics['wall_seconds']:.1f}s, {metrics['generated_tokens']} tokens, "
          f"peak {metrics['peak_vram_allocated_bytes'] / 2**30:.2f} GiB: {status}", flush=True)
    if not analysis["ok"] and run["mode"] == "multi-page" and len(pages) > 1 and run.get("halve_on_failure", True):
        # Fallback by halving; single pages stay in base mode within this recipe.
        middle = len(pages) // 2
        for half in (pages[:middle], pages[middle:]):
            run_unit(out, run, repeat, half, rendered, recipe, rid, generate, fallback_of=unit_key(pages))
    return record


def execute(plan: dict[str, Any], inputs: Path, out: Path, generate: Generate, only: set[str] | None) -> None:
    write_json(out / "plan.json", plan)
    env_path = out / "environment" / f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json"
    write_json(env_path, environment_record())
    for mode in MODES:
        recipe = recipe_for(plan, mode)
        write_json(out / "recipes" / f"{mode}.json", {"recipe": recipe, "recipe_id": recipe_id(recipe)})
    for run in plan["runs"]:
        if only and run["id"] not in only:
            continue
        source = plan["sources"][run["source"]]
        pages = run.get("pages") or [1]
        rendered = render_source(run["source"], source, inputs, out / "rendered", plan["dpi"], pages)
        recipe = recipe_for(plan, run["mode"])
        rid = recipe_id(recipe)
        for repeat in repeats_for(run):
            for unit in units_for(run, pages):
                run_unit(out, run, repeat, unit, rendered, recipe, rid, generate)
    summarize(out)


def reanalyze(out: Path) -> None:
    """Recompute derived analysis from retained raw output; raw evidence is untouched."""
    for path in sorted((out / "units").glob("*/r*/*/result.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        raw = (path.parent / "raw.txt").read_bytes()
        if sha256_bytes(raw) != record["raw_sha256"]:
            raise SystemExit(f"{path.parent / 'raw.txt'} no longer matches its recorded digest")
        token_ids = json.loads((path.parent / "token_ids.json").read_text(encoding="utf-8"))
        previous = record["analysis"]
        events = list(previous.get("guard_events", []))
        events += [{"source": "ngram", "kind": "banned_greedy", "step": None}] * previous.get("ngram_interventions", 0)
        result = {"text": raw.decode("utf-8"), "token_ids": token_ids, "finish_reason": record["finish_reason"]}
        record["analysis"] = analyze(result, record["rendered"], events)
        write_json(path, record)
    summarize(out)


def summarize(out: Path) -> dict[str, Any]:
    runs: dict[str, Any] = {}
    for path in sorted((out / "units").glob("*/r*/*/result.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        run = runs.setdefault(record["run"], {"mode": record["mode"], "source": record["source"],
                                              "units": 0, "failed": [], "pages_generated": 0,
                                              "wall_seconds": 0.0, "peak_vram_allocated_bytes": 0,
                                              "repeat_digests": {}, "repeat_text_digests": {}})
        run["units"] += 1
        run["pages_generated"] += len(record["pages"])
        run["wall_seconds"] += record["metrics"]["wall_seconds"]
        run["peak_vram_allocated_bytes"] = max(run["peak_vram_allocated_bytes"],
                                               record["metrics"]["peak_vram_allocated_bytes"])
        key = unit_key(record["pages"])
        if not record["analysis"]["ok"]:
            run["failed"].append({"repeat": record["repeat"], "unit": key,
                                  "failures": record["analysis"]["failures"]})
        run["repeat_digests"].setdefault(key, {})[f"r{record['repeat']}"] = record["raw_sha256"]
        run["repeat_text_digests"].setdefault(key, {})[f"r{record['repeat']}"] = record["analysis"].get("text_sha256")
    for run in runs.values():
        run["seconds_per_page"] = run["wall_seconds"] / run["pages_generated"] if run["pages_generated"] else None
        digests = run.pop("repeat_digests")
        multi = {k: v for k, v in digests.items() if len(v) > 1}
        run["repeat_identical"] = (all(len(set(v.values())) == 1 for v in multi.values()) if multi else None)
        run["repeat_differences"] = sorted(k for k, v in multi.items() if len(set(v.values())) > 1)
        texts = {k: v for k, v in run.pop("repeat_text_digests").items() if len(v) > 1}
        # Byte differences confined to box coordinates still leave the text identical.
        run["repeat_text_identical"] = (all(len(set(v.values())) == 1 for v in texts.values()) if texts else None)
    summary = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "runs": runs}
    write_json(out / "summary.json", summary)
    return summary


# ---------------------------------------------------------------- self-test

def self_test() -> None:
    """Exercise everything except the model, with a deterministic fake."""
    import fitz

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = root / "inputs"
        inputs.mkdir()
        doc = fitz.open()
        for index in range(3):
            page = doc.new_page(width=200, height=300)
            page.insert_text((20, 40), f"Page {index + 1}")
        doc.save(inputs / "fake.pdf")
        doc.close()
        plan = {
            "dpi": 72,
            "sources": {"fake": {"file": "fake.pdf", "sha256": sha256_file(inputs / "fake.pdf")}},
            "runs": [
                {"id": "local", "source": "fake", "mode": "page-local", "pages": [1, 2], "repeats": 2},
                {"id": "multi", "source": "fake", "mode": "multi-page", "pages": [1, 2, 3], "chunk": 3},
            ],
        }
        calls: list[int] = []

        def fake(images: list[str], mode: str):
            calls.append(len(images))
            pages_out = len(images) if len(images) != 3 else 2  # make the 3-page chunk fail
            text = "".join(f"<PAGE><BLOCK>title<LEVEL1>[1, 2, 300, 40]<FORK>Title {i}\n"
                           f"<BLOCK>text[10, 50, 500, 600]<FORK>Body continues\n<MERGE>\n"
                           f"<BLOCK>text[510, 50, 990, 600]<FORK>in the next column\n"
                           for i in range(pages_out))
            ids = []
            for _ in range(pages_out):
                ids += [PAGE_TOKEN_ID, 5, 6, 7]
            return ({"text": text, "token_ids": ids, "prompt_token_ids": [0] * 10,
                     "finish_reason": "stop", "num_pages_out": pages_out, "expected_pages": len(images)},
                    [], {"wall_seconds": 1.0, "generated_tokens": len(ids), "peak_vram_allocated_bytes": 1})

        out = root / "out"
        execute(load_plan_dict(plan), inputs, out, fake, None)
        assert calls == [1, 1, 1, 1, 3, 1, 2], calls
        execute(load_plan_dict(plan), inputs, out, fake, None)
        assert len(calls) == 7, "completed units must not rerun"
        local = json.loads((out / "units/local/r1/p0001/result.json").read_text())
        assert local["analysis"]["ok"], local["analysis"]
        page = local["analysis"]["pages"][0]
        assert page["blocks"] == 3 and page["invalid_boxes"] == 0, page
        assert page["heading_levels"] == [1] and page["merges"] == 1, page
        v1, invalid = parse_blocks("<|ref|>text<|/ref|><|det|>[[10, 20, 500, 600]]<|/det|>\nBody\n"
                                   "<|det|>title [1, 2, 3, 4]<|/det|>Title\n<|det|>text [9, 9, 1200, 5]<|/det|>bad\n",
                                   100, 100)
        assert len(v1) == 3 and invalid == 1, v1
        broken = analyze({"text": "<PAGE><BLOCK>other[508,polygons\n<BLOCK>text[1, 1, 9, 9]<FORK>ok",
                          "token_ids": [PAGE_TOKEN_ID, 1], "finish_reason": "stop"},
                         [{"page": 1, "width": 10, "height": 10}], [])
        assert broken["failures"] == ["malformed_blocks x1"], broken
        assert text_sha256("<BLOCK>text[1, 2, 3, 4]<FORK>a") == text_sha256("<BLOCK>text[1, 2, 3, 5]<FORK>a")
        reanalyze(out)
        assert json.loads((out / "summary.json").read_text())["runs"]["local"]["repeat_text_identical"] is True
        failed = json.loads((out / "units/multi/r1/p0001-p0003/result.json").read_text())
        assert "pages_out=2 expected=3" in failed["analysis"]["failures"], failed["analysis"]
        assert (out / "units/multi/r1/p0002-p0003/result.json").exists()
        summary = json.loads((out / "summary.json").read_text())
        assert summary["runs"]["local"]["repeat_identical"] is True, summary
        assert len(summary["runs"]["multi"]["failed"]) == 1, summary
        events = [{"source": "page_guard", "kind": "eos_suppressed", "step": 3}]
        analysis = analyze({"text": "<PAGE>a", "token_ids": [PAGE_TOKEN_ID, 1], "finish_reason": "length"},
                           [{"page": 1, "width": 10, "height": 10}], events)
        assert analysis["failures"] == ["finish_reason=length", "early_stop_suppressed x1"], analysis
    print("self-test passed")


def load_plan_dict(plan: dict[str, Any]) -> dict[str, Any]:
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as stream:
        json.dump(plan, stream)
    try:
        return load_plan(Path(stream.name))
    finally:
        os.unlink(stream.name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--inputs", type=Path, required=True, help="directory holding the plan's source files")
    run.add_argument("--model-dir", type=Path, required=True)
    run.add_argument("--out", type=Path, required=True)
    run.add_argument("--only", action="append", help="run id to execute (repeatable)")
    run.add_argument("--allow-emulated-bf16", action="store_true")
    summary = sub.add_parser("summarize")
    summary.add_argument("--out", type=Path, required=True)
    again = sub.add_parser("reanalyze")
    again.add_argument("--out", type=Path, required=True)
    sub.add_parser("self-test")
    args = parser.parse_args()
    if args.command == "self-test":
        self_test()
    elif args.command == "reanalyze":
        reanalyze(args.out)
        print(json.dumps(json.loads((args.out / "summary.json").read_text()), indent=2))
    elif args.command == "summarize":
        print(json.dumps(summarize(args.out), indent=2))
    else:
        plan = load_plan(args.plan)
        runner = ModelRunner(args.model_dir, args.allow_emulated_bf16)
        execute(plan, args.inputs, args.out, runner, set(args.only) if args.only else None)
        print(json.dumps(json.loads((args.out / "summary.json").read_text()), indent=2))


if __name__ == "__main__":
    main()
