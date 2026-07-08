"""
Isolated marker conversion entrypoint.

The worker uses this module when conversion must be abortable or isolated from
native crashes in marker/PyTorch. It writes the normal BlobForge output files
directly to the provided output directory.
"""
import argparse
import json
import os
import sys
import traceback


def _extract_marker_meta(rendered) -> dict:
    """Convert marker metadata to a JSON-serializable dictionary."""
    meta = {}
    if hasattr(rendered, "metadata") and rendered.metadata:
        try:
            if hasattr(rendered.metadata, "model_dump"):
                meta = rendered.metadata.model_dump()
            elif hasattr(rendered.metadata, "dict"):
                meta = rendered.metadata.dict()
            elif isinstance(rendered.metadata, dict):
                meta = rendered.metadata
        except Exception as exc:
            meta = {"error": str(exc)}
    return meta


def run_conversion(pdf_path: str, out_dir: str) -> dict:
    """Convert a PDF with marker and write content/assets/metadata to out_dir."""
    from marker.converters.pdf import PdfConverter
    from marker.models import create_model_dict
    from marker.output import text_from_rendered

    os.makedirs(out_dir, exist_ok=True)
    artifact_dict = create_model_dict()
    converter = PdfConverter(
        artifact_dict=artifact_dict,
        config={},
    )

    rendered = converter(pdf_path)
    text, _ext, images = text_from_rendered(rendered)

    for img_name in images.keys():
        text = text.replace(f"({img_name})", f"(assets/{img_name})")

    md_path = os.path.join(out_dir, "content.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(text)

    if images:
        assets_dir = os.path.join(out_dir, "assets")
        os.makedirs(assets_dir, exist_ok=True)
        for img_name, img in images.items():
            img_path = os.path.join(assets_dir, img_name)
            if hasattr(img, "mode") and img.mode != "RGB":
                img = img.convert("RGB")
            img.save(img_path)

    marker_meta = _extract_marker_meta(rendered)
    meta_path = os.path.join(out_dir, "marker_meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(marker_meta, f, default=str)

    result = {
        "output_chars": len(text),
        "image_count": len(images),
    }
    result_path = os.path.join(out_dir, "conversion_result.json")
    with open(result_path, "w", encoding="utf-8") as f:
        json.dump(result, f)

    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="BlobForge isolated marker conversion child")
    parser.add_argument("pdf_path")
    parser.add_argument("out_dir")
    args = parser.parse_args(argv)

    try:
        result = run_conversion(args.pdf_path, args.out_dir)
    except Exception:
        traceback.print_exc()
        return 1

    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
