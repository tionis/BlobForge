import struct

import pytest

from blobforge.normalization import (
    normalize_datalab_pages,
    normalize_mistral_pages,
    raster_dimensions,
    referenced_asset_names,
)


def test_mistral_normalization_uses_typed_blocks_and_geometry():
    pages, stats = normalize_mistral_pages(
        [
            {
                "dimensions": {"width": 788, "height": 1023},
                "blocks": [
                    {"type": "header", "content": "RULEBOOK"},
                    {"type": "text", "content": "# Useful heading"},
                    {
                        "type": "table",
                        "content": "| Name | | Value |\n| --- | --- | --- |\n| Spell | | 3 |",
                    },
                    {
                        "type": "image",
                        "content": "![art](figure.jpeg)",
                        "top_left_y": 300,
                        "bottom_right_y": 800,
                    },
                    {"type": "footer", "content": "2  RULEBOOK"},
                    {
                        "type": "image",
                        "content": "![publisher](logo.jpeg)",
                        "top_left_y": 950,
                        "bottom_right_y": 1010,
                    },
                ],
            }
        ]
    )

    assert "RULEBOOK" not in pages[0]
    assert "<table>" in pages[0]
    assert 'colspan="2"' in pages[0]
    assert "figure.jpeg" in pages[0]
    assert "logo.jpeg" not in pages[0]
    assert stats == {
        "headers_removed": 1,
        "footers_removed": 1,
        "footer_images_removed": 1,
        "tables_converted": 1,
    }


def test_mistral_normalization_fails_closed_without_native_evidence():
    with pytest.raises(ValueError, match="requires dimensions and blocks"):
        normalize_mistral_pages([{"markdown": "unstructured"}])


def test_datalab_normalization_isolates_exact_captions_and_recurring_furniture():
    pages = []
    dimensions = {}
    for page in range(4):
        logo = f"logo-{page}.jpeg"
        dimensions[logo] = (140, 154)
        body = (
            "![A dramatic hero](assets/hero.jpeg)\n\n"
            "A dramatic hero\n\n"
            "| Character | | Score |\n"
            "| --- | --- | --- |\n"
            "| Ada | | 5 |\n\n"
            f"![Small publisher dragon logo](assets/{logo})"
        )
        pages.append(body)
    dimensions["hero.jpeg"] = (734, 1086)

    normalized, stats = normalize_datalab_pages(pages, dimensions)

    assert all(page.count("A dramatic hero") == 1 for page in normalized)
    assert all("publisher dragon" not in page for page in normalized)
    assert all("<table>" in page and 'colspan="2"' in page for page in normalized)
    assert stats == {
        "descriptions_isolated": 4,
        "footer_images_removed": 4,
        "tables_converted": 4,
    }


def test_datalab_keeps_nonrecurring_small_image_and_nonexact_caption():
    markdown = "![Map](assets/map.png)\n\nMap of the city"
    normalized, stats = normalize_datalab_pages([markdown], {"map.png": (200, 200)})
    assert normalized == [markdown]
    assert stats["descriptions_isolated"] == 0
    assert stats["footer_images_removed"] == 0


def test_asset_reference_and_png_dimensions():
    png = b"\x89PNG\r\n\x1a\n" + b"\0" * 8 + struct.pack(">II", 17, 23)
    assert raster_dimensions(png) == (17, 23)
    assert raster_dimensions(b"not an image") is None
    assert referenced_asset_names("![one](assets/a.png) ![two](b.jpeg)") == {
        "a.png",
        "b.jpeg",
    }
