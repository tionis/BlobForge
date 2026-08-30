from blobforge.normalization import (
    recover_typed_text_list_runs,
    strip_markdown_list_decorations,
)


def test_existing_markdown_lists_lose_only_decorative_prefix():
    value, count = strip_markdown_list_decorations(
        "- ◆ **First:** text\n- ordinary\n1. ♦ numbered\nAt ♦, keep the mechanic."
    )
    assert value == (
        "- **First:** text\n- ordinary\n1. numbered\nAt ♦, keep the mechanic."
    )
    assert count == 2


def test_typed_run_recovers_list_but_preserves_inline_and_single_glyph():
    blocks = [
        {"type": "text", "content": "At ♦, keep this rule."},
        {"type": "text", "content": "◆ **First:** text"},
        {"type": "text", "content": "◆ **Second:** text"},
        {"type": "title", "content": "## Break"},
        {"type": "text", "content": "♦ A single ambiguous symbol"},
    ]
    replacements, count = recover_typed_text_list_runs(blocks)
    assert replacements == {
        1: "- **First:** text",
        2: "- **Second:** text",
    }
    assert count == 2


def test_typed_spend_options_are_recovered_only_as_a_run():
    blocks = [
        {"type": "text", "content": "♦ Spend 1 Momentum to warn."},
        {"type": "text", "content": "♦ Spend 2 Momentum to swap."},
    ]
    replacements, count = recover_typed_text_list_runs(blocks)
    assert replacements == {
        0: "- Spend 1 Momentum to warn.",
        1: "- Spend 2 Momentum to swap.",
    }
    assert count == 2
