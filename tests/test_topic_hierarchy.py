"""Synthetic, publication-independent topic evidence regressions."""
from blobforge.normalization.topics import _contents_rows, recover_topics
from blobforge.markdown_outline import markdown_outline


def fixture(missing=False):
    text = '# Chapter 1\n\n# Tools\n\n## Hammer\n\n# Saw\n\n# Process\n\n# Step A\n\n# Step B\n\n# Results\n\n# Scores\n'
    outline = markdown_outline(text)
    nodes = outline['nodes']
    nodes[0]['id'] = 'major-0'
    nodes[0]['section']['end'] = len(text.encode())
    toc = ('# Chapter 1\n| **Tools** | 1 | **Process** | 1 |\n'
           '| *Hammer* | 1 | Step A | 1 |\n| *Saw* | 1 | Step B | 1 |\n'
           '| | | **Results** | 1 |\n| | | Scores | 1 |')
    if missing:
        toc = toc.replace('**Process**', '**Absent**')
    pages = [{'index': 0, 'markdown': toc}]
    mappings = {'mappings': [{'document': {'start': 0, 'end': len(text.encode())},
                            'source': {'selectors': [{'start': 1}]}}]}
    report = {'major_sections': [{'title': 'Chapter 1'}], 'toc_pages': [0],
              'alignment_offset': 0, 'diagnostics': []}
    return outline, pages, mappings, report


def test_styled_topics_override_ocr_levels_and_read_columns_downward():
    args = fixture()
    result = recover_topics(*args)
    nodes = {n['title']: n for n in result['nodes']}
    for parent, child in [('Tools', 'Hammer'), ('Tools', 'Saw'), ('Process', 'Step A'), ('Process', 'Step B')]:
        assert nodes[child]['parent'] == nodes[parent]['id']
        assert nodes[child]['level'] == 4
    assert args[3]['topic_hierarchy']['chapters'][0]['matched_entries'] == 8


def test_missing_topic_rejects_chapter_instead_of_absorbing_neighbours():
    args = fixture(missing=True)
    before = [(n['id'], n['level']) for n in args[0]['nodes']]
    result = recover_topics(*args)
    assert [(n['id'], n['level']) for n in result['nodes']] == before
    assert any('alignment_incomplete' in d for d in args[3]['diagnostics'])


def test_number_in_heading_is_not_confused_with_separate_page_label():
    rows = _contents_rows([{'index': 0, 'markdown': '### **In the Year 3000**\n\n**17**\n\nTABLE OF CONTENTS\n\n3'}], [0])
    assert rows == [('**In the Year 3000**', 17)]


def test_table_emphasis_does_not_interpret_html_code_or_escaped_stars():
    from blobforge.normalization.table_emphasis import render_table_emphasis
    assert render_table_emphasis('<td>**Bold** and *soft* &amp; safe</td>') == '<td><strong>Bold</strong> and <em>soft</em> &amp; safe</td>'
    for value in ['<td>`**literal**`</td>', '<td>\\*literal\\*</td>', '<td><code>**literal**</code></td>', '<p>**outside**</p>', '```html\n<td>**literal**</td>\n```\n']:
        assert render_table_emphasis(value) == value


def test_recipe_replays_old_extraction_with_a_distinct_immutable_profile(tmp_path):
    from blobforge.recipe_runtime import mistral_wiki_v6_recipe, mistral_wiki_v7_recipe
    from blobforge.recipe_lifecycle import assert_reprocessable
    args = dict(max_pages=10, max_cost_usd=1, response_cache=tmp_path, api_rights_confirmed=True)
    old, new = mistral_wiki_v6_recipe(**args), mistral_wiki_v7_recipe(**args)
    assert old.parameters['normalization_profile'] == 'wiki-v5'
    assert new.parameters['normalization_profile'] == 'wiki-v6'
    assert old.recipe_digest != new.recipe_digest
    assert old.recipe['lifecycle']['extraction'] == new.recipe['lifecycle']['extraction']
    assert_reprocessable(old.recipe, new.recipe)
