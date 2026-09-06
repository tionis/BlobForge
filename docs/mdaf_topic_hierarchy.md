# Detailed topic hierarchy and wiki readability

Recipe 1.6.0 (`mistral-ocr-4.1-wiki-v7.json`, profile wiki-v6) adds styled
contents recovery without changing the expensive extraction identity. Older
recipes remain frozen. Compatible retained OCR can be reprocessed offline.

Chapter boundaries alone do not establish a usable wiki. The previous recipe
retained OCR heading levels inside chapters, so siblings could be mistaken for
parents and splitting by size selected arbitrary fragments. Count reduction,
byte coverage and valid links are necessary but insufficient acceptance tests.

The new pass reads paired contents columns vertically, preserves styles and
separate page-number lines, and scopes matching to each recovered chapter.
Repeated bold topic rows plus subordinate rows establish a two/three-tier
pattern: bold topics, plain children, and italic children of the latest plain
entry (or the topic). Matching requires a unique body title, with corroborated
page alignment disambiguating repeated titles. Consecutive same-title headings
on one page may form one topic (for example a table caption followed by prose).
Missing bold anchors or less than 85% alignment rejects the chapter pass.
Unlisted headings inherit local context, explicitly marked for review. Books
without suitable evidence retain their previous hierarchy with diagnostics.

The nine-book retained corpus is an offline acceptance set, not embedded rules.
Storypath Core System aligns all 123 contents entries. Tests assert Skills owns
both Artistry and Culture, dice steps share their proper topic, and status
effects remain together. Other books exercise both recovery and fallback.
Source titles, publishers and game vocabulary are not detection conditions.

Simple emphasis in sanitized HTML table cells is rendered as strong/em tags;
complex inline syntax remains literal. This changes primary Markdown table
markup, so all outline/source offsets are regenerated. Native OCR and asset
bytes remain unchanged. Do not compare new outline offsets against old text.

Vulcan must explicitly import the outline and render its ATX levels relative to
each note without replacing authored titles or anchors. For a topic-oriented
wiki use levels 2 through 3, then review topic ownership, note sizes, rendered
headings and navigation. Keep generated review destinations separate from
edited canonical wikis. No paid OCR is required by this release.
