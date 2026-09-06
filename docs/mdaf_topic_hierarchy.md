# Detailed topic hierarchy and wiki readability

## Compatible bounded recovery release

Recipe 1.8.0 (`mistral-ocr-4.1-wiki-v9.json`, profile wiki-v8) adds
`bounded-contents-v2`; routing revision 7 and the default worker select it.
All previous recipes and profiles remain frozen. The extraction identity and
primary/native/asset bytes remain unchanged from 1.7.0. Automatic replay uses
retained evidence only, never a new provider request or transferred allowance.

A missing top-level contents anchor no longer rejects every confirmed topic in
its chapter. Its aligned source page must be independently locatable. Retain
the original hierarchy from the preceding confirmed topic (extended to an
original chapter child when needed) through the uncertainty page, ending at
the next confirmed topic after that page. Unlocatable gaps, less than 85%
alignment or fewer than three confirmed topics outside these guarded regions
reject recovery. The report records `unverified_regions` as primary UTF-8 byte
ranges with source pages and emits `contents_unverified_regions_retained`.
These are uncertainty fences, not repaired text or inferred citation labels.

Wrapped contents headings ending with a colon may join the following numbered
title in a temporary parsing view. Unrecognized section/fiction boundaries
become uncertainty fences rather than ordinary topics of the preceding section.
Accent and `and`/`&` differences may match only a unique body heading on the
aligned page. Ambiguous aliases are never resolved by choosing the first one.
An unstyled tail after italic children requires corroborating indentation;
otherwise recovery is rejected with `contents_style_transition_unresolved`.

Synthetic workshop manuals exercise missing and ambiguous evidence, preserved
regions, Unicode aliases, wrapped labels, formatting loss and coverage-wide
fallback. Private corpus checks must assert actual parent ownership and preserve
the approved Storypath outline. Scion Powers' lost contents styles and books
with chapter-only contents still require stronger evidence; no general font-only
fallback is enabled. Vulcan's source-size/coverage summary assists review but is
not a semantic readiness score. MDAF v1 remains unchanged; its consumer guidance
clarifies this distinction.

## Compatible multi-evidence release

Recipe 1.7.0 (`mistral-ocr-4.1-wiki-v8.json`, profile wiki-v7) extends the
frozen 1.6.0 behavior. Extraction identity, primary Markdown, native response
and retained assets are unchanged relative to 1.6.0. The worker default and
routing revision 6 select the new immutable recipe; automatic compatible
reprocessing uses retained evidence and does not authorize provider requests.

New evidence is chapter-local: repeated plain/italic, capitalization contrast,
explicit contents heading levels, and repeated native contents indentation.
Indentation requires aligned number columns, repeated separated tiers and
page-relative measurements; arbitrary body typography is not sufficient.
Mixed styles can retain an unstyled procedure prefix when consecutive steps
establish ownership. Page ordering and duplicate-row reconciliation handle
interleaved columns. Parenthetical body qualifiers may match a contents title
only with a unique heading on the corroborated page. Missing top-level anchors
still reject a chapter; subordinate alignment must reach 85 percent.

Chapter-title aliases require independent agreement between an aligned printed
page, a spacing/hyphenation or one-edit title difference, and an established
chapter typography population. This repair applies only to geometry-led chapter
recovery, not numbered or explicit contents hierarchies. Exact-title candidates
are not reconsidered using size alone. Recovered aliases are diagnosed for
review; they never create citation page labels or alter authored text.

The expanded private corpus is an acceptance set, not a source of title-based
conditions. Synthetic workshop manuals test the same rules, missing/ambiguous
evidence, DPI invariance, procedure ownership and frozen-profile compatibility.
Every release review must include actual imports, complete nonoverlapping byte
ownership, unchanged native/assets, semantic parent assertions and a list of
remaining large sections. A chapter-only contents page still does not establish
topic ownership: broad body-font clustering was rejected after false promotions
across multiple books. Such books retain their prior outline and review warning.

No MDAF schema or Vulcan command change is needed. The existing aligned-outline
contract, import skill, source-span checks and fresh-destination guardrails apply.
Do not replace edited wikis with automatically reprocessed artifact output.

## Previous styled-contents release

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
