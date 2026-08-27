# Poppler deterministic baseline

This zero-model adapter uses the installed `pdftotext -layout` tool. Form-feed
page boundaries become exact source mappings. It is fast and reproducible, but
does not infer headings, tables, reading order, or images; it is a lower-bound
control rather than a candidate wiki converter.
