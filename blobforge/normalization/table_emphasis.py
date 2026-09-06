"""Render simple authored emphasis inside already-sanitized HTML table cells."""
import re


def render_table_emphasis(markdown):
    def cell(match):
        value = match[2]
        # Complex inline syntax remains literal; never interpret HTML attributes,
        # escapes or code as Markdown. Existing sanitized tags remain untouched.
        if any(char in value for char in "<`\\"):
            return match[0]
        value = re.sub(r"(?<!\*)\*\*([^*\n]+)\*\*(?!\*)", r"<strong>\1</strong>", value)
        value = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"<em>\1</em>", value)
        return match[1] + value + match[3]
    output = []
    fence = None
    for line in markdown.splitlines(keepends=True):
        marker = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)", line)
        if marker:
            if fence is None:
                fence = (marker[1][0], len(marker[1]))
            elif marker[1][0] == fence[0] and len(marker[1]) >= fence[1] and not marker[2].strip():
                fence = None
            output.append(line)
        elif fence is not None:
            output.append(line)
        else:
            output.append(re.sub(r"(<t[dh](?:\s[^>]*)?>)([^<]*)(</t[dh]>)", cell, line))
    return ''.join(output)
