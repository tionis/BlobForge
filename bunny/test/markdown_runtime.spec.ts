// @vitest-environment jsdom
import { describe, expect, it } from "vitest";
import "../src/markdown_runtime";

describe("browser Markdown runtime", () => {
  it("renders GFM, sanitizes active content, resolves assets, and creates stable heading ids", () => {
    const rendered = window.BlobForgeMarkdown.render(`
# Rules & Safety
# Rules & Safety

| Die | Result |
| --- | --- |
| d20 | Success |

![Map](assets/map.png)

<img src=x onerror="alert(1)">
<script>alert(2)</script>
<p style="position:fixed">Styled</p>
[bad](javascript:alert(3))
`, (path) => path === "assets/map.png" ? "blob:map" : null);

    const document = new DOMParser().parseFromString(rendered.html, "text/html");
    expect(document.querySelector("table")?.textContent).toContain("d20");
    expect(document.querySelector('img[src="blob:map"]')?.getAttribute("alt")).toBe("Map");
    expect(document.querySelector("script")).toBeNull();
    expect(document.querySelector("[onerror]")).toBeNull();
    expect(document.querySelector("[style]")).toBeNull();
    expect(document.querySelector('a[href^="javascript:"]')).toBeNull();
    expect(rendered.headings).toEqual([
      { id: "rules-safety", level: 1, text: "Rules & Safety" },
      { id: "rules-safety-2", level: 1, text: "Rules & Safety" },
    ]);
  });
});
