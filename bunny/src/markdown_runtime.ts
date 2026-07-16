import DOMPurify from "dompurify";
import { marked } from "marked";

export interface RenderedMarkdown {
  html: string;
  headings: Array<{ id: string; level: number; text: string }>;
}

function slug(value: string): string {
  return value.normalize("NFKD").toLowerCase().replace(/[^\p{L}\p{N}]+/gu, "-").replace(/^-|-$/g, "") || "section";
}

function safeDecode(value: string): string {
  try { return decodeURIComponent(value); } catch { return value; }
}

function render(markdown: string, resolveAsset: (path: string) => string | null): RenderedMarkdown {
  const source = markdown.replace(/^[\u200B-\u200F\uFEFF]/, "");
  const parsed = marked.parse(source, { gfm: true, breaks: false, async: false }) as string;
  const clean = DOMPurify.sanitize(parsed, {
    USE_PROFILES: { html: true },
    FORBID_TAGS: ["style", "form", "input", "button"],
    FORBID_ATTR: ["style"],
  });
  const container = document.createElement("div");
  container.innerHTML = clean;

  for (const image of container.querySelectorAll("img")) {
    const original = safeDecode(image.getAttribute("src") || "").replace(/^\.\//, "");
    const resolved = resolveAsset(original);
    if (resolved) {
      image.src = resolved;
      image.loading = "lazy";
    } else {
      const fallback = document.createElement("span");
      fallback.textContent = image.alt ? `[Image: ${image.alt}]` : "[Missing image]";
      image.replaceWith(fallback);
    }
  }

  for (const link of container.querySelectorAll("a")) {
    const href = link.getAttribute("href") || "";
    if (/^https?:\/\//i.test(href)) {
      link.target = "_blank";
      link.rel = "noopener noreferrer";
    } else if (!href.startsWith("#")) {
      link.removeAttribute("href");
    }
  }

  const used = new Map<string, number>();
  const headings = [...container.querySelectorAll("h1,h2,h3,h4,h5,h6")].map((heading) => {
    const text = heading.textContent?.trim() || "Section";
    const base = slug(text);
    const sequence = used.get(base) || 0;
    used.set(base, sequence + 1);
    const id = sequence ? `${base}-${sequence + 1}` : base;
    heading.id = id;
    return { id, level: Number(heading.tagName.slice(1)), text };
  });
  return { html: container.innerHTML, headings };
}

declare global {
  interface Window {
    BlobForgeMarkdown: { render: typeof render };
  }
}

window.BlobForgeMarkdown = { render };
