"""Reusable source-evidence enrichment for converter Markdown."""

from .align import AlignmentResult, align_markdown_to_pdf, validate_alignment_publication
from .contract import MarkdownBlock, PdfBlock, PdfEvidence, PdfLine, PdfPage, PdfWord
from .markdown import segment_markdown
from .pdf import extract_pdf_evidence, poppler_version, sanitize_poppler_xhtml

__all__ = [
    "AlignmentResult",
    "MarkdownBlock",
    "PdfBlock",
    "PdfEvidence",
    "PdfLine",
    "PdfPage",
    "PdfWord",
    "align_markdown_to_pdf",
    "extract_pdf_evidence",
    "poppler_version",
    "sanitize_poppler_xhtml",
    "segment_markdown",
    "validate_alignment_publication",
]
