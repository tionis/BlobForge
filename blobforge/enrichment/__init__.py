"""Reusable source-evidence enrichment for converter Markdown."""

from .align import AlignmentResult, align_markdown_to_pdf
from .contract import MarkdownBlock, PdfBlock, PdfEvidence, PdfPage
from .markdown import segment_markdown
from .pdf import extract_pdf_evidence, poppler_version

__all__ = [
    "AlignmentResult",
    "MarkdownBlock",
    "PdfBlock",
    "PdfEvidence",
    "PdfPage",
    "align_markdown_to_pdf",
    "extract_pdf_evidence",
    "poppler_version",
    "segment_markdown",
]
