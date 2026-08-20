"""Validation and error classification for the optional conversion runtime."""

from importlib import import_module


CONVERSION_CONFIGURATION_EXIT_CODE = 78


class WorkerConfigurationError(RuntimeError):
    """Raised when this host cannot provide the configured conversion runtime."""


def ensure_conversion_runtime() -> None:
    """Validate Marker imports without loading model weights or claiming work."""
    required_symbols = (
        ("marker.converters.pdf", "PdfConverter"),
        ("marker.models", "create_model_dict"),
        ("marker.output", "text_from_rendered"),
    )
    try:
        for module_name, symbol in required_symbols:
            module = import_module(module_name)
            getattr(module, symbol)
    except Exception as exc:
        raise WorkerConfigurationError(
            "Marker conversion runtime is unavailable. In a repository checkout, "
            "install it with `uv sync --extra convert`; for a native tool install, "
            "include BlobForge's `convert` extra. No jobs were claimed. "
            f"Underlying error: {type(exc).__name__}: {exc}"
        ) from exc
