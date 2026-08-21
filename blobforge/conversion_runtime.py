"""Validation and error classification for the optional conversion runtime."""

import shutil
from importlib import import_module

CONVERSION_CONFIGURATION_EXIT_CODE = 78


class WorkerConfigurationError(RuntimeError):
    """Raised when this host cannot provide the configured conversion runtime."""


def _import_optional_module(module_name: str):
    """Import an optional module, distinguishing absence from a broken import."""
    try:
        return import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name == module_name:
            return None
        raise


def _validate_surya_inference_backend() -> None:
    """Validate external services introduced by Surya's VLM architecture."""
    inference = _import_optional_module("surya.inference")
    if inference is None:
        # Surya versions used by Marker 1.x load task-specific models directly.
        return

    autodetect_backend = getattr(inference, "_autodetect_backend", None)
    if not callable(autodetect_backend):
        return

    settings_module = import_module("surya.settings")
    settings = settings_module.settings
    if getattr(settings, "SURYA_INFERENCE_URL", None):
        return

    backend = str(autodetect_backend()).lower()
    if backend == "llamacpp":
        binary = getattr(settings, "LLAMA_CPP_BINARY", None) or "llama-server"
        if shutil.which(str(binary)) is None:
            raise WorkerConfigurationError(
                "Surya selected its llama.cpp inference backend, but the "
                f"`{binary}` executable was not found. Install llama.cpp and "
                "set `LLAMA_CPP_BINARY`, or configure `SURYA_INFERENCE_URL` to "
                "an existing inference server. BlobForge's supported Marker "
                "1.x stack can be restored with `uv sync --extra convert`. "
                "No jobs were claimed."
            )
        return

    if backend == "vllm":
        if shutil.which("docker") is None:
            raise WorkerConfigurationError(
                "Surya selected its vLLM inference backend, but the `docker` "
                "executable was not found. Install Docker with the NVIDIA "
                "Container Toolkit, or configure `SURYA_INFERENCE_URL` to an "
                "existing inference server. BlobForge's supported Marker 1.x "
                "stack can be restored with `uv sync --extra convert`. No jobs "
                "were claimed."
            )
        return

    raise WorkerConfigurationError(
        f"Surya selected unsupported inference backend `{backend}`. Configure "
        "`SURYA_INFERENCE_BACKEND` as `llamacpp` or `vllm`, or set "
        "`SURYA_INFERENCE_URL`. No jobs were claimed."
    )


def ensure_conversion_runtime() -> None:
    """Validate Marker and its external backend without loading model weights."""
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

    try:
        _validate_surya_inference_backend()
    except WorkerConfigurationError:
        raise
    except Exception as exc:
        raise WorkerConfigurationError(
            "Surya inference backend validation failed before coordinator "
            "contact. No jobs were claimed. "
            f"Underlying error: {type(exc).__name__}: {exc}"
        ) from exc
