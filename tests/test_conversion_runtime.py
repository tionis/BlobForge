"""Regression tests for worker conversion-host startup validation."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from blobforge.conversion_runtime import (
    WorkerConfigurationError,
    ensure_conversion_runtime,
)

REQUIRED_SYMBOLS = {
    "marker.converters.pdf": "PdfConverter",
    "marker.models": "create_model_dict",
    "marker.output": "text_from_rendered",
}


def _runtime_importer(*, backend="llamacpp", url=None, binary=None, legacy=False):
    def import_module(name):
        if name in REQUIRED_SYMBOLS:
            return SimpleNamespace(**{REQUIRED_SYMBOLS[name]: object()})
        if name == "surya.inference":
            if legacy:
                raise ModuleNotFoundError(
                    "No module named 'surya.inference'", name="surya.inference"
                )
            return SimpleNamespace(_autodetect_backend=lambda: backend)
        if name == "surya.settings":
            return SimpleNamespace(
                settings=SimpleNamespace(
                    SURYA_INFERENCE_URL=url,
                    LLAMA_CPP_BINARY=binary,
                )
            )
        raise AssertionError(f"unexpected import: {name}")

    return import_module


def test_marker_1_runtime_does_not_require_external_inference_server():
    with patch(
        "blobforge.conversion_runtime.import_module",
        side_effect=_runtime_importer(legacy=True),
    ), patch("blobforge.conversion_runtime.shutil.which") as which:
        ensure_conversion_runtime()

    which.assert_not_called()


def test_llamacpp_backend_requires_binary_before_claiming_work():
    with patch(
        "blobforge.conversion_runtime.import_module",
        side_effect=_runtime_importer(binary="/opt/llama-server"),
    ), patch(
        "blobforge.conversion_runtime.shutil.which", return_value=None
    ), pytest.raises(WorkerConfigurationError) as exc_info:
        ensure_conversion_runtime()

    message = str(exc_info.value)
    assert "/opt/llama-server" in message
    assert "LLAMA_CPP_BINARY" in message
    assert "SURYA_INFERENCE_URL" in message
    assert "No jobs were claimed" in message


def test_llamacpp_backend_accepts_resolved_binary():
    with patch(
        "blobforge.conversion_runtime.import_module",
        side_effect=_runtime_importer(),
    ), patch(
        "blobforge.conversion_runtime.shutil.which",
        return_value="/usr/local/bin/llama-server",
    ):
        ensure_conversion_runtime()


def test_external_inference_url_does_not_require_local_backend_tools():
    with patch(
        "blobforge.conversion_runtime.import_module",
        side_effect=_runtime_importer(url="http://inference.internal:8000"),
    ), patch("blobforge.conversion_runtime.shutil.which") as which:
        ensure_conversion_runtime()

    which.assert_not_called()


def test_vllm_backend_requires_docker_before_claiming_work():
    with patch(
        "blobforge.conversion_runtime.import_module",
        side_effect=_runtime_importer(backend="vllm"),
    ), patch(
        "blobforge.conversion_runtime.shutil.which", return_value=None
    ), pytest.raises(WorkerConfigurationError) as exc_info:
        ensure_conversion_runtime()

    message = str(exc_info.value)
    assert "docker" in message
    assert "NVIDIA Container Toolkit" in message
    assert "No jobs were claimed" in message


def test_broken_surya_import_is_reported_as_configuration_error():
    importer = _runtime_importer()

    def broken_import(name):
        if name == "surya.inference":
            raise ModuleNotFoundError("No module named 'surya.helper'", name="surya.helper")
        return importer(name)

    with patch(
        "blobforge.conversion_runtime.import_module", side_effect=broken_import
    ), pytest.raises(WorkerConfigurationError) as exc_info:
        ensure_conversion_runtime()

    assert "surya.helper" in str(exc_info.value)
