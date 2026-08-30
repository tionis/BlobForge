"""Versioned converter-adapter boundary for isolated extraction engines."""

from .contract import CONTRACT, ConversionBundle, ConversionRequest, load_bundle
from .runner import (
    AdapterCancelled,
    ConverterExecutionError,
    ConverterRunResult,
    ProviderProbe,
    probe_provider,
    run_converter,
)

__all__ = [
    "CONTRACT",
    "AdapterCancelled",
    "ConverterExecutionError",
    "ConversionBundle",
    "ConversionRequest",
    "ConverterRunResult",
    "ProviderProbe",
    "load_bundle",
    "probe_provider",
    "run_converter",
]
