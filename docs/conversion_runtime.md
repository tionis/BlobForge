# Conversion runtime compatibility

BlobForge's production conversion contract is Marker 1.x. The `convert` and
`all` extras require `marker-pdf>=1.10.2,<2`, and `uv.lock` currently resolves
Marker 1.10.2 with Surya 0.17.1. Repository workers should be installed with:

```bash
uv sync --extra convert
```

Native tool installations should include the `convert` extra. The upper bound
is intentional: `uv pip install -e ".[convert]"` and `uv tool install` resolve
from package metadata and are not guaranteed to obey the repository lock.

## Why Marker 2 is a compatibility boundary

Marker 1.x uses separate task-specific models for OCR, layout, reading order,
tables, and mathematical expressions. Marker 2 delegates scanned or garbled
pages to Surya 0.22's generative vision-language model, which emits full-page
HTML or layout JSON through a local or remote inference server. Digital PDFs
with usable embedded text may remain similar, but scanned and complex pages can
change materially in recognized text, block boundaries, reading order, table
HTML, equation markup, and whitespace.

Do not remove the `<2` bound as a routine dependency upgrade. Adoption requires
an A/B conversion of a representative corpus, review of Markdown and extracted
assets, a decision about mixed-version output reproducibility, and explicit
inference-server provisioning in every worker installation path.

## Startup validation

Workers validate the conversion host before requesting coordinator identity,
registering, starting heartbeats, or claiming a lease. The validation checks
Marker's required Python imports. It also recognizes the external inference
contract used by newer Surya installations that may remain in a drifted native
environment:

- `SURYA_INFERENCE_URL` uses an already-running compatible server.
- `llamacpp` requires `llama-server` on `PATH`, or an executable path in
  `LLAMA_CPP_BINARY`.
- `vllm` requires Docker; the host must separately provide a working NVIDIA GPU
  and NVIDIA Container Toolkit.

A missing prerequisite raises a host-configuration error and exits before any
job is claimed. BlobForge does not install native executables during worker
startup: choosing and verifying a platform-specific llama.cpp build or a GPU
container runtime is an installation/deployment responsibility, and silently
mutating a production host would make workers less reproducible.
