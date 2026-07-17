# Dependency security policy

BlobForge uses a universal `uv.lock`, so every version retained for any supported
Python or platform must be outside applicable advisory ranges. Security floors
are centralized in `[tool.uv].constraint-dependencies` rather than duplicating
transitive packages as public runtime dependencies. The regression test in
`tests/test_dependency_security.py` rejects any locked resolution below those
floors.

## Supported Python versions

The minimum supported Python version is 3.10. Python 3.9 selected
`marker-pdf 0.2.17` plus old urllib3, Torch, filelock, scikit-learn, and related
packages. Keeping that resolution would make a clean universal lock impossible.

## Marker compatibility overrides

`marker-pdf 1.10.2` and `surya-ocr 0.17.1` declare Pillow `<11`, while the first
release covering all current Pillow advisories is 12.2. BlobForge overrides
that stale cap to Pillow 12.2 or newer. This is an intentional compatibility
boundary and must be revalidated on Marker upgrades. Validation includes the
normal Marker import path and a complete conversion of `assets/lorem.pdf`; the
initial remediation passed with Pillow 12.3.

Do not override Marker's Transformers `<5` cap. Surya 0.17.1 imports removed
Transformers 4 APIs including `transformers.onnx`, pruning helpers, and private
tokenizer helpers. Transformers 5 therefore breaks Marker before conversion.

## Non-applicable open advisories

Four GitHub advisories cannot be upgraded without breaking supported conversion
and do not cross a BlobForge trust boundary:

- Dependabot alerts 25, 41, and 44 affect Transformers `Trainer` or loading
  attacker-selected model definitions. BlobForge performs inference using the
  fixed Surya models selected by the installed Marker release; submitted PDFs
  cannot select a model or invoke `Trainer`.
- Alert 36 affects `torch.jit.script`. BlobForge and Marker's exercised PDF
  conversion path do not call TorchScript. GitHub records no patched release;
  Torch 2.10 is retained to preserve the CUDA 12 worker fleet while fixing the
  other two Torch advisories.

These alerts should be dismissed as `not_used` only after this policy and the
patched lockfile are committed to the default branch. Re-evaluate the dismissal
if BlobForge adds user-selectable models, training, TorchScript, or changes its
Marker/Surya stack.
