"""Regression checks for dependency versions covered by Dependabot alerts."""

import re
from pathlib import Path

from packaging.version import Version


ROOT = Path(__file__).resolve().parents[1]

# Each floor is at or above every patched version in the corresponding open
# advisory. Alerts whose vulnerable code is not used by BlobForge are documented
# in docs/dependency_security.md and excluded from the floor check.
SECURE_FLOORS = {
    "cryptography": "48.0.1",
    "filelock": "3.20.3",
    "idna": "3.15",
    "pillow": "12.2.0",
    "pyasn1": "0.6.3",
    "pydantic-settings": "2.14.2",
    "pygments": "2.20.0",
    "pytest": "9.0.3",
    "python-dotenv": "1.2.2",
    "requests": "2.33.0",
    "scikit-learn": "1.5.0",
    "soupsieve": "2.8.4",
    "torch": "2.10.0",
    "urllib3": "2.7.0",
}


def locked_versions() -> dict[str, list[Version]]:
    text = (ROOT / "uv.lock").read_text(encoding="utf-8")
    versions: dict[str, list[Version]] = {}
    for block in re.findall(r"\[\[package\]\]\n(.*?)(?=\n\[\[package\]\]|\Z)", text, re.DOTALL):
        name = re.search(r'^name = "([^"]+)"$', block, re.MULTILINE)
        version = re.search(r'^version = "([^"]+)"$', block, re.MULTILINE)
        if name and version:
            versions.setdefault(name.group(1).lower(), []).append(Version(version.group(1)))
    return versions


def test_all_locked_resolutions_meet_security_floors():
    versions = locked_versions()
    for package, floor in SECURE_FLOORS.items():
        assert package in versions, f"{package} is missing from uv.lock"
        assert min(versions[package]) >= Version(floor), (
            f"{package} retains vulnerable resolution(s): {versions[package]} < {floor}"
        )


def test_marker_compatibility_override_is_explicit():
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'requires-python = ">=3.10"' in pyproject
    assert '"pillow>=12.2.0"' in pyproject
