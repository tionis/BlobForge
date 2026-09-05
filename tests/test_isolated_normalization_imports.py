"""Hosted adapter imports must not inherit the coordinator's package environment."""
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize('provider', ['mistral', 'datalab'])
def test_adapter_imports_without_coordinator_site_packages(provider):
    root = Path(__file__).resolve().parents[1]
    adapter = root / 'evaluators' / provider / 'adapter.py'
    result = subprocess.run(
        [sys.executable, '-I', '-S', '-c',
         'import runpy,sys; runpy.run_path(sys.argv[1])', str(adapter)],
        cwd=root, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
