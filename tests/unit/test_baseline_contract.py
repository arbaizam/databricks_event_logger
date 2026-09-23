"""Keep the independently captured baseline contract runnable without Git."""

import subprocess
import sys
from pathlib import Path

import pytest


def test_saved_baseline_contract():
    pytest.importorskip("numpy")
    pytest.importorskip("pyspark")
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, str(root / "scripts/compare_baseline.py")],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
