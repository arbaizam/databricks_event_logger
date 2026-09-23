"""Compare public observations against a source checkout or the saved baseline.

Run from the repository root with dev and spark-test extras installed:
    python scripts/compare_baseline.py --baseline ../review-baseline

Without --baseline, compare to the checked-in 77f33cf observation digests.
--write-baseline is only for regenerating that fixture from the old checkout;
do not use the refactored source to approve changes to the reference contract.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/baseline_contract.json"


def observe(source: Path) -> dict:
    """Run the same probe in a fresh interpreter against source/src."""
    with tempfile.TemporaryDirectory(prefix="event-logger-comparison-") as temporary:
        output = Path(temporary) / "observations.json"
        result = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts/_compatibility_probe.py"),
                str(source.resolve()),
                str(output),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode:
            raise RuntimeError(result.stdout + result.stderr)
        return json.loads(output.read_text(encoding="utf-8"))


def digests(observations: dict) -> dict:
    """Keep small, deterministic per-area fingerprints instead of a huge fixture."""
    groups = defaultdict(dict)
    for name, value in observations.items():
        groups[name.split("/")[0]][name] = value
    return {
        name: {
            "count": len(values),
            "sha256": hashlib.sha256(
                json.dumps(values, sort_keys=True, ensure_ascii=True).encode("utf-8")
            ).hexdigest(),
        }
        for name, values in sorted(groups.items())
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--write-baseline", action="store_true")
    args = parser.parse_args()
    if args.write_baseline and args.baseline is None:
        parser.error("--write-baseline requires --baseline pointing to commit 77f33cf")
    if args.baseline:
        revision = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=args.baseline, text=True,
        ).strip()
        if not revision.startswith("77f33cf"):
            parser.error("--baseline must point to the original 77f33cf checkout")
        baseline = observe(args.baseline)
        expected = digests(baseline)
    else:
        baseline = None
        expected = json.loads(FIXTURE.read_text(encoding="utf-8"))["areas"]
    if args.write_baseline:
        FIXTURE.write_text(json.dumps({"revision": revision, "areas": expected}, indent=2) + "\n")
    current = observe(ROOT)
    actual = digests(current)
    differences = [
        name for name in expected.keys() | actual.keys() if expected.get(name) != actual.get(name)
    ]
    if differences:
        print("Different areas:", ", ".join(sorted(differences)))
        if baseline is not None:
            for name in sorted(baseline.keys() | current.keys()):
                if baseline.get(name) != current.get(name):
                    print(name, "\nbaseline:", baseline.get(name), "\ncurrent:", current.get(name))
        return 1
    print(f"Matched {len(current)} observations against 77f33cf.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
