#!/usr/bin/env python3
"""Fail unless every library source has exact 100% line, function and region coverage."""
import json
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parent.parent
report = subprocess.check_output(
    ["swift", "test", "--show-codecov-path"], cwd=root, text=True
).strip()
data = json.loads(Path(report).read_text())
expected = {str(path.resolve()) for path in (root / "Sources").rglob("*.swift")}
seen = set()
failures = []
for dataset in data["data"]:
    for file in dataset["files"]:
        path = str(Path(file["filename"]).resolve())
        if path not in expected:
            continue
        seen.add(path)
        measurements = []
        for metric in ("lines", "functions", "regions"):
            value = file["summary"][metric]
            covered, total = value["covered"], value["count"]
            measurements.append(f"{metric}: {covered}/{total} ({100 * covered / max(total, 1):.2f}%)")
            if total == 0 or covered != total:
                failures.append(f"{Path(path).name}: incomplete {metric}")
        print(f"{Path(path).relative_to(root)} — {', '.join(measurements)}")
if not expected or seen != expected:
    failures.append(f"Missing library coverage: {sorted(expected - seen)}")
if failures:
    sys.exit("Coverage gate FAILED: " + "; ".join(failures))
print("Coverage gate PASSED: 100% of library lines, functions and regions.")
