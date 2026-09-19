#!/usr/bin/env python3
"""Compare Criterion baselines and fail if mean time regresses by more than a threshold."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def estimates(root: Path, baseline: str) -> dict[str, float]:
    found: dict[str, float] = {}
    for path in root.rglob("estimates.json"):
        if path.parent.name != baseline:
            continue
        group = path.parent.parent
        key = str(group.relative_to(root)).replace("\\", "/")
        data = json.loads(path.read_text(encoding="utf-8"))
        mean = data.get("mean", {}).get("point_estimate")
        if mean is None:
            continue
        found[key] = float(mean)
    return found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--criterion-dir", default="target/criterion")
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--current", required=True)
    parser.add_argument("--threshold", type=float, default=0.10)
    parser.add_argument(
        "--min-ns",
        type=float,
        default=1000.0,
        help="Ignore relative regressions smaller than this absolute nanosecond delta "
        "(CI noise on sub-microsecond benches).",
    )
    args = parser.parse_args()

    root = Path(args.criterion_dir)
    if not root.exists():
        print(f"missing criterion dir: {root}", file=sys.stderr)
        return 1

    baseline = estimates(root, args.baseline)
    current = estimates(root, args.current)
    if not baseline or not current:
        print(
            f"missing estimates (baseline={len(baseline)} current={len(current)})",
            file=sys.stderr,
        )
        return 1

    failed = False
    for key, base_mean in sorted(baseline.items()):
        new_mean = current.get(key)
        if new_mean is None:
            print(f"missing current estimate for {key}", file=sys.stderr)
            failed = True
            continue
        if base_mean <= 0:
            continue
        delta = (new_mean - base_mean) / base_mean
        abs_delta = new_mean - base_mean
        status = "ok"
        if delta > args.threshold and abs_delta > args.min_ns:
            status = "REGRESSED"
            failed = True
        elif delta > args.threshold:
            status = "noise"
        print(
            f"{status:9} {key}: {delta:+.1%} ({base_mean:.4g} -> {new_mean:.4g})"
        )

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
