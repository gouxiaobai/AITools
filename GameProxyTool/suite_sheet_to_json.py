#!/usr/bin/env python3
"""Convert .xlsx suite sheet to GameProxyTool suite JSON."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from suite_xlsx import load_suite_from_xlsx_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert suite sheet (.xlsx) to suite JSON")
    parser.add_argument("input", help="Input sheet path (.xlsx)")
    parser.add_argument("-o", "--output", default=None, help="Output JSON path (default: suites/<input_stem>.json)")
    parser.add_argument("--suite-name", default=None, help="Override suite name")
    args = parser.parse_args()

    input_path = Path(args.input).resolve()
    if not input_path.exists():
        raise SystemExit(f"input not found: {input_path}")
    if input_path.suffix.lower() != ".xlsx":
        raise SystemExit("input must be .xlsx")

    suite = load_suite_from_xlsx_path(input_path, default_suite_name=args.suite_name or input_path.stem)

    out_path = Path(args.output).resolve() if args.output else (Path.cwd() / "suites" / f"{input_path.stem}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(suite, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"OK: wrote {out_path}")
    print(f"suite={suite['name']} cases={len(suite['cases'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
