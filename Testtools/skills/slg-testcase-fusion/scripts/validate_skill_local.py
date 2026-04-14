#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL_MD = ROOT / "SKILL.md"
OPENAI_YAML = ROOT / "agents" / "openai.yaml"

REQUIRED_LOCAL_FILES = [
    SKILL_MD,
    OPENAI_YAML,
    ROOT / "references" / "fusion-workflow.md",
    ROOT / "references" / "output-bias.md",
]

REQUIRED_EXTERNAL_FILES = [
    ROOT.parent / "by-slg-testcase" / "SKILL.md",
    ROOT.parent / "by-slg-testcase" / "references" / "slg-modules.md",
    ROOT.parent / "by-slg-testcase" / "references" / "testcase-strategy.md",
    ROOT.parent / "by-slg-testcase" / "references" / "testcase-template.md",
    ROOT.parent / "by-slg-testcase" / "references" / "import-template-mapping.md",
    ROOT.parents[1] / "requirements-to-testcases" / "SKILL.md",
    ROOT.parents[1] / "requirements-to-testcases" / "scripts" / "ingest_requirements.py",
    ROOT.parents[1] / "requirements-to-testcases" / "scripts" / "gen_testcases.py",
]


def read_utf8(path: Path) -> str:
    data = path.read_bytes()
    if data.startswith(b"\xef\xbb\xbf"):
        raise ValueError("contains UTF-8 BOM")
    return data.decode("utf-8")


def ensure_exists(paths: list[Path]) -> list[str]:
    errors: list[str] = []
    for path in paths:
        if not path.exists():
            errors.append(f"missing file: {path}")
    return errors


def ensure_contains(text: str, needle: str, label: str) -> list[str]:
    if needle in text:
        return []
    return [f"missing required text in {label}: {needle}"]


def main() -> int:
    errors = ensure_exists(REQUIRED_LOCAL_FILES) + ensure_exists(REQUIRED_EXTERNAL_FILES)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    try:
        skill_text = read_utf8(SKILL_MD)
        openai_yaml_text = read_utf8(OPENAI_YAML)
    except Exception as exc:
        print(f"ERROR: failed to read local skill files: {exc}")
        return 1

    for needle in [
        "name: slg-testcase-fusion",
        "../by-slg-testcase/references/slg-modules.md",
        "../../requirements-to-testcases/scripts/ingest_requirements.py",
        "../../requirements-to-testcases/scripts/gen_testcases.py",
        "| 标题* | 所属分组 | 维护人 | 前置条件 | 步骤描述 | 预期结果 | 用例等级 | 用例类型 |",
        "派对组合活动|派对射击|",
    ]:
        errors.extend(ensure_contains(skill_text, needle, "SKILL.md"))

    for needle in [
        'display_name: "SLG Testcase Fusion"',
        'short_description: "Fuse requirement ingestion with SLG-focused testcase output"',
        'default_prompt: "Use $slg-testcase-fusion',
    ]:
        errors.extend(ensure_contains(openai_yaml_text, needle, "agents/openai.yaml"))

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print(f"OK: {ROOT}")
    print("- local files exist")
    print("- external dependency files exist")
    print("- SKILL.md contains fusion references and the template-aligned table header")
    print("- openai.yaml includes the expected interface values")
    return 0


if __name__ == "__main__":
    sys.exit(main())
