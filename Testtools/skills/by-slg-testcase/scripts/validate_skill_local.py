#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL_MD = ROOT / "SKILL.md"
OPENAI_YAML = ROOT / "agents" / "openai.yaml"
REFERENCE_DIR = ROOT / "references"

REQUIRED_FILES = [
    SKILL_MD,
    OPENAI_YAML,
    REFERENCE_DIR / "slg-modules.md",
    REFERENCE_DIR / "import-template-mapping.md",
    REFERENCE_DIR / "testcase-strategy.md",
    REFERENCE_DIR / "testcase-template.md",
]

DEFAULT_TABLE_HEADER = (
    "| 标题* | 所属分组 | 维护人 | 前置条件 | 步骤描述 | 预期结果 | 用例等级 | 用例类型 |"
)


def read_utf8(path: Path) -> str:
    data = path.read_bytes()
    if data.startswith(b"\xef\xbb\xbf"):
        raise ValueError("contains UTF-8 BOM")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"not valid UTF-8: {exc}") from exc
    if "\ufffd" in text:
        raise ValueError("contains replacement character U+FFFD")
    return text


def extract_frontmatter(text: str) -> dict[str, str]:
    match = re.match(r"^---\n(.*?)\n---\n", text, re.DOTALL)
    if not match:
        raise ValueError("missing YAML frontmatter")

    frontmatter: dict[str, str] = {}
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if ":" not in line:
            raise ValueError(f"invalid frontmatter line: {raw_line}")
        key, value = line.split(":", 1)
        frontmatter[key.strip()] = value.strip()
    return frontmatter


def extract_code_block_table_header(template_text: str) -> str:
    match = re.search(
        r"\*\*测试用例导入表：\*\*\n\n(\|.+\|)\n(\|[-| ]+\|)",
        template_text,
        re.MULTILINE,
    )
    if not match:
        raise ValueError("cannot find default testcase import table header in testcase-template.md")
    return match.group(1).strip()


def ensure_contains(text: str, needle: str, label: str) -> None:
    if needle not in text:
        raise ValueError(f"missing required text in {label}: {needle}")


def validate_openai_yaml(text: str) -> None:
    ensure_contains(text, "interface:", "agents/openai.yaml")
    for key in ("display_name:", "short_description:", "default_prompt:"):
        ensure_contains(text, key, "agents/openai.yaml")


def main() -> int:
    errors: list[str] = []
    decoded: dict[Path, str] = {}

    for path in REQUIRED_FILES:
        if not path.exists():
            errors.append(f"missing file: {path}")
            continue
        try:
            decoded[path] = read_utf8(path)
        except ValueError as exc:
            errors.append(f"{path}: {exc}")

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    skill_text = decoded[SKILL_MD]
    template_text = decoded[REFERENCE_DIR / "testcase-template.md"]
    strategy_text = decoded[REFERENCE_DIR / "testcase-strategy.md"]
    openai_yaml_text = decoded[OPENAI_YAML]

    try:
        frontmatter = extract_frontmatter(skill_text)
        for required_key in ("name", "description"):
            if required_key not in frontmatter or not frontmatter[required_key]:
                raise ValueError(f"frontmatter missing {required_key}")
    except ValueError as exc:
        errors.append(f"{SKILL_MD}: {exc}")

    for ref_name in (
        "references/slg-modules.md",
        "references/import-template-mapping.md",
        "references/testcase-strategy.md",
        "references/testcase-template.md",
    ):
        if ref_name not in skill_text:
            errors.append(f"{SKILL_MD}: missing reference link {ref_name}")

    try:
        template_header = extract_code_block_table_header(template_text)
        if DEFAULT_TABLE_HEADER not in skill_text:
            errors.append(f"{SKILL_MD}: missing default import table header")
        if template_header != DEFAULT_TABLE_HEADER:
            errors.append(
                "references/testcase-template.md: default testcase import table header does not match expected header"
            )
        if "默认输出列为：标题*、所属分组、维护人、前置条件、步骤描述、预期结果、用例等级、用例类型。" not in strategy_text:
            errors.append(
                "references/testcase-strategy.md: missing template-aligned default output rule"
            )
    except ValueError as exc:
        errors.append(str(exc))

    try:
        validate_openai_yaml(openai_yaml_text)
    except ValueError as exc:
        errors.append(str(exc))

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print(f"OK: {ROOT}")
    print("Checked:")
    print("- required files exist")
    print("- all checked files are UTF-8 without BOM")
    print("- SKILL.md frontmatter has name and description")
    print("- reference files mentioned in SKILL.md exist")
    print("- default testcase import table header is consistent")
    print("- openai.yaml includes required interface fields")
    return 0


if __name__ == "__main__":
    sys.exit(main())
