---
name: slg-testcase-fusion
description: Fuse the multi-format requirement ingestion pipeline from requirements-to-testcases with the SLG-specific analysis and BM-import-aligned testcase output style from by-slg-testcase. Use when Codex needs to turn SLG game feature specs, event requirements, spreadsheets, screenshots, PDFs, DOCX, OCR images, or mixed requirement folders into executable Chinese test cases, while preserving assumptions or traceability only as supporting artifacts and keeping the final output biased toward by-slg-testcase.
---

# SLG Testcase Fusion

## Overview

Use this skill as a fusion layer, not as a replacement for the two existing skills. Treat `requirements-to-testcases` as the preprocessing and normalization layer, and treat `by-slg-testcase` as the domain authority for SLG decomposition, risk expansion, and final BM-import-friendly output.

## Workflow

1. Inventory the input materials and classify them as:
   - primary fact sources
   - supporting evidence
   - deferred reading
2. If the input is multi-format, noisy, image-heavy, or spread across folders, use the ingestion scripts from `../../requirements-to-testcases/scripts/` to normalize it first.
3. Convert the normalized output into four buckets before testcase drafting:
   - confirmed facts
   - open questions
   - inherited logic
   - experience-based SLG risks
4. Use the SLG decomposition and testcase strategy from `../by-slg-testcase/references/` to organize modules and high-risk scenarios.
5. Generate the final answer in the output shape preferred by `by-slg-testcase`, unless the user explicitly asks for the generic JSON/XLSX structure from `requirements-to-testcases`.

## Decision Rules

- Prefer reading primary requirement text before screenshots, OCR fragments, or broad config tables.
- Prefer `requirements-to-testcases` for ingestion, OCR, standardization, and draft traceability.
- Prefer `by-slg-testcase` for module grouping, risk coverage, combination-activity拆解, and final table wording.
- When the feature is a combination activity, always place child gameplay groups under the combination root group in `所属分组`.
- For example, use `派对组合活动|派对射击|...`, `派对组合活动|派对拼图|...`, `派对组合活动|派对BP|...` instead of making `派对射击`, `派对拼图`, and `派对BP` peer groups of `派对组合活动`.
- Do not mechanically emit the generic `requirements / test_cases / traceability` structure if the user asked for import-ready SLG cases.
- Do not upgrade assumptions into confirmed facts. Keep unsupported conclusions in `待确认`, `继承逻辑`, or a short appendix.
- If the user does not ask for traceability, keep it as an internal aid or a short appendix instead of making it the primary output.

## Preferred Execution Path

### 1. Normalize Inputs When Needed

Use `../../requirements-to-testcases/scripts/ingest_requirements.py` when:
- the user provides `.docx`, `.xlsx`, `.jpg`, `.png`, or mixed folders
- OCR or embedded Excel images are likely needed
- raw materials are fragmented and need to be normalized into a `requirements.json` draft

Use `../../requirements-to-testcases/scripts/gen_testcases.py` only when a quick structural draft or trace matrix helps reduce ambiguity. Treat its output as a draft, not the final output authority.

### 2. Rebuild the Requirement View for SLG Testing

Before producing the final testcase table, rewrite the normalized input into:
- 已确认
- 待确认
- 继承逻辑
- 基于 SLG 经验补充的风险

If module boundaries are unclear, read `../by-slg-testcase/references/slg-modules.md`.
If testcase depth is unclear, read `../by-slg-testcase/references/testcase-strategy.md`.
If final columns or wording are unclear, read `../by-slg-testcase/references/import-template-mapping.md` and `../by-slg-testcase/references/testcase-template.md`.

### 3. Bias the Final Output Toward by-slg-testcase

Default final output order:

**需求确认项：**
- 已确认
- 待确认
- 继承逻辑
- 基于经验补充的风险

**测试用例导入表：**

| 标题* | 所属分组 | 维护人 | 前置条件 | 步骤描述 | 预期结果 | 用例等级 | 用例类型 |
|------|------|------|------|------|------|------|------|

**需要补充的信息：**

Grouping rule for combination activities:
- Use the combination activity as the first-level group.
- Put each child activity under that root group.
- Keep shared entry, tab, settlement, mail, and supplement logic under the combination root or a shared child branch under that root.
- Do not output sibling groups such as `派对组合活动` and `派对射击` at the same level when `派对射击` belongs to that combination activity.

Only append `assumptions`, `open_questions`, or `traceability` when one of these is true:
- the user explicitly asks for them
- the source material is incomplete and hiding uncertainty would be misleading
- the user asks for an audit trail or review artifact

When appending those sections, keep them secondary. The main body should still read like `by-slg-testcase`, not like a generic export from `requirements-to-testcases`.

## References

- Read `references/fusion-workflow.md` when deciding how far to use the preprocessing pipeline versus manual SLG reasoning.
- Read `references/output-bias.md` when the final result starts drifting toward generic JSON, over-explaining traceability, or losing the BM import shape.
- Read `../by-slg-testcase/references/slg-modules.md` when the feature spans multiple gameplay modules.
- Read `../by-slg-testcase/references/testcase-strategy.md` when deciding which abnormal, boundary, concurrency, or settlement scenarios deserve separate cases.
- Read `../by-slg-testcase/references/testcase-template.md` and `../by-slg-testcase/references/import-template-mapping.md` before producing import-ready output.
- Use `../../requirements-to-testcases/scripts/ingest_requirements.py` and `../../requirements-to-testcases/scripts/gen_testcases.py` as preprocessing aids.

## Local Maintenance

- After editing this skill, run `python scripts/validate_skill_local.py`.
- If external dependency paths change, update the validator and the reference list together.
