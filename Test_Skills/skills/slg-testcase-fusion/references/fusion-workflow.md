# Fusion Workflow

Use this reference when deciding how to combine the two existing skills without duplicating them.

## Core Positioning

- `requirements-to-testcases` provides preprocessing, extraction, OCR support, normalized requirement drafts, and traceability scaffolding.
- `by-slg-testcase` provides SLG module decomposition, risk-oriented testcase design, and the preferred BM-import-ready final format.
- `slg-testcase-fusion` chooses when to invoke each side and resolves conflicts in favor of `by-slg-testcase` for the final answer shape.

## Recommended Sequence

1. Read the user's request and identify whether the goal is:
   - import-ready Chinese SLG testcases
   - a generic requirement-to-testcase conversion artifact
   - both
2. If the source material is fragmented or non-text-first, preprocess with:
   - `../../requirements-to-testcases/scripts/ingest_requirements.py`
3. If the material is large or ambiguous, optionally generate a structural draft with:
   - `../../requirements-to-testcases/scripts/gen_testcases.py`
4. Reclassify the result into:
   - confirmed facts
   - open questions
   - inherited logic
   - SLG risk supplements
5. Reorganize the testcase plan according to SLG module boundaries and activity structure.
6. If the feature is a combination activity, build the group path from the combination root downward instead of treating child activities as peer roots.
7. Emit examples such as:
   - `派对组合活动|入口与页签|默认页签`
   - `派对组合活动|派对射击|预告与主页|状态切换`
   - `派对组合活动|派对拼图|宝箱奖励|大宝箱`
   - `派对组合活动|派对BP|任务与奖励|任务结算`
8. Emit the final result in the `by-slg-testcase` output style by default.

## When To Lean Harder On requirements-to-testcases

- The user gives mixed file formats and wants extraction first.
- OCR from screenshots or Excel embedded images is necessary.
- The user explicitly requests assumptions, open questions, or traceability.
- The first problem is input normalization rather than testcase wording.

## When To Lean Harder On by-slg-testcase

- The user wants direct import-ready Chinese cases.
- The feature is an SLG activity, backend rule, multi-page event, ranking, settlement, alliance cooperation, or a reused legacy gameplay variant.
- The source is incomplete but the domain risks are obvious and need to be called out carefully.
- The final answer should look like a human-authored SLG testcase sheet rather than a generic generated artifact.
