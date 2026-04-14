# Output Bias

Use this reference when the fusion process starts drifting away from the intended final style.

## Default Bias

Bias the final output toward `by-slg-testcase`, especially in these areas:

- section order
- testcase column shape
- SLG module grouping
- combination-root-first group hierarchy
- combination-activity decomposition
- risk-driven scenario expansion
- concise Chinese testcase wording

## What To Keep From requirements-to-testcases

Keep these as supporting capabilities, not the main presentation:

- multi-format ingestion
- OCR extraction
- normalized `requirements.json`
- assumptions
- open questions
- traceability
- generic JSON/XLSX draft generation

## Conflict Resolution

If the two approaches pull in different directions, use these rules:

1. Final user-facing testcase table follows `by-slg-testcase`.
2. Generic traceability stays secondary unless explicitly requested.
3. Assumptions must never be silently upgraded into confirmed facts.
4. In combination activities, `所属分组` must start from the combination root before any child activity branch.
5. If ingestion output is mechanically correct but domain coverage is weak, expand using SLG risk rules.
6. If SLG experience suggests a risk but evidence is missing, surface it outside the import table instead of fabricating hard rules.
