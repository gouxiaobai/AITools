# XLSX Mapping Guide

## Goal

Normalize `.xlsx` 埋点需求文档 into comparable event/field records for log verification and test report generation.

## Preferred Sheets

Use this priority when multiple sheets exist:
1. `打点需求`
2. Sheets explicitly describing event definitions and parameters
3. Report or warehouse logic sheets only as supplemental context

## Normalized Schema

Map source cells to the following normalized fields:

| normalized_field | meaning |
|---|---|
| module_name | 功能模块名（如地块解锁） |
| event_name | 事件名（如 cityplot_regionunlock） |
| event_desc | 事件中文说明 |
| trigger_condition | 触发条件/触发时机 |
| field_name | 参数名（如 region_id） |
| field_type | 参数类型（string/int/bool 等） |
| required_flag | 是否必填 |
| value_rule | 取值规则/枚举范围 |
| sample_value | 示例值 |
| notes | 补充口径 |

## Heuristics

- If event row has no explicit parameter rows, create one record with `field_name=__event_level__`.
- If merged cells cause blanks, inherit nearest non-empty value from previous rows in the same logical block.
- If same `event_name + field_name` appears multiple times, merge notes and keep strictest rule.
- If field type is absent, infer from value rule and mark as `inferred`.

## Validation Rules

- Reject records missing `event_name`.
- Mark as `pending_confirm` if trigger condition is missing.
- Mark as `pending_confirm` if value rule conflicts across rows.
- If requirement includes user/account scope, validate it together with input `user_id` and mark mismatch as `pending_confirm`.

## Output for Comparison

Produce two datasets:
1. `expected_events`: unique event-level requirements
2. `expected_fields`: event + field level requirements

Both datasets must keep source traceability:
- `source_sheet`
- `source_row_index`
- `source_excerpt`
