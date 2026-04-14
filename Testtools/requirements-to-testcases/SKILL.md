---
name: requirements-to-testcases
description: 从需求文档（PRD、用户故事、规格说明、Markdown、TXT、DOCX、XLSX、JPG、PNG）生成结构化测试用例与需求追踪矩阵。适用于需要把需求转换为测试场景、边界/异常用例、假设项与需求-用例映射的场景。
---

# 需求转测试用例

## 工作流程

1. 使用 `scripts/ingest_requirements.py` 将输入需求标准化为 `requirements.json`。
2. 识别歧义并输出 `ASSUMPTION` 与 `OPEN_QUESTION`。
3. 使用 `scripts/gen_testcases.py` 生成测试用例与追踪矩阵。
4. 按 `references/coverage-checklist.md` 做覆盖检查。

## 输入约定

支持输入：
- `.md` / `.txt`
- `.docx`（依赖 `python-docx`）
- `.xlsx/.xlsm/.xltx/.xltm`（依赖 `openpyxl`，支持读取单元格文本与内嵌图片 OCR）
- `.jpg/.jpeg/.png`（OCR 依赖 `pytesseract+Pillow`、`easyocr` 或 `rapidocr-onnxruntime`）
- 粘贴文本

推荐命令：
```bash
python scripts/ingest_requirements.py <input_file_or_dir> -o requirements.json
```

若输入不可机读或信息不完整：
- 继续最佳努力抽取
- 不确定内容写入 `ASSUMPTION`
- 待确认内容写入 `OPEN_QUESTION`

## 输出约定

标准输出字段（见 `references/test-case-template.md`）：
- `requirements`
- `test_cases`
- `assumptions`
- `open_questions`
- `traceability`

每条测试用例最少字段：
- `测试用例ID`
- `标题`
- `优先级`
- `类型`（`happy`/`negative`/`boundary`/`nfr`）
- `前置条件`
- `步骤`
- `预期结果`
- `关联需求ID列表`

## 覆盖规则

每条需求至少：
- 1 条正向用例
- 1 条负向/失败路径用例
- 存在约束时增加 1 条边界用例

非功能需求（性能/安全/可靠性）必须生成 `nfr` 用例。

## 质量门禁

输出前必须满足：
- 每个需求 ID 均存在追踪映射
- 不存在未映射用例的需求
- 测试用例 ID 唯一
- 步骤可执行，预期结果可验证

## XLSX 输出

直接输出 Excel：
```bash
python scripts/gen_testcases.py requirements.json -o testcases.xlsx
```

可选两步法（兼容旧流程）：
```bash
python scripts/gen_testcases.py requirements.json -o testcases.json
python scripts/export_testcases_xlsx.py testcases.json -o testcases.xlsx
```

Excel 工作表：
- `test_cases`
- `requirements`
- `traceability`
- `notes`

## 资源使用

- `scripts/ingest_requirements.py`：多格式输入解析
- `scripts/extract_requirements.py`：纯文本需求抽取
- `scripts/gen_testcases.py`：生成测试用例（JSON/XLSX）
- `scripts/trace_matrix.py`：追踪矩阵重建/校验
- `references/coverage-checklist.md`：覆盖检查
- `references/nfr-rules.md`：NFR 映射
