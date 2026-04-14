# 测试用例模板

生成机器可读输出时，使用如下 JSON 结构：

```json
{
  "requirements": [
    {
      "requirement_id": "REQ-001",
      "source_text": "...",
      "category": "functional"
    }
  ],
  "test_cases": [
    {
      "测试用例ID": "TC-001",
      "标题": "...",
      "优先级": "P0",
      "类型": "happy",
      "前置条件": ["..."],
      "步骤": ["..."],
      "预期结果": "...",
      "关联需求ID列表": ["REQ-001"]
    }
  ],
  "assumptions": ["..."],
  "open_questions": ["..."],
  "traceability": [
    {
      "requirement_id": "REQ-001",
      "test_case_ids": ["TC-001", "TC-002"]
    }
  ]
}
```

`P0/P1/P2` 优先级建议：
- `P0`：核心业务流程或安全/合规关键路径
- `P1`：重要但非阻断路径
- `P2`：低影响或低频边缘场景
