#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def build_trace(test_cases):
    mapping = {}
    for tc in test_cases:
        tcid = tc.get("测试用例ID", tc.get("test_case_id", ""))
        requirement_ids = tc.get("关联需求ID列表", tc.get("requirement_ids", []))
        for rid in requirement_ids:
            mapping.setdefault(rid, []).append(tcid)
    return [{"requirement_id": rid, "test_case_ids": ids} for rid, ids in sorted(mapping.items())]


def main():
    ap = argparse.ArgumentParser(description="基于测试用例 JSON 生成需求追踪矩阵")
    ap.add_argument("testcases_json", help="包含 test_cases 的 JSON 路径")
    ap.add_argument("-o", "--output", default="traceability.json", help="输出 JSON 路径")
    args = ap.parse_args()

    data = json.loads(Path(args.testcases_json).read_text(encoding="utf-8"))
    trace = build_trace(data.get("test_cases", []))
    Path(args.output).write_text(json.dumps({"traceability": trace}, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"已生成 {len(trace)} 条追踪映射 -> {args.output}")


if __name__ == "__main__":
    main()
