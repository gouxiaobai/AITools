#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path


def _extract_numbers(text: str):
    return re.findall(r"\d+(?:\.\d+)?", text)


def _domain_steps(req_id: str, source_text: str):
    t = source_text
    if "激活条件" in t or "前置" in t or "已建立第一小队" in t:
        return [
            "准备两组场景：满足前置条件与不满足前置条件",
            "依次触发探险车剧情结束、建立小队等关键前置动作",
            f"观察 {req_id} 是否按条件激活/不激活对应表演逻辑",
        ]
    if "刷新" in t:
        return [
            "配置小队人数（2/3/4/5）并进入防守场景",
            "记录丧尸刷新数量、刷新时机与刷新点分布",
            f"校验 {req_id} 描述的刷新规则与实际表现是否一致",
        ]
    if "攻击" in t or "行走" in t or "奔跑" in t or "坐标" in t:
        return [
            "在可观测日志/战斗回放中记录丧尸与英雄行为",
            "关注目标选择、行走/奔跑切换、攻击频率与死亡触发",
            f"核对 {req_id} 对行为逻辑的定义是否被严格执行",
        ]
    if "掉落" in t or "概率" in t or "物资" in t:
        return [
            "批量触发丧尸死亡并统计掉落数据",
            "记录掉落种类、数量、分散情况与飘飞目标位置",
            f"对照 {req_id} 验证概率与表现规则",
        ]
    return [
        "准备满足前置条件的场景与数据",
        f"触发 {req_id} 对应功能流程",
        "记录系统行为、数值和时序表现",
    ]


def _build_steps(req_id: str, source_text: str, kind: str):
    steps = [f"读取需求：{source_text}"]
    if kind == "happy":
        steps.extend(_domain_steps(req_id, source_text))
    elif kind == "negative":
        steps.extend(
            [
                "构造不满足条件或异常输入场景",
                f"触发 {req_id} 对应流程",
                "观察系统是否阻止错误流程并保持状态一致",
            ]
        )
    elif kind == "boundary":
        nums = _extract_numbers(source_text)
        boundary_hint = f"（边界值候选：{', '.join(nums)}）" if nums else ""
        steps.extend(
            [
                f"基于需求中的阈值/数量/时间设计边界输入 {boundary_hint}",
                f"触发 {req_id} 对应流程",
                "记录临界点上下系统行为差异",
            ]
        )
    else:
        steps.extend(
            [
                "构造非功能测试场景（性能/稳定性/可靠性等）",
                f"执行 {req_id} 对应流程并采集指标",
                "对照阈值或规则判定是否达标",
            ]
        )
    return steps


def _build_expected(source_text: str, kind: str):
    if kind == "happy":
        return f"系统应满足需求描述：{source_text}"
    if kind == "negative":
        return f"在异常/不满足条件场景下，系统不应违反需求约束：{source_text}"
    if kind == "boundary":
        return f"系统在边界值附近应保持与需求一致的稳定行为：{source_text}"
    return f"系统在非功能维度应满足需求约束：{source_text}"


def mk_case(tc_id, title, kind, req_id, source_text):
    return {
        "测试用例ID": tc_id,
        "标题": title,
        "优先级": "P0" if any(k in source_text.lower() for k in ["must", "必须", "不得", "禁止"]) else "P1",
        "类型": kind,
        "前置条件": ["系统已部署且可访问"],
        "步骤": _build_steps(req_id, source_text, kind),
        "预期结果": _build_expected(source_text, kind),
        "关联需求ID列表": [req_id],
    }


def generate(requirements):
    cases = []
    trace = []
    tc_counter = 1

    for req in requirements:
        rid = req["requirement_id"]
        text = req["source_text"]

        local_ids = []
        for kind, suffix in [("happy", "正向路径"), ("negative", "负向/失败路径")]:
            tcid = f"TC-{tc_counter:03d}"
            tc_counter += 1
            cases.append(mk_case(tcid, f"{rid} {suffix}", kind, rid, text))
            local_ids.append(tcid)

        if any(
            k in text.lower()
            for k in [
                "max", "min", "length", "range", "limit", "at least", "at most",
                "最大", "最小", "长度", "范围", "上限", "下限", "至少", "至多",
            ]
        ):
            tcid = f"TC-{tc_counter:03d}"
            tc_counter += 1
            cases.append(mk_case(tcid, f"{rid} 边界值验证", "boundary", rid, text))
            local_ids.append(tcid)

        if req.get("category") == "non-functional":
            tcid = f"TC-{tc_counter:03d}"
            tc_counter += 1
            cases.append(mk_case(tcid, f"{rid} 非功能验证", "nfr", rid, text))
            local_ids.append(tcid)

        trace.append({"requirement_id": rid, "test_case_ids": local_ids})

    return cases, trace


def build_output(requirements):
    test_cases, traceability = generate(requirements)
    return {
        "requirements": requirements,
        "test_cases": test_cases,
        "assumptions": [
            "当前结果为基础草案，需结合业务规则进一步细化。",
            "缺失的业务约束应在 open_questions 中补充确认。",
        ],
        "open_questions": [],
        "traceability": traceability,
    }


def write_output(output_data, output_path: Path, template_path: str = "", group: str = "未分组", owner: str = "", case_type: str = ""):
    if output_path.suffix.lower() == ".xlsx":
        # Local import to avoid hard dependency when only JSON output is needed.
        from export_testcases_xlsx import build_workbook, build_import_template_workbook

        if template_path:
            wb = build_import_template_workbook(
                data=output_data,
                template_path=Path(template_path),
                group=group,
                owner=owner,
                case_type_default=case_type,
            )
        else:
            wb = build_workbook(output_data)
        wb.save(output_path)
        return "xlsx"

    output_path.write_text(json.dumps(output_data, indent=2, ensure_ascii=False), encoding="utf-8")
    return "json"


def main():
    ap = argparse.ArgumentParser(description="基于需求 JSON 生成基础测试用例（支持 JSON / XLSX 输出）")
    ap.add_argument("requirements_json", help="extract_requirements.py / ingest_requirements.py 产出的 JSON 路径")
    ap.add_argument("-o", "--output", default="testcases.json", help="输出路径：.json 或 .xlsx")
    ap.add_argument("--template", default="", help="导出为 .xlsx 时可指定导入模板路径")
    ap.add_argument("--group", default="未分组", help="导出为模板时写入“所属分组”")
    ap.add_argument("--owner", default="", help="导出为模板时写入“维护人”")
    ap.add_argument("--case-type", default="", help="导出为模板时写入“用例类型”")
    args = ap.parse_args()

    data = json.loads(Path(args.requirements_json).read_text(encoding="utf-8"))
    requirements = data.get("requirements", [])

    out_data = build_output(requirements)
    out_path = Path(args.output)
    kind = write_output(
        out_data,
        out_path,
        template_path=args.template,
        group=args.group,
        owner=args.owner,
        case_type=args.case_type,
    )

    print(f"已生成 {len(out_data['test_cases'])} 条测试用例 -> {out_path} ({kind})")


if __name__ == "__main__":
    main()
