#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path

REQ_PATTERNS = [
    re.compile(r"\bshall\b", re.I),
    re.compile(r"\bmust\b", re.I),
    re.compile(r"\bshould\b", re.I),
    re.compile(r"\brequire(?:s|d)?\b", re.I),
    re.compile(r"应当|应该|应支持|必须|需要|不得|禁止"),
]
NFR_HINTS = re.compile(
    r"\b(latency|throughput|concurrency|security|auth|availability|reliability|timeout|sla)\b|"
    r"性能|并发|吞吐|延迟|安全|鉴权|认证|可用性|可靠性|超时|SLA",
    re.I,
)

DOMAIN_HINTS = re.compile(
    r"条件|激活|触发|规则|逻辑|刷新|攻击|掉落|概率|数量|时间|坐标|前置|上阵|下阵|死亡|小队|丧尸|防守|探险车",
    re.I,
)

NOISE_PATTERNS = [
    re.compile(r"^\[Sheet\]"),
    re.compile(r"^\[Sheet Image\]"),
    re.compile(r"文档修改信息|修改人|修改日期|所属版本"),
    re.compile(r"^一、|^二、|^三、|^四、|^五、|^六、|^七、|^八、|^九、|^十、"),
    re.compile(r"^设计目的$|^条件内容$|^防守表演序列逻辑$"),
    re.compile(r"^序号\s*\|\s*需求类型"),
    re.compile(r"\|\s*v\d+(\.\d+)*$", re.I),
    re.compile(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}"),
]


def classify(text: str) -> str:
    return "non-functional" if NFR_HINTS.search(text) else "functional"


def is_noise_line(line: str) -> bool:
    if len(line.strip()) < 6:
        return True
    for p in NOISE_PATTERNS:
        if p.search(line):
            return True
    return False


def line_score(line: str) -> int:
    score = 0
    if any(p.search(line) for p in REQ_PATTERNS):
        score += 3
    if DOMAIN_HINTS.search(line):
        score += 2
    if "：" in line or ":" in line:
        score += 1
    if re.search(r"\d", line):
        score += 1
    if len(line) >= 14:
        score += 1
    return score


def extract_requirements(raw: str):
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    candidates = [ln for ln in lines if not is_noise_line(ln)]

    # 优先使用高置信度可测需求行。
    scored = [(ln, line_score(ln)) for ln in candidates]
    strong = [ln for ln, s in scored if s >= 3]

    if strong:
        items = strong
    else:
        # 回退：按得分取 Top-N，避免把文档标题当作需求。
        top = sorted(scored, key=lambda x: x[1], reverse=True)
        items = [ln for ln, s in top if s > 0][:30]

    if not items:
        items = candidates[:20]

    # 去重但保持顺序
    deduped = []
    seen = set()
    for it in items:
        if it not in seen:
            seen.add(it)
            deduped.append(it)
    items = deduped

    results = []
    for i, text in enumerate(items, start=1):
        results.append(
            {
                "requirement_id": f"REQ-{i:03d}",
                "source_text": text,
                "category": classify(text),
            }
        )
    return results


def main():
    ap = argparse.ArgumentParser(description="从需求文档中抽取可测试需求")
    ap.add_argument("input", help="需求文档路径（.md/.txt）")
    ap.add_argument("-o", "--output", default="requirements.json", help="输出 JSON 路径")
    args = ap.parse_args()

    text = Path(args.input).read_text(encoding="utf-8", errors="ignore")
    reqs = extract_requirements(text)
    Path(args.output).write_text(
        json.dumps({"requirements": reqs}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"已抽取 {len(reqs)} 条需求 -> {args.output}")


if __name__ == "__main__":
    main()
