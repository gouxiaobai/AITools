# 研究实验SOP

## 目标
让每次参数推荐和选股结果都能复现，避免“调参不可解释”。

## 实验流程
1. 设定 `start-date/end-date`、策略范围、市场范围。
2. 运行 `param-recommend`，自动创建或绑定 `experiment_id`。
3. 检查实验报告中的窗口、成本、滑点、稳定性。
4. 运行 `select-stock` 输出候选与调仓建议。
5. 以基线对照评估收益、回撤、命中率、换手。

## 必填实验元数据
- `experiment_id`
- `source_start_date` / `source_end_date`
- `train_window` / `valid_window`
- `walk_forward_splits`
- `cost_bps` / `slippage_bps`

## 输出要求
- 同一输入数据与同一参数，结果必须可复现。
- 发布时必须可追溯到实验ID。
