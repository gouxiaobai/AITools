# 参数治理SOP

## 目标
确保参数调整可追溯、可灰度、可回滚，避免未验证参数直接全量发布。

## 标准流程
1. 运行 `param-recommend` 生成参数提案与实验ID。
2. 运行 `param-diff` 完成人工编辑与风险检查。
3. 运行 `param-apply` 时绑定 `experiment-id`，并启用发布闸门阈值。
4. 灰度观察后全量；异常时执行 `param-rollback`。
5. 用 `param-monitor` 复盘发布成功率、冲突率、回滚率。

## 发布闸门建议阈值
- `gate-min-stability >= 0.30`
- `gate-min-hit-rate >= 0.45`
- `gate-max-dd-mean <= 0.20`

## 审计字段
- `experiment_id`
- `batch_id`
- `rollout_scope`
- `apply_log_id`
- `rollback_ref`
