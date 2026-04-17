# Architecture

## Layered Structure

- `src/app/cli.py`: single CLI entry routing.
- `src/core/`: config/env/notion client/property helpers.
- `src/stores/`: SQLite-oriented read/write stores.
- `src/services/`: domain services (recommendation/research/risk/selection).
- `src/commands/`: command handlers per domain.
- `src/stock_pipeline.py`: compatibility facade for legacy imports and CLI execution.
- `src/legacy/stock_pipeline_impl.py`: legacy implementation retained for stepwise migration.

## Recommendation Service Layout

- `services/recommendation/runner.py`: facade orchestrator.
- `services/recommendation/recommend_runner.py`: recommend-prices application flow.
- `services/recommendation/backtest_runner.py`: backtest application flow.
- `services/recommendation/strategy_registry.py`: strategy registry and unified evaluator contract.
- `services/recommendation/signals.py`: concrete strategy implementations and registration.

## Compatibility

- Existing command form remains supported:
  - `python src/stock_pipeline.py <cmd>`
- Existing dashboard workflow remains compatible.
- Symbol re-exports are preserved in `stock_pipeline.py` for transition.

## Migration Rule

New features must be added to `services/` + `commands/`, not to `legacy/stock_pipeline_impl.py`.
Legacy forwarding functions emit `DeprecationWarning` once per function to guide migration.
