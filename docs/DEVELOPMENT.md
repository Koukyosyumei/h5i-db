# Development


```bash
# One package at a time with capped threads: several suites start DataFusion
# sessions and tokio runtimes, so running the whole workspace at once can
# exhaust memory on a small machine.
for pkg in h5i-db-core h5i-db-query h5i-db-backtest h5i-db-cli h5i-db-ui; do
  cargo test -p $pkg -- --test-threads=2
done
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
cargo run -p h5i-db-bench --profile bench-fast --bin h5i-db-fork-bench
python3 benchmarks/backtest_compare/run.py \
  --output benchmarks/backtest_compare/results.json   # vs NautilusTrader and LEAN
```

Workspace crates under `crates/`: `core` (versioned storage kernel), `query`
(DataFusion layer), `backtest` (replay kernel, venue models, settlement),
`venues` (Kalshi, Polymarket, Hyperliquid loaders), `cli` (the agent-facing
binary), `ui` (review surface), `observability`, `python`
(`pip install h5i-db`), `bench`.
