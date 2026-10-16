<p align="center">
  <img src="./docs/_static/logo.png" alt="h5i-db logo" height="126">
</p>

<p align="center">
  <a href="https://github.com/h5i-dev/h5i-db/actions/workflows/ci.yml"><img alt="tests" src="https://github.com/h5i-dev/h5i-db/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://pepy.tech/projects/h5i-db"><img alt="downloads" src="https://static.pepy.tech/personalized-badge/h5i-db?period=total&units=INTERNATIONAL_SYSTEM&left_color=GRAY&right_color=GREEN&left_text=downloads"></a>
  <a href="https://github.com/h5i-dev/h5i-db/blob/main/LICENSE"><img alt="Apache-2.0" src="https://img.shields.io/github/license/h5i-dev/h5i-db?color=blue"></a>
</p>

<p align="center"><b>English</b> · <a href="README.es.md">Español</a> · <a href="README.fr.md">Français</a> · <a href="README.zh-CN.md">简体中文</a> · <a href="README.ja.md">日本語</a></p>

<h1 align="center">Backtesting on Versioned Market Data</h1>

**h5i-db** is an embedded time-series database and event-driven backtester for quant research, written in Rust. Use it from Python, and review what your agents ran in a local web UI.

- **Backtest from Python:** `pip install h5i-db`, no server. Replays 3.05M events/s, and every run is pinned to a data version, so it can be re-executed and verified.
- **No lookahead by construction:** pin a decision time, and no row from after it can reach your frame.
- **Cheap experiments:** each run executes in its own fork of the database. A parameter sweep is one fork per trial, ranked out of sample, with no data copied.
- **Review in the browser:** `h5i-db ui` puts trials, forks, versions and pending writes on one page, ordered by what needs a human next.

```bash
pip install h5i-db                              # Python library: database + backtester
cargo install h5i-db-cli                        # CLI and web UI
h5i-db ui market.db                             # review surface on 127.0.0.1:7351
npx skills add h5i-dev/h5i-db                   # agent skill (Claude Code, Codex, Cursor, …)
```

---

## 1. Backtest in Python

```python
import h5i_db
from h5i_db import backtest

db = h5i_db.Database("market.db")

config = backtest.BacktestConfig(
    run_id="momentum-001",
    data=backtest.DataConfig(signals="signals", snapshot="2024-q1"),   # the pin
    portfolio=backtest.PortfolioConfig(starting_cash=100_000.0),
    execution=backtest.ExecutionConfig(fee_kind="kalshi", fee_rate=0.07),
    risk=backtest.RiskConfig(max_order_quantity=500.0),
)

backtest.inspect(db, config).raise_for_errors()  # refuse claims the data can't support
result = backtest.execute(db, config)            # replays inside fork "bt-momentum-001"

result.summary()                  # fills, final cash, how far it actually simulated
result.explain()                  # why orders were rejected or never filled
result.fills                      # Arrow, or just SQL it: SELECT * FROM bt_fills
result.tearsheet("run.html")
result.verify()                   # re-execute the stored config and compare
```

A parameter grid becomes one fork per trial. Give it train and holdout windows, and the leaderboard is read out of sample:

```python
board = backtest.study(
    db, study_id="fees", base=config,
    parameters={"execution.fee_rate": [0.0, 0.02, 0.07]},
    validation=backtest.ValidationWindows(
        train=("2024-01-01", "2024-04-01"), holdout=("2024-04-01", "2024-07-01")
    ),
).leaderboard("holdout_final_cash")
```

- **Venues:** Kalshi, Polymarket, Hyperliquid, Binance, Limitless, Opinion, Manifold, and any OHLCV or trade dump. Loaders read files you already have; see the [venue guide](crates/h5i-db-venues/README.md).
- **Statistics:** factor and performance metrics at `alphalens` and `empyrical` parity, plus deflated Sharpe and probability of backtest overfitting.
- **Data in and out:** `db.append`, `db.sql(...)`, time-series SQL (ASOF join, `time_bucket`, `vwap`, rolling windows), and any past version through `db.read(..., version=n)`.

`python examples/backtest_report_demo.py` writes a self-contained HTML report:

<p align="center">
  <img src="./docs/_static/report-screenshot-1.png" width="49%" />
  <img src="./docs/_static/report-screenshot-2.png" width="49%" />
</p>

---

## 2. Web UI

```bash
h5i-db ui market.db                     # loopback only, read-only by default
h5i-db ui market.db --allow-mutations   # also apply or discard pending plans
```

<p align="center">
  <img src="./docs/_static/backtest-ui.png" alt="h5i-db review UI: an experiment's leaderboard of backtest trials" width="99%" />
</p>

- **Experiments:** trials grouped by study, with leaderboard, by-parameter and compare views. Attention order is: needs a decision, then failed or warned, then finished and unseen, then running. A trial counts as seen only when you open it.
- **Forks:** a live tree of every fork, showing what it wrote, whether it is ahead of its base, and whether a promote would conflict.
- **History and plans:** the version timeline with audit badges, version diffs, and agent writes waiting as previewable plans.
- **SQL:** a scratchpad that reports how many segments each query pruned.

---

## 3. Documentation

- [Manual](https://db.h5i.dev/manual/): [Backtesting](https://db.h5i.dev/manual/backtest/), [Quant](https://db.h5i.dev/manual/quant/), [CLI](https://db.h5i.dev/manual/cli/), [Notebooks](https://db.h5i.dev/manual/notebooks/)
- [Python API](https://db.h5i.dev/api/) · [Cookbook](https://github.com/h5i-dev/h5i-db-cookbook) · [Agent skill](skills/h5i-db/SKILL.md)
- [Benchmarks](docs/BENCHMARKS.md): against DuckDB, Polars, ArcticDB, NautilusTrader and LEAN
- [Why h5i-db](docs/OVERVIEW.md): what it gives agents, and when not to use it
- [Development](docs/DEVELOPMENT.md) · [Design](DESIGN.md) · [Operations](docs/OPERATIONS.md) · [Roadmap](docs/ROADMAP.md)

---

## 4. License

Apache-2.0. See [LICENSE](./LICENSE).
