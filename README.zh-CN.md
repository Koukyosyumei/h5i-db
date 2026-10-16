# h5i-db

<a href="https://github.com/h5i-dev/h5i-db/actions/workflows/ci.yml"><img alt="tests" src="https://github.com/h5i-dev/h5i-db/actions/workflows/ci.yml/badge.svg"></a>
<a href="https://pepy.tech/projects/h5i-db"><img alt="downloads" src="https://static.pepy.tech/personalized-badge/h5i-db?period=total&units=INTERNATIONAL_SYSTEM&left_color=GRAY&right_color=GREEN&left_text=downloads"></a>
<a href="https://github.com/h5i-dev/h5i-db/blob/main/LICENSE"><img alt="Apache-2.0" src="https://img.shields.io/github/license/h5i-dev/h5i-db?color=blue"></a>

[English](README.md) · [Español](README.es.md) · [Français](README.fr.md) · **简体中文** · [日本語](README.ja.md)

**h5i-db** 是一个面向量化研究的嵌入式时序数据库和事件驱动回测器。

- **在 Python 里回测：** `pip install h5i-db`，无需服务器。每秒重放 305 万个事件，每次运行都固定在某个数据版本上。
- **从构造上杜绝前视偏差：** 固定一个决策时刻，此后的任何行都进不了你的数据帧。
- **低成本实验：** 每次运行都在数据库自己的 fork 中执行。参数扫描就是每个试验一个 fork，按样本外结果排名，不复制任何数据。
- **在浏览器里审阅：** `h5i-db ui` 把试验、fork、版本和待处理的写入放在同一页上，按“最需要人来处理”的顺序排列。

```bash
pip install h5i-db                              # Python library: database + backtester
cargo install h5i-db-cli                        # CLI and web UI
h5i-db ui market.db                             # review surface on 127.0.0.1:7351
npx skills add h5i-dev/h5i-db                   # agent skill (Claude Code, Codex, Cursor, …)
```

---

## 1. 在 Python 里回测

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

参数网格会变成每个试验一个 fork。给它训练窗口和留出窗口，排行榜就按样本外结果给出：

```python
board = backtest.study(
    db, study_id="fees", base=config,
    parameters={"execution.fee_rate": [0.0, 0.02, 0.07]},
    validation=backtest.ValidationWindows(
        train=("2024-01-01", "2024-04-01"), holdout=("2024-04-01", "2024-07-01")
    ),
).leaderboard("holdout_final_cash")
```

- **交易场所：** Kalshi、Polymarket、Hyperliquid、Binance、Limitless、Opinion、Manifold，以及任何 OHLCV 或成交导出数据。加载器直接读取你手头已有的文件；详见[场所指南](crates/h5i-db-venues/README.md)。
- **统计：** 与 `alphalens`、`empyrical` 对齐的因子与绩效指标，外加 deflated Sharpe 和回测过拟合概率。
- **数据的写入与读取：** `db.append`、`db.sql(...)`、时序 SQL（ASOF join、`time_bucket`、`vwap`、滑动窗口），以及通过 `db.read(..., version=n)` 读取任意历史版本。

---

## 2. 研究实验室

`apps/lab/demo.sh` 会在 127.0.0.1:8202 启动实验室，同时用 Docker 运行 PostgreSQL、生成合成行情并运行三个脚本化的智能体（用 `apps/lab/demo.sh down` 停止）。在自己的数据上运行的方法见 [apps/lab/README.md](apps/lab/README.md)。

- **审阅队列：** 已冻结的策略，附带代码、参数、权益曲线、成本和留出集结果，可以 promote、拒绝或评论。
- **排行榜：** 按收缩夏普比率（deflated Sharpe）给策略族排名；一个策略族尝试的变体越多，这个值越低。原始收益只显示在试验上。
- **试验：** 每次计数的运行，包含收益、最大回撤、夏普比率、成交笔数、成本和权益迷你曲线，全部来自引擎的成交。
- **告警：** 越过决策时间的读取、被拒绝的回测、已用掉的留出集。

<p align="center">
  <img src="./docs/_static/lab-ui.png" alt="h5i-db research lab: review queue, leaderboard and trials" width="99%" />
</p>

---

## 3. 文档

- [手册](https://db.h5i.dev/manual/)：[回测](https://db.h5i.dev/manual/backtest/)、[量化](https://db.h5i.dev/manual/quant/)、[CLI](https://db.h5i.dev/manual/cli/)、[Notebook](https://db.h5i.dev/manual/notebooks/)
- [Python API](https://db.h5i.dev/api/) · [实例集](https://github.com/h5i-dev/h5i-db-cookbook) · [智能体 skill](skills/h5i-db/SKILL.md)
- [基准测试](docs/BENCHMARKS.md)：对比 DuckDB、Polars、ArcticDB、NautilusTrader 和 LEAN
- [为什么选 h5i-db](docs/OVERVIEW.md)：它能为智能体提供什么，以及什么时候不该用它
- [开发](docs/DEVELOPMENT.md) · [设计](DESIGN.md) · [运维](docs/OPERATIONS.md) · [路线图](docs/ROADMAP.md)

---

## 4. 许可证

Apache-2.0，详见 [LICENSE](./LICENSE)。
