<p align="center">
  <img src="./docs/_static/logo.png" alt="h5i-db logo" height="126">
</p>

<p align="center">
  <a href="https://github.com/h5i-dev/h5i-db/actions/workflows/ci.yml"><img alt="tests" src="https://github.com/h5i-dev/h5i-db/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://pepy.tech/projects/h5i-db"><img alt="downloads" src="https://static.pepy.tech/personalized-badge/h5i-db?period=total&units=INTERNATIONAL_SYSTEM&left_color=GRAY&right_color=GREEN&left_text=downloads"></a>
  <a href="https://github.com/h5i-dev/h5i-db/blob/main/LICENSE"><img alt="Apache-2.0" src="https://img.shields.io/github/license/h5i-dev/h5i-db?color=blue"></a>
</p>

<p align="center"><a href="README.md">English</a> · <b>Español</b> · <a href="README.fr.md">Français</a> · <a href="README.zh-CN.md">简体中文</a> · <a href="README.ja.md">日本語</a></p>

<h1 align="center">Backtesting sobre datos de mercado versionados</h1>

**h5i-db** es una base de datos de series temporales embebida y un backtester orientado a eventos para la investigación cuantitativa, escritos en Rust. Se usa desde Python, y lo que ejecutaron tus agentes se revisa en una interfaz web local.

- **Backtesting desde Python:** `pip install h5i-db`, sin servidor. Reproduce 3,05 M de eventos/s, y cada ejecución queda fijada a una versión de los datos, así que se puede volver a ejecutar y verificar.
- **Sin sesgo de anticipación, por construcción:** fija un instante de decisión y ninguna fila posterior podrá llegar a tu marco de datos.
- **Experimentos baratos:** cada ejecución corre en su propio fork de la base de datos. Un barrido de parámetros es un fork por prueba, clasificado fuera de muestra, sin copiar datos.
- **Revisión en el navegador:** `h5i-db ui` reúne pruebas, forks, versiones y escrituras pendientes en una sola página, ordenadas según lo que necesita a una persona primero.

```bash
pip install h5i-db                              # Python library: database + backtester
cargo install h5i-db-cli                        # CLI and web UI
h5i-db ui market.db                             # review surface on 127.0.0.1:7351
npx skills add h5i-dev/h5i-db                   # agent skill (Claude Code, Codex, Cursor, …)
```

---

## 1. Backtesting en Python

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

Una malla de parámetros se convierte en un fork por prueba. Dale ventanas de entrenamiento y de validación, y la clasificación se lee fuera de muestra:

```python
board = backtest.study(
    db, study_id="fees", base=config,
    parameters={"execution.fee_rate": [0.0, 0.02, 0.07]},
    validation=backtest.ValidationWindows(
        train=("2024-01-01", "2024-04-01"), holdout=("2024-04-01", "2024-07-01")
    ),
).leaderboard("holdout_final_cash")
```

- **Mercados:** Kalshi, Polymarket, Hyperliquid, Binance, Limitless, Opinion, Manifold, y cualquier volcado OHLCV o de operaciones. Los cargadores leen archivos que ya tienes; consulta la [guía de mercados](crates/h5i-db-venues/README.md).
- **Estadística:** métricas de factores y de rendimiento con paridad `alphalens` y `empyrical`, además de Sharpe deflactado y probabilidad de sobreajuste del backtest.
- **Entrada y salida de datos:** `db.append`, `db.sql(...)`, SQL de series temporales (ASOF join, `time_bucket`, `vwap`, ventanas móviles), y cualquier versión pasada con `db.read(..., version=n)`.

`python examples/backtest_report_demo.py` genera un informe HTML autocontenido:

<p align="center">
  <img src="./docs/_static/report-screenshot-1.png" width="49%" />
  <img src="./docs/_static/report-screenshot-2.png" width="49%" />
</p>

---

## 2. Laboratorio de investigación

`apps/lab` es un registro para una persona que ejecuta muchos agentes sobre una misma base de datos. Un agente envía una estrategia como un programa corto de reglas con parámetros JSON. Un worker la somete a backtest en el fork del agente a través de la API de servicio y registra la prueba tanto si la ejecución salió bien como si no. El protocolo es un kernel de h5i-app con pruebas en Lean: cada prueba se cuenta, un agente no puede leer más allá de su instante de decisión ni dentro del holdout sellado, el holdout se evalúa una sola vez y solo una persona hace promote.

```bash
apps/lab/demo.sh         # PostgreSQL in Docker, synthetic market, three scripted agents, 127.0.0.1:8202
apps/lab/demo.sh down
```

<p align="center">
  <img src="./docs/_static/lab-ui.png" alt="h5i-db research lab: review queue, leaderboard and trials" width="99%" />
</p>

- **Cola de revisión:** estrategias congeladas con su código, parámetros, curva de capital, costes y resultado del holdout, para hacer promote, rechazarlas o comentarlas.
- **Clasificación:** familias ordenadas por Sharpe deflactado, que baja cuantas más variantes prueba una familia. Los rendimientos en bruto se quedan en las pruebas.
- **Pruebas:** cada ejecución contada con rendimiento, drawdown máximo, Sharpe, ejecuciones, costes y una minigráfica del capital, todo a partir de las ejecuciones del motor.
- **Alertas:** lecturas más allá de un instante de decisión, backtests rechazados, un holdout ya usado.

Con tus propios datos, el laboratorio necesita PostgreSQL y una base de datos h5i-db con un catálogo de instrumentos, instantáneas del libro de órdenes y un sello sobre el periodo de holdout. [apps/lab/README.md](apps/lab/README.md) describe la configuración, el lenguaje de estrategias y lo que cubren las pruebas.

---

## 3. Documentación

- [Manual](https://db.h5i.dev/manual/): [Backtesting](https://db.h5i.dev/manual/backtest/), [Cuantitativa](https://db.h5i.dev/manual/quant/), [CLI](https://db.h5i.dev/manual/cli/), [Notebooks](https://db.h5i.dev/manual/notebooks/)
- [API de Python](https://db.h5i.dev/api/) · [Recetario](https://github.com/h5i-dev/h5i-db-cookbook) · [Skill para agentes](skills/h5i-db/SKILL.md)
- [Benchmarks](docs/BENCHMARKS.md): frente a DuckDB, Polars, ArcticDB, NautilusTrader y LEAN
- [Por qué h5i-db](docs/OVERVIEW.md): qué aporta a los agentes, y cuándo no usarlo
- [Desarrollo](docs/DEVELOPMENT.md) · [Diseño](DESIGN.md) · [Operaciones](docs/OPERATIONS.md) · [Hoja de ruta](docs/ROADMAP.md)

---

## 4. Licencia

Apache-2.0. Véase [LICENSE](./LICENSE).
