<p align="center">
  <img src="./docs/_static/logo.png" alt="h5i-db logo" height="126">
</p>

<p align="center">
  <a href="https://github.com/h5i-dev/h5i-db/actions/workflows/ci.yml"><img alt="tests" src="https://github.com/h5i-dev/h5i-db/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://pepy.tech/projects/h5i-db"><img alt="downloads" src="https://static.pepy.tech/personalized-badge/h5i-db?period=total&units=INTERNATIONAL_SYSTEM&left_color=GRAY&right_color=GREEN&left_text=downloads"></a>
  <a href="https://github.com/h5i-dev/h5i-db/blob/main/LICENSE"><img alt="Apache-2.0" src="https://img.shields.io/github/license/h5i-dev/h5i-db?color=blue"></a>
</p>

<p align="center"><a href="README.md">English</a> · <a href="README.es.md">Español</a> · <b>Français</b> · <a href="README.zh-CN.md">简体中文</a> · <a href="README.ja.md">日本語</a></p>

<h1 align="center">Backtesting sur des données de marché versionnées</h1>

**h5i-db** est une base de données de séries temporelles embarquée et un backtester événementiel pour la recherche quantitative, écrits en Rust. On l'utilise depuis Python, et on relit ce que les agents ont exécuté dans une interface web locale.

- **Backtester depuis Python :** `pip install h5i-db`, sans serveur. Rejoue 3,05 M d'événements/s, et chaque exécution est fixée à une version des données, ce qui permet de la relancer et de la vérifier.
- **Aucun biais d'anticipation, par construction :** fixez un instant de décision, et aucune ligne postérieure ne peut atteindre votre trame.
- **Des expériences peu coûteuses :** chaque exécution tourne dans son propre fork de la base. Un balayage de paramètres, c'est un fork par essai, classé hors échantillon, sans aucune copie de données.
- **Relecture dans le navigateur :** `h5i-db ui` réunit essais, forks, versions et écritures en attente sur une seule page, triés selon ce qui demande d'abord un humain.

```bash
pip install h5i-db                              # Python library: database + backtester
cargo install h5i-db-cli                        # CLI and web UI
h5i-db ui market.db                             # review surface on 127.0.0.1:7351
npx skills add h5i-dev/h5i-db                   # agent skill (Claude Code, Codex, Cursor, …)
```

---

## 1. Backtester en Python

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

Une grille de paramètres devient un fork par essai. Donnez-lui des fenêtres d'entraînement et de validation, et le classement se lit hors échantillon :

```python
board = backtest.study(
    db, study_id="fees", base=config,
    parameters={"execution.fee_rate": [0.0, 0.02, 0.07]},
    validation=backtest.ValidationWindows(
        train=("2024-01-01", "2024-04-01"), holdout=("2024-04-01", "2024-07-01")
    ),
).leaderboard("holdout_final_cash")
```

- **Places :** Kalshi, Polymarket, Hyperliquid, Binance, Limitless, Opinion, Manifold, et tout export OHLCV ou de transactions. Les chargeurs lisent les fichiers que vous avez déjà ; voir le [guide des places](crates/h5i-db-venues/README.md).
- **Statistiques :** métriques de facteurs et de performance à parité `alphalens` et `empyrical`, plus le Sharpe dégonflé et la probabilité de surapprentissage du backtest.
- **Entrée et sortie des données :** `db.append`, `db.sql(...)`, SQL de séries temporelles (jointure ASOF, `time_bucket`, `vwap`, fenêtres glissantes), et toute version passée via `db.read(..., version=n)`.

`python examples/backtest_report_demo.py` produit un rapport HTML autonome :

<p align="center">
  <img src="./docs/_static/report-screenshot-1.png" width="49%" />
  <img src="./docs/_static/report-screenshot-2.png" width="49%" />
</p>

---

## 2. Interface web

```bash
h5i-db ui market.db                     # loopback only, read-only by default
h5i-db ui market.db --allow-mutations   # also apply or discard pending plans
```

<p align="center">
  <img src="./docs/_static/backtest-ui.png" alt="h5i-db review UI: an experiment's leaderboard of backtest trials" width="99%" />
</p>

- **Expériences :** essais regroupés par étude, avec des vues classement, par paramètre et comparaison. L'ordre d'attention est : demande une décision, puis échoué ou avec avertissement, puis terminé et non vu, puis en cours. Un essai ne compte comme vu que lorsque vous l'ouvrez.
- **Forks :** un arbre en direct de tous les forks, qui montre ce que chacun a écrit, s'il est en avance sur sa base, et si un promote entrerait en conflit.
- **Historique et plans :** la chronologie des versions avec badges d'audit, les différences entre versions, et les écritures d'agents en attente sous forme de plans prévisualisables.
- **SQL :** un brouillon qui indique combien de segments chaque requête a écartés.

---

## 3. Documentation

- [Manuel](https://db.h5i.dev/manual/) : [Backtesting](https://db.h5i.dev/manual/backtest/), [Quantitatif](https://db.h5i.dev/manual/quant/), [CLI](https://db.h5i.dev/manual/cli/), [Notebooks](https://db.h5i.dev/manual/notebooks/)
- [API Python](https://db.h5i.dev/api/) · [Livre de recettes](https://github.com/h5i-dev/h5i-db-cookbook) · [Skill pour agents](skills/h5i-db/SKILL.md)
- [Benchmarks](docs/BENCHMARKS.md) : face à DuckDB, Polars, ArcticDB, NautilusTrader et LEAN
- [Pourquoi h5i-db](docs/OVERVIEW.md) : ce qu'il apporte aux agents, et quand ne pas l'utiliser
- [Développement](docs/DEVELOPMENT.md) · [Conception](DESIGN.md) · [Exploitation](docs/OPERATIONS.md) · [Feuille de route](docs/ROADMAP.md)

---

## 4. Licence

Apache-2.0. Voir [LICENSE](./LICENSE).
