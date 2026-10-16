# lab

A local research ledger for one person running many agents against one
h5i-db database. Agents submit strategies, a worker backtests each one on
the agent's fork, and a web page shows the review queue, leaderboard,
trials and alerts. Only the human can promote, reject or comment.

## Demo

```
apps/lab/demo.sh        # start on 127.0.0.1:8202
apps/lab/demo.sh down   # stop and remove the PostgreSQL container
```

The script needs Docker. It starts PostgreSQL, writes 180 days of hourly
book snapshots for four synthetic instruments, seals the last 30 days as the
holdout, and runs three scripted agents (`demo/agent.py`). It prints where
the human token is; paste that into the page. `DEMO_DIR`, `DEMO_PG_NAME`,
`DEMO_PG_PORT` and `DEMO_LISTEN` override the defaults.

## Run on your own data

The h5i-db database needs an `instruments` catalog, book snapshots (from
`h5i_db_backtest::store` or the venue loaders) and a seal over the holdout
span.

| Variable | Meaning |
|---|---|
| `DATABASE_URL` | PostgreSQL URL for the ledger |
| `H5I_APP_SECRET` | key that signs bearer tokens |
| `H5I_DB_PATH` | h5i-db directory |
| `LISTEN` | address, default `127.0.0.1:8201` |
| `LAB_TABLE` | market data table the engine replays, `book_deltas` for book data |
| `LAB_SEAL` | holdout seal name, default `holdout` |
| `LAB_BOX`, `LAB_BOX_DIR` | box settings for enforced agents (see Trust) |

```
H5I_APP_ISSUE=1:0:1 cargo run --release -p lab-server
```

This prints a one-day human token (`org:role:id`; role 0 is human, 1 agent,
2 worker).

## Strategies

An agent submits a short program in a rule language plus JSON parameters:

```
# moving-average crossover, long only
universe ALFA DLTA
let fast = sma($fast)
let slow = sma($slow)
long when fast > slow
flat when fast < slow
size $units
```

with `{"fast": 12, "slow": 72, "units": 150}`. On each book snapshot the
program can read `mid`, `spread`, `position`, and `sma`, `ema`, `std`,
`zscore`, `ret`, `high`, `low` over a lookback, with arithmetic, comparisons
and `and`/`or`/`not`. `long when` sets the target to `size` units, `flat
when` sets it to zero, and a market order closes the gap. Positions are
long-only spot. The grammar is in `server/src/strategy.rs`.

The program cannot open files, sockets or processes, so it sees only what
the engine replays: nothing past the decision time and nothing in the sealed
holdout.

## Scoring and the holdout

Every run counts as a trial, including ones the engine rejects. Families are
ranked by deflated Sharpe (Bailey and López de Prado), which falls as a
family runs more trials. Raw returns stay on each trial and are never a
rank. The formula is in `server/src/deflated.rs`.

A seal grants its holdout once. The first family submitted runs its frozen
program over the sealed span and passes if it ends above its starting cash
after costs. Later families get `holdout_used` and an alert; the next
candidate needs a new seal.

Failed deliveries can be retried from the alerts panel
(`POST /api/outbox/replay`).

## Trust

An agent enrolled with `sandbox = 0` is advisory. The API refuses its
queries for the holdout or past its decision time, but a process with a
shell can still open the data directory.

An agent enrolled with `sandbox = 1` is enforced. The runner starts it with
`h5i box run`, and the box policy must leave the data directory out of
`fs.read`. If the process tier cannot run on the host, the agent does not
start. On Ubuntu 24.04 with AppArmor this usually means unprivileged user
namespaces are blocked: set `kernel.apparmor_restrict_unprivileged_userns = 0`
or give `h5i` an AppArmor profile with the `userns` flag. Check with
`h5i box probe` (look for `process tier runnable = yes`).

## Proofs

The protocol is an h5i-app kernel in `kernel/`, with Lean proofs in
`proofs/`.

```
export H5I_LAKE_CACHE=/var/tmp/h5i-lake-cache
h5i app doctor
h5i app prove
```

`h5i app check` rejects `sorry`, `native_decide`, and any axiom beyond
`propext`, `Classical.choice` and `Quot.sound`.

The page's colors come from the Harbor Dark theme (MIT); see
[THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).
