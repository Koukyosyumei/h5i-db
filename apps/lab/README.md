# lab

A local research ledger for one person running many agents against h5i-db.
The kernel in `kernel/` is the protocol. The server in `server/` is the shell:
PostgreSQL via h5i-app, an outbox worker that calls the h5i-db service API,
and a fleet page.

```
apps/lab/demo.sh        # PostgreSQL in Docker, synthetic market, three scripted agents
apps/lab/demo.sh down
```

## Strategies

An agent submits a strategy as text in a small rule language, a short name,
and JSON parameters:

```
# moving-average crossover, long only
universe ALFA DLTA
let fast = sma($fast)
let slow = sma($slow)
long when fast > slow
flat when fast < slow
size $units
```

with `{"fast": 12, "slow": 72, "units": 150}`. On each book snapshot of an
instrument in `universe`, the program reads that instrument's mid-price
history (`mid`, `spread`, `position`, `sma`, `ema`, `std`, `zscore`, `ret`,
`high`, `low` of a lookback in snapshots, arithmetic, comparisons, `and`,
`or`, `not`). `long when` sets the target to `size` units, `flat when` to
zero, and a market order closes the gap. The grammar is in
`server/src/strategy.rs`. The server parses a request before the ledger
sees it and stores the parameters in one canonical text.

The worker runs the program inside the h5i-db backtest engine, through
`Handle::backtest` on the agent's fork, at the requested decision time. In
h5i-db the trial's code identity is `blake3:<hash of the code text>`.

Why a rule language interpreted in Rust, not Python in a subprocess: the
program can only read what the engine's replay hands it. It cannot open a
file, a socket, or a process, so the decision-time ceiling and the sealed
holdout hold for it without a sandbox, the same way for advisory and
enforced agents. Every run goes through the service handle, so h5i-db writes
the `bt_trial` row whether the run succeeds or the engine rejects it. A
Python strategy would need the box (which fails closed on hosts without the
process tier) and a second, scoped reader that the backtest service does not
have. What the language gives up is generality: no state beyond the price
history, long-only spot positions, one rule set per program.

## Records

Hypotheses, method names, strategy code (with its name), parameters, and
agent names are `Text` rows in the ledger. The family, pending, trial, and
agent rows name them by id. A text is written once: a second submission of
the same body gets the first row's id, and no command rewrites one
(`intern_names_the_text`). An agent freezes a trial it already ran: `freeze`
takes a trial id and copies that trial's code, parameters, and data version
(`freeze_requires_a_counted_trial`). The worker's measurements of a trial are
a `Curve` row written with the trial.

The reported score is the deflated Sharpe ratio (Bailey and López de Prado).
The worker computes it from the family's trial count and each trial's return
statistics — per-period Sharpe, skew, kurtosis, and the number of simple
returns — and stores the probability as an integer in millionths
(`corrected`). The kernel does not evaluate that formula. It guarantees that
`Report` returns only that stored integer and the trial count, that a family
has no raw-best field, and that only the worker's `RecordTrial` writes the
integer. The leaderboard and the review queue are built from those two
numbers. A trial's raw score stays on the trial row and on the lineage table.
It is not a rank.

What the kernel does not guarantee: that the stored integer is the deflated
Sharpe of the equity curve. That arithmetic, the normal cdf, and the
selection hurdle live in `server/src/deflated.rs`. The curve is the
account as the engine holds it during the run: at each book snapshot, cash
plus every position at the engine's mark (`ctx.cash`, `ctx.mark`), one
point per instant. The run's `bt_equity` rows hold the same account, but
once several runs share a fork the table comes back in time order, and a
run's rows cannot be told from another's. Fills and costs (commissions plus
the half spread paid against the mid) come from the engine's fills. The
worker stores return, maximum drawdown, annualised Sharpe, fills, costs,
win rate of round trips, and a 64-point sparkline as the trial's `Curve`. A
replayed delivery (the engine returns the stored receipt and does not run
the strategy) or an engine rejection is recorded as no returns (Sharpe 0,
kurtosis 3, zero samples). That trial still counts, and the family's
corrected score is recomputed with it included. The page shows the
correction as a percentage. The API field stays the integer.

## Trust

An agent enrolled with `sandbox = 0` is **advisory**. The API refuses a query
that asks for a sealed holdout or for a decision time past the latest one
that agent registered, and the agent cannot record its own trial. A process
with a shell can still open the data directory.

An agent enrolled with `sandbox = 1` is **enforced**. The runner starts it
with `h5i box run` (`LAB_BOX`, `LAB_BOX_DIR`). The box policy must omit the
data directory from `fs.read`, so the file open fails as well. The runner
never falls back to a plain shell. An explicit process box fails closed when
`h5i box probe` reports that the process tier cannot execute on the host.

The process tier always `unshare`s `CLONE_NEWUSER` before it executes a
command. Capability bits are not enough. `h5i box probe` and
`h5i box capabilities` can report Landlock, user namespaces, and seccomp as
present, and `claim process satisfiable = yes`, while
`process tier runnable = no`. The functional check runs `true` in the
sandbox. On Ubuntu 24.04 with AppArmor enabled,
`kernel.unprivileged_userns_clone = 1`, and
`kernel.apparmor_restrict_unprivileged_userns = 1`, that `unshare` returns
`EPERM` (os error 13) for an unprivileged user. The host requirement is an
AppArmor policy that allows this user's unprivileged user namespaces: either
`apparmor_restrict_unprivileged_userns = 0`, or a profile on `h5i` with the
`userns` flag. Those are host settings. The workspace tier is not a
substitute. It does not deny the data directory. While the process tier is
not runnable, `h5i box create --isolation process` fails and the runner does
not start the agent.

## Delivery

The fork is created on the first trial, not at enrollment. A trial effect runs
a backtest on that fork. When the engine rejects the run (for example a
program that does not parse, or no instruments catalog), the worker records
a trial with no returns and files a `backtest` alert, and the database still
gets a `bt_trial` row for that run. Reconcile matches the two. The attempt counts,
and the outbox does not retry a rejection. A delivery that fails until it is
marked dead stays dead. A human puts those rows back on the queue with
`POST /api/outbox/replay` (the alerts panel does this). The attempt count
resets, and the same idempotency key delivers the first result again.
A promote publishes only tables the fork owns. Pins are the base it reads. A fork that never wrote has nothing to
publish; the ledger still records the human's promotion.

Starting an agent puts a one-day token for that agent in `LAB_TOKEN` and
runs it in its own process group. Pause, resume, and stop signal that
group (`SIGSTOP`, `SIGCONT`, `SIGKILL`). Review comments, rejections, and
alerts are ledger rows (`Comment`, `Reject`, `NoteAlert`), so they are still
there after a restart. Only the human can reject, comment, or promote. A
rejected family stays rejected, and promote conflicts. The worker (not an
agent) files alerts. A review comment is visible to the owning agent at
`GET /api/comments`. The review row shows the frozen code, parameters, and
data version.

The holdout is one seal (`LAB_SEAL`, default `holdout`) over the market data
table the engine replays (`LAB_TABLE`, `book_deltas` for book data). The
first family submitted to it gets the one grant: the worker runs the frozen
program over the sealed span in memory, records the summary in h5i-db's
evaluation record, and the family passes when the run ended above its
starting cash after costs. Every later family gets `holdout_used` and an
alert; the next candidate needs a new seal.

## Prove

```
export H5I_LAKE_CACHE=/var/tmp/h5i-lake-cache
h5i app doctor
h5i app prove
```

`h5i app check` rejects `sorry`, `native_decide`, and any axiom beyond
`propext`, `Classical.choice`, and `Quot.sound`.

## Run

`DATABASE_URL` is a PostgreSQL URL. `H5I_APP_SECRET` signs bearer tokens.
`H5I_DB_PATH` is the h5i-db directory. `LISTEN` defaults to `127.0.0.1:8201`.
The database needs the backtest's market data: an `instruments` catalog and
book snapshots (`h5i_db_backtest::store`, or the venue loaders), and a seal
over the holdout span.

```
H5I_APP_ISSUE=1:0:1 cargo run --release -p lab-server
```

prints a one-day human token (`org:role:id`, role 0 human, 1 agent, 2 worker).

## Demo

`demo.sh` builds the server, starts PostgreSQL in a container
(`h5idb-demo-pg` on port 5470), writes 180 days of hourly book snapshots for
four synthetic instruments with `lab-demo-data` (`server/src/market.rs`),
seals the last 30, starts the lab on `127.0.0.1:8202`, enrolls three
scripted agents (`demo/agent.py`), and starts them through the runner. The
agents pre-register, request trials, freeze one, and one submits to the
holdout. The breakout agent asks for data past its decision time, which is
refused and filed as an alert. The script then acts as the human: it
promotes the family whose holdout passed and comments on another. It prints
where the human token is; paste it into the page. `DEMO_DIR`,
`DEMO_PG_NAME`, `DEMO_PG_PORT`, and `DEMO_LISTEN` override the defaults.

## Theme

The fleet page uses the Harbor Dark palette (background `#1B1B1B`, foreground
`#efebdc`, accent `#e75a50`, and the rest of the ANSI ramp) as CSS variables.
Those values are the theme's `colors.toml`. The theme is MIT, copyright 2025
HANCORE:

https://github.com/HANCORE-linux/omarchy-harbordark-theme

```
MIT License

Copyright (c) 2025 HANCORE

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
