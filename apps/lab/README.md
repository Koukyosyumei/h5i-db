# lab

A local research ledger for one person running many agents against h5i-db.
The kernel in `kernel/` is the protocol. The server in `server/` is the shell:
PostgreSQL via h5i-app, an outbox worker that calls the h5i-db service API,
and a fleet page.

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
selection hurdle live in `server/src/deflated.rs`. The worker reads
`bt_equity` in scan order after the run (not ordered by simulated time,
because a later run can restart the clock), takes the rows appended by this
trial, and refuses a scan that is truncated or longer than 100,000 points.
A missing curve, a refused scan, or an engine rejection is recorded as no
returns (Sharpe 0, kurtosis 3, zero samples). That trial still counts, and
the family's corrected score is recomputed with it included. The page shows
`corrected / 1000000` to four decimals. The API field stays the integer.

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

## Delivery

The fork is created on the first trial, not at enrollment. A trial effect runs
a backtest on that fork. When the engine rejects the run, the worker records
a score of zero and a trial with no returns, so the attempt still counts and
the outbox does not retry a rejection. A promote publishes only tables the
fork owns. Pins are the base it reads. A fork that never wrote has nothing to
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

```
H5I_APP_ISSUE=1:0:1 cargo run -p lab-server
```

prints a one-day human token (`org:role:id`, role 0 human, 1 agent, 2 worker).

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
