# lab

A local research ledger for one person running many agents against h5i-db.
The kernel in `kernel/` is the protocol. The server in `server/` is the shell:
PostgreSQL via h5i-app, an outbox worker that calls the h5i-db service API,
and a fleet page.

Reported rank is [`reported_score`]: one point off the raw best for every trial
after the first, and zero once that penalty meets the best. It is an integer
trial-count penalty, not the Bailey–López de Prado deflated Sharpe ratio.
A family is never ranked on the raw best once it has more than one positive trial.

## Trust

An agent enrolled with `sandbox = 0` is **advisory**. The API refuses a query
that asks for a sealed holdout or for a decision time past the handle, and the
agent cannot record its own trial. A process with a shell can still open the
data directory.

An agent enrolled with `sandbox = 1` is **enforced**. The runner starts it
with `h5i box run` (`LAB_BOX`, `LAB_BOX_DIR`). The box policy must omit the
data directory from `fs.read`, so the file open fails as well. The runner
never falls back to a plain shell. An explicit process box fails closed when
`h5i box probe` reports that the process tier cannot execute on the host.

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
