# Why h5i-db

What the design gives agents, and where h5i-db is the wrong tool.

## Why for agents

- **Reproducible inputs:** every read resolves to a version, so "which data did
this run see" has an answer, and re-running against that version is O(1) rather
than an archaeology project.
- **Don't let a result destroy the context window.** `H5I_DB_PROFILE=agent` caps
every query and spills the rest to Parquet, reporting the true row count and
where the withheld rows live.
- **Errors that can be acted on:** the stderr envelope carries `next_actions`
(runnable commands), `did_you_mean` for typos, and a `retryable` flag.
- **Branch without copying.** `fork` opens a writable workspace over a pinned
view of every table and duplicates no data, so an edit or an experiment costs
one small file and is as cheap to discard as to keep.
- **Privilege control.** Mutations preview through `plan`/`apply` and policy can
require that gate; `--idempotency-key` makes a retried ingest replay; an opt-in 
`data-policy` rejects malformed rows fail-closed.
- **A backtest run is a branch.** Each run executes inside its own
fork and writes its orders, fills, positions and equity curve there as ordinary
tables. So two runs diff at fill level with `fork_diff`, a whole sweep aggregates
in one cross-fork query, the one worth keeping is `promote`d and the rest are
dropped.
- **In-terminal notebooks, for the reasons quants already use Jupyter.** Partial
execution, a kernel that holds state between steps, results recorded beside the
code that made them. An agent gets all three from the command line, through
`nb exec` and `nb run --cells 3-7`.
- **The review surface routes attention rather than ranking.** `h5i-db ui` orders
trials by what needs a human next: decision required, then failed or warned, then
finished and unseen, then running, then seen. Scanning a list does not mark work
reviewed; a trial counts as seen only when its detail is opened.

## When *not* to use h5i-db

- **Distributed, multi-terabyte warehouses:** single-node and embedded by
  design. Reach for ClickHouse, Snowflake or a lakehouse.
- **OLTP or high-concurrency serving:** one writer at a time, no row-level
  MVCC, no interactive transactions. Use Postgres.
- **Sub-microsecond tick capture:** the write cadence this is built for is
  minute bars, end-of-day, and vendor files, not the capture layer itself.
  That is kdb+ territory.
- **Databases with no time column:** the whole design assumes a time index;
  without one you lose pruning, the ASOF join, and point-in-time reads.
- **Live trading:** the backtester never routes a real order. There are no
  brokerage adapters, no portfolio optimiser and no plotting API; the boundary
  is simulation and evaluation.
