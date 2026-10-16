# Benchmarks

How h5i-db is built for speed, and how it compares with other engines.

## Why it's fast

- **Manifest pruning:** every version's manifest carries per-segment time
  ranges and column min/max. Narrow queries prune whole segments before a
  single file is opened.
- **Declared sort order:** segments are stored time-sorted and the query
  layer tells DataFusion so. OHLCV rollups stream instead of sorting 20M rows
  first (every baseline pays that sort), and the ASOF join is sort-free.
- **Immutable segments:** footer metadata is cached unconditionally (sound
  because segments never change), cutting ~40% off warm scans.
- **Version-aware aggregate states:** OHLCV/VWAP rollups persist mergeable
  states per immutable segment; re-queries merge states in milliseconds
  instead of recomputing, scanning only newly appended segments.
- **Lazy replay:** the backtest kernel pulls records one at a time instead of
  materializing a window, so memory stays flat whether a run replays a day or a
  hundred million events.
- **Indexed order matching:** resting orders are keyed by market and price, so a
  new print wakes only the orders it actually crosses instead of rescanning
  every open one.

## Results

Full methodology and results in [benchmarks](../benchmarks).

**Database**

| | DuckDB | Polars | pandas | PyArrow | ArcticDB | **h5i-db** |
|---|---|---|---|---|---|---|
| User-facing versioning / time travel | ✗¹ | ✗ | ✗ | ✗ | ✓ | ✓ (O(1) version reads) |
| SQL joins/windows/CTEs | ✓ | partial | ✗ | ✗ | ✗ | ✓ (DataFusion) |
| ASOF join | ✓ | ✓ | ✓ | ✗² | ✗ | ✓⁴ (sort-free on sorted storage) |
| Previewable mutations (plan/apply) | ✗ | ✗ | ✗ | ✗ | ✗ | ✓, policy-enforceable |
| Concurrent writers | MVCC | n/a | n/a | n/a | unsafe³ | CAS + explicit conflict |
| 20M-row narrow time-range scan | 45.5 ms | 28.1 ms | 23.9 ms | 22.8 ms | **4.2 ms**⁵ | 10.0 ms |
| 20M-row 1-min OHLCV+VWAP | 7237 ms | 7309 ms | 5115 ms | 7121 ms | 3504 ms | **1558 ms** |
| 20M-row ASOF join (by symbol) | 11566 ms | **1485 ms** | 6624 ms | ✗² | 7008 ms | 1548 ms |

¹ `AT (VERSION …)` syntax exists but native storage rejects it.
² Experimental `join_asof` exists but is ~1000× slower, impractical at this scale.
³ Documented single-writer-per-symbol assumption.
⁴ Native `ASOF JOIN … MATCH_CONDITION` SQL syntax and an `asof_join(...)`
  table function (SQL and Python).
⁵ ArcticDB's native time index wins narrow point reads from its own LMDB
  store; h5i-db's manifest pruning is second and beats every general engine.

**Backtesting**

| engine | measured boundary | median | throughput |
|---|---|---:|---:|
| **h5i-db** | decoded records through the replay kernel | **65.7 ms** | **3.05 M events/s** |
| h5i-db `wide` | same kernel, 128-bit fixed point | 94 ms⁷ | 2.13 M events/s⁷ |
| **h5i-db** | same kernel, strategy as a Python callback per event | 278 ms⁶ | 719 k events/s⁶ |
| h5i-db `wide` | same, 128-bit fixed point | 306 ms⁶ ⁷ | 653 k events/s⁶ ⁷ |
| **h5i-db** | full persisted run: scan, decode, fork, replay, write | 280 ms | 713 k events/s |
| h5i-db `wide` | same, 128-bit fixed point | 280 ms | 713 k events/s |
| NautilusTrader 1.230.0 | in-memory objects through `BacktestEngine.run()` | 767 ms | 261 k events/s |
| LEAN `11ba019f6` | first `Slice` callback to `OnEndOfAlgorithm`, disk-fed | 2,033 ms | 98.4 k events/s |

Medians of three fresh-process runs after one warm-up; each adapter verifies it
saw all 200k events and submitted all 200 orders. The measured boundaries differ,
as the column says, and the benchmark checks counts rather than PnL equivalence.
⁶ The other rows never call Python; this one crosses into it per event, as
Nautilus does. Callback against callback the gap is 3.1×, not 13×. Derived
(native kernel plus measured boundary cost), not timed directly.
⁷ `--features wide`, off by default; see
[Precision and range](https://db.h5i.dev/manual/backtest/). Derived, not timed
directly; method in [RESULTS.md](../benchmarks/backtest_compare/RESULTS.md).
