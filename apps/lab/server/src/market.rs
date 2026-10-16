//! Synthetic market data for the demo and the tests.
//!
//! Hourly book snapshots for a few spot instruments, from a seeded generator,
//! written with the backtest crate's own store functions, plus the
//! instruments catalog the engine needs. Nothing here is a result: the demo's
//! numbers come from backtests run over this data.

use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, OutcomeId};
use h5i_db_backtest::store;
use h5i_db_backtest::types::{Price, Qty, Stamps, UnixNanos};
use h5i_db_core::Database;

pub const HOUR_NS: i64 = 3_600_000_000_000;
pub const DAY_NS: i64 = 24 * HOUR_NS;
/// 2025-01-01T00:00:00Z.
pub const START_NS: i64 = 1_735_689_600_000_000_000;
pub const VENUE: &str = "demo";

/// How one instrument's mid-price moves, per hour.
#[derive(Clone, Copy, Debug)]
pub enum Process {
    /// Log returns with a drift that changes sign every `regime_days`, and
    /// some carry-over from the previous hour.
    Trend {
        drift: f64,
        regime_days: u32,
        carry: f64,
        vol: f64,
    },
    /// Pulled back towards `level` at rate `pull` per hour.
    Revert { level: f64, pull: f64, vol: f64 },
}

#[derive(Clone, Debug)]
pub struct Series {
    pub id: &'static str,
    pub start: f64,
    /// Quoted spread, in basis points of the mid.
    pub spread_bps: f64,
    pub process: Process,
}

/// The demo's instruments.
pub fn demo_series() -> Vec<Series> {
    vec![
        Series {
            id: "ALFA",
            start: 120.0,
            spread_bps: 4.0,
            process: Process::Trend {
                drift: 0.00012,
                regime_days: 41,
                carry: 0.03,
                vol: 0.006,
            },
        },
        Series {
            id: "BRVO",
            start: 48.0,
            spread_bps: 6.0,
            process: Process::Revert {
                level: 50.0,
                pull: 0.012,
                vol: 0.006,
            },
        },
        Series {
            id: "CHRL",
            start: 75.0,
            spread_bps: 5.0,
            process: Process::Trend {
                drift: 0.0,
                regime_days: 30,
                carry: 0.0,
                vol: 0.006,
            },
        },
        Series {
            id: "DLTA",
            start: 210.0,
            spread_bps: 3.0,
            process: Process::Trend {
                drift: 0.0001,
                regime_days: 53,
                carry: 0.03,
                vol: 0.005,
            },
        },
    ]
}

/// xorshift64*: deterministic, no dependency.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn uniform(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }

    /// Standard normal, Box-Muller.
    fn normal(&mut self) -> f64 {
        let u = self.uniform();
        let v = self.uniform();
        (-2.0 * u.ln()).sqrt() * (2.0 * std::f64::consts::PI * v).cos()
    }
}

/// Hourly mid-prices of one series.
pub fn mids(series: &Series, hours: usize, seed: u64) -> Vec<f64> {
    let mut rng = Rng(seed | 1);
    let mut price = series.start;
    let mut last = 0.0;
    let mut out = Vec::with_capacity(hours);
    for hour in 0..hours {
        let step = match series.process {
            Process::Trend {
                drift,
                regime_days,
                carry,
                vol,
            } => {
                let regime = (hour / (regime_days as usize * 24)) % 2;
                let sign = if regime == 0 { 1.0 } else { -0.6 };
                drift * sign + carry * last + vol * rng.normal()
            }
            Process::Revert { level, pull, vol } => {
                pull * (level.ln() - price.ln()) + vol * rng.normal()
            }
        };
        last = step;
        price *= step.exp();
        out.push(price);
    }
    out
}

fn price(value: f64) -> Result<Price, String> {
    Price::from_f64((value * 100.0).round() / 100.0).map_err(|e| e.to_string())
}

/// One snapshot: five levels a side, one tick apart, 400 units each.
fn snapshot(id: &InstrumentId, at: i64, mid: f64, spread_bps: f64) -> Result<Record, String> {
    let half = (mid * spread_bps / 20_000.0).max(0.01);
    let size = Qty::from_f64(400.0).map_err(|e| e.to_string())?;
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for level in 0..5 {
        let offset = half + level as f64 * 0.01;
        bids.push((price(mid - offset)?, size));
        asks.push((price(mid + offset)?, size));
    }
    Ok(Record::new(
        Stamps::immediate(UnixNanos::new(at)),
        id.clone(),
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot { bids, asks },
    ))
}

/// Write the catalog and `days` of hourly snapshots for `series`.
/// Returns the first instant after the data.
pub async fn seed(db: &Database, series: &[Series], days: u32, seed: u64) -> Result<i64, String> {
    store::create_market_data_tables(db)
        .await
        .map_err(|e| e.to_string())?;
    let mut catalog = Vec::new();
    for item in series {
        catalog.push(Instrument::spot(item.id, VENUE).map_err(|e| e.to_string())?);
    }
    store::write_instruments(db, &catalog, UnixNanos::new(START_NS - DAY_NS))
        .await
        .map_err(|e| e.to_string())?;
    let hours = days as usize * 24;
    let mut records = Vec::with_capacity(hours * series.len());
    for (index, item) in series.iter().enumerate() {
        let id = InstrumentId::new(item.id).map_err(|e| e.to_string())?;
        let path = mids(item, hours, seed.wrapping_add(index as u64 * 7919));
        for (hour, mid) in path.iter().enumerate() {
            records.push(snapshot(
                &id,
                START_NS + hour as i64 * HOUR_NS,
                *mid,
                item.spread_bps,
            )?);
        }
    }
    records.sort_by_key(|record| record.ts().get());
    store::write_book_events(db, &records)
        .await
        .map_err(|e| e.to_string())?;
    Ok(START_NS + hours as i64 * HOUR_NS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_generator_is_deterministic_and_positive() {
        let series = demo_series();
        let a = mids(&series[0], 500, 3);
        let b = mids(&series[0], 500, 3);
        assert_eq!(a, b);
        assert!(a.iter().all(|p| *p > 0.0));
        let reverting = mids(&series[1], 3000, 5);
        let mean = reverting.iter().sum::<f64>() / reverting.len() as f64;
        assert!((mean - 50.0).abs() < 5.0, "{mean}");
    }
}
