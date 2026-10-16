//! Per-trial numbers for the page, from one run's equity curve and fills.
//!
//! The worker stores the result as the trial's curve row (JSON). Every number
//! is read off what the engine produced: the `bt_equity` rows this run
//! appended, and the fills the engine reported to the strategy.

use crate::strategy::Tally;

/// Points kept for the sparkline. The endpoints are always kept.
pub const SPARK_POINTS: usize = 64;

const YEAR_NS: f64 = 365.25 * 86_400.0 * 1e9;

/// Largest peak-to-trough fall, as a negative fraction (0 when it never fell).
pub fn max_drawdown(curve: &[f64]) -> f64 {
    let mut peak = f64::NEG_INFINITY;
    let mut worst: f64 = 0.0;
    for &point in curve {
        if !point.is_finite() {
            continue;
        }
        peak = peak.max(point);
        if peak > 0.0 {
            worst = worst.min(point / peak - 1.0);
        }
    }
    worst
}

/// Even-spaced sample of a curve, endpoints kept.
pub fn downsample(curve: &[f64], points: usize) -> Vec<f64> {
    if curve.len() <= points || points < 2 {
        return curve.to_vec();
    }
    let last = curve.len() - 1;
    (0..points)
        .map(|index| curve[index * last / (points - 1)])
        .collect()
}

/// Annualised Sharpe from simple returns between samples, scaled by the
/// median spacing of the samples. `None` with fewer than three returns or no
/// variation.
pub fn annual_sharpe(points: &[(i64, f64)]) -> Option<(f64, i64)> {
    let mut returns = Vec::new();
    let mut gaps = Vec::new();
    for pair in points.windows(2) {
        let (t0, v0) = pair[0];
        let (t1, v1) = pair[1];
        let gap = t1 - t0;
        if v0 == 0.0 || gap <= 0 {
            continue;
        }
        let step = v1 / v0 - 1.0;
        if step.is_finite() {
            returns.push(step);
            gaps.push(gap);
        }
    }
    if returns.len() < 3 {
        return None;
    }
    gaps.sort_unstable();
    let interval = gaps[gaps.len() / 2];
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let sd = (returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0)).sqrt();
    if sd == 0.0 || interval <= 0 {
        return None;
    }
    Some((mean / sd * (YEAR_NS / interval as f64).sqrt(), interval))
}

fn round(value: f64, places: i32) -> f64 {
    let scale = 10f64.powi(places);
    (value * scale).round() / scale
}

/// The JSON the worker stores for a trial that ran.
pub fn measurement(
    starting_cash: f64,
    points: &[(i64, f64)],
    tally: &Tally,
    identity: &serde_json::Value,
) -> serde_json::Value {
    let curve: Vec<f64> = points.iter().map(|(_, equity)| *equity).collect();
    let last = curve.last().copied().unwrap_or(starting_cash);
    let net = last - starting_cash;
    let mut out = serde_json::json!({
        "starting_cash": starting_cash,
        "final_equity": round(last, 2),
        "net": round(net, 2),
        "costs": round(tally.costs, 2),
        "commissions": round(tally.commissions, 2),
        "gross": round(net + tally.costs, 2),
        "return": round(net / starting_cash, 6),
        "max_drawdown": round(max_drawdown(&curve), 6),
        "fills": tally.fills,
        "orders": tally.orders,
        "round_trips": tally.round_trips,
        "equity_points": curve.len(),
        "equity": downsample(&curve, SPARK_POINTS).into_iter().map(|v| round(v, 2)).collect::<Vec<_>>(),
    });
    if tally.round_trips > 0 {
        out["win_rate"] = serde_json::json!(round(tally.wins as f64 / tally.round_trips as f64, 4));
    }
    if let Some((sharpe, interval)) = annual_sharpe(points) {
        out["sharpe"] = serde_json::json!(round(sharpe, 4));
        out["sample_interval_ns"] = serde_json::json!(interval);
    }
    if let (Some(first), Some(end)) = (points.first(), points.last()) {
        out["first_ns"] = serde_json::json!(first.0);
        out["last_ns"] = serde_json::json!(end.0);
    }
    if let serde_json::Value::Object(extra) = identity {
        for (key, value) in extra {
            out[key] = value.clone();
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drawdown_is_the_worst_fall_from_a_peak() {
        assert_eq!(
            max_drawdown(&[100.0, 120.0, 90.0, 130.0, 117.0]),
            90.0 / 120.0 - 1.0
        );
        assert_eq!(max_drawdown(&[1.0, 2.0, 3.0]), 0.0);
    }

    #[test]
    fn the_sparkline_keeps_both_ends() {
        let curve: Vec<f64> = (0..1000).map(f64::from).collect();
        let small = downsample(&curve, 64);
        assert_eq!(small.len(), 64);
        assert_eq!(small[0], 0.0);
        assert_eq!(*small.last().unwrap(), 999.0);
    }

    #[test]
    fn a_measurement_comes_from_the_curve_and_the_fills() {
        let hour = 3_600_000_000_000i64;
        let points: Vec<(i64, f64)> = [100.0, 101.0, 99.0, 103.0, 104.0]
            .iter()
            .enumerate()
            .map(|(i, v)| (i as i64 * hour, *v * 1000.0))
            .collect();
        let tally = Tally {
            fills: 4,
            orders: 4,
            costs: 50.0,
            commissions: 0.0,
            round_trips: 2,
            wins: 1,
        };
        let out = measurement(
            100_000.0,
            &points,
            &tally,
            &serde_json::json!({"rejected": null}),
        );
        assert_eq!(out["net"], 4000.0);
        assert_eq!(out["gross"], 4050.0);
        assert_eq!(out["return"], 0.04);
        assert_eq!(out["fills"], 4);
        assert_eq!(out["win_rate"], 0.5);
        assert!(out["sharpe"].as_f64().unwrap() > 0.0);
        assert!(out["max_drawdown"].as_f64().unwrap() < 0.0);
    }
}
