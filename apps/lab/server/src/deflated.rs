//! Deflated Sharpe ratio, outside the kernel.
//!
//! Bailey and López de Prado (2014) deflate a selected strategy's Sharpe
//! ratio by the number of trials and by the variance of those trials' Sharpe
//! ratios, then adjust for skew and kurtosis. This module is the only place
//! that formula runs. The kernel stores the resulting integer and the trial
//! count; it does not evaluate it.
//!
//! The Sharpe here is per period, not annualised. Annualising and then
//! applying the formula would count the sample length twice. Kurtosis is the
//! fourth standardised moment (a normal sample is 3). The stored correction
//! is the probability times [`CORRECTED_SCALE`].

/// One trial's return statistics. `samples` is the number of simple returns.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TrialStats {
    pub sharpe: f64,
    pub skew: f64,
    pub kurtosis: f64,
    pub samples: u64,
}

/// `corrected = round(probability * CORRECTED_SCALE)`, in `0..=CORRECTED_SCALE`.
pub const CORRECTED_SCALE: u64 = 1_000_000;

/// Signed statistics are stored as `(value + STAT_OFFSET) * CORRECTED_SCALE`.
/// The kernel does not interpret the encoding.
pub const STAT_OFFSET: f64 = 1_000.0;

const EULER: f64 = 0.5772156649015329;

pub fn no_returns() -> TrialStats {
    TrialStats {
        sharpe: 0.0,
        skew: 0.0,
        kurtosis: 3.0,
        samples: 0,
    }
}

pub fn encode_stat(value: f64) -> u64 {
    let value = if value.is_finite() { value } else { 0.0 };
    let shifted = (value + STAT_OFFSET) * CORRECTED_SCALE as f64;
    if shifted <= 0.0 {
        0
    } else {
        shifted.round().min(u64::MAX as f64) as u64
    }
}

pub fn decode_stat(stored: u64) -> f64 {
    stored as f64 / CORRECTED_SCALE as f64 - STAT_OFFSET
}

/// Simple returns from an equity curve, in scan order.
pub fn stats_from_equity(equity: &[f64]) -> TrialStats {
    let mut returns = Vec::with_capacity(equity.len().saturating_sub(1));
    for pair in equity.windows(2) {
        let previous = pair[0];
        let value = pair[1];
        if previous == 0.0 || !previous.is_finite() || !value.is_finite() {
            continue;
        }
        let step = value / previous - 1.0;
        if step.is_finite() {
            returns.push(step);
        }
    }
    stats_from_returns(&returns)
}

pub fn stats_from_returns(returns: &[f64]) -> TrialStats {
    let count = returns.len();
    if count < 2 {
        return TrialStats {
            sharpe: 0.0,
            skew: 0.0,
            kurtosis: 3.0,
            samples: count as u64,
        };
    }
    let n = count as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let mut m2 = 0.0;
    let mut m3 = 0.0;
    let mut m4 = 0.0;
    for value in returns {
        let delta = value - mean;
        let delta2 = delta * delta;
        m2 += delta2;
        m3 += delta2 * delta;
        m4 += delta2 * delta2;
    }
    let sample_var = m2 / (n - 1.0);
    let sample_std = sample_var.sqrt();
    if sample_std == 0.0 {
        return TrialStats {
            sharpe: 0.0,
            skew: 0.0,
            kurtosis: 3.0,
            samples: count as u64,
        };
    }
    let population_std = (m2 / n).sqrt();
    TrialStats {
        sharpe: mean / sample_std,
        skew: (m3 / n) / population_std.powi(3),
        kurtosis: (m4 / n) / population_std.powi(4),
        samples: count as u64,
    }
}

/// Probability that the selected trial's Sharpe exceeds the multiple-testing
/// hurdle, scaled by [`CORRECTED_SCALE`]. The selected trial is the one with
/// the highest Sharpe. With one trial the hurdle is zero, so this is the
/// probabilistic Sharpe ratio.
pub fn deflated_sharpe(trials: &[TrialStats]) -> u64 {
    let Some(selected) = trials
        .iter()
        .max_by(|left, right| left.sharpe.total_cmp(&right.sharpe))
    else {
        return 0;
    };
    if selected.samples < 2 {
        return 0;
    }
    let sr0 = selection_hurdle(trials);
    let width = 1.0 - selected.skew * selected.sharpe
        + ((selected.kurtosis - 1.0) / 4.0) * selected.sharpe * selected.sharpe;
    if width <= 0.0 {
        return 0;
    }
    let z = (selected.sharpe - sr0) * ((selected.samples - 1) as f64).sqrt() / width.sqrt();
    if !z.is_finite() {
        return 0;
    }
    let probability = norm_cdf(z).clamp(0.0, 1.0);
    (probability * CORRECTED_SCALE as f64).round() as u64
}

fn selection_hurdle(trials: &[TrialStats]) -> f64 {
    let n = trials.len();
    if n < 2 {
        return 0.0;
    }
    let count = n as f64;
    let mean = trials.iter().map(|trial| trial.sharpe).sum::<f64>() / count;
    let variance = trials
        .iter()
        .map(|trial| {
            let delta = trial.sharpe - mean;
            delta * delta
        })
        .sum::<f64>()
        / (count - 1.0);
    if variance <= 0.0 {
        return 0.0;
    }
    let z1 = inv_norm((1.0 - 1.0 / count).clamp(1e-12, 1.0 - 1e-12));
    let z2 = inv_norm((1.0 - 1.0 / (count * std::f64::consts::E)).clamp(1e-12, 1.0 - 1e-12));
    variance.sqrt() * ((1.0 - EULER) * z1 + EULER * z2)
}

/// Abramowitz and Stegun 26.2.17, absolute error about `7.5e-8`.
fn norm_cdf(x: f64) -> f64 {
    if x < 0.0 {
        return 1.0 - norm_cdf(-x);
    }
    let t = 1.0 / (1.0 + 0.2316419 * x);
    let density = 0.3989422804014327 * (-0.5 * x * x).exp();
    let poly = t
        * (0.319381530
            + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
    (1.0 - density * poly).clamp(0.0, 1.0)
}

/// Acklam's rational approximation of the inverse standard normal.
fn inv_norm(p: f64) -> f64 {
    let p = p.clamp(1e-12, 1.0 - 1e-12);
    let a = [
        -3.969683028665376e+01,
        2.209460984245205e+02,
        -2.759285104469687e+02,
        1.383577518672690e+02,
        -3.066479806614716e+01,
        2.506628277459239e+00,
    ];
    let b = [
        -5.447609879822406e+01,
        1.615858368580409e+02,
        -1.556989798598866e+02,
        6.680131188771972e+01,
        -1.328068155288572e+01,
    ];
    let c = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e+00,
        -2.549732539343734e+00,
        4.374664141464968e+00,
        2.938163982698783e+00,
    ];
    let d = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e+00,
        3.754408661907416e+00,
    ];
    let plow = 0.02425;
    let phigh = 1.0 - plow;
    if p < plow {
        let q = (-2.0 * p.ln()).sqrt();
        (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    } else if p > phigh {
        let q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    } else {
        let q = p - 0.5;
        let r = q * q;
        (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
            / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_normal_cdf_and_its_inverse_agree_on_known_points() {
        assert!((norm_cdf(0.0) - 0.5).abs() < 1e-6);
        assert!((norm_cdf(1.959963984540054) - 0.975).abs() < 1e-5);
        assert!((inv_norm(0.5)).abs() < 1e-8);
        assert!((inv_norm(0.975) - 1.959963984540054).abs() < 1e-6);
        assert!((norm_cdf(inv_norm(0.9)) - 0.9).abs() < 1e-5);
    }

    #[test]
    fn a_single_zero_mean_sample_is_a_coin_flip() {
        let returns: Vec<f64> = (0..40)
            .map(|i| if i % 2 == 0 { 0.01 } else { -0.01 })
            .collect();
        let stats = stats_from_returns(&returns);
        assert!(stats.samples >= 2);
        assert!(stats.sharpe.abs() < 1e-9, "{}", stats.sharpe);
        let corrected = deflated_sharpe(&[stats]);
        assert!((corrected as i64 - 500_000).abs() < 2_000, "{corrected}");
    }

    #[test]
    fn more_trials_deflate_the_same_sharpe() {
        let selected = TrialStats {
            sharpe: 0.5,
            skew: 0.0,
            kurtosis: 3.0,
            samples: 100,
        };
        let alone = deflated_sharpe(&[selected]);
        let mut many = vec![selected];
        many.extend(
            std::iter::repeat(TrialStats {
                sharpe: 0.0,
                skew: 0.0,
                kurtosis: 3.0,
                samples: 100,
            })
            .take(20),
        );
        let crowded = deflated_sharpe(&many);
        assert!(alone > 500_000, "{alone}");
        assert!(crowded < alone, "{crowded} vs {alone}");
    }

    #[test]
    fn identical_sharpes_have_no_selection_hurdle() {
        let trial = TrialStats {
            sharpe: 0.4,
            skew: 0.0,
            kurtosis: 3.0,
            samples: 80,
        };
        assert_eq!(
            deflated_sharpe(&[trial]),
            deflated_sharpe(&[trial, trial, trial])
        );
    }

    #[test]
    fn no_returns_do_not_invent_a_score() {
        assert_eq!(deflated_sharpe(&[]), 0);
        assert_eq!(deflated_sharpe(&[no_returns()]), 0);
        assert_eq!(
            encode_stat(decode_stat(encode_stat(-1.25))),
            encode_stat(-1.25)
        );
    }

    #[test]
    fn a_rising_curve_outranks_noise() {
        let rising: Vec<f64> = (0..30).map(|i| 100.0 + i as f64).collect();
        let stats = stats_from_equity(&rising);
        assert!(stats.sharpe > 0.0);
        assert!(deflated_sharpe(&[stats]) > 900_000);
    }
}
