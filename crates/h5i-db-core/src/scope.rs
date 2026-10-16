//! Read scope: one predicate every reader applies.
//!
//! A scope is a capability on a [`crate::Database`] handle, not a role. It
//! carries an event-time ceiling, extra excluded spans, and at most one
//! holdout grant. Sealed ranges live in the seal cache and are unioned in
//! here at classify time, so a handle opened before a seal still hides it on
//! the next scan.
//!
//! [`ReadScope::classify`] is the enforcement point. Segment pruning, row
//! filters, and planner statistics all consume its [`Visibility`].

use arrow::array::{Array, BooleanArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, TimeUnit};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Half-open event-time span in nanoseconds. `table == None` applies to every
/// table. `end_ns` is exclusive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeSpan {
    pub table: Option<String>,
    pub start_ns: i64,
    pub end_ns: i64,
}

/// The one sealed span a holdout evaluation may read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoldoutGrant {
    pub seal_id: String,
    pub table: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

/// A sealed span as classify sees it. Copied out of the seal cache so this
/// module does not depend on the seal object's on-disk shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealSpan {
    pub id: String,
    pub table: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

/// What a handle is allowed to read.
///
/// Default is unrestricted: no ceiling, no holes, no grant. That value is the
/// hot path — callers check it before building a predicate.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadScope {
    /// Event time after this nanosecond instant is invisible. `None` means
    /// no ceiling.
    pub event_ceiling_ns: Option<i64>,
    /// Extra holes, in addition to the live seal cache.
    pub excluded: Vec<TimeSpan>,
    /// When set, the matching seal is not a hole for this handle.
    pub holdout_grant: Option<HoldoutGrant>,
}

/// Raw-unit visibility for one table. `ceiling_raw` is an inclusive maximum.
/// Each hole is half-open `[start, end)` in the time column's unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Visibility {
    pub ceiling_raw: Option<i64>,
    pub holes: Vec<(i64, i64)>,
}

/// A restrictive scope resolved to one timestamp column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enforcement {
    pub time_column: String,
    pub visibility: Visibility,
}

impl ReadScope {
    /// No ceiling, no caller exclusions, no grant.
    ///
    /// This does not look at seals. The database combines it with the seal
    /// flag for the scan hot path.
    pub fn is_unrestricted(&self) -> bool {
        self.event_ceiling_ns.is_none() && self.excluded.is_empty() && self.holdout_grant.is_none()
    }

    /// Hot path: this handle adds nothing and the database has published no
    /// seal. One flag, no predicate.
    pub fn is_hot_unrestricted(&self, seals_any: bool) -> bool {
        !seals_any && self.is_unrestricted()
    }

    /// The enforcement point.
    ///
    /// `Ok(None)` means this table has nothing to hide, even when some other
    /// table is sealed. `unit` is required only when the table is restricted;
    /// a missing timestamp then fails the read instead of guessing a unit.
    pub fn classify(
        &self,
        table: &str,
        unit: Option<TimeUnit>,
        seals: &[SealSpan],
    ) -> Result<Option<Visibility>> {
        let mut holes_ns: Vec<(i64, i64)> = Vec::new();
        for span in &self.excluded {
            let applies = match &span.table {
                None => true,
                Some(name) => name == table,
            };
            if applies && span.start_ns < span.end_ns {
                holes_ns.push((span.start_ns, span.end_ns));
            }
        }
        for seal in seals {
            if seal.table != table || seal.start_ns >= seal.end_ns {
                continue;
            }
            if self.grant_covers(seal) {
                continue;
            }
            holes_ns.push((seal.start_ns, seal.end_ns));
        }
        if self.event_ceiling_ns.is_none() && holes_ns.is_empty() {
            return Ok(None);
        }
        let Some(unit) = unit else {
            return Err(Error::invalid(format!(
                "table {table:?} has no timestamp time column, so a decision-time ceiling or \
                 sealed range cannot be enforced; give it a timestamp column"
            )));
        };
        let ceiling_raw = match self.event_ceiling_ns {
            Some(ns) => Some(ns_to_unit_floor(ns, unit)?),
            None => None,
        };
        let mut holes = Vec::with_capacity(holes_ns.len());
        for (start, end) in holes_ns {
            let start_raw = ns_to_unit_floor(start, unit)?;
            let end_raw = ns_to_unit_ceil(end, unit)?;
            if start_raw < end_raw {
                holes.push((start_raw, end_raw));
            }
        }
        Ok(Some(Visibility { ceiling_raw, holes }))
    }

    fn grant_covers(&self, seal: &SealSpan) -> bool {
        self.holdout_grant.as_ref().is_some_and(|grant| {
            grant.seal_id == seal.id
                && grant.table == seal.table
                && grant.start_ns == seal.start_ns
                && grant.end_ns == seal.end_ns
        })
    }
}

/// Floor a nanosecond instant into `unit`. Never rounds a ceiling into a
/// later unit, and starts an exclusion early.
pub fn ns_to_unit_floor(ns: i64, unit: TimeUnit) -> Result<i64> {
    let divisor = unit_ns(unit);
    ns.checked_div_euclid(divisor)
        .ok_or_else(|| Error::invalid("time unit conversion overflow"))
}

/// Ceil a nanosecond instant into `unit`. An exclusive seal end rounds up so
/// a partial unit is excluded.
pub fn ns_to_unit_ceil(ns: i64, unit: TimeUnit) -> Result<i64> {
    let divisor = unit_ns(unit);
    let quotient = ns
        .checked_div_euclid(divisor)
        .ok_or_else(|| Error::invalid("time unit conversion overflow"))?;
    let remainder = ns
        .checked_rem_euclid(divisor)
        .ok_or_else(|| Error::invalid("time unit conversion overflow"))?;
    if remainder == 0 {
        Ok(quotient)
    } else {
        quotient
            .checked_add(1)
            .ok_or_else(|| Error::invalid("time unit conversion overflow"))
    }
}

fn unit_ns(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    }
}

/// `true` when every raw value in the inclusive range is invisible.
///
/// A missing range is not hidden: the segment is read and filtered by row.
pub fn segment_hidden(time_range: Option<(i64, i64)>, vis: &Visibility) -> bool {
    let Some((min, max)) = time_range else {
        return false;
    };
    if min > max {
        return false;
    }
    if let Some(ceiling) = vis.ceiling_raw
        && min > ceiling
    {
        return true;
    }
    vis.holes
        .iter()
        .any(|&(start, end)| min >= start && max < end)
}

/// `true` when every raw value in the inclusive range is visible.
///
/// A missing range is not fully visible, so statistics stay inexact.
pub fn segment_fully_visible(time_range: Option<(i64, i64)>, vis: &Visibility) -> bool {
    let Some((min, max)) = time_range else {
        return false;
    };
    if min > max {
        return false;
    }
    if let Some(ceiling) = vis.ceiling_raw
        && max > ceiling
    {
        return false;
    }
    vis.holes
        .iter()
        .all(|&(start, end)| max < start || min >= end)
}

/// Whether one raw timestamp survives the scope.
pub fn row_visible(raw: i64, vis: &Visibility) -> bool {
    if let Some(ceiling) = vis.ceiling_raw
        && raw > ceiling
    {
        return false;
    }
    vis.holes
        .iter()
        .all(|&(start, end)| raw < start || raw >= end)
}

/// Drop rows the scope hides. Idempotent: filtering twice equals filtering once.
///
/// A null in the time column is corruption, the same rule as the write path.
pub fn filter_record_batch(
    batch: &RecordBatch,
    time_col: &str,
    vis: &Visibility,
) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let idx = batch.schema().index_of(time_col).map_err(|_| {
        Error::internal("scope filter requested but time column is not in the batch")
    })?;
    let casted = arrow::compute::cast(batch.column(idx), &DataType::Int64).map_err(Error::Arrow)?;
    let values = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::internal("time column cast to i64 failed"))?;
    if values.null_count() > 0 {
        return Err(Error::corruption(
            time_col,
            "null time value in stored segment (writes reject nulls; segment is corrupt)",
        ));
    }
    let mask: BooleanArray = values
        .values()
        .iter()
        .map(|&raw| Some(row_visible(raw, vis)))
        .collect();
    arrow::compute::filter_record_batch(batch, &mask).map_err(Error::Arrow)
}

/// Timestamp unit of `time_col`, if that column is a timestamp.
pub fn timestamp_unit(schema: &arrow::datatypes::Schema, time_col: &str) -> Option<TimeUnit> {
    let field = schema.field_with_name(time_col).ok()?;
    match field.data_type() {
        DataType::Timestamp(unit, _) => Some(*unit),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::TimestampNanosecondArray;
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn vis(ceiling: Option<i64>, holes: Vec<(i64, i64)>) -> Visibility {
        Visibility {
            ceiling_raw: ceiling,
            holes,
        }
    }

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampNanosecondArray::from(values.to_vec()))],
        )
        .unwrap()
    }

    #[test]
    fn unrestricted_scope_classifies_open_even_with_unrelated_seals() {
        let scope = ReadScope::default();
        let seals = vec![SealSpan {
            id: "h".into(),
            table: "other".into(),
            start_ns: 0,
            end_ns: 10,
        }];
        let got = scope
            .classify("trades", Some(TimeUnit::Nanosecond), &seals)
            .unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn ceiling_and_seal_hide_rows_and_a_grant_reopens_only_its_span() {
        let seal = SealSpan {
            id: "hold".into(),
            table: "trades".into(),
            start_ns: 10,
            end_ns: 20,
        };
        let mut scope = ReadScope {
            event_ceiling_ns: Some(15),
            ..ReadScope::default()
        };
        let vis = scope
            .classify(
                "trades",
                Some(TimeUnit::Nanosecond),
                std::slice::from_ref(&seal),
            )
            .unwrap()
            .unwrap();
        assert_eq!(vis.ceiling_raw, Some(15));
        assert_eq!(vis.holes, vec![(10, 20)]);
        assert!(!row_visible(16, &vis));
        assert!(!row_visible(12, &vis));
        assert!(row_visible(9, &vis));

        scope.holdout_grant = Some(HoldoutGrant {
            seal_id: "hold".into(),
            table: "trades".into(),
            start_ns: 10,
            end_ns: 20,
        });
        let granted = scope
            .classify("trades", Some(TimeUnit::Nanosecond), &[seal])
            .unwrap()
            .unwrap();
        assert!(granted.holes.is_empty());
        assert!(row_visible(12, &granted));
        assert!(!row_visible(16, &granted));
    }

    #[test]
    fn coarser_units_round_the_safe_direction() {
        // 1500 ns on a microsecond column floors to 1. 2500 ns ceils to 3.
        assert_eq!(ns_to_unit_floor(1500, TimeUnit::Microsecond).unwrap(), 1);
        assert_eq!(ns_to_unit_ceil(2500, TimeUnit::Microsecond).unwrap(), 3);
        let scope = ReadScope {
            event_ceiling_ns: Some(1500),
            excluded: vec![TimeSpan {
                table: Some("trades".into()),
                start_ns: 1500,
                end_ns: 2500,
            }],
            holdout_grant: None,
        };
        let vis = scope
            .classify("trades", Some(TimeUnit::Microsecond), &[])
            .unwrap()
            .unwrap();
        assert_eq!(vis.ceiling_raw, Some(1));
        assert_eq!(vis.holes, vec![(1, 3)]);
    }

    #[test]
    fn non_timestamp_fails_closed_only_when_restricted() {
        let scope = ReadScope {
            event_ceiling_ns: Some(1),
            ..ReadScope::default()
        };
        let err = scope.classify("trades", None, &[]).unwrap_err();
        assert_eq!(err.code(), "invalid_input");
        assert!(
            ReadScope::default()
                .classify("trades", None, &[])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn segment_fate_and_row_filter_agree() {
        let vis = vis(Some(10), vec![(3, 5)]);
        assert!(segment_hidden(Some((11, 12)), &vis));
        assert!(segment_hidden(Some((3, 4)), &vis));
        assert!(!segment_hidden(None, &vis));
        assert!(segment_fully_visible(Some((0, 2)), &vis));
        assert!(!segment_fully_visible(Some((4, 6)), &vis));
        assert!(!segment_fully_visible(None, &vis));

        let filtered = filter_record_batch(&batch(&[1, 3, 4, 10, 11]), "ts", &vis).unwrap();
        let col = filtered
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(col.values(), &[1, 10]);
        let again = filter_record_batch(&filtered, "ts", &vis).unwrap();
        assert_eq!(again.num_rows(), filtered.num_rows());
    }

    #[test]
    fn negative_instants_use_euclid() {
        // -1 ns ceils to the next microsecond boundary, which is 0.
        assert_eq!(ns_to_unit_ceil(-1, TimeUnit::Microsecond).unwrap(), 0);
        assert_eq!(ns_to_unit_floor(-1, TimeUnit::Microsecond).unwrap(), -1);
    }

    #[test]
    fn seeded_rows_never_cross_a_ceiling_or_a_hole() {
        let mut state: u64 = 0x5eed_5c0e;
        let mut next = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            state
        };
        for _ in 0..256 {
            let ceiling = next() as i64;
            let use_ceiling = next() % 2 == 0;
            let hole_start = next() as i64;
            let hole_end = hole_start.saturating_add((next() % 1_000) as i64);
            let vis = vis(use_ceiling.then_some(ceiling), vec![(hole_start, hole_end)]);
            for _ in 0..16 {
                let raw = next() as i64;
                let visible = row_visible(raw, &vis);
                if use_ceiling && raw > ceiling {
                    assert!(!visible, "row {raw} passed ceiling {ceiling}");
                }
                if hole_start < hole_end && raw >= hole_start && raw < hole_end {
                    assert!(!visible, "row {raw} passed hole [{hole_start}, {hole_end})");
                }
            }
            let min = next() as i64;
            let max = min.saturating_add((next() % 64) as i64);
            if segment_hidden(Some((min, max)), &vis) {
                assert!(!row_visible(min, &vis));
                assert!(!row_visible(max, &vis));
                let mid = min.saturating_add(max.saturating_sub(min) / 2);
                assert!(!row_visible(mid, &vis));
            }
            if segment_fully_visible(Some((min, max)), &vis) {
                assert!(row_visible(min, &vis));
                assert!(row_visible(max, &vis));
            }
        }
    }
}
