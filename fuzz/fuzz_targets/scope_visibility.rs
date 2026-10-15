//! Fuzz the scope predicate. A row after the ceiling, or inside a hole, must
//! stay invisible. Segment pruning has to agree with the row predicate, and
//! filtering a batch twice is the same as filtering it once.
#![no_main]

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use h5i_db_core::{
    HoldoutGrant, ReadScope, SealSpan, TimeSpan, filter_record_batch, row_visible,
    segment_fully_visible, segment_hidden,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 50 {
        return;
    }
    let flags = data[0];
    let unit = match data[1] % 4 {
        0 => TimeUnit::Nanosecond,
        1 => TimeUnit::Microsecond,
        2 => TimeUnit::Millisecond,
        _ => TimeUnit::Second,
    };
    let ceiling = i64::from_le_bytes(data[2..10].try_into().unwrap());
    let start = i64::from_le_bytes(data[10..18].try_into().unwrap());
    let end = i64::from_le_bytes(data[18..26].try_into().unwrap());
    let raw = i64::from_le_bytes(data[26..34].try_into().unwrap());
    let seg_min = i64::from_le_bytes(data[34..42].try_into().unwrap());
    let seg_max = i64::from_le_bytes(data[42..50].try_into().unwrap());

    let scope = ReadScope {
        event_ceiling_ns: (flags & 1 != 0).then_some(ceiling),
        excluded: if flags & 2 != 0 {
            vec![TimeSpan {
                table: None,
                start_ns: start,
                end_ns: end,
            }]
        } else {
            Vec::new()
        },
        holdout_grant: (flags & 4 != 0).then(|| HoldoutGrant {
            seal_id: "s".into(),
            table: "t".into(),
            start_ns: start,
            end_ns: end,
        }),
    };
    let seal = SealSpan {
        id: "s".into(),
        table: "t".into(),
        start_ns: start,
        end_ns: end,
    };
    let Ok(classified) = scope.classify("t", Some(unit), &[seal]) else {
        return;
    };
    let Some(vis) = classified else {
        return;
    };

    if let Some(limit) = vis.ceiling_raw
        && raw > limit
    {
        assert!(!row_visible(raw, &vis), "row {raw} passed ceiling {limit}");
    }
    for &(hole_start, hole_end) in &vis.holes {
        if hole_start < hole_end && raw >= hole_start && raw < hole_end {
            assert!(
                !row_visible(raw, &vis),
                "row {raw} passed hole [{hole_start}, {hole_end})"
            );
        }
    }

    if seg_min <= seg_max {
        if segment_hidden(Some((seg_min, seg_max)), &vis) {
            let mid = seg_min.saturating_add(seg_max.saturating_sub(seg_min) / 2);
            assert!(!row_visible(seg_min, &vis));
            assert!(!row_visible(seg_max, &vis));
            assert!(!row_visible(mid, &vis));
        }
        if segment_fully_visible(Some((seg_min, seg_max)), &vis) {
            assert!(row_visible(seg_min, &vis));
            assert!(row_visible(seg_max, &vis));
        }
    }

    // A matching grant, and nothing else, reopens the sealed span.
    if flags & 0b111 == 0b100 && unit == TimeUnit::Nanosecond && start < end {
        let mid = start.saturating_add(end.saturating_sub(start) / 2);
        assert!(vis.holes.is_empty());
        assert!(row_visible(mid, &vis));
    }
    // The same span with no grant stays closed.
    if flags & 0b111 == 0 && unit == TimeUnit::Nanosecond && start < end {
        let mid = start.saturating_add(end.saturating_sub(start) / 2);
        assert!(!row_visible(mid, &vis));
    }

    let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![raw, seg_min, seg_max]))],
    )
    .expect("batch");
    let Ok(filtered) = filter_record_batch(&batch, "ts", &vis) else {
        return;
    };
    let values = filtered
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64");
    for index in 0..values.len() {
        let value = values.value(index);
        assert!(row_visible(value, &vis));
        if let Some(limit) = vis.ceiling_raw {
            assert!(value <= limit, "filtered row {value} is after {limit}");
        }
    }
    let again = filter_record_batch(&filtered, "ts", &vis).expect("second filter");
    assert_eq!(again.num_rows(), filtered.num_rows());
});
