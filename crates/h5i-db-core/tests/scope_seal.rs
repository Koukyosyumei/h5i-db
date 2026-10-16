//! Scope, seals, and holdout. These are the engine halves of the service
//! boundary: a ceiling hides later event time, a seal is readable only through
//! begin_holdout, and a repeated promote returns the first result.

use std::sync::Arc;

use arrow::array::{Float64Array, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::{
    Database, Error, HoldoutOpen, HoldoutRequest, ReadAt, ScanOptions, SealRequest, TableOptions,
    WriteOptions,
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("price", DataType::Float64, false),
    ]))
}

fn batch(ts: &[i64], price: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts.to_vec())),
            Arc::new(Float64Array::from(price.to_vec())),
        ],
    )
    .unwrap()
}

fn options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".into()),
        ..Default::default()
    }
}

async fn setup() -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("db")).await.unwrap();
    db.create_table("trades", schema(), options())
        .await
        .unwrap();
    (dir, db)
}

fn prices(batches: &[RecordBatch]) -> Vec<f64> {
    let mut out = Vec::new();
    for batch in batches {
        let col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        out.extend(col.values());
    }
    out
}

async fn read_prices(db: &Database) -> Vec<f64> {
    let (batches, _) = db
        .scan("trades", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    prices(&batches)
}

#[tokio::test]
async fn a_ceiling_hides_later_rows_and_prunes_a_segment_that_is_wholly_after() {
    let (_dir, db) = setup().await;
    db.append(
        "trades",
        vec![batch(&[1, 2], &[1.0, 2.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    db.append(
        "trades",
        vec![batch(&[100], &[9.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let scoped = db.with_event_ceiling(50);
    let (batches, report) = scoped
        .scan("trades", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(prices(&batches), vec![1.0, 2.0]);
    assert!(
        report.segments_pruned >= 1,
        "a segment wholly after the ceiling is not read: {report:?}"
    );
    // The unscoped handle is unchanged.
    assert_eq!(read_prices(&db).await, vec![1.0, 2.0, 9.0]);
}

#[tokio::test]
async fn a_sealed_span_is_hidden_until_a_holdout_grant_and_only_once() {
    let (dir, db) = setup().await;
    db.append(
        "trades",
        vec![batch(&[1, 10, 15, 30], &[1.0, 10.0, 15.0, 30.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    db.seal_range(SealRequest {
        id: "final".into(),
        table: "trades".into(),
        start_ns: 10,
        end_ns: 20,
        note: Some("holdout".into()),
        idempotency_key: Some("seal-1".into()),
        idempotency_fingerprint: Some("fp-seal".into()),
    })
    .await
    .unwrap();

    assert_eq!(read_prices(&db).await, vec![1.0, 30.0]);

    // A new open sees the seal too. The cache is loaded from the objects.
    let reopened = Database::open(&dir.path().join("db")).await.unwrap();
    assert_eq!(read_prices(&reopened).await, vec![1.0, 30.0]);

    let request = HoldoutRequest {
        seal_id: "final".into(),
        idempotency_key: "eval-1".into(),
        fingerprint: "strategy-a".into(),
        code_identity: "strategy-a".into(),
        parameters: "{}".into(),
        data_version: "v1".into(),
    };
    let HoldoutOpen::Session(session) = db.begin_holdout(request.clone()).await.unwrap() else {
        panic!("first evaluation must grant a read");
    };
    assert_eq!(
        read_prices(session.database()).await,
        vec![1.0, 10.0, 15.0, 30.0]
    );
    let record = session
        .finish(serde_json::json!({"sharpe": 1.2}))
        .await
        .unwrap();
    assert!(record.is_completed());
    assert_eq!(record.summary.unwrap()["sharpe"], 1.2);

    // The same key returns the summary and does not grant another read.
    match db.begin_holdout(request).await.unwrap() {
        HoldoutOpen::Completed(again) => {
            assert_eq!(again.summary.unwrap()["sharpe"], 1.2);
        }
        HoldoutOpen::Session(_) => panic!("a completed evaluation must not grant a second read"),
    }

    let other = HoldoutRequest {
        seal_id: "final".into(),
        idempotency_key: "eval-2".into(),
        fingerprint: "strategy-b".into(),
        code_identity: "strategy-b".into(),
        parameters: "{}".into(),
        data_version: "v1".into(),
    };
    let err = db.begin_holdout(other).await.unwrap_err();
    assert!(matches!(err, Error::HoldoutUsed { .. }), "{err}");
    assert_eq!(err.code(), "holdout_used");
    assert!(!err.retryable());
    // The refusal reads nothing: the ordinary scan is still sealed.
    assert_eq!(read_prices(&db).await, vec![1.0, 30.0]);
}

#[tokio::test]
async fn resealing_with_the_same_fingerprint_replays_and_a_different_one_conflicts() {
    let (_dir, db) = setup().await;
    db.append("trades", vec![batch(&[1], &[1.0])], WriteOptions::default())
        .await
        .unwrap();
    let request = SealRequest {
        id: "s".into(),
        table: "trades".into(),
        start_ns: 0,
        end_ns: 5,
        note: None,
        idempotency_key: Some("k".into()),
        idempotency_fingerprint: Some("a".into()),
    };
    db.seal_range(request.clone()).await.unwrap();
    db.seal_range(request).await.unwrap();
    let mut changed = SealRequest {
        id: "s".into(),
        table: "trades".into(),
        start_ns: 0,
        end_ns: 9,
        note: None,
        idempotency_key: Some("k".into()),
        idempotency_fingerprint: Some("b".into()),
    };
    let err = db.seal_range(changed.clone()).await.unwrap_err();
    assert!(matches!(err, Error::IdempotencyConflict { .. }), "{err}");
    // The original range is what stays sealed.
    changed.idempotency_fingerprint = Some("a".into());
    changed.end_ns = 5;
    db.seal_range(changed).await.unwrap();
}

#[tokio::test]
async fn an_integer_time_column_fails_closed_when_a_ceiling_is_set() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("db")).await.unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
    db.create_table(
        "t",
        schema,
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)])),
        vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
    )
    .unwrap();
    db.append("t", vec![batch], WriteOptions::default())
        .await
        .unwrap();
    let err = db
        .with_event_ceiling(10)
        .scan("t", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), "invalid_input");
}

#[tokio::test]
async fn a_fork_does_not_read_another_forks_rows() {
    let (_dir, db) = setup().await;
    db.append("trades", vec![batch(&[1], &[1.0])], WriteOptions::default())
        .await
        .unwrap();
    db.create_fork("a", None, None, Default::default())
        .await
        .unwrap();
    db.create_fork("b", None, None, Default::default())
        .await
        .unwrap();
    let a = db.open_fork("a").await.unwrap();
    a.append("trades", vec![batch(&[2], &[2.0])], WriteOptions::default())
        .await
        .unwrap();
    let b = db.open_fork("b").await.unwrap();
    assert_eq!(read_prices(&b).await, vec![1.0]);
    assert_eq!(read_prices(&db).await, vec![1.0]);
    assert_eq!(read_prices(&a).await, vec![1.0, 2.0]);
}

#[tokio::test]
async fn promoting_the_same_fork_table_twice_returns_the_first_result() {
    let (_dir, db) = setup().await;
    db.append("trades", vec![batch(&[1], &[1.0])], WriteOptions::default())
        .await
        .unwrap();
    db.create_fork("agent", None, None, Default::default())
        .await
        .unwrap();
    let fork = db.open_fork("agent").await.unwrap();
    fork.append("trades", vec![batch(&[2], &[2.0])], WriteOptions::default())
        .await
        .unwrap();
    let first = db.promote("agent", "trades").await.unwrap();
    let second = db.promote("agent", "trades").await.unwrap();
    assert_eq!(second.base_sequence, first.base_sequence);
    assert_eq!(second.segments_linked, 0);
    assert_eq!(read_prices(&db).await, vec![1.0, 2.0]);

    // A later write on the base is a real conflict, not a replay.
    db.append("trades", vec![batch(&[3], &[3.0])], WriteOptions::default())
        .await
        .unwrap();
    let err = db.promote("agent", "trades").await.unwrap_err();
    assert!(matches!(err, Error::PromoteConflict { .. }), "{err}");
}

#[tokio::test]
async fn promoting_a_created_table_twice_returns_the_first_result() {
    let (_dir, db) = setup().await;
    db.create_fork("agent", None, None, Default::default())
        .await
        .unwrap();
    let fork = db.open_fork("agent").await.unwrap();
    fork.create_table("features", schema(), options())
        .await
        .unwrap();
    fork.write(
        "features",
        vec![batch(&[1], &[4.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let first = db.promote("agent", "features").await.unwrap();
    let second = db.promote("agent", "features").await.unwrap();
    assert_eq!(second.base_sequence, first.base_sequence);
    assert_eq!(second.segments_linked, 0);
    let (batches, _) = db
        .scan("features", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(prices(&batches), vec![4.0]);
    // The table left the fork.
    assert!(
        fork.resolve("features", ReadAt::Latest).await.is_err(),
        "a promoted created table is no longer on the fork"
    );
}

#[tokio::test]
async fn create_fork_at_version_pins_that_version() {
    let (_dir, db) = setup().await;
    db.append("trades", vec![batch(&[1], &[1.0])], WriteOptions::default())
        .await
        .unwrap();
    db.append("trades", vec![batch(&[2], &[2.0])], WriteOptions::default())
        .await
        .unwrap();
    db.create_fork_at("past", None, ReadAt::Version(1), Default::default())
        .await
        .unwrap();
    let past = db.open_fork("past").await.unwrap();
    assert_eq!(read_prices(&past).await, vec![1.0]);
    assert_eq!(read_prices(&db).await, vec![1.0, 2.0]);
}
