//! Idempotency-key tests (ROADMAP VI-A5).
//!
//! Agents retry. When the failure is ambiguous — a timeout that may or may not
//! have committed — a blind retry appends the same rows twice, and duplicated
//! ticks are silent poison: nothing errors, the data is simply wrong from then
//! on. The key makes the retry find the commit it already produced.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use h5i_db_core::{Database, TableOptions, WriteOptions};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

fn batch(ts: &[i64], price: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ts.to_vec())),
            Arc::new(Float64Array::from(price.to_vec())),
        ],
    )
    .unwrap()
}

fn keyed(key: &str) -> WriteOptions {
    WriteOptions {
        idempotency_key: Some(key.to_string()),
        ..Default::default()
    }
}

async fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table(
        "trades",
        schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    (dir, db)
}

async fn row_count(db: &Database) -> u64 {
    db.resolve("trades", h5i_db_core::ReadAt::Latest)
        .await
        .unwrap()
        .manifest
        .rows
}

#[tokio::test]
async fn a_repeated_key_returns_the_original_commit_and_writes_nothing() {
    let (_dir, db) = setup().await;
    let first = db
        .append(
            "trades",
            vec![batch(&[1, 2], &[10.0, 20.0])],
            keyed("ingest-42"),
        )
        .await
        .unwrap();
    assert_eq!(row_count(&db).await, 2);

    // The retry after an ambiguous failure: same key, same rows.
    let replay = db
        .append(
            "trades",
            vec![batch(&[1, 2], &[10.0, 20.0])],
            keyed("ingest-42"),
        )
        .await
        .unwrap();

    assert_eq!(replay.sequence, first.sequence, "must not create a version");
    assert_eq!(row_count(&db).await, 2, "the rows must not be duplicated");
    assert_eq!(
        replay.segments_added, 0,
        "a replay added nothing and must not claim otherwise"
    );
}

#[tokio::test]
async fn a_retry_that_would_otherwise_be_rejected_still_replays() {
    let (_dir, db) = setup().await;
    // The nastiest case: the first attempt landed, so a blind retry of the
    // same ordered append would now be *out of order* and fail. With the key
    // the retry is recognised for what it is and succeeds idempotently.
    let first = db
        .append("trades", vec![batch(&[1, 2], &[10.0, 20.0])], keyed("k1"))
        .await
        .unwrap();
    let blind = db
        .append(
            "trades",
            vec![batch(&[1, 2], &[10.0, 20.0])],
            WriteOptions::default(),
        )
        .await;
    assert!(blind.is_err(), "a blind retry is rejected as out of order");

    let replay = db
        .append("trades", vec![batch(&[1, 2], &[10.0, 20.0])], keyed("k1"))
        .await
        .unwrap();
    assert_eq!(replay.sequence, first.sequence);
}

#[tokio::test]
async fn different_keys_are_different_writes() {
    let (_dir, db) = setup().await;
    db.append("trades", vec![batch(&[1], &[10.0])], keyed("a"))
        .await
        .unwrap();
    db.append("trades", vec![batch(&[2], &[20.0])], keyed("b"))
        .await
        .unwrap();
    assert_eq!(row_count(&db).await, 2);

    // And an unkeyed write is never deduplicated against a keyed one.
    db.append(
        "trades",
        vec![batch(&[3], &[30.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(row_count(&db).await, 3);
}

#[tokio::test]
async fn the_key_survives_intervening_commits() {
    let (_dir, db) = setup().await;
    let first = db
        .append("trades", vec![batch(&[1], &[10.0])], keyed("early"))
        .await
        .unwrap();
    // Other work lands in between, so the match is no longer at the head.
    for i in 2..10i64 {
        db.append(
            "trades",
            vec![batch(&[i], &[i as f64])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    let rows_before = row_count(&db).await;

    let replay = db
        .append("trades", vec![batch(&[1], &[10.0])], keyed("early"))
        .await
        .unwrap();
    assert_eq!(replay.sequence, first.sequence);
    assert_eq!(row_count(&db).await, rows_before);
}

#[tokio::test]
async fn range_mutations_are_guarded_too() {
    let (_dir, db) = setup().await;
    db.append(
        "trades",
        vec![batch(&[1, 2, 3], &[10.0, 20.0, 30.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let first = db
        .delete_range("trades", 1, 2, keyed("cleanup-7"))
        .await
        .unwrap();
    let after = row_count(&db).await;
    assert_eq!(after, 2);

    // Replaying a deletion must not delete a second range.
    let replay = db
        .delete_range("trades", 1, 2, keyed("cleanup-7"))
        .await
        .unwrap();
    assert_eq!(replay.sequence, first.sequence);
    assert_eq!(row_count(&db).await, after);
}

#[tokio::test]
async fn the_key_is_recorded_on_the_commit_it_created() {
    let (_dir, db) = setup().await;
    db.append("trades", vec![batch(&[1], &[10.0])], keyed("visible"))
        .await
        .unwrap();
    // Recorded on the version chain, which is the only record that survives
    // the crash this guard exists for.
    let manifest = db
        .resolve("trades", h5i_db_core::ReadAt::Latest)
        .await
        .unwrap()
        .manifest;
    assert_eq!(
        manifest
            .user_meta
            .get(h5i_db_core::IDEMPOTENCY_META_KEY)
            .and_then(|v| v.as_str()),
        Some("visible")
    );
}

fn fingerprinted(key: &str, fingerprint: &str) -> WriteOptions {
    WriteOptions {
        idempotency_key: Some(key.to_string()),
        idempotency_fingerprint: Some(fingerprint.to_string()),
        ..Default::default()
    }
}

#[tokio::test]
async fn the_same_key_and_fingerprint_replay_and_a_different_fingerprint_conflicts() {
    let (_dir, db) = setup().await;
    let first = db
        .append(
            "trades",
            vec![batch(&[1], &[10.0])],
            fingerprinted("k", "fp-a"),
        )
        .await
        .unwrap();
    let replay = db
        .append(
            "trades",
            vec![batch(&[1], &[10.0])],
            fingerprinted("k", "fp-a"),
        )
        .await
        .unwrap();
    assert_eq!(replay.sequence, first.sequence);
    assert_eq!(row_count(&db).await, 1);

    let err = db
        .append(
            "trades",
            vec![batch(&[9], &[90.0])],
            fingerprinted("k", "fp-b"),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, h5i_db_core::Error::IdempotencyConflict { .. }),
        "{err}"
    );
    assert!(!err.retryable());
    assert_eq!(err.code(), "idempotency_conflict");
    assert_eq!(row_count(&db).await, 1);
}

#[tokio::test]
async fn a_fingerprint_does_not_match_a_key_that_recorded_none() {
    let (_dir, db) = setup().await;
    db.append("trades", vec![batch(&[1], &[10.0])], keyed("legacy"))
        .await
        .unwrap();
    let err = db
        .append(
            "trades",
            vec![batch(&[1], &[10.0])],
            fingerprinted("legacy", "later"),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, h5i_db_core::Error::IdempotencyConflict { .. }),
        "{err}"
    );
    assert_eq!(row_count(&db).await, 1);
}

#[tokio::test]
async fn a_fork_does_not_replay_a_key_that_landed_on_the_base() {
    let (_dir, db) = setup().await;
    db.append("trades", vec![batch(&[1], &[10.0])], keyed("shared"))
        .await
        .unwrap();
    let base_rows = row_count(&db).await;
    db.create_fork("agent", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let fork = db.open_fork("agent").await.unwrap();
    let written = fork
        .append("trades", vec![batch(&[2], &[20.0])], keyed("shared"))
        .await
        .unwrap();
    assert_eq!(written.rows_total, base_rows + 1);
    assert_eq!(row_count(&db).await, base_rows);
    let replay = fork
        .append("trades", vec![batch(&[2], &[20.0])], keyed("shared"))
        .await
        .unwrap();
    assert_eq!(replay.sequence, written.sequence);
    assert_eq!(row_count(&fork).await, base_rows + 1);
}
