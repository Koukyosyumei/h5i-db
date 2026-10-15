//! SQL, table functions, and planner statistics share one scope.
//!
//! The engine scan is tested in h5i-db-core. These tests are the readers that
//! used to bypass it: DataFusion SQL, joins, windows, `h5i` / `latest_on` /
//! `asof_join`, and a metadata aggregate.

use std::sync::Arc;

use arrow::array::{
    Array, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::{Database, HoldoutOpen, HoldoutRequest, SealRequest, TableOptions, WriteOptions};
use h5i_db_query::{H5iSession, ResearchPin, SessionOptions};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

fn batch(ts: &[i64], symbol: &[&str], price: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts.to_vec()).with_timezone("UTC")),
            Arc::new(StringArray::from(symbol.to_vec())),
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

async fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table("trades", schema(), options())
        .await
        .unwrap();
    db.append(
        "trades",
        vec![batch(
            &[1_000, 2_000, 5_000, 9_000],
            &["AAPL", "AAPL", "AAPL", "AAPL"],
            &[1.0, 2.0, 50.0, 9.0],
        )],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    (dir, db)
}

async fn session(db: &Arc<Database>) -> H5iSession {
    H5iSession::new(Arc::clone(db), SessionOptions::default())
        .await
        .unwrap()
}

async fn f64_col(s: &H5iSession, sql: &str) -> Vec<f64> {
    let batches = s
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        if let Some(col) = column.as_any().downcast_ref::<Float64Array>() {
            out.extend(col.values());
        } else if let Some(col) = column.as_any().downcast_ref::<Int64Array>() {
            out.extend(col.values().iter().map(|v| *v as f64));
        } else if let Some(col) = column.as_any().downcast_ref::<UInt64Array>() {
            out.extend(col.values().iter().map(|v| *v as f64));
        } else {
            panic!("{sql}: unexpected column type {}", column.data_type());
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn decision_time_hides_later_rows_from_sql_joins_windows_and_table_functions() {
    let (_dir, db) = setup().await;
    let s = H5iSession::new_pinned(
        Arc::clone(&db),
        SessionOptions::default(),
        ResearchPin::default().with_event_time_cutoff_ns(3_000),
    )
    .await
    .unwrap();

    assert_eq!(
        f64_col(&s, "SELECT count(*) AS c FROM trades").await,
        vec![2.0]
    );
    assert_eq!(
        f64_col(&s, "SELECT max(price) AS c FROM trades").await,
        vec![2.0]
    );
    // Four unclipped rows of one symbol would make this 16.
    assert_eq!(
        f64_col(
            &s,
            "SELECT count(*) AS c FROM trades a JOIN trades b ON a.symbol = b.symbol"
        )
        .await,
        vec![4.0]
    );
    assert_eq!(
        f64_col(&s, "SELECT max(price) OVER () AS c FROM trades").await,
        vec![2.0, 2.0]
    );
    assert_eq!(
        f64_col(&s, "SELECT count(*) AS c FROM h5i('trades')").await,
        vec![2.0]
    );
    assert_eq!(
        f64_col(
            &s,
            "SELECT max(price) AS c FROM latest_on('trades', 'symbol')"
        )
        .await,
        vec![2.0]
    );
    assert_eq!(
        f64_col(
            &s,
            "SELECT count(*) AS c FROM asof_join('trades', 'trades', 'ts', 'ts', 'symbol')"
        )
        .await,
        vec![2.0]
    );
    // A filter pushed into the scan must not bring the later rows back, and
    // must not hide the rows the ceiling still allows.
    assert_eq!(
        f64_col(&s, "SELECT count(*) AS c FROM trades WHERE price >= 0").await,
        vec![2.0]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sealed_span_stays_hidden_in_sql_including_a_session_opened_before_the_seal() {
    let (_dir, db) = setup().await;
    let s = session(&db).await;
    assert_eq!(
        f64_col(&s, "SELECT max(price) AS c FROM trades").await,
        vec![50.0]
    );

    db.seal_range(SealRequest {
        id: "hold".into(),
        table: "trades".into(),
        start_ns: 3_000,
        end_ns: 8_000,
        note: None,
        idempotency_key: Some("seal-1".into()),
        idempotency_fingerprint: Some("fp-1".into()),
    })
    .await
    .unwrap();

    // No refresh: the provider reads the live seal cache.
    assert_eq!(
        f64_col(&s, "SELECT max(price) AS c FROM trades").await,
        vec![9.0],
        "max(price) must not answer from the sealed row"
    );
    let mut prices = f64_col(&s, "SELECT price AS c FROM trades ORDER BY price").await;
    prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(prices, vec![1.0, 2.0, 9.0]);

    let open = db
        .begin_holdout(HoldoutRequest {
            seal_id: "hold".into(),
            idempotency_key: "eval-1".into(),
            fingerprint: "once".into(),
            code_identity: "test".into(),
            parameters: "{}".into(),
            data_version: "v".into(),
        })
        .await
        .unwrap();
    let HoldoutOpen::Session(granted) = open else {
        panic!("first holdout evaluation should be granted");
    };
    let held = H5iSession::new(
        Arc::new(granted.database().clone()),
        SessionOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        f64_col(&held, "SELECT max(price) AS c FROM trades").await,
        vec![50.0]
    );

    let denied = db
        .begin_holdout(HoldoutRequest {
            seal_id: "hold".into(),
            idempotency_key: "eval-2".into(),
            fingerprint: "twice".into(),
            code_identity: "test".into(),
            parameters: "{}".into(),
            data_version: "v".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(denied.code(), "holdout_used");
    // The ordinary session still cannot see the sealed price.
    assert_eq!(
        f64_col(&s, "SELECT max(price) AS c FROM trades").await,
        vec![9.0]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn forks_on_a_fork_handle_is_refused() {
    let (_dir, db) = setup().await;
    db.create_fork("agent", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let fork = Arc::new(db.open_fork("agent").await.unwrap());
    let s = session(&fork).await;
    let err = s
        .sql("SELECT count(*) FROM forks('trades')")
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("refuses"),
        "a fork handle must not read sibling forks: {msg}"
    );
}
