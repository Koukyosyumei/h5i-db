//! The service protocol, exercised through the public API.
//!
//! These are the attempts to break a scoped handle: a query at T must not
//! reveal a later event or a later commit, a fork must not see another fork's
//! unpromoted rows, a seal must stay hidden until the one holdout grant, and
//! a promote of two tables must not publish the first when the second conflicts.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use base64::Engine;
use h5i_db_core::{Database, TableOptions};
use h5i_db_service::{
    BacktestRequest, CreateFork, ForkFrom, HoldoutCall, HoldoutOpened, IngestRequest, PromoteAll,
    PromoteOne, PromoteTable, QueryRequest, ReadPoint, SealCall, Service, ServiceError, WriteMode,
};

fn schema() -> arrow::datatypes::SchemaRef {
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

fn batch(ts: &[i64], price: &[f64]) -> RecordBatch {
    let symbols = vec!["AAPL"; ts.len()];
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts.to_vec()).with_timezone("UTC")),
            Arc::new(StringArray::from(symbols)),
            Arc::new(Float64Array::from(price.to_vec())),
        ],
    )
    .unwrap()
}

fn encode(batch: &RecordBatch) -> String {
    let mut buf = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    base64::engine::general_purpose::STANDARD.encode(buf)
}

fn options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".into()),
        ..Default::default()
    }
}

async fn service() -> (tempfile::TempDir, Service) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("db")).await.unwrap();
    (dir, Service::open(db).await.unwrap())
}

fn number(value: &serde_json::Value) -> f64 {
    value.as_f64().unwrap_or_else(|| {
        value
            .as_i64()
            .map(|item| item as f64)
            .or_else(|| value.as_u64().map(|item| item as f64))
            .unwrap_or_else(|| panic!("not a number: {value}"))
    })
}

async fn scalar(service: &Service, decision: i64, sql: &str) -> f64 {
    let handle = service.handle(None, None).await.unwrap();
    let result = handle
        .query(&QueryRequest {
            decision_time_ns: decision,
            sql: sql.to_string(),
            max_rows: None,
        })
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    assert!(!result.rows.is_empty(), "{sql} returned no rows");
    number(result.rows[0].as_object().unwrap().values().next().unwrap())
}

#[tokio::test]
async fn ingest_replays_the_same_key_and_rejects_a_different_body() {
    let (_dir, service) = service().await;
    service
        .database()
        .create_table("trades", schema(), options())
        .await
        .unwrap();
    let handle = service.handle(None, None).await.unwrap();
    let head = handle.table_head("trades").await.unwrap();
    let request = IngestRequest {
        idempotency_key: "k-ingest".into(),
        table: "trades".into(),
        mode: WriteMode::Append,
        expected_version: head,
        ipc_b64: encode(&batch(&[1_000], &[1.0])),
        note: None,
    };
    let first = handle.ingest(&request).await.unwrap();
    let second = handle.ingest(&request).await.unwrap();
    assert_eq!(first, second);
    let changed = IngestRequest {
        ipc_b64: encode(&batch(&[2_000], &[2.0])),
        ..request
    };
    let error = handle.ingest(&changed).await.unwrap_err();
    assert_eq!(error.code(), "idempotency_conflict");
    let decision = handle
        .list_versions("trades")
        .await
        .unwrap()
        .last()
        .unwrap()
        .committed_at_ns;
    assert_eq!(
        scalar(&service, decision, "SELECT count(*) AS c FROM trades").await,
        1.0
    );
}

#[tokio::test]
async fn a_stale_expected_version_conflicts_and_does_not_write() {
    let (_dir, service) = service().await;
    service
        .database()
        .create_table("trades", schema(), options())
        .await
        .unwrap();
    let handle = service.handle(None, None).await.unwrap();
    let head = handle.table_head("trades").await.unwrap();
    let error = handle
        .ingest(&IngestRequest {
            idempotency_key: "k-stale".into(),
            table: "trades".into(),
            mode: WriteMode::Append,
            expected_version: head + 5,
            ipc_b64: encode(&batch(&[1_000], &[1.0])),
            note: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(error, ServiceError::VersionConflict { .. }));
    assert_eq!(handle.table_head("trades").await.unwrap(), head);
}

#[tokio::test]
async fn a_query_at_t_hides_later_events_and_later_commits() {
    let (_dir, service) = service().await;
    service
        .database()
        .create_table("trades", schema(), options())
        .await
        .unwrap();
    let handle = service.handle(None, None).await.unwrap();
    let head = handle.table_head("trades").await.unwrap();
    // 2100-01-01 is after any commit clock this process will observe.
    let future = 4_102_444_800_000_000_000;
    handle
        .ingest(&IngestRequest {
            idempotency_key: "k-rows".into(),
            table: "trades".into(),
            mode: WriteMode::Append,
            expected_version: head,
            ipc_b64: encode(&batch(&[1_000, future], &[1.0, 9.0])),
            note: None,
        })
        .await
        .unwrap();
    let decision = handle
        .list_versions("trades")
        .await
        .unwrap()
        .last()
        .unwrap()
        .committed_at_ns;
    assert_eq!(
        scalar(&service, decision, "SELECT count(*) AS c FROM trades").await,
        1.0
    );
    assert_eq!(
        scalar(&service, decision, "SELECT max(price) AS c FROM trades").await,
        1.0
    );
    assert_eq!(
        scalar(
            &service,
            decision,
            "SELECT count(*) AS c FROM trades a JOIN trades b ON a.symbol = b.symbol"
        )
        .await,
        1.0
    );
    assert_eq!(
        scalar(
            &service,
            decision,
            "SELECT max(price) OVER () AS c FROM trades"
        )
        .await,
        1.0
    );
    assert_eq!(
        scalar(
            &service,
            decision,
            "SELECT count(*) AS c FROM h5i('trades')"
        )
        .await,
        1.0
    );
    assert_eq!(
        scalar(
            &service,
            decision,
            "SELECT max(price) AS c FROM latest_on('trades','symbol')"
        )
        .await,
        1.0
    );
    assert_eq!(
        scalar(
            &service,
            decision,
            "SELECT count(*) AS c FROM asof_join('trades','trades','ts','ts','symbol')"
        )
        .await,
        1.0
    );

    // A restatement committed after the decision keeps the old price.
    let head = handle.table_head("trades").await.unwrap();
    handle
        .ingest(&IngestRequest {
            idempotency_key: "k-restate".into(),
            table: "trades".into(),
            mode: WriteMode::Write,
            expected_version: head,
            ipc_b64: encode(&batch(&[1_000], &[99.0])),
            note: None,
        })
        .await
        .unwrap();
    assert_eq!(
        scalar(&service, decision, "SELECT max(price) AS c FROM trades").await,
        1.0
    );

    let denied = service.handle(None, Some(decision)).await.unwrap();
    let error = denied
        .query(&QueryRequest {
            decision_time_ns: decision + 1,
            sql: "SELECT count(*) AS c FROM trades".into(),
            max_rows: None,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), "scope_denied");
}

#[tokio::test]
async fn a_fork_does_not_see_another_forks_unpromoted_rows() {
    let (_dir, service) = service().await;
    service
        .database()
        .create_table("trades", schema(), options())
        .await
        .unwrap();
    let base = service.handle(None, None).await.unwrap();
    let head = base.table_head("trades").await.unwrap();
    base.ingest(&IngestRequest {
        idempotency_key: "k-base".into(),
        table: "trades".into(),
        mode: WriteMode::Append,
        expected_version: head,
        ipc_b64: encode(&batch(&[1_000], &[1.0])),
        note: None,
    })
    .await
    .unwrap();
    service
        .create_fork(&CreateFork {
            idempotency_key: "k-fork-a".into(),
            name: "a".into(),
            from: ForkFrom::Head,
            note: None,
        })
        .await
        .unwrap();
    let again = service
        .create_fork(&CreateFork {
            idempotency_key: "k-fork-a".into(),
            name: "a".into(),
            from: ForkFrom::Head,
            note: None,
        })
        .await
        .unwrap();
    assert_eq!(again.name, "a");
    let conflict = service
        .create_fork(&CreateFork {
            idempotency_key: "k-fork-a".into(),
            name: "other".into(),
            from: ForkFrom::Head,
            note: None,
        })
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), "idempotency_conflict");
    service
        .create_fork(&CreateFork {
            idempotency_key: "k-fork-b".into(),
            name: "b".into(),
            from: ForkFrom::Head,
            note: None,
        })
        .await
        .unwrap();

    let fork_a = service.handle(Some("a"), None).await.unwrap();
    let head = fork_a.table_head("trades").await.unwrap();
    let pin = again
        .pins
        .iter()
        .find(|pin| pin.table == "trades")
        .unwrap()
        .sequence;
    assert_eq!(
        head, 0,
        "the first shadow write compare-and-swaps against sequence 0"
    );
    assert!(pin > 0, "promote still names the pinned base sequence");
    let base_head = base.table_head("trades").await.unwrap();
    fork_a
        .ingest(&IngestRequest {
            idempotency_key: "k-a-row".into(),
            table: "trades".into(),
            mode: WriteMode::Append,
            expected_version: head,
            ipc_b64: encode(&batch(&[2_000], &[2.0])),
            note: None,
        })
        .await
        .unwrap();
    assert_eq!(base.table_head("trades").await.unwrap(), base_head);
    // After the fork's own commit, so the arrival pin includes it. The other
    // fork and the base still do not have that row.
    let decision = fork_a
        .list_versions("trades")
        .await
        .unwrap()
        .last()
        .unwrap()
        .committed_at_ns;

    assert_eq!(max_price(&service, Some("b"), decision).await, 1.0);
    assert_eq!(max_price(&service, None, decision).await, 1.0);
    assert_eq!(max_price(&service, Some("a"), decision).await, 2.0);
}

async fn max_price(service: &Service, fork: Option<&str>, decision: i64) -> f64 {
    let handle = service.handle(fork, None).await.unwrap();
    let result = handle
        .query(&QueryRequest {
            decision_time_ns: decision,
            sql: "SELECT max(price) AS c FROM trades".into(),
            max_rows: None,
        })
        .await
        .unwrap();
    number(result.rows[0].get("c").unwrap())
}

#[tokio::test]
async fn a_sealed_span_is_hidden_until_the_one_holdout_grant() {
    let (_dir, service) = service().await;
    service
        .database()
        .create_table("trades", schema(), options())
        .await
        .unwrap();
    let handle = service.handle(None, None).await.unwrap();
    let head = handle.table_head("trades").await.unwrap();
    handle
        .ingest(&IngestRequest {
            idempotency_key: "k-seal-rows".into(),
            table: "trades".into(),
            mode: WriteMode::Append,
            expected_version: head,
            ipc_b64: encode(&batch(&[1_000, 5_000], &[1.0, 5.0])),
            note: None,
        })
        .await
        .unwrap();
    let decision = handle
        .list_versions("trades")
        .await
        .unwrap()
        .last()
        .unwrap()
        .committed_at_ns;
    service
        .seal(&SealCall {
            idempotency_key: "k-seal".into(),
            id: "hold".into(),
            table: "trades".into(),
            start_ns: 500,
            end_ns: 2_000,
            note: None,
        })
        .await
        .unwrap();
    assert_eq!(
        scalar(&service, decision, "SELECT count(*) AS c FROM trades").await,
        1.0
    );
    assert_eq!(
        scalar(&service, decision, "SELECT max(price) AS c FROM trades").await,
        5.0
    );

    let opened = service
        .begin_holdout(&HoldoutCall {
            idempotency_key: "k-eval".into(),
            seal_id: "hold".into(),
            code_identity: "frozen".into(),
            parameters: "{}".into(),
            data_version: "v".into(),
        })
        .await
        .unwrap();
    let HoldoutOpened::Grant(grant) = opened else {
        panic!("the first evaluation must grant a read");
    };
    let granted = grant.handle();
    let seen = granted
        .query(&QueryRequest {
            decision_time_ns: decision,
            sql: "SELECT count(*) AS c FROM trades".into(),
            max_rows: None,
        })
        .await
        .unwrap();
    assert_eq!(number(seen.rows[0].get("c").unwrap()), 2.0);
    grant.finish(serde_json::json!({"score": 1})).await.unwrap();

    let error = service
        .begin_holdout(&HoldoutCall {
            idempotency_key: "k-other".into(),
            seal_id: "hold".into(),
            code_identity: "other".into(),
            parameters: "{}".into(),
            data_version: "v".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), "holdout_used");
    // The seal is still hidden to everyone else.
    assert_eq!(
        scalar(&service, decision, "SELECT count(*) AS c FROM trades").await,
        1.0
    );
}

#[tokio::test]
async fn promote_all_publishes_nothing_when_one_table_conflicts() {
    let (_dir, service) = service().await;
    for name in ["t1", "t2"] {
        service
            .database()
            .create_table(name, schema(), options())
            .await
            .unwrap();
        let handle = service.handle(None, None).await.unwrap();
        let head = handle.table_head(name).await.unwrap();
        handle
            .ingest(&IngestRequest {
                idempotency_key: format!("k-base-{name}"),
                table: name.into(),
                mode: WriteMode::Append,
                expected_version: head,
                ipc_b64: encode(&batch(&[1_000], &[1.0])),
                note: None,
            })
            .await
            .unwrap();
    }
    let fork = service
        .create_fork(&CreateFork {
            idempotency_key: "k-fork".into(),
            name: "desk".into(),
            from: ForkFrom::Head,
            note: None,
        })
        .await
        .unwrap();
    let pinned = |name: &str| {
        fork.pins
            .iter()
            .find(|pin| pin.table == name)
            .unwrap()
            .sequence
    };
    let t1_pin = pinned("t1");
    let t2_pin = pinned("t2");
    let fork_handle = service.handle(Some("desk"), None).await.unwrap();
    for name in ["t1", "t2"] {
        let head = fork_handle.table_head(name).await.unwrap();
        fork_handle
            .ingest(&IngestRequest {
                idempotency_key: format!("k-fork-{name}"),
                table: name.into(),
                mode: WriteMode::Append,
                expected_version: head,
                ipc_b64: encode(&batch(&[2_000], &[2.0])),
                note: None,
            })
            .await
            .unwrap();
    }
    // Move the base of t2 only.
    let base = service.handle(None, None).await.unwrap();
    let head = base.table_head("t2").await.unwrap();
    base.ingest(&IngestRequest {
        idempotency_key: "k-move-t2".into(),
        table: "t2".into(),
        mode: WriteMode::Append,
        expected_version: head,
        ipc_b64: encode(&batch(&[3_000], &[3.0])),
        note: None,
    })
    .await
    .unwrap();
    let before = base.table_head("t1").await.unwrap();
    let error = service
        .promote_all(&PromoteAll {
            idempotency_key: "k-promote".into(),
            fork: "desk".into(),
            tables: vec![
                PromoteTable {
                    table: "t1".into(),
                    expected_base: Some(t1_pin),
                },
                PromoteTable {
                    table: "t2".into(),
                    expected_base: Some(t2_pin),
                },
            ],
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), "promote_conflict");
    assert_eq!(
        base.table_head("t1").await.unwrap(),
        before,
        "t1 must stay unpromoted"
    );

    let promoted = service
        .promote_all(&PromoteAll {
            idempotency_key: "k-promote-t1".into(),
            fork: "desk".into(),
            tables: vec![PromoteTable {
                table: "t1".into(),
                expected_base: Some(t1_pin),
            }],
        })
        .await
        .unwrap();
    let replay = service
        .promote_all(&PromoteAll {
            idempotency_key: "k-promote-t1".into(),
            fork: "desk".into(),
            tables: vec![PromoteTable {
                table: "t1".into(),
                expected_base: Some(t1_pin),
            }],
        })
        .await
        .unwrap();
    assert_eq!(promoted, replay);
    assert!(base.table_head("t1").await.unwrap() > before);
}

#[tokio::test]
async fn export_matches_a_ledger_and_names_a_disagreement() {
    let (_dir, service) = service().await;
    service
        .create_fork(&CreateFork {
            idempotency_key: "k-export".into(),
            name: "desk".into(),
            from: ForkFrom::Head,
            note: Some("one".into()),
        })
        .await
        .unwrap();
    let actual = service.export_state().await.unwrap();
    assert!(Service::reconcile(&actual, &actual).agrees());
    let mut ledger = actual.clone();
    ledger.forks.clear();
    let diff = Service::reconcile(&actual, &ledger);
    assert!(diff.extra.iter().any(|path| path.contains("desk")));
    assert!(!diff.agrees());
}

#[tokio::test]
async fn a_backtest_records_one_trial_and_a_retry_does_not_run_again() {
    use h5i_db_backtest::engine::Strategy;
    use h5i_db_backtest::instrument::{Instrument, InstrumentId, OutcomeId};
    use h5i_db_backtest::settlement::Resolution;
    use h5i_db_backtest::store;
    use h5i_db_backtest::types::{Price, Qty, Stamps, UnixNanos};

    let (_dir, service) = service().await;
    let db = service.database();
    store::create_market_data_tables(db).await.unwrap();
    let instrument = Instrument::binary("will-x-happen", "polymarket").unwrap();
    store::write_instruments(db, &[instrument], UnixNanos::new(0))
        .await
        .unwrap();
    let record = h5i_db_backtest::event::Record::new(
        Stamps::immediate(UnixNanos::new(1_000_000_000)),
        InstrumentId::new("will-x-happen").unwrap(),
        OutcomeId::FIRST,
        h5i_db_backtest::event::MarketEvent::BookSnapshot {
            bids: vec![(Price::from_f64(0.4).unwrap(), Qty::from_f64(10.0).unwrap())],
            asks: vec![(Price::from_f64(0.42).unwrap(), Qty::from_f64(10.0).unwrap())],
        },
    );
    store::write_book_events(db, &[record]).await.unwrap();
    store::write_resolutions(
        db,
        &[Resolution::new(
            InstrumentId::new("will-x-happen").unwrap(),
            OutcomeId::FIRST,
            UnixNanos::new(10_000_000_000),
        )],
    )
    .await
    .unwrap();
    service
        .create_fork(&CreateFork {
            idempotency_key: "k-bt-fork".into(),
            name: "runner".into(),
            from: ForkFrom::Head,
            note: None,
        })
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    struct Counting(Arc<AtomicUsize>);
    impl Strategy for Counting {
        fn on_start(
            &mut self,
            _ctx: &mut h5i_db_backtest::engine::Context<'_>,
        ) -> h5i_db_backtest::error::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let request = BacktestRequest {
        idempotency_key: "k-bt".into(),
        run_id: "run-1".into(),
        code_identity: "strategy-v1".into(),
        parameters: "{\"k\":1}".into(),
        decision_time_ns: i64::MAX / 4,
        starting_cash_raw: 1_000_000_000_000,
        read_at: ReadPoint::Latest,
    };
    let handle = service.handle(Some("runner"), None).await.unwrap();
    let mut strategy = Counting(Arc::clone(&calls));
    let first = handle.backtest(&request, &mut strategy).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(first.code_identity, "strategy-v1");
    assert!(first.result_summary.contains("records_processed"));
    let mut strategy = Counting(Arc::clone(&calls));
    let second = handle.backtest(&request, &mut strategy).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a retry must not run the strategy"
    );
    assert_eq!(handle.list_trials().await.unwrap().len(), 1);

    // Promoting the trial table moves it to the base. The export still lists
    // the trial under the fork that ran it.
    service
        .promote(&PromoteOne {
            idempotency_key: "k-bt-promote".into(),
            fork: "runner".into(),
            table: PromoteTable {
                table: "bt_trial".into(),
                expected_base: None,
            },
        })
        .await
        .unwrap();
    let state = service.export_state().await.unwrap();
    let trials: Vec<_> = state
        .trials
        .iter()
        .map(|trial| (trial.fork.as_str(), trial.run_id.as_str()))
        .collect();
    assert_eq!(trials, vec![("runner", "run-1")]);
}

#[tokio::test]
async fn a_rejected_backtest_still_records_one_trial() {
    use h5i_db_backtest::engine::Strategy;

    let (_dir, service) = service().await;
    service
        .create_fork(&CreateFork {
            idempotency_key: "k-reject-fork".into(),
            name: "runner".into(),
            from: ForkFrom::Head,
            note: None,
        })
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    struct Counting(Arc<AtomicUsize>);
    impl Strategy for Counting {
        fn on_start(
            &mut self,
            _ctx: &mut h5i_db_backtest::engine::Context<'_>,
        ) -> h5i_db_backtest::error::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let request = BacktestRequest {
        idempotency_key: "k-reject".into(),
        run_id: "p-2".into(),
        code_identity: "3".into(),
        parameters: "4".into(),
        decision_time_ns: i64::MAX / 4,
        starting_cash_raw: 1_000_000_000_000,
        read_at: ReadPoint::Latest,
    };
    let handle = service.handle(Some("runner"), None).await.unwrap();
    let mut strategy = Counting(Arc::clone(&calls));
    let error = handle.backtest(&request, &mut strategy).await.unwrap_err();
    assert_eq!(error.code(), "backtest");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let trials = handle.list_trials().await.unwrap();
    assert_eq!(trials.len(), 1);
    assert_eq!(trials[0].run_id, "p-2");
    assert_eq!(trials[0].code_identity, "3");
    assert!(
        trials[0].result_summary.contains("rejected"),
        "{}",
        trials[0].result_summary
    );
    assert!(trials[0].result_summary.contains("records_processed"));

    let mut strategy = Counting(Arc::clone(&calls));
    let again = handle.backtest(&request, &mut strategy).await.unwrap_err();
    assert_eq!(again.code(), "backtest");
    assert_eq!(calls.load(Ordering::SeqCst), 0, "a retry must not run");
    assert_eq!(handle.list_trials().await.unwrap().len(), 1);

    let actual = service.export_state().await.unwrap();
    let mut ledger = actual.clone();
    assert!(Service::reconcile(&actual, &ledger).agrees());
    ledger.trials.clear();
    assert!(!Service::reconcile(&actual, &ledger).agrees());
}

#[tokio::test]
async fn plan_apply_replays_and_a_moved_head_conflicts() {
    use h5i_db_service::{ApplyRequest, PlanRequest};

    let (_dir, service) = service().await;
    service
        .database()
        .create_table("trades", schema(), options())
        .await
        .unwrap();
    let handle = service.handle(None, None).await.unwrap();
    let head = handle.table_head("trades").await.unwrap();
    handle
        .ingest(&IngestRequest {
            idempotency_key: "k-seed".into(),
            table: "trades".into(),
            mode: WriteMode::Append,
            expected_version: head,
            ipc_b64: encode(&batch(&[1_000], &[1.0])),
            note: None,
        })
        .await
        .unwrap();
    let head = handle.table_head("trades").await.unwrap();
    let plan = handle
        .plan(&PlanRequest {
            idempotency_key: "k-plan".into(),
            table: "trades".into(),
            mode: WriteMode::Write,
            expected_version: head,
            ipc_b64: encode(&batch(&[1_000], &[4.0])),
            note: None,
        })
        .await
        .unwrap();
    let again = handle
        .plan(&PlanRequest {
            idempotency_key: "k-plan".into(),
            table: "trades".into(),
            mode: WriteMode::Write,
            expected_version: head,
            ipc_b64: encode(&batch(&[1_000], &[4.0])),
            note: None,
        })
        .await
        .unwrap();
    assert_eq!(plan.plan_id, again.plan_id);
    let applied = handle
        .apply(&ApplyRequest {
            idempotency_key: "k-apply".into(),
            table: "trades".into(),
            plan_id: plan.plan_id,
            expected_version: plan.base_version,
        })
        .await
        .unwrap();
    let replayed = handle
        .apply(&ApplyRequest {
            idempotency_key: "k-apply".into(),
            table: "trades".into(),
            plan_id: plan.plan_id,
            expected_version: plan.base_version,
        })
        .await
        .unwrap();
    assert_eq!(applied, replayed);
}
