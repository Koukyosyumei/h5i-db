//! A submitted program runs as a real backtest through the service handle.
//!
//! The data is the demo's synthetic market. The run fills against the book,
//! stops at the decision time, and never reads the sealed span. The holdout
//! run sees the sealed span only through the grant.

use h5i_db_core::Database;
use h5i_db_service::{
    BacktestRequest, CreateFork, ForkFrom, HoldoutCall, HoldoutOpened, QueryRequest, ReadPoint,
    SealCall, Service,
};
use lab_server::market::{DAY_NS, START_NS, demo_series, seed};
use lab_server::strategy::{Program, RuleStrategy};
use lab_server::{STARTING_CASH, evaluate_holdout_run, holdout_passed};

const CROSS: &str = "universe ALFA DLTA\nlet fast = sma($fast)\nlet slow = sma($slow)\nlong when fast > slow\nflat when fast < slow\nsize $units\n";

fn number(value: &serde_json::Value) -> f64 {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .unwrap_or(f64::NAN)
}

#[tokio::test(flavor = "current_thread")]
async fn a_program_trades_on_its_fork_up_to_the_decision_time_and_not_in_the_seal() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("lab.db")).await.unwrap();
    let end = seed(&db, &demo_series(), 40, 11).await.unwrap();
    let service = Service::open(db).await.unwrap();
    let seal_start = end - 10 * DAY_NS;
    service
        .seal(&SealCall {
            idempotency_key: "seal".into(),
            id: "holdout".into(),
            table: "book_deltas".into(),
            start_ns: seal_start,
            end_ns: end,
            note: None,
        })
        .await
        .unwrap();
    service
        .create_fork(&CreateFork {
            idempotency_key: "fork-a-1".into(),
            name: "a-1".into(),
            from: ForkFrom::Head,
            note: None,
        })
        .await
        .unwrap();
    let handle = service.handle(Some("a-1"), None).await.unwrap();
    let params = serde_json::json!({"fast": 6, "slow": 24, "units": 100});
    let program = Program::compile(CROSS, &params).unwrap();
    let mut strategy = RuleStrategy::new(program);
    let decision = START_NS + 25 * DAY_NS;
    let receipt = handle
        .backtest(
            &BacktestRequest {
                idempotency_key: "bt-1".into(),
                run_id: "p-1".into(),
                code_identity: lab_server::code_identity(CROSS.as_bytes()),
                parameters: "{}".into(),
                decision_time_ns: decision,
                starting_cash_raw: (STARTING_CASH as i64) * 1_000_000_000,
                read_at: ReadPoint::Latest,
            },
            &mut strategy,
        )
        .await
        .unwrap();
    assert!(receipt.code_identity.starts_with("blake3:"));
    assert!(strategy.tally.fills > 2, "{:?}", strategy.tally);
    let rows = handle
        .query(&QueryRequest {
            decision_time_ns: i64::MAX / 4,
            sql: "SELECT max(CAST(ts AS BIGINT)) AS last, count(*) AS n FROM bt_equity".into(),
            max_rows: Some(1),
        })
        .await
        .unwrap()
        .rows;
    let last = number(&rows[0]["last"]);
    assert!(number(&rows[0]["n"]) > 100.0, "{rows:?}");
    assert!(
        last <= decision as f64,
        "equity at {last} is past the decision time {decision}"
    );

    // Past the seal: the run still stops where the seal starts.
    let mut later = RuleStrategy::new(Program::compile(CROSS, &params).unwrap());
    handle
        .backtest(
            &BacktestRequest {
                idempotency_key: "bt-2".into(),
                run_id: "p-2".into(),
                code_identity: "x".into(),
                parameters: "{}".into(),
                decision_time_ns: end + DAY_NS,
                starting_cash_raw: (STARTING_CASH as i64) * 1_000_000_000,
                read_at: ReadPoint::Latest,
            },
            &mut later,
        )
        .await
        .unwrap();
    let rows = handle
        .query(&QueryRequest {
            decision_time_ns: i64::MAX / 4,
            sql: "SELECT max(CAST(ts AS BIGINT)) AS last FROM bt_equity".into(),
            max_rows: Some(1),
        })
        .await
        .unwrap()
        .rows;
    assert!(number(&rows[0]["last"]) < seal_start as f64, "{rows:?}");

    // Without the grant the sealed span is empty; with it, the program trades.
    let plain = evaluate_holdout_run(
        service.database(),
        "holdout",
        CROSS.as_bytes(),
        params.to_string().as_bytes(),
    )
    .await
    .unwrap();
    assert_eq!(plain["records"], 0, "{plain}");
    assert!(!holdout_passed(&plain));
    let opened = service
        .begin_holdout(&HoldoutCall {
            idempotency_key: "ho".into(),
            seal_id: "holdout".into(),
            code_identity: "c".into(),
            parameters: "p".into(),
            data_version: "1".into(),
        })
        .await
        .unwrap();
    let HoldoutOpened::Grant(grant) = opened else {
        panic!("first evaluation")
    };
    let summary = evaluate_holdout_run(
        grant.handle().database(),
        "holdout",
        CROSS.as_bytes(),
        params.to_string().as_bytes(),
    )
    .await
    .unwrap();
    assert!(summary["records"].as_u64().unwrap() > 400, "{summary}");
    assert!(
        summary["equity_points"].as_u64().unwrap() > 100,
        "{summary}"
    );
    grant.finish(summary).await.unwrap();

    let broken = evaluate_holdout_run(service.database(), "holdout", b"universe ALFA", b"{}").await;
    assert!(broken.unwrap_err().contains("long when"));
}

#[tokio::test(flavor = "current_thread")]
async fn the_sampled_curve_is_the_engines_equity() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("lab.db")).await.unwrap();
    seed(&db, &demo_series(), 20, 4).await.unwrap();
    let params = serde_json::json!({"fast": 6, "slow": 24, "units": 100});
    let mut strategy = RuleStrategy::new(Program::compile(CROSS, &params).unwrap());
    let report = h5i_db_backtest::run::run_in_place(
        &db,
        h5i_db_backtest::run::RunSpec::new(
            "check",
            h5i_db_backtest::types::Money::from_f64(STARTING_CASH).unwrap(),
        ),
        &mut strategy,
        |builder| builder,
    )
    .await
    .unwrap();
    let engine: Vec<f64> = report
        .result
        .equity
        .iter()
        .map(|point| point.equity.to_f64())
        .collect();
    let sampled: Vec<f64> = strategy.curve.iter().map(|(_, equity)| *equity).collect();
    assert!(strategy.tally.fills > 2);
    // One point per instant in both. The engine samples at the first record
    // of an instant, before the other books of that hour arrive; the sampler
    // keeps the state after the last one. The two are the same account a few
    // marks apart.
    assert_eq!(engine.len(), sampled.len());
    let gaps: Vec<f64> = engine
        .iter()
        .zip(&sampled)
        .map(|(left, right)| (left - right).abs() / STARTING_CASH)
        .collect();
    let worst = gaps.iter().copied().fold(0.0, f64::max);
    let mean = gaps.iter().sum::<f64>() / gaps.len() as f64;
    assert!(worst < 0.01 && mean < 0.001, "worst {worst}, mean {mean}");
}
