//! Seal, fork, and ledger checks through the service API.
//!
//! A query at a decision time past the handle ceiling is refused. A sealed
//! span is absent until the one holdout grant. A fork does not see another
//! fork's unpromoted row. The kernel refuses an agent that tries to record
//! its own trial or to read another agent's family. Dropping a trial from
//! the ledger snapshot shows up as an extra row in the database.

use std::sync::Arc;

use arrow::array::{Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use base64::Engine;
use h5i_db_core::{Database, TableOptions};
use h5i_db_service::{
    CreateFork, ForkFrom, ForkState, HoldoutCall, HoldoutOpened, IngestRequest, QueryRequest,
    SealCall, Service, StateDocument, TrialState, WriteMode,
};
use lab_kernel as k;
use lab_server::kernel_claims;

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

fn human() -> k::Principal {
    k::Principal {
        org: 1,
        user: 1,
        role: k::ROLE_HUMAN,
        agent: 0,
    }
}
fn agent(id: u64) -> k::Principal {
    k::Principal {
        org: 1,
        user: id,
        role: k::ROLE_AGENT,
        agent: id,
    }
}
fn worker() -> k::Principal {
    k::Principal {
        org: 1,
        user: 9,
        role: k::ROLE_WORKER,
        agent: 0,
    }
}

fn apply(
    s: &k::Snapshot,
    actor: k::Principal,
    cmd: k::Command,
) -> (k::Snapshot, Result<k::Reply, k::Error>) {
    match k::transition(&actor, s, &cmd) {
        Ok((ws, reply)) => (k::apply(s, &ws), Ok(reply)),
        Err(error) => (s.clone(), Err(error)),
    }
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

async fn scalar(service: &Service, fork: Option<&str>, decision: i64, sql: &str) -> f64 {
    let handle = service.handle(fork, None).await.unwrap();
    let result = handle
        .query(&QueryRequest {
            decision_time_ns: decision,
            sql: sql.to_string(),
            max_rows: None,
        })
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    number(result.rows[0].as_object().unwrap().values().next().unwrap())
}

#[tokio::test]
async fn a_sealed_span_is_hidden_and_a_fork_does_not_see_another() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("db")).await.unwrap();
    let service = Service::open(db).await.unwrap();
    service
        .database()
        .create_table("trades", schema(), options())
        .await
        .unwrap();

    let capped = service.handle(None, Some(100)).await.unwrap();
    let denied = capped
        .query(&QueryRequest {
            decision_time_ns: 1_000,
            sql: "SELECT 1 AS n".into(),
            max_rows: Some(10),
        })
        .await;
    assert_eq!(denied.unwrap_err().code(), "scope_denied");

    let base = service.handle(None, None).await.unwrap();
    let head = base.table_head("trades").await.unwrap();
    base.ingest(&IngestRequest {
        idempotency_key: "rows".into(),
        table: "trades".into(),
        mode: WriteMode::Append,
        expected_version: head,
        ipc_b64: encode(&batch(&[1_000, 5_000], &[1.0, 5.0])),
        note: None,
    })
    .await
    .unwrap();
    let decision = base
        .list_versions("trades")
        .await
        .unwrap()
        .last()
        .unwrap()
        .committed_at_ns;
    service
        .seal(&SealCall {
            idempotency_key: "seal-1".into(),
            id: "holdout".into(),
            table: "trades".into(),
            start_ns: 500,
            end_ns: 2_000,
            note: None,
        })
        .await
        .unwrap();
    assert_eq!(
        scalar(&service, None, decision, "SELECT count(*) AS c FROM trades").await,
        1.0
    );
    assert_eq!(
        scalar(
            &service,
            None,
            decision,
            "SELECT max(price) AS c FROM trades"
        )
        .await,
        5.0
    );

    let opened = service
        .begin_holdout(&HoldoutCall {
            idempotency_key: "ho-1".into(),
            seal_id: "holdout".into(),
            code_identity: "9".into(),
            parameters: "8".into(),
            data_version: "1".into(),
        })
        .await
        .unwrap();
    let HoldoutOpened::Grant(grant) = opened else {
        panic!("the first evaluation opens a grant");
    };
    let seen = grant
        .handle()
        .query(&QueryRequest {
            decision_time_ns: decision,
            sql: "SELECT count(*) AS c FROM trades".into(),
            max_rows: None,
        })
        .await
        .unwrap();
    assert_eq!(number(seen.rows[0].get("c").unwrap()), 2.0);
    let again = service
        .begin_holdout(&HoldoutCall {
            idempotency_key: "ho-2".into(),
            seal_id: "holdout".into(),
            code_identity: "9".into(),
            parameters: "8".into(),
            data_version: "1".into(),
        })
        .await;
    assert_eq!(again.unwrap_err().code(), "holdout_used");
    grant.finish(serde_json::json!({"rows": 2})).await.unwrap();
    assert_eq!(
        scalar(&service, None, decision, "SELECT count(*) AS c FROM trades").await,
        1.0
    );

    service
        .create_fork(&CreateFork {
            idempotency_key: "fork-a".into(),
            name: "a".into(),
            from: ForkFrom::Head,
            note: None,
        })
        .await
        .unwrap();
    service
        .create_fork(&CreateFork {
            idempotency_key: "fork-b".into(),
            name: "b".into(),
            from: ForkFrom::Head,
            note: None,
        })
        .await
        .unwrap();
    let fork_a = service.handle(Some("a"), None).await.unwrap();
    let head = fork_a.table_head("trades").await.unwrap();
    fork_a
        .ingest(&IngestRequest {
            idempotency_key: "a-row".into(),
            table: "trades".into(),
            mode: WriteMode::Append,
            expected_version: head,
            ipc_b64: encode(&batch(&[8_000], &[9.0])),
            note: None,
        })
        .await
        .unwrap();
    let fork_decision = fork_a
        .list_versions("trades")
        .await
        .unwrap()
        .last()
        .unwrap()
        .committed_at_ns;
    assert_eq!(
        scalar(
            &service,
            Some("a"),
            fork_decision,
            "SELECT max(price) AS c FROM trades"
        )
        .await,
        9.0
    );
    assert_eq!(
        scalar(
            &service,
            Some("b"),
            fork_decision,
            "SELECT max(price) AS c FROM trades"
        )
        .await,
        5.0
    );
    assert_eq!(
        scalar(
            &service,
            None,
            fork_decision,
            "SELECT max(price) AS c FROM trades"
        )
        .await,
        5.0
    );
}

#[tokio::test]
async fn an_agent_cannot_drop_a_trial_and_the_ledger_diff_shows_it() {
    let mut snap = k::Snapshot::default();
    snap = apply(
        &snap,
        human(),
        k::Command::Enroll {
            agent: 1,
            budget: 2,
            token_budget: 2,
            sandbox: 0,
        },
    )
    .0;
    snap = apply(
        &snap,
        human(),
        k::Command::Enroll {
            agent: 2,
            budget: 2,
            token_budget: 2,
            sandbox: 1,
        },
    )
    .0;
    let (snap, created) = apply(
        &snap,
        agent(1),
        k::Command::PreRegister {
            hypothesis: 3,
            method: 4,
        },
    );
    let k::Reply::Created(family) = created.unwrap() else {
        panic!("family")
    };
    let (snap, pending) = apply(
        &snap,
        agent(1),
        k::Command::RequestTrial {
            family,
            code: 9,
            params: 8,
            decision_time: 100,
        },
    );
    let k::Reply::Created(pending_id) = pending.unwrap() else {
        panic!("pending")
    };
    assert!(matches!(
        apply(
            &snap,
            agent(1),
            k::Command::RecordTrial {
                pending: pending_id,
                data_version: 1,
                score: 1,
                tokens: 0
            },
        )
        .1,
        Err(k::Error::Forbidden)
    ));
    let (snap, _) = apply(
        &snap,
        worker(),
        k::Command::RecordTrial {
            pending: pending_id,
            data_version: 1,
            score: 5,
            tokens: 0,
        },
    );
    assert_eq!(snap.families[0].trials, 1);
    assert!(matches!(
        apply(&snap, agent(2), k::Command::Report { family }).1,
        Err(k::Error::Forbidden)
    ));

    let actual = StateDocument {
        format: h5i_db_service::STATE_FORMAT,
        forks: vec![ForkState {
            name: "a-1".into(),
            created_at_ns: 0,
            note: None,
            as_of_ns: None,
            parent: None,
            depth: 0,
            pins: Vec::new(),
            owned: Vec::new(),
        }],
        versions: vec![],
        trials: vec![TrialState {
            fork: "a-1".into(),
            run_id: format!("p-{pending_id}"),
            ts_ns: 1,
            config_digest: "d".into(),
            code_identity: "9".into(),
            parameters: "8".into(),
            data_version: "1".into(),
            decision_time_ns: Some(100),
            scope: "fork".into(),
            result_summary: "{}".into(),
        }],
        evaluations: vec![],
        promotions: vec![],
    };
    let claimed = kernel_claims(&snap, &actual, "holdout");
    let json = serde_json::to_string(&claimed).unwrap();
    let parsed: StateDocument = serde_json::from_str(&json).unwrap();
    assert!(
        Service::reconcile(&actual, &parsed).agrees(),
        "the recorded trial is in both"
    );

    let mut dropped = parsed.clone();
    dropped.trials.clear();
    let diff = Service::reconcile(&actual, &dropped);
    assert!(!diff.agrees());
    assert!(
        diff.extra.iter().any(|path| path.contains("trials/")),
        "{diff:?}"
    );
}
