//! Delivers a ledger effect to h5i-db and turns the result into a command.
//!
//! The kernel never sees the database. A retried delivery uses the same
//! idempotency key, so a second pass does not run the strategy again and does
//! not open the holdout a second time.
//!
//! The deflated Sharpe ratio is computed here, from the family's stored trial
//! statistics and this run's equity curve. The kernel stores the integer it
//! is given. It does not evaluate the formula.

use std::future::Future;
use std::sync::Arc;

use h5i_app_core::TenantId;
use h5i_app_pg::Engine;
use h5i_db_backtest::engine::Strategy;
use h5i_db_service::{
    BacktestRequest, CreateFork, ForkFrom, ForkState, HoldoutCall, HoldoutOpened, HoldoutReceipt,
    PromoteOne, PromoteTable, PromotionState, QueryRequest, QueryResult, ReadPoint, Service,
    StateDocument, TrialState,
};
use lab_kernel as k;

use crate::deflated::{
    TrialStats, decode_stat, deflated_sharpe, encode_stat, no_returns, stats_from_equity,
};
use crate::{Lab, LabStore};

/// As-of for the equity read. The worker handle has no ceiling. As-of uses
/// the commit clock, so the pending's decision time would hide the rows just
/// written.
const EQUITY_READ_NS: i64 = i64::MAX / 4;

/// A longer curve is not summarized. The query cap makes a longer scan
/// truncated, and a truncated prefix is refused.
const EQUITY_CAP: usize = 100_000;

pub struct DbWorker {
    pub engine: Arc<Engine<Lab, LabStore>>,
    pub db: Arc<Service>,
    pub seal_id: String,
    pub holdout_table: String,
}

fn worker(org: u64) -> k::Principal {
    k::Principal {
        org,
        user: 1,
        role: k::ROLE_WORKER,
        agent: 0,
    }
}

fn fork_name(agent: u64) -> String {
    format!("a-{agent}")
}

/// The fork is created on the first trial, not at enrollment.
fn agent_has_work(snap: &k::Snapshot, agent: u64) -> bool {
    snap.pendings.iter().any(|row| row.agent == agent)
        || snap.trials.iter().any(|row| row.agent == agent)
        || snap.families.iter().any(|row| {
            row.agent == agent
                && (row.trials != 0
                    || row.frozen_code != 0
                    || row.holdout != k::HOLDOUT_NONE
                    || row.promoted != 0)
        })
}

impl DbWorker {
    /// Record an alert in the ledger. A repeat of the same kind and detail is one row.
    async fn note(&self, org: u64, kind: &str, detail: &str) {
        let kind_b = kind.as_bytes();
        let detail_b = detail.as_bytes();
        if let Ok(snap) = self.engine.snapshot(TenantId(org)).await {
            if snap
                .alerts
                .iter()
                .any(|row| row.kind.as_slice() == kind_b && row.detail.as_slice() == detail_b)
            {
                return;
            }
        }
        let cmd = k::Command::NoteAlert {
            kind: kind_b.to_vec(),
            detail: detail_b.to_vec(),
        };
        match self.engine.execute(&worker(org), &cmd).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => tracing::error!(?error, "alert refused"),
            Err(error) => tracing::error!(%error, "alert was not recorded"),
        }
    }
}

/// Project the ledger onto the database document.
///
/// Forks, trials, the holdout evaluation and promotions are what the kernel
/// claims. Version rows are the database commit log: the ledger does not store
/// a second copy, so they are taken from `actual` and a diff does not flag
/// them. A trial the database has and the kernel does not is `extra`. A trial
/// the kernel has and the database does not is `missing`.
pub fn kernel_claims(snap: &k::Snapshot, actual: &StateDocument, seal_id: &str) -> StateDocument {
    let mut claimed = actual.clone();
    claimed.forks = snap
        .agents
        .iter()
        .filter(|agent| agent_has_work(snap, agent.id))
        .map(|agent| {
            let name = format!("a-{}", agent.fork_id);
            actual
                .forks
                .iter()
                .find(|fork| fork.name == name)
                .cloned()
                .unwrap_or(ForkState {
                    name,
                    created_at_ns: 0,
                    note: None,
                    as_of_ns: None,
                    parent: None,
                    depth: 0,
                    pins: Vec::new(),
                    owned: Vec::new(),
                })
        })
        .collect();
    claimed.trials = snap
        .trials
        .iter()
        .map(|trial| {
            let fork = format!("a-{}", trial.agent);
            let run_id = format!("p-{}", trial.id);
            actual
                .trials
                .iter()
                .find(|row| row.fork == fork && row.run_id == run_id)
                .cloned()
                .unwrap_or(TrialState {
                    fork,
                    run_id,
                    ts_ns: 0,
                    config_digest: String::new(),
                    code_identity: trial.code.to_string(),
                    parameters: trial.params.to_string(),
                    data_version: trial.data_version.to_string(),
                    decision_time_ns: i64::try_from(trial.decision_time).ok(),
                    scope: String::new(),
                    result_summary: String::new(),
                })
        })
        .collect();
    let finished = snap
        .families
        .iter()
        .any(|family| family.holdout == k::HOLDOUT_PASS || family.holdout == k::HOLDOUT_FAIL);
    claimed.evaluations = if finished {
        match actual.evaluations.iter().find(|row| row.seal_id == seal_id) {
            Some(row) => vec![row.clone()],
            None => vec![HoldoutReceipt {
                seal_id: seal_id.to_string(),
                state: "missing".into(),
                code_identity: String::new(),
                parameters: String::new(),
                data_version: String::new(),
                summary: None,
            }],
        }
    } else {
        Vec::new()
    };
    let promoted: Vec<String> = snap
        .families
        .iter()
        .filter(|family| family.promoted != 0)
        .map(|family| format!("a-{}", family.agent))
        .collect();
    claimed.promotions = actual
        .promotions
        .iter()
        .filter(|row| promoted.iter().any(|fork| fork == &row.fork))
        .cloned()
        .collect();
    for fork_name in &promoted {
        if claimed.promotions.iter().any(|row| &row.fork == fork_name) {
            continue;
        }
        // A fork that exists and owns nothing published nothing, which is the
        // whole of its delta. A missing fork, or one that still owns rows,
        // has not landed.
        let still_owns = actual
            .forks
            .iter()
            .find(|fork| &fork.name == fork_name)
            .map(|fork| !fork.owned.is_empty())
            .unwrap_or(true);
        if still_owns {
            claimed.promotions.push(PromotionState {
                fork: fork_name.clone(),
                table: "missing".into(),
                kind: "promote".into(),
                base_sequence: None,
                fork_sequence: None,
                rows: None,
            });
        }
    }
    claimed
}

/// Tables a promote effect should publish: the ones the fork owns.
///
/// A pin is the base the fork reads. Promoting it fails, because the fork
/// does not own that table until it writes. `expected_base` is the pin
/// sequence when the owned table was copied from one, and `None` when the
/// fork created the table.
pub fn tables_to_promote(fork: &ForkState) -> Vec<PromoteTable> {
    fork.owned
        .iter()
        .map(|owned| PromoteTable {
            table: owned.table.clone(),
            expected_base: fork
                .pins
                .iter()
                .find(|pin| pin.table == owned.table)
                .map(|pin| pin.sequence),
        })
        .collect()
}

enum HoldoutCount {
    Rows(u64),
    Used(String),
}

struct Touch;

impl Strategy for Touch {
    fn on_start(
        &mut self,
        _ctx: &mut h5i_db_backtest::engine::Context<'_>,
    ) -> h5i_db_backtest::error::Result<()> {
        Ok(())
    }
}

/// h5i-db futures are not `Send` (backtest trait objects, and some fork
/// closures). The outbox future has to be `Send`, so each database call runs
/// on a fresh thread with its own runtime. Only the `Send` result comes back.
async fn on_db<T, F, Fut>(work: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, String>>,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let outcome = (|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            rt.block_on(work())
        })();
        let _ = tx.send(outcome);
    });
    tokio::task::spawn_blocking(move || {
        let inner = rx.recv().map_err(|e| e.to_string())?;
        inner
    })
    .await
    .map_err(|e| e.to_string())?
}

fn score_of(summary: &str) -> u64 {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(summary) else {
        return 0;
    };
    value
        .get("records_processed")
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
}

struct TrialMeasure {
    score: u64,
    stats: TrialStats,
}

fn json_f64(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .filter(|number| number.is_finite())
}

fn row_count(rows: &[serde_json::Value]) -> Option<u64> {
    let cell = rows.first()?.get("n")?;
    cell.as_u64()
        .or_else(|| cell.as_i64().and_then(|n| u64::try_from(n).ok()))
        .or_else(|| cell.as_str().and_then(|text| text.parse().ok()))
}

/// How many equity rows were already on the fork.
///
/// A missing table is zero. Any other count error refuses the curve, so a
/// failed count is not treated as an empty history.
fn equity_start(result: Result<QueryResult, String>) -> Option<usize> {
    match result {
        Ok(query) => row_count(&query.rows).and_then(|n| usize::try_from(n).ok()),
        Err(error) if error.contains("not found") => Some(0),
        Err(_) => None,
    }
}

/// Points appended after `before`, in the order the query returned them.
///
/// That order is the scan order of the append-only segments. Sorting by
/// simulated time is wrong: a later run can restart the clock at zero and
/// interleave. `None` means the scan was truncated, shorter than `before`,
/// or a point did not parse. The caller records that trial as having no
/// returns rather than a biased prefix.
fn equity_tail(rows: &[serde_json::Value], before: usize, truncated: bool) -> Option<Vec<f64>> {
    if truncated || rows.len() < before {
        return None;
    }
    let mut curve = Vec::with_capacity(rows.len() - before);
    for row in rows.iter().skip(before) {
        curve.push(json_f64(row.get("equity")?)?);
    }
    Some(curve)
}

fn stats_of(trial: &k::Trial) -> TrialStats {
    TrialStats {
        sharpe: decode_stat(trial.sharpe),
        skew: decode_stat(trial.skew),
        kurtosis: decode_stat(trial.kurt),
        samples: trial.samples,
    }
}

/// Family correction, then this trial's statistics in the kernel's encoding.
fn record_fields(prior: &[TrialStats], current: TrialStats) -> (u64, u64, u64, u64, u64) {
    let mut trials = Vec::with_capacity(prior.len() + 1);
    trials.extend_from_slice(prior);
    trials.push(current);
    (
        deflated_sharpe(&trials),
        encode_stat(current.sharpe),
        encode_stat(current.skew),
        encode_stat(current.kurtosis),
        current.samples,
    )
}

impl DbWorker {
    pub async fn deliver(&self, org: u64, key: &str, kind: u64, id: u64) -> Result<(), String> {
        if kind == k::EFFECT_TRIAL {
            self.trial(org, key, id).await
        } else if kind == k::EFFECT_HOLDOUT {
            self.holdout(org, key, id).await
        } else if kind == k::EFFECT_PROMOTE {
            self.promote(org, key, id).await
        } else {
            Err(format!("unknown effect {kind}"))
        }
    }

    async fn trial(&self, org: u64, key: &str, pending_id: u64) -> Result<(), String> {
        let snap = self
            .engine
            .snapshot(TenantId(org))
            .await
            .map_err(|e| e.to_string())?;
        let Some(pending) = k::find_pending(&snap.pendings, pending_id) else {
            return Err(format!("pending {pending_id} is not in the ledger"));
        };
        if pending.open != 1 {
            return Ok(());
        }
        let fork = fork_name(pending.agent);
        let decision = i64::try_from(pending.decision_time).unwrap_or(i64::MAX);
        let code = pending.code.to_string();
        let params = pending.params.to_string();
        let agent = pending.agent;
        let db = Arc::clone(&self.db);
        let effect_key = key.to_string();
        let ran = on_db(move || async move {
            match db
                .create_fork(&CreateFork {
                    idempotency_key: format!("fork-{fork}"),
                    name: fork.clone(),
                    from: ForkFrom::Head,
                    note: Some(format!("agent {agent}")),
                })
                .await
            {
                Ok(_) => {}
                Err(error) if error.code() == "fork_exists" => {}
                Err(error) => return Err(error.to_string()),
            }
            let handle = db
                .handle(Some(&fork), None)
                .await
                .map_err(|e| e.to_string())?;
            let counted = handle
                .query(&QueryRequest {
                    decision_time_ns: EQUITY_READ_NS,
                    sql: "SELECT count(*) AS n FROM bt_equity".into(),
                    max_rows: Some(10),
                })
                .await
                .map_err(|e| e.to_string());
            let before = equity_start(counted);
            let mut strategy = Touch;
            let receipt = handle
                .backtest(
                    &BacktestRequest {
                        idempotency_key: format!("bt-{effect_key}"),
                        run_id: format!("p-{pending_id}"),
                        code_identity: code,
                        parameters: params,
                        decision_time_ns: decision,
                        starting_cash_raw: 1_000_000_000_000,
                        read_at: ReadPoint::Latest,
                    },
                    &mut strategy,
                )
                .await
                .map_err(|e| e.to_string())?;
            let stats = match before {
                Some(start) => match handle
                    .query(&QueryRequest {
                        decision_time_ns: EQUITY_READ_NS,
                        sql: "SELECT equity FROM bt_equity".into(),
                        max_rows: Some(EQUITY_CAP),
                    })
                    .await
                {
                    Ok(curve) => match equity_tail(&curve.rows, start, curve.truncated) {
                        Some(points) => stats_from_equity(&points),
                        None => no_returns(),
                    },
                    Err(_) => no_returns(),
                },
                None => no_returns(),
            };
            Ok(TrialMeasure {
                score: score_of(&receipt.result_summary),
                stats,
            })
        })
        .await;
        let measured = match ran {
            Ok(measured) => measured,
            // The engine answered. Retrying will not grow an instruments
            // catalog, and leaving the pending open would spend budget
            // without counting the attempt. The empty trial still counts,
            // and it still moves the family's corrected score.
            Err(error) if error.starts_with("backtest:") => {
                self.note(org, "backtest", &error).await;
                TrialMeasure {
                    score: 0,
                    stats: no_returns(),
                }
            }
            Err(error) => return Err(error),
        };
        let snap = self
            .engine
            .snapshot(TenantId(org))
            .await
            .map_err(|e| e.to_string())?;
        let Some(pending) = k::find_pending(&snap.pendings, pending_id) else {
            return Err(format!("pending {pending_id} is not in the ledger"));
        };
        if pending.open != 1 {
            return Ok(());
        }
        let prior: Vec<TrialStats> = snap
            .trials
            .iter()
            .filter(|row| row.family == pending.family)
            .map(stats_of)
            .collect();
        let (corrected, sharpe, skew, kurt, samples) = record_fields(&prior, measured.stats);
        let cmd = k::Command::RecordTrial {
            pending: pending_id,
            data_version: 1,
            score: measured.score,
            corrected,
            sharpe,
            skew,
            kurt,
            samples,
            tokens: 0,
        };
        self.engine
            .execute_idempotent(&worker(org), key, &cmd)
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| format!("record trial: {e:?}"))?;
        Ok(())
    }

    async fn holdout(&self, org: u64, key: &str, family_id: u64) -> Result<(), String> {
        let snap = self
            .engine
            .snapshot(TenantId(org))
            .await
            .map_err(|e| e.to_string())?;
        let Some(family) = k::find_family(&snap.families, family_id) else {
            return Err(format!("family {family_id} is not in the ledger"));
        };
        if family.holdout != k::HOLDOUT_QUEUED {
            return Ok(());
        }
        let db = Arc::clone(&self.db);
        let effect_key = key.to_string();
        let seal_id = self.seal_id.clone();
        let table = self.holdout_table.clone();
        let code = family.frozen_code.to_string();
        let params = family.frozen_params.to_string();
        let data_version = family.frozen_data.to_string();
        let opened = on_db(move || async move {
            match db
                .begin_holdout(&HoldoutCall {
                    idempotency_key: format!("ho-{effect_key}"),
                    seal_id,
                    code_identity: code,
                    parameters: params,
                    data_version,
                })
                .await
            {
                Ok(HoldoutOpened::Completed(receipt)) => Ok(HoldoutCount::Rows(
                    receipt
                        .summary
                        .as_ref()
                        .and_then(|v| v.get("rows"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0),
                )),
                Ok(HoldoutOpened::Grant(grant)) => {
                    let query = grant
                        .handle()
                        .query(&QueryRequest {
                            decision_time_ns: i64::MAX / 4,
                            sql: format!("SELECT count(*) AS n FROM {table}"),
                            max_rows: Some(10),
                        })
                        .await
                        .map_err(|e| e.to_string())?;
                    let n = query
                        .rows
                        .first()
                        .and_then(|row| row.get("n"))
                        .and_then(|v| {
                            v.as_u64()
                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                        })
                        .unwrap_or(0);
                    grant
                        .finish(serde_json::json!({"rows": n}))
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(HoldoutCount::Rows(n))
                }
                Err(error) if error.code() == "holdout_used" => {
                    Ok(HoldoutCount::Used(error.to_string()))
                }
                Err(error) => Err(error.to_string()),
            }
        })
        .await?;
        let rows = match opened {
            HoldoutCount::Rows(rows) => rows,
            HoldoutCount::Used(detail) => {
                self.note(org, "holdout", &detail).await;
                return Err(detail);
            }
        };
        let passed = if rows > 0 {
            k::HOLDOUT_PASS
        } else {
            k::HOLDOUT_FAIL
        };
        self.engine
            .execute_idempotent(
                &worker(org),
                key,
                &k::Command::EvaluateHoldout {
                    family: family_id,
                    passed,
                },
            )
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| format!("evaluate holdout: {e:?}"))?;
        Ok(())
    }

    async fn promote(&self, org: u64, key: &str, family_id: u64) -> Result<(), String> {
        let snap = self
            .engine
            .snapshot(TenantId(org))
            .await
            .map_err(|e| e.to_string())?;
        let Some(family) = k::find_family(&snap.families, family_id) else {
            return Err(format!("family {family_id} is not in the ledger"));
        };
        let fork = fork_name(family.agent);
        let db = Arc::clone(&self.db);
        let effect_key = key.to_string();
        let exported = on_db({
            let fork = fork.clone();
            move || async move {
                let state = db.export_state().await.map_err(|e| e.to_string())?;
                let row = state
                    .forks
                    .iter()
                    .find(|item| item.name == fork)
                    .ok_or_else(|| format!("fork {fork} is not in h5i-db"))?;
                for table in tables_to_promote(row) {
                    db.promote(&PromoteOne {
                        idempotency_key: format!("{effect_key}-{}", table.table),
                        fork: fork.clone(),
                        table,
                    })
                    .await
                    .map_err(|e| e.to_string())?;
                }
                db.export_state().await.map_err(|e| e.to_string())
            }
        })
        .await?;
        let diff = Service::reconcile(&exported, &kernel_claims(&snap, &exported, &self.seal_id));
        if !diff.agrees() {
            self.note(org, "reconcile", &format!("{diff:?}")).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use h5i_db_service::{OwnedHead, PinReceipt, QueryResult};

    fn fork(name: &str, owned: Vec<OwnedHead>, pins: Vec<PinReceipt>) -> ForkState {
        ForkState {
            name: name.into(),
            created_at_ns: 0,
            note: None,
            as_of_ns: None,
            parent: None,
            depth: 0,
            pins,
            owned,
        }
    }

    fn family(id: u64, agent: u64, promoted: u64) -> k::Family {
        k::Family {
            id,
            agent,
            task: id,
            hypothesis: 1,
            method: 1,
            trials: 0,
            corrected: 0,
            frozen_code: 1,
            frozen_params: 1,
            frozen_data: 1,
            holdout: k::HOLDOUT_PASS,
            promoted,
            rejected: 0,
        }
    }

    #[test]
    fn a_pin_is_not_promoted_until_the_fork_owns_it() {
        let pinned = fork(
            "a-1",
            vec![],
            vec![PinReceipt {
                table: "trades".into(),
                sequence: 4,
                manifest_checksum: "abc".into(),
            }],
        );
        assert!(tables_to_promote(&pinned).is_empty());

        let wrote = fork(
            "a-1",
            vec![OwnedHead {
                table: "trades".into(),
                sequence: 1,
                rows: 2,
            }],
            vec![PinReceipt {
                table: "trades".into(),
                sequence: 4,
                manifest_checksum: "abc".into(),
            }],
        );
        let tables = tables_to_promote(&wrote);
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].table, "trades");
        assert_eq!(tables[0].expected_base, Some(4));
    }

    #[test]
    fn an_empty_fork_needs_no_promotion_row() {
        let snap = k::Snapshot {
            agents: vec![k::Agent {
                id: 1,
                budget: 1,
                used: 0,
                token_budget: 1,
                tokens_used: 0,
                fork_id: 1,
                sandbox: 0,
            }],
            families: vec![family(1, 1, 1)],
            ..k::Snapshot::default()
        };
        let actual = StateDocument {
            format: h5i_db_service::STATE_FORMAT,
            forks: vec![fork("a-1", vec![], vec![])],
            versions: vec![],
            trials: vec![],
            evaluations: vec![HoldoutReceipt {
                seal_id: "holdout".into(),
                state: "completed".into(),
                code_identity: "1".into(),
                parameters: "1".into(),
                data_version: "1".into(),
                summary: None,
            }],
            promotions: vec![],
        };
        let claimed = kernel_claims(&snap, &actual, "holdout");
        assert!(
            Service::reconcile(&actual, &claimed).agrees(),
            "a fork with no owned rows has nothing to publish"
        );

        let still = StateDocument {
            forks: vec![fork(
                "a-1",
                vec![OwnedHead {
                    table: "trades".into(),
                    sequence: 1,
                    rows: 1,
                }],
                vec![],
            )],
            ..actual
        };
        let claimed = kernel_claims(&snap, &still, "holdout");
        assert!(!Service::reconcile(&still, &claimed).agrees());
    }

    #[test]
    fn an_idle_agent_is_not_a_missing_fork() {
        let snap = k::Snapshot {
            agents: vec![
                k::Agent {
                    id: 1,
                    budget: 1,
                    used: 1,
                    token_budget: 1,
                    tokens_used: 0,
                    fork_id: 1,
                    sandbox: 0,
                },
                k::Agent {
                    id: 2,
                    budget: 1,
                    used: 0,
                    token_budget: 1,
                    tokens_used: 0,
                    fork_id: 2,
                    sandbox: 1,
                },
            ],
            trials: vec![k::Trial {
                id: 2,
                family: 1,
                agent: 1,
                code: 3,
                params: 4,
                data_version: 1,
                decision_time: 5,
                score: 0,
                sharpe: 0,
                skew: 0,
                kurt: 0,
                samples: 0,
                tokens: 0,
            }],
            ..k::Snapshot::default()
        };
        let actual = StateDocument {
            format: h5i_db_service::STATE_FORMAT,
            forks: vec![fork("a-1", vec![], vec![])],
            versions: vec![],
            trials: vec![],
            evaluations: vec![],
            promotions: vec![],
        };
        let claimed = kernel_claims(&snap, &actual, "holdout");
        assert_eq!(claimed.forks.len(), 1);
        assert_eq!(claimed.forks[0].name, "a-1");
        assert!(claimed.trials.iter().any(|row| row.run_id == "p-2"));
    }

    #[test]
    fn a_rejected_backtest_reconciles_with_the_ledger_trial() {
        let snap = k::Snapshot {
            agents: vec![k::Agent {
                id: 1,
                budget: 1,
                used: 1,
                token_budget: 1,
                tokens_used: 0,
                fork_id: 1,
                sandbox: 0,
            }],
            trials: vec![k::Trial {
                id: 2,
                family: 1,
                agent: 1,
                code: 3,
                params: 4,
                data_version: 1,
                decision_time: 5,
                score: 0,
                sharpe: 0,
                skew: 0,
                kurt: 0,
                samples: 0,
                tokens: 0,
            }],
            ..k::Snapshot::default()
        };
        let rejected = TrialState {
            fork: "a-1".into(),
            run_id: "p-2".into(),
            ts_ns: 7,
            config_digest: "d".into(),
            code_identity: "3".into(),
            parameters: "4".into(),
            data_version: "1".into(),
            decision_time_ns: Some(5),
            scope: "fork=a-1;window=all".into(),
            result_summary: "{\"records_processed\":0,\"rejected\":\"no instruments\"}".into(),
        };
        let actual = StateDocument {
            format: h5i_db_service::STATE_FORMAT,
            forks: vec![fork("a-1", vec![], vec![])],
            versions: vec![],
            trials: vec![rejected],
            evaluations: vec![],
            promotions: vec![],
        };
        let claimed = kernel_claims(&snap, &actual, "holdout");
        assert!(
            Service::reconcile(&actual, &claimed).agrees(),
            "the database trial is the ledger trial"
        );
        assert!(claimed.trials[0].result_summary.contains("rejected"));

        let missing = StateDocument {
            trials: vec![],
            ..actual
        };
        assert!(
            !Service::reconcile(&missing, &kernel_claims(&snap, &missing, "holdout")).agrees(),
            "a ledger trial with no bt_trial row does not reconcile"
        );
    }

    #[test]
    fn the_equity_tail_is_the_appended_scan_and_a_truncated_scan_is_refused() {
        let rows = vec![
            serde_json::json!({"equity": 10.0}),
            serde_json::json!({"equity": 11.0}),
            serde_json::json!({"equity": 12.1}),
        ];
        assert_eq!(equity_tail(&rows, 1, false).unwrap(), vec![11.0, 12.1]);
        assert!(equity_tail(&rows, 1, true).is_none());
        assert!(equity_tail(&rows, 4, false).is_none());
        assert!(equity_tail(&[serde_json::json!({"equity": "no"})], 0, false).is_none());
        let missing = equity_start(Err("query: table \"bt_equity\" not found".into()));
        assert_eq!(missing, Some(0));
        assert_eq!(equity_start(Err("query: lock timeout".into())), None);
        let counted = QueryResult {
            decision_time_ns: 0,
            rows: vec![serde_json::json!({"n": 4})],
            truncated: false,
        };
        assert_eq!(equity_start(Ok(counted)), Some(4));
    }

    #[test]
    fn a_failed_trial_still_deflates_the_family() {
        let good = TrialStats {
            sharpe: 0.5,
            skew: 0.0,
            kurtosis: 3.0,
            samples: 100,
        };
        let alone = deflated_sharpe(&[good]);
        let (crowded, sharpe, _, _, samples) = record_fields(&[good], no_returns());
        assert_eq!(samples, 0);
        assert_eq!(sharpe, encode_stat(0.0));
        assert!(alone > 500_000, "{alone}");
        assert!(crowded < alone, "{crowded} vs {alone}");
    }
}
