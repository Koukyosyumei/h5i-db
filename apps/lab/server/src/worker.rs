//! Delivers a ledger effect to h5i-db and turns the result into a command.
//!
//! The kernel never sees the database. A retried delivery uses the same
//! idempotency key, so a second pass does not run the strategy again and does
//! not open the holdout a second time.
//!
//! The deflated Sharpe ratio is computed here, from the family's stored trial
//! statistics and this run's equity curve. The kernel stores the integer it
//! is given. It does not evaluate the formula.
//!
//! A trial runs the agent's program ([`crate::strategy`]) in the backtest
//! engine on the agent's fork, at the requested decision time. The holdout
//! runs the frozen program once over the sealed span and passes when it made
//! money after costs.

use std::future::Future;
use std::sync::Arc;

use h5i_app_core::TenantId;
use h5i_app_pg::Engine;
use h5i_db_backtest::engine::{Context, Engine as BacktestEngine, Strategy};
use h5i_db_backtest::replay::{Replay, priority};
use h5i_db_backtest::types::{Money, UnixNanos};
use h5i_db_backtest::window::TimeWindow;
use h5i_db_core::database::ReadAt;
use h5i_db_service::{
    BacktestRequest, CreateFork, ForkFrom, ForkState, HoldoutCall, HoldoutOpened, HoldoutReceipt,
    PromoteOne, PromoteTable, PromotionState, ReadPoint, Service,
    StateDocument, TrialState,
};
use lab_kernel as k;

use crate::deflated::{
    TrialStats, decode_stat, deflated_sharpe, encode_stat, no_returns, stats_from_equity,
};
use crate::measure::measurement;
use crate::strategy::{Program, RuleStrategy, Tally};
use crate::{Lab, LabStore};

/// Starting cash of every trial and of the holdout run, in units.
pub const STARTING_CASH: f64 = 100_000.0;

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
                    code_identity: code_identity(&text_body(snap, trial.code)),
                    parameters: String::from_utf8_lossy(&text_body(snap, trial.params))
                        .into_owned(),
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

enum HoldoutOutcome {
    Summary(serde_json::Value),
    Used(String),
}

/// Body of the text with this id, or empty when there is none.
pub fn text_body(snap: &k::Snapshot, id: u64) -> Vec<u8> {
    snap.texts
        .iter()
        .find(|row| row.id == id)
        .map(|row| row.body.clone())
        .unwrap_or_default()
}

/// What h5i-db records as the code identity: a content hash of the code text.
pub fn code_identity(code: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(code).to_hex())
}

/// The program a trial or holdout runs. A program that does not parse still
/// goes to the engine, which refuses it at the start, so h5i-db records the
/// attempt like any other rejected run.
enum Run {
    Rules(RuleStrategy),
    Refused(String),
}

impl Run {
    fn new(code: &[u8], params: &[u8]) -> Self {
        let code = String::from_utf8_lossy(code);
        let params =
            serde_json::from_slice::<serde_json::Value>(params).unwrap_or(serde_json::Value::Null);
        match Program::compile(&code, &params) {
            Ok(program) => Run::Rules(RuleStrategy::new(program)),
            Err(error) => Run::Refused(format!("strategy: {error}")),
        }
    }

    fn tally(&self) -> Tally {
        match self {
            Run::Rules(rules) => rules.tally,
            Run::Refused(_) => Tally::default(),
        }
    }

    fn curve(&self) -> &[(i64, f64)] {
        match self {
            Run::Rules(rules) => &rules.curve,
            Run::Refused(_) => &[],
        }
    }
}

impl Strategy for Run {
    fn on_start(&mut self, ctx: &mut Context<'_>) -> h5i_db_backtest::error::Result<()> {
        match self {
            Run::Rules(rules) => rules.on_start(ctx),
            Run::Refused(error) => Err(h5i_db_backtest::error::BacktestError::invalid(
                error.clone(),
            )),
        }
    }

    fn on_event(
        &mut self,
        ctx: &mut Context<'_>,
        record: &h5i_db_backtest::event::Record,
    ) -> h5i_db_backtest::error::Result<()> {
        match self {
            Run::Rules(rules) => rules.on_event(ctx, record),
            Run::Refused(_) => Ok(()),
        }
    }

    fn on_fill(
        &mut self,
        ctx: &mut Context<'_>,
        fill: &h5i_db_backtest::order::Fill,
    ) -> h5i_db_backtest::error::Result<()> {
        match self {
            Run::Rules(rules) => rules.on_fill(ctx, fill),
            Run::Refused(_) => Ok(()),
        }
    }
}

/// Run `run` over the sealed span of `seal_id`, in memory, on a handle that
/// holds the one holdout grant. Nothing is written to the database; the
/// summary goes into the evaluation record.
async fn evaluate_sealed(
    db: &h5i_db_core::Database,
    seal_id: &str,
    run: &mut Run,
) -> Result<serde_json::Value, String> {
    use h5i_db_backtest::store;
    let path = h5i_db_core::layout::seal_path(seal_id);
    let bytes = db
        .backend()
        .get_opt(&path)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("seal {seal_id} is not in h5i-db"))?;
    let seal: h5i_db_core::Seal = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    let window = TimeWindow::new(UnixNanos::new(seal.start_ns), UnixNanos::new(seal.end_ns))
        .map_err(|e| e.to_string())?;
    let instruments = store::read_instruments(db, ReadAt::Latest)
        .await
        .map_err(|e| e.to_string())?;
    let book = store::read_book_events(db, ReadAt::Latest, Some(window))
        .await
        .map_err(|e| e.to_string())?;
    let trades = store::trade_source(db, ReadAt::Latest, Some(window))
        .await
        .map_err(|e| e.to_string())?;
    let mut replay = Replay::builder()
        .stream("book", priority::SNAPSHOT, book)
        .source("trades", priority::TRADE, trades)
        .build()
        .map_err(|e| e.to_string())?;
    let cash = Money::from_f64(STARTING_CASH).map_err(|e| e.to_string())?;
    let mut engine = BacktestEngine::builder(instruments)
        .starting_cash(cash)
        .start(window.start())
        .build();
    let result = engine.run(&mut replay, run).map_err(|e| e.to_string())?;
    Ok(measurement(
        STARTING_CASH,
        run.curve(),
        &run.tally(),
        &serde_json::json!({
            "seal": seal_id,
            "window_ns": [seal.start_ns, seal.end_ns],
            "records": result.records_processed,
        }),
    ))
}

/// Run `code` with `params` over the sealed span of `seal_id` on `db`, which
/// has to be a handle holding that seal's grant to see anything.
pub async fn evaluate_holdout_run(
    db: &h5i_db_core::Database,
    seal_id: &str,
    code: &[u8],
    params: &[u8],
) -> Result<serde_json::Value, String> {
    let mut run = Run::new(code, params);
    evaluate_sealed(db, seal_id, &mut run).await
}

/// A holdout passes when the frozen program ran and ended above its starting cash.
pub fn holdout_passed(summary: &serde_json::Value) -> bool {
    summary.get("error").is_none()
        && summary
            .get("net")
            .and_then(|value| value.as_f64())
            .is_some_and(|net| net > 0.0)
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

struct TrialMeasure {
    score: u64,
    stats: TrialStats,
    curve: serde_json::Value,
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
        let code = text_body(&snap, pending.code);
        let params = text_body(&snap, pending.params);
        let identity = code_identity(&code);
        let parameters = String::from_utf8_lossy(&params).into_owned();
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
            let mut run = Run::new(&code, &params);
            let cash = Money::from_f64(STARTING_CASH).map_err(|e| e.to_string())?;
            let receipt = handle
                .backtest(
                    &BacktestRequest {
                        idempotency_key: format!("bt-{effect_key}"),
                        run_id: format!("p-{pending_id}"),
                        code_identity: identity,
                        parameters,
                        decision_time_ns: decision,
                        starting_cash_raw: cash.raw() as i64,
                        read_at: ReadPoint::Latest,
                    },
                    &mut run,
                )
                .await
                .map_err(|e| e.to_string())?;
            // The engine's account as the run went. A replay of a finished
            // run does not call the strategy, so it has no curve to measure.
            let points = (!run.curve().is_empty()).then(|| run.curve().to_vec());
            let ids = serde_json::json!({
                "run_id": receipt.run_id,
                "code_identity": receipt.code_identity,
                "data_version": receipt.data_version,
            });
            Ok(match points {
                Some(points) => {
                    let curve: Vec<f64> = points.iter().map(|(_, equity)| *equity).collect();
                    let measured = measurement(STARTING_CASH, &points, &run.tally(), &ids);
                    let ret = measured["return"].as_f64().unwrap_or(0.0);
                    TrialMeasure {
                        score: encode_stat(ret),
                        stats: stats_from_equity(&curve),
                        curve: measured,
                    }
                }
                // A replay of a finished run, or a curve that cannot be
                // attributed to this run: no returns, and the trial counts.
                None => {
                    let mut curve = ids;
                    curve["unmeasured"] =
                        serde_json::json!("the run was a replay; its curve was not observed");
                    TrialMeasure {
                        score: encode_stat(0.0),
                        stats: no_returns(),
                        curve,
                    }
                }
            })
        })
        .await;
        let measured = match ran {
            Ok(measured) => measured,
            // The engine answered. Retrying will not change the program or the
            // data, and leaving the pending open would spend budget without
            // counting the attempt. The empty trial still counts, and it still
            // moves the family's corrected score.
            Err(error) if error.starts_with("backtest:") => {
                self.note(org, "backtest", &error).await;
                TrialMeasure {
                    score: encode_stat(0.0),
                    stats: no_returns(),
                    curve: serde_json::json!({ "rejected": error }),
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
            curve: serde_json::to_vec(&measured.curve).map_err(|e| e.to_string())?,
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
        let code = text_body(&snap, family.frozen_code);
        let params = text_body(&snap, family.frozen_params);
        let identity = code_identity(&code);
        let parameters = String::from_utf8_lossy(&params).into_owned();
        let data_version = family.frozen_data.to_string();
        let opened = on_db(move || async move {
            match db
                .begin_holdout(&HoldoutCall {
                    idempotency_key: format!("ho-{effect_key}"),
                    seal_id: seal_id.clone(),
                    code_identity: identity,
                    parameters,
                    data_version,
                })
                .await
            {
                Ok(HoldoutOpened::Completed(receipt)) => Ok(HoldoutOutcome::Summary(
                    receipt.summary.unwrap_or(serde_json::Value::Null),
                )),
                Ok(HoldoutOpened::Grant(grant)) => {
                    let handle = grant.handle();
                    let mut run = Run::new(&code, &params);
                    let summary = match evaluate_sealed(handle.database(), &seal_id, &mut run).await
                    {
                        Ok(summary) => summary,
                        Err(error) => serde_json::json!({ "error": error }),
                    };
                    grant
                        .finish(summary.clone())
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(HoldoutOutcome::Summary(summary))
                }
                Err(error) if error.code() == "holdout_used" => {
                    Ok(HoldoutOutcome::Used(error.to_string()))
                }
                Err(error) => Err(error.to_string()),
            }
        })
        .await?;
        let summary = match opened {
            HoldoutOutcome::Summary(summary) => summary,
            HoldoutOutcome::Used(detail) => {
                self.note(org, "holdout", &detail).await;
                return Err(detail);
            }
        };
        let passed = if holdout_passed(&summary) {
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
    use h5i_db_service::{OwnedHead, PinReceipt};

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
                label: 0,
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
                    label: 0,
                },
                k::Agent {
                    id: 2,
                    budget: 1,
                    used: 0,
                    token_budget: 1,
                    tokens_used: 0,
                    fork_id: 2,
                    sandbox: 1,
                    label: 0,
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
                label: 0,
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
