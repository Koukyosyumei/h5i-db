//! A capability handle: one fork, one decision-time ceiling, no seals except
//! a holdout grant already on the database. It can ingest, query, plan,
//! apply, and backtest. It cannot seal, promote, or open another fork.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use base64::Engine;
use h5i_db_core::{
    Database, IDEMPOTENCY_FINGERPRINT_META_KEY, IDEMPOTENCY_META_KEY, MutationPlan, ReadAt,
    WriteOptions,
};
use h5i_db_query::{H5iSession, ResearchPin, SessionOptions};

use crate::canonical::request_fingerprint;
use crate::effect::with_effect;
use crate::error::{Result, ServiceError, engine, invalid};
use crate::types::{
    ApplyRequest, BacktestReceipt, BacktestRequest, CommitReceipt, IngestRequest, PlanReceipt,
    PlanRequest, QueryRequest, QueryResult, ReadPoint, TrialState, WriteMode,
};

const DEFAULT_MAX_ROWS: usize = 10_000;
const HARD_MAX_ROWS: usize = 100_000;

/// Read and write access bounded by a fork and a decision time.
#[derive(Clone)]
pub struct Handle {
    db: Database,
    fork: Option<String>,
    ceiling_ns: Option<i64>,
}

impl Handle {
    pub(crate) fn new(db: Database, fork: Option<String>, ceiling_ns: Option<i64>) -> Self {
        Self {
            db,
            fork,
            ceiling_ns,
        }
    }

    pub fn fork_name(&self) -> Option<&str> {
        self.fork.as_deref()
    }

    pub fn ceiling_ns(&self) -> Option<i64> {
        self.ceiling_ns
    }

    /// The database this handle reads and writes. Already scoped.
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Sequence the next ingest or plan must name in `expected_version`.
    ///
    /// A fork name that still resolves to its pin has no shadow yet. The next
    /// write copies that pin into a new table whose first manifest is sequence
    /// 0, and the compare-and-swap is against that 0. The pin sequence is a
    /// different number: promote names it as `expected_base`.
    pub async fn table_head(&self, table: &str) -> Result<u64> {
        writable_head(&self.db, table).await
    }

    pub async fn list_versions(&self, table: &str) -> Result<Vec<h5i_db_core::VersionSummary>> {
        self.db.list_versions(table).await.map_err(engine)
    }

    pub async fn ingest(&self, request: &IngestRequest) -> Result<CommitReceipt> {
        let fingerprint = request_fingerprint(request)?;
        let batches = decode_ipc(&request.ipc_b64)?;
        let db = self.db.clone();
        let request = request.clone();
        let key = request.idempotency_key.clone();
        let stored_fingerprint = fingerprint.clone();
        with_effect(&self.db, &key, &fingerprint, "ingest", move || async move {
            let fingerprint = stored_fingerprint;
            let opts = write_options(
                &request.idempotency_key,
                &fingerprint,
                request.expected_version,
                request.note.clone(),
            );
            let result = match &request.mode {
                WriteMode::Append => db
                    .append(&request.table, batches, opts)
                    .await
                    .map_err(engine)?,
                WriteMode::Write => db
                    .write(&request.table, batches, opts)
                    .await
                    .map_err(engine)?,
                WriteMode::ReplaceRange { start, end } => db
                    .replace_range(&request.table, *start, *end, batches, opts)
                    .await
                    .map_err(engine)?,
            };
            Ok(CommitReceipt::from(result))
        })
        .await
    }

    pub async fn plan(&self, request: &PlanRequest) -> Result<PlanReceipt> {
        let fingerprint = request_fingerprint(request)?;
        let db = self.db.clone();
        let request = request.clone();
        let key = request.idempotency_key.clone();
        let stored_fingerprint = fingerprint.clone();
        with_effect(&self.db, &key, &fingerprint, "plan", move || async move {
            let fingerprint = stored_fingerprint;
            if let Some(existing) = find_plan(&db, &request).await? {
                return Ok(PlanReceipt::from(&existing));
            }
            let head = writable_head(&db, &request.table).await?;
            if head != request.expected_version {
                return Err(ServiceError::VersionConflict {
                    table: request.table.clone(),
                    expected: request.expected_version,
                    actual: head,
                });
            }
            let batches = decode_ipc(&request.ipc_b64)?;
            let opts = write_options(
                &request.idempotency_key,
                &fingerprint,
                request.expected_version,
                request.note.clone(),
            );
            let plan = match &request.mode {
                WriteMode::Append => {
                    return Err(invalid(
                        "plan covers write and replace_range; append publishes directly",
                    ));
                }
                WriteMode::Write => db
                    .plan_write(&request.table, batches, opts)
                    .await
                    .map_err(engine)?,
                WriteMode::ReplaceRange { start, end } => db
                    .plan_replace_range(&request.table, *start, *end, batches, opts)
                    .await
                    .map_err(engine)?,
            };
            if plan.base_version != request.expected_version {
                let _ = db.discard_plan(&request.table, plan.plan_id).await;
                return Err(ServiceError::VersionConflict {
                    table: request.table.clone(),
                    expected: request.expected_version,
                    actual: plan.base_version,
                });
            }
            Ok(PlanReceipt::from(&plan))
        })
        .await
    }

    pub async fn apply(&self, request: &ApplyRequest) -> Result<CommitReceipt> {
        let fingerprint = request_fingerprint(request)?;
        let db = self.db.clone();
        let request = request.clone();
        let key = request.idempotency_key.clone();
        with_effect(&self.db, &key, &fingerprint, "apply", move || async move {
            let plan = db
                .load_plan(&request.table, request.plan_id)
                .await
                .map_err(engine)?;
            if plan.base_version != request.expected_version {
                return Err(ServiceError::VersionConflict {
                    table: request.table,
                    expected: request.expected_version,
                    actual: plan.base_version,
                });
            }
            let result = db.apply_plan(&plan).await.map_err(engine)?;
            Ok(CommitReceipt::from(result))
        })
        .await
    }

    /// Query at `decision_time_ns`. Arrival and event time are both that instant.
    ///
    /// A request later than this handle's ceiling is [`ServiceError::ScopeDenied`].
    /// An earlier request uses the earlier instant, so a handle cannot be talked
    /// into a wider view.
    pub async fn query(&self, request: &QueryRequest) -> Result<QueryResult> {
        let decision = self.effective_decision(request.decision_time_ns)?;
        let cap = request
            .max_rows
            .unwrap_or(DEFAULT_MAX_ROWS)
            .min(HARD_MAX_ROWS);
        let session = H5iSession::new_pinned(
            Arc::new(self.db.clone()),
            SessionOptions::default(),
            ResearchPin::at(ReadAt::AsOf(decision)).with_event_time_cutoff_ns(decision),
        )
        .await
        .map_err(|error| ServiceError::engine("query", error.to_string()))?;
        let frame = session
            .sql(&request.sql)
            .await
            .map_err(|error| ServiceError::engine("query", error.to_string()))?;
        let batches = frame
            .collect()
            .await
            .map_err(|error| ServiceError::engine("query", error.to_string()))?;
        let (batches, truncated) = cap_batches(batches, cap);
        Ok(QueryResult {
            decision_time_ns: decision,
            rows: batches_to_json(&batches)?,
            truncated,
        })
    }

    /// Run `strategy` on this handle's fork and record a trial.
    ///
    /// The same key and request returns the stored receipt and does not call
    /// `strategy`. A run the engine rejects still leaves a `bt_trial` row.
    /// There is no flag that skips the trial row.
    pub async fn backtest(
        &self,
        request: &BacktestRequest,
        strategy: &mut dyn h5i_db_backtest::engine::Strategy,
    ) -> Result<BacktestReceipt> {
        let fingerprint = request_fingerprint(request)?;
        let decision = self.effective_decision(request.decision_time_ns)?;
        if let ReadPoint::AsOfNs(arrived) = request.read_at {
            self.effective_decision(arrived)?;
        }
        if let Some(existing) = load_effect_backtest(&self.db, &request.idempotency_key).await? {
            if existing_matches(&existing, &fingerprint) {
                return Ok(existing.receipt);
            }
            return Err(ServiceError::IdempotencyConflict {
                key: request.idempotency_key.clone(),
                detail: "this key already recorded a different backtest".into(),
            });
        }
        if let Some(stored) = self.find_trial(&request.run_id).await? {
            if stored.code_identity == request.code_identity
                && stored.parameters == request.parameters
                && stored.decision_time_ns == Some(request.decision_time_ns)
            {
                if let Some(detail) = rejected_detail(&stored.result_summary) {
                    return Err(ServiceError::engine("backtest", detail));
                }
                let receipt = receipt_from_trial(&stored);
                let key = request.idempotency_key.clone();
                crate::effect::with_effect(&self.db, &key, &fingerprint, "backtest", || async {
                    Ok(receipt.clone())
                })
                .await?;
                return Ok(receipt);
            }
            return Err(ServiceError::IdempotencyConflict {
                key: request.idempotency_key.clone(),
                detail: format!(
                    "run_id {:?} is already a trial with a different identity",
                    request.run_id
                ),
            });
        }

        let mut db = self.db.clone();
        db = db.with_event_ceiling(decision);
        let spec = h5i_db_backtest::run::RunSpec::new(
            request.run_id.clone(),
            h5i_db_backtest::types::Money::from_raw(request.starting_cash_raw),
        )
        .code_identity(request.code_identity.clone())
        .parameters(request.parameters.clone())
        .decision_time_ns(request.decision_time_ns)
        .read_at(to_read_at(&request.read_at));
        if let Err(error) =
            h5i_db_backtest::run::run_in_place(&db, spec.clone(), strategy, |builder| builder).await
        {
            let detail = error.to_string();
            h5i_db_backtest::run::record_rejection(&db, &spec, &detail)
                .await
                .map_err(|error| ServiceError::engine("storage", error.to_string()))?;
            return Err(ServiceError::engine("backtest", detail));
        }
        let stored = self
            .find_trial(&request.run_id)
            .await?
            .ok_or_else(|| ServiceError::engine("backtest", "the run wrote no bt_trial row"))?;
        let receipt = receipt_from_trial(&stored);
        let key = request.idempotency_key.clone();
        crate::effect::with_effect(&self.db, &key, &fingerprint, "backtest", || async {
            Ok(receipt.clone())
        })
        .await?;
        Ok(receipt)
    }

    pub async fn list_trials(&self) -> Result<Vec<TrialState>> {
        let fork = self.fork.clone().unwrap_or_else(|| "base".to_string());
        read_trials(&self.db, &fork).await
    }

    fn effective_decision(&self, requested: i64) -> Result<i64> {
        if let Some(ceiling) = self.ceiling_ns
            && requested > ceiling
        {
            return Err(ServiceError::ScopeDenied {
                detail: format!("decision time {requested} is after the handle ceiling {ceiling}"),
            });
        }
        Ok(requested)
    }

    async fn find_trial(&self, run_id: &str) -> Result<Option<TrialState>> {
        let fork = self.fork.clone().unwrap_or_else(|| "base".to_string());
        let trials = read_trials(&self.db, &fork).await?;
        Ok(trials.into_iter().find(|trial| trial.run_id == run_id))
    }
}

fn rejected_detail(summary: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(summary)
        .ok()?
        .get("rejected")
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

fn existing_matches(existing: &StoredBacktest, fingerprint: &str) -> bool {
    existing.fingerprint == fingerprint
}

struct StoredBacktest {
    fingerprint: String,
    receipt: BacktestReceipt,
}

async fn load_effect_backtest(db: &Database, key: &str) -> Result<Option<StoredBacktest>> {
    let path = h5i_db_core::layout::service_effect_path(key);
    let Some(bytes) = db.backend().get_opt(&path).await.map_err(engine)? else {
        return Ok(None);
    };
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| ServiceError::engine("corruption", error.to_string()))?;
    let fingerprint = value
        .get("fingerprint")
        .and_then(|item| item.as_str())
        .unwrap_or("")
        .to_string();
    let operation = value
        .get("operation")
        .and_then(|item| item.as_str())
        .unwrap_or("");
    if operation != "backtest" {
        return Err(ServiceError::IdempotencyConflict {
            key: key.to_string(),
            detail: format!("key {key} already recorded operation {operation}"),
        });
    }
    let receipt = serde_json::from_value(
        value
            .get("result")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )
    .map_err(|error| ServiceError::engine("metadata", error.to_string()))?;
    Ok(Some(StoredBacktest {
        fingerprint,
        receipt,
    }))
}

fn receipt_from_trial(trial: &TrialState) -> BacktestReceipt {
    BacktestReceipt {
        run_id: trial.run_id.clone(),
        fork: trial.fork.clone(),
        config_digest: trial.config_digest.clone(),
        code_identity: trial.code_identity.clone(),
        parameters: trial.parameters.clone(),
        data_version: trial.data_version.clone(),
        decision_time_ns: trial.decision_time_ns,
        scope: trial.scope.clone(),
        result_summary: trial.result_summary.clone(),
    }
}

pub(crate) async fn read_trials(db: &Database, fork: &str) -> Result<Vec<TrialState>> {
    let names = db.list_tables().await.map_err(engine)?;
    if !names.iter().any(|entry| entry.name == "bt_trial") {
        return Ok(Vec::new());
    }
    let (batches, _) = db
        .scan(
            "bt_trial",
            ReadAt::Latest,
            h5i_db_core::ScanOptions::default(),
        )
        .await
        .map_err(engine)?;
    let mut trials = Vec::new();
    for batch in &batches {
        let rows = batch.num_rows();
        for row in 0..rows {
            trials.push(trial_from_row(batch, row, fork)?);
        }
    }
    trials.sort_by(|left, right| {
        left.run_id
            .cmp(&right.run_id)
            .then(left.ts_ns.cmp(&right.ts_ns))
    });
    Ok(trials)
}

fn trial_from_row(batch: &RecordBatch, row: usize, fork: &str) -> Result<TrialState> {
    use arrow::array::{Array, Int64Array, StringArray, TimestampNanosecondArray};
    let schema = batch.schema();
    let string = |name: &str| -> Result<String> {
        let column = batch.column(
            schema
                .index_of(name)
                .map_err(|error| ServiceError::engine("schema_mismatch", error.to_string()))?,
        );
        let values = column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ServiceError::engine("schema_mismatch", format!("{name} is not utf8"))
            })?;
        Ok(values.value(row).to_string())
    };
    let ts = batch
        .column(
            schema
                .index_of("ts")
                .map_err(|error| ServiceError::engine("schema_mismatch", error.to_string()))?,
        )
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| ServiceError::engine("schema_mismatch", "bt_trial.ts is not a timestamp"))?
        .value(row);
    let decision = batch
        .column(
            schema
                .index_of("decision_time_ns")
                .map_err(|error| ServiceError::engine("schema_mismatch", error.to_string()))?,
        )
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            ServiceError::engine("schema_mismatch", "decision_time_ns is not an int64")
        })?;
    Ok(TrialState {
        fork: fork.to_string(),
        run_id: string("run_id")?,
        ts_ns: ts,
        config_digest: string("config_digest")?,
        code_identity: string("code_identity")?,
        parameters: string("parameters")?,
        data_version: string("data_version")?,
        decision_time_ns: if decision.is_null(row) {
            None
        } else {
            Some(decision.value(row))
        },
        scope: string("scope")?,
        result_summary: string("result_summary")?,
    })
}

fn to_read_at(point: &ReadPoint) -> ReadAt {
    match point {
        ReadPoint::Latest => ReadAt::Latest,
        ReadPoint::AsOfNs(ns) => ReadAt::AsOf(*ns),
        ReadPoint::Version(version) => ReadAt::Version(*version),
    }
}

/// Head sequence a write will compare-and-swap against.
///
/// See [`Handle::table_head`].
async fn writable_head(db: &Database, table: &str) -> Result<u64> {
    let resolved = db.resolve(table, ReadAt::Latest).await.map_err(engine)?;
    if let Some(fork) = db.fork()
        && fork.pin(resolved.entry.table_id).is_some()
    {
        return Ok(0);
    }
    Ok(resolved.head_sequence)
}

fn write_options(
    key: &str,
    fingerprint: &str,
    expected_version: u64,
    note: Option<String>,
) -> WriteOptions {
    let mut user_meta = serde_json::Map::new();
    user_meta.insert(
        IDEMPOTENCY_META_KEY.to_string(),
        serde_json::Value::String(key.to_string()),
    );
    user_meta.insert(
        IDEMPOTENCY_FINGERPRINT_META_KEY.to_string(),
        serde_json::Value::String(fingerprint.to_string()),
    );
    WriteOptions {
        expected_version: Some(expected_version),
        note,
        user_meta,
        idempotency_key: Some(key.to_string()),
        idempotency_fingerprint: Some(fingerprint.to_string()),
    }
}

async fn find_plan(db: &Database, request: &PlanRequest) -> Result<Option<MutationPlan>> {
    let plans = match db.list_plans(&request.table).await {
        Ok(plans) => plans,
        Err(error) if error.code() == "table_not_found" => return Ok(None),
        Err(error) => return Err(engine(error)),
    };
    for plan in plans {
        let key = plan
            .user_meta
            .get(IDEMPOTENCY_META_KEY)
            .and_then(|value| value.as_str());
        let fingerprint = plan
            .user_meta
            .get(IDEMPOTENCY_FINGERPRINT_META_KEY)
            .and_then(|value| value.as_str());
        if key == Some(request.idempotency_key.as_str()) {
            let wanted = request_fingerprint(request)?;
            if fingerprint == Some(wanted.as_str()) {
                return Ok(Some(plan));
            }
            return Err(ServiceError::IdempotencyConflict {
                key: request.idempotency_key.clone(),
                detail: format!(
                    "plan for {:?} was built from a different request",
                    request.table
                ),
            });
        }
    }
    Ok(None)
}

pub(crate) fn decode_ipc(ipc_b64: &str) -> Result<Vec<RecordBatch>> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(ipc_b64)
        .map_err(|error| invalid(format!("ipc base64: {error}")))?;
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|error| invalid(format!("ipc stream: {error}")))?;
    let mut out = Vec::new();
    for batch in reader {
        out.push(batch.map_err(|error| invalid(format!("ipc batch: {error}")))?);
    }
    Ok(out)
}

fn cap_batches(batches: Vec<RecordBatch>, cap: usize) -> (Vec<RecordBatch>, bool) {
    let mut left = cap;
    let mut out = Vec::new();
    let mut truncated = false;
    for batch in batches {
        if left == 0 {
            truncated = batch.num_rows() > 0 || truncated;
            if batch.num_rows() > 0 {
                truncated = true;
            }
            break;
        }
        if batch.num_rows() <= left {
            left -= batch.num_rows();
            out.push(batch);
        } else {
            out.push(batch.slice(0, left));
            truncated = true;
            left = 0;
        }
    }
    (out, truncated)
}

fn batches_to_json(batches: &[RecordBatch]) -> Result<Vec<serde_json::Value>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema: SchemaRef = batches[0].schema();
    for batch in batches {
        if batch.schema() != schema {
            return Err(ServiceError::engine(
                "schema_mismatch",
                "query batches do not share a schema",
            ));
        }
    }
    let mut buf = Vec::new();
    {
        let mut writer = arrow::json::writer::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, arrow::json::writer::JsonArray>(&mut buf);
        for batch in batches {
            writer
                .write(batch)
                .map_err(|error| ServiceError::engine("arrow", error.to_string()))?;
        }
        writer
            .finish()
            .map_err(|error| ServiceError::engine("arrow", error.to_string()))?;
    }
    serde_json::from_slice(&buf)
        .map_err(|error| ServiceError::engine("metadata", error.to_string()))
}
