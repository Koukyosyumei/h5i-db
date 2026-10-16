//! Serde requests and receipts. A strategy trait object is not one of these:
//! backtest takes it beside the request, and a replay returns the stored
//! receipt without calling the strategy again.

use serde::{Deserialize, Serialize};

use h5i_db_core::{CommitResult, Fork, ForkTableKind, HoldoutRecord, PromoteResult, Seal};

/// Where a new fork pins main.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForkFrom {
    /// Current head of every table.
    Head,
    /// Last commit with `committed_at <=` this nanosecond timestamp, per table.
    AsOfNs(i64),
    /// This sequence on each table that has it. Tables without it are not pinned.
    Version(u64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateFork {
    pub idempotency_key: String,
    pub name: String,
    pub from: ForkFrom,
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkReceipt {
    pub name: String,
    pub created_at_ns: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub as_of_ns: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub depth: u32,
    pub pins: Vec<PinReceipt>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PinReceipt {
    pub table: String,
    pub sequence: u64,
    pub manifest_checksum: String,
}

impl From<&Fork> for ForkReceipt {
    fn from(fork: &Fork) -> Self {
        let mut pins: Vec<PinReceipt> = fork
            .pins
            .values()
            .map(|pin| PinReceipt {
                table: pin.table_name.clone(),
                sequence: pin.sequence,
                manifest_checksum: pin.manifest_checksum.clone(),
            })
            .collect();
        pins.sort_by(|left, right| left.table.cmp(&right.table));
        Self {
            name: fork.name.clone(),
            created_at_ns: fork.created_at_ns,
            note: fork.note.clone(),
            as_of_ns: fork.as_of_ns,
            parent: fork.parent.clone(),
            depth: fork.depth(),
            pins,
        }
    }
}

/// How an ingest or a plan writes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum WriteMode {
    Append,
    Write,
    ReplaceRange { start: i64, end: i64 },
}

/// Arrow IPC stream, standard base64, of the batches to write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestRequest {
    pub idempotency_key: String,
    pub table: String,
    pub mode: WriteMode,
    /// Head sequence the write must land on. Mandatory.
    pub expected_version: u64,
    pub ipc_b64: String,
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitReceipt {
    pub table: String,
    pub sequence: u64,
    pub op: String,
    pub rows_total: u64,
    pub segments_total: usize,
    pub segments_added: usize,
    pub segments_deduped: usize,
    pub committed_at_ns: i64,
}

impl From<CommitResult> for CommitReceipt {
    fn from(result: CommitResult) -> Self {
        Self {
            table: result.table,
            sequence: result.sequence,
            op: result.op,
            rows_total: result.rows_total,
            segments_total: result.segments_total,
            segments_added: result.segments_added,
            segments_deduped: result.segments_deduped,
            committed_at_ns: result.committed_at_ns,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanRequest {
    pub idempotency_key: String,
    pub table: String,
    pub mode: WriteMode,
    pub expected_version: u64,
    pub ipc_b64: String,
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanReceipt {
    pub plan_id: uuid::Uuid,
    pub table: String,
    pub base_version: u64,
    pub expires_at_ns: i64,
    pub checksum: String,
    pub rows_before: u64,
    pub rows_after: u64,
    pub rows_affected: u64,
}

impl From<&h5i_db_core::MutationPlan> for PlanReceipt {
    fn from(plan: &h5i_db_core::MutationPlan) -> Self {
        Self {
            plan_id: plan.plan_id,
            table: plan.table.clone(),
            base_version: plan.base_version,
            expires_at_ns: plan.expires_at_ns,
            checksum: plan.checksum.clone(),
            rows_before: plan.summary.rows_before,
            rows_after: plan.summary.rows_after,
            rows_affected: plan.summary.rows_affected,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApplyRequest {
    pub idempotency_key: String,
    pub table: String,
    pub plan_id: uuid::Uuid,
    /// Must equal the plan's `base_version`.
    pub expected_version: u64,
}

/// Arrival pin for a backtest. Independent of the decision-time ceiling,
/// which is always applied. `Latest` is the bulk-ingest case: the commit
/// clock and the event clock are not the same number.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadPoint {
    Latest,
    AsOfNs(i64),
    Version(u64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BacktestRequest {
    pub idempotency_key: String,
    pub run_id: String,
    pub code_identity: String,
    pub parameters: String,
    pub decision_time_ns: i64,
    /// Fixed-point raw units of starting cash (1e-9). See `Money`.
    pub starting_cash_raw: i64,
    pub read_at: ReadPoint,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BacktestReceipt {
    pub run_id: String,
    pub fork: String,
    pub config_digest: String,
    pub code_identity: String,
    pub parameters: String,
    pub data_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_time_ns: Option<i64>,
    pub scope: String,
    pub result_summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    /// Event time and, for this call, the arrival pin. Both axes are set to
    /// this instant. A value after the handle ceiling is `scope_denied`.
    pub decision_time_ns: i64,
    pub sql: String,
    /// Cap on returned rows. Defaults to 10_000 and cannot exceed 100_000.
    pub max_rows: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResult {
    pub decision_time_ns: i64,
    pub rows: Vec<serde_json::Value>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SealCall {
    pub idempotency_key: String,
    pub id: String,
    pub table: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SealReceipt {
    pub id: String,
    pub table: String,
    pub start_ns: i64,
    pub end_ns: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl From<Seal> for SealReceipt {
    fn from(seal: Seal) -> Self {
        Self {
            id: seal.id,
            table: seal.table,
            start_ns: seal.start_ns,
            end_ns: seal.end_ns,
            note: seal.note,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HoldoutCall {
    pub idempotency_key: String,
    pub seal_id: String,
    pub code_identity: String,
    pub parameters: String,
    pub data_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HoldoutReceipt {
    pub seal_id: String,
    pub state: String,
    pub code_identity: String,
    pub parameters: String,
    pub data_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<serde_json::Value>,
}

impl From<HoldoutRecord> for HoldoutReceipt {
    fn from(record: HoldoutRecord) -> Self {
        Self {
            seal_id: record.seal_id,
            state: record.state,
            code_identity: record.code_identity,
            parameters: record.parameters,
            data_version: record.data_version,
            summary: record.summary,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromoteTable {
    pub table: String,
    /// Pinned base sequence. `None` only for a table the fork created.
    pub expected_base: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromoteOne {
    pub idempotency_key: String,
    pub fork: String,
    pub table: PromoteTable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromoteAll {
    pub idempotency_key: String,
    pub fork: String,
    pub tables: Vec<PromoteTable>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromoteReceipt {
    pub fork: String,
    pub table: String,
    pub kind: ForkTableKind,
    pub base_sequence: u64,
    pub rows: u64,
    pub segments_linked: usize,
    pub bytes_copied: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rebased_from: Option<u64>,
}

impl From<PromoteResult> for PromoteReceipt {
    fn from(result: PromoteResult) -> Self {
        Self {
            fork: result.fork,
            table: result.table,
            kind: result.kind,
            base_sequence: result.base_sequence,
            rows: result.rows,
            segments_linked: result.segments_linked,
            bytes_copied: result.bytes_copied,
            rebased_from: result.rebased_from,
        }
    }
}

/// One ordered document a ledger can diff. Arrays are sorted by their natural key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateDocument {
    pub format: u32,
    pub forks: Vec<ForkState>,
    pub versions: Vec<VersionState>,
    pub trials: Vec<TrialState>,
    pub evaluations: Vec<HoldoutReceipt>,
    pub promotions: Vec<PromotionState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkState {
    pub name: String,
    pub created_at_ns: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub as_of_ns: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub depth: u32,
    pub pins: Vec<PinReceipt>,
    pub owned: Vec<OwnedHead>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OwnedHead {
    pub table: String,
    pub sequence: u64,
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionState {
    pub fork: String,
    pub table: String,
    pub sequence: u64,
    pub op: String,
    pub committed_at_ns: i64,
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrialState {
    pub fork: String,
    pub run_id: String,
    pub ts_ns: i64,
    pub config_digest: String,
    pub code_identity: String,
    pub parameters: String,
    pub data_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_time_ns: Option<i64>,
    pub scope: String,
    pub result_summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromotionState {
    pub fork: String,
    pub table: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fork_sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReconcileDiff {
    /// In the ledger, absent from the database.
    pub missing: Vec<String>,
    /// In the database, absent from the ledger.
    pub extra: Vec<String>,
    pub mismatches: Vec<FieldMismatch>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldMismatch {
    pub path: String,
    pub ledger: serde_json::Value,
    pub actual: serde_json::Value,
}

impl ReconcileDiff {
    pub fn agrees(&self) -> bool {
        self.missing.is_empty() && self.extra.is_empty() && self.mismatches.is_empty()
    }
}
