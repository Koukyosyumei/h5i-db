//! The worker entry point. Mutating calls take an idempotency key. A handle
//! is the capability a caller is allowed to use; this type is what builds one.

use serde::{Deserialize, Serialize};

use h5i_db_core::layout;
use h5i_db_core::{
    Database, HoldoutOpen, HoldoutRequest, HoldoutSession, IDEMPOTENCY_FINGERPRINT_META_KEY,
    IDEMPOTENCY_META_KEY, ReadAt, SealRequest,
};

use crate::canonical::request_fingerprint;
use crate::effect::with_effect;
use crate::error::{Result, ServiceError, engine, invalid};
use crate::handle::Handle;
use crate::state;
use crate::types::{
    CreateFork, ForkFrom, ForkReceipt, HoldoutCall, HoldoutReceipt, PromoteAll, PromoteOne,
    PromoteReceipt, PromoteTable, ReconcileDiff, SealCall, SealReceipt, StateDocument,
};

/// What [`Service::begin_holdout`] returns. `Grant` is the only way to read a
/// sealed span, and it is not serializable: the rows stay in the process that
/// holds the grant.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum HoldoutOpened {
    Completed(HoldoutReceipt),
    Grant(HoldoutGrant),
}

/// A live holdout evaluation. Dropping it without [`HoldoutGrant::finish`]
/// leaves the seal `opened`, which still denies every other key.
#[derive(Debug)]
pub struct HoldoutGrant {
    session: HoldoutSession,
}

impl HoldoutGrant {
    /// Read-only handle that can see this seal and still hides every other one.
    pub fn handle(&self) -> Handle {
        Handle::new(self.session.database().clone(), None, None)
    }

    pub async fn finish(self, summary: serde_json::Value) -> Result<HoldoutReceipt> {
        let record = self.session.finish(summary).await.map_err(engine)?;
        Ok(HoldoutReceipt::from(record))
    }
}

/// Library API a worker calls. Not a server, and not a role.
pub struct Service {
    db: Database,
}

impl Service {
    /// Open over a base database and finish any promote journal a crash left behind.
    pub async fn open(db: Database) -> Result<Self> {
        recover_promote_journals(&db).await?;
        Ok(Self { db })
    }

    pub fn database(&self) -> &Database {
        &self.db
    }

    /// A handle limited to `fork` (or the base, when `fork` is `None`) and to
    /// event times at or before `ceiling_ns`.
    pub async fn handle(&self, fork: Option<&str>, ceiling_ns: Option<i64>) -> Result<Handle> {
        let mut db = match fork {
            Some(name) => self.db.open_fork(name).await.map_err(engine)?,
            None => self.db.clone(),
        };
        if let Some(ceiling) = ceiling_ns {
            db = db.with_event_ceiling(ceiling);
        }
        Ok(Handle::new(db, fork.map(str::to_string), ceiling_ns))
    }

    pub async fn create_fork(&self, request: &CreateFork) -> Result<ForkReceipt> {
        let fingerprint = request_fingerprint(request)?;
        let key = request.idempotency_key.clone();
        let db = self.db.clone();
        let request = request.clone();
        let run_key = key.clone();
        let run_fingerprint = fingerprint.clone();
        with_effect(
            &self.db,
            &key,
            &fingerprint,
            "create_fork",
            move || async move {
                let key = run_key;
                let fingerprint = run_fingerprint;
                if let Some(existing) = fork_with_key(&db, &key).await? {
                    return match_fork(&existing, &key, &fingerprint);
                }
                let mut meta = serde_json::Map::new();
                meta.insert(
                    IDEMPOTENCY_META_KEY.to_string(),
                    serde_json::Value::String(key.clone()),
                );
                meta.insert(
                    IDEMPOTENCY_FINGERPRINT_META_KEY.to_string(),
                    serde_json::Value::String(fingerprint.clone()),
                );
                let at = match request.from {
                    ForkFrom::Head => ReadAt::Latest,
                    ForkFrom::AsOfNs(ns) => ReadAt::AsOf(ns),
                    ForkFrom::Version(version) => ReadAt::Version(version),
                };
                match db
                    .create_fork_at(&request.name, request.note.clone(), at, meta)
                    .await
                {
                    Ok(fork) => Ok(ForkReceipt::from(&fork)),
                    Err(h5i_db_core::Error::ForkExists { name }) => {
                        let existing = h5i_db_core::fork::load(db.backend(), &name)
                            .await
                            .map_err(engine)?;
                        match_fork(&existing, &key, &fingerprint)
                    }
                    Err(error) => Err(engine(error)),
                }
            },
        )
        .await
    }

    pub async fn list_forks(&self) -> Result<Vec<ForkReceipt>> {
        let forks = h5i_db_core::fork::list(self.db.backend())
            .await
            .map_err(engine)?;
        Ok(forks.iter().map(ForkReceipt::from).collect())
    }

    pub async fn seal(&self, request: &SealCall) -> Result<SealReceipt> {
        let fingerprint = request_fingerprint(request)?;
        let key = request.idempotency_key.clone();
        let db = self.db.clone();
        let request = request.clone();
        let run_key = key.clone();
        let run_fingerprint = fingerprint.clone();
        with_effect(&self.db, &key, &fingerprint, "seal", move || async move {
            let key = run_key;
            let fingerprint = run_fingerprint;
            let seal = db
                .seal_range(SealRequest {
                    id: request.id,
                    table: request.table,
                    start_ns: request.start_ns,
                    end_ns: request.end_ns,
                    note: request.note,
                    idempotency_key: Some(key),
                    idempotency_fingerprint: Some(fingerprint),
                })
                .await
                .map_err(engine)?;
            Ok(SealReceipt::from(seal))
        })
        .await
    }

    pub async fn begin_holdout(&self, request: &HoldoutCall) -> Result<HoldoutOpened> {
        let fingerprint = request_fingerprint(request)?;
        let opened = self
            .db
            .begin_holdout(HoldoutRequest {
                seal_id: request.seal_id.clone(),
                idempotency_key: request.idempotency_key.clone(),
                fingerprint,
                code_identity: request.code_identity.clone(),
                parameters: request.parameters.clone(),
                data_version: request.data_version.clone(),
            })
            .await
            .map_err(engine)?;
        Ok(match opened {
            HoldoutOpen::Completed(record) => {
                HoldoutOpened::Completed(HoldoutReceipt::from(record))
            }
            HoldoutOpen::Session(session) => HoldoutOpened::Grant(HoldoutGrant { session }),
        })
    }

    pub async fn promote(&self, request: &PromoteOne) -> Result<PromoteReceipt> {
        let fingerprint = request_fingerprint(request)?;
        let key = request.idempotency_key.clone();
        let db = self.db.clone();
        let request = request.clone();
        with_effect(
            &self.db,
            &key,
            &fingerprint,
            "promote",
            move || async move {
                promote_ready(&db, &request.fork, &request.table).await?;
                let result = db
                    .promote(&request.fork, &request.table.table)
                    .await
                    .map_err(engine)?;
                Ok(PromoteReceipt::from(result))
            },
        )
        .await
    }

    /// Promote every listed table, or none of them, when no concurrent writer races.
    ///
    /// The journal is written after a preflight of every table and before any
    /// promote. Each promote is one atomic commit. A crash after the journal
    /// is rolled forward by [`Service::open`], and each promote replays if it
    /// already landed. Before the journal is durable, nothing new is promised.
    /// A reader can see a prefix while the loop is in progress.
    pub async fn promote_all(&self, request: &PromoteAll) -> Result<Vec<PromoteReceipt>> {
        let fingerprint = request_fingerprint(request)?;
        let key = request.idempotency_key.clone();
        if let Some(done) = crate::effect::stored::<Vec<PromoteReceipt>>(
            &self.db,
            &key,
            &fingerprint,
            "promote_all",
        )
        .await?
        {
            return Ok(done);
        }
        if request.tables.is_empty() {
            return Err(invalid("promote_all needs at least one table"));
        }
        let mut names = std::collections::BTreeSet::new();
        for table in &request.tables {
            if !names.insert(table.table.clone()) {
                return Err(invalid(format!(
                    "promote_all lists {:?} twice",
                    table.table
                )));
            }
        }
        if let Some(journal) = load_journal(&self.db, &key).await? {
            if journal.fingerprint != fingerprint {
                return Err(ServiceError::IdempotencyConflict {
                    key: key.clone(),
                    detail: "this key already journals a different promote".into(),
                });
            }
            return finish_journal(&self.db, &journal).await;
        }
        for table in &request.tables {
            promote_ready(&self.db, &request.fork, table).await?;
        }
        let journal = PromoteJournal {
            key: key.clone(),
            fingerprint: fingerprint.clone(),
            fork: request.fork.clone(),
            tables: request.tables.clone(),
        };
        write_journal(&self.db, &journal).await?;
        finish_journal(&self.db, &journal).await
    }

    pub async fn export_state(&self) -> Result<StateDocument> {
        state::export_state(&self.db).await
    }

    pub fn reconcile(actual: &StateDocument, ledger: &StateDocument) -> ReconcileDiff {
        state::reconcile(actual, ledger)
    }
}

fn match_fork(fork: &h5i_db_core::Fork, key: &str, fingerprint: &str) -> Result<ForkReceipt> {
    let stored_key = fork
        .user_meta
        .get(IDEMPOTENCY_META_KEY)
        .and_then(|value| value.as_str());
    let stored_fingerprint = fork
        .user_meta
        .get(IDEMPOTENCY_FINGERPRINT_META_KEY)
        .and_then(|value| value.as_str());
    if stored_key == Some(key) && stored_fingerprint == Some(fingerprint) {
        return Ok(ForkReceipt::from(fork));
    }
    if stored_key == Some(key) {
        return Err(ServiceError::IdempotencyConflict {
            key: key.to_string(),
            detail: format!(
                "fork {:?} already exists for a different request",
                fork.name
            ),
        });
    }
    Err(ServiceError::engine(
        "fork_exists",
        format!("fork {:?} already exists", fork.name),
    ))
}

async fn fork_with_key(db: &Database, key: &str) -> Result<Option<h5i_db_core::Fork>> {
    let forks = h5i_db_core::fork::list(db.backend())
        .await
        .map_err(engine)?;
    Ok(forks.into_iter().find(|fork| {
        fork.user_meta
            .get(IDEMPOTENCY_META_KEY)
            .and_then(|value| value.as_str())
            == Some(key)
    }))
}

async fn promote_ready(db: &Database, fork_name: &str, table: &PromoteTable) -> Result<()> {
    let fork = h5i_db_core::fork::load(db.backend(), fork_name)
        .await
        .map_err(engine)?;
    match fork.pin_by_name(&table.table) {
        Some((_, pin)) => {
            let Some(expected) = table.expected_base else {
                return Err(invalid(format!(
                    "promote of pinned table {:?} requires its base sequence {}",
                    table.table, pin.sequence
                )));
            };
            if expected != pin.sequence {
                return Err(ServiceError::VersionConflict {
                    table: table.table.clone(),
                    expected,
                    actual: pin.sequence,
                });
            }
        }
        None => {
            if let Some(expected) = table.expected_base {
                return Err(ServiceError::VersionConflict {
                    table: table.table.clone(),
                    expected,
                    actual: 0,
                });
            }
        }
    }
    let entry = h5i_db_core::fork::load_entry(db.backend(), fork_name, &table.table)
        .await
        .map_err(engine)?;
    let Some(entry) = entry else {
        let recorded = fork
            .user_meta
            .get("h5i.promoted_created")
            .and_then(|value| value.get(&table.table))
            .is_some();
        if recorded {
            return Ok(());
        }
        return Err(invalid(format!(
            "fork {fork_name:?} does not own table {:?}",
            table.table
        )));
    };
    let Some(origin) = entry.origin.clone() else {
        return Ok(());
    };
    let base = db
        .resolve(&table.table, ReadAt::Latest)
        .await
        .map_err(engine)?;
    if base.manifest.sequence == origin.base_sequence {
        return Ok(());
    }
    let fork_db = db.open_fork(fork_name).await.map_err(engine)?;
    let fork_head = fork_db
        .resolve(&table.table, ReadAt::Latest)
        .await
        .map_err(engine)?;
    if shadow_matches(
        &base.manifest,
        fork_name,
        &entry.table_id.to_string(),
        fork_head.manifest.sequence,
    ) {
        return Ok(());
    }
    let diff = db
        .fork_diff(fork_name, Some(&table.table))
        .await
        .map_err(engine)?;
    if diff
        .tables
        .iter()
        .any(|row| row.table == table.table && row.base_moved_by_compaction_only)
    {
        return Ok(());
    }
    Err(ServiceError::PromoteConflict {
        fork: fork_name.to_string(),
        table: table.table.clone(),
        base: origin.base_sequence,
        actual: base.head_sequence,
    })
}

fn shadow_matches(
    manifest: &h5i_db_core::VersionManifest,
    fork_name: &str,
    fork_table_id: &str,
    fork_sequence: u64,
) -> bool {
    let Some(meta) = manifest
        .user_meta
        .get(h5i_db_core::fork::FORKED_FROM_META_KEY)
    else {
        return false;
    };
    meta.get("fork").and_then(|value| value.as_str()) == Some(fork_name)
        && meta.get("fork_table_id").and_then(|value| value.as_str()) == Some(fork_table_id)
        && meta.get("fork_sequence").and_then(|value| value.as_u64()) == Some(fork_sequence)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PromoteJournal {
    key: String,
    fingerprint: String,
    fork: String,
    tables: Vec<PromoteTable>,
}

async fn load_journal(db: &Database, key: &str) -> Result<Option<PromoteJournal>> {
    let path = layout::service_promote_journal_path(key);
    let Some(bytes) = db.backend().get_opt(&path).await.map_err(engine)? else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| ServiceError::engine("corruption", format!("promote journal: {error}")))
}

async fn write_journal(db: &Database, journal: &PromoteJournal) -> Result<()> {
    let path = layout::service_promote_journal_path(&journal.key);
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|error| ServiceError::engine("metadata", error.to_string()))?;
    if !db
        .backend()
        .put_if_absent(&path, bytes::Bytes::from(bytes))
        .await
        .map_err(engine)?
    {
        let existing = load_journal(db, &journal.key).await?.ok_or_else(|| {
            ServiceError::engine("storage", "promote journal vanished after a lost create")
        })?;
        if existing.fingerprint != journal.fingerprint {
            return Err(ServiceError::IdempotencyConflict {
                key: journal.key.clone(),
                detail: "this key already journals a different promote".into(),
            });
        }
        return Ok(());
    }
    db.backend()
        .sync_objects(std::slice::from_ref(&path))
        .await
        .map_err(engine)?;
    Ok(())
}

async fn finish_journal(db: &Database, journal: &PromoteJournal) -> Result<Vec<PromoteReceipt>> {
    let mut receipts = Vec::new();
    for (index, table) in journal.tables.iter().enumerate() {
        match db.promote(&journal.fork, &table.table).await {
            Ok(result) => receipts.push(PromoteReceipt::from(result)),
            Err(error) => {
                if index == 0 {
                    let path = layout::service_promote_journal_path(&journal.key);
                    let _ = db.backend().delete(&path).await;
                }
                return Err(engine(error));
            }
        }
    }
    let stored = crate::effect::with_effect(
        db,
        &journal.key,
        &journal.fingerprint,
        "promote_all",
        || async { Ok(receipts) },
    )
    .await?;
    let path = layout::service_promote_journal_path(&journal.key);
    db.backend().delete(&path).await.map_err(engine)?;
    Ok(stored)
}

async fn recover_promote_journals(db: &Database) -> Result<()> {
    let prefix = layout::service_promote_journal_prefix();
    let metas = match db.backend().list(&prefix).await {
        Ok(metas) => metas,
        Err(error) if error.code() == "storage" && error.to_string().contains("not found") => {
            return Ok(());
        }
        Err(error) => return Err(engine(error)),
    };
    let mut paths: Vec<_> = metas
        .into_iter()
        .map(|meta| meta.location)
        .filter(|path| path.as_ref().ends_with(".json"))
        .collect();
    paths.sort();
    for path in paths {
        let bytes = db.backend().get(&path).await.map_err(engine)?;
        let journal: PromoteJournal = serde_json::from_slice(&bytes).map_err(|error| {
            ServiceError::engine("corruption", format!("promote journal: {error}"))
        })?;
        finish_journal(db, &journal).await?;
    }
    Ok(())
}
