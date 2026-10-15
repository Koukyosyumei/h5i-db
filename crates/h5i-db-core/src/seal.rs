//! Sealed holdout ranges and the one evaluation that may read them.
//!
//! A seal is an application object under `seals/<id>.json`. It does not raise
//! the format's minimum reader version: an older reader simply does not hide
//! the rows. The cache is shared by every handle of one open and updated in
//! place, so a session built before the seal still filters on its next scan.
//!
//! [`Database::begin_holdout`] is the only constructor of a grant. The opened
//! record is durable before any granted read.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::database::Database;
use crate::error::{Error, Result};
use crate::layout;
use crate::scope::{HoldoutGrant, SealSpan};

const STATE_OPENED: &str = "opened";
const STATE_COMPLETED: &str = "completed";

/// One sealed half-open range, in event-time nanoseconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seal {
    pub id: String,
    pub table: String,
    pub start_ns: i64,
    pub end_ns: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_fingerprint: Option<String>,
    #[serde(default)]
    pub checksum: String,
}

impl Seal {
    fn compute_checksum(&self) -> Result<String> {
        let mut clone = self.clone();
        clone.checksum = String::new();
        Ok(crate::util::checksum_hex(&serde_json::to_vec(&clone)?))
    }

    fn seal(mut self) -> Result<Self> {
        self.checksum = self.compute_checksum()?;
        Ok(self)
    }

    fn verify(&self, object: &str) -> Result<()> {
        if self.checksum != self.compute_checksum()? {
            return Err(Error::corruption(object, "seal checksum mismatch"));
        }
        Ok(())
    }

    pub fn span(&self) -> SealSpan {
        SealSpan {
            id: self.id.clone(),
            table: self.table.clone(),
            start_ns: self.start_ns,
            end_ns: self.end_ns,
        }
    }
}

/// Arguments of [`Database::seal_range`].
#[derive(Debug, Clone)]
pub struct SealRequest {
    pub id: String,
    pub table: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub note: Option<String>,
    pub idempotency_key: Option<String>,
    pub idempotency_fingerprint: Option<String>,
}

/// Shared, in-process view of the seals of one open.
///
/// `any` is stored with `Release` after the list is published. A reader that
/// observes `true` then sees the list. The write-path benchmark never seals,
/// so it observes `false` and does not take the lock.
pub struct SealCache {
    inner: RwLock<Arc<Vec<Seal>>>,
    any: AtomicBool,
}

impl SealCache {
    pub fn empty() -> Arc<Self> {
        Self::from_seals(Vec::new())
    }

    pub fn from_seals(seals: Vec<Seal>) -> Arc<Self> {
        let any = !seals.is_empty();
        Arc::new(Self {
            inner: RwLock::new(Arc::new(seals)),
            any: AtomicBool::new(any),
        })
    }

    pub fn any(&self) -> bool {
        self.any.load(Ordering::Acquire)
    }

    pub fn snapshot(&self) -> Arc<Vec<Seal>> {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn replace(&self, seals: Vec<Seal>) {
        let any = !seals.is_empty();
        if any {
            // Publish "there is a seal" before swapping, so a reader that
            // takes the lock cannot miss a seal that was stored and then
            // raced the flag. A reader that still observes `false` ran before
            // this store.
            self.any.store(true, Ordering::Release);
        }
        *self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::new(seals);
        self.any.store(any, Ordering::Release);
    }
}

/// Durable record of one holdout evaluation. Written in state `opened` before
/// the granted read, then `completed` with the caller's summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HoldoutRecord {
    pub seal_id: String,
    pub idempotency_key: String,
    pub fingerprint: String,
    pub code_identity: String,
    pub parameters: String,
    pub data_version: String,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<serde_json::Value>,
    #[serde(default)]
    pub checksum: String,
}

impl HoldoutRecord {
    fn compute_checksum(&self) -> Result<String> {
        let mut clone = self.clone();
        clone.checksum = String::new();
        Ok(crate::util::checksum_hex(&serde_json::to_vec(&clone)?))
    }

    fn seal(mut self) -> Result<Self> {
        self.checksum = self.compute_checksum()?;
        Ok(self)
    }

    fn verify(&self, object: &str) -> Result<()> {
        if self.checksum != self.compute_checksum()? {
            return Err(Error::corruption(object, "holdout checksum mismatch"));
        }
        Ok(())
    }

    pub fn is_completed(&self) -> bool {
        self.state == STATE_COMPLETED
    }
}

/// What [`Database::begin_holdout`] returns.
#[derive(Debug)]
pub enum HoldoutOpen {
    /// This key already finished. The summary is the stored one. No second read.
    Completed(HoldoutRecord),
    /// The grant is live. Dropping it without [`HoldoutSession::finish`] leaves
    /// the record `opened`, which still denies every other key.
    Session(HoldoutSession),
}

/// A granted read of one sealed span.
#[derive(Debug)]
pub struct HoldoutSession {
    granted: Database,
    writer: Database,
    seal_id: String,
    idempotency_key: String,
    fingerprint: String,
}

impl HoldoutSession {
    /// The handle that can see this seal and still hides every other one.
    pub fn database(&self) -> &Database {
        &self.granted
    }

    pub fn seal_id(&self) -> &str {
        &self.seal_id
    }

    /// Store the summary and mark the evaluation completed.
    pub async fn finish(self, summary: serde_json::Value) -> Result<HoldoutRecord> {
        complete_holdout(
            &self.writer,
            &self.seal_id,
            &self.idempotency_key,
            &self.fingerprint,
            summary,
        )
        .await
    }
}

/// Arguments of [`Database::begin_holdout`].
#[derive(Debug, Clone)]
pub struct HoldoutRequest {
    pub seal_id: String,
    pub idempotency_key: String,
    pub fingerprint: String,
    pub code_identity: String,
    pub parameters: String,
    pub data_version: String,
}

impl Database {
    /// Seal `[start_ns, end_ns)` of one table. The same idempotency key and
    /// fingerprint returns the first seal. A different fingerprint is
    /// [`Error::IdempotencyConflict`].
    pub async fn seal_range(&self, request: SealRequest) -> Result<Seal> {
        self.ensure_writable("seal_range")?;
        self.ensure_base("seal_range")?;
        crate::database::validate_table_name(&request.id)?;
        crate::database::validate_table_name(&request.table)?;
        if request.start_ns >= request.end_ns {
            return Err(Error::invalid(format!(
                "seal range is empty: start {} must be < end {}",
                request.start_ns, request.end_ns
            )));
        }
        // The name has to resolve. A typo would otherwise hide nothing and
        // look like a successful seal.
        let _entry = self.entry(&request.table).await?;
        let path = layout::seal_path(&request.id);
        if let Some(bytes) = self.backend().get_opt(&path).await? {
            let existing = decode_seal(&bytes, path.as_ref())?;
            return match fingerprint_match(
                existing.idempotency_key.as_deref(),
                existing.idempotency_fingerprint.as_deref(),
                request.idempotency_key.as_deref(),
                request.idempotency_fingerprint.as_deref(),
            ) {
                FingerprintMatch::Same => {
                    if existing.table != request.table
                        || existing.start_ns != request.start_ns
                        || existing.end_ns != request.end_ns
                    {
                        return Err(Error::IdempotencyConflict {
                            key: request.idempotency_key.unwrap_or_default(),
                            detail: format!(
                                "seal {:?} already exists with a different range",
                                request.id
                            ),
                        });
                    }
                    Ok(existing)
                }
                FingerprintMatch::Conflict => Err(Error::IdempotencyConflict {
                    key: request
                        .idempotency_key
                        .clone()
                        .unwrap_or_else(|| request.id.clone()),
                    detail: format!(
                        "seal {:?} was already written with a different request",
                        request.id
                    ),
                }),
                FingerprintMatch::Absent => {
                    if existing.table == request.table
                        && existing.start_ns == request.start_ns
                        && existing.end_ns == request.end_ns
                        && request.idempotency_key.is_none()
                    {
                        Ok(existing)
                    } else {
                        Err(Error::invalid(format!(
                            "seal {:?} already exists",
                            request.id
                        )))
                    }
                }
            };
        }
        let seal = Seal {
            id: request.id,
            table: request.table,
            start_ns: request.start_ns,
            end_ns: request.end_ns,
            note: request.note,
            idempotency_key: request.idempotency_key,
            idempotency_fingerprint: request.idempotency_fingerprint,
            checksum: String::new(),
        }
        .seal()?;
        let bytes = serde_json::to_vec_pretty(&seal)?;
        if !self.backend().put_if_absent(&path, bytes.into()).await? {
            // Lost the race. The winner's object is the record.
            let bytes = self.backend().get(&path).await?;
            return decode_seal(&bytes, path.as_ref());
        }
        self.backend().sync_objects(&[path]).await?;
        self.refresh_seal_cache().await?;
        Ok(seal)
    }

    /// Seals as currently cached. [`Self::reload_seals`] reads the objects.
    pub fn cached_seals(&self) -> Arc<Vec<Seal>> {
        self.seals.snapshot()
    }

    /// Re-read `seals/` and publish the list to every handle of this open.
    pub async fn reload_seals(&self) -> Result<Vec<Seal>> {
        let seals = load_seals(self.backend()).await?;
        self.seals.replace(seals.clone());
        Ok(seals)
    }

    async fn refresh_seal_cache(&self) -> Result<()> {
        self.reload_seals().await?;
        Ok(())
    }

    /// Open the single granted read of a seal.
    ///
    /// The `opened` record is durable before this returns a session. The same
    /// key and fingerprint may finish after a crash. Any other fingerprint
    /// is [`Error::HoldoutUsed`] and reads nothing. A completed record returns
    /// the stored summary and does not grant another read.
    pub async fn begin_holdout(&self, request: HoldoutRequest) -> Result<HoldoutOpen> {
        self.ensure_writable("begin_holdout")?;
        self.ensure_base("begin_holdout")?;
        if request.idempotency_key.is_empty() || request.fingerprint.is_empty() {
            return Err(Error::invalid(
                "a holdout evaluation needs an idempotency key and a strategy fingerprint",
            ));
        }
        let seal = self.load_seal(&request.seal_id).await?;
        let path = layout::holdout_path(&request.seal_id);
        if let Some(existing) = load_holdout_opt(self.backend(), &request.seal_id).await? {
            return self.decide_existing(existing, &request, &seal);
        }
        let record = HoldoutRecord {
            seal_id: request.seal_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            fingerprint: request.fingerprint.clone(),
            code_identity: request.code_identity.clone(),
            parameters: request.parameters.clone(),
            data_version: request.data_version.clone(),
            state: STATE_OPENED.to_string(),
            summary: None,
            checksum: String::new(),
        }
        .seal()?;
        let bytes = serde_json::to_vec_pretty(&record)?;
        if !self.backend().put_if_absent(&path, bytes.into()).await? {
            let existing = load_holdout(self.backend(), &request.seal_id).await?;
            return self.decide_existing(existing, &request, &seal);
        }
        self.backend().sync_objects(&[path]).await?;
        Ok(HoldoutOpen::Session(self.session_for(&seal, &request)))
    }

    fn decide_existing(
        &self,
        existing: HoldoutRecord,
        request: &HoldoutRequest,
        seal: &Seal,
    ) -> Result<HoldoutOpen> {
        if existing.idempotency_key != request.idempotency_key
            || existing.fingerprint != request.fingerprint
        {
            return Err(Error::HoldoutUsed {
                seal_id: request.seal_id.clone(),
                detail: "this seal already has an evaluation under a different key or fingerprint"
                    .into(),
            });
        }
        if existing.state == STATE_COMPLETED {
            return Ok(HoldoutOpen::Completed(existing));
        }
        if existing.state != STATE_OPENED {
            return Err(Error::corruption(
                layout::holdout_path(&request.seal_id).as_ref(),
                format!("unknown holdout state {:?}", existing.state),
            ));
        }
        Ok(HoldoutOpen::Session(self.session_for(seal, request)))
    }

    fn session_for(&self, seal: &Seal, request: &HoldoutRequest) -> HoldoutSession {
        let grant = HoldoutGrant {
            seal_id: seal.id.clone(),
            table: seal.table.clone(),
            start_ns: seal.start_ns,
            end_ns: seal.end_ns,
        };
        HoldoutSession {
            granted: self.read_only_view().with_holdout_grant(grant),
            writer: self.clone(),
            seal_id: seal.id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            fingerprint: request.fingerprint.clone(),
        }
    }

    async fn load_seal(&self, id: &str) -> Result<Seal> {
        let path = layout::seal_path(id);
        let bytes = self
            .backend()
            .get_opt(&path)
            .await?
            .ok_or_else(|| Error::invalid(format!("seal {id:?} not found")))?;
        decode_seal(&bytes, path.as_ref())
    }

    /// Every holdout record, ordered by seal id.
    pub async fn list_holdouts(&self) -> Result<Vec<HoldoutRecord>> {
        let mut out = Vec::new();
        for meta in list_prefix(self.backend(), &layout::holdout_prefix()).await? {
            if !meta.location.as_ref().ends_with(".json") {
                continue;
            }
            let bytes = self.backend().get(&meta.location).await?;
            out.push(decode_holdout(&bytes, meta.location.as_ref())?);
        }
        out.sort_by(|a, b| a.seal_id.cmp(&b.seal_id));
        Ok(out)
    }
}

async fn complete_holdout(
    db: &Database,
    seal_id: &str,
    key: &str,
    fingerprint: &str,
    summary: serde_json::Value,
) -> Result<HoldoutRecord> {
    db.ensure_writable("finish_holdout")?;
    let mut record = load_holdout(db.backend(), seal_id).await?;
    if record.idempotency_key != key || record.fingerprint != fingerprint {
        return Err(Error::HoldoutUsed {
            seal_id: seal_id.to_string(),
            detail: "finish does not match the evaluation that opened this seal".into(),
        });
    }
    if record.state == STATE_COMPLETED {
        return Ok(record);
    }
    record.state = STATE_COMPLETED.to_string();
    record.summary = Some(summary);
    record.checksum = String::new();
    let record = record.seal()?;
    let path = layout::holdout_path(seal_id);
    db.backend()
        .put(&path, serde_json::to_vec_pretty(&record)?.into())
        .await?;
    db.backend().sync_objects(&[path]).await?;
    Ok(record)
}

pub async fn load_cache(backend: &crate::Backend) -> Result<Arc<SealCache>> {
    Ok(SealCache::from_seals(load_seals(backend).await?))
}

async fn load_seals(backend: &crate::Backend) -> Result<Vec<Seal>> {
    let mut seals = Vec::new();
    for meta in list_prefix(backend, &layout::seals_prefix()).await? {
        if !meta.location.as_ref().ends_with(".json") {
            continue;
        }
        let bytes = backend.get(&meta.location).await?;
        seals.push(decode_seal(&bytes, meta.location.as_ref())?);
    }
    seals.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(seals)
}

async fn list_prefix(
    backend: &crate::Backend,
    prefix: &object_store::path::Path,
) -> Result<Vec<object_store::ObjectMeta>> {
    match backend.list(prefix).await {
        Ok(metas) => Ok(metas),
        Err(Error::ObjectStore(object_store::Error::NotFound { .. })) => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

fn decode_seal(bytes: &[u8], object: &str) -> Result<Seal> {
    let seal: Seal = serde_json::from_slice(bytes)
        .map_err(|e| Error::corruption(object, format!("seal parse: {e}")))?;
    seal.verify(object)?;
    Ok(seal)
}

fn decode_holdout(bytes: &[u8], object: &str) -> Result<HoldoutRecord> {
    let record: HoldoutRecord = serde_json::from_slice(bytes)
        .map_err(|e| Error::corruption(object, format!("holdout parse: {e}")))?;
    record.verify(object)?;
    Ok(record)
}

async fn load_holdout(backend: &crate::Backend, seal_id: &str) -> Result<HoldoutRecord> {
    load_holdout_opt(backend, seal_id)
        .await?
        .ok_or_else(|| Error::invalid(format!("holdout record for seal {seal_id:?} not found")))
}

async fn load_holdout_opt(
    backend: &crate::Backend,
    seal_id: &str,
) -> Result<Option<HoldoutRecord>> {
    let path = layout::holdout_path(seal_id);
    let Some(bytes) = backend.get_opt(&path).await? else {
        return Ok(None);
    };
    Ok(Some(decode_holdout(&bytes, path.as_ref())?))
}

enum FingerprintMatch {
    Same,
    Conflict,
    Absent,
}

/// Both sides absent is the legacy "same object" case. Both present and equal
/// matches. Anything else, including one side missing a fingerprint, conflicts
/// when a key is involved. No key on either side is `Absent` so the caller
/// can fall back to comparing the payload.
fn fingerprint_match(
    stored_key: Option<&str>,
    stored_fp: Option<&str>,
    request_key: Option<&str>,
    request_fp: Option<&str>,
) -> FingerprintMatch {
    match (stored_key, request_key) {
        (None, None) => match (stored_fp, request_fp) {
            (None, None) => FingerprintMatch::Absent,
            (Some(s), Some(r)) if s == r => FingerprintMatch::Same,
            _ => FingerprintMatch::Conflict,
        },
        (Some(stored), Some(request)) if stored == request => match (stored_fp, request_fp) {
            (Some(s), Some(r)) if s == r => FingerprintMatch::Same,
            (None, None) => FingerprintMatch::Same,
            _ => FingerprintMatch::Conflict,
        },
        (Some(_), Some(_)) => FingerprintMatch::Conflict,
        _ => FingerprintMatch::Conflict,
    }
}
