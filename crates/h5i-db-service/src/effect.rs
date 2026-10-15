//! Durable effect log: one object per idempotency key.
//!
//! Consulted before the engine. Written after the engine commit. A crash in
//! between is recovered by the engine's own replay (manifest key, fork
//! metadata, seal object, holdout record, or promote replay) and then the
//! log is filled in. The log has no lookback bound; the manifest scan still
//! does (`IDEMPOTENCY_LOOKBACK`).

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use h5i_db_core::Database;
use h5i_db_core::layout;

use crate::error::{Result, ServiceError, engine};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct EffectRecord {
    key: String,
    fingerprint: String,
    operation: String,
    result: Value,
}

/// The stored result for this key, if the log already has a matching record.
pub async fn stored<T: DeserializeOwned>(
    db: &Database,
    key: &str,
    fingerprint: &str,
    operation: &str,
) -> Result<Option<T>> {
    match load(db, key).await? {
        None => Ok(None),
        Some(existing) => Ok(Some(decode_match(key, fingerprint, operation, existing)?)),
    }
}

pub async fn with_effect<T, F, Fut>(
    db: &Database,
    key: &str,
    fingerprint: &str,
    operation: &str,
    run: F,
) -> Result<T>
where
    T: Serialize + DeserializeOwned,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    crate::canonical::require_key(key)?;
    if let Some(existing) = load(db, key).await? {
        return decode_match(key, fingerprint, operation, existing);
    }
    let value = run().await?;
    store(db, key, fingerprint, operation, &value).await?;
    Ok(value)
}

fn decode_match<T: DeserializeOwned>(
    key: &str,
    fingerprint: &str,
    operation: &str,
    existing: EffectRecord,
) -> Result<T> {
    if existing.fingerprint != fingerprint || existing.operation != operation {
        return Err(ServiceError::IdempotencyConflict {
            key: key.to_string(),
            detail: format!(
                "key {key} already recorded operation {} with a different request",
                existing.operation
            ),
        });
    }
    serde_json::from_value(existing.result).map_err(|error| {
        ServiceError::engine(
            "metadata",
            format!("effect {key} does not decode as {operation}: {error}"),
        )
    })
}

async fn load(db: &Database, key: &str) -> Result<Option<EffectRecord>> {
    let path = layout::service_effect_path(key);
    let Some(bytes) = db.backend().get_opt(&path).await.map_err(engine)? else {
        return Ok(None);
    };
    let record: EffectRecord = serde_json::from_slice(&bytes).map_err(|error| {
        ServiceError::engine(
            "corruption",
            format!("effect {} parse: {error}", path.as_ref()),
        )
    })?;
    if record.key != key {
        return Err(ServiceError::engine(
            "corruption",
            format!(
                "effect {} is for key {:?}, not {key:?}",
                path.as_ref(),
                record.key
            ),
        ));
    }
    Ok(Some(record))
}

async fn store<T: Serialize>(
    db: &Database,
    key: &str,
    fingerprint: &str,
    operation: &str,
    result: &T,
) -> Result<()> {
    let path = layout::service_effect_path(key);
    let record = EffectRecord {
        key: key.to_string(),
        fingerprint: fingerprint.to_string(),
        operation: operation.to_string(),
        result: serde_json::to_value(result)
            .map_err(|error| ServiceError::engine("metadata", error.to_string()))?,
    };
    let bytes = serde_json::to_vec_pretty(&record)
        .map_err(|error| ServiceError::engine("metadata", error.to_string()))?;
    if !db
        .backend()
        .put_if_absent(&path, bytes::Bytes::from(bytes))
        .await
        .map_err(engine)?
    {
        let existing = load(db, key).await?.ok_or_else(|| {
            ServiceError::engine(
                "storage",
                format!("effect {} vanished after a lost create", path.as_ref()),
            )
        })?;
        return decode_match::<Value>(key, fingerprint, operation, existing).map(|_| ());
    }
    db.backend()
        .sync_objects(std::slice::from_ref(&path))
        .await
        .map_err(engine)?;
    Ok(())
}
