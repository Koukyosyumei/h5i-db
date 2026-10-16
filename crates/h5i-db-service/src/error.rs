//! Typed errors for the worker API.
//!
//! Engine errors keep their stable `code()`. This crate adds `scope_denied`
//! for a handle that was asked to see past its ceiling. Idempotency mismatches
//! stay `idempotency_conflict` and are not retryable.

use serde::{Deserialize, Serialize};

/// Failure of a service call. Serializable so a worker can return it as a command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServiceError {
    #[error("scope denied: {detail}")]
    ScopeDenied { detail: String },

    #[error("idempotency conflict for key {key}: {detail}")]
    IdempotencyConflict { key: String, detail: String },

    #[error("version conflict on table {table:?}: expected head {expected}, found {actual}")]
    VersionConflict {
        table: String,
        expected: u64,
        actual: u64,
    },

    #[error(
        "cannot promote table {table:?} from fork {fork:?}: base moved from version {base} to {actual}"
    )]
    PromoteConflict {
        fork: String,
        table: String,
        base: u64,
        actual: u64,
    },

    #[error("holdout {seal_id} already used: {detail}")]
    HoldoutUsed { seal_id: String, detail: String },

    #[error("{code}: {detail}")]
    Engine { code: String, detail: String },
}

impl ServiceError {
    /// Stable machine-readable code.
    pub fn code(&self) -> &str {
        match self {
            Self::ScopeDenied { .. } => "scope_denied",
            Self::IdempotencyConflict { .. } => "idempotency_conflict",
            Self::VersionConflict { .. } => "version_conflict",
            Self::PromoteConflict { .. } => "promote_conflict",
            Self::HoldoutUsed { .. } => "holdout_used",
            Self::Engine { code, .. } => code,
        }
    }

    pub fn engine(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::Engine {
            code: code.into(),
            detail: detail.into(),
        }
    }
}

pub type Result<T> = std::result::Result<T, ServiceError>;

pub(crate) fn engine(err: h5i_db_core::Error) -> ServiceError {
    use h5i_db_core::Error;
    match err {
        Error::IdempotencyConflict { key, detail } => {
            ServiceError::IdempotencyConflict { key, detail }
        }
        Error::VersionConflict {
            table,
            expected,
            actual,
        } => ServiceError::VersionConflict {
            table,
            expected,
            actual,
        },
        Error::PromoteConflict {
            fork,
            table,
            base,
            actual,
            ..
        } => ServiceError::PromoteConflict {
            fork,
            table,
            base,
            actual,
        },
        Error::HoldoutUsed { seal_id, detail } => ServiceError::HoldoutUsed { seal_id, detail },
        other => ServiceError::engine(other.code(), other.to_string()),
    }
}

pub(crate) fn invalid(detail: impl Into<String>) -> ServiceError {
    ServiceError::engine("invalid_input", detail)
}
