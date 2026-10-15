//! Worker API for h5i-db.
//!
//! The engine stays an embedded library: no daemon, no role, no agent noun.
//! This crate is what a lab calls. A [`Handle`] is a capability — one fork,
//! one decision-time ceiling, sealed ranges excluded — and every read through
//! it uses the engine's single scope check. Who may build a handle is the
//! caller's decision.
//!
//! # Idempotency
//!
//! Every mutating call takes an idempotency key. The fingerprint is blake3 of
//! the canonical JSON of the request with the key removed. The same key and
//! fingerprint return the first result. The same key and a different
//! fingerprint is [`ServiceError::IdempotencyConflict`]. The effect log is
//! `service/effects/<blake3(key)>.json`, consulted first and written after the
//! engine commit. A crash between the two is recovered by the engine's own
//! replay, then the log is filled in.
//!
//! # Conditional commits
//!
//! Ingest, plan, and apply require `expected_version`: commit on top of that
//! head, or [`ServiceError::VersionConflict`]. Promote requires the base
//! sequence the fork pinned, or [`ServiceError::PromoteConflict`].
//!
//! One table commit is atomic at `HEAD`. A multi-table promote writes a
//! journal first (`service/promote-journal/`); [`Service::open`] finishes it.
//! Before the journal is durable, nothing new is promised. After it is, the
//! durable outcome is the whole list. A reader can observe a prefix during
//! the swap loop, the same window a multi-table transaction already has.
//!
//! # Query
//!
//! [`Handle::query`] sets both the arrival pin and the event-time ceiling to
//! the requested decision time, unless the handle's own ceiling is earlier, in
//! which case the call is `scope_denied` rather than silently widened.
//!
//! See DESIGN.md §14.

mod canonical;
mod effect;
mod error;
mod handle;
mod service;
mod state;
mod types;

pub use error::{Result, ServiceError};
pub use handle::Handle;
pub use service::{HoldoutGrant, HoldoutOpened, Service};
pub use state::STATE_FORMAT;
pub use types::*;
