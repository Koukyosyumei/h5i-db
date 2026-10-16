//! Deterministic listing of forks, versions, trials, evaluations, and promotions,
//! and a field-level diff against a ledger snapshot of the same shape.

use h5i_db_core::fork::FORKED_FROM_META_KEY;
use h5i_db_core::{Database, ReadAt};

use crate::error::{Result, engine};
use crate::handle::read_trials;

/// The backtest's trial table (`h5i_db_backtest::schema::TRIAL`).
const TRIAL_TABLE: &str = h5i_db_backtest::schema::TRIAL;
use crate::types::{
    FieldMismatch, ForkState, HoldoutReceipt, OwnedHead, PinReceipt, PromotionState, ReconcileDiff,
    StateDocument, TrialState, VersionState,
};

pub const STATE_FORMAT: u32 = 1;

pub async fn export_state(db: &Database) -> Result<StateDocument> {
    let mut forks = Vec::new();
    let mut versions = Vec::new();
    let mut trials = Vec::new();
    let mut promotions = Vec::new();

    let listed = h5i_db_core::fork::list(db.backend())
        .await
        .map_err(engine)?;
    for fork in listed {
        let fork_db = db.open_fork(&fork.name).await.map_err(engine)?;
        let entries = h5i_db_core::fork::list_entries(db.backend(), &fork.name)
            .await
            .map_err(engine)?;
        let mut owned = Vec::new();
        for entry in &entries {
            let resolved = fork_db
                .resolve(&entry.name, ReadAt::Latest)
                .await
                .map_err(engine)?;
            owned.push(OwnedHead {
                table: entry.name.clone(),
                sequence: resolved.head_sequence,
                rows: resolved.manifest.rows,
            });
            for version in fork_db.list_versions(&entry.name).await.map_err(engine)? {
                versions.push(VersionState {
                    fork: fork.name.clone(),
                    table: entry.name.clone(),
                    sequence: version.sequence,
                    op: version.op,
                    committed_at_ns: version.committed_at_ns,
                    rows: version.rows,
                });
            }
        }
        owned.sort_by(|left, right| left.table.cmp(&right.table));
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
        if let Some(created) = fork
            .user_meta
            .get("h5i.promoted_created")
            .and_then(|value| value.as_object())
        {
            for (table, value) in created {
                // The fork's trial table moved to the base. Its rows are still
                // this fork's trials.
                if table == TRIAL_TABLE {
                    trials.extend(read_trials(db, &fork.name).await?);
                }
                promotions.push(PromotionState {
                    fork: fork.name.clone(),
                    table: table.clone(),
                    kind: "created".to_string(),
                    base_sequence: None,
                    fork_sequence: value.get("sequence").and_then(|item| item.as_u64()),
                    rows: value.get("rows").and_then(|item| item.as_u64()),
                });
            }
        }
        forks.push(ForkState {
            name: fork.name.clone(),
            created_at_ns: fork.created_at_ns,
            note: fork.note.clone(),
            as_of_ns: fork.as_of_ns,
            parent: fork.parent.clone(),
            depth: fork.depth(),
            pins,
            owned,
        });
        trials.extend(read_trials(&fork_db, &fork.name).await?);
    }

    let base_tables = db.list_tables().await.map_err(engine)?;
    for entry in base_tables {
        let resolved = db
            .resolve(&entry.name, ReadAt::Latest)
            .await
            .map_err(engine)?;
        let Some(meta) = resolved.manifest.user_meta.get(FORKED_FROM_META_KEY) else {
            continue;
        };
        let Some(fork_name) = meta.get("fork").and_then(|value| value.as_str()) else {
            continue;
        };
        // Shadow manifests also carry `h5i.forked_from`, but with the base
        // table id rather than `fork_table_id`. A promotion records the fork
        // table. Skip the shadow's own copy.
        if meta
            .get("fork_table_id")
            .and_then(|value| value.as_str())
            .is_none()
        {
            continue;
        }
        // A promoted trial table now lives on the base, and the fork no longer
        // has it. Its rows are still that fork's trials.
        if entry.name == TRIAL_TABLE {
            trials.extend(read_trials(db, fork_name).await?);
        }
        promotions.push(PromotionState {
            fork: fork_name.to_string(),
            table: entry.name,
            kind: "shadowed".to_string(),
            base_sequence: Some(resolved.manifest.sequence),
            fork_sequence: meta.get("fork_sequence").and_then(|value| value.as_u64()),
            rows: Some(resolved.manifest.rows),
        });
    }

    let evaluations = db
        .list_holdouts()
        .await
        .map_err(engine)?
        .into_iter()
        .map(HoldoutReceipt::from)
        .collect();

    forks.sort_by(|left, right| left.name.cmp(&right.name));
    versions.sort_by(|left, right| {
        left.fork
            .cmp(&right.fork)
            .then(left.table.cmp(&right.table))
            .then(left.sequence.cmp(&right.sequence))
    });
    trials.sort_by(|left, right| {
        left.fork
            .cmp(&right.fork)
            .then(left.run_id.cmp(&right.run_id))
            .then(left.ts_ns.cmp(&right.ts_ns))
    });
    // A promoted shadow can leave the same rows on the fork and on the base.
    trials.dedup_by(|right, left| {
        left.fork == right.fork && left.run_id == right.run_id && left.ts_ns == right.ts_ns
    });
    promotions.sort_by(|left, right| {
        left.fork
            .cmp(&right.fork)
            .then(left.table.cmp(&right.table))
            .then(left.kind.cmp(&right.kind))
    });

    Ok(StateDocument {
        format: STATE_FORMAT,
        forks,
        versions,
        trials,
        evaluations,
        promotions,
    })
}

/// Diff `actual` (the database) against `ledger`. Keys are stable identities,
/// not array positions, so one missing fork does not shift every later row.
pub fn reconcile(actual: &StateDocument, ledger: &StateDocument) -> ReconcileDiff {
    let mut diff = ReconcileDiff::default();
    if actual.format != ledger.format {
        diff.mismatches.push(FieldMismatch {
            path: "format".to_string(),
            ledger: serde_json::json!(ledger.format),
            actual: serde_json::json!(actual.format),
        });
    }
    diff_keyed(&mut diff, "forks", &actual.forks, &ledger.forks, |fork| {
        fork.name.clone()
    });
    diff_keyed(
        &mut diff,
        "versions",
        &actual.versions,
        &ledger.versions,
        |version| format!("{}/{}/{}", version.fork, version.table, version.sequence),
    );
    diff_keyed(
        &mut diff,
        "trials",
        &actual.trials,
        &ledger.trials,
        |trial: &TrialState| format!("{}/{}/{}", trial.fork, trial.run_id, trial.ts_ns),
    );
    diff_keyed(
        &mut diff,
        "evaluations",
        &actual.evaluations,
        &ledger.evaluations,
        |record| record.seal_id.clone(),
    );
    diff_keyed(
        &mut diff,
        "promotions",
        &actual.promotions,
        &ledger.promotions,
        |promotion| format!("{}/{}/{}", promotion.fork, promotion.table, promotion.kind),
    );
    diff.missing.sort();
    diff.extra.sort();
    diff.mismatches
        .sort_by(|left, right| left.path.cmp(&right.path));
    diff
}

fn diff_keyed<T, F>(
    diff: &mut ReconcileDiff,
    collection: &str,
    actual: &[T],
    ledger: &[T],
    key_of: F,
) where
    T: serde::Serialize,
    F: Fn(&T) -> String,
{
    let mut actual_map = std::collections::BTreeMap::new();
    for item in actual {
        actual_map.insert(key_of(item), item);
    }
    let mut ledger_map = std::collections::BTreeMap::new();
    for item in ledger {
        ledger_map.insert(key_of(item), item);
    }
    for key in ledger_map.keys() {
        if !actual_map.contains_key(key) {
            diff.missing.push(format!("{collection}/{key}"));
        }
    }
    for key in actual_map.keys() {
        if !ledger_map.contains_key(key) {
            diff.extra.push(format!("{collection}/{key}"));
        }
    }
    for key in ledger_map.keys() {
        let (Some(ledger_item), Some(actual_item)) = (ledger_map.get(key), actual_map.get(key))
        else {
            continue;
        };
        let ledger_value = serde_json::to_value(ledger_item).unwrap_or(serde_json::Value::Null);
        let actual_value = serde_json::to_value(actual_item).unwrap_or(serde_json::Value::Null);
        diff_values(
            diff,
            &format!("{collection}/{key}"),
            &ledger_value,
            &actual_value,
        );
    }
}

fn diff_values(
    diff: &mut ReconcileDiff,
    path: &str,
    ledger: &serde_json::Value,
    actual: &serde_json::Value,
) {
    match (ledger, actual) {
        (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
            let mut keys: Vec<&String> = left.keys().chain(right.keys()).collect();
            keys.sort();
            keys.dedup();
            for key in keys {
                let child = format!("{path}/{key}");
                match (left.get(key), right.get(key)) {
                    (None, Some(_)) => diff.extra.push(child),
                    (Some(_), None) => diff.missing.push(child),
                    (Some(left_value), Some(right_value)) => {
                        diff_values(diff, &child, left_value, right_value);
                    }
                    (None, None) => {}
                }
            }
        }
        (left, right) if left != right => diff.mismatches.push(FieldMismatch {
            path: path.to_string(),
            ledger: left.clone(),
            actual: right.clone(),
        }),
        _ => {}
    }
}
