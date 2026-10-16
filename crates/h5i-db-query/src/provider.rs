//! Snapshot-bound `TableProvider`: one instance per resolved table version.
//!
//! Scan pipeline: pushed-down filters → `PruningPredicate` over manifest
//! statistics (segment pruning, zero I/O) → `ParquetSource` over the
//! surviving segment objects (row-group/page pruning + row-level predicate
//! pushdown handled by DataFusion's Parquet machinery).

use std::ops::Not;
use std::sync::Arc;

pub use crate::metrics::{ScanMetrics, ScanMetricsCollector};
use crate::predicate_cache::{
    PredicateCache, PredicateCacheMode, PredicateCacheStats, eligible_predicate,
};
use crate::pruning::{ManifestPruningStats, json_stat_to_scalar};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, DFSchema, ScalarValue, Statistics};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, col, lit};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr, expressions};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_pruning::PruningPredicate;
use h5i_db_core::{
    Enforcement, ResolvedTable, SegmentMeta, Visibility, segment_fully_visible, segment_hidden,
};

/// Fold manifest segment stats into planner `Statistics` (DESIGN §7 Tier 1):
/// exact row counts always; per-column min/max/null-count exact when every
/// segment recorded the stat, absent otherwise (never guessed).
fn manifest_statistics(schema: &SchemaRef, segments: &[&SegmentMeta]) -> Statistics {
    let num_rows = segments.iter().map(|s| s.rows as usize).sum::<usize>();
    // Encoded Parquet bytes understate in-memory Arrow size — keep Inexact.
    let total_bytes = segments.iter().map(|s| s.bytes as usize).sum::<usize>();

    let column_statistics = schema
        .fields()
        .iter()
        .map(|field| {
            let stat_type = match field.data_type() {
                DataType::Dictionary(_, value) if **value == DataType::Utf8 => DataType::Utf8,
                other => other.clone(),
            };
            let per_seg: Vec<Option<&h5i_db_core::ColumnStats>> = segments
                .iter()
                .map(|seg| seg.columns.get(field.name()))
                .collect();
            let mut stats = ColumnStatistics::new_unknown();
            if per_seg.iter().all(|s| s.is_some()) && !per_seg.is_empty() {
                let cols: Vec<_> = per_seg.into_iter().flatten().collect();
                stats.null_count =
                    Precision::Exact(cols.iter().map(|c| c.null_count as usize).sum());
                let mins: Option<Vec<ScalarValue>> = cols
                    .iter()
                    .map(|c| {
                        c.min
                            .as_ref()
                            .and_then(|v| json_stat_to_scalar(v, &stat_type))
                    })
                    .collect();
                if let Some(mins) = mins
                    && let Some(min) = mins.into_iter().reduce(|a, b| {
                        if b.partial_cmp(&a) == Some(std::cmp::Ordering::Less) {
                            b
                        } else {
                            a
                        }
                    })
                {
                    stats.min_value = Precision::Exact(min);
                }
                let maxs: Option<Vec<ScalarValue>> = cols
                    .iter()
                    .map(|c| {
                        c.max
                            .as_ref()
                            .and_then(|v| json_stat_to_scalar(v, &stat_type))
                    })
                    .collect();
                if let Some(maxs) = maxs
                    && let Some(max) = maxs.into_iter().reduce(|a, b| {
                        if b.partial_cmp(&a) == Some(std::cmp::Ordering::Greater) {
                            b
                        } else {
                            a
                        }
                    })
                {
                    stats.max_value = Precision::Exact(max);
                }
            }
            stats
        })
        .collect();

    Statistics {
        num_rows: Precision::Exact(num_rows),
        total_byte_size: Precision::Inexact(total_bytes),
        column_statistics,
    }
}

/// A `TableProvider` bound to one immutable table version.
pub struct H5iTableProvider {
    resolved: ResolvedTable,
    object_store_url: ObjectStoreUrl,
    metrics: ScanMetricsCollector,
    predicate_cache: Option<PredicateCache>,
    /// When set, [`Database::enforcement`] clips this scan. Absent means the
    /// provider was built without a handle and stays unrestricted, which is
    /// what direct constructor tests rely on.
    db: Option<Arc<h5i_db_core::Database>>,
}

impl H5iTableProvider {
    pub fn new(
        resolved: ResolvedTable,
        object_store_url: ObjectStoreUrl,
        metrics: ScanMetricsCollector,
    ) -> Self {
        Self {
            resolved,
            object_store_url,
            metrics,
            predicate_cache: None,
            db: None,
        }
    }

    /// Bind the handle whose ceiling, seals, and holdout grant this scan obeys.
    ///
    /// The seal cache is shared, so a seal published after the provider was
    /// built is still hidden on the next scan.
    pub fn with_database(mut self, db: Arc<h5i_db_core::Database>) -> Self {
        self.db = Some(db);
        self
    }

    pub fn with_predicate_cache(
        mut self,
        backend: h5i_db_core::Backend,
        mode: PredicateCacheMode,
    ) -> Self {
        if mode != PredicateCacheMode::Disabled {
            self.predicate_cache = Some(PredicateCache::new(backend, mode));
        }
        self
    }

    pub fn resolved(&self) -> &ResolvedTable {
        &self.resolved
    }

    /// `Ok(None)` is the unrestricted fast path, including a provider that was
    /// never given a database.
    fn enforcement(&self) -> DfResult<Option<Enforcement>> {
        let Some(db) = &self.db else {
            return Ok(None);
        };
        db.enforcement(
            &self.resolved.entry.name,
            self.resolved.schema.as_ref(),
            self.resolved.spec.time_column.as_deref(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    /// Prune segments with the pushed-down predicate against manifest stats.
    fn prune_segments(&self, state: &dyn Session, filters: &[Expr]) -> DfResult<Vec<&SegmentMeta>> {
        let segments = &self.resolved.manifest.segments;
        let Some(predicate) = conjunction(filters.to_vec()) else {
            return Ok(segments.iter().collect());
        };
        let df_schema = DFSchema::try_from(self.schema())?;
        let physical = state.create_physical_expr(predicate, &df_schema)?;
        let pruning = match PruningPredicate::try_new(physical, self.schema()) {
            Ok(p) => p,
            // A predicate the pruning machinery can't analyze must never
            // drop segments — scan everything.
            Err(e) => {
                tracing::debug!("pruning predicate unavailable: {e}");
                return Ok(segments.iter().collect());
            }
        };
        let stats = ManifestPruningStats::new(segments, self.schema());
        match pruning.prune(&stats) {
            Ok(keep) => Ok(segments
                .iter()
                .zip(keep)
                .filter_map(|(s, k)| k.then_some(s))
                .collect()),
            Err(e) => {
                tracing::debug!("pruning evaluation failed, scanning all: {e}");
                Ok(segments.iter().collect())
            }
        }
    }

    /// Declared output ordering, which the planner takes as licence to skip a
    /// sort. Two conditions must hold together, and neither implies the other:
    ///
    /// * every segment is internally `sorted` by the sort key, and
    /// * the segments do not interleave, i.e. their `time_range`s are
    ///   non-overlapping and ascending **in manifest order**, which is the
    ///   order the files are handed to the scan below.
    ///
    /// `sorted` alone was the old test, and it is not enough: a backfill
    /// appended after a later batch, a range replacement, or simply a manifest
    /// written before segment lists were kept ordered all produce sorted
    /// segments whose concatenation is not sorted. Claiming an ordering there
    /// makes the planner drop a sort it still needs, and the query silently
    /// returns rows out of order (worse, window functions and ASOF joins read
    /// the wrong neighbours). Old manifests exist, so this stays defensive
    /// even once writers guarantee the ordering.
    ///
    /// A segment with no recorded `time_range` declines the claim: absence of
    /// bounds is not proof of disjointness.
    fn output_ordering(&self) -> Option<LexOrdering> {
        let spec = &self.resolved.spec;
        let tc = spec.time_column.as_ref()?;
        let segments = &self.resolved.manifest.segments;
        if segments.is_empty() || !segments.iter().all(|s| s.sorted) {
            return None;
        }
        let mut previous_max: Option<i64> = None;
        for segment in segments {
            let (min, max) = segment.time_range?;
            // Touching bounds (`previous_max == min`) are fine: equal keys next
            // to each other are still non-decreasing.
            if min > max || previous_max.is_some_and(|previous| previous > min) {
                return None;
            }
            previous_max = Some(max);
        }
        let idx = self.schema().index_of(tc).ok()?;
        LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(
            expressions::Column::new(tc, idx),
        ))])
    }
}

impl std::fmt::Debug for H5iTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("H5iTableProvider")
            .field("table", &self.resolved.entry.name)
            .field("version", &self.resolved.manifest.sequence)
            .finish()
    }
}

#[async_trait]
impl TableProvider for H5iTableProvider {
    fn schema(&self) -> SchemaRef {
        self.resolved.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Exact table-level statistics straight from the manifest — lets the
    /// planner answer metadata-only aggregates and pick join sides with zero
    /// object I/O.
    fn statistics(&self) -> Option<Statistics> {
        // A failed enforcement means the scope cannot be applied. Unknown
        // statistics are safe; exact min/max would answer `max(ts)` from a
        // row the handle is not allowed to read.
        let enforcement = match self.enforcement() {
            Ok(value) => value,
            Err(_) => return None,
        };
        let segments: Vec<&SegmentMeta> = match &enforcement {
            None => self.resolved.manifest.segments.iter().collect(),
            Some(enf) => self
                .resolved
                .manifest
                .segments
                .iter()
                .filter(|segment| !segment_hidden(segment.time_range, &enf.visibility))
                .collect(),
        };
        let mut stats = manifest_statistics(&self.schema(), &segments);
        if let Some(enf) = &enforcement {
            let exact = segments
                .iter()
                .all(|segment| segment_fully_visible(segment.time_range, &enf.visibility));
            if !exact {
                stats = stats.to_inexact();
                for column in &mut stats.column_statistics {
                    column.min_value = Precision::Absent;
                    column.max_value = Precision::Absent;
                }
            }
        }
        Some(stats)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Inexact everywhere: pruning skips whole segments/row-groups and the
        // Parquet reader applies row filters, but DataFusion re-validates —
        // always correct, never trusted blindly.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let enforcement = self.enforcement()?;
        // The scope predicate joins the user filters for pruning and for the
        // predicate-cache key. A scoped scan must not reuse an unscoped sidecar.
        let scope_expr = match &enforcement {
            Some(enf) => {
                let dtype = self
                    .schema()
                    .field_with_name(&enf.time_column)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
                    .data_type()
                    .clone();
                Some(scope_predicate(&enf.time_column, &enf.visibility, &dtype)?)
            }
            None => None,
        };
        let extended;
        let effective: &[Expr] = if let Some(expr) = scope_expr.clone() {
            let mut buf = filters.to_vec();
            buf.push(expr);
            extended = buf;
            &extended
        } else {
            filters
        };
        // Keep the time column available to the scope filter, then drop it if
        // the caller did not ask for it. Evaluation order is filter, then
        // limit, then projection.
        let mut projection_owned = projection.cloned();
        let mut drop_appended_time = false;
        if let Some(enf) = &enforcement
            && let Ok(idx) = self.schema().index_of(&enf.time_column)
            && let Some(cols) = projection_owned.as_mut()
            && !cols.contains(&idx)
        {
            cols.push(idx);
            drop_appended_time = true;
        }
        // A file-scan limit counts rows before the scope filter and can return
        // fewer visible rows than requested. The outer limit still trims.
        let file_limit = if enforcement.is_some() { None } else { limit };

        let mut survivors = self.prune_segments(state, effective)?;
        if let Some(enf) = &enforcement {
            survivors.retain(|segment| !segment_hidden(segment.time_range, &enf.visibility));
        }
        let predicate_cache = self
            .predicate_cache
            .as_ref()
            .and_then(|_| eligible_predicate(&self.schema(), effective));
        let mut cache_stats = PredicateCacheStats::default();
        if self.predicate_cache.is_some() && predicate_cache.is_none() && !effective.is_empty() {
            cache_stats.rejected = 1;
        }

        let mut source = ParquetSource::new(self.schema());
        // Footer-metadata cache: segments are immutable and content-addressed,
        // so caching parsed Parquet footers across queries is unconditionally
        // sound and saves one footer read+parse per segment per query
        // (measured: ~40% of warm full-scan latency at 50 segments).
        let metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();
        if let Ok(store) = state.runtime_env().object_store(&self.object_store_url) {
            source = source.with_parquet_file_reader_factory(Arc::new(
                datafusion_datasource_parquet::CachedParquetFileReaderFactory::new(
                    store,
                    metadata_cache,
                ),
            ));
        }
        // The predicate enables Parquet row-group + page pruning. Row-level
        // filter pushdown (`with_pushdown_filters`) is deliberately NOT
        // enabled: measured on tick data it costs ~2x on selective time-range
        // scans versus decode-then-filter (segments are already pruned by the
        // manifest, so decoded batches are mostly relevant anyway).
        if let Some(predicate) = conjunction(effective.to_vec()) {
            let df_schema = DFSchema::try_from(self.schema())?;
            if let Ok(physical) = state.create_physical_expr(predicate, &df_schema) {
                source = source.with_predicate(physical);
            }
        }

        // Post-pruning statistics for the physical plan: exact over the
        // surviving segments when the scan emits them verbatim; degraded to
        // inexact when a predicate (row-group/page pruning) or limit can
        // shrink the output. A scope that still straddles a segment cannot
        // publish that segment's min/max: `max(ts)` would answer from a row
        // the handle cannot read.
        let mut scan_stats = manifest_statistics(&self.schema(), &survivors);
        if !effective.is_empty() || file_limit.is_some() {
            scan_stats = scan_stats.to_inexact();
        }
        if let Some(enf) = &enforcement {
            let exact = survivors
                .iter()
                .all(|segment| segment_fully_visible(segment.time_range, &enf.visibility));
            if !exact {
                scan_stats = scan_stats.to_inexact();
                for column in &mut scan_stats.column_statistics {
                    column.min_value = Precision::Absent;
                    column.max_value = Precision::Absent;
                }
            }
        }

        let mut builder =
            FileScanConfigBuilder::new(self.object_store_url.clone(), Arc::new(source))
                .with_projection_indices(projection_owned.clone())
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .with_statistics(scan_stats)
                .with_limit(file_limit);
        if let Some(ordering) = self.output_ordering() {
            builder = builder.with_output_ordering(vec![ordering]);
        }
        for seg in &survivors {
            let mut file = PartitionedFile::new(seg.path.clone(), seg.bytes);
            if let (Some(cache), Some(predicate)) = (&self.predicate_cache, &predicate_cache) {
                let application = cache.apply(state, seg, predicate).await;
                cache_stats.lookups += application.stats.lookups;
                cache_stats.hits += application.stats.hits;
                cache_stats.misses += application.stats.misses;
                cache_stats.builds += application.stats.builds;
                cache_stats.rejected += application.stats.rejected;
                cache_stats.row_groups_reused += application.stats.row_groups_reused;
                cache_stats.evictions += application.stats.evictions;
                if let Some(access_plan) = application.access_plan {
                    file = file.with_extension(access_plan);
                }
            }
            builder = builder.with_file(file);
        }
        self.metrics.record(ScanMetrics {
            query_id: None,
            table: self.resolved.entry.name.clone(),
            version: self.resolved.manifest.sequence,
            segments_total: self.resolved.manifest.segments.len(),
            segments_pruned: self.resolved.manifest.segments.len() - survivors.len(),
            segments_scanned: survivors.len(),
            bytes_scheduled: survivors.iter().map(|s| s.bytes).sum(),
            predicate_cache_lookups: cache_stats.lookups,
            predicate_cache_hits: cache_stats.hits,
            predicate_cache_misses: cache_stats.misses,
            predicate_cache_builds: cache_stats.builds,
            predicate_cache_rejected: cache_stats.rejected,
            predicate_cache_row_groups_reused: cache_stats.row_groups_reused,
            predicate_cache_evictions: cache_stats.evictions,
        });
        let mut plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(builder.build());
        if let Some(expr) = scope_expr {
            let df_schema = DFSchema::try_from(plan.schema())?;
            let physical = state.create_physical_expr(expr, &df_schema)?;
            plan = Arc::new(FilterExec::try_new(physical, plan)?);
            if drop_appended_time {
                let input_schema = plan.schema();
                let keep = input_schema.fields().len().saturating_sub(1);
                let exprs = (0..keep).map(|i| {
                    let name = input_schema.field(i).name().to_string();
                    (
                        Arc::new(expressions::Column::new(&name, i)) as Arc<dyn PhysicalExpr>,
                        name,
                    )
                });
                plan = Arc::new(ProjectionExec::try_new(exprs, plan)?);
            }
        }
        Ok(plan)
    }
}

/// Inclusive ceiling and half-open holes, in the time column's own unit.
///
/// Values in `vis` are already converted; this does not scale them again.
fn scope_predicate(time_col: &str, vis: &Visibility, dtype: &DataType) -> DfResult<Expr> {
    let mut parts = Vec::new();
    if let Some(ceiling) = vis.ceiling_raw {
        parts.push(col(time_col).lt_eq(lit(scope_literal(ceiling, dtype)?)));
    }
    for &(start, end) in &vis.holes {
        let inside = col(time_col)
            .gt_eq(lit(scope_literal(start, dtype)?))
            .and(col(time_col).lt(lit(scope_literal(end, dtype)?)));
        parts.push(inside.not());
    }
    conjunction(parts).ok_or_else(|| {
        DataFusionError::Internal("scope enforcement produced an empty predicate".into())
    })
}

fn scope_literal(raw: i64, dtype: &DataType) -> DfResult<ScalarValue> {
    let DataType::Timestamp(unit, tz) = dtype else {
        return Err(DataFusionError::Plan(format!(
            "a decision-time ceiling or sealed range needs a timestamp column, found {dtype}"
        )));
    };
    Ok(match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(raw), tz.clone()),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(raw), tz.clone()),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(raw), tz.clone()),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(raw), tz.clone()),
    })
}
