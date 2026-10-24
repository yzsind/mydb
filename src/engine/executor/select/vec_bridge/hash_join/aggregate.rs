//! Direct join-to-aggregate consumption shared by every streaming join.
use super::*;
use crate::engine::executor::select::vec_bridge::JOIN_AGG_MULTIPLICITY_ROWS_SAVED;
use crate::engine::executor::vec::{schema::BatchField, stream::BatchPartitioning};
use crate::types::ColumnType;

impl HashJoinBatchOperator<'_> {
    pub(super) fn output_column_source(
        &self,
        output_column: usize,
    ) -> Option<vec_hash_agg::DirectJoinColumn> {
        let build_width = self.build.schema().len();
        let probe_width = self.probe.schema().len();
        if self.build_is_left {
            if output_column < build_width {
                Some(vec_hash_agg::DirectJoinColumn::Build(output_column))
            } else {
                (output_column - build_width < probe_width).then_some(
                    vec_hash_agg::DirectJoinColumn::Probe(output_column - build_width),
                )
            }
        } else if output_column < probe_width {
            Some(vec_hash_agg::DirectJoinColumn::Probe(output_column))
        } else {
            (output_column - probe_width < build_width).then_some(
                vec_hash_agg::DirectJoinColumn::Build(output_column - probe_width),
            )
        }
    }

    /// Drive an adjacent HashAggregate from `(probe row, build row index)`
    /// matches. This is an internal layout of the same HashJoin and aggregate
    /// operators: unsupported layouts return before `initialize`, while spill
    /// or another runtime layout continues by feeding ordinary batches without
    /// replaying either child.
    pub(super) fn consume_matches_into_hash_aggregate(
        &mut self,
        aggregate: &mut vec_hash_agg::HashAggregateCore,
        computed_expressions: Option<&[Expr]>,
    ) -> Result<bool, QueryError> {
        use crate::parser::JoinType;

        if self.state.as_ref().is_some_and(|state| {
            state.probe_started || state.probe_done || !state.pending.is_empty()
        }) {
            return Ok(false);
        }
        let preserve_build = (self.join_type == JoinType::Left && self.build_is_left)
            || (self.join_type == JoinType::Right && !self.build_is_left);
        if self.residual.is_some() || self.reverse_semi_anti {
            return Ok(false);
        }

        let (group_columns, aggregate_specs) = aggregate.direct_join_columns();
        let aggregate_specs = aggregate_specs.to_vec();
        let single_group = group_columns.is_empty() && self.join_type == JoinType::Inner;
        let count_star_only = single_group && aggregate.direct_join_single_group_count_star_only();
        let mut build_group_sources = Vec::with_capacity(group_columns.len());
        let mut probe_group_sources = Vec::with_capacity(group_columns.len());
        for &column in group_columns {
            match self.output_column_source(column) {
                Some(vec_hash_agg::DirectJoinColumn::Build(build_column)) => {
                    build_group_sources.push((column, build_column))
                }
                Some(vec_hash_agg::DirectJoinColumn::Probe(probe_column)) => {
                    probe_group_sources.push((column, probe_column))
                }
                None => return Ok(false),
            }
        }
        let build_group_mode = !single_group
            && (preserve_build || self.join_type == JoinType::Inner)
            && build_group_sources.len() == group_columns.len();
        let probe_group_mode =
            self.join_type == JoinType::Inner && probe_group_sources.len() == group_columns.len();
        let probe_count_multiplicity_mode = probe_group_mode
            && aggregate_specs.iter().all(|spec| {
                matches!(
                    spec.func,
                    vec_hash_agg::AggregateKind::Count
                        | vec_hash_agg::AggregateKind::CountStar
                        | vec_hash_agg::AggregateKind::Sum
                        | vec_hash_agg::AggregateKind::Avg
                )
            });
        if !single_group && !build_group_mode && !probe_group_mode {
            return Ok(false);
        }
        // ROLLUP can retain raw build-group states without materializing
        // repeated dimension payloads. Probe groups may reset/spill their
        // dense handles, so they keep the ordinary finest-group driver.
        if aggregate.direct_build_rollup && !build_group_mode {
            return Ok(false);
        }
        let mut aggregate_sources = Vec::with_capacity(aggregate_specs.len());
        let mut computed_probe_programs = computed_expressions
            .map(|expressions| vec![None; expressions.len()])
            .unwrap_or_default();
        for spec in &aggregate_specs {
            if spec.func == vec_hash_agg::AggregateKind::CountStar {
                aggregate_sources.push(None);
            } else {
                if let Some(source) = self.output_column_source(spec.input_column) {
                    aggregate_sources.push(Some(source));
                } else {
                    // A computed input is bound to the probe row. An unmatched
                    // preserved build row has no probe row: `SUM(0)` or
                    // `COUNT(COALESCE(p.x, 1))` must see the expression's
                    // value, not a NULL probe lane.
                    let Some(expressions) = computed_expressions.filter(|_| !preserve_build) else {
                        return Ok(false);
                    };
                    let Some(expression_index) = spec.input_column.checked_sub(self.schema.len())
                    else {
                        return Ok(false);
                    };
                    let Some(expression) = expressions.get(expression_index) else {
                        return Ok(false);
                    };
                    let Some(program) =
                        expr_batch::compile_for_batch(expression, self.probe.schema())?
                    else {
                        // A build-side or mixed-side expression cannot be
                        // evaluated once per probe row. Preserve the ordinary
                        // joined-batch expression path for that shape.
                        return Ok(false);
                    };
                    computed_probe_programs[expression_index] = Some(program);
                    aggregate_sources.push(Some(vec_hash_agg::DirectJoinColumn::Probe(
                        self.probe.schema().len() + expression_index,
                    )));
                }
            }
        }
        // Clone only the tiny immutable descriptors before mutating aggregate.
        let build_group_sources = build_group_sources;
        let probe_group_sources = probe_group_sources;
        let aggregate_sources = aggregate_sources;
        aggregate.set_input_schema(self.schema.clone());

        self.initialize()?;
        // With unique build keys, probe-only groups have no join
        // multiplicity to fold. Keep the initialized join and feed ordinary
        // bounded batches to the typed aggregate instead of preparing one
        // generic group handle per matched row. This applies to aggregate
        // functions as well as key-only deduplication: the direct group arena
        // would otherwise serialize a planned parallel SUM/AVG and bypass its
        // compact typed states. No probe input has been read or is replayed.
        if !single_group
            && ((build_group_mode && !preserve_build && !aggregate.direct_build_rollup)
                || probe_group_mode)
            && self.state.as_ref().is_some_and(|state| {
                state
                    .payload
                    .i64_index
                    .as_ref()
                    .map(|index| index.all_unique())
                    .unwrap_or_else(|| {
                        state
                            .payload
                            .i64_pair_index
                            .as_ref()
                            .map(|index| index.values().all(|hits| hits.len() <= 1))
                            .unwrap_or_else(|| {
                                state.payload.index.values().all(|hits| hits.len() <= 1)
                            })
                    })
            })
        {
            return Ok(false);
        }
        if self.output.is_some() || self.grace_output.is_some() {
            return Ok(false);
        }
        if self.finished && self.state.is_none() {
            // Empty INNER/SEMI build short-circuits before opening the probe.
            // Complete the adjacent aggregate with an empty input instead of
            // falling through to state-dependent match processing.
            if single_group {
                aggregate.prepare_direct_join_single_group();
            } else {
                aggregate.prepare_direct_join_probe_groups(0);
            }
            aggregate.finish_direct_join_input(0, 0)?;
            return Ok(true);
        }

        let build_rows = self
            .state
            .as_ref()
            .and_then(|state| state.payload.columns.first())
            .map(ColumnVector::len)
            .unwrap_or(0);
        let compact_count_mode = preserve_build
            && !aggregate.direct_build_rollup
            && build_group_mode
            && matches!(build_group_sources.as_slice(), [(_, build_column)] if *build_column == self.build_keys[0])
            && aggregate_specs.iter().all(|spec| {
                matches!(
                    spec.func,
                    vec_hash_agg::AggregateKind::Count | vec_hash_agg::AggregateKind::CountStar
                )
            }) && self.state.as_ref().is_some_and(|state| {
            state
                .payload
                .i64_index
                .as_ref()
                .is_some_and(vec_hash_join::I64BuildIndex::all_unique)
                && matches!(
                    state.payload.columns.get(build_group_sources[0].1),
                    Some(ColumnVector::I64 { data, nulls })
                        if nulls.as_ref().is_none_or(|validity| validity.count_ones() == data.len())
                )
        });
        let compact_count_workers = if compact_count_mode {
            match self.probe.properties().partitioning {
                BatchPartitioning::RoundRobin { partitions } => partitions.max(1),
                _ => 1,
            }
        } else {
            1
        };
        // A stable dense handle is valid only while the direct group table is
        // resident. If the configured query quota cannot hold a conservative
        // capacity estimate, keep this same opened join and feed its ordinary
        // spill-capable batches instead.
        let mut direct_capacity = if compact_count_mode && compact_count_workers > 1 {
            aggregate.direct_join_parallel_count_capacity_bytes(build_rows, compact_count_workers)
        } else if compact_count_mode {
            aggregate.direct_join_count_capacity_bytes(build_rows)
        } else if single_group {
            aggregate.direct_join_group_capacity_bytes(1)
        } else if build_group_mode {
            build_rows
                .saturating_mul(std::mem::size_of::<u32>())
                .saturating_add(aggregate.direct_join_group_capacity_bytes(build_rows))
        } else {
            aggregate.direct_join_group_capacity_bytes(build_rows.min(64))
        };
        if aggregate.direct_build_rollup {
            let state = self.state.as_ref().expect("direct build state initialized");
            let mut key = Vec::new();
            for row in 0..build_rows {
                if row % 1024 == 0 {
                    crate::engine::cancel::check_cancelled()?;
                }
                key.clear();
                let mut values = 0usize;
                for &(output_column, build_column) in &build_group_sources {
                    let column = &state.payload.columns[build_column];
                    values = values.saturating_add(column.materialized_value_bytes(row));
                    crate::engine::executor::vec::key_encode::encode_from_column_row_with_collation(
                        column,
                        row,
                        self.schema.fields()[output_column].source.collation_id,
                        &mut key,
                    );
                }
                direct_capacity = direct_capacity
                    .saturating_add(values)
                    .saturating_add(key.len().saturating_sub(64))
                    .saturating_add(std::mem::size_of::<vec_hash_agg::GroupBucketSnapshot>())
                    .saturating_add(
                        aggregate_specs
                            .len()
                            .saturating_mul(std::mem::size_of::<vec_hash_agg::AggStateSnapshot>()),
                    );
            }
        }
        if !aggregate.reserve_direct_join_capacity(direct_capacity) {
            return Ok(false);
        }

        if compact_count_mode && compact_count_workers > 1 {
            let probe_key = self.probe_keys[0];
            let build_group_column = build_group_sources[0].1;
            let (counts, matched_words, probe_batches, probe_rows, match_rows) = {
                let state = self.state.as_ref().expect("general hash join initialized");
                run_parallel_unique_build_count_probe(
                    self.probe.as_mut(),
                    probe_key,
                    state
                        .payload
                        .i64_index
                        .as_ref()
                        .expect("compact direct count owns an integer index"),
                    &state.payload.columns,
                    &aggregate_sources,
                    build_rows,
                    compact_count_workers,
                )?
            };
            {
                let state = self.state.as_mut().expect("general hash join initialized");
                let matched_build = state
                    .matched_build
                    .as_mut()
                    .expect("build-preserved compact count owns a match bitmap");
                for (build_row, target) in matched_build.iter_mut().enumerate() {
                    *target = direct_count_group_matched(&matched_words, build_row);
                }
                aggregate.install_direct_join_count_groups(
                    &state.payload.columns[build_group_column],
                    counts,
                )?;
            }
            let mut aggregate_rows = match_rows;
            {
                let state = self.state.as_ref().expect("general hash join initialized");
                for build_row in 0..build_rows {
                    if !direct_count_group_matched(&matched_words, build_row) {
                        aggregate_rows = aggregate_rows.saturating_add(1);
                        aggregate.update_direct_join_group(
                            build_row as u32,
                            &state.payload.columns,
                            build_row,
                            None,
                            &aggregate_sources,
                        )?;
                    }
                }
            }
            self.report_pushed_runtime_rejections();
            aggregate.finish_direct_join_input(probe_batches, aggregate_rows)?;
            analyze_stats::add_pending_join_runtime(0, probe_rows, match_rows);
            self.finish_input();
            return Ok(true);
        }

        // INNER joins must not publish groups belonging only to unmatched
        // build rows. Resolve on first hit, then reuse the stable dense handle.
        let lazy_build_groups = build_group_mode && !preserve_build;
        let mut handles =
            if compact_count_mode {
                let state = self.state.as_ref().expect("general hash join initialized");
                aggregate.prepare_direct_join_count_groups(
                    &state.payload.columns[build_group_sources[0].1],
                )?;
                None
            } else if single_group {
                aggregate.prepare_direct_join_single_group();
                None
            } else if lazy_build_groups {
                aggregate.prepare_direct_join_lazy_build_groups(build_rows);
                Some(vec![u32::MAX; build_rows])
            } else if build_group_mode {
                let state = self.state.as_ref().expect("general hash join initialized");
                Some(aggregate.prepare_direct_join_build_groups(
                    &state.payload.columns,
                    &build_group_sources,
                )?)
            } else {
                aggregate.prepare_direct_join_probe_groups(build_rows.min(64));
                None
            };
        let mut probe_batches = 0u64;
        let mut probe_rows = 0u64;
        let mut match_rows = 0u64;
        let mut key = Vec::with_capacity(64);
        while let Some(mut batch) = self.probe.next_batch()? {
            crate::engine::cancel::check_cancelled()?;
            if !computed_probe_programs.is_empty() {
                let original = batch;
                let extra = computed_probe_programs
                    .iter()
                    .map(|program| match program {
                        Some(program) => expr_batch::eval_to_column(program, &original),
                        None => Ok(ColumnVector::Null {
                            len: original.num_rows(),
                        }),
                    })
                    .collect::<Result<Vec<_>, QueryError>>()?;
                let selection = original.selection().cloned();
                let mut columns = original.columns().to_vec();
                columns.extend(extra);
                let mut fields = original.schema().fields().to_vec();
                fields.extend((0..computed_probe_programs.len()).map(|index| {
                    BatchField {
                        table: "__join_expr".into(),
                        alias: None,
                        name: format!("v{index}"),
                        column_type: computed_probe_programs[index]
                            .as_ref()
                            .map(|program| program.result_type().column_type())
                            .unwrap_or(ColumnType::VarString),
                        source: Default::default(),
                    }
                }));
                batch = Batch::new(Arc::new(BatchSchema::new(fields)), columns);
                if let Some(selection) = selection {
                    batch = batch.with_selection(selection);
                }
            }
            probe_batches = probe_batches.saturating_add(1);
            probe_rows = probe_rows.saturating_add(batch.visible_rows() as u64);
            let state = self.state.as_mut().expect("general hash join initialized");
            let lookup = vec_hash_join::PayloadProbe::new(
                &state.payload,
                &batch,
                &self.probe_keys,
                &self.null_safe,
                &self.key_collations,
            );
            for probe_row in batch.visible_indices() {
                let hits = lookup.lookup(probe_row, &mut key).1;
                let Some(hits) = hits else {
                    if self.join_type == JoinType::Inner {
                        analyze_stats::add_pending_runtime_filter_rejected(1);
                    }
                    continue;
                };
                if single_group {
                    match_rows = match_rows.saturating_add(hits.len() as u64);
                    if !count_star_only {
                        aggregate.update_direct_join_single_group(
                            &state.payload.columns,
                            hits,
                            Some((&batch, probe_row)),
                            &aggregate_sources,
                        )?;
                    }
                    continue;
                }
                let probe_handle = if probe_group_mode {
                    Some(aggregate.direct_join_probe_group(
                        &batch,
                        probe_row,
                        &probe_group_sources,
                    )?)
                } else {
                    None
                };
                if probe_count_multiplicity_mode {
                    match_rows = match_rows.saturating_add(hits.len() as u64);
                    JOIN_AGG_MULTIPLICITY_ROWS_SAVED.fetch_add(
                        hits.len().saturating_sub(1) as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    aggregate.update_direct_join_probe_group_counts(
                        probe_handle.expect("probe count group handle missing"),
                        &state.payload.columns,
                        hits,
                        &batch,
                        probe_row,
                        &aggregate_sources,
                    )?;
                    continue;
                }
                for &build_row in hits {
                    match_rows = match_rows.saturating_add(1);
                    if let Some(matched) = state.matched_build.as_mut() {
                        matched[build_row as usize] = true;
                    }
                    let handle = if compact_count_mode {
                        build_row
                    } else if let Some(handle) = probe_handle {
                        handle
                    } else {
                        let handle = &mut handles.as_mut().expect("build group handles present")
                            [build_row as usize];
                        if *handle == u32::MAX {
                            *handle = aggregate.direct_join_build_group(
                                &state.payload.columns,
                                build_row as usize,
                                &build_group_sources,
                            )?;
                        }
                        *handle
                    };
                    aggregate.update_direct_join_group(
                        handle,
                        &state.payload.columns,
                        build_row as usize,
                        Some((&batch, probe_row)),
                        &aggregate_sources,
                    )?;
                }
            }
        }

        if count_star_only {
            aggregate.update_direct_join_single_group_count_star(match_rows)?;
        }

        let mut aggregate_rows = match_rows;
        if preserve_build {
            let state = self.state.as_ref().expect("general hash join initialized");
            for (build_row, matched) in state
                .matched_build
                .as_ref()
                .expect("preserved build match bitmap")
                .iter()
                .enumerate()
            {
                if !matched {
                    aggregate_rows = aggregate_rows.saturating_add(1);
                    aggregate.update_direct_join_group(
                        if compact_count_mode {
                            build_row as u32
                        } else {
                            handles.as_ref().expect("build group handles present")[build_row]
                        },
                        &state.payload.columns,
                        build_row,
                        None,
                        &aggregate_sources,
                    )?;
                }
            }
        }
        self.report_pushed_runtime_rejections();
        aggregate.finish_direct_join_input(probe_batches, aggregate_rows)?;
        analyze_stats::add_pending_join_runtime(0, probe_rows, match_rows);
        self.finish_input();
        Ok(true)
    }
}
