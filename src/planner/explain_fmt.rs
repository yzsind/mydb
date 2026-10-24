//! EXPLAIN rows and expression rendering.

use super::*;

pub fn explain_rows(plan: &PhysicalPlan) -> Vec<ExplainRow> {
    let mut rows = Vec::new();
    let mut next_plan_id = 0;
    collect_explain_rows(plan, &mut rows, &mut next_plan_id);
    rows
}

/// Non-index operators share the same empty key fields and ALL access shape.
/// Formatting and actual plan-node identity remain explicit at each caller.
pub(super) fn explain_operator_row(
    id: usize,
    select_type: String,
    table: String,
    rows: usize,
    filtered: f64,
    extra: String,
    plan_id: Option<PlanNodeId>,
) -> ExplainRow {
    ExplainRow {
        id,
        select_type,
        table,
        rows,
        filtered,
        extra,
        plan_id,
        partitions: "NULL".to_string(),
        access_type: "ALL".to_string(),
        possible_keys: String::new(),
        key: String::new(),
        key_len: String::new(),
        ref_: String::new(),
    }
}

pub(super) fn explain_index_access_row(
    plan: &PhysicalPlan,
    id: usize,
    node_id: PlanNodeId,
) -> ExplainRow {
    use explain_access::{IndexAccess, IndexAccessKind};
    let access = IndexAccess::from_plan(plan).expect("index scan matched by caller");
    let mut extra = if access.covering {
        "Using index".to_string()
    } else {
        String::new()
    };
    if let IndexAccessKind::MultiLookup { probes, .. } = access.kind {
        append_extra(
            &mut extra,
            &format!("Using IN-list multi lookup; probes={probes}"),
        );
    }
    if access.reverse {
        append_extra(&mut extra, "Backward index scan");
    }
    if let Some(note) = access.hint {
        append_extra(&mut extra, note);
    }
    let reference = match plan {
        PhysicalPlan::IndexLookup { key, .. } => explain_ref_for_index_key(key),
        _ => String::new(),
    };
    let mut row = explain_scan_row(
        id,
        access.table,
        access.access_type(),
        access.possible_keys,
        &access.index.name,
        Some(access.key_length()),
        &reference,
        access.estimated_rows,
        access.access_rows,
        access.filter.as_ref(),
        &extra,
    );
    row.plan_id = Some(node_id);
    row
}

pub(super) fn explain_scan_row(
    id: usize,
    table: &PlanTable,
    access_type: &str,
    possible_keys: &[String],
    key: &str,
    key_len_bytes: Option<usize>,
    ref_str: &str,
    estimated_rows: f64,
    access_rows: f64,
    filter: Option<&Expr>,
    extra: &str,
) -> ExplainRow {
    let mut extra = extra.to_string();
    if let Some(filter) = filter {
        append_extra(
            &mut extra,
            &format!("Using where ({})", expr_to_explain_string(filter)),
        );
    }
    if let Some(system_time) = &table.system_time {
        let mut note = format!(
            "FOR SYSTEM_TIME AS OF {}",
            expr_to_explain_string(&system_time.expr)
        );
        if let Some(scn) = crate::engine::executor::temporal::resolved_scn(system_time.binding_id) {
            note.push_str(&format!("; scn={scn}"));
        }
        if key.is_empty() {
            note.push_str("; historical heap fallback");
        } else {
            note.push_str("; historical old-key merge; heap version recheck");
        }
        append_extra(&mut extra, &note);
    }
    // Phase 6: surface stats freshness so users can correlate poor
    // plans with missing or out-of-date ANALYZE.
    if let Some(tag) = stats_freshness_tag(&table.schema, table.stats_sidecar_delta) {
        append_extra(&mut extra, tag);
    }
    // NB: conditional key-domain stats (schema.stats.conditional_key_domains)
    // feed CBO cardinality estimation (see filtered_key_domain_for_schema), but
    // their mere presence is not surfaced in EXPLAIN Extra. Like MySQL, Extra
    // describes the chosen access method; the freshness tag above only flags
    // problems (missing/stale), so we do not enumerate available statistics on
    // every scan row.
    let filtered = if access_rows > 0.0 {
        (estimated_rows / access_rows * 100.0).clamp(0.01, 100.0)
    } else {
        100.0
    };
    ExplainRow {
        id,
        select_type: "SIMPLE".to_string(),
        table: table
            .alias
            .clone()
            .unwrap_or_else(|| table.schema.name.clone()),
        partitions: explain_partitions_for_scan(table, filter),
        access_type: access_type.to_string(),
        possible_keys: null_string(possible_keys.join(",")),
        key: null_string(key.to_string()),
        key_len: match key_len_bytes {
            Some(len) => len.to_string(),
            None => "NULL".to_string(),
        },
        ref_: if ref_str.is_empty() {
            "NULL".to_string()
        } else {
            ref_str.to_string()
        },
        rows: access_rows.ceil() as usize,
        filtered,
        extra,
        plan_id: None,
    }
}

pub(super) fn explain_partitions_for_scan(table: &PlanTable, filter: Option<&Expr>) -> String {
    let Some(part) = table.schema.partitioning.as_ref() else {
        return "NULL".to_string();
    };
    let storage_ids = table
        .resolved_partition_storage_ids(filter)
        .unwrap_or_else(|| part.storage_ids());
    let mut names = Vec::new();
    for storage_id in storage_ids {
        for partition in &part.partitions {
            if partition.subpartitions.is_empty() && partition.storage_id == storage_id {
                names.push(partition.name.clone());
            } else if let Some(subpartition) = partition
                .subpartitions
                .iter()
                .find(|subpartition| subpartition.storage_id == storage_id)
            {
                names.push(subpartition.name.clone());
            }
        }
    }
    names.sort();
    names.dedup();
    if names.is_empty() {
        "empty".to_string()
    } else {
        names.join(",")
    }
}

pub(super) fn explain_ref_for_index_key(key: &IndexKey) -> String {
    key.0.iter().map(|_| "const").collect::<Vec<_>>().join(",")
}

pub(super) fn explain_ref_for_join(
    condition: Option<&Expr>,
    right_index: &IndexSchema,
    right_table: &PlanTable,
    eq_prefix_len: usize,
) -> String {
    let Some(condition) = condition else {
        return String::new();
    };
    let conjuncts = expr_conjuncts(condition);
    let mut refs = Vec::new();
    for (pos, col) in right_index.columns.iter().take(eq_prefix_len).enumerate() {
        let mut found = false;
        for conj in &conjuncts {
            if let Expr::Binary { left, op, right } = conj {
                if !matches!(op, BinaryOp::Eq | BinaryOp::NullSafeEq) {
                    continue;
                }
                let right_name = right_table
                    .alias
                    .as_deref()
                    .unwrap_or(&right_table.schema.name);
                if let Some(ast) = right_index
                    .key_expressions
                    .get(pos)
                    .and_then(Option::as_ref)
                    .and_then(|part| part.ast.as_ref())
                {
                    let found_ref = [
                        (left.as_ref(), right.as_ref()),
                        (right.as_ref(), left.as_ref()),
                    ]
                    .into_iter()
                    .find(|(_, candidate)| {
                        expr_semantically_eq(
                            &strip_qualifier_for_table(candidate, right_table),
                            ast,
                        )
                    })
                    .map(|(reference, _)| expr_to_explain_string(reference));
                    if let Some(reference) = found_ref {
                        refs.push(reference);
                        found = true;
                        break;
                    }
                }
                if let Some(left_ref) = match_join_ref_pair(left, right, col, right_name)
                    .or_else(|| match_join_ref_pair(right, left, col, right_name))
                {
                    refs.push(left_ref);
                    found = true;
                    break;
                }
            }
        }
        if !found {
            refs.push("const".to_string());
        }
    }
    refs.join(",")
}

pub(super) fn explain_binary_operand(expr: &Expr, parent: BinaryOp) -> String {
    let text = expr_to_explain_string(expr);
    if matches!(parent, BinaryOp::And)
        && matches!(
            expr,
            Expr::Binary {
                op: BinaryOp::Or,
                ..
            }
        )
    {
        format!("({text})")
    } else {
        text
    }
}

pub fn explain_result_columns() -> Vec<Column> {
    vec![
        Column {
            source: Default::default(),
            name: "id".to_string(),
            column_type: ColumnType::LongLong,
        },
        Column {
            source: Default::default(),
            name: "select_type".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "table".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "partitions".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "type".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "possible_keys".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "key".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "key_len".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "ref".to_string(),
            column_type: ColumnType::VarString,
        },
        Column {
            source: Default::default(),
            name: "rows".to_string(),
            column_type: ColumnType::LongLong,
        },
        Column {
            source: Default::default(),
            name: "filtered".to_string(),
            column_type: ColumnType::Double,
        },
        Column {
            source: Default::default(),
            name: "Extra".to_string(),
            column_type: ColumnType::VarString,
        },
    ]
}

pub fn explain_rows_to_values(rows: Vec<ExplainRow>) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            vec![
                Value::Int(row.id as i64),
                Value::String(row.select_type),
                Value::String(row.table),
                explain_string_value(row.partitions),
                Value::String(row.access_type),
                explain_string_value(row.possible_keys),
                explain_string_value(row.key),
                explain_string_value(row.key_len),
                explain_string_value(row.ref_),
                Value::Int(row.rows as i64),
                Value::Double(row.filtered),
                if row.extra.is_empty() {
                    Value::Null
                } else {
                    Value::String(row.extra)
                },
            ]
        })
        .collect()
}

pub(super) fn explain_string_value(value: String) -> Value {
    if value == "NULL" {
        Value::Null
    } else {
        Value::String(value)
    }
}

pub(super) fn collect_explain_rows(
    plan: &PhysicalPlan,
    rows: &mut Vec<ExplainRow>,
    next_plan_id: &mut u32,
) {
    let node_id = PlanNodeId(*next_plan_id);
    *next_plan_id = next_plan_id
        .checked_add(1)
        .expect("physical plan exhausted PlanNodeId space");
    match plan {
        PhysicalPlan::NullExtend { input, .. } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(&mut row.extra, "NULL extension after anti join");
            }
        }

        PhysicalPlan::SetOp {
            left,
            right,
            op,
            all,
            ..
        } => {
            collect_explain_rows(left, rows, next_plan_id);
            collect_explain_rows(right, rows, next_plan_id);
            rows.push(explain_operator_row(
                rows.len() + 1,
                "PRIMARY".to_string(),
                "<set operation>".to_string(),
                estimated_rows(plan).round() as usize,
                100.0,
                format!("Batch {:?}{}", op, if *all { " ALL" } else { "" }),
                Some(node_id),
            ));
        }
        PhysicalPlan::RecursiveUnion {
            anchor,
            recursive,
            name,
            union_all,
            max_depth,
            ..
        } => {
            collect_explain_rows(anchor, rows, next_plan_id);
            collect_explain_rows(recursive, rows, next_plan_id);
            rows.push(explain_operator_row(
                rows.len() + 1,
                "RECURSIVE".to_string(),
                format!("<recursive {name}>"),
                estimated_rows(plan).round() as usize,
                100.0,
                format!(
                    "Recursive union {}; max_depth={max_depth}",
                    if *union_all { "ALL" } else { "DISTINCT" }
                ),
                Some(node_id),
            ));
        }
        PhysicalPlan::ValuesScan {
            estimated_rows,
            filter,
            ..
        }
        | PhysicalPlan::JsonTableScan {
            estimated_rows,
            filter,
            ..
        } => rows.push(explain_operator_row(
            rows.len() + 1,
            "SIMPLE".to_string(),
            if matches!(plan, PhysicalPlan::ValuesScan { .. }) {
                "<values>".to_string()
            } else {
                "<json_table>".to_string()
            },
            estimated_rows.round() as usize,
            if filter.is_some() { 10.0 } else { 100.0 },
            "Batch source".to_string(),
            Some(node_id),
        )),
        PhysicalPlan::SystemScan {
            name,
            estimated_rows,
            filter,
            ..
        } => rows.push(explain_operator_row(
            rows.len() + 1,
            "SIMPLE".to_string(),
            name.clone(),
            estimated_rows.round() as usize,
            if filter.is_some() { 10.0 } else { 100.0 },
            "Batch system scan".to_string(),
            Some(node_id),
        )),
        PhysicalPlan::SeqScan {
            table,
            filter,
            estimated_rows,
            access_rows,
            possible_keys,
            hint_note,
            ..
        } => {
            let extra = hint_note.as_deref().unwrap_or("");
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "ALL",
                possible_keys,
                "",
                None,
                "",
                *estimated_rows,
                *access_rows,
                filter.as_ref(),
                extra,
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::ZoneMapScan {
            table,
            filter,
            estimated_rows,
            access_rows,
            possible_keys,
            hint_note,
            zone_map,
            ..
        } => {
            let mut extra = format!(
                "Zone map; pages={}/{}; ranges={}; estimate={}",
                zone_map.surviving_pages,
                zone_map.total_pages,
                zone_map.ranges,
                if zone_map.estimate_exact {
                    "exact"
                } else {
                    "sampled"
                }
            );
            if let Some(note) = hint_note {
                append_extra(&mut extra, note);
            }
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "zone",
                possible_keys,
                "",
                None,
                "",
                *estimated_rows,
                *access_rows,
                filter.as_ref(),
                &extra,
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::ParallelIndexFastScan {
            table,
            index,
            low,
            high,
            filter,
            estimated_rows,
            access_rows,
            possible_keys,
            hint_note,
            degree,
            ..
        } => {
            let mut extra = "Using index".to_string();
            if let Some(note) = hint_note {
                append_extra(&mut extra, note);
            }
            append_extra(
                &mut extra,
                &format!("Parallel index-only scan; workers={degree}"),
            );
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "index",
                possible_keys,
                &index.name,
                Some(table.schema.index_key_len(
                    index,
                    crate::planner::explain_access::range_prefix_len(index, low, high),
                )),
                "",
                *estimated_rows,
                *access_rows,
                filter.as_ref(),
                &extra,
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::ParallelIndexRangeScan {
            table,
            index,
            low,
            high,
            filter,
            estimated_rows,
            access_rows,
            possible_keys,
            hint_note,
            degree,
            ..
        } => {
            let mut extra = String::new();
            if let Some(note) = hint_note {
                append_extra(&mut extra, note);
            }
            append_extra(
                &mut extra,
                &format!("Parallel index range scan; workers={degree}"),
            );
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "range",
                possible_keys,
                &index.name,
                Some(table.schema.index_key_len(
                    index,
                    crate::planner::explain_access::range_prefix_len(index, low, high),
                )),
                "",
                *estimated_rows,
                *access_rows,
                filter.as_ref(),
                &extra,
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::IndexMerge {
            table,
            root,
            filter,
            estimated_rows,
            access_rows,
            possible_keys,
            hint_note,
            ..
        } => {
            let mut names = Vec::new();
            root.collect_index_names(&mut names);
            let mut key_lengths = Vec::new();
            collect_index_merge_key_lengths(table, root, &mut key_lengths);
            let algorithm = match root {
                IndexMergeNode::Union {
                    sort_union: true, ..
                } => "sort_union",
                IndexMergeNode::Union { .. } => "union",
                IndexMergeNode::Intersection { .. } => "intersect",
                IndexMergeNode::Leaf(_) => "union",
            };
            let mut extra = format!("Using {algorithm}({})", names.join(","));
            if let Some(note) = hint_note {
                append_extra(&mut extra, note);
            }
            let first = names.first().map(String::as_str).unwrap_or("");
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "index_merge",
                possible_keys,
                first,
                None,
                "",
                *estimated_rows,
                *access_rows,
                filter.as_ref(),
                &extra,
            );
            row.key = null_string(names.join(","));
            row.key_len = null_string(key_lengths.join(","));
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::IndexLookup { .. }
        | PhysicalPlan::IndexMultiLookup { .. }
        | PhysicalPlan::IndexRangeScan { .. }
        | PhysicalPlan::IndexFastScan { .. } => {
            rows.push(explain_index_access_row(plan, rows.len() + 1, node_id));
        }

        PhysicalPlan::FulltextScan {
            table,
            index,
            filter,
            estimated_rows,
            access_rows,
            possible_keys,
            hint_note,
            ..
        } => {
            let mut extra_str = "Using full-text index".to_string();
            if let Some(note) = hint_note {
                append_extra(&mut extra_str, note);
            }
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "fulltext",
                possible_keys,
                &index.name,
                None,
                "",
                *estimated_rows,
                *access_rows,
                filter.as_ref(),
                &extra_str,
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::SpatialScan {
            table,
            index,
            filter,
            estimated_rows,
            access_rows,
            possible_keys,
            hint_note,
            ..
        } => {
            let mut extra_str = "Using spatial index".to_string();
            if let Some(note) = hint_note {
                append_extra(&mut extra_str, note);
            }
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "range",
                possible_keys,
                &index.name,
                None,
                "",
                *estimated_rows,
                *access_rows,
                filter.as_ref(),
                &extra_str,
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::VectorIndexScan {
            table,
            index,
            metric,
            k,
            ef_search,
            filter,
            estimated_rows,
            access_rows,
            possible_keys,
            hint_note,
            ..
        } => {
            // EXPLAIN "Using vector index (<algo>)" with the metric / k and the
            // algorithm-appropriate search-effort knob (ef_search for HNSW,
            // nprobe for IVF) so a user can confirm the KNN scan fired.
            let (algo, effort_key) = match index.vector_algo() {
                crate::storage::schema::VectorAlgo::Ivf => ("IVF", "nprobe"),
                crate::storage::schema::VectorAlgo::Hnsw => ("HNSW", "ef_search"),
            };
            let mut extra_str = format!(
                "Using vector index ({algo}); metric={} k={} {effort_key}={}",
                metric.as_str(),
                k,
                ef_search
            );
            if let Some(note) = hint_note {
                append_extra(&mut extra_str, note);
            }
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "ref",
                possible_keys,
                &index.name,
                None,
                "",
                *estimated_rows,
                *access_rows,
                filter.as_ref(),
                &extra_str,
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::VectorBruteScan {
            table,
            metric,
            k,
            estimated_rows,
            access_rows,
            possible_keys,
            ..
        } => {
            // EXPLAIN: full-table exact KNN (no index). access_type ALL with
            // an Extra that names the metric / k so it's distinguishable from
            // a plain seq scan + sort.
            let extra_str = format!("Using brute-force vector scan; metric={} k={}", metric, k);
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "ALL",
                possible_keys,
                "",
                None,
                "",
                *estimated_rows,
                *access_rows,
                None,
                &extra_str,
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }

        PhysicalPlan::IndexInLookup {
            table,
            index,
            key_prefix,
            subquery_plan,
            filter,
            estimated_rows,
            access_rows,
            possible_keys,
            covering,
            hint_note,
            ..
        } => {
            let kl = table.schema.index_key_len(index, key_prefix.0.len() + 1);
            let ref_str = explain_ref_for_index_key(key_prefix);
            let mut extra_str = String::new();
            if *covering {
                extra_str.push_str("Using index");
            }
            append_extra(&mut extra_str, "Using IN subquery lookup");
            if let Some(note) = hint_note {
                append_extra(&mut extra_str, note);
            }
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "ref",
                possible_keys,
                &index.name,
                Some(kl),
                &ref_str,
                *estimated_rows,
                *access_rows,
                filter.as_ref(),
                &extra_str,
            );
            row.plan_id = Some(node_id);
            rows.push(row);
            // Traditional EXPLAIN keeps the internally-generated SIP
            // subquery out of the main table list, but the runtime stats
            // overlay walks it as a real physical child. Reserve its preorder
            // ID span so every following visible row stays aligned with the
            // stable PlanNodeIds used by EXPLAIN ANALYZE.
            *next_plan_id = next_plan_id
                .checked_add(subquery_plan.node_count() as u32)
                .expect("physical plan exhausted PlanNodeId space");
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            collect_explain_rows(left, rows, next_plan_id);
            collect_explain_rows(right, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(&mut row.extra, "Nested loop");
                if let Some(condition) = condition {
                    append_extra(
                        &mut row.extra,
                        &format!("condition={}", expr_to_explain_string(condition)),
                    );
                }
                match join_type {
                    JoinType::Semi => append_extra(&mut row.extra, "Using semi join"),
                    JoinType::Anti => append_extra(&mut row.extra, "Using anti join"),
                    _ => {}
                }
            }
        }
        PhysicalPlan::IndexNestedLoopJoin {
            left,
            right_table,
            right_index,
            condition,
            eq_prefix_len,
            estimated_rows: _,
            right_covering,
            join_type,
            ..
        } => {
            collect_explain_rows(left, rows, next_plan_id);
            let access_type = if right_index.unique && *eq_prefix_len >= right_index.columns.len() {
                "eq_ref"
            } else {
                "ref"
            };
            let kl = right_table
                .schema
                .index_key_len(right_index, *eq_prefix_len);
            let ref_str =
                explain_ref_for_join(condition.as_ref(), right_index, right_table, *eq_prefix_len);
            let mut extra_str = String::from("Index nested loop");
            if *right_covering {
                append_extra(&mut extra_str, "Using index");
            }
            if let Some(condition) = condition {
                append_extra(
                    &mut extra_str,
                    &format!("condition={}", expr_to_explain_string(condition)),
                );
            }
            match join_type {
                JoinType::Semi => append_extra(&mut extra_str, "Using semi join"),
                JoinType::Anti => append_extra(&mut extra_str, "Using anti join"),
                _ => {}
            }
            let mut row = explain_scan_row(
                rows.len() + 1,
                right_table,
                access_type,
                &[right_index.name.clone()],
                &right_index.name,
                Some(kl),
                &ref_str,
                estimate_index_lookup_rows(
                    &right_table.schema,
                    right_index,
                    *eq_prefix_len,
                    false,
                    right_table.schema.stats.row_count.max(1) as f64,
                ),
                estimate_index_lookup_rows(
                    &right_table.schema,
                    right_index,
                    *eq_prefix_len,
                    false,
                    right_table.schema.stats.row_count.max(1) as f64,
                ),
                None,
                &extra_str,
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            null_safe,
            build_side,
            residual,
            output_projection,
            ..
        } => {
            collect_explain_rows(left, rows, next_plan_id);
            collect_explain_rows(right, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                // B10 (explain-mysql80-compat): MySQL 8.0 marks the probe table
                // of a hash join `Using join buffer (hash join)` in the
                // traditional format (the TREE format shows the explicit
                // `Hash join` operator). Emit the standard phrase first, then
                // mydb's richer build-side/keys/residual diagnostic.
                append_extra(&mut row.extra, "Using join buffer (hash join)");
                append_extra(
                    &mut row.extra,
                    &format_hash_join_extra(
                        *build_side,
                        left_keys,
                        right_keys,
                        null_safe,
                        residual.as_ref(),
                    ),
                );
                if let Some(projection) = output_projection {
                    append_extra(
                        &mut row.extra,
                        &format!("Projected join ({} cols)", projection.len()),
                    );
                }
                match join_type {
                    JoinType::Semi => append_extra(&mut row.extra, "Using semi join"),
                    JoinType::Anti => append_extra(&mut row.extra, "Using anti join"),
                    _ => {}
                }
            }
        }
        PhysicalPlan::AdaptiveJoin {
            left,
            right,
            join_type,
            candidates,
            preferred,
            ..
        } => {
            if *preferred == AdaptiveJoinChoice::IndexNestedLoop {
                if let Some(AdaptiveJoinCandidate::IndexNestedLoop {
                    right_table,
                    right_index,
                    condition,
                    eq_prefix_len,
                    estimated_rows,
                    right_covering,
                    ..
                }) = candidates
                    .iter()
                    .find(|candidate| adaptive_candidate_choice(candidate) == *preferred)
                {
                    collect_explain_rows(left, rows, next_plan_id);
                    let access_type =
                        if right_index.unique && *eq_prefix_len >= right_index.columns.len() {
                            "eq_ref"
                        } else {
                            "ref"
                        };
                    let kl = right_table
                        .schema
                        .index_key_len(right_index, *eq_prefix_len);
                    let ref_str = explain_ref_for_join(
                        condition.as_ref(),
                        right_index,
                        right_table,
                        *eq_prefix_len,
                    );
                    let mut extra_str = format!(
                        "Adaptive join (preferred={}, candidates=[{}]); Index nested loop",
                        adaptive_choice_name(*preferred),
                        format_adaptive_join_candidates(candidates)
                    );
                    if *right_covering {
                        append_extra(&mut extra_str, "Using index");
                    }
                    if let Some(condition) = condition {
                        append_extra(
                            &mut extra_str,
                            &format!("condition={}", expr_to_explain_string(condition)),
                        );
                    }
                    let mut row = explain_scan_row(
                        rows.len() + 1,
                        right_table,
                        access_type,
                        &[right_index.name.clone()],
                        &right_index.name,
                        Some(kl),
                        &ref_str,
                        *estimated_rows,
                        *estimated_rows,
                        None,
                        &extra_str,
                    );
                    row.plan_id = Some(node_id);
                    rows.push(row);
                    return;
                }
            }
            collect_explain_rows(left, rows, next_plan_id);
            collect_explain_rows(right, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(
                    &mut row.extra,
                    &format!(
                        "Adaptive join (preferred={}, candidates=[{}])",
                        adaptive_choice_name(*preferred),
                        format_adaptive_join_candidates(candidates)
                    ),
                );
                match join_type {
                    JoinType::Semi => append_extra(&mut row.extra, "Using semi join"),
                    JoinType::Anti => append_extra(&mut row.extra, "Using anti join"),
                    _ => {}
                }
            }
        }
        PhysicalPlan::HashAggregate {
            input,
            aggregates,
            group_by,
            ..
        } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(&mut row.extra, "Using temporary");
                // Phase E: surface accumulator counts so plan inspection
                // shows whether GROUP BY work runs through the
                // accumulator path (Phase A) rather than the legacy
                // per-group materialization.
                append_extra(
                    &mut row.extra,
                    &format!(
                        "Accumulators: {} agg, {} group-key",
                        aggregates.len(),
                        group_by.len()
                    ),
                );
            }
        }
        PhysicalPlan::HashRollupAggregate {
            input,
            aggregates,
            group_by,
            ..
        } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(&mut row.extra, "Using temporary");
                append_extra(&mut row.extra, "Using rollup");
                append_extra(
                    &mut row.extra,
                    &format!(
                        "Accumulators: {} agg, {} group-key",
                        aggregates.len(),
                        group_by.len()
                    ),
                );
            }
        }
        PhysicalPlan::HashGroupingSetsAggregate {
            input,
            aggregates,
            group_by,
            grouping_sets,
            ..
        } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(&mut row.extra, "Using temporary");
                append_extra(
                    &mut row.extra,
                    &format!("Using grouping sets ({})", grouping_sets.len()),
                );
                append_extra(
                    &mut row.extra,
                    &format!(
                        "Accumulators: {} agg, {} group-key",
                        aggregates.len(),
                        group_by.len()
                    ),
                );
            }
        }
        PhysicalPlan::ParallelHashAggregate {
            input,
            aggregates,
            group_by,
            degree,
            ..
        } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(
                    &mut row.extra,
                    &format!("Parallel aggregate; workers={}", *degree),
                );
                append_extra(
                    &mut row.extra,
                    &format!(
                        "Accumulators: {} agg, {} group-key",
                        aggregates.len(),
                        group_by.len()
                    ),
                );
            }
        }
        PhysicalPlan::ParallelGroupingSetsAggregate {
            input,
            aggregates,
            group_by,
            grouping_sets,
            degree,
            ..
        } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(
                    &mut row.extra,
                    &format!("Parallel grouping sets aggregate; workers={}", *degree),
                );
                append_extra(
                    &mut row.extra,
                    &format!("Using grouping sets ({})", grouping_sets.len()),
                );
                append_extra(
                    &mut row.extra,
                    &format!(
                        "Accumulators: {} agg, {} group-key",
                        aggregates.len(),
                        group_by.len()
                    ),
                );
            }
        }
        PhysicalPlan::ParallelRollupAggregate {
            input,
            aggregates,
            group_by,
            degree,
            ..
        } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(
                    &mut row.extra,
                    &format!("Parallel rollup aggregate; workers={}", *degree),
                );
                append_extra(&mut row.extra, "Using rollup");
                append_extra(
                    &mut row.extra,
                    &format!(
                        "Accumulators: {} agg, {} group-key",
                        aggregates.len(),
                        group_by.len()
                    ),
                );
            }
        }
        PhysicalPlan::Sort { input, .. } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(&mut row.extra, "Using filesort");
            }
        }
        PhysicalPlan::IncrementalSort {
            input,
            presorted_prefix,
            ..
        } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(
                    &mut row.extra,
                    &format!("Using incremental filesort (presorted={presorted_prefix})"),
                );
            }
        }
        PhysicalPlan::TopK {
            input,
            k,
            dedup_projection,
            ..
        } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                let kind = if dedup_projection.is_some() {
                    "distinct top-K"
                } else {
                    "top-K"
                };
                append_extra(&mut row.extra, &format!("Using {kind} (k={k})"));
            }
        }
        PhysicalPlan::TopWindow {
            input,
            limit,
            offset,
            ..
        } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(
                    &mut row.extra,
                    &format!("Using top-window (limit={limit}, offset={offset})"),
                );
            }
        }
        PhysicalPlan::ParallelTopK {
            input,
            k,
            degree,
            dedup_projection,
            ..
        } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                let kind = if dedup_projection.is_some() {
                    "parallel distinct top-K"
                } else {
                    "parallel top-K"
                };
                append_extra(
                    &mut row.extra,
                    &format!("Using {kind} (k={k}, workers={degree})"),
                );
            }
        }
        PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Apply { outer: input, .. }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::Limit { input, .. } => {
            collect_explain_rows(input, rows, next_plan_id);
        }
        PhysicalPlan::WindowAgg { input, windows, .. } => {
            collect_explain_rows(input, rows, next_plan_id);
            let names: Vec<String> = windows
                .iter()
                .map(|w| format!("{}()", w.name.to_uppercase()))
                .collect();
            rows.push(explain_operator_row(
                0,
                "WINDOW".to_string(),
                "<window>".to_string(),
                0,
                100.0,
                format!("Window functions: {}", names.join(", ")),
                Some(node_id),
            ));
        }
        PhysicalPlan::CteScan {
            name,
            alias,
            estimated_rows,
            ..
        } => {
            rows.push(explain_operator_row(
                0,
                "SIMPLE".to_string(),
                alias.clone().unwrap_or_else(|| name.clone()),
                *estimated_rows as usize,
                100.0,
                format!("CTE scan: {}", name),
                Some(node_id),
            ));
        }
        PhysicalPlan::GenerateSeries {
            relation,
            estimated_rows,
            ..
        } => {
            rows.push(explain_operator_row(
                0,
                "SIMPLE".to_string(),
                relation.clone(),
                *estimated_rows as usize,
                100.0,
                "Function scan: generate_series".to_string(),
                Some(node_id),
            ));
        }
        PhysicalPlan::StreamingAggregate { input, .. } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(&mut row.extra, "Using streaming aggregate");
            }
        }
        PhysicalPlan::StreamingRollupAggregate { input, .. } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(&mut row.extra, "Using streaming rollup aggregate");
            }
        }
        PhysicalPlan::StreamingGroupingSetsAggregate {
            input,
            grouping_sets,
            ..
        } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(
                    &mut row.extra,
                    &format!("Using streaming grouping sets ({})", grouping_sets.len()),
                );
            }
        }
        PhysicalPlan::StreamingDistinct { input, .. } => {
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(&mut row.extra, "Using streaming distinct");
            }
        }
        PhysicalPlan::IndexAggregate {
            table,
            index,
            low,
            high,
            filter,
            estimated_rows,
            access_rows,
            ..
        } => {
            let prefix_len = crate::planner::explain_access::range_prefix_len(index, low, high);
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "index",
                &[index.name.clone()],
                &index.name,
                Some(table.schema.index_key_len(index, prefix_len)),
                "",
                *estimated_rows,
                *access_rows,
                None,
                if filter.is_some() {
                    "Using where; Using index for aggregate"
                } else {
                    "Using index for aggregate"
                },
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::CoveringIndexAggregate {
            table,
            index,
            low,
            high,
            estimated_rows,
            access_rows,
            ..
        } => {
            let prefix_len = crate::planner::explain_access::range_prefix_len(index, low, high);
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "index",
                &[index.name.clone()],
                &index.name,
                Some(table.schema.index_key_len(index, prefix_len)),
                "",
                *estimated_rows,
                *access_rows,
                None,
                "Using index for aggregate",
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::ParallelIndexAggregate {
            table,
            index,
            low,
            high,
            estimated_rows,
            access_rows,
            degree,
            ..
        } => {
            let prefix_len = crate::planner::explain_access::range_prefix_len(index, low, high);
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "index",
                &[index.name.clone()],
                &index.name,
                Some(table.schema.index_key_len(index, prefix_len)),
                "",
                *estimated_rows,
                *access_rows,
                None,
                "Using index for aggregate",
            );
            append_extra(
                &mut row.extra,
                &format!("Parallel index aggregate; workers={degree}"),
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::ParallelSeqScan {
            table,
            filter,
            estimated_rows,
            access_rows,
            possible_keys,
            hint_note,
            degree,
            ..
        } => {
            // Parallel scan looks like a SeqScan in EXPLAIN but with a
            // "Parallel scan; workers=N" tag in `extra`.
            let mut extra = hint_note.clone().unwrap_or_default();
            append_extra(&mut extra, &format!("Parallel scan; workers={}", *degree));
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "ALL",
                possible_keys,
                "",
                None,
                "",
                *estimated_rows,
                *access_rows,
                filter.as_ref(),
                &extra,
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::ParallelZoneMapScan {
            table,
            filter,
            estimated_rows,
            access_rows,
            possible_keys,
            hint_note,
            degree,
            zone_map,
            ..
        } => {
            let mut extra = format!(
                "Parallel zone map; workers={degree}; pages={}/{}; ranges={}",
                zone_map.surviving_pages, zone_map.total_pages, zone_map.ranges
            );
            if let Some(note) = hint_note {
                append_extra(&mut extra, note);
            }
            let mut row = explain_scan_row(
                rows.len() + 1,
                table,
                "zone",
                possible_keys,
                "",
                None,
                "",
                *estimated_rows,
                *access_rows,
                filter.as_ref(),
                &extra,
            );
            row.plan_id = Some(node_id);
            rows.push(row);
        }
        PhysicalPlan::Exchange {
            input,
            degree,
            output_order,
            ..
        } => {
            // Recurse first so per-worker child rows render above the
            // exchange row, then append the exchange marker.
            collect_explain_rows(input, rows, next_plan_id);
            let label = match output_order {
                ExchangeOrder::Unordered => format!("Exchange; workers={}", *degree),
                ExchangeOrder::Sorted => format!("Exchange Merge; workers={}", *degree),
            };
            let mut row = explain_operator_row(
                rows.len() + 1,
                "SIMPLE".to_string(),
                "<exchange>".to_string(),
                estimated_rows(plan).round() as usize,
                100.0,
                label,
                Some(node_id),
            );
            // Carry through alias-table from inner plan if we want
            // to surface it; simpler default is "<exchange>".
            let _ = &mut row;
            rows.push(row);
        }
        PhysicalPlan::ParallelSort { input, degree, .. } => {
            // Render input rows first, then tag the last with the
            // parallel-sort marker (analogous to how Sort does it).
            collect_explain_rows(input, rows, next_plan_id);
            if let Some(row) = rows.last_mut() {
                append_extra(
                    &mut row.extra,
                    &format!("Parallel sort; workers={}", *degree),
                );
            }
        }
    }
}
