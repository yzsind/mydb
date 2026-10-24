//! Cardinality and selectivity estimates used by access and join planning.

use super::*;

/// Estimate the cardinality visible above an aggregate without changing the
/// number of states the aggregate itself must build. HAVING predicates refer
/// to derived values for which base-table histograms are not directly
/// applicable, so use a deterministic post-aggregate prior. The exact
/// `COUNT(*) > 0` family is recognised because every materialised group is
/// non-empty by construction.
pub(super) fn estimate_having_output_rows(having: Option<&Expr>, group_count: f64) -> f64 {
    let Some(having) = having else {
        return group_count;
    };
    if group_count <= 0.0 {
        return 0.0;
    }
    match having {
        Expr::Literal(crate::parser::Literal::Bool(true)) => return group_count,
        Expr::Literal(crate::parser::Literal::Bool(false) | crate::parser::Literal::Null) => {
            return 0.0;
        }
        _ if count_star_predicate_keeps_every_group(having) => return group_count,
        _ => {}
    }
    // Expected cardinalities may legitimately be fractional. Once fewer than
    // one group is expected, a generic selectivity prior must not call
    // `clamp(1, group_count)` (invalid bounds) or inflate the estimate.
    if group_count <= 1.0 {
        return group_count;
    }
    // A simple aggregate-to-constant comparison is normally a tail query
    // (top/bottom groups). It is materially more selective than an arbitrary
    // expression over aggregate output. Keep the prior SQL-shape based and
    // deterministic: runtime memory/cache/load must never change topology.
    let selectivity = if aggregate_literal_comparison(having) {
        0.001
    } else {
        0.10
    };
    (group_count * selectivity).clamp(1.0, group_count)
}

/// Conservative posting selectivity estimate used when persisted token-DF
/// statistics are unavailable. Unlike the legacy unconditional 30%, it is
/// sensitive to the parser output, search mode and deferred parameters.
pub(super) fn estimate_fulltext_access_rows(
    table: &TableSchema,
    specs: &[FulltextMatchPlan],
    total_rows: f64,
) -> f64 {
    let mut selectivity = 1.0_f64;
    let mut restricting = 0usize;
    for spec in specs.iter().filter(|spec| spec.restrict_to_matches) {
        restricting += 1;
        let factor = if spec.query_param.is_some() {
            // The concrete query is deliberately absent from a reusable
            // prepared plan. Keep a conservative candidate estimate.
            0.50
        } else {
            let default_meta = crate::storage::schema::FulltextMeta::default();
            let meta = spec.index.fulltext.as_ref().unwrap_or(&default_meta);
            let tokenizer = crate::storage::fulltext::build_tokenizer_from_meta(meta);
            let tokens = tokenizer
                .tokenize(&spec.query)
                .into_iter()
                .collect::<std::collections::HashSet<_>>();
            let token_count = tokens.len();
            if token_count == 0 {
                0.0
            } else if let Some(stats) = table
                .stats
                .indexes
                .get(&crate::storage::normalize_name(&spec.index.name))
                .and_then(|stats| stats.fulltext.as_ref())
                .filter(|stats| stats.doc_count > 0)
            {
                let fallback_df = stats
                    .df_histogram
                    .get(stats.df_histogram.len() / 2)
                    .copied()
                    .unwrap_or_else(|| (stats.doc_count / 10).max(1));
                let dfs = tokens
                    .iter()
                    .map(|token| {
                        stats
                            .token_df_top
                            .iter()
                            .find(|entry| entry.token == *token)
                            .map(|entry| entry.document_count)
                            .unwrap_or(fallback_df)
                    })
                    .collect::<Vec<_>>();
                let estimated_docs = match spec.mode {
                    crate::parser::FtMode::NaturalLanguage => dfs.iter().sum::<u64>(),
                    crate::parser::FtMode::Boolean => dfs.iter().copied().min().unwrap_or(0),
                }
                .min(stats.doc_count);
                (estimated_docs as f64 / stats.doc_count as f64)
                    .clamp(1.0 / total_rows.max(1.0), 1.0)
            } else {
                match spec.mode {
                    // Natural-language terms use OR candidate semantics.
                    crate::parser::FtMode::NaturalLanguage => {
                        (1.0 - 0.90_f64.powi(token_count.min(16) as i32)).clamp(0.10, 0.80)
                    }
                    // Boolean syntax may contain required/excluded terms;
                    // use a deliberately broad estimate until runtime maps
                    // expose exact DFs.
                    crate::parser::FtMode::Boolean => 0.35,
                }
            }
        };
        selectivity *= factor;
    }
    if restricting == 0 {
        total_rows
    } else if selectivity == 0.0 {
        0.0
    } else {
        (total_rows * selectivity).clamp(1.0, total_rows.max(1.0))
    }
}

pub(super) fn estimate_ngram_prefilter_rows(
    table: &TableSchema,
    index: &IndexSchema,
    tokens: &[String],
    total_rows: f64,
) -> f64 {
    let Some(stats) = table
        .stats
        .indexes
        .get(&crate::storage::normalize_name(&index.name))
        .and_then(|stats| stats.fulltext.as_ref())
        .filter(|stats| stats.doc_count > 0)
    else {
        return (total_rows * 0.25_f64.powi(tokens.len().min(4) as i32))
            .clamp(1.0, total_rows.max(1.0));
    };
    let fallback = stats
        .df_histogram
        .get(stats.df_histogram.len() / 2)
        .copied()
        .unwrap_or_else(|| (stats.doc_count / 4).max(1));
    let smallest_df = tokens
        .iter()
        .map(|token| {
            stats
                .token_df_top
                .iter()
                .find(|entry| entry.token == *token)
                .map(|entry| entry.document_count)
                .unwrap_or(fallback)
        })
        .min()
        .unwrap_or(stats.doc_count);
    ((smallest_df as f64 / stats.doc_count as f64) * total_rows).clamp(1.0, total_rows.max(1.0))
}

pub(crate) fn estimate_filter_rows(table: &TableSchema, expr: &Expr, total_rows: f64) -> f64 {
    if total_rows <= 0.0 {
        return 0.0;
    }
    // Row predicates share the same typed key interpretation as access-path
    // enumeration. A partial prefix supplies a conservative upper estimate.
    let row_predicate = match expr {
        Expr::InList { expr, .. } | Expr::Between { expr, .. } => {
            matches!(expr.as_ref(), Expr::Tuple(_))
        }
        Expr::Binary { left, right, .. } => {
            matches!(left.as_ref(), Expr::Tuple(_)) || matches!(right.as_ref(), Expr::Tuple(_))
        }
        _ => false,
    };
    if row_predicate {
        if let Some(rows) = tuple_access::candidates(table, expr, &HashMap::new())
            .iter()
            .map(|access| estimate_index_access_rows(table, access, total_rows))
            .min_by(f64::total_cmp)
        {
            return rows;
        }
    }
    match expr {
        Expr::Literal(crate::parser::Literal::Bool(value)) => {
            if *value {
                total_rows
            } else {
                0.0
            }
        }
        Expr::Literal(crate::parser::Literal::Int(value)) | Expr::BoundValue(Value::Int(value)) => {
            if *value != 0 {
                total_rows
            } else {
                0.0
            }
        }
        Expr::Literal(crate::parser::Literal::UInt(value))
        | Expr::BoundValue(Value::UInt(value)) => {
            if *value != 0 {
                total_rows
            } else {
                0.0
            }
        }
        Expr::Literal(crate::parser::Literal::Null) | Expr::BoundValue(Value::Null) => 0.0,
        Expr::Binary { left, op, right } if *op == BinaryOp::And => {
            estimate_and_rows(table, left, right, total_rows)
        }
        Expr::Binary { left, op, right } if *op == BinaryOp::Or => {
            let left_rows = estimate_filter_rows(table, left, total_rows);
            if left == right {
                return left_rows;
            }
            let right_rows = estimate_filter_rows(table, right, total_rows);
            // Inclusion-exclusion under the same independence assumption
            // used by AND. Identical predicates are handled exactly above;
            // the general case avoids systematic double-counting.
            let overlap = left_rows * right_rows / total_rows;
            (left_rows + right_rows - overlap).clamp(0.0, total_rows)
        }
        Expr::Binary { left, op, right } => {
            if let (Some(column), Some(value)) = (expr_column_name(left), expr_literal_value(right))
            {
                estimate_column_predicate_rows(table, column, *op, &value, total_rows)
            } else if let (Some(value), Some(column)) =
                (expr_literal_value(left), expr_column_name(right))
            {
                estimate_column_predicate_rows(table, column, flip_cmp(*op), &value, total_rows)
            } else {
                default_filter_rows(total_rows)
            }
        }
        Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } => {
            if let (Some(column), Some(low), Some(high)) = (
                expr_column_name(expr),
                expr_literal_value(low),
                expr_literal_value(high),
            ) {
                estimate_between_rows(table, column, &low, &high, total_rows)
            } else {
                default_filter_rows(total_rows)
            }
        }
        Expr::Between {
            expr,
            negated: true,
            low,
            high,
        } => {
            if let (Some(column), Some(low), Some(high)) = (
                expr_column_name(expr),
                expr_literal_value(low),
                expr_literal_value(high),
            ) {
                let matched = estimate_between_rows(table, column, &low, &high, total_rows);
                (non_null_rows(table, column, total_rows) - matched).clamp(0.0, total_rows)
            } else {
                default_filter_rows(total_rows)
            }
        }
        Expr::IsNull(expr) => expr_column_name(expr)
            .and_then(|column| table.stats.columns.get(&normalize_expr_column(column)))
            .map(|stats| stats.null_count as f64 * stats_scale(table, total_rows))
            .unwrap_or_else(|| total_rows * 0.10)
            .clamp(0.0, total_rows),
        Expr::IsNotNull(expr) => expr_column_name(expr)
            .and_then(|column| table.stats.columns.get(&normalize_expr_column(column)))
            .map(|stats| {
                table.stats.row_count.saturating_sub(stats.null_count) as f64
                    * stats_scale(table, total_rows)
            })
            .unwrap_or_else(|| total_rows * 0.90)
            .clamp(0.0, total_rows),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if let Some(column) = expr_column_name(expr) {
                let mut values: Vec<Value> = Vec::new();
                let mut contains_null = false;
                // Long lists dedupe through hash buckets; `ValueKey` equality is
                // implied by `Value` equality, and `==` keeps the result exact.
                let mut buckets: HashMap<crate::engine::expr::ValueKey, Vec<usize>> =
                    HashMap::new();
                let hashed = list.len() > 256;
                for value in list.iter().filter_map(expr_literal_value) {
                    if matches!(value, Value::Null) {
                        contains_null = true;
                    } else if hashed {
                        let bucket = buckets
                            .entry(crate::engine::expr::ValueKey::from(&value))
                            .or_default();
                        if !bucket.iter().any(|&index| values[index] == value) {
                            bucket.push(values.len());
                            values.push(value);
                        }
                    } else if !values.iter().any(|seen| seen == &value) {
                        values.push(value);
                    }
                }
                // `x NOT IN (..., NULL, ...)` is UNKNOWN for every row that
                // did not match a concrete item, hence no row survives WHERE.
                if *negated && contains_null {
                    return 0.0;
                }
                let non_null = non_null_rows(table, column, total_rows);
                let sum: f64 = values
                    .iter()
                    .map(|value| estimate_eq_rows(table, column, value, total_rows))
                    .sum::<f64>()
                    .min(non_null);
                if *negated {
                    (non_null - sum).clamp(0.0, total_rows)
                } else {
                    sum.clamp(0.0, total_rows)
                }
            } else {
                default_filter_rows(total_rows)
            }
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
        } => {
            let est = match (expr_column_name(expr), expr_literal_string(pattern)) {
                (Some(column), Some(pat)) => {
                    estimate_like_rows(table, column, &pat, *escape_char, total_rows)
                }
                _ => default_filter_rows(total_rows),
            };
            let est = if *negated {
                expr_column_name(expr)
                    .map(|column| non_null_rows(table, column, total_rows))
                    .unwrap_or(total_rows)
                    - est
            } else {
                est
            };
            est.clamp(0.0, total_rows)
        }
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => (total_rows - estimate_filter_rows(table, expr, total_rows)).clamp(0.0, total_rows),
        _ => default_filter_rows(total_rows),
    }
}

/// DB-aware selectivity for scan predicates containing an uncorrelated
/// `IN (subquery)`. The legacy scalar estimator deliberately has no catalog
/// handle and therefore falls back to 25% for this shape. SIP-generated
/// filters use the subquery as a key-domain restriction, so that fallback can
/// turn one selected key out of five into an eight-row estimate on a 25-row
/// table. Besides being inaccurate itself, the inflated scan NDV is multiplied
/// again by downstream equijoins.
///
/// Keep the public/catalog-free estimator as the oracle for every ordinary
/// predicate. Only recurse through AND/OR nodes that actually contain a
/// subquery, and only estimate an IN keyset when the target column has NDV
/// statistics and the subquery is independently plannable.
pub(super) fn estimate_filter_rows_with_subqueries(
    db: &Database,
    table: &TableSchema,
    expr: &Expr,
    total_rows: f64,
) -> f64 {
    if total_rows <= 0.0 {
        return 0.0;
    }
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } if sip_expr_has_subquery(left) || sip_expr_has_subquery(right) => {
            let left_rows = estimate_filter_rows_with_subqueries(db, table, left, total_rows);
            let right_rows = estimate_filter_rows_with_subqueries(db, table, right, total_rows);
            (left_rows * right_rows / total_rows).clamp(0.0, total_rows)
        }
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } if sip_expr_has_subquery(left) || sip_expr_has_subquery(right) => {
            let left_rows = estimate_filter_rows_with_subqueries(db, table, left, total_rows);
            if left == right {
                return left_rows;
            }
            let right_rows = estimate_filter_rows_with_subqueries(db, table, right, total_rows);
            let overlap = left_rows * right_rows / total_rows;
            (left_rows + right_rows - overlap).clamp(0.0, total_rows)
        }
        Expr::InSubquery {
            expr: value_expr,
            subquery,
            negated: false,
        } => {
            let Some(column) = expr_column_name(value_expr) else {
                return estimate_filter_rows(table, expr, total_rows);
            };
            let Some(target_ndv) = ndv_for_column(table, column).map(|value| value as f64) else {
                return estimate_filter_rows(table, expr, total_rows);
            };
            let Some(keyset_rows) = estimate_in_lookup_subquery_result_rows(db, subquery)
                .or_else(|| estimate_subquery_result_rows(db, subquery))
            else {
                return estimate_filter_rows(table, expr, total_rows);
            };
            let non_null = non_null_rows(table, column, total_rows);
            (non_null * (keyset_rows / target_ndv.max(1.0)).clamp(0.0, 1.0)).clamp(0.0, total_rows)
        }
        _ => estimate_filter_rows(table, expr, total_rows),
    }
}

/// Estimate an AND chain under the ordinary column-independence model, but
/// treat lower+upper bounds on the same column as one range. Multiplying the
/// two one-sided selectivities turns a narrow interval in the middle of a
/// domain into roughly 25% of the table (for example one month of a seven-year
/// DATE domain), which can reverse a hash join's build side and trigger an
/// unnecessary Grace spill.
pub(super) fn estimate_independent_conjuncts(
    table: &TableSchema,
    conjuncts: &[&Expr],
    total_rows: f64,
    excluded: &[usize],
) -> f64 {
    use std::collections::{HashMap, HashSet};

    let mut ranges: HashMap<String, ConjunctRangeBounds> = HashMap::new();
    for (index, conjunct) in conjuncts.iter().enumerate() {
        if excluded.contains(&index) {
            continue;
        }
        let Some((column, op, value)) = range_column_value(conjunct) else {
            continue;
        };
        let collation = table
            .column_index(&column)
            .ok()
            .and_then(|i| table.columns[i].collation.as_deref());
        let bounds = ranges.entry(column).or_insert_with(|| ConjunctRangeBounds {
            comparable: true,
            ..ConjunctRangeBounds::default()
        });
        bounds.conjuncts.push(index);
        let slot = if matches!(op, BinaryOp::Gt | BinaryOp::GtEq) {
            &mut bounds.lower
        } else {
            &mut bounds.upper
        };
        match slot {
            None => *slot = Some((op, value)),
            Some((current_op, current)) => {
                let lower = matches!(op, BinaryOp::Gt | BinaryOp::GtEq);
                match tighter_range_bound(op, &value, *current_op, current, lower, collation) {
                    Some(true) => *slot = Some((op, value)),
                    Some(false) => {}
                    None => bounds.comparable = false,
                }
            }
        }
    }

    let mut covered = HashSet::new();
    let mut selectivity = 1.0;
    for (column, bounds) in &ranges {
        let (Some((low_op, low)), Some((high_op, high))) = (&bounds.lower, &bounds.upper) else {
            continue;
        };
        if !bounds.comparable {
            continue;
        }
        let rows = estimate_range_rows_for_column(
            table,
            column,
            Some((*low_op, low)),
            Some((*high_op, high)),
            total_rows,
        );
        selectivity *= rows / total_rows;
        covered.extend(bounds.conjuncts.iter().copied());
    }
    for (index, conjunct) in conjuncts.iter().enumerate() {
        if excluded.contains(&index) || covered.contains(&index) {
            continue;
        }
        selectivity *= estimate_filter_rows(table, conjunct, total_rows) / total_rows;
    }
    (selectivity * total_rows).clamp(0.0, total_rows)
}

/// Estimate the surviving rows for an `AND` predicate.
///
/// Baseline keeps the column-independence product. The Phase 3 bounded
/// refinement: when several conjuncts are equality predicates
/// (`col = const`) that together cover the *leading prefix* of some
/// index, the composite prefix NDV (`index_prefix_cardinality`) gives a
/// correlation-aware estimate for that group — `total / prefix_ndv` —
/// instead of multiplying each column's individual selectivity (which
/// assumes the columns are independent and badly under-estimates
/// correlated columns). Conjuncts outside the matched prefix still
/// compose under independence, so the change is a strict, bounded
/// improvement that only ever fires when a real composite index +
/// per-prefix NDV stats exist.
pub(super) fn estimate_and_rows(
    table: &TableSchema,
    left: &Expr,
    right: &Expr,
    total_rows: f64,
) -> f64 {
    // Flatten the whole AND chain so a 3+ way conjunction is visible.
    let mut conjuncts = Vec::new();
    flatten_and(left, &mut conjuncts);
    flatten_and(right, &mut conjuncts);

    // Equality and positive IN constraints on one column describe intersecting
    // domains, not independent events. This is particularly important after
    // derived/CTE predicate pushdown adds an already-present restriction. Keep
    // this normalization local to estimation: SQL expressions and their
    // evaluation order remain untouched.
    let mut domains: std::collections::BTreeMap<String, (Vec<Value>, Vec<usize>)> =
        std::collections::BTreeMap::new();
    for (index, conjunct) in conjuncts.iter().enumerate() {
        let Some((column, values)) = estimation_finite_domain(table, conjunct) else {
            continue;
        };
        match domains.entry(column) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((values, vec![index]));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let column = entry.key().clone();
                let (domain, indices) = entry.get_mut();
                domain.retain(|value| {
                    values
                        .iter()
                        .any(|other| estimation_domain_values_equal(table, &column, value, other))
                });
                indices.push(index);
            }
        }
    }
    let mut replacements = Vec::new();
    let mut covered_domains = std::collections::HashSet::new();
    for (column, (values, indices)) in domains {
        if indices.len() < 2 {
            continue;
        }
        if values.is_empty() {
            return 0.0;
        }
        let column = Box::new(Expr::Identifier(column));
        let replacement = if values.len() == 1 {
            Expr::Binary {
                left: column,
                op: BinaryOp::Eq,
                right: Box::new(Expr::BoundValue(values.into_iter().next().unwrap())),
            }
        } else {
            Expr::InList {
                expr: column,
                list: values.into_iter().map(Expr::BoundValue).collect(),
                negated: false,
            }
        };
        replacements.push(replacement);
        covered_domains.extend(indices);
    }
    conjuncts = conjuncts
        .into_iter()
        .enumerate()
        .filter_map(|(index, expr)| (!covered_domains.contains(&index)).then_some(expr))
        .chain(replacements.iter())
        .collect();

    // Independence-product baseline and fallback. Same-column ordered bounds
    // are coalesced before multiplying selectivities.
    let independence = || estimate_independent_conjuncts(table, &conjuncts, total_rows, &[]);

    if total_rows <= 0.0 || table.stats.row_count == 0 {
        return independence();
    }

    // Map each equality column -> the conjunct index that produced it.
    // (First writer wins; duplicate equalities on the same column are
    // degenerate and rare.)
    use std::collections::HashMap;
    let mut eq_cols: HashMap<String, usize> = HashMap::new();
    for (i, c) in conjuncts.iter().enumerate() {
        if let Some((col, _val)) = equality_column_value(c) {
            eq_cols.entry(col).or_insert(i);
        }
    }
    if eq_cols.len() < 2 {
        // Need at least two correlated equality columns for a composite
        // prefix to beat independence.
        return independence();
    }

    // Find the longest index prefix fully covered by equality columns,
    // for which we have a per-prefix NDV stat.
    let mut best: Option<(usize, u64, Vec<usize>)> = None; // (prefix_len, ndv, covered_conjunct_idxs)
    for index in &table.indexes {
        // Walk the leading columns while they're all equality-constrained.
        let mut covered = Vec::new();
        let mut prefix_len = 0usize;
        for col in &index.columns {
            let norm = normalize_expr_column(col);
            match eq_cols.get(&norm) {
                Some(&idx) => {
                    covered.push(idx);
                    prefix_len += 1;
                }
                None => break,
            }
        }
        if prefix_len < 2 {
            continue;
        }
        if let Some(ndv) = table.index_prefix_cardinality(&index.name, prefix_len) {
            if ndv == 0 {
                continue;
            }
            let take = match &best {
                Some((best_len, _, _)) => prefix_len > *best_len,
                None => true,
            };
            if take {
                best = Some((prefix_len, ndv, covered));
            }
        }
    }

    // Phase 3 (full): also consider collected multi-column groups
    // (composite-index column *sets* and foreign-key column tuples),
    // matched order-independently against the equality columns. This
    // covers correlated groups the leading-prefix walk above misses — an
    // FK pair with no covering index, or an index queried out of column
    // order (`WHERE b = ? AND a = ?` against `INDEX (a, b)`). Prefer
    // whichever source covers the most columns.
    for mc in table.stats.multi_column_stats.values() {
        if mc.distinct_count == 0 || mc.columns.len() < 2 {
            continue;
        }
        let mut covered = Vec::with_capacity(mc.columns.len());
        let mut all_eq = true;
        for col in &mc.columns {
            match eq_cols.get(&normalize_expr_column(col)) {
                Some(&idx) => covered.push(idx),
                None => {
                    all_eq = false;
                    break;
                }
            }
        }
        if !all_eq {
            continue;
        }
        let take = match &best {
            Some((best_len, _, _)) => mc.columns.len() > *best_len,
            None => true,
        };
        if take {
            best = Some((mc.columns.len(), mc.distinct_count, covered));
        }
    }

    let Some((_group_cols, ndv, covered)) = best else {
        return independence();
    };

    // Estimate the covered (correlated) group via composite prefix NDV,
    // scaled from the sampled population to `total_rows`. Distinct keys
    // are evenly populated under the standard uniform-within-NDV model.
    let scale = stats_scale(table, total_rows);
    let analyzed_non_empty = (table.stats.row_count as f64).max(1.0);
    // A conjunction cannot exceed any of its marginals. Joint NDV keeps
    // correlated columns from being multiplied independently, but its average
    // must not override a rare (or absent) concrete value's frequency.
    let group_rows = covered.iter().fold(
        ((analyzed_non_empty / ndv as f64) * scale).clamp(0.0, total_rows),
        |rows, &idx| {
            let Some((column, _)) = equality_column_value(conjuncts[idx]) else {
                return rows;
            };
            // Without value frequencies, a marginal is another uniform-NDV
            // guess, not evidence against the correlation-aware group prior.
            let has_frequency = table
                .stats
                .columns
                .get(&column)
                .is_some_and(|stats| !stats.top_values.is_empty() || stats.frequency.complete);
            if has_frequency {
                rows.min(estimate_filter_rows(table, conjuncts[idx], total_rows))
            } else {
                rows
            }
        },
    );

    // Remaining (uncovered) conjuncts compose under independence on top
    // of the correlated group.
    let remaining_rows = estimate_independent_conjuncts(table, &conjuncts, total_rows, &covered);
    (group_rows * remaining_rows / total_rows).clamp(0.0, total_rows)
}

pub(super) fn estimate_like_rows(
    table: &TableSchema,
    column: &str,
    pattern: &str,
    escape: Option<char>,
    total_rows: f64,
) -> f64 {
    let shape = crate::engine::expr::analyze_like_sargable_pattern(pattern, escape);
    if let Some(crate::engine::expr::LikeSargablePattern::Literal(literal)) = shape.as_ref() {
        return estimate_eq_rows(table, column, &Value::String(literal.clone()), total_rows);
    }
    if let Some(crate::engine::expr::LikeSargablePattern::Prefix(prefix)) = shape {
        let collation = table
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(column))
            .and_then(|c| c.collation.as_deref());
        let next = crate::engine::expr::like_prefix_successor_for_collation(&prefix, collation);
        // Base range estimate from the histogram on the constant
        // prefix. B8: apply a "residual pattern" discount for any
        // non-wildcard chars that appear AFTER the first `%` / `_`,
        // which the histogram range can't account for. Each extra
        // literal char shrinks the estimated match ratio geometrically
        // via `like_char_selectivity` (PG-style approach).
        let range_rows = estimate_range_rows_for_column(
            table,
            column,
            Some((BinaryOp::GtEq, &Value::String(prefix.clone()))),
            next.as_ref().map(|n| (BinaryOp::Lt, n as &Value)),
            total_rows,
        );
        let residual = crate::engine::expr::like_literal_segments(pattern, escape)
            .map(|segments| {
                segments
                    .iter()
                    .map(|segment| segment.chars().count())
                    .sum::<usize>()
                    .saturating_sub(prefix.chars().count())
            })
            .unwrap_or(0);
        return apply_like_residual_discount(range_rows, residual);
    }
    // Fully unanchored (`'%foo%'`, `'%foo'`, `'_foo'`): the configured
    // default is already the selectivity of a complete LIKE pattern, not of
    // its first literal character.  Discounting it once for *every* literal
    // character double-counts the pattern selectivity and badly underestimates
    // ordinary word searches (for example a five-character contains pattern
    // becomes 0.05 * 0.85^5 = 2.2%).  Keep a short, typical pattern on the
    // whole-pattern default and only add the geometric tail for unusually
    // long literals.  This remains schema/query agnostic while preserving the
    // useful distinction between `%foo%` and a much longer multi-segment
    // pattern.
    let base = default_like_rows(total_rows);
    let literal_chars = crate::engine::expr::like_literal_segments(pattern, escape)
        .map(|segments| segments.iter().map(|segment| segment.chars().count()).sum())
        .unwrap_or(0);
    apply_like_residual_discount(base, unanchored_like_literal_tail(literal_chars))
}

pub(super) fn estimate_column_predicate_rows(
    table: &TableSchema,
    column: &str,
    op: BinaryOp,
    value: &Value,
    total_rows: f64,
) -> f64 {
    match op {
        BinaryOp::Eq => estimate_eq_rows(table, column, value, total_rows),
        BinaryOp::NullSafeEq if matches!(value, Value::Null) => {
            (total_rows - non_null_rows(table, column, total_rows)).clamp(0.0, total_rows)
        }
        BinaryOp::NullSafeEq => estimate_eq_rows(table, column, value, total_rows),
        BinaryOp::NotEq => {
            let non_null = non_null_rows(table, column, total_rows);
            (non_null - estimate_eq_rows(table, column, value, total_rows)).clamp(0.0, total_rows)
        }
        BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq => {
            estimate_range_rows_for_column(table, column, Some((op, value)), None, total_rows)
        }
        _ => default_filter_rows(total_rows),
    }
}

pub(super) fn estimate_eq_rows(
    table: &TableSchema,
    column: &str,
    value: &Value,
    total_rows: f64,
) -> f64 {
    if total_rows <= 0.0 || matches!(value, Value::Null) {
        return 0.0;
    }
    let normalized = normalize_expr_column(column);
    let column_schema = table
        .columns
        .iter()
        .find(|candidate| candidate.name.eq_ignore_ascii_case(&normalized));
    let coerced = column_schema.and_then(|schema| {
        crate::engine::expr::coerce_comparison_constant_for_type(value, &schema.data_type)
    });
    let value = coerced.as_ref().unwrap_or(value);
    let Some(stats) = table.stats.columns.get(&normalized) else {
        return default_eq_rows(total_rows);
    };
    let scale = stats_scale(table, total_rows);
    let collation = column_schema
        .and_then(|column| column.collation.as_deref())
        .map(crate::charset::collation_id);
    let matching_top_count = crate::engine::expr::with_compare_collation(collation, || {
        stats
            .top_values
            .iter()
            .filter(|freq| stats_values_equal(&freq.value, value))
            .map(|freq| freq.count)
            .sum::<u64>()
    });
    if matching_top_count > 0 {
        let non_null = (table.stats.row_count.saturating_sub(stats.null_count)) as f64 * scale;
        let uniform = if stats.distinct_count > 0 {
            non_null / stats.distinct_count as f64
        } else {
            0.0
        };
        if stats.frequency.source != crate::storage::FrequencySource::LowerBound {
            return (matching_top_count as f64 * scale).clamp(0.0, total_rows);
        }
        // A sketch counter is a lower bound. Use the uniform prior only
        // within its recorded error interval, never as an unconditional
        // minimum. SQL-equal physical values each contribute an error bound.
        let count = matching_top_count as f64 * scale;
        let upper = stats
            .frequency
            .count_error
            .map(|error| {
                let matches = crate::engine::expr::with_compare_collation(collation, || {
                    stats
                        .top_values
                        .iter()
                        .filter(|f| stats_values_equal(&f.value, value))
                        .count()
                });
                count + error as f64 * matches as f64 * scale
            })
            .unwrap_or(total_rows);
        return count.max(uniform.min(upper)).clamp(0.0, total_rows);
    }
    let non_null = (table.stats.row_count.saturating_sub(stats.null_count)) as f64 * scale;
    if stats.frequency.complete
        && stats.frequency.source != crate::storage::FrequencySource::LowerBound
    {
        return match stats.frequency.source {
            crate::storage::FrequencySource::Exact => 0.0,
            // Absence from a page sample is not proof of population absence.
            // Keep one observed-row equivalent as an uncertainty allowance.
            crate::storage::FrequencySource::Sample => {
                (total_rows / stats.frequency.sample_rows.max(1) as f64).min(non_null)
            }
            crate::storage::FrequencySource::LowerBound => non_null,
        };
    }
    if stats.distinct_count > 0 {
        // For lower-bound counters this is a conservative upper estimate of
        // residual mass, not a claim that the retained frequencies are exact.
        let top_mass = stats.top_values.iter().map(|f| f.count).sum::<u64>() as f64 * scale;
        let top_ndv = stats.top_values.len() as u64;
        let residual_rows = (non_null - top_mass).max(0.0);
        let residual_ndv = stats.distinct_count.saturating_sub(top_ndv).max(1) as f64;
        (residual_rows / residual_ndv).clamp(0.0, total_rows)
    } else {
        default_eq_rows(total_rows)
    }
}

pub(super) fn estimate_index_lookup_rows(
    table: &TableSchema,
    index: &IndexSchema,
    prefix_len: usize,
    exact: bool,
    total_rows: f64,
) -> f64 {
    if index.unique && (exact || prefix_len >= index.columns.len()) {
        return total_rows.min(1.0).max(0.0);
    }
    if table.stats.row_count == 0 {
        return default_eq_rows(total_rows);
    }
    let stat_card = table
        .index_prefix_cardinality(&index.name, prefix_len)
        .map(|cardinality| {
            if prefix_len == 1 {
                index
                    .columns
                    .first()
                    .map(|column| single_column_lookup_cardinality(table, column, cardinality))
                    .unwrap_or(cardinality)
            } else {
                cardinality
            }
        });
    stat_card
        .filter(|cardinality| *cardinality > 0)
        .map(|cardinality| (total_rows / cardinality as f64).clamp(0.0, total_rows))
        .unwrap_or_else(|| default_eq_rows(total_rows))
}

/// Concrete lookup keys can use column frequencies; parameterized join
/// probes still use average prefix cardinality. A composite key's population
/// cannot exceed any full-column equality in that key. Keep joint NDV as the
/// prior (rather than multiplying correlated marginals independently).
pub(super) fn estimate_constant_index_lookup_rows(
    table: &TableSchema,
    index: &IndexSchema,
    key: &IndexKey,
    exact: bool,
    total_rows: f64,
) -> f64 {
    let fallback = estimate_index_lookup_rows(table, index, key.0.len(), exact, total_rows);
    if index.unique && (exact || key.0.len() >= index.columns.len()) {
        return fallback;
    }
    let mut rows = fallback;
    for (pos, part) in key.0.iter().enumerate() {
        // Truncated and expression keys don't identify a whole column value.
        if index.column_prefix_len(pos).is_some()
            || !matches!(
                index.key_part(pos),
                None | Some(crate::storage::IndexKeyPart::Column(_))
            )
        {
            continue;
        }
        let Some(column) = index.columns.get(pos).and_then(|name| {
            table
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(name))
        }) else {
            continue;
        };
        let Some(value) = concrete_key_part_value(part, &column.data_type) else {
            continue;
        };
        let Some(stats) = table
            .stats
            .columns
            .get(&normalize_expr_column(&column.name))
        else {
            continue;
        };
        if key.0.len() > 1 && stats.top_values.is_empty() && !stats.frequency.complete {
            continue;
        }
        let marginal = estimate_column_predicate_rows(
            table,
            &column.name,
            BinaryOp::NullSafeEq,
            &value,
            total_rows,
        );
        // A single equality uses its frequency directly, including hot keys
        // above the NDV average. Composite equalities only supply an upper bound.
        rows = if key.0.len() == 1 {
            marginal
        } else {
            rows.min(marginal)
        };
    }
    rows
}

pub(super) fn estimate_constant_index_multi_lookup_rows(
    table: &TableSchema,
    index: &IndexSchema,
    keys: &[IndexKey],
    exact: bool,
    total_rows: f64,
) -> f64 {
    if let Some(key) = keys.first() {
        if index.unique && (exact || key.0.len() >= index.columns.len()) {
            return (estimate_index_lookup_rows(table, index, key.0.len(), exact, total_rows)
                * keys.len() as f64)
                .clamp(0.0, total_rows.max(0.0));
        }
    }
    keys.iter()
        .map(|key| estimate_constant_index_lookup_rows(table, index, key, exact, total_rows))
        .sum::<f64>()
        .clamp(0.0, total_rows.max(0.0))
}

pub(super) fn estimate_between_rows(
    table: &TableSchema,
    column: &str,
    low: &Value,
    high: &Value,
    total_rows: f64,
) -> f64 {
    estimate_range_rows_for_column(
        table,
        column,
        Some((BinaryOp::GtEq, low)),
        Some((BinaryOp::LtEq, high)),
        total_rows,
    )
}

pub(super) fn estimate_range_rows_for_column(
    table: &TableSchema,
    column: &str,
    first: Option<(BinaryOp, &Value)>,
    second: Option<(BinaryOp, &Value)>,
    total_rows: f64,
) -> f64 {
    let normalized = normalize_expr_column(column);
    let column_type = table
        .columns
        .iter()
        .find(|candidate| candidate.name.eq_ignore_ascii_case(&normalized))
        .map(|schema| &schema.data_type);
    let first_owned = first.map(|(op, value)| {
        let value = column_type
            .and_then(|data_type| {
                crate::engine::expr::coerce_comparison_constant_for_type(value, data_type)
            })
            .unwrap_or_else(|| value.clone());
        (op, value)
    });
    let second_owned = second.map(|(op, value)| {
        let value = column_type
            .and_then(|data_type| {
                crate::engine::expr::coerce_comparison_constant_for_type(value, data_type)
            })
            .unwrap_or_else(|| value.clone());
        (op, value)
    });
    let first = first_owned.as_ref().map(|(op, value)| (*op, value));
    let second = second_owned.as_ref().map(|(op, value)| (*op, value));
    let Some(stats) = table.stats.columns.get(&normalized) else {
        return default_range_rows(total_rows);
    };
    let (Some(min), Some(max)) = (stats.min.as_ref(), stats.max.as_ref()) else {
        return default_range_rows(total_rows);
    };
    let numeric_bounds = numeric_stat_value_opt(min)
        .zip(numeric_stat_value_opt(max))
        .filter(|_| {
            first
                .iter()
                .chain(second.iter())
                .all(|(_, value)| numeric_stat_value_opt(value).is_some())
        });
    let Some((min_n, max_n)) = numeric_bounds else {
        return estimate_ordered_range_rows(
            table, column, stats, min, max, first, second, total_rows,
        );
    };
    // Track raw user bounds (strict vs inclusive) for top_values matching.
    let mut user_low = f64::NEG_INFINITY;
    let mut user_low_inclusive = true;
    let mut user_high = f64::INFINITY;
    let mut user_high_inclusive = true;
    for (op, value) in first.into_iter().chain(second) {
        let v = numeric_stat_value(value);
        match op {
            BinaryOp::Gt => {
                if v > user_low || (v == user_low && user_low_inclusive) {
                    user_low = v;
                    user_low_inclusive = false;
                }
            }
            BinaryOp::GtEq => {
                if v > user_low {
                    user_low = v;
                    user_low_inclusive = true;
                }
            }
            BinaryOp::Lt => {
                if v < user_high || (v == user_high && user_high_inclusive) {
                    user_high = v;
                    user_high_inclusive = false;
                }
            }
            BinaryOp::LtEq => {
                if v < user_high {
                    user_high = v;
                    user_high_inclusive = true;
                }
            }
            _ => {}
        }
    }
    if (max_n - min_n).abs() < f64::EPSILON {
        let point = min_n;
        let above_low = if user_low_inclusive {
            point >= user_low
        } else {
            point > user_low
        };
        let below_high = if user_high_inclusive {
            point <= user_high
        } else {
            point < user_high
        };
        return if above_low && below_high {
            total_rows.max(0.0)
        } else {
            0.0
        };
    }
    // Clip against [min, max] for the linear-interpolation portion.
    let low = user_low.max(min_n);
    let high = user_high.min(max_n);
    if high < low {
        return 0.0;
    }
    // Tier 1 #7-lite: combine exact `top_values` frequencies with a
    // linear interpolation over the *remaining* rows. For skewed
    // columns (most real OLTP data) this is dramatically more
    // accurate than pure linear over [min, max] alone.
    let mut top_in_range: f64 = 0.0;
    let mut top_total: f64 = 0.0;
    for freq in &stats.top_values {
        let v = numeric_stat_value(&freq.value);
        let scaled_count = freq.count as f64 * stats_scale(table, total_rows);
        top_total += scaled_count;
        let above_low = if user_low_inclusive {
            v >= user_low
        } else {
            v > user_low
        };
        let below_high = if user_high_inclusive {
            v <= user_high
        } else {
            v < user_high
        };
        if above_low && below_high {
            top_in_range += scaled_count;
        }
    }
    let non_top_total = (total_rows - top_total).max(0.0);
    let non_top_in_range = if !stats.histogram.is_empty() {
        // Phase B #B6: prefer the equi-depth histogram. The first
        // bucket spans `[min, upper_1]`, subsequent ones
        // `(upper_{k-1}, upper_k]`. Linear interpolation within
        // boundary buckets handles partial overlaps.
        let hist_in_range = histogram_estimate_in_range(
            &stats.histogram,
            min_n,
            user_low,
            user_high,
            user_low_inclusive,
            user_high_inclusive,
        ) * stats_scale(table, total_rows);
        (hist_in_range - top_in_range).max(0.0)
    } else if stats.top_values.is_empty() {
        if let Some(rows) = estimate_discrete_temporal_range_rows(
            stats,
            user_low,
            user_low_inclusive,
            user_high,
            user_high_inclusive,
            total_rows,
        ) {
            return rows;
        }
        ((high - low) / (max_n - min_n)) * non_top_total
    } else {
        ((high - low) / (max_n - min_n)) * non_top_total
    };
    (top_in_range + non_top_in_range).clamp(0.0, total_rows)
}

pub(super) fn estimate_discrete_temporal_range_rows(
    stats: &crate::storage::ColumnStats,
    user_low: f64,
    user_low_inclusive: bool,
    user_high: f64,
    user_high_inclusive: bool,
    total_rows: f64,
) -> Option<f64> {
    if !user_low.is_finite() || !user_high.is_finite() || stats.distinct_count == 0 {
        return None;
    }

    let (min_day, max_day) = match (stats.min.as_ref()?, stats.max.as_ref()?) {
        (Value::Date(min), Value::Date(max)) => (*min as i64, *max as i64),
        _ => return None,
    };

    let non_null = (total_rows - stats.null_count as f64).max(0.0);
    if non_null <= 0.0 {
        return Some(0.0);
    }

    // With bounded catalog stats we may have lost histograms/top-values
    // for wide dimension tables. DATE columns are discrete, so a bounded
    // range over a dense calendar should estimate from the number of
    // covered days rather than from min/max interpolation. This also
    // avoids the MySQL zero-date sentinel (`i32::MIN`) stretching the
    // denominator to billions of days after histogram trimming.
    let zero_date_sentinel = min_day == i32::MIN as i64;
    if !zero_date_sentinel && max_day > min_day {
        let span = (max_day - min_day + 1) as f64;
        let density = stats.distinct_count as f64 / span;
        if density < 0.25 {
            return None;
        }
    }

    let low = if user_low_inclusive {
        user_low.ceil() as i64
    } else {
        user_low.floor() as i64 + 1
    };
    let high = if user_high_inclusive {
        user_high.floor() as i64
    } else {
        user_high.ceil() as i64 - 1
    };
    if high < low {
        return Some(0.0);
    }
    let covered_values = (high - low + 1) as f64;
    let rows_per_distinct = non_null / stats.distinct_count as f64;
    Some((covered_values * rows_per_distinct).clamp(0.0, total_rows.max(0.0)))
}

pub(crate) fn estimate_group_by_rows_for_plan(
    plan: &PhysicalPlan,
    group_by: &[Expr],
    input_rows: f64,
) -> f64 {
    if group_by.is_empty() {
        return 1.0;
    }
    if input_rows <= 0.0 {
        return 0.0;
    }
    let mut rows = 1.0_f64;
    for expr in group_by {
        let Some(column) = expr_column_name(expr) else {
            return default_group_by_rows(input_rows);
        };
        let Some(ndv) = ndv_in_plan(plan, column) else {
            return default_group_by_rows(input_rows);
        };
        rows *= ndv as f64;
    }
    rows.clamp(0.0, input_rows)
}

pub(super) fn estimate_grouping_sets_rows_for_plan(
    plan: &PhysicalPlan,
    group_by: &[Expr],
    grouping_sets: &[Vec<usize>],
    input_rows: f64,
) -> f64 {
    if grouping_sets.is_empty() {
        return estimate_group_by_rows_for_plan(plan, group_by, input_rows);
    }
    let mut rows = 0.0_f64;
    for set in grouping_sets {
        let exprs: Vec<Expr> = set
            .iter()
            .filter_map(|idx| group_by.get(*idx).cloned())
            .collect();
        rows += estimate_group_by_rows_for_plan(plan, &exprs, input_rows);
    }
    rows.clamp(0.0, input_rows.max(0.0) * grouping_sets.len().max(1) as f64)
}

/// Range estimate for text and other orderable values that do not have a
/// meaningful numeric distance. Equi-depth buckets still provide rank
/// information: complete buckets are counted exactly and a boundary bucket
/// contributes half its population. This deliberately avoids pretending
/// arbitrary strings are the number zero.
pub(super) fn estimate_ordered_range_rows(
    table: &TableSchema,
    column: &str,
    stats: &crate::storage::ColumnStats,
    min: &Value,
    max: &Value,
    first: Option<(BinaryOp, &Value)>,
    second: Option<(BinaryOp, &Value)>,
    total_rows: f64,
) -> f64 {
    let collation = table
        .columns
        .iter()
        .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
        .and_then(|candidate| candidate.collation.as_deref());
    let compare = |left: &Value, right: &Value| {
        crate::storage::partitioning::compare_value_with_collation(left, right, collation)
    };

    let mut low: Option<(&Value, bool)> = None;
    let mut high: Option<(&Value, bool)> = None;
    for (op, value) in first.into_iter().chain(second) {
        match op {
            BinaryOp::Gt | BinaryOp::GtEq => {
                let inclusive = op == BinaryOp::GtEq;
                if low.is_none_or(|(current, current_inclusive)| {
                    compare(value, current).is_gt()
                        || (compare(value, current).is_eq() && current_inclusive && !inclusive)
                }) {
                    low = Some((value, inclusive));
                }
            }
            BinaryOp::Lt | BinaryOp::LtEq => {
                let inclusive = op == BinaryOp::LtEq;
                if high.is_none_or(|(current, current_inclusive)| {
                    compare(value, current).is_lt()
                        || (compare(value, current).is_eq() && current_inclusive && !inclusive)
                }) {
                    high = Some((value, inclusive));
                }
            }
            _ => {}
        }
    }

    let in_range = |value: &Value| {
        let above_low = low.is_none_or(|(bound, inclusive)| {
            let ord = compare(value, bound);
            ord.is_gt() || (inclusive && ord.is_eq())
        });
        let below_high = high.is_none_or(|(bound, inclusive)| {
            let ord = compare(value, bound);
            ord.is_lt() || (inclusive && ord.is_eq())
        });
        above_low && below_high
    };

    if !in_range(min) && !in_range(max) {
        if low.is_some_and(|(bound, _)| compare(bound, max).is_gt())
            || high.is_some_and(|(bound, _)| compare(bound, min).is_lt())
        {
            return 0.0;
        }
    }

    let scale = stats_scale(table, total_rows);
    let non_null = non_null_rows(table, column, total_rows);
    let top_in_range = stats
        .top_values
        .iter()
        .filter(|freq| in_range(&freq.value))
        .map(|freq| freq.count as f64 * scale)
        .sum::<f64>();

    if stats.histogram.is_empty() {
        if in_range(min) && in_range(max) {
            return non_null;
        }
        return default_range_rows(non_null).max(top_in_range).min(non_null);
    }

    let mut previous = min;
    let mut previous_cumulative = 0u64;
    let mut histogram_rows = 0.0;
    for bucket in &stats.histogram {
        let bucket_rows =
            bucket.cumulative_count.saturating_sub(previous_cumulative) as f64 * scale;
        let lower_inside = in_range(previous);
        let upper_inside = in_range(&bucket.upper);
        if lower_inside && upper_inside {
            histogram_rows += bucket_rows;
        } else if lower_inside || upper_inside {
            histogram_rows += bucket_rows * 0.5;
        } else {
            let crosses_low = low.is_some_and(|(bound, _)| {
                !compare(previous, bound).is_gt() && !compare(&bucket.upper, bound).is_lt()
            });
            let crosses_high = high.is_some_and(|(bound, _)| {
                !compare(previous, bound).is_gt() && !compare(&bucket.upper, bound).is_lt()
            });
            if crosses_low || crosses_high {
                histogram_rows += bucket_rows * 0.5;
            }
        }
        previous = &bucket.upper;
        previous_cumulative = bucket.cumulative_count;
    }
    histogram_rows.max(top_in_range).clamp(0.0, non_null)
}

pub(super) fn estimate_index_access_rows(
    table: &TableSchema,
    access: &IndexAccess,
    total_rows: f64,
) -> f64 {
    match &access.kind {
        IndexAccessKind::Lookup { key, exact } => {
            estimate_constant_index_lookup_rows(table, &access.index, key, *exact, total_rows)
        }
        IndexAccessKind::MultiLookup { keys, exact } => estimate_constant_index_multi_lookup_rows(
            table,
            &access.index,
            keys,
            *exact,
            total_rows,
        ),
        IndexAccessKind::Range { low, high } => {
            estimate_index_range_access_rows(table, &access.index, low, high, total_rows)
        }
    }
}

pub(super) fn estimate_index_output_rows(
    table: &TableSchema,
    filter: Option<&Expr>,
    access_rows: f64,
    total_rows: f64,
) -> f64 {
    let Some(filter) = filter else {
        return access_rows;
    };
    if total_rows <= 0.0 {
        return access_rows.max(1.0);
    }
    let filtered_rows = estimate_filter_rows(table, filter, total_rows);
    filtered_rows.clamp(1.0, access_rows.max(1.0))
}

pub(super) fn estimate_index_range_access_rows(
    table: &TableSchema,
    index: &IndexSchema,
    low: &Option<IndexBound>,
    high: &Option<IndexBound>,
    total_rows: f64,
) -> f64 {
    if total_rows <= 0.0 {
        return 0.0;
    }
    // Find only the prefix actually fixed by BOTH endpoints. A tuple
    // bound (a,b)>(1,2) does not constrain a=1.
    let common = match (low, high) {
        (Some(low), Some(high)) => low
            .key
            .0
            .iter()
            .zip(&high.key.0)
            .take_while(|(a, b)| a == b && !matches!(a, KeyPart::Param { .. }))
            .count(),
        _ => 0,
    };
    let width = low
        .as_ref()
        .map_or(0, |b| b.key.0.len())
        .max(high.as_ref().map_or(0, |b| b.key.0.len()));
    if width <= common + 1 {
        return estimate_scalar_index_range_access_rows(table, index, low, high, total_rows);
    }
    let domain = if common == 0 {
        total_rows
    } else {
        estimate_index_lookup_rows(table, index, common, false, total_rows)
    };
    if let (Some(lo), Some(hi)) = (low, high) {
        if lo.key.0.len() == hi.key.0.len()
            && lo
                .key
                .0
                .iter()
                .chain(&hi.key.0)
                .all(|p| !matches!(p, KeyPart::Param { .. }))
        {
            match lo.key.cmp(&hi.key) {
                std::cmp::Ordering::Greater => return 0.0,
                std::cmp::Ordering::Equal => {
                    return if lo.inclusive && hi.inclusive {
                        domain
                    } else {
                        0.0
                    }
                }
                _ => {}
            }
        }
    }
    if table.stats.row_count == 0 {
        return default_range_rows(domain);
    }
    let cdf = |bound: &IndexBound, inclusive: bool| -> Option<f64> {
        let mut less = 0.0;
        for pos in common..bound.key.0.len() {
            let column = index.columns.get(pos)?;
            let schema = table
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(column))?;
            let value = concrete_key_part_value(&bound.key.0[pos], &schema.data_type)?;
            let prefix_rows = if pos == 0 {
                total_rows
            } else {
                estimate_index_lookup_rows(table, index, pos, false, total_rows)
            };
            let fraction = estimate_range_rows_for_column(
                table,
                column,
                None,
                Some((BinaryOp::Lt, &value)),
                total_rows,
            ) / total_rows;
            less += prefix_rows * fraction;
        }
        if inclusive {
            less += if bound.key.0.is_empty() {
                total_rows
            } else {
                estimate_index_lookup_rows(table, index, bound.key.0.len(), false, total_rows)
            };
        }
        Some(less.clamp(0.0, domain))
    };
    let upper = high
        .as_ref()
        .and_then(|b| cdf(b, b.inclusive))
        .unwrap_or(domain);
    let lower = low
        .as_ref()
        .and_then(|b| cdf(b, !b.inclusive))
        .unwrap_or(0.0);
    (upper - lower).clamp(0.0, domain)
}

pub(super) fn estimate_scalar_index_range_access_rows(
    table: &TableSchema,
    index: &IndexSchema,
    low: &Option<IndexBound>,
    high: &Option<IndexBound>,
    total_rows: f64,
) -> f64 {
    if total_rows <= 0.0 {
        return 0.0;
    }
    let Some(bound) = low.as_ref().or(high.as_ref()) else {
        return default_range_rows(total_rows);
    };
    let Some(range_pos) = bound.key.0.len().checked_sub(1) else {
        return default_range_rows(total_rows);
    };
    let Some(range_column) = index.columns.get(range_pos) else {
        return default_range_rows(total_rows);
    };
    let Some(column_schema) = table
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(range_column))
    else {
        return default_range_rows(total_rows);
    };
    let low_value = low
        .as_ref()
        .and_then(|bound| bound.key.0.get(range_pos))
        .and_then(|part| concrete_key_part_value(part, &column_schema.data_type));
    let high_value = high
        .as_ref()
        .and_then(|bound| bound.key.0.get(range_pos))
        .and_then(|part| concrete_key_part_value(part, &column_schema.data_type));
    let low_pred = low_value.as_ref().map(|value| {
        (
            if low.as_ref().is_some_and(|bound| bound.inclusive) {
                BinaryOp::GtEq
            } else {
                BinaryOp::Gt
            },
            value,
        )
    });
    let high_pred = high_value.as_ref().map(|value| {
        (
            if high.as_ref().is_some_and(|bound| bound.inclusive) {
                BinaryOp::LtEq
            } else {
                BinaryOp::Lt
            },
            value,
        )
    });
    let range_rows =
        estimate_range_rows_for_column(table, range_column, low_pred, high_pred, total_rows);
    if range_pos == 0 {
        return range_rows;
    }
    let prefix_rows = estimate_index_lookup_rows(table, index, range_pos, false, total_rows);
    let range_fraction = (range_rows / total_rows.max(1.0)).clamp(0.0, 1.0);
    (prefix_rows * range_fraction).clamp(1.0, total_rows)
}

pub(super) fn estimate_in_lookup_subquery_result_rows(
    db: &Database,
    subquery: &Select,
) -> Option<f64> {
    let regular = estimate_subquery_result_rows(db, subquery)?;
    let conservative = estimate_missing_stats_keyset_rows(db, subquery)?;
    Some(regular.max(conservative))
}

pub(super) fn estimate_missing_stats_keyset_rows(db: &Database, subquery: &Select) -> Option<f64> {
    if !subquery.joins.is_empty()
        || subquery.derived.is_some()
        || !subquery.group_by.is_empty()
        || has_aggregate_select_items(&subquery.projection)
        || subquery.distinct
    {
        return None;
    }
    let tables = subquery_plan_tables(db, subquery)?;
    let [table] = tables.as_slice() else {
        return None;
    };
    let predicate = subquery.selection.as_ref()?;
    if !predicate_has_missing_eq_stats(&table.schema, predicate) {
        return None;
    }
    let mut rows = table_row_estimate(db, table).ok()?.max(1.0);
    rows = estimate_filter_rows_missing_eq_conservative(&table.schema, predicate, rows);
    if let Some(limit) = subquery.limit.as_ref().and_then(LimitValue::as_usize) {
        rows = rows.min(limit as f64);
    }
    if let Some(offset) = subquery.offset.as_ref().and_then(LimitValue::as_usize) {
        rows = (rows - offset as f64).max(1.0);
    }
    Some(rows.max(1.0))
}

pub(super) fn estimate_filter_rows_missing_eq_conservative(
    table: &TableSchema,
    expr: &Expr,
    total_rows: f64,
) -> f64 {
    match expr {
        Expr::Binary { left, op, right } if *op == BinaryOp::And => {
            let left_rows = estimate_filter_rows_missing_eq_conservative(table, left, total_rows);
            let right_rows = estimate_filter_rows_missing_eq_conservative(table, right, total_rows);
            ((left_rows / total_rows) * (right_rows / total_rows) * total_rows).max(1.0)
        }
        Expr::Binary { left, op, right } if *op == BinaryOp::Or => {
            let left_rows = estimate_filter_rows_missing_eq_conservative(table, left, total_rows);
            let right_rows = estimate_filter_rows_missing_eq_conservative(table, right, total_rows);
            (left_rows + right_rows).min(total_rows).max(1.0)
        }
        Expr::Binary { left, op, right } if *op == BinaryOp::Eq => {
            if let (Some(column), Some(value)) = (expr_column_name(left), expr_literal_value(right))
            {
                estimate_eq_rows_missing_eq_conservative(table, column, &value, total_rows)
            } else if let (Some(value), Some(column)) =
                (expr_literal_value(left), expr_column_name(right))
            {
                estimate_eq_rows_missing_eq_conservative(table, column, &value, total_rows)
            } else {
                default_filter_rows(total_rows)
            }
        }
        other => estimate_filter_rows(table, other, total_rows),
    }
}

pub(super) fn estimate_eq_rows_missing_eq_conservative(
    table: &TableSchema,
    column: &str,
    value: &Value,
    total_rows: f64,
) -> f64 {
    if table
        .stats
        .columns
        .contains_key(&normalize_expr_column(column))
    {
        return estimate_eq_rows(table, column, value, total_rows);
    }
    // Missing column stats on a keyset-producing dimension filter are
    // especially dangerous: an over-optimistic lookup_count makes a
    // large fact-table IN lookup look cheap while hiding millions of
    // random probes. Use a conservative floor only for this access-path
    // comparison; the ordinary WHERE selectivity fallback remains
    // unchanged elsewhere.
    let selectivity = if total_rows >= 10_000.0 { 0.25 } else { 0.005 };
    (total_rows * selectivity).clamp(1.0, total_rows.max(1.0))
}

pub(super) fn estimate_subquery_result_rows(db: &Database, subquery: &Select) -> Option<f64> {
    if !subquery.joins.is_empty() || subquery.derived.is_some() {
        if let Some(rows) = estimate_subquery_result_rows_from_plan(db, subquery) {
            return Some(rows);
        }
    }
    estimate_subquery_result_rows_heuristic(db, subquery)
}

pub(super) fn estimate_subquery_result_rows_from_plan(
    db: &Database,
    subquery: &Select,
) -> Option<f64> {
    SUBQUERY_ROW_ESTIMATE_DEPTH.with(|depth| {
        let current = depth.get();
        if current >= 2 {
            return None;
        }
        depth.set(current + 1);
        let result = with_hint_warnings_paused(|| {
            let mut planned_subquery = subquery.clone();
            normalize_select_predicates(db, &mut planned_subquery);
            // Subquery row estimation also benefits from decorrelation —
            // but only if the resulting AST is still plannable here. The
            // estimate is best-effort; bail out cleanly if it isn't.
            if decorrelate_enabled() {
                crate::planner_decorrelate::scalar::decorrelate_scalar_subqueries(
                    db,
                    &mut planned_subquery,
                );
                // If decorrelation produced derived joins, plan_select
                // can't plan them here — return None so the heuristic
                // path takes over.
                if planned_subquery.joins.iter().any(|j| j.derived.is_some()) {
                    return None;
                }
            }
            let plan = build_physical_plan(db, &planned_subquery).ok()?;
            Some(estimated_rows(&plan))
        });
        depth.set(current);
        result
    })
}

pub(super) fn estimate_subquery_result_rows_heuristic(
    db: &Database,
    subquery: &Select,
) -> Option<f64> {
    let tables = subquery_plan_tables(db, subquery)?;
    let mut rows = 1.0_f64;
    for table in &tables {
        rows *= table_row_estimate(db, table).ok()?;
    }
    if let Some(predicate) = &subquery.selection {
        if let Some(first) = tables.first() {
            rows = estimate_filter_rows(&first.schema, predicate, rows);
        }
    }
    if !subquery.group_by.is_empty() {
        rows = estimate_grouped_subquery_rows(&tables, &subquery.group_by, rows);
        rows = estimate_having_output_rows(subquery.having.as_ref(), rows);
    } else if has_aggregate_select_items(&subquery.projection) {
        rows = 1.0;
    }
    if subquery.distinct {
        rows *= 0.5;
    }
    if let Some(limit) = subquery.limit.as_ref().and_then(LimitValue::as_usize) {
        rows = rows.min(limit as f64);
    }
    if let Some(offset) = subquery.offset.as_ref().and_then(LimitValue::as_usize) {
        rows = (rows - offset as f64).max(0.0);
    }
    Some(rows.max(0.0))
}

pub(super) fn estimate_grouped_subquery_rows(
    tables: &[PlanTable],
    group_by: &[Expr],
    input_rows: f64,
) -> f64 {
    if group_by.is_empty() {
        return 1.0;
    }
    let mut rows = 1.0_f64;
    for expr in group_by {
        let Some(column) = expr_column_name(expr) else {
            return default_group_by_rows(input_rows);
        };
        let Some(ndv) = tables
            .iter()
            .find_map(|table| ndv_for_column(&table.schema, column))
        else {
            return default_group_by_rows(input_rows);
        };
        rows *= ndv as f64;
    }
    rows.clamp(1.0, input_rows.max(1.0))
}

/// Estimate the number of distinct equality-prefix keys a batched INLJ will
/// route. Reuses the same existing per-column/composite NDV sources as hash
/// join cardinality; no new statistics or runtime feedback are involved.
pub(super) fn estimate_index_nlj_probe_keys(
    left: &PhysicalPlan,
    right_table: &PlanTable,
    right_index: &IndexSchema,
    eq_prefix_len: usize,
    condition: Option<&Expr>,
    left_rows: f64,
) -> f64 {
    let Some(condition) = condition else {
        return left_rows.max(1.0);
    };
    let conjuncts = expr_conjuncts(condition);
    let mut outer_keys = Vec::with_capacity(eq_prefix_len);
    for pos in 0..eq_prefix_len {
        let Some(key) = conjuncts.iter().find_map(|expr| {
            equality_between_index_part_and_left_plan(expr, right_table, right_index, pos, left)
        }) else {
            return left_rows.max(1.0);
        };
        outer_keys.push(key);
    }
    let ndv = if outer_keys.len() > 1 {
        composite_ndv_in_plan(left, &outer_keys)
            .or_else(|| multi_column_ndv_in_plan(left, &outer_keys))
    } else {
        outer_keys
            .first()
            .and_then(expr_column_name)
            .and_then(|column| ndv_in_plan(left, column))
    };
    ndv.map(|value| (value as f64).min(left_rows).max(1.0))
        .unwrap_or_else(|| left_rows.max(1.0))
}

pub(super) fn estimate_index_nlj_operator_cost(
    ctx: &crate::engine::cbo::CboContext,
    left: &PhysicalPlan,
    right_table: &PlanTable,
    right_index: &IndexSchema,
    eq_prefix_len: usize,
    batchable: bool,
    join_type: JoinType,
    left_rows: f64,
    output_rows: f64,
    condition: Option<&Expr>,
    right_only_residual: Option<&Expr>,
    right_covering: bool,
) -> crate::engine::cbo::Cost {
    use crate::engine::cbo::{cbo_consts, operator_cost};

    let outer_cost = plan_cost(left);
    let right_width = operator_cost::estimated_row_width(&right_table.schema);
    let right_total_rows = right_table.schema.stats.row_count.max(1) as f64;
    let table_pages = table_heap_pages_for_cost(&right_table.schema);
    let height = index_cost_height(&right_table.schema, right_index, right_total_rows);
    let unique_exact = right_index.unique && eq_prefix_len >= right_index.columns.len();
    let per_match = estimate_index_lookup_rows(
        &right_table.schema,
        right_index,
        eq_prefix_len,
        unique_exact,
        right_total_rows,
    );
    // Same outer-NDV clamp as `estimate_index_nlj_rows`: matched-rows
    // here must agree with the operator's output-row estimate, or the
    // cost and cardinality drift apart on sampled-NDV bias.
    let per_match = clamp_index_lookup_rows_with_outer_ndv(
        per_match,
        left,
        right_table,
        right_index,
        condition,
        right_total_rows,
    );
    let per_match =
        dense_filtered_domain_rows_for_inlj(left, right_table, right_index, condition, left_rows)
            .map(|rows| (rows / left_rows.max(1.0)).max(per_match))
            .unwrap_or(per_match);
    let per_match = (per_match
        * index_nlj_dependent_range_fraction(
            left,
            right_table,
            right_index,
            eq_prefix_len,
            condition,
        ))
    .max(1.0);
    let probe_keys = estimate_index_nlj_probe_keys(
        left,
        right_table,
        right_index,
        eq_prefix_len,
        condition,
        left_rows,
    );
    let residual_qual_cost = right_only_residual
        .map(|expr| qual_cost_per_row(expr))
        .unwrap_or(0.0);
    if batchable
        && should_cost_index_nlj_as_batch(join_type, left_rows, per_match, right_total_rows)
    {
        // Anti/Semi short-circuit (executor:
        // `Database::keys_with_visible_match_with_handle`) stops
        // per-key scanning at the first visible match. The inner-side
        // work is bounded by ~1 visible entry per outer key, NOT
        // `left_rows * per_match` (the inner-join footprint). Without
        // this correction `matched_rows` over-states inner work by a
        // factor of `per_match` (~21 for TPC-H Q22 customer→orders),
        // and the batched INLJ cost runs ~20× higher than its actual
        // runtime. We keep a small fudge of `2 * left_rows` for the
        // residual-fails-then-retry case; bounded above by the
        // unsplit inner footprint so the correction can never inflate
        // cost.
        let semi_anti = matches!(join_type, JoinType::Semi | JoinType::Anti);
        let effective_per_match = if semi_anti {
            right_only_residual.map_or_else(
                // Retain the historical conservative two-entry allowance for
                // visibility/version checks on a residual-free existence
                // lookup.
                || per_match.min(2.0),
                |residual| {
                    use crate::engine::cbo::selectivity::Selectivity;
                    let total = right_total_rows.max(1.0);
                    let kept = crate::engine::cbo::selectivity::default_selectivity()
                        .predicate_rows(&right_table.schema, residual, total);
                    semi_anti_expected_inner_rows(per_match, kept / total)
                },
            )
        } else {
            per_match
        };
        let matched_rows = (left_rows * effective_per_match).max(1.0);
        let leaf_pages = operator_cost::estimated_index_leaf_pages(
            &right_table.schema,
            right_index,
            matched_rows,
        );
        // Deliberately NOT stats-backed: the batched-INLJ vs hash-join
        // balance was calibrated against this constant (see the
        // INLJ/HJ inversion fix); feeding the ANALYZE-sampled
        // correlation here flips join-order heuristics that the
        // calibration already accounts for. Single-table range-scan
        // costing uses `index_cost_correlation` instead.
        let correlation = index_cost_correlation(&right_table.schema, right_index);
        // Batched probes sort+dedup row_ids and stream the heap in
        // ascending page order, so the inner leg is priced on distinct
        // pages (`batched_index_lookup`), not per-row random reads.
        // The large-secondary penalty is deliberately NOT applied here:
        // it proxies per-row random heap I/O, which this model already
        // prices directly — applying both double-charges the same work
        // (the old `index_range_scan(corr=0.5)` + ×4 penalty stack made
        // a warm-cache FK probe look ~10× its real cost).
        // NOTE: real `table_pages`, not the bounded working set from
        // `table_pages_for_index_nlj_lookup`. That bound exists to model
        // cache residency of small *per-row* keysets; the batch model's
        // geometry (Yao distinct-page count, full-sweep cap) must use
        // the actual heap size or a 240k-row fetch gets capped at a
        // few-hundred-page "sweep" and prices as free.
        let batched_estimate = operator_cost::estimate_batched_index_lookup(
            ctx,
            operator_cost::BatchedIndexLookupInput {
                index_height: height,
                leaf_pages,
                matched_rows,
                probe_keys,
                table_pages,
                output_width: right_width,
                correlation,
                // Cumulative diagnostic pages are a stable upper-bound for
                // the outer subtree's resident working set. They come from
                // schema/statistics and the fixed CBO resource baseline, not
                // from live buffer-pool state.
                competing_pages: (outer_cost.io_seq_pages + outer_cost.io_rnd_pages).max(0.0),
            },
        );
        let mut batched_lookup = batched_estimate.cost;
        // Coverage penalty: when batched probes touch a large fraction
        // of the inner table, the "amortize to a single index_range_scan"
        // assumption breaks down — we are effectively reading the whole
        // inner side, but without the prefetch correlation of a clean
        // sequential scan, and we still pay random-access cost on the
        // heap. Floor batched_lookup at a fraction of the full-scan cost
        // proportional to coverage; the linear blend keeps the cost
        // surface smooth so small input changes don't flip plans.
        // Row coverage is a poor proxy for a secondary-index heap sweep: a
        // small fraction of rows distributed across many keys can touch most
        // heap pages (the common FK fan-out shape). Use the distinct-page
        // geometry calculated by the lookup model instead.
        let distinct_coverage =
            (batched_estimate.distinct_heap_pages / table_pages.max(1.0)).clamp(0.0, 1.0);
        // A chunked probe can revisit the same broad page population once per
        // key batch. Treat that repeated sweep pressure as the coverage signal
        // for a secondary index that is known to require heap qualification.
        // A secondary index whose key prefixes the clustered primary key can
        // serve a covering/PK-style join directly when there is no right-only
        // residual. Keep that important point/covering path on the historical
        // primary-like curve. Once a right-only predicate proves that inner
        // rows must be qualified, however, the same index pays its leaf walk,
        // heap fetch and repeated-page pressure like any other non-covering
        // secondary lookup.
        let primary_like = right_covering && right_only_residual.is_none();
        let revisit_coverage = if primary_like {
            distinct_coverage
        } else {
            (distinct_coverage * batched_estimate.probe_batches).min(1.0)
        };
        // A handful of pages can have 100% percentage coverage without being
        // a broad probe in the operational sense. Keep sparse point/batch
        // lookups on small relations out of the wide-range penalty; their
        // ordinary index/heap cost already prices all pages. The fixed floor
        // is a versioned planning baseline, not a live cache observation.
        const BROAD_HEAP_PAGE_FLOOR: f64 = 32.0;
        let broad_page_weight =
            (batched_estimate.distinct_heap_pages / BROAD_HEAP_PAGE_FLOOR).clamp(0.0, 1.0);
        let coverage = revisit_coverage * broad_page_weight;
        // The full-sweep lower bound corrects broad probes only when their
        // modeled working set exceeds the stable planning-cache baseline.
        // Secondary lookups use the stronger no-prefetch surcharge below;
        // primary-like paths use their separate, milder sweep curve.
        let logical_heap_sweeps = batched_estimate.logical_heap_page_visits / table_pages.max(1.0);
        let exceeds_planning_cache =
            batched_estimate.working_set_pages > ctx.effective_cache_pages as f64;
        let coverage_floor_weight = inlj_coverage_floor_weight(
            coverage,
            distinct_coverage,
            matched_rows,
            right_total_rows,
            batched_estimate.working_set_pages,
            ctx.effective_cache_pages as f64,
        );
        if !primary_like && coverage_floor_weight > 0.0 {
            // Pass 0.0 for qual_cost: residual CPU is added separately
            // below to avoid double-counting.
            let full_scan = operator_cost::seq_scan(
                ctx,
                table_pages,
                right_total_rows,
                right_total_rows,
                right_width,
                0.0,
            );
            // Secondary non-covering probes that touch a broad slice of
            // the heap are worse than a clean scan: they still pay the
            // index probe + row-id sort/dedup machinery, and the heap
            // access pattern lacks the prefetch locality of a sequential
            // fact scan. Treat that as a cost floor above full-scan cost
            // once coverage is high enough. Primary-like probes keep the
            // historical full-scan floor because their clustered access
            // behaves much closer to a sweep.
            // A broad non-covering probe still pays the secondary-index walk,
            // RID collection/sort and one or more no-prefetch heap passes.
            // Scale smoothly with page coverage instead of using a
            // query/table-specific cutoff.
            // Existence probes do not materialize every row in an equality
            // fanout: the executor stops each key at the first visible row
            // satisfying the residual. Their expected row/page work is
            // already reflected in `matched_rows` above, so floor them at the
            // corresponding fraction of a clean heap sweep without adding
            // the materializing secondary-lookup surcharge used by ordinary
            // INNER/LEFT joins.
            let scan_floor = if semi_anti {
                full_scan.total * logical_heap_sweeps.clamp(distinct_coverage, 1.0)
            } else {
                full_scan.total * (1.5 + coverage)
            };
            let floor = batched_lookup.total * (1.0 - coverage_floor_weight)
                + scan_floor.max(batched_lookup.total) * coverage_floor_weight;
            if floor > batched_lookup.total {
                batched_lookup.total = floor;
            }
        } else if primary_like && coverage_floor_weight > 0.0 {
            // mydb's PRIMARY index is not clustered: the leaf stores a RowId
            // and projected non-key columns still require the separate heap.
            // Consequently a broad residual-free PK join is no more covering
            // than a broad PK join with a residual. Each fixed planning chunk
            // can revisit the same heap-page population; price those visits
            // against a clean sequential scan, without the extra 1.5x
            // secondary-index surcharge. Sparse probes and working sets that
            // fit the stable planning-cache baseline retain the ordinary
            // distinct-page lookup cost instead of being charged twice.
            //
            // The blend is continuous at the broad-probe boundary and uses
            // only persisted page geometry plus the fixed planning chunk.
            let full_scan = operator_cost::seq_scan(
                ctx,
                table_pages,
                right_total_rows,
                right_total_rows,
                right_width,
                0.0,
            );
            let scan_floor = full_scan.total
                * logical_heap_sweeps.clamp(coverage, batched_estimate.probe_batches);
            let floor = batched_lookup.total * (1.0 - coverage_floor_weight)
                + scan_floor.max(batched_lookup.total) * coverage_floor_weight;
            if floor > batched_lookup.total {
                batched_lookup.total = floor;
            }
        }
        // Residual filter CPU: non-covering INL evaluates right-only
        // predicates on every matched row after the heap fetch; this is
        // O(matched_rows) work the executor has to do regardless of how
        // the lookup is batched. Hash join's scan-side filter is already
        // priced below the join (the build/probe sees fewer rows), so
        // omitting this here was the systemic INL under-pricing.
        if residual_qual_cost > 0.0 {
            let residual_cpu = matched_rows * residual_qual_cost;
            batched_lookup.total += residual_cpu;
            batched_lookup.cpu_units += residual_cpu;
        }
        // Batched-INL bookkeeping CPU. `execute_batched_inlj_noncovering`
        // runs `candidate_row_ids.sort_unstable()` + `dedup()` over
        // `matched_rows` entries so the heap can be streamed in row_id
        // order. That sort is O(matched_rows × log matched_rows) and is
        // the dominant CPU when the batched footprint is large (e.g.
        // 6M-row TPC-H lineitem PK with a 1.5M-row outer); modeling
        // the inner pass as a single `index_range_scan` skipped it
        // entirely.
        {
            let cpu_op = cbo_consts().cpu_operator_cost();
            let m = matched_rows.max(1.0);
            let row_id_sort = m * m.max(2.0).log2() * cpu_op;
            batched_lookup.total += row_id_sort;
            batched_lookup.cpu_units += row_id_sort;
        }
        let mut cost = operator_cost::batched_index_nested_loop_join(
            &outer_cost,
            &batched_lookup,
            left_rows,
            matched_rows,
            output_rows,
            left.output_width() + right_width,
        );
        // Batched INLJ splits the probe loop across outer workers, but
        // the parent operator still has to consume the outer subtree
        // once. Keep the cumulative-cost invariant (`parent >= outer`)
        // and discount only the incremental lookup/join work. This still
        // lets fact→PK probes benefit from worker parallelism without
        // making a join look cheaper than its already-built input.
        //
        // Granted only when the probe loop actually parallelises
        // cleanly: PK-shaped probes (what the discount was calibrated
        // for), or an outer that is a bare (parallel) scan whose
        // worker chunks the executor owns end-to-end. A fanout
        // *secondary* probe driven by another join's output gets no
        // divisor: its chunk-parallelism fights the upstream pipeline
        // (stage barrier + re-chunk; measured ~1.3× end-to-end on
        // TPC-H Q10, nowhere near `degree`), and granting it the full
        // divisor made scattered FK probes beat chain-streamable hash
        // joins they lose to at runtime (TPC-H Q7 customer→orders via
        // idx_orders_custkey).
        let parallel_friendly = acts_like_primary_for_cost(&right_table.schema, right_index)
            || plan_root_is_scan_or_exchange(left);
        let degree = if parallel_friendly {
            inlj_outer_parallelism_for_cost(left, left_rows, right_total_rows)
        } else {
            1
        };
        let parallel_speedup = if degree > 1 && right_only_residual.is_none() {
            // Point probes partition cleanly across workers. A broad probe is
            // different: workers repeatedly contend on and revisit the same
            // stable heap-page population, so speedup approaches one as
            // persistent page coverage approaches one. This uses only schema
            // statistics, page counts and the fixed planning chunk; no live
            // cache/RSS/load signal can affect the physical plan.
            1.0 + (degree as f64 - 1.0) * (1.0 - coverage)
        } else {
            1.0
        };
        if parallel_speedup > 1.0 {
            let inc_total = (cost.total - outer_cost.total).max(0.0);
            let inc_cpu = (cost.cpu_units - outer_cost.cpu_units).max(0.0);
            cost.total = outer_cost.total + inc_total / parallel_speedup;
            cost.cpu_units = outer_cost.cpu_units + inc_cpu / parallel_speedup;
        }
        crate::engine::cbo::trace::event(
            "batched_index_nlj_cost",
            serde_json::json!({
                "table": right_table.schema.name,
                "index": right_index.name,
                "outer_rows": left_rows,
                "probe_keys": probe_keys,
                "matched_rows": matched_rows,
                "heap_pages": table_pages,
                "distinct_heap_pages": batched_estimate.distinct_heap_pages,
                "logical_heap_page_visits": batched_estimate.logical_heap_page_visits,
                "logical_heap_sweeps": logical_heap_sweeps,
                "distinct_page_coverage": distinct_coverage,
                "revisit_page_coverage": revisit_coverage,
                "broad_page_weight": broad_page_weight,
                "page_coverage": coverage,
                "working_set_pages": batched_estimate.working_set_pages,
                "exceeds_planning_cache": exceeds_planning_cache,
                "coverage_floor_weight": coverage_floor_weight,
                "probe_batches": batched_estimate.probe_batches,
                "parallel_speedup": parallel_speedup,
                "lookup_cost": batched_lookup.total,
                "join_cost": cost.total,
            }),
        );
        return cost;
    }
    let mut per_lookup = if should_cost_index_nlj_as_small_batch(join_type, left_rows, per_match) {
        let leaf_pages = operator_cost::estimated_index_leaf_pages(
            &right_table.schema,
            right_index,
            (left_rows * per_match).max(1.0),
        );
        // Deliberately NOT stats-backed: the batched-INLJ vs hash-join
        // balance was calibrated against this constant (see the
        // INLJ/HJ inversion fix); feeding the ANALYZE-sampled
        // correlation here flips join-order heuristics that the
        // calibration already accounts for. Single-table range-scan
        // costing uses `index_cost_correlation` instead.
        let correlation = if acts_like_primary_for_cost(&right_table.schema, right_index) {
            1.0
        } else {
            0.5
        };
        let lookup_table_pages = table_pages_for_index_nlj_lookup(
            &right_table.schema,
            right_index,
            table_pages,
            left_rows,
        );
        let mut batched = operator_cost::calibrated_inlj_range_probe(
            ctx,
            height,
            leaf_pages,
            per_match,
            lookup_table_pages,
            right_width,
            correlation,
        );
        batched.total /= left_rows.max(1.0);
        batched.io_seq_pages /= left_rows.max(1.0);
        batched.io_rnd_pages /= left_rows.max(1.0);
        batched.cpu_units /= left_rows.max(1.0);
        batched
    } else {
        let effective_table_pages = table_pages_for_index_nlj_lookup(
            &right_table.schema,
            right_index,
            table_pages,
            left_rows,
        );
        operator_cost::index_lookup(ctx, height, per_match, effective_table_pages, right_width)
    };
    apply_large_secondary_index_nlj_penalty(
        &mut per_lookup,
        &right_table.schema,
        right_index,
        left_rows,
        per_match,
    );
    // Residual filter CPU per probe: each outer row evaluates the
    // right-only residual against its per_match matched rows.
    if residual_qual_cost > 0.0 {
        let probe_residual = per_match.max(0.0) * residual_qual_cost;
        per_lookup.total += probe_residual;
        per_lookup.cpu_units += probe_residual;
    }
    operator_cost::index_nested_loop_join(
        &outer_cost,
        &per_lookup,
        left_rows,
        output_rows,
        left.output_width() + right_width,
    )
}

pub(super) fn estimate_hash_join_operator_cost(
    ctx: &crate::engine::cbo::CboContext,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    join_type: JoinType,
    left_keys: &[Expr],
    right_keys: &[Expr],
    residual: Option<&Expr>,
    left_rows: f64,
    right_rows: f64,
) -> (crate::engine::cbo::Cost, BuildSide, f64) {
    use crate::engine::cbo::operator_cost;

    let build_side = choose_build_side_with_costs(
        join_type,
        left_rows,
        right_rows,
        Some(plan_cost_total(left)),
        Some(plan_cost_total(right)),
    );
    let match_est =
        estimate_equijoin_rows(left, right, left_keys, right_keys, left_rows, right_rows);
    let left_cost = plan_cost(left);
    let right_cost = plan_cost(right);
    let left_width = left.output_width();
    let right_width = right.output_width();
    let (build_cost, probe_cost, build_rows, probe_rows, build_width, probe_plan) = match build_side
    {
        BuildSide::Right => (
            &right_cost,
            &left_cost,
            right_rows,
            left_rows,
            right_width,
            left,
        ),
        BuildSide::Left => (
            &left_cost,
            &right_cost,
            left_rows,
            right_rows,
            left_width,
            right,
        ),
    };
    // For Inner/Left/Right, `match_rate` is "fraction of probe rows
    // that find at least one match", which `match_est / probe_rows`
    // approximates (FK-like joins push this past 1.0 — clamped).
    //
    // For Anti/Semi we need the SAME semantic but the `match_est /
    // probe_rows` ratio leaks the per-probe match *count* (e.g.
    // customer × 10 orders/customer → 10, clamped to 1.0). That
    // collapses anti's "no match" rate to 0 and makes anti output
    // estimate degenerate to `rows=1`, undercutting INLJ on cost.
    // For Anti/Semi, compute match_rate as inner_ndv / outer_universe
    // — the PG eqjoinsel_semi heuristic — so the formula reflects
    // "fraction of distinct outer keys present in inner".
    let match_rate = match join_type {
        JoinType::Semi | JoinType::Anti => {
            estimate_join_existence_match_rate(left, right, left_keys, right_keys)
        }
        _ if probe_rows > 0.0 => (match_est / probe_rows).clamp(0.0, 1.0),
        _ => 0.0,
    };
    // Hash-join output cardinality:
    //   * Inner/Cross ≈ match_est (inner-join matches).
    //   * Outer joins additionally emit every preserved-side row that finds
    //     no match (NULL-extended on the other side). So a LEFT join can
    //     never produce fewer than `left_rows`, a RIGHT join fewer than
    //     `right_rows`, and a FULL join fewer than `max(left_rows,
    //     right_rows)`. Treating them as `match_est` (an inner estimate)
    //     badly under-counts whenever matches are sparse — e.g. a 7194-row
    //     LEFT side with only ~36 matches must still yield ~7194 rows.
    //     `unmatched_<side>` uses the existence (semi) match-rate oriented to
    //     the preserved side; the `.max(preserved_rows)` floor keeps the
    //     estimate correct even when NDV stats are missing and the rate
    //     falls back to 1.0.
    //   * Semi emits the probe row at most once per probe with ≥1
    //     match → output ≈ probe_rows * match_rate.
    //   * Anti emits the probe row when no match exists → output =
    //     probe_rows * (1 - match_rate).
    // (Anti always probes from LEFT per choose_build_side_with_costs,
    // so probe_rows ≡ left_rows.)
    let output_est = match join_type {
        JoinType::Semi => (probe_rows * match_rate).max(1.0).min(probe_rows.max(1.0)),
        JoinType::Anti => (probe_rows * (1.0 - match_rate)).max(1.0),
        JoinType::Left => {
            let unmatched_left = left_rows
                * (1.0 - estimate_join_existence_match_rate(left, right, left_keys, right_keys));
            (match_est + unmatched_left.max(0.0)).max(left_rows)
        }
        JoinType::Right => {
            let unmatched_right = right_rows
                * (1.0 - estimate_join_existence_match_rate(right, left, right_keys, left_keys));
            (match_est + unmatched_right.max(0.0)).max(right_rows)
        }
        JoinType::Full => {
            let unmatched_left = left_rows
                * (1.0 - estimate_join_existence_match_rate(left, right, left_keys, right_keys));
            let unmatched_right = right_rows
                * (1.0 - estimate_join_existence_match_rate(right, left, right_keys, left_keys));
            (match_est + unmatched_left.max(0.0) + unmatched_right.max(0.0))
                .max(left_rows.max(right_rows))
        }
        JoinType::Inner | JoinType::Cross => match_est,
    };
    if reverse_semi_anti_hash_join_eligible(join_type, residual.is_some(), probe_rows, build_rows) {
        let key_width = left_keys
            .iter()
            .zip(right_keys)
            .map(|(left_key, right_key)| {
                let width = |plan: &PhysicalPlan, key: &Expr| {
                    expr_column_name(key)
                        .and_then(|column| average_column_width_in_plan(plan, column))
                        .unwrap_or_else(|| plan.output_width())
                };
                // Include value tags/lengths in the generic encoded key.
                8.0 + width(left, left_key).max(width(right, right_key))
            })
            .sum();
        if let Some(cost) = operator_cost::reverse_semi_anti_hash_join(
            ctx,
            build_cost,
            probe_cost,
            operator_cost::ReverseSemiAntiHashJoinInput {
                build_rows,
                probe_rows,
                probe_width: left_width,
                key_width,
                matched_build_rows: match_est,
                output_rows: output_est,
            },
        ) {
            crate::engine::cbo::trace::event(
                "reverse_semi_anti_hash_join_cost",
                serde_json::json!({
                    "probe_rows": probe_rows,
                    "build_rows": build_rows,
                    "key_width": key_width,
                    "matched_build_rows": match_est,
                    "state_bytes": cost.mem_bytes,
                    "join_cost": cost.total,
                }),
            );
            return (cost, build_side, output_est);
        }
    }
    // Keep planning aligned with both executor Bloom lanes. Builds up to the
    // static threshold get a filter immediately; larger selective INNER/SEMI
    // joins use the adaptive in-memory filter, while the vector Grace path
    // builds one before spilling the probe partitions. The adaptive sampler
    // enables at a >=50% miss rate, hence the stable selectivity gate here.
    // The Grace bitmap caps its sizing input at 8M rows and rounds to a
    // 134M-bit allocation; beyond roughly 32M inserted keys its false-positive
    // rate no longer supports the conservative 12.5% costing assumption.
    const BLOOM_COST_MAX_BUILD_ROWS: f64 = 32_000_000.0;
    let bloom_applicable = build_rows <= 1_000_000.0
        || (matches!(join_type, JoinType::Inner | JoinType::Semi)
            && build_rows <= BLOOM_COST_MAX_BUILD_ROWS
            && match_rate <= 0.5);
    let mut cost = operator_cost::hash_join(
        ctx,
        build_cost,
        probe_cost,
        build_rows,
        probe_rows,
        build_width,
        output_est,
        left_width + right_width,
        match_rate,
        bloom_applicable,
    );
    apply_hash_join_parallel_probe_cost_adjustment(
        &mut cost, probe_plan, probe_cost, build_rows, probe_rows, join_type, build_side, residual,
        match_rate,
    );
    (cost, build_side, output_est)
}

/// Probability that a probe row finds at least one matching build row,
/// for Semi/Anti hash-join cost. PG's `eqjoinsel_semi` heuristic in
/// miniature: `inner_ndv / outer_key_universe`, with the outer side's
/// *base* (unfiltered) NDV as the universe so a filtered probe doesn't
/// shrink the denominator and inflate match_rate.
///
/// Missing statistics use the public default join selectivity rather than
/// assuming every probe matches.
pub(crate) fn estimate_join_existence_match_rate(
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    left_keys: &[Expr],
    right_keys: &[Expr],
) -> f64 {
    let (lk, rk) = match (left_keys.first(), right_keys.first()) {
        (Some(lk), Some(rk)) => (lk, rk),
        _ => return crate::engine::cbo::cbo_consts().default_join_selectivity(),
    };
    let inner_ndv = composite_ndv_in_plan(right, right_keys)
        .or_else(|| expr_column_name(rk).and_then(|c| ndv_in_plan(right, c)))
        // `ndv_in_plan` deliberately preserves base-column statistics through
        // filters.  That is useful for ordinary equijoin fanout, but a
        // semi/anti existence estimate needs the number of distinct keys that
        // can actually reach the join.  A filtered input cannot contain more
        // distinct keys than rows, even when its base column is unique.
        // Without this bound, a selective filtered unique-key build looks as
        // though it still covers the entire outer key universe, collapsing an
        // anti join to one estimated row and starving downstream operators of
        // both cost and memory.
        .map(|ndv| (ndv as f64).min(estimated_rows(right).max(0.0)));
    let outer_universe = composite_ndv_in_plan(left, left_keys)
        .or_else(|| expr_column_name(lk).and_then(|c| ndv_in_plan(left, c)));
    match (inner_ndv, outer_universe) {
        (Some(inner), Some(outer)) => (inner / (outer.max(1) as f64)).clamp(0.0, 1.0),
        _ => crate::engine::cbo::cbo_consts().default_join_selectivity(),
    }
}

pub(crate) fn estimate_equijoin_rows(
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    left_keys: &[Expr],
    right_keys: &[Expr],
    left_rows: f64,
    right_rows: f64,
) -> f64 {
    if left_rows <= 0.0 || right_rows <= 0.0 {
        return 0.0;
    }
    if left_keys.len() != right_keys.len() || left_keys.is_empty() {
        return left_rows
            * right_rows
            * crate::engine::cbo::cbo_consts().default_join_selectivity();
    }

    // Predicate propagation deliberately emits transitive equalities because
    // they are useful join-order edges.  Cardinality estimation must collapse
    // the pairs proven equivalent by the already-built inputs *before* asking
    // for a composite NDV.  Doing it only in the single-column fallback loses
    // an otherwise exact `(a,b)` index statistic when the extracted key list
    // is `(a,b,equivalent_a)`.
    let mut effective_left_keys = Vec::with_capacity(left_keys.len());
    let mut effective_right_keys = Vec::with_capacity(right_keys.len());
    for (left_key, right_key) in left_keys.iter().zip(right_keys) {
        let redundant = effective_left_keys.iter().zip(&effective_right_keys).any(
            |(prior_left, prior_right)| {
                exprs_equivalent_in_plan(left, left_key, prior_left)
                    && exprs_equivalent_in_plan(right, right_key, prior_right)
            },
        );
        if !redundant {
            effective_left_keys.push(left_key.clone());
            effective_right_keys.push(right_key.clone());
        }
    }
    let left_keys = effective_left_keys.as_slice();
    let right_keys = effective_right_keys.as_slice();

    let dense_domain_rows = if left_keys.len() == 1 && right_keys.len() == 1 {
        estimate_dense_filtered_domain_join_rows(
            left,
            right,
            &left_keys[0],
            &right_keys[0],
            left_rows,
            right_rows,
        )
        .or_else(|| {
            estimate_dense_filtered_domain_join_rows(
                right,
                left,
                &right_keys[0],
                &left_keys[0],
                right_rows,
                left_rows,
            )
        })
    } else {
        None
    };
    if left_keys.len() == 1 && right_keys.len() == 1 {
        if keyset_prefilter_guarantees_unique_join(left, right, &left_keys[0], &right_keys[0]) {
            return left_rows.clamp(0.0, left_rows * right_rows);
        }
        if keyset_prefilter_guarantees_unique_join(right, left, &right_keys[0], &left_keys[0]) {
            return right_rows.clamp(0.0, left_rows * right_rows);
        }
    }
    // Preserve joint statistics even when only one side has a covering
    // index/group. Discarding the known tuple NDV and multiplying individual
    // selectivities can invent a key universe much larger than either table.
    if left_keys.len() > 1 {
        let l_joint = composite_ndv_in_plan(left, left_keys)
            .or_else(|| multi_column_ndv_in_plan(left, left_keys));
        let r_joint = composite_ndv_in_plan(right, right_keys)
            .or_else(|| multi_column_ndv_in_plan(right, right_keys));
        if l_joint.is_some() || r_joint.is_some() {
            let l_joint = l_joint.or_else(|| uncollected_joint_ndv_in_plan(left, left_keys));
            let r_joint = r_joint.or_else(|| uncollected_joint_ndv_in_plan(right, right_keys));
            if let (Some(ln), Some(rn)) = (l_joint, r_joint) {
                let denom = ln.max(rn) as f64;
                let raw = left_rows * right_rows / denom.max(1.0);
                return raw.clamp(0.0, left_rows * right_rows);
            }
        }
    }
    let mut selectivity = 1.0_f64;
    let mut has_stats = false;
    for (lk, rk) in left_keys.iter().zip(right_keys.iter()) {
        let l_ndv = equijoin_ndv_in_plan(left, lk);
        let r_ndv = equijoin_ndv_in_plan(right, rk);
        if let (Some(ln), Some(rn)) = (l_ndv, r_ndv) {
            // FK-style join: |L ⋈ R| ≈ left_rows * right_rows / max(ndv).
            // Using `max` instead of `min` keeps the estimate sane when one
            // side has been pre-filtered (lower row count) but its NDV
            // statistic still reflects the unfiltered base column.
            let denom = ln.max(rn);
            selectivity *= 1.0 / denom.max(1.0);
            has_stats = true;
        }
    }
    if has_stats {
        // When no joint statistic exists, use ordinary independence. Joint
        // NDV above is the only generic evidence that permits correlation
        // adjustment; benchmark-shaped damping and arbitrary output caps are
        // intentionally absent.
        let raw = left_rows * right_rows * selectivity;
        return raw
            .max(dense_domain_rows.unwrap_or(0.0))
            .clamp(0.0, left_rows * right_rows);
    }
    // No stats anywhere — use the documented and tunable fallback.
    dense_domain_rows
        .unwrap_or_else(|| {
            left_rows * right_rows * crate::engine::cbo::cbo_consts().default_join_selectivity()
        })
        .clamp(0.0, left_rows * right_rows)
}

pub(super) fn estimate_dense_filtered_domain_join_rows(
    domain_plan: &PhysicalPlan,
    other_plan: &PhysicalPlan,
    domain_key: &Expr,
    other_key: &Expr,
    _domain_rows: f64,
    other_rows: f64,
) -> Option<f64> {
    const MIN_DENSE_DOMAIN_DENSITY: f64 = 0.50;
    let domain = filtered_key_domain_in_plan(domain_plan, domain_key)?;
    if domain.density < MIN_DENSE_DOMAIN_DENSITY {
        return None;
    }
    let other_col = expr_column_name(other_key)?;
    let raw_other_ndv = ndv_in_plan(other_plan, other_col)? as f64;
    if raw_other_ndv <= 0.0 {
        return None;
    }
    let (other_min, other_max) = key_numeric_min_max_in_plan(other_plan, other_col)?;
    // The non-filtered side's row spread is divided across `other_ndv`
    // distinct keys; an over-sampled NDV (the KMV sketch routinely places a
    // dense surrogate-key FK like `ss_sold_date_sk` several × above its true
    // distinct count) shrinks the per-key group and deflates the estimated
    // join cardinality, which under-prices index-driven access paths over
    // the (faster) vectorized seqscan + hash join. An integer column cannot
    // have more distinct values than its value-range span, so cap the NDV
    // there — a hard, always-valid bound that only ever corrects an
    // over-estimate.
    let other_value_span = (other_max.floor() - other_min.ceil() + 1.0).floor();
    let other_ndv = if other_value_span.is_finite() && other_value_span >= 1.0 {
        raw_other_ndv.min(other_value_span)
    } else {
        raw_other_ndv
    };
    let domain_span = (domain.max - domain.min + 1.0).max(1.0);
    let overlap_low = domain.min.max(other_min);
    let overlap_high = domain.max.min(other_max);
    if overlap_high < overlap_low {
        return Some(0.0);
    }
    let overlap_span = (overlap_high - overlap_low + 1.0).clamp(1.0, domain_span);
    let selected_keys =
        (domain.key_distinct * (overlap_span / domain_span)).clamp(1.0, other_ndv.max(1.0));
    Some((other_rows * (selected_keys / other_ndv)).clamp(0.0, other_rows.max(0.0)))
}

pub(super) fn estimate_index_nlj_rows(
    left: &PhysicalPlan,
    right_table: &PlanTable,
    right_index: &IndexSchema,
    eq_prefix_len: usize,
    left_rows: f64,
    join_type: JoinType,
    condition: Option<&Expr>,
) -> f64 {
    if matches!(join_type, JoinType::Semi | JoinType::Anti) {
        let match_rate =
            estimate_semi_anti_match_rate(left, right_table, right_index, eq_prefix_len, condition);
        let kept_rate = if matches!(join_type, JoinType::Anti) {
            (1.0 - match_rate).max(0.0)
        } else {
            match_rate
        };
        return (left_rows * kept_rate).max(1.0);
    }
    let right_total_rows = right_table.schema.stats.row_count.max(1) as f64;
    let rows_per_lookup = estimate_index_lookup_rows(
        &right_table.schema,
        right_index,
        eq_prefix_len,
        right_index.unique && eq_prefix_len >= right_index.columns.len(),
        right_total_rows,
    );
    let rows_per_lookup = clamp_index_lookup_rows_with_outer_ndv(
        rows_per_lookup,
        left,
        right_table,
        right_index,
        condition,
        right_total_rows,
    );
    let rows_per_lookup =
        dense_filtered_domain_rows_for_inlj(left, right_table, right_index, condition, left_rows)
            .map(|rows| (rows / left_rows.max(1.0)).max(rows_per_lookup))
            .unwrap_or(rows_per_lookup);
    let rows_per_lookup = rows_per_lookup
        * index_nlj_dependent_range_fraction(
            left,
            right_table,
            right_index,
            eq_prefix_len,
            condition,
        );
    let matched = (left_rows * rows_per_lookup).max(1.0);
    // A LEFT join emits every outer (preserved) row at least once, even
    // when the inner index lookup is selective enough that
    // `rows_per_lookup < 1`. Floor the estimate at `left_rows` so the
    // outer-join output never collapses below its preserved side — mirrors
    // the hash-join path. Inner joins keep the raw matched estimate.
    if matches!(join_type, JoinType::Left) {
        matched.max(left_rows)
    } else {
        matched
    }
}

/// PG-style semi/anti selectivity for the INLJ shape. Estimates the
/// fraction of outer rows whose join key has at least one matching
/// inner row.
///
/// Formula: `inner_ndv / outer_key_universe`, clamped to `[0, 1]`.
///   * `inner_ndv`: distinct keys on the indexed inner side (from the
///     index's prefix cardinality, falling back to the inner column's
///     stats, then to a conservative `row_count / 16` floor).
///   * `outer_key_universe`: distinct values the outer side can take
///     on. We use `ndv_in_plan` on the outer column — which returns
///     the *base table* NDV (filter-independent), the right proxy for
///     "how big the outer key space is", not "how big after filter".
///
/// Falls back to 1.0 (the original 100%-pass assumption) when we
/// can't identify the outer column from the condition — keeps Anti
/// from emitting an absurdly small estimate when stats are missing.
pub(super) fn estimate_semi_anti_match_rate(
    left: &PhysicalPlan,
    right_table: &PlanTable,
    right_index: &IndexSchema,
    eq_prefix_len: usize,
    condition: Option<&Expr>,
) -> f64 {
    let inner_col = match right_index.columns.first() {
        Some(c) => c.as_str(),
        None => return crate::engine::cbo::cbo_consts().default_join_selectivity(),
    };
    let outer_col = condition.and_then(|cond| {
        expr_conjuncts(cond).into_iter().find_map(|conj| {
            equality_between_table_column_and_left_plan(conj, right_table, inner_col, left)
                .as_ref()
                .and_then(expr_column_name)
                .map(str::to_string)
        })
    });
    let Some(outer_col) = outer_col else {
        return crate::engine::cbo::cbo_consts().default_join_selectivity();
    };
    let inner_ndv = right_table
        .schema
        .index_prefix_cardinality(&right_index.name, eq_prefix_len)
        .or_else(|| {
            if eq_prefix_len <= 1 {
                ndv_for_column(&right_table.schema, inner_col)
            } else {
                None
            }
        });
    let outer_keys: Option<Vec<Expr>> = condition.and_then(|condition| {
        right_index
            .columns
            .iter()
            .take(eq_prefix_len)
            .map(|column| {
                expr_conjuncts(condition).into_iter().find_map(|conj| {
                    equality_between_table_column_and_left_plan(conj, right_table, column, left)
                })
            })
            .collect()
    });
    let outer_universe = outer_keys
        .as_ref()
        .filter(|keys| keys.len() > 1)
        .and_then(|keys| composite_ndv_in_plan(left, keys))
        .or_else(|| {
            if eq_prefix_len <= 1 {
                ndv_in_plan(left, outer_col.as_str())
            } else {
                None
            }
        })
        .map(|v| v as f64);
    match (inner_ndv, outer_universe) {
        (Some(inner), Some(outer)) => (inner as f64 / outer.max(1.0)).clamp(0.0, 1.0),
        _ => crate::engine::cbo::cbo_consts().default_join_selectivity(),
    }
}
