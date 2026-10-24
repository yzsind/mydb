//! Sideways information propagation.

use super::*;

pub(crate) fn propagate_sideways_dimension_filters(db: &Database, select: &mut Select) {
    // Resolve structural eligibility before consulting any tuning knobs.
    // Single-table statements are the dominant OLTP shape and can never
    // participate in sideways propagation; reading five absent environment
    // variables first serialized concurrent planners on the process-wide
    // environment lock for work that was guaranteed to return `None`.
    let tables = match collect_sip_tables(db, select) {
        Some(tables) if tables.len() >= 2 => tables,
        _ => return,
    };
    if crate::util::env::var("MYDB_SIP_DISABLE").ok().as_deref() == Some("1") {
        return;
    }
    let max_hops: usize = crate::util::env::var("MYDB_SIP_MAX_HOPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    if max_hops < 2 {
        return;
    }
    // Cap the SOURCE dimension's row count — pulling a filter out of a
    // big table through a synthesized IN-subquery costs more than it
    // saves (we'd be scanning the dim table once via the subquery on top
    // of the existing inner-join). Default 10_000 fits nation / region /
    // small lookup tables cleanly while excluding fact tables.
    let max_source_rows: u64 = crate::util::env::var("MYDB_SIP_MAX_SOURCE_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    // Date and calendar dimensions are often tens of thousands of rows but a
    // selective source predicate still makes the one-time IN-subquery cheap.
    // Keep the old 10k default for non-selective sources; allow a larger cap
    // only when the source filter is equality / IN / LIKE / IS NULL shaped.
    let max_selective_source_rows: u64 =
        crate::util::env::var("MYDB_SIP_MAX_SELECTIVE_SOURCE_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100_000);
    // Cap each intermediate (transit) table on the path the same way:
    // a path that has to scan a fact table inside the subquery is the
    // worst kind of "fan-out + late filter" — it negates SIP's win.
    let max_transit_rows: u64 = crate::util::env::var("MYDB_SIP_MAX_TRANSIT_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    // A propagated membership test is useful only when the source predicate
    // materially shrinks its dimension.  Equality syntax alone is not enough:
    // low-NDV columns can match every row (for example a constant timezone
    // column), leaving a redundant per-fact-row Apply in front of the join.
    // Use the same catalog estimator as normal scan planning so this gate is
    // stable and benefits every star-schema query shape.
    let max_source_selectivity: f64 = crate::util::env::var("MYDB_SIP_MAX_SOURCE_SELECTIVITY")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|value: &f64| value.is_finite() && *value > 0.0 && *value <= 1.0)
        .unwrap_or(0.75);

    let col_to_alias = build_col_to_alias(&tables);
    let row_count_of = |alias: &str| -> u64 {
        tables
            .iter()
            .find(|t| t.alias == alias)
            .map(|t| t.row_count)
            .unwrap_or(u64::MAX)
    };
    let source_row_limit = |filters: &[Expr]| -> u64 {
        if filters.iter().any(sip_predicate_is_selective) {
            max_source_rows.max(max_selective_source_rows)
        } else {
            max_source_rows
        }
    };
    let source_is_selective = |alias: &str, filters: &[Expr]| -> bool {
        let Some(table) = tables.iter().find(|table| table.alias == alias) else {
            return false;
        };
        let Some(filter) = combine_conjuncts(filters.to_vec()) else {
            return false;
        };
        let total_rows = table.row_count.max(1) as f64;
        estimate_filter_rows(&table.schema, &filter, total_rows) / total_rows
            <= max_source_selectivity
    };

    let mut where_conjuncts: Vec<Expr> = Vec::new();
    if let Some(e) = &select.selection {
        split_conjuncts(e, &mut where_conjuncts);
    }
    let mut join_conjuncts: Vec<Expr> = Vec::new();
    for join in &select.joins {
        if !matches!(join.join_type, JoinType::Inner | JoinType::Cross) {
            continue;
        }
        if let Some(c) = &join.condition {
            split_conjuncts(c, &mut join_conjuncts);
        }
    }

    let mut edges: Vec<(String, String, String, String)> = Vec::new();
    let mut const_filters: HashMap<String, Vec<Expr>> = HashMap::new();

    for c in where_conjuncts.iter().chain(join_conjuncts.iter()) {
        let aliases = sip_collect_aliases_in_expr(c, &tables, &col_to_alias);
        if aliases.is_empty() {
            continue;
        }
        if aliases.len() == 1 {
            let alias = aliases.iter().next().unwrap().clone();
            if !sip_expr_has_subquery(c) {
                // Skip pure col=col self-equalities (e.g. `a.x = a.x`).
                let is_self_eq = matches!(
                    c,
                    Expr::Binary {
                        op: BinaryOp::Eq,
                        ..
                    }
                ) && matches!(c, Expr::Binary { left, right, .. } if {
                    sip_resolve_column(left, &col_to_alias).is_some()
                        && sip_resolve_column(right, &col_to_alias).is_some()
                });
                if !is_self_eq {
                    const_filters.entry(alias).or_default().push(c.clone());
                }
            }
        } else if aliases.len() == 2 {
            if let Expr::Binary {
                left,
                right,
                op: BinaryOp::Eq,
            } = c
            {
                if let (Some((a1, c1)), Some((a2, c2))) = (
                    sip_resolve_column(left, &col_to_alias),
                    sip_resolve_column(right, &col_to_alias),
                ) {
                    if a1 != a2 {
                        edges.push((a1, c1, a2, c2));
                    }
                }
            }
        }
    }

    if const_filters.is_empty() || edges.is_empty() {
        return;
    }

    // ---- Strong-fact-filter guard ----
    // The CBO can be misled by a synthetic SIP predicate when a much
    // stronger const filter already exists on a fact table elsewhere in
    // the query. Example: TPC-H Q8 carries `part.p_type='...'` (~1/200
    // selectivity, 1451 / 200000 rows). SIP would propagate
    // `customer.c_nationkey IN (region.r_name='AMERICA')` (~5/25 =
    // 20%). The CBO then sees `customer` as the cheaper build side
    // and reorders the plan around it — but the resulting INLJ over
    // `lineitem` ends up touching 8× more rows than the original
    // part-driven plan. To stay out of the CBO's way in those queries
    // we detect "strong fact-table filters" and disable SIP for the
    // whole select.
    //
    // Heuristic: a `const_filter` qualifies as a "strong fact filter"
    // when it sits on a large table (row_count > max_source_rows) AND
    // includes at least one equality / IN-list / LIKE predicate — the
    // shapes that yield small selectivities in practice. Range
    // filters on big tables (orders.o_orderdate BETWEEN ...) don't
    // count because their selectivity is typically 10–50 %, which
    // doesn't dominate SIP's contribution to the plan.
    let has_strong_fact_filter = const_filters.iter().any(|(alias, filters)| {
        let row_count = row_count_of(alias);
        if row_count <= source_row_limit(filters) {
            return false;
        }
        filters.iter().any(sip_predicate_is_selective)
    });
    if has_strong_fact_filter {
        return;
    }

    let mut graph: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
    for (a1, c1, a2, c2) in &edges {
        graph
            .entry(a1.clone())
            .or_default()
            .push((a2.clone(), c1.clone(), c2.clone()));
        graph
            .entry(a2.clone())
            .or_default()
            .push((a1.clone(), c2.clone(), c1.clone()));
    }

    // Existing `target.col IN (...)` predicates the user (or an earlier
    // pass) already added — don't duplicate.
    let mut already_in_lookup: HashSet<(String, String)> = HashSet::new();
    for c in &where_conjuncts {
        if let Expr::InSubquery {
            expr,
            negated: false,
            ..
        } = c
        {
            if let Some((a, col)) = sip_resolve_column(expr, &col_to_alias) {
                already_in_lookup.insert((a, col));
            }
        }
        if let Expr::InList {
            expr,
            negated: false,
            ..
        } = c
        {
            if let Some((a, col)) = sip_resolve_column(expr, &col_to_alias) {
                already_in_lookup.insert((a, col));
            }
        }
    }

    let mut added: HashSet<(String, String)> = HashSet::new();
    let mut new_predicates: Vec<Expr> = Vec::new();

    let source_aliases: Vec<String> = {
        let mut v: Vec<String> = const_filters
            .keys()
            .filter(|a| {
                let filters = const_filters.get(*a).map(Vec::as_slice).unwrap_or(&[]);
                row_count_of(a) <= source_row_limit(filters) && source_is_selective(a, filters)
            })
            .cloned()
            .collect();
        v.sort();
        v
    };

    for source_alias in &source_aliases {
        let mut visited: HashSet<String> = HashSet::new();
        visited.insert(source_alias.clone());
        let mut queue: VecDeque<(String, Vec<SipStep>)> = VecDeque::new();
        queue.push_back((source_alias.clone(), Vec::new()));

        while let Some((cur, path)) = queue.pop_front() {
            if path.len() >= max_hops {
                continue;
            }
            let mut neighbors = graph.get(&cur).cloned().unwrap_or_default();
            // Deterministic order keeps plan-cache fingerprints stable.
            neighbors.sort();
            for (neighbor, my_col, their_col) in neighbors {
                if visited.contains(&neighbor) {
                    continue;
                }
                visited.insert(neighbor.clone());
                let mut new_path = path.clone();
                new_path.push(SipStep {
                    from_alias: cur.clone(),
                    from_col: my_col.clone(),
                    to_alias: neighbor.clone(),
                    to_col: their_col.clone(),
                });

                if !const_filters.contains_key(&neighbor) {
                    let target_alias = neighbor.clone();
                    let target_col = new_path.last().unwrap().to_col.clone();
                    let key = (target_alias.clone(), target_col.clone());
                    let transit_too_big = new_path
                        .iter()
                        .take(new_path.len() - 1)
                        .any(|s| row_count_of(&s.to_alias) > max_transit_rows);
                    // SIP only pays off when the target is a *larger* fact
                    // table the small filtered source can shrink. When the
                    // target is no larger than the source, the synthesized
                    // IN-subquery shrinks nothing and only perturbs the plan:
                    // on a direct join it duplicates the join key as a
                    // redundant `IndexInLookup` (defeating the batched INLJ),
                    // and when the "source" is actually the bigger (filtered
                    // fact) table it pushes the filter *backwards* onto the
                    // small PK dimension, flipping the INLJ drive direction to
                    // the worse orientation. The CBO already has full
                    // visibility of an equal-or-smaller join, so leave it
                    // alone. (Real star schemas always satisfy fact > dim, so
                    // this never suppresses a beneficial push.)
                    let target_not_larger =
                        row_count_of(&target_alias) <= row_count_of(source_alias);
                    if !transit_too_big
                        && !target_not_larger
                        && !added.contains(&key)
                        && !already_in_lookup.contains(&key)
                    {
                        if let Some(subq) =
                            build_sip_subquery(&new_path, source_alias, &tables, &const_filters)
                        {
                            new_predicates.push(Expr::InSubquery {
                                expr: Box::new(Expr::CompoundIdentifier(vec![
                                    target_alias,
                                    target_col,
                                ])),
                                subquery: Box::new(subq),
                                negated: false,
                            });
                            added.insert(key);
                        }
                    }
                }

                queue.push_back((neighbor, new_path));
            }
        }
    }

    if new_predicates.is_empty() {
        return;
    }

    let mut combined = where_conjuncts;
    combined.extend(new_predicates);
    select.selection = combine_conjuncts(combined);
}

/// Drop any `db.` prefix the qualification pass put on a table name —
/// the alias mydb's column resolver matches against in WHERE qualifiers
/// is the unqualified form. e.g. `"tpch.customer"` → `"customer"`.
pub(super) fn sip_short_table_name(qualified: &str) -> String {
    qualified
        .rsplit('.')
        .next()
        .unwrap_or(qualified)
        .to_string()
}

pub(super) fn collect_sip_tables(db: &Database, select: &Select) -> Option<Vec<SipTable>> {
    if select.from.is_empty() {
        return None;
    }
    let mut out = Vec::new();
    let mut seen_aliases = HashSet::new();

    for table in &select.from {
        let sip_table = sip_table_from_ref(db, table)?;
        if !seen_aliases.insert(sip_table.alias.clone()) {
            return None;
        }
        out.push(sip_table);
    }

    for join in &select.joins {
        if !matches!(join.join_type, JoinType::Inner | JoinType::Cross) {
            return None;
        }
        if join.derived.is_some() || join.lateral {
            return None;
        }
        if crate::engine::executor::cte::has_cte(&join.table.name) {
            return None;
        }
        let sip_table = sip_table_from_ref(db, &join.table)?;
        if !seen_aliases.insert(sip_table.alias.clone()) {
            return None;
        }
        out.push(sip_table);
    }

    Some(out)
}

pub(super) fn sip_table_from_ref(db: &Database, table: &TableRef) -> Option<SipTable> {
    if crate::engine::executor::cte::has_cte(&table.name) {
        return None;
    }
    let alias = table
        .alias
        .clone()
        .unwrap_or_else(|| sip_short_table_name(&table.name))
        .to_ascii_lowercase();
    let schema = db.table(&table.name).ok()?;
    let columns: HashSet<String> = schema
        .iter_visible_columns()
        .map(|(_, c)| c.name.to_ascii_lowercase())
        .collect();
    Some(SipTable {
        alias,
        table_name: table.name.clone(),
        columns,
        row_count: schema.stats.row_count,
        schema,
    })
}

pub(super) fn sip_collect_aliases_in_expr(
    expr: &Expr,
    tables: &[SipTable],
    col_to_alias: &HashMap<String, String>,
) -> HashSet<String> {
    let mut out = HashSet::new();
    sip_walk_collect_aliases(expr, tables, col_to_alias, &mut out);
    out
}

pub(super) fn sip_walk_collect_aliases(
    expr: &Expr,
    tables: &[SipTable],
    col_to_alias: &HashMap<String, String>,
    out: &mut HashSet<String>,
) {
    crate::parser::visit::inspect_value_expr(
        expr,
        |expr| {
            match expr {
                Expr::Identifier(name) => {
                    let n = name.to_ascii_lowercase();
                    if let Some(alias) = col_to_alias.get(&n) {
                        out.insert(alias.clone());
                    }
                }
                Expr::CompoundIdentifier(parts) => {
                    if parts.len() >= 2 {
                        let qual = parts[parts.len() - 2].to_ascii_lowercase();
                        if tables.iter().any(|t| t.alias == qual) {
                            out.insert(qual);
                        }
                    } else if let Some(name) = parts.last() {
                        let n = name.to_ascii_lowercase();
                        if let Some(alias) = col_to_alias.get(&n) {
                            out.insert(alias.clone());
                        }
                    }
                }
                Expr::Binary { .. }
                | Expr::Unary { .. }
                | Expr::IsNull(_)
                | Expr::IsNotNull(_)
                | Expr::InList { .. }
                | Expr::Between { .. }
                | Expr::Like { .. }
                | Expr::Function { .. }
                | Expr::Case { .. }
                | Expr::Tuple(_) => return true,
                _ => {}
            }
            false
        },
        |_| {},
    );
}

pub(super) fn sip_resolve_column(
    expr: &Expr,
    col_to_alias: &HashMap<String, String>,
) -> Option<(String, String)> {
    match expr {
        Expr::Identifier(name) => {
            let n = name.to_ascii_lowercase();
            col_to_alias.get(&n).map(|a| (a.clone(), n))
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let alias = parts[parts.len() - 2].to_ascii_lowercase();
            let col = parts.last().unwrap().to_ascii_lowercase();
            Some((alias, col))
        }
        _ => None,
    }
}

/// "Is this single-table conjunct a high-selectivity predicate that
/// could dominate the join order if the CBO sees it?"
///
/// We count:
///   * column `=` literal / placeholder (incl. IS NULL / IS NOT NULL)
///   * column `IN (...)`
///   * column `LIKE 'pattern'` (treated as selective — even with leading
///     `%` it's at worst a column-wide scan, which is what we'd hit
///     anyway, but `LIKE 'X%'` tends to be quite selective)
///
/// We skip range predicates (`<`, `<=`, `>`, `>=`, `BETWEEN`) because
/// they typically span 10–50 % of the table — not strong enough to
/// dominate a plan and not strong enough to be misled by SIP either.
pub(super) fn sip_predicate_is_selective(expr: &Expr) -> bool {
    match expr {
        Expr::Binary {
            op: BinaryOp::Eq, ..
        } => true,
        Expr::InList { negated: false, .. } => true,
        Expr::Like { negated: false, .. } => true,
        Expr::IsNull(_) | Expr::IsNotNull(_) => true,
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => sip_predicate_is_selective(left) || sip_predicate_is_selective(right),
        _ => false,
    }
}

pub(super) fn sip_expr_has_subquery(expr: &Expr) -> bool {
    crate::parser::visit::any_expr_matching(
        expr,
        |node| {
            matches!(
                node,
                Expr::Subquery(_)
                    | Expr::InSubquery { .. }
                    | Expr::Exists { .. }
                    | Expr::QuantifiedComparison { .. }
            )
        },
        |node| crate::parser::visit::scalar_expr_children(node) || matches!(node, Expr::Tuple(_)),
    )
}

/// Build the IN-subquery as a single-level SELECT with INNER JOIN chain
/// from `source` through the intermediate hops, projecting the column
/// that connects to the target. e.g. for path `region -> nation -> customer`:
///   SELECT nation.n_nationkey
///   FROM   region INNER JOIN nation ON nation.n_regionkey = region.r_regionkey
///   WHERE  region.r_name = 'ASIA'
/// Single-level form matches what users hand-write and what mydb's
/// IN-subquery HashSet / IndexInLookup machinery handles cleanly.
pub(super) fn build_sip_subquery(
    path: &[SipStep],
    source_alias: &str,
    tables: &[SipTable],
    const_filters: &HashMap<String, Vec<Expr>>,
) -> Option<Select> {
    let source_filters = const_filters.get(source_alias)?;
    if source_filters.is_empty() {
        return None;
    }
    if path.is_empty() {
        return None;
    }

    let lookup_table = |alias: &str| -> Option<String> {
        tables
            .iter()
            .find(|t| t.alias == alias)
            .map(|t| t.table_name.clone())
    };

    let source_name = lookup_table(source_alias)?;
    // The last hop projects the column that the OUTER fact-table predicate
    // will compare against. `last_step.from_alias` is the table just BEFORE
    // the target; `last_step.from_col` is the column on that table that
    // equals `target.to_col`.
    let last_step = path.last().unwrap();

    let mut subq = sip_empty_select();
    subq.from = vec![TableRef {
        name: source_name,
        alias: Some(source_alias.to_string()),
        system_time: None,
        index_hints: Vec::new(),
        partitions: Vec::new(),
        column_aliases: Vec::new(),
        generate_series: None,
    }];

    // INNER JOIN each intermediate hop (steps 0 .. len-2) — the last hop
    // would join the TARGET, which we don't want inside the subquery.
    for i in 0..path.len().saturating_sub(1) {
        let step = &path[i];
        let next_alias = path[i + 1].from_alias.clone();
        let next_table = lookup_table(&next_alias)?;
        subq.joins.push(crate::parser::Join {
            table: TableRef {
                name: next_table,
                alias: Some(next_alias.clone()),
                system_time: None,
                index_hints: Vec::new(),
                partitions: Vec::new(),
                column_aliases: Vec::new(),
                generate_series: None,
            },
            join_type: JoinType::Inner,
            condition: Some(Expr::Binary {
                left: Box::new(Expr::CompoundIdentifier(vec![
                    step.from_alias.clone(),
                    step.from_col.clone(),
                ])),
                op: BinaryOp::Eq,
                right: Box::new(Expr::CompoundIdentifier(vec![
                    next_alias,
                    step.to_col.clone(),
                ])),
            }),
            using_columns: Vec::new(),
            natural: false,
            derived: None,
            straight: false,
            lateral: false,
        });
    }

    subq.projection = vec![SelectItem::Expr {
        expr: Expr::CompoundIdentifier(vec![
            last_step.from_alias.clone(),
            last_step.from_col.clone(),
        ]),
        alias: None,
    }];
    subq.selection = combine_conjuncts(source_filters.clone());
    // `Select.table` is the canonical "primary FROM table name" that
    // `execute_select` checks to decide whether the statement is a
    // FROM-less const select vs. a real table query. `qualify_select_tables`
    // sets it from `from[0]` for parsed statements; SIP-synthesized
    // subqueries skip that pass entirely, so we mirror the wiring here.
    subq.table = subq.from.first().map(|t| t.name.clone());

    Some(subq)
}

pub(super) fn sip_empty_select() -> Select {
    Select {
        ..Select::default()
    }
}
