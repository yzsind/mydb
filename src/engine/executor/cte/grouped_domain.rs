//! Restrict a single grouped producer by required optional-join key domains.
//! The original outer joins and predicates still determine rows and values.
use super::*;
use crate::engine::executor::select::vec_bridge;
use crate::parser::visit;

pub(in crate::engine::executor) struct Domain {
    pub producer: usize,
    pub builds: Vec<usize>,
    predicates: Vec<Expr>,
    key_names: Vec<Vec<String>>,
    source_keys: Vec<Expr>,
}

fn plain(select: &Select) -> bool {
    select.derived.is_none()
        && select.values.is_none()
        && select.json_table.is_none()
        && select.rls_security_context.is_none()
        && select.rls_expression_settings.is_none()
        && !select.for_update
        && select.lock_clause.is_none()
        && !select.sql_calc_found_rows
        && !select.straight_join
        && !select.no_merge
        && select.query_hints.is_empty()
        && select.named_windows.is_empty()
        && select
            .from
            .iter()
            .chain(select.joins.iter().map(|j| &j.table))
            .all(|t| {
                t.system_time.is_none()
                    && t.sample.is_none()
                    && t.generate_series.is_none()
                    && t.column_aliases.is_empty()
                    && t.index_hints.is_empty()
                    && t.partitions.is_empty()
            })
        && select
            .joins
            .iter()
            .all(|j| j.derived.is_none() && !j.lateral && !j.natural && j.using_columns.is_empty())
}

fn output_names(cte: &CteDef) -> Option<Vec<String>> {
    let query = cte.query.as_select()?;
    let names = if cte.columns.is_empty() {
        query
            .projection
            .iter()
            .map(|p| match p {
                SelectItem::Expr { expr, alias } => alias
                    .clone()
                    .or_else(|| expr_output_name(expr))
                    .map(|n| n.to_ascii_lowercase()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?
    } else {
        cte.columns.iter().map(|n| n.to_ascii_lowercase()).collect()
    };
    if names.len() != query.projection.len()
        || names.iter().collect::<HashSet<_>>().len() != names.len()
    {
        return None;
    }
    Some(names)
}

fn resolve(expr: &Expr, aliases: &[String], names: &[Vec<String>]) -> Option<(usize, String)> {
    let (qualifier, name) = match expr {
        Expr::Identifier(n) => (None, n.to_ascii_lowercase()),
        Expr::CompoundIdentifier(p) if p.len() == 2 => {
            (Some(p[0].to_ascii_lowercase()), p[1].to_ascii_lowercase())
        }
        _ => return None,
    };
    let mut matches = names.iter().enumerate().filter(|(i, cols)| {
        qualifier.as_ref().is_none_or(|a| a == &aliases[*i]) && cols.contains(&name)
    });
    let (index, _) = matches.next()?;
    matches.next().is_none().then_some((index, name))
}

fn split_or<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::Binary {
        left,
        op: BinaryOp::Or,
        right,
    } = expr
    {
        split_or(left, out);
        split_or(right, out);
    } else {
        out.push(expr);
    }
}

pub(in crate::engine::executor) fn prove(
    ctes: &[CteDef],
    body: &Statement,
    keys: &[String],
    dependencies: &[HashSet<String>],
) -> Option<Domain> {
    if crate::util::env::var("MYDB_GROUPED_CTE_DOMAIN")
        .is_ok_and(|s| matches!(s.as_str(), "0" | "off" | "false"))
    {
        return None;
    }
    let Statement::Select(consumer) = body else {
        return None;
    };
    if !plain(consumer)
        || consumer.from.len() != 1
        || !(2..=4).contains(&consumer.joins.len())
        || select_has_nondeterministic(consumer)
        || ctes
            .iter()
            .any(|c| !statement_is_deterministic_for_cte_cache(&c.query))
    {
        return None;
    }
    let sources = consumer
        .from
        .iter()
        .chain(consumer.joins.iter().map(|j| &j.table))
        .collect::<Vec<_>>();
    let indices = sources
        .iter()
        .map(|t| {
            ctes.iter()
                .position(|c| c.name.eq_ignore_ascii_case(&t.name))
        })
        .collect::<Option<Vec<_>>>()?;
    if indices.iter().collect::<HashSet<_>>().len() != indices.len() {
        return None;
    }
    let producer = indices[0];
    if !dependencies[producer].is_empty()
        || indices[1..]
            .iter()
            .any(|&i| dependencies[i].contains(&keys[producer]))
    {
        return None;
    }
    let counts = compute_cte_consumer_counts(ctes, body, keys);
    if counts.get(&keys[producer]) != Some(&1) {
        return None;
    }
    let base = &ctes[producer];
    let select = base.query.as_select()?;
    if base.is_recursive
        || !plain(select)
        || select.distinct
        || select.group_by.is_empty()
        || !select.grouping_sets.is_empty()
        || select.group_by_modifier != crate::parser::GroupByModifier::None
        || select.having.is_some()
        || select.limit.is_some()
        || select.offset.is_some()
        || !select.order_by.is_empty()
        || !statement_is_deterministic_for_cte_cache(&base.query)
    {
        return None;
    }
    // Do not move evaluation of arbitrary functions, subqueries, or windows.
    // The retained keys are direct GROUP BY inputs; aggregate arguments are
    // direct numeric columns, checked against the bound input below.
    for item in &select.projection {
        let SelectItem::Expr { expr, .. } = item else {
            return None;
        };
        match expr {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {}
            Expr::Function {
                name,
                args,
                distinct: false,
            } if ["sum", "count", "min", "max", "avg"]
                .iter()
                .any(|n| name.eq_ignore_ascii_case(n))
                && args.iter().all(|e| {
                    matches!(
                        e,
                        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Wildcard
                    )
                }) => {}
            _ => return None,
        }
    }
    if select
        .selection
        .iter()
        .chain(select.joins.iter().filter_map(|j| j.condition.as_ref()))
        .any(|e| !vec_bridge::join_domain::safe_predicate(e))
    {
        return None;
    }
    let aliases = sources
        .iter()
        .map(|t| table_alias_name(t).to_ascii_lowercase())
        .collect::<Vec<_>>();
    if aliases.iter().collect::<HashSet<_>>().len() != aliases.len() {
        return None;
    }
    let names = indices
        .iter()
        .map(|&i| output_names(&ctes[i]))
        .collect::<Option<Vec<_>>>()?;
    let mut mappings = Vec::new();
    for (i, join) in consumer.joins.iter().enumerate() {
        if join.join_type != crate::parser::JoinType::Left {
            return None;
        }
        let mut conjuncts = Vec::new();
        split_and(join.condition.as_ref()?, &mut conjuncts);
        let mut map = std::collections::BTreeMap::new();
        for term in conjuncts {
            let Expr::Binary {
                left,
                op: BinaryOp::Eq,
                right,
            } = term
            else {
                return None;
            };
            let (a, b) = (
                resolve(&left, &aliases, &names)?,
                resolve(&right, &aliases, &names)?,
            );
            let (base, other) = if a.0 == 0 && b.0 == i + 1 {
                (a, b)
            } else if b.0 == 0 && a.0 == i + 1 {
                (b, a)
            } else {
                return None;
            };
            if map.insert(base.1, other.1).is_some() {
                return None;
            }
        }
        if map.len() < 2 {
            return None;
        }
        mappings.push(map);
    }
    let left_names = mappings[0].keys().cloned().collect::<Vec<_>>();
    if mappings
        .iter()
        .any(|m| m.keys().cloned().collect::<Vec<_>>() != left_names)
    {
        return None;
    }
    let source_keys = left_names
        .iter()
        .map(|n| {
            let position = names[0].iter().position(|x| x == n)?;
            let SelectItem::Expr { expr, .. } = &select.projection[position] else {
                return None;
            };
            (matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                && select.group_by.contains(expr))
            .then(|| expr.clone())
        })
        .collect::<Option<Vec<_>>>()?;
    let mut conjuncts = Vec::new();
    split_and(consumer.selection.as_ref()?, &mut conjuncts);
    for predicate in conjuncts {
        let mut terms = Vec::new();
        split_or(&predicate, &mut terms);
        if terms.len() != consumer.joins.len() {
            continue;
        }
        let mut predicates = vec![None; consumer.joins.len()];
        for term in terms {
            if !vec_bridge::join_domain::safe_predicate(term) {
                return None;
            }
            let mut branch = None;
            let mut rewritten = term.clone();
            let valid = visit::try_rewrite_value_expr(&mut rewritten, |node| {
                if matches!(node, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                    let (index, name) = resolve(node, &aliases, &names).ok_or(())?;
                    if index == 0 || branch.is_some_and(|b| b != index) {
                        return Err(());
                    }
                    branch = Some(index);
                    *node = Expr::Identifier(name);
                    return Ok(false);
                }
                Ok(true)
            })
            .is_ok();
            if !valid {
                return None;
            }
            let slot = branch?.checked_sub(1)?;
            if predicates[slot].replace(rewritten).is_some() {
                return None;
            }
        }
        return Some(Domain {
            producer,
            builds: indices[1..].to_vec(),
            predicates: predicates.into_iter().collect::<Option<Vec<_>>>()?,
            key_names: mappings
                .iter()
                .map(|m| left_names.iter().map(|n| m[n].clone()).collect())
                .collect(),
            source_keys,
        });
    }
    None
}

impl Domain {
    pub(in crate::engine::executor) fn filter(
        &self,
        db: &Arc<Database>,
        session: &SessionContext,
        ctes: &[CteDef],
    ) -> Result<Option<crate::planner::RuntimeFilter>, QueryError> {
        let source = ctes[self.producer]
            .query
            .as_select()
            .expect("proven select");
        let numeric = |expr: &Expr| {
            visit::try_inspect_value_expr(expr, |node| {
                let (qualifier, name) = match node {
                    Expr::Identifier(n) => (None, n.as_str()),
                    Expr::CompoundIdentifier(p) if p.len() == 2 => {
                        (Some(p[0].as_str()), p[1].as_str())
                    }
                    Expr::CompoundIdentifier(_) => return Err(()),
                    _ => return Ok(true),
                };
                let (_, table) = resolve_column_ref_table(
                    db,
                    &session.current_database,
                    source,
                    qualifier,
                    name,
                )
                .ok_or(())?;
                use crate::storage::DataTypeKind as T;
                if !matches!(
                    table.columns[table.column_index(name).map_err(|_| ())?].data_type,
                    T::TinyInt
                        | T::SmallInt
                        | T::MediumInt
                        | T::Int
                        | T::BigInt
                        | T::Decimal { .. }
                        | T::Float { .. }
                        | T::Double { .. }
                ) {
                    return Err(());
                }
                Ok(true)
            })
            .is_ok()
        };
        if source
            .projection
            .iter()
            .any(|p| matches!(p,SelectItem::Expr {expr,..} if !numeric(expr)))
            || source
                .group_by
                .iter()
                .chain(source.selection.iter())
                .chain(source.joins.iter().filter_map(|j| j.condition.as_ref()))
                .any(|e| !numeric(e))
        {
            return Ok(None);
        }
        let mut query = ctes[self.producer].query.clone();
        fold_query_block_constants(&mut query);
        super::super::qualification::qualify_statement_tables(db, session, &mut query)?;
        let Some(plan) = crate::planner::plan_query_physical(db, &query)? else {
            return Ok(None);
        };
        let Some((positions, target_keys)) =
            vec_bridge::group_input_filter::producer_keys(&plan, &self.source_keys)
        else {
            return Ok(None);
        };
        let plans = self
            .builds
            .iter()
            .map(|&i| {
                crate::planner::try_plan_cte_scan(&placeholder_table_ref(ctes[i].name.clone()))
            })
            .collect::<Option<Vec<_>>>();
        let Some(plans) = plans else {
            return Ok(None);
        };
        let names = self
            .key_names
            .iter()
            .map(|names| positions.iter().map(|&p| names[p].clone()).collect())
            .collect::<Vec<_>>();
        vec_bridge::join_domain::grouped_producer_domain(
            &plans,
            &self.predicates,
            &names,
            target_keys,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::StorageQueryEngine;
    const QUERY:&str="WITH l AS (SELECT x,y,SUM(v) s FROM gd_f GROUP BY x,y), a AS (SELECT x ax,y ay,SUM(v) av FROM gd_a GROUP BY x,y), b AS (SELECT x bx,y by_,SUM(v) bv FROM gd_b GROUP BY x,y) SELECT x,y,s,av,bv FROM l LEFT JOIN a ON x=ax AND y=ay LEFT JOIN b ON by_=y AND bx=x WHERE COALESCE(av,0)>0 OR COALESCE(bv,0)>0 ORDER BY x,y";
    fn rows(
        engine: &StorageQueryEngine,
        session: &mut SessionContext,
        sql: &str,
        mode: &str,
    ) -> crate::engine::ResultSet {
        crate::util::env::with_vars(&[("MYDB_GROUPED_CTE_DOMAIN", Some(mode))], || {
            let QueryResponse::ResultSet(result) = engine
                .execute_with_session(session, sql)
                .unwrap()
                .materialize()
                .unwrap()
            else {
                panic!("result");
            };
            result
        })
    }
    #[test]
    fn grouped_optional_domain_preserves_sums_nulls_and_statement_scope() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        for sql in ["CREATE TABLE gd_f(x BIGINT UNSIGNED,y BIGINT,v DECIMAL(20,2))","CREATE TABLE gd_a(x BIGINT UNSIGNED,y BIGINT,v DECIMAL(20,2))","CREATE TABLE gd_b(x BIGINT UNSIGNED,y BIGINT,v DECIMAL(20,2))",
            "INSERT INTO gd_f VALUES(1,1,1.25),(1,1,2.50),(2,2,9),(3,3,8),(4,4,NULL),(5,NULL,7),(NULL,1,6),(18446744073709551615,1,12)",
            "INSERT INTO gd_a VALUES(1,1,1),(1,1,2),(2,2,0),(3,3,-1),(NULL,1,2),(5,NULL,3),(18446744073709551615,1,1)",
            "INSERT INTO gd_b VALUES(1,1,-1),(4,4,1),(4,4,2),(3,3,0)",
            "CREATE TABLE gd_r(rx BIGINT UNSIGNED,ry BIGINT)",
            "INSERT INTO gd_r VALUES(1,1),(1,1),(NULL,1)"] {engine.execute(sql).unwrap();}
        let mut session = SessionContext::default();
        let before = rows(&engine, &mut session, QUERY, "off");
        let hits = vec_bridge::group_input_filter::HITS.load(std::sync::atomic::Ordering::Relaxed);
        let after = rows(&engine, &mut session, QUERY, "force");
        assert_eq!(before.rows, after.rows);
        assert_eq!(before.columns, after.columns);
        assert_eq!(after.rows.len(), 3);
        assert!(
            vec_bridge::group_input_filter::HITS.load(std::sync::atomic::Ordering::Relaxed) > hits,
            "filter must execute"
        );
        let hits = vec_bridge::group_input_filter::HITS.load(std::sync::atomic::Ordering::Relaxed);
        let analyzed = rows(
            &engine,
            &mut session,
            &format!("EXPLAIN ANALYZE FORMAT=TREE {QUERY}"),
            "force",
        );
        assert!(
            vec_bridge::group_input_filter::HITS.load(std::sync::atomic::Ordering::Relaxed) > hits,
            "analyze must apply the same domain"
        );
        let tree = format!("{:?}", analyzed.rows);
        assert!(tree.contains("actual time="), "{tree}");
        for query in [
            QUERY.replace(" WHERE COALESCE(av,0)>0 OR COALESCE(bv,0)>0", ""),
            QUERY.replace("COALESCE(av,0)>0", "COALESCE(av,1)>0"),
            QUERY.replace("x=ax", "x<=>ax"),
            QUERY.replace("x=ax", "x=ax AND av<>999"),
            QUERY.replace("ORDER BY x,y", "AND x IN(SELECT x FROM l) ORDER BY x,y"),
        ] {
            let before = rows(&engine, &mut session, &query, "off");
            let hits =
                vec_bridge::group_input_filter::HITS.load(std::sync::atomic::Ordering::Relaxed);
            let after = rows(&engine, &mut session, &query, "force");
            assert_eq!(before.rows, after.rows, "{query}");
            assert_eq!(before.columns, after.columns);
            assert_eq!(
                vec_bridge::group_input_filter::HITS.load(std::sync::atomic::Ordering::Relaxed),
                hits,
                "unsafe shape must decline: {query}"
            );
        }
        let anti=QUERY.replace("SELECT x,y,SUM(v) s FROM gd_f GROUP BY x,y", "SELECT f.x,f.y,SUM(f.v) s FROM gd_f f LEFT JOIN gd_r r ON f.x=r.rx AND f.y=r.ry WHERE r.rx IS NULL GROUP BY f.x,f.y");
        let before = rows(&engine, &mut session, &anti, "off");
        let hits = vec_bridge::group_input_filter::HITS.load(std::sync::atomic::Ordering::Relaxed);
        let after = rows(&engine, &mut session, &anti, "force");
        assert_eq!(before.rows, after.rows);
        assert_eq!(after.rows.len(), 2);
        assert!(
            vec_bridge::group_input_filter::HITS.load(std::sync::atomic::Ordering::Relaxed) > hits
        );
        // Repeating an unrestricted consumer in the same session must see all
        // producer groups, including those omitted by the previous domain.
        let unrestricted = QUERY.replace(" WHERE COALESCE(av,0)>0 OR COALESCE(bv,0)>0", "");
        assert_eq!(
            rows(&engine, &mut session, &unrestricted, "force")
                .rows
                .len(),
            7
        );
    }
}
