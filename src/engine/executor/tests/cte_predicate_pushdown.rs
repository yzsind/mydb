//! WS-A / WS-B regression coverage: pushing group-key equality constants
//! *through* aggregating CTE bodies, propagated across the outer query's
//! join-equality equivalence classes (TPC-DS Q78 shape).
//!
//! Two layers of coverage:
//!  * AST-level: parse a `WITH`, run the CTE predicate-pushdown passes, and
//!    assert exactly which CTE bodies gained the constant/domain (positive
//!    cases) or stayed untouched (ROLLUP / HAVING / aggregate-output /
//!    unconstrained shared-body must never be over-filtered).
//!  * Execution-level: a Q78-shaped query over tiny tables, run with the gate
//!    on and off, must return byte-identical rows — the rewrite is a pure
//!    plan-shape change.
use super::*;
use crate::parser::{CteDef, Statement};

/// Parse a `WITH …` and run the WS-A/B rewrite with the gate forced on/off.
/// Returns the (possibly rewritten) CTE definitions for inspection.
fn rewrite_ctes(sql: &str, gate_on: bool) -> Vec<CteDef> {
    let stmt = crate::parser::parse_statement(sql).expect("parse WITH");
    let Statement::WithCte(w) = stmt else {
        panic!("expected a WITH statement");
    };
    let mut ctes = w.ctes;
    let body = *w.body;
    let val = if gate_on { Some("on") } else { Some("off") };
    with_test_env(&[("MYDB_PRED_PUSHDOWN_AGG", val)], || {
        crate::engine::executor::cte::push_reference_domain_predicates_into_ctes(&mut ctes, &body);
        crate::engine::executor::cte::push_groupkey_predicates_into_ctes(&mut ctes, &body);
    });
    ctes
}

/// Debug-render a named CTE body's WHERE clause (empty string when absent).
fn body_where(ctes: &[CteDef], name: &str) -> String {
    let cte = ctes
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| panic!("no CTE named {name}"));
    let crate::parser::QueryExpr::Select(s) = &cte.query else {
        return String::new();
    };
    format!("{:?}", s.selection)
}

/// Run the *full* `execute_with_cte` rewrite sequence (inline → prune →
/// push-common → push-groupkey) so we observe whether single-use grouped CTEs
/// survive inlining and end up with the constant pushed into their bodies.
fn full_pipeline(sql: &str, gate_on: bool) -> (Vec<CteDef>, Statement) {
    let stmt = crate::parser::parse_statement(sql).expect("parse WITH");
    let Statement::WithCte(w) = stmt else {
        panic!("expected a WITH statement");
    };
    let mut ctes = w.ctes;
    let mut body = *w.body;
    let val = if gate_on { Some("on") } else { Some("off") };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    with_test_env(&[("MYDB_PRED_PUSHDOWN_AGG", val)], || {
        crate::engine::executor::cte::apply_pre_materialization_rewrites(
            engine.database(),
            "test",
            &mut ctes,
            &mut body,
            true,
        );
    });
    (ctes, body)
}

/// The real Q78 shape: three single-use grouped CTEs (`fact LEFT JOIN returns`
/// anti-join + `JOIN date_dim` + GROUP BY), `ss LEFT JOIN ws LEFT JOIN cs`,
/// outer `ss_sold_year = 2002`. After the full pipeline the CTEs must survive
/// (not inlined away) and each body must gain `d_year = 2002`.
#[test]
fn q78_full_pipeline_pushes_year_into_all_three_cte_bodies() {
    let sql = "with \
      ws as (select d_year AS ws_sold_year, ws_item_sk, ws_bill_customer_sk ws_customer_sk, \
               sum(ws_quantity) ws_qty \
             from web_sales \
             left join web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk \
             join date_dim on ws_sold_date_sk = d_date_sk \
             where wr_order_number is null \
             group by d_year, ws_item_sk, ws_bill_customer_sk), \
      cs as (select d_year AS cs_sold_year, cs_item_sk, cs_bill_customer_sk cs_customer_sk, \
               sum(cs_quantity) cs_qty \
             from catalog_sales \
             left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk \
             join date_dim on cs_sold_date_sk = d_date_sk \
             where cr_order_number is null \
             group by d_year, cs_item_sk, cs_bill_customer_sk), \
      ss as (select d_year AS ss_sold_year, ss_item_sk, ss_customer_sk, \
               sum(ss_quantity) ss_qty \
             from store_sales \
             left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk \
             join date_dim on ss_sold_date_sk = d_date_sk \
             where sr_ticket_number is null \
             group by d_year, ss_item_sk, ss_customer_sk) \
      select ss_sold_year, ss_item_sk, ss_customer_sk, ss_qty \
      from ss \
      left join ws on (ws_sold_year=ss_sold_year and ws_item_sk=ss_item_sk and ws_customer_sk=ss_customer_sk) \
      left join cs on (cs_sold_year=ss_sold_year and cs_item_sk=ss_item_sk and cs_customer_sk=ss_customer_sk) \
      where (coalesce(ws_qty,0)>0 or coalesce(cs_qty,0)>0) and ss_sold_year=2002";
    let (_ctes, body) = full_pipeline(sql, true);
    // Single-use grouped CTEs are inlined as derived tables; the pushed
    // `d_year = 2002` rides along into each derived subquery's WHERE.
    let Statement::Select(s) = &body else {
        panic!("expected select body");
    };
    let mut derived_wheres: Vec<String> = Vec::new();
    let render = |sub: &crate::parser::SubQuery| -> String {
        match sub.as_select() {
            Some(sel) => format!("{:?}", sel.selection),
            None => String::new(),
        }
    };
    if let Some(d) = &s.derived {
        derived_wheres.push(render(d));
    }
    for j in &s.joins {
        if let Some(d) = &j.derived {
            derived_wheres.push(render(d));
        }
    }
    assert_eq!(
        derived_wheres.len(),
        3,
        "all three CTEs should inline into derived tables, got {derived_wheres:?}"
    );
    for (i, w) in derived_wheres.iter().enumerate() {
        assert!(
            w.contains("d_year") && w.contains("2002"),
            "derived table #{i} should carry d_year=2002, got: {w}"
        );
    }
}

/// A group-key equality on the CTE's GROUP BY output column is rewritten to the
/// underlying key expression and pushed into the body's WHERE.
#[test]
fn groupkey_constant_lands_in_grouped_cte_body() {
    let sql = "with ss as (\
                 select d_year as ss_year, k, sum(v) sv \
                 from t group by d_year, k) \
               select ss_year, k, sv from ss where ss_year = 2002";
    let pushed = rewrite_ctes(sql, true);
    let where_on = body_where(&pushed, "ss");
    // The body gained `d_year = 2002` (the group-key source expr, not the
    // output alias). The original body had no WHERE.
    assert!(
        where_on.contains("d_year") && where_on.contains("2002"),
        "expected d_year = 2002 pushed into ss body, got: {where_on}"
    );

    // Gate off: body untouched.
    let untouched = rewrite_ctes(sql, false);
    assert_eq!(body_where(&untouched, "ss"), "None");
}

/// The join equality `ws_year = ss_year` carries the outer `ss_year = 2002`
/// constant into the (LEFT-joined, null-supplying) `ws` body as well — pushing
/// into the independently materialised body is always sound.
#[test]
fn equiclass_propagates_constant_to_joined_ctes() {
    let sql = "with \
                 ss as (select d_year as ss_year, item, sum(q) ssq \
                        from store group by d_year, item), \
                 ws as (select d_year as ws_year, item, sum(q) wsq \
                        from web group by d_year, item) \
               select ss_year, ss.item, ssq, wsq \
               from ss left join ws on ws_year = ss_year and ws.item = ss.item \
               where ss_year = 2002";
    let pushed = rewrite_ctes(sql, true);
    let ss = body_where(&pushed, "ss");
    let ws = body_where(&pushed, "ws");
    assert!(
        ss.contains("d_year") && ss.contains("2002"),
        "ss body should gain d_year=2002, got: {ss}"
    );
    assert!(
        ws.contains("d_year") && ws.contains("2002"),
        "ws body should gain d_year=2002 via equi-class, got: {ws}"
    );
}

/// A ROLLUP body must NOT be filtered before aggregation: the constant would
/// wrongly drop the synthetic-NULL super-group.
#[test]
fn rollup_body_is_not_pushed() {
    let sql = "with ss as (\
                 select d_year as ss_year, k, sum(v) sv \
                 from t group by d_year, k with rollup) \
               select ss_year, k, sv from ss where ss_year = 2002";
    let pushed = rewrite_ctes(sql, true);
    assert_eq!(
        body_where(&pushed, "ss"),
        "None",
        "ROLLUP body must not be over-filtered"
    );
}

/// A HAVING body is rejected (the filter could interact with post-aggregate
/// semantics).
#[test]
fn having_body_is_not_pushed() {
    let sql = "with ss as (\
                 select d_year as ss_year, k, sum(v) sv \
                 from t group by d_year, k having sum(v) > 0) \
               select ss_year, k, sv from ss where ss_year = 2002";
    let pushed = rewrite_ctes(sql, true);
    assert_eq!(body_where(&pushed, "ss"), "None");
}

/// A filter on an *aggregate* output column can never be pushed through the
/// aggregate.
#[test]
fn aggregate_output_column_is_not_pushed() {
    let sql = "with ss as (\
                 select d_year as ss_year, k, sum(v) sv \
                 from t group by d_year, k) \
               select ss_year, k, sv from ss where sv = 100";
    let pushed = rewrite_ctes(sql, true);
    assert_eq!(
        body_where(&pushed, "ss"),
        "None",
        "filter on aggregate output must not be pushed"
    );
}

/// A CTE referenced more than once (self-join with a different constant per
/// alias) is shared materialisation. Pushing either alias's singleton would
/// corrupt the other use, but pushing the union of all constrained alias
/// domains is safe: the outer alias-local predicates still enforce the exact
/// side-specific value.
#[test]
fn shared_cte_alias_domains_union_into_body() {
    let sql = "with yt as (\
                 select d_year as yr, cust, sum(v) sv \
                 from t group by d_year, cust) \
               select a.cust \
               from yt a join yt b on a.cust = b.cust \
               where a.yr = 2001 and b.yr = 2001 + 1";
    let pushed = rewrite_ctes(sql, true);
    let where_on = body_where(&pushed, "yt");
    assert!(
        where_on.contains("d_year") && where_on.contains("2001") && where_on.contains("2002"),
        "shared CTE should gain d_year IN (2001, 2002), got: {where_on}"
    );

    let untouched = rewrite_ctes(sql, false);
    assert_eq!(body_where(&untouched, "yt"), "None");
}

/// If any direct reference lacks a domain for a candidate output column, the
/// shared body cannot be narrowed to the constrained aliases' union: the
/// unconstrained alias may still need rows outside that domain.
#[test]
fn shared_cte_unconstrained_alias_is_not_filtered() {
    let sql = "with yt as (\
                 select d_year as yr, cust, sum(v) sv \
                 from t group by d_year, cust) \
               select a.cust \
               from yt a join yt b on a.cust = b.cust \
               where a.yr = 2001";
    let pushed = rewrite_ctes(sql, true);
    assert_eq!(
        body_where(&pushed, "yt"),
        "None",
        "shared CTE with an unconstrained alias must not be narrowed"
    );
}

/// Q39 shape: the shared CTE is a plain SELECT over a derived aggregate. The
/// alias-domain union first lands on the CTE body's `d_moy` output, then the
/// regular derived-table pushdown can carry it through the derived aggregate
/// because `d_moy` is a GROUP BY key, while aggregate outputs remain above.
#[test]
fn shared_cte_domain_pushes_through_derived_group_key() {
    let sql = "with inv as (\
                 select d_moy, item, mean \
                 from (select d_moy, item, avg(q) mean \
                       from inventory group by d_moy, item) foo \
                 where mean > 0) \
               select a.item \
               from inv a join inv b on a.item = b.item \
               where a.d_moy = 2 and b.d_moy = 2 + 1";
    let mut pushed = rewrite_ctes(sql, true);
    let cte = pushed
        .iter_mut()
        .find(|c| c.name.eq_ignore_ascii_case("inv"))
        .expect("inv CTE");
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    crate::engine::executor::cte::pushdown_inline_derived_predicates(
        engine.database(),
        &mut cte.query,
    );
    let crate::parser::QueryExpr::Select(outer) = &mut cte.query else {
        panic!("expected inv select");
    };

    assert!(
        outer.selection.is_none(),
        "both predicates should move inside the producer"
    );
    let inner = outer
        .derived
        .as_ref()
        .and_then(crate::parser::SubQuery::as_select)
        .expect("derived aggregate");
    let inner_where = format!("{:?}", inner.selection);
    assert!(
        inner_where.contains("d_moy") && inner_where.contains("2") && inner_where.contains("3"),
        "derived aggregate should receive d_moy IN (2, 3), got: {inner_where}"
    );
    let inner_having = format!("{:?}", inner.having);
    assert!(
        inner_having.contains("avg") && inner_having.contains("q"),
        "aggregate-output filter belongs in HAVING: {inner_having}"
    );
}

#[test]
fn catalog_backed_union_pushdown_uses_the_session_database() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE DATABASE analytics").unwrap();
    engine
        .execute(
            "CREATE TABLE analytics.date_dim (d_date_sk INT PRIMARY KEY, d_date DATE, \
             KEY idx_date (d_date))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE analytics.store_dim (store_sk INT PRIMARY KEY, store_id VARCHAR(16))",
        )
        .unwrap();
    for table in ["fact_sales", "fact_returns"] {
        engine
            .execute(&format!(
                "CREATE TABLE analytics.{table} (id INT PRIMARY KEY, date_sk INT, store_sk INT, \
                 amount DECIMAL(12,2), KEY idx_date (date_sk))"
            ))
            .unwrap();
    }

    let mut statement = crate::parser::parse_statement(
        "SELECT store_id, SUM(amount) \
         FROM (SELECT store_sk, date_sk, amount FROM fact_sales \
               UNION ALL \
               SELECT store_sk, date_sk, amount FROM fact_returns) salesreturns, \
              date_dim, store_dim \
         WHERE salesreturns.date_sk = date_dim.d_date_sk \
           AND date_dim.d_date BETWEEN CAST('2020-01-05' AS DATE) \
                                   AND CAST('2020-01-06' AS DATE) \
           AND salesreturns.store_sk = store_dim.store_sk \
         GROUP BY store_id",
    )
    .unwrap();
    let mut session = SessionContext::default();
    session.current_database = "analytics".into();
    crate::engine::executor::qualification::qualify_statement_tables(
        engine.database(),
        &session,
        &mut statement,
    )
    .unwrap();
    crate::engine::executor::cte::pushdown_inline_derived_predicates(
        engine.database(),
        &mut statement,
    );

    let Statement::Select(select) = statement else {
        panic!("expected SELECT");
    };
    let Some(crate::parser::SubQuery::SetOp(set_statement)) = select.derived else {
        panic!("expected UNION ALL derived source");
    };
    let set = set_statement.as_ref();
    let crate::parser::QueryExpr::Select(left) = &set.left else {
        panic!("expected left SELECT");
    };
    let crate::parser::QueryExpr::Select(right) = &set.right else {
        panic!("expected right SELECT");
    };
    for (branch, name) in [(left, "fact_sales"), (right, "fact_returns")] {
        let selection = format!("{:?}", branch.selection);
        assert!(
            selection.contains("InSubquery") && selection.contains("analytics.date_dim"),
            "{name} should receive the filtered date domain from analytics.date_dim: {selection}"
        );
    }
}

/// Keep the exact Q39 comma-join / CASE-projection shape pinned: both aliases
/// constrain `d_moy`, so the materialised `inv` body should get the union.
#[test]
fn q39_comma_join_shape_pushes_month_domain() {
    let sql = "with inv as (\
                 select w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, \
                        stdev, mean, case mean when 0 then null else stdev/mean end cov \
                 from (select w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, \
                              stddev_samp(inv_quantity_on_hand) stdev, \
                              avg(inv_quantity_on_hand) mean \
                       from inventory, item, warehouse, date_dim \
                       where inv_item_sk = i_item_sk \
                         and inv_warehouse_sk = w_warehouse_sk \
                         and inv_date_sk = d_date_sk \
                         and d_year = 2000 \
                       group by w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) foo \
                 where case mean when 0 then 0 else stdev/mean end > 1) \
               select inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov, \
                      inv2.w_warehouse_sk, inv2.i_item_sk, inv2.d_moy, inv2.mean, inv2.cov \
               from inv inv1, inv inv2 \
               where inv1.i_item_sk = inv2.i_item_sk \
                 and inv1.w_warehouse_sk = inv2.w_warehouse_sk \
                 and inv1.d_moy = 2 \
                 and inv2.d_moy = 2 + 1";
    let pushed = rewrite_ctes(sql, true);
    let where_on = body_where(&pushed, "inv");
    assert!(
        where_on.contains("d_moy") && where_on.contains("2") && where_on.contains("3"),
        "Q39-shaped CTE should gain d_moy IN (2, 3), got: {where_on}"
    );
}

fn collect_branch_wheres<'q>(stmt: impl Into<crate::parser::QueryRef<'q>>, out: &mut Vec<String>) {
    let stmt = stmt.into();
    match stmt {
        crate::parser::QueryRef::Select(s) => out.push(format!("{:?}", s.selection)),
        crate::parser::QueryRef::SetOperation(set) => {
            collect_branch_wheres(&set.left, out);
            collect_branch_wheres(&set.right, out);
        }
        _ => {}
    }
}

/// Q4 shape: one shared, self-joined `UNION ALL` aggregate CTE. Each outer
/// alias constrains `dyear`, so the shared body can be narrowed to the union
/// of those alias domains and that predicate must be distributed into every
/// UNION branch's grouped body.
#[test]
fn q4_union_all_shared_cte_pushes_year_domain_into_each_branch() {
    let sql = "with year_total as (\
                 select d_year dyear, cust, sum(v) year_total, 's' sale_type \
                 from store_sales group by d_year, cust \
                 union all \
                 select d_year dyear, cust, sum(v) year_total, 'c' sale_type \
                 from catalog_sales group by d_year, cust \
                 union all \
                 select d_year dyear, cust, sum(v) year_total, 'w' sale_type \
                 from web_sales group by d_year, cust) \
               select s2.cust \
               from year_total s1, year_total s2, year_total c1, \
                    year_total c2, year_total w1, year_total w2 \
               where s2.cust = s1.cust \
                 and s1.cust = c1.cust \
                 and s1.cust = c2.cust \
                 and s1.cust = w1.cust \
                 and s1.cust = w2.cust \
                 and s1.sale_type = 's' and s2.sale_type = 's' \
                 and c1.sale_type = 'c' and c2.sale_type = 'c' \
                 and w1.sale_type = 'w' and w2.sale_type = 'w' \
                 and s1.dyear = 1999 and s2.dyear = 1999 + 1 \
                 and c1.dyear = 1999 and c2.dyear = 1999 + 1 \
                 and w1.dyear = 1999 and w2.dyear = 1999 + 1";
    let pushed = rewrite_ctes(sql, true);
    let cte = pushed
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case("year_total"))
        .expect("year_total CTE");
    let mut wheres = Vec::new();
    collect_branch_wheres(&cte.query, &mut wheres);
    assert_eq!(wheres.len(), 3, "expected three UNION ALL branches");
    for (idx, where_on) in wheres.iter().enumerate() {
        assert!(
            where_on.contains("d_year") && where_on.contains("1999") && where_on.contains("2000"),
            "branch #{idx} should carry d_year IN (1999, 2000), got: {where_on}"
        );
    }

    let untouched = rewrite_ctes(sql, false);
    let cte = untouched
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case("year_total"))
        .expect("year_total CTE");
    let mut wheres = Vec::new();
    collect_branch_wheres(&cte.query, &mut wheres);
    assert!(
        wheres.iter().all(|w| w == "None"),
        "gate off should leave every UNION branch untouched, got: {wheres:?}"
    );
}

#[test]
fn q4_union_all_shape_result_identical_with_and_without_pushdown() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for ddl in [
        "CREATE TABLE store_sales (d_year INT, cust INT, v INT)",
        "CREATE TABLE catalog_sales (d_year INT, cust INT, v INT)",
        "CREATE TABLE web_sales (d_year INT, cust INT, v INT)",
    ] {
        engine.execute(ddl).unwrap();
    }
    engine
        .execute(
            "INSERT INTO store_sales VALUES \
             (1998,1,100),(1999,1,10),(2000,1,30),(2001,1,999)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO catalog_sales VALUES (1999,1,20),(2000,1,80),(2001,1,777)")
        .unwrap();
    engine
        .execute("INSERT INTO web_sales VALUES (1999,1,40),(2000,1,60),(1998,1,555)")
        .unwrap();

    let sql = "with year_total as (\
                 select d_year dyear, cust, sum(v) year_total, 's' sale_type \
                 from store_sales group by d_year, cust \
                 union all \
                 select d_year dyear, cust, sum(v) year_total, 'c' sale_type \
                 from catalog_sales group by d_year, cust \
                 union all \
                 select d_year dyear, cust, sum(v) year_total, 'w' sale_type \
                 from web_sales group by d_year, cust) \
               select s2.cust \
               from year_total s1, year_total s2 \
               where s2.cust = s1.cust \
                 and s1.sale_type = 's' and s2.sale_type = 's' \
                 and s1.dyear = 1999 and s2.dyear = 2000 \
                 and s1.year_total > 0 \
                 and s2.year_total / s1.year_total > 2";

    let run = |gate: &str| -> Vec<Vec<String>> {
        with_test_env(&[("MYDB_PRED_PUSHDOWN_AGG", Some(gate))], || {
            let QueryResponse::ResultSet(r) = engine.execute(sql).unwrap() else {
                panic!("expected result set");
            };
            r.rows
                .iter()
                .map(|row| row.iter().map(|v| format!("{v:?}")).collect())
                .collect()
        })
    };
    let off = run("off");
    let on = run("on");
    assert_eq!(on, off, "Q4-shaped UNION pushdown must not change results");
    assert_eq!(on.len(), 1, "fixture should produce one surviving customer");
}

#[test]
fn union_all_cte_parallel_branch_materialization_matches_serial() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for ddl in [
        "CREATE TABLE a (k INT, v INT)",
        "CREATE TABLE b (k INT, v INT)",
        "CREATE TABLE c (k INT, v INT)",
    ] {
        engine.execute(ddl).unwrap();
    }
    engine
        .execute("INSERT INTO a VALUES (1,10),(2,20)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (2,200),(3,300)")
        .unwrap();
    engine
        .execute("INSERT INTO c VALUES (3,3000),(4,4000)")
        .unwrap();

    let sql = "with u as (\
                 select k, v, 'a' src from a \
                 union all \
                 select k, v, 'b' src from b \
                 union all \
                 select k, v, 'c' src from c) \
               select k, src, v from u where k >= 2 order by src, k, v";

    let run = |parallel: &str| -> Vec<Vec<String>> {
        with_test_env(
            &[
                ("MYDB_CTE_INLINE", Some("off")),
                ("MYDB_CTE_UNION_BRANCH_PARALLEL", Some(parallel)),
            ],
            || {
                let QueryResponse::ResultSet(r) = engine.execute(sql).unwrap() else {
                    panic!("expected result set");
                };
                r.rows
                    .iter()
                    .map(|row| row.iter().map(|v| format!("{v:?}")).collect())
                    .collect()
            },
        )
    };
    let serial = run("off");
    let parallel = run("on");
    assert_eq!(
        parallel, serial,
        "parallel UNION ALL CTE branch materialization must match serial"
    );
    assert_eq!(
        parallel.len(),
        5,
        "fixture should exercise all three branches"
    );
}

/// End-to-end: a Q78-shaped query (three grouped CTEs, `ss LEFT JOIN ws LEFT
/// JOIN cs`, outer `ss_year = <c>`) must return identical rows with the gate on
/// and off. Pins the rewrite as a pure plan-shape change.
#[test]
fn q78_shape_result_identical_with_and_without_pushdown() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for ddl in [
        "CREATE TABLE store (yr INT, item INT, cust INT, q INT)",
        "CREATE TABLE web (yr INT, item INT, cust INT, q INT)",
        "CREATE TABLE cat (yr INT, item INT, cust INT, q INT)",
    ] {
        engine.execute(ddl).unwrap();
    }
    // Two years, overlapping (item,cust) keys so the joins produce matches.
    engine
        .execute(
            "INSERT INTO store VALUES \
             (2001,1,10,5),(2002,1,10,7),(2002,1,10,3),(2002,2,20,9),(2002,3,30,4)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO web VALUES (2001,1,10,2),(2002,1,10,6),(2002,2,20,1)")
        .unwrap();
    engine
        .execute("INSERT INTO cat VALUES (2002,1,10,8),(2002,3,30,5),(2001,2,20,2)")
        .unwrap();

    let sql = "with \
                 ss as (select yr as ss_year, item ss_item, cust ss_cust, sum(q) ss_qty \
                        from store group by yr, item, cust), \
                 ws as (select yr as ws_year, item ws_item, cust ws_cust, sum(q) ws_qty \
                        from web group by yr, item, cust), \
                 cs as (select yr as cs_year, item cs_item, cust cs_cust, sum(q) cs_qty \
                        from cat group by yr, item, cust) \
               select ss_year, ss_item, ss_cust, ss_qty, \
                      coalesce(ws_qty,0)+coalesce(cs_qty,0) other_qty \
               from ss \
               left join ws on (ws_year=ss_year and ws_item=ss_item and ws_cust=ss_cust) \
               left join cs on (cs_year=ss_year and cs_item=ss_item and cs_cust=ss_cust) \
               where (coalesce(ws_qty,0)>0 or coalesce(cs_qty,0)>0) and ss_year=2002 \
               order by ss_year, ss_item, ss_cust";

    let run = |gate: &str| -> Vec<Vec<String>> {
        with_test_env(&[("MYDB_PRED_PUSHDOWN_AGG", Some(gate))], || {
            let QueryResponse::ResultSet(r) = engine.execute(sql).unwrap() else {
                panic!("expected result set");
            };
            r.rows
                .iter()
                .map(|row| row.iter().map(|v| format!("{v:?}")).collect())
                .collect()
        })
    };
    let off = run("off");
    let on = run("on");
    assert_eq!(on, off, "pushdown must not change results");
    // Sanity: the query actually returns the expected 2002 rows that have a
    // matching ws or cs row (item1/cust10 and item3/cust30; item2/cust20 has a
    // ws match too).
    assert!(!on.is_empty(), "expected non-empty Q78-shape result");
}

/// Q75 shape: a multi-reference CTE whose body *aggregates over a derived
/// UNION* (`SELECT ... FROM (A UNION B) sd GROUP BY ...`) — not a UNION CTE
/// directly (that is the Q4 shape above). WS-D pushes `d_year IN (...)` into
/// the grouped body; the recursion in `push_output_domain_predicate` must then
/// carry it into each UNION branch so the filter reaches the branches' base
/// tables (indexed `date_dim` scan) instead of stalling above the materialised
/// union. Note the branches are joined by a plain `UNION` (distinct), exercising
/// the relaxed `set.all` guard.
#[test]
fn q75_grouped_cte_over_union_derived_pushes_domain_into_branches() {
    let sql = "with all_sales as (\
                 select d_year, i_item_sk, sum(cnt) cnt from (\
                   select d_year, i_item_sk, cs_quantity cnt \
                     from catalog_sales join date_dim on d_date_sk = cs_sold_date_sk \
                   union \
                   select d_year, i_item_sk, ss_quantity cnt \
                     from store_sales join date_dim on d_date_sk = ss_sold_date_sk\
                 ) sd group by d_year, i_item_sk) \
               select c.i_item_sk \
               from all_sales c, all_sales p \
               where c.i_item_sk = p.i_item_sk \
                 and c.d_year = 2002 and p.d_year = 2002 - 1";

    let branch_wheres = |ctes: &[CteDef]| -> Vec<String> {
        let cte = ctes
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case("all_sales"))
            .expect("all_sales CTE");
        let crate::parser::QueryExpr::Select(body) = &cte.query else {
            return Vec::new();
        };
        let mut out = Vec::new();
        match body.derived.as_ref() {
            Some(crate::parser::SubQuery::SetOp(stmt)) => collect_branch_wheres(stmt, &mut out),
            _ => {}
        }
        out
    };

    let on = branch_wheres(&rewrite_ctes(sql, true));
    assert_eq!(on.len(), 2, "expected two UNION branches, got: {on:?}");
    for (i, w) in on.iter().enumerate() {
        assert!(
            w.contains("d_year") && w.contains("2002") && w.contains("2001"),
            "branch #{i} should carry d_year IN (2002, 2001), got: {w}"
        );
    }

    let off = branch_wheres(&rewrite_ctes(sql, false));
    assert!(
        off.iter().all(|w| w == "None"),
        "gate off must leave UNION branches untouched, got: {off:?}"
    );
}

#[test]
fn nonrecursive_with_query_is_supported_inside_derived_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE derived_cte_t (v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO derived_cte_t VALUES (1), (2)")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute(
            "SELECT outer_t.v \
             FROM derived_cte_t AS outer_t, \
                  (WITH RECURSIVE c AS \
                       (SELECT * FROM derived_cte_t WHERE v > 1) \
                   SELECT * FROM c) AS inner_t \
             ORDER BY outer_t.v",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result
            .rows
            .iter()
            .map(|row| row[0].to_display_string())
            .collect::<Vec<_>>(),
        ["1", "2"]
    );
}

#[test]
fn parallel_union_branch_spools_preserve_wide_rows_under_low_budget() {
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE wide_union (k INT, v VARCHAR(1024))")
        .unwrap();
    let payload = "x".repeat(512);
    for start in (0..1200).step_by(100) {
        let values = (start..start + 100)
            .map(|k| format!("({k},'{payload}')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO wide_union VALUES {values}"))
            .unwrap();
    }
    let sql = "WITH u AS (SELECT k,v FROM wide_union UNION ALL SELECT k,v FROM wide_union UNION ALL SELECT k,v FROM wide_union) SELECT COUNT(DISTINCT k),SUM(LENGTH(v)),MIN(k),MAX(k) FROM u";
    let run = |parallel| {
        with_test_env(
            &[
                ("MYDB_CTE_INLINE", Some("off")),
                ("MYDB_CTE_UNION_BRANCH_PARALLEL", Some(parallel)),
            ],
            || {
                let budget = std::sync::Arc::new(MemBudget::new(64 * 1024));
                let result = with_query_budget(budget.clone(), || {
                    engine.execute(sql).unwrap().materialize().unwrap()
                });
                assert_eq!(budget.used(), 0);
                let QueryResponse::ResultSet(result) = result else {
                    panic!("result set");
                };
                result.rows
            },
        )
    };
    assert_eq!(run("on"), run("off"));
}
