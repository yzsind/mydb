//! Direct entry-point parity against a nested-loop expression oracle. SQL
//! routing alone can select a different physical join and miss these loops.
use super::*;
use crate::parser::JoinType;
use crate::storage::{DurabilityMode, IsolationLevel, StorageConfig};

fn expression(sql: &str) -> Expr {
    crate::parser::parse_expr_fragment(sql).unwrap()
}

fn values(rows: &[PlanRow]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.fields
                .iter()
                .map(|f| f.value.to_display_string())
                .collect()
        })
        .collect()
}

#[test]
fn inlj_pair_paths_match_nested_loop_for_join_kinds_nulls_and_fallbacks() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        StorageConfig {
            durability: DurabilityMode::Memory,
            ..StorageConfig::from_env()
        },
    )
    .unwrap();
    engine
        .execute("CREATE TABLE l(id INT PRIMARY KEY,k INT,lo DECIMAL(12,2))")
        .unwrap();
    engine
        .execute("INSERT INTO l VALUES(1,1,1.50),(2,1,4.00),(3,2,3.00),(4,9,0),(5,NULL,0)")
        .unwrap();
    engine
        .execute("CREATE TABLE r(id INT PRIMARY KEY,k INT,v DECIMAL(12,2),INDEX kv(k,v))")
        .unwrap();
    engine
        .execute("INSERT INTO r VALUES(1,1,1.25),(2,1,2.50),(3,1,NULL),(4,2,3.75),(5,3,4.00)")
        .unwrap();
    engine.execute("ANALYZE TABLE r").unwrap();
    let db = engine.db_for_tests();
    let txn = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted);
    let Statement::Select(select) =
        crate::parser::parse_statement("SELECT * FROM test.l AS l").unwrap()
    else {
        panic!("select")
    };
    let left = crate::planner::plan_query_physical(db, &select)
        .unwrap()
        .unwrap();
    let left_rows = execute_plan_rows(db, txn, &left).unwrap();
    let schema = db.snapshot_table_schema("test.r").unwrap();
    let right = PlanTable::new(schema.id, schema.clone(), Some("r".into()));
    let index = schema
        .indexes
        .iter()
        .find(|index| index.name == "kv")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT k,v FROM r ORDER BY k,v").unwrap()
    else {
        panic!("rows")
    };
    let right_rows: Vec<_> = rs
        .rows
        .into_iter()
        .map(|row| projected_values_to_plan_row(&right, &[1, 2], row).unwrap())
        .collect();
    let mut left_fields = Vec::new();
    collect_plan_fields(&left, &mut left_fields);
    let left_refs = field_ref_set(&left_fields);
    for suffix in [
        "",
        " AND r.v + 0 >= 1.50",
        " AND r.v + 0 > l.lo",
        " AND r.v + 0 >= 1.50 AND r.v + 0 > l.lo",
        " AND r.v + 0 > l.lo + (SELECT 0)",
    ] {
        let condition = expression(&format!("r.k = l.k{suffix}"));
        let plan = compile_index_lookup_plan(&condition, &right, index).unwrap();
        assert!(
            plan.range.is_none(),
            "fixture must exercise residual evaluation"
        );
        let (right_only, joined) =
            split_right_only_residual(plan.residual.as_ref(), &right, &left_refs);
        if suffix.contains("SELECT") {
            assert!(JoinedResidualPre::compile(
                joined.as_ref().unwrap(),
                &left_rows[0],
                &right_rows[0]
            )
            .is_none());
        }
        for kind in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Semi,
            JoinType::Anti,
        ] {
            let mut expected = Vec::new();
            let lookup = new_join_lookup();
            let mut ctx = EvalQueryContext::default();
            for l in &left_rows {
                let mut matched = false;
                for r in &right_rows {
                    let joined = join_rows(l, r, &lookup);
                    if !eval_plan_bool_with_db(db, txn, &condition, &joined, &mut ctx).unwrap() {
                        continue;
                    }
                    matched = true;
                    match kind {
                        JoinType::Semi => {
                            expected.push(l.clone());
                            break;
                        }
                        JoinType::Anti => {}
                        _ => expected.push(joined),
                    }
                }
                if !matched {
                    match kind {
                        JoinType::Left => {
                            expected.push(null_extend_right_table(l, &right, Some(&[1, 2])))
                        }
                        JoinType::Anti => expected.push(l.clone()),
                        _ => {}
                    }
                }
            }
            for covering in [false, true] {
                let before = INLJ_PER_ROW_EXECUTIONS.load(Ordering::Relaxed);
                let actual = crate::util::env::with_vars(
                    &[("MYDB_INLJ_BATCH_MAX_RIGHT_ROWS", Some("1"))],
                    || {
                        execute_index_nested_loop_join_from_left_rows_inner(
                            db,
                            txn,
                            &left,
                            &left_rows,
                            &right,
                            index,
                            kind,
                            &plan,
                            covering,
                            Some(&[1, 2]),
                            &[],
                            None,
                        )
                    },
                )
                .unwrap();
                assert!(INLJ_PER_ROW_EXECUTIONS.load(Ordering::Relaxed) > before);
                assert_eq!(
                    values(&actual),
                    values(&expected),
                    "covering={covering}, kind={kind:?}, {suffix}"
                );
            }
            let actual = execute_batched_inlj_covering(
                db,
                txn,
                &left_rows,
                &right,
                index,
                kind,
                &plan,
                right_only.as_ref(),
                joined.as_ref(),
                Some(&[1, 2]),
                &[],
            )
            .unwrap();
            assert_eq!(
                values(&actual),
                values(&expected),
                "batch covering, kind={kind:?}, {suffix}"
            );
        }
    }
}

fn one_value_row(table: &str, value: Value) -> PlanRow {
    PlanRow::from_fields(vec![PlanField::new(
        table,
        None,
        "v",
        ColumnType::LongLong,
        value,
    )])
}

#[test]
fn inlj_pair_matching_is_sticky_and_semi_emission_stops_candidates() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    let txn = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted);
    let residual = expression("r.v > l.v");
    let left = one_value_row("l", Value::Int(5));
    for kind in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Semi,
        JoinType::Anti,
    ] {
        let mut output = InljPairOutput::new(&db, txn, kind, Some(&residual));
        let mut rows = Vec::new();
        let mut matched = false;
        let mut ctx = EvalQueryContext::default();
        assert!(output
            .push(
                &left,
                one_value_row("r", Value::Null),
                &mut rows,
                &mut matched,
                &mut ctx
            )
            .unwrap()
            .is_continue());
        assert!(!matched);
        assert!(rows.is_empty());
        let control = output
            .push(
                &left,
                one_value_row("r", Value::Int(10)),
                &mut rows,
                &mut matched,
                &mut ctx,
            )
            .unwrap();
        assert!(matched);
        assert_eq!(control.is_break(), kind == JoinType::Semi);
        match kind {
            JoinType::Semi => assert_eq!(values(&rows), vec![vec!["5"]]),
            JoinType::Anti => assert!(rows.is_empty()),
            _ => assert_eq!(values(&rows), vec![vec!["5", "10"]]),
        }
        if control.is_continue() {
            let before = values(&rows);
            let _ = output
                .push(
                    &left,
                    one_value_row("r", Value::Int(1)),
                    &mut rows,
                    &mut matched,
                    &mut ctx,
                )
                .unwrap();
            assert!(
                matched,
                "a rejected later candidate must not erase an earlier match"
            );
            assert_eq!(values(&rows), before);
        }
    }
}

#[test]
fn inlj_pair_anti_retains_later_subquery_errors_after_matching() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        StorageConfig {
            durability: DurabilityMode::Memory,
            ..StorageConfig::from_env()
        },
    )
    .unwrap();
    engine
        .execute("CREATE TABLE guard_values(id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO guard_values VALUES(1,0),(2,0)")
        .unwrap();
    let db = engine.db_for_tests();
    let txn = TxnView::read_only(db.visible_scn(), IsolationLevel::ReadCommitted);
    let residual = expression("r.v > (SELECT v FROM test.guard_values WHERE id <= r.v)");
    let left = one_value_row("l", Value::Int(0));
    assert!(
        JoinedResidualPre::compile(&residual, &left, &one_value_row("r", Value::Int(1))).is_none()
    );
    for kind in [JoinType::Inner, JoinType::Left, JoinType::Anti] {
        let mut output = InljPairOutput::new(db, txn, kind, Some(&residual));
        let mut rows = Vec::new();
        let mut matched = false;
        let mut ctx = EvalQueryContext::default();
        assert!(output
            .push(
                &left,
                one_value_row("r", Value::Int(1)),
                &mut rows,
                &mut matched,
                &mut ctx
            )
            .unwrap()
            .is_continue());
        assert!(matched);
        let error = output
            .push(
                &left,
                one_value_row("r", Value::Int(2)),
                &mut rows,
                &mut matched,
                &mut ctx,
            )
            .unwrap_err();
        assert!(
            error.is(crate::error::catalog::ER_SUBQUERY_NO_1_ROW),
            "{error:?}"
        );
        assert_eq!(rows.len(), usize::from(kind != JoinType::Anti));
    }
}

struct InljPairOutput<'a> {
    db: &'a Arc<Database>,
    txn: TxnView,
    kind: JoinType,
    residual: Option<&'a Expr>,
    compiled: LazyJoinedResidualPre,
    lookup: Arc<RowSchema>,
}
impl<'a> InljPairOutput<'a> {
    fn new(
        db: &'a Arc<Database>,
        txn: TxnView,
        kind: JoinType,
        residual: Option<&'a Expr>,
    ) -> Self {
        Self {
            db,
            txn,
            kind,
            residual,
            compiled: LazyJoinedResidualPre::new(),
            lookup: new_join_lookup(),
        }
    }
    fn push(
        &mut self,
        left: &PlanRow,
        right: PlanRow,
        out: &mut Vec<PlanRow>,
        matched: &mut bool,
        ctx: &mut EvalQueryContext,
    ) -> Result<std::ops::ControlFlow<()>, QueryError> {
        let db = self.db;
        let txn = self.txn;
        let join_type = self.kind;
        let semi_or_anti = matches!(join_type, JoinType::Semi | JoinType::Anti);
        let residual = self.residual;
        let residual_pre = &mut self.compiled;
        let join_lk = &self.lookup;
        let left_row = left;
        let mut eval_ctx = ctx;
        let mut matched_pair = *matched;
        let before = out.len();
        for right_row in std::iter::once(right) {
            emit_inlj_pair!(
                db,
                txn,
                join_type,
                semi_or_anti,
                residual,
                residual_pre,
                join_lk,
                left_row,
                right_row,
                out,
                matched_pair,
                eval_ctx
            );
        }
        *matched = matched_pair;
        Ok(if join_type == JoinType::Semi && out.len() > before {
            std::ops::ControlFlow::Break(())
        } else {
            std::ops::ControlFlow::Continue(())
        })
    }
}
