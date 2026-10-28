use super::*;

#[test]
fn vector_audit_folded_query_values_preserve_text_and_binary_domains() {
    for text in ["abcd", "abcdefghijklmnop", "1,2,3,4", "[1,nan]"] {
        for value in [
            Value::String(text.into()),
            Value::SmallString(crate::types::SmallStringBuf::from_slice(text.as_bytes())),
            Value::Bytes(text.as_bytes().to_vec()),
        ] {
            assert!(value_to_vector_bytes(&value).is_none(), "{value:?}");
        }
    }
    let vector = crate::engine::vector::codec::from_f32(&[1.0, 2.0]);
    for value in [
        Value::Vector(vector.clone()),
        Value::String("[1,2]".into()),
        Value::Bytes(b"[1,2]".to_vec()),
    ] {
        assert_eq!(value_to_vector_bytes(&value), Some(vector.clone()));
    }
    assert!(value_to_vector_bytes(&Value::Bytes(vector.payload)).is_none());
}

#[test]
fn sqlproc_sus_streaming_group_contiguity_does_not_imply_order() {
    let input = PhysicalPlan::ValuesScan {
        rows: vec![],
        columns: vec![],
        filter: None,
        estimated_rows: 2.0,
        cost: crate::engine::cbo::Cost::scalar(1.0),
    };
    let key = Expr::Identifier("a".into());
    let plan = PhysicalPlan::StreamingAggregate {
        input: Box::new(input),
        group_by: vec![key.clone()],
        having: None,
        aggregates: vec![],
        estimated_rows: 2.0,
        cost: crate::engine::cbo::Cost::scalar(1.0),
    };
    assert!(!plan_satisfies_order_by(
        &plan,
        &[OrderBy {
            expr: key,
            asc: true,
            nulls_first: None
        }]
    ));
}
use crate::engine::params::with_params;
use crate::engine::{executor::StorageQueryEngine, QueryResponse};
use crate::parser::{number_statement_placeholders, parse_statement, Statement};
use crate::storage::{
    DurabilityMode, IndexBound, IndexKey, KeyPart, ParamTarget, StartupValidationMode,
    StorageConfig,
};
use std::time::Duration;

#[test]
fn derived_subquery_pushdown_proves_all_producers_and_rejects_hidden_other_correlations() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE binding_dim(k INT,tag VARCHAR(10))")
        .unwrap();
    let Statement::Select(source) = parse_statement("SELECT d.tag FROM binding_dim d").unwrap()
    else {
        panic!("select")
    };
    let table = plan_table(engine.db_for_tests(), &source.from[0]).unwrap();
    for (sql, expected) in [
        ("SELECT q.tag FROM (SELECT d2.tag,RANK() OVER(PARTITION BY d2.tag ORDER BY d2.k) rn FROM binding_dim d2) q WHERE q.rn<=1", true),
        ("SELECT q.tag FROM (SELECT d2.tag FROM binding_dim d2 WHERE d2.k=f.k) q", false),
        ("SELECT q.tag FROM (SELECT d2.tag FROM binding_dim d2 WHERE d2.k=d.k) q", true),
        ("SELECT q.tag FROM (SELECT d2.tag FROM binding_dim d2 UNION ALL SELECT d3.tag FROM binding_dim d3) q", true),
        ("SELECT q.tag FROM (SELECT d2.tag FROM binding_dim d2 UNION ALL SELECT d3.tag FROM binding_dim d3 WHERE d3.k=f.k) q", false),
        ("SELECT q.rv FROM (SELECT RAND() rv FROM binding_dim d2) q", false),
    ] {
        let Statement::Select(subquery) = parse_statement(sql).unwrap() else { panic!("select") };
        assert_eq!(subquery_correlation_only_uses_table(engine.db_for_tests(), &subquery, &table), expected, "{sql}");
    }
}

fn open_test_engine(dir: &std::path::Path) -> StorageQueryEngine {
    StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir,
        StorageConfig {
            durability: DurabilityMode::Memory,
            checkpoint_interval: Duration::from_millis(60_000),
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            lock_wait: Duration::from_millis(1_000),
            group_commit_window: Duration::from_micros(0),
            wal_segment_bytes: 64 * 1024 * 1024,
            buffer_pool_pages: 512,
            buffer_pool_io_threads: 1,
            buffer_pool_prefetch_pages: 0,
            undo_retention: Duration::from_millis(60_000),
            heap_update_reserve_bytes: 2048,
            startup_validation: StartupValidationMode::Off,
            startup_validate_indexes: false,
        },
    )
    .unwrap()
}

#[test]
fn key_only_compact_cost_requires_supported_fixed_width_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE compact_keys (k BIGINT, d DATE, u BIGINT UNSIGNED, s VARCHAR(20), f DOUBLE, n DECIMAL(20,2))").unwrap();
    let Statement::Select(select) = parse_statement("SELECT * FROM compact_keys").unwrap() else {
        panic!("SELECT")
    };
    let plan = plan_select(engine.db_for_tests(), &select)
        .unwrap()
        .physical;
    let key = |name: &str| Expr::Identifier(name.into());
    assert!(compact_key_only_group_state_bytes(&plan, &[key("k"), key("d")], &[]).is_some());
    for name in ["u", "s", "f", "n"] {
        assert!(
            compact_key_only_group_state_bytes(&plan, &[key(name)], &[]).is_none(),
            "{name}"
        );
    }
    assert!(compact_key_only_group_state_bytes(&plan, &[], &[]).is_none());
    assert!(compact_key_only_group_state_bytes(&plan, &vec![key("k"); 9], &[]).is_none());
    assert!(compact_key_only_group_state_bytes(&plan, &[key("k")], &[key("k")]).is_none());
}

#[test]
fn or_pushdown_derives_union_domain_for_each_repeated_column() {
    let Statement::Select(select) = parse_statement(
        "SELECT 1 FROM addr WHERE \
         (country='US' AND state IN ('CA','OR') AND profit BETWEEN 0 AND 10) OR \
         (country='US' AND state IN ('NY') AND profit BETWEEN 20 AND 30) OR \
         (country='US' AND state IN ('TX','VA') AND profit BETWEEN 40 AND 50)",
    )
    .unwrap() else {
        panic!("expected SELECT");
    };
    let predicate = select.selection.expect("test query has WHERE");
    let branches: Vec<Vec<Expr>> = split_disjuncts_top(&predicate)
        .into_iter()
        .map(|branch| {
            let mut conjuncts = Vec::new();
            split_conjuncts(&branch, &mut conjuncts);
            conjuncts
        })
        .collect();
    let derived = derive_or_column_domain_unions(&branches);

    let domain = |name: &str| {
        derived.iter().find_map(|expr| match expr {
            Expr::InList {
                expr,
                list,
                negated: false,
            } if expr_column_name(expr).is_some_and(|column| column.eq_ignore_ascii_case(name)) => {
                Some(list)
            }
            _ => None,
        })
    };
    assert_eq!(domain("country").expect("country domain").len(), 1);
    assert_eq!(domain("state").expect("state union domain").len(), 5);
    assert!(
        domain("profit").is_none(),
        "BETWEEN is not an exact IN domain"
    );
}

#[test]
fn having_cardinality_is_separate_from_aggregate_state_groups() {
    fn having(sql: &str) -> Expr {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!("expected SELECT");
        };
        select.having.expect("test query has HAVING")
    }

    let groups = 10_000.0;
    assert_eq!(estimate_having_output_rows(None, groups), groups);
    assert_eq!(
        estimate_having_output_rows(
            Some(&having("SELECT k FROM t GROUP BY k HAVING COUNT(*) > 0")),
            groups,
        ),
        groups,
        "every materialised group has COUNT(*) >= 1"
    );
    assert_eq!(
        estimate_having_output_rows(
            Some(&having("SELECT k FROM t GROUP BY k HAVING SUM(v) > 300")),
            groups,
        ),
        10.0
    );
    assert_eq!(
        estimate_having_output_rows(
            Some(&having("SELECT k FROM t GROUP BY k HAVING SUM(v) > SUM(w)")),
            groups,
        ),
        1_000.0,
        "non-literal aggregate predicates keep the conservative prior"
    );
    assert_eq!(
        estimate_having_output_rows(
            Some(&having("SELECT k FROM t GROUP BY k HAVING FALSE")),
            groups,
        ),
        0.0
    );
    assert_eq!(
        estimate_having_output_rows(
            Some(&having("SELECT k FROM t GROUP BY k HAVING SUM(v) > 300")),
            0.04,
        ),
        0.04,
        "fractional expected groups must not be inflated or panic"
    );
}

/// Parse a SELECT and return the normalized inner `Select` after the
/// planner's predicate-normalization pass (which runs outer→inner
/// simplification). Used by the outer-join conversion tests below.
fn normalized_select(db: &Database, sql: &str) -> Select {
    let statement = parse_statement(sql).unwrap();
    let Statement::Select(select) = statement else {
        panic!("expected SELECT");
    };
    let mut select = select;
    normalize_select_predicates(db, &mut select);
    select
}

fn parsed_filter(sql: &str) -> Expr {
    let Statement::Select(select) = parse_statement(sql).unwrap() else {
        panic!("expected SELECT");
    };
    select.selection.expect("query must have WHERE")
}

#[test]
fn inlj_coverage_floor_tracks_broad_heap_pages_even_when_resident() {
    // Residency removes storage misses, but a broad non-covering probe
    // still pays RID routing/sort, buffer lookup and heap decode. Page
    // coverage therefore controls the smooth floor independently of row
    // selectivity and the stable planning-cache baseline.
    let sparse_resident =
        inlj_coverage_floor_weight(0.52, 0.519, 40_008.0, 6_001_215.0, 56_682.0, 65_536.0);
    let dense_resident =
        inlj_coverage_floor_weight(0.99, 0.993, 1_200_243.0, 6_001_215.0, 57_972.0, 65_536.0);
    let pressured =
        inlj_coverage_floor_weight(0.52, 0.519, 40_008.0, 6_001_215.0, 70_000.0, 65_536.0);
    assert_eq!(sparse_resident, 1.0);
    assert_eq!(dense_resident, 1.0);
    assert_eq!(pressured, 1.0);
    assert_eq!(
        inlj_coverage_floor_weight(0.10, 0.90, 1_000.0, 2_000.0, 70_000.0, 65_536.0),
        0.0
    );
}

#[test]
fn unanchored_like_default_is_not_discounted_twice_for_typical_words() {
    assert_eq!(unanchored_like_literal_tail("green".len()), 0);
    assert_eq!(unanchored_like_literal_tail("specialrequests".len()), 7);
    let base = 200_000.0;
    assert_eq!(apply_like_residual_discount(base, 0), base);
    assert!(apply_like_residual_discount(base, 7) < base);
    assert_eq!(apply_like_residual_discount(0.5, 12), 1.0);
    assert_eq!(apply_like_residual_discount(0.0, 12), 0.0);
}

#[test]
fn semi_anti_residual_cost_uses_truncated_geometric_probe_work() {
    let expected = 1.0 + 0.75 + 0.75_f64.powi(2) + 0.75_f64.powi(3);
    let observed = semi_anti_expected_inner_rows(4.0, 0.25);
    assert!((observed - expected).abs() < 1e-12, "{observed}");
    assert_eq!(semi_anti_expected_inner_rows(4.0, 1.0), 1.0);
    assert_eq!(semi_anti_expected_inner_rows(4.0, 0.0), 4.0);
    assert_eq!(semi_anti_expected_inner_rows(0.0, 0.5), 0.0);
}

#[test]
fn reverse_semi_anti_cost_keeps_scan_work_and_bounds_retained_state() {
    use crate::engine::cbo::{operator_cost, CboContext, Cost};
    let ctx = CboContext::defaults_for_test();
    let build = Cost::from_components(10.0, 200_000.0, 500_000.0, 0.0, 30_000.0, 0.0, 0.0);
    let probe = Cost::from_components(5.0, 40_000.0, 120_000.0, 0.0, 10_000.0, 0.0, 0.0);
    // Exercise narrow, composite and wide encoded keys, independently of
    // the integer fast lane or optional runtime-filter pushdown.
    for key_width in [12.0, 80.0, 1024.0] {
        let input = |build_rows| operator_cost::ReverseSemiAntiHashJoinInput {
            build_rows,
            probe_rows: 10_000.0,
            probe_width: 64.0,
            key_width,
            matched_build_rows: 30_000.0,
            output_rows: 8_000.0,
        };
        let small =
            operator_cost::reverse_semi_anti_hash_join(&ctx, &build, &probe, input(1_000_000.0))
                .unwrap();
        let large =
            operator_cost::reverse_semi_anti_hash_join(&ctx, &build, &probe, input(20_000_000.0))
                .unwrap();
        assert!(small.is_valid() && large.is_valid());
        assert!(small.total >= build.total + probe.total);
        assert_eq!(large.startup, large.total);
        assert_eq!(large.io_seq_pages, build.io_seq_pages + probe.io_seq_pages);
        assert_eq!(large.mem_bytes, small.mem_bytes);
        assert_eq!(large.spill_bytes, 0.0);
        assert!(
            large.total > small.total,
            "streaming still prices every build key"
        );
    }
    let wide = operator_cost::ReverseSemiAntiHashJoinInput {
        build_rows: 20_000_000.0,
        probe_rows: 500_000.0,
        probe_width: 1024.0,
        key_width: 80.0,
        matched_build_rows: 1_000_000.0,
        output_rows: 400_000.0,
    };
    assert!(operator_cost::reverse_semi_anti_hash_join(&ctx, &build, &probe, wide).is_none());
    for join_type in [JoinType::Semi, JoinType::Anti] {
        assert!(reverse_semi_anti_hash_join_eligible(
            join_type, false, 100.0, 201.0
        ));
        assert!(!reverse_semi_anti_hash_join_eligible(
            join_type, false, 100.0, 200.0
        ));
        assert!(!reverse_semi_anti_hash_join_eligible(
            join_type, true, 100.0, 1000.0
        ));
    }
    for join_type in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
    ] {
        assert!(!reverse_semi_anti_hash_join_eligible(
            join_type, false, 100.0, 1000.0
        ));
    }
}

#[test]
fn reverse_semi_anti_cost_selects_hash_over_broad_noncovering_lookup() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE existence_probe (id INT PRIMARY KEY, category INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE existence_build (id INT PRIMARY KEY, fk INT, a INT, b INT, INDEX(fk))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO existence_probe VALUES (1,1),(2,2)")
        .unwrap();
    engine
        .execute("INSERT INTO existence_build VALUES (1,1,0,1),(2,2,0,1)")
        .unwrap();
    engine.execute("ANALYZE TABLE existence_probe").unwrap();
    engine.execute("ANALYZE TABLE existence_build").unwrap();
    let db = engine.db_for_tests();
    let ctx = crate::engine::cbo::CboContext::from_db(db);
    let scan = |name: &str, key: &str, rows: u64, pages: u64, kept: f64, cost: f64, filter| {
        let mut schema = db.table(name).unwrap();
        let stats = &mut Arc::make_mut(&mut schema).stats;
        stats.row_count = rows;
        stats.heap_page_count = pages;
        stats.columns.get_mut(key).unwrap().distinct_count = 15_000_000;
        for index_stats in stats.indexes.values_mut() {
            index_stats.prefix_distinct_counts[0] = 15_000_000;
        }
        PhysicalPlan::SeqScan {
            table: PlanTable::new(schema.id, schema, None),
            filter,
            estimated_rows: kept,
            access_rows: rows as f64,
            cost: crate::engine::cbo::Cost::scalar(cost),
            possible_keys: vec![],
            runtime_filters: vec![],
            hint_note: None,
            projected_columns: None,
        }
    };
    let outer = scan(
        "existence_probe",
        "id",
        15_000_000,
        120_000,
        500_000.0,
        40_000.0,
        None,
    );
    let inner = scan(
        "existence_build",
        "fk",
        60_000_000,
        500_000,
        20_000_000.0,
        160_000.0,
        Some(parsed_filter("SELECT * FROM existence_build WHERE a < b")),
    );
    let condition = parsed_filter("SELECT * FROM existence_probe, existence_build WHERE existence_probe.id = existence_build.fk");
    let (left_keys, right_keys, _, _) =
        extract_join_keys_for_plans(&outer, &inner, &condition).unwrap();
    for join_type in [JoinType::Semi, JoinType::Anti] {
        let normal = estimate_hash_join_operator_cost(
            &ctx,
            &outer,
            &inner,
            join_type,
            &left_keys,
            &right_keys,
            Some(&Expr::Literal(crate::parser::Literal::Bool(true))),
            estimated_rows(&outer),
            estimated_rows(&inner),
        )
        .0;
        let reverse = estimate_hash_join_operator_cost(
            &ctx,
            &outer,
            &inner,
            join_type,
            &left_keys,
            &right_keys,
            None,
            estimated_rows(&outer),
            estimated_rows(&inner),
        )
        .0;
        assert!(
            normal.spill_bytes > 0.0,
            "ordinary build must exceed the grant"
        );
        assert_eq!(reverse.spill_bytes, 0.0);
        let forced_lookup = choose_join_with_hints(
            &ctx,
            outer.clone(),
            inner.clone(),
            join_type,
            Some(condition.clone()),
            &[QueryHint::NoHashJoin { tables: vec![] }],
        );
        assert!(
            matches!(forced_lookup, PhysicalPlan::IndexNestedLoopJoin { .. }),
            "{forced_lookup:?}"
        );
        assert!(
            normal.total > plan_cost(&forced_lookup).total,
            "fixture reproduces ordinary hash overpricing"
        );
        let chosen = choose_join_with_hints(
            &ctx,
            outer.clone(),
            inner.clone(),
            join_type,
            Some(condition.clone()),
            &[],
        );
        assert!(
            matches!(chosen, PhysicalPlan::HashJoin { .. }),
            "{chosen:?}"
        );
        assert!(reverse.total < plan_cost(&forced_lookup).total);
    }
}

#[test]
fn access_path_frontier_prunes_by_cost_not_access_shape() {
    let mut frontier = AccessPathFrontier::new(40.0);
    // This incumbent may be a non-unique lookup, a small IN list, a range
    // scan, or a sequential scan; only its complete cost is relevant.
    frontier.consider(3.0);
    assert!(!frontier.should_expand(3.0, false));
    assert!(!frontier.should_expand(8.0, false));
    assert!(frontier.should_expand(2.0, false));
    assert!(frontier.should_expand(8.0, true));
}

#[test]
fn zone_map_preview_matches_full_sampling_work_and_hint_forces_expansion() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE zm_preview (id BIGINT, payload VARCHAR(32), INDEX(id))")
        .unwrap();
    engine
        .execute("INSERT INTO zm_preview VALUES (1, 'one'), (2, 'two')")
        .unwrap();

    let Statement::Select(select) =
        parse_statement("SELECT payload FROM zm_preview WHERE id = 1").unwrap()
    else {
        panic!("expected SELECT");
    };
    let filter = select.selection.as_ref().unwrap();
    let table = engine.db_for_tests().table("zm_preview").unwrap();
    let preview = engine
        .db_for_tests()
        .zone_map_cost_lower_bound(&table, filter)
        .unwrap();
    let full = engine
        .db_for_tests()
        .zone_map_cost_details(&table, filter)
        .unwrap();
    assert_eq!(preview.zones_examined, full.zones_examined);

    crate::storage::reset_zone_map_cost_estimate_calls();
    let Statement::Select(forced) =
        parse_statement("SELECT /*+ ZONE_MAP(zm_preview) */ payload FROM zm_preview WHERE id = 1")
            .unwrap()
    else {
        panic!("expected SELECT");
    };
    let _ = plan_select(engine.db_for_tests(), &forced).unwrap();
    assert!(
        crate::storage::zone_map_cost_estimate_calls() > 0,
        "an explicit ZONE_MAP hint must retain the full costing path"
    );
}

#[test]
fn cost_frontier_prunes_zone_expansion_for_cheap_nonunique_and_range_indexes() {
    struct CboReset {
        rnd_page: f64,
        cpu_operator: f64,
    }
    impl Drop for CboReset {
        fn drop(&mut self) {
            crate::engine::cbo::consts::set_variable("cbo_rnd_page_cost", self.rnd_page).unwrap();
            crate::engine::cbo::consts::set_variable("cbo_cpu_operator_cost", self.cpu_operator)
                .unwrap();
        }
    }
    let prior_rnd = crate::engine::cbo::consts::variable_value("cbo_rnd_page_cost")
        .unwrap()
        .parse()
        .unwrap();
    let prior_cpu_operator = crate::engine::cbo::consts::variable_value("cbo_cpu_operator_cost")
        .unwrap()
        .parse()
        .unwrap();
    let _reset = CboReset {
        rnd_page: prior_rnd,
        cpu_operator: prior_cpu_operator,
    };
    // Make the test's index candidate explicitly cheap. This preserves the
    // branch-and-bound decision while avoiding a 21 MiB heap whose only
    // purpose was to raise the Zone Map metadata lower bound above the
    // default random-page cost.
    crate::engine::cbo::consts::set_variable("cbo_rnd_page_cost", 0.01).unwrap();
    crate::engine::cbo::consts::set_variable("cbo_cpu_operator_cost", 1.0).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE zm_cost_prune (id BIGINT, pad VARCHAR(4096), INDEX idx_id(id)) \
             ZONE_MAP=ON ZONE_SIZE=64K",
        )
        .unwrap();
    let pad = "x".repeat(3000);
    // One bulk statement keeps setup cheap while enough wide rows retain
    // the multi-zone cost gap that makes the index frontier decisive.
    let rows = (0..500)
        .map(|id| format!("({id},'{pad}')"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO zm_cost_prune VALUES {rows}"))
        .unwrap();
    engine.execute("ANALYZE TABLE zm_cost_prune").unwrap();

    for sql in [
        "SELECT id FROM zm_cost_prune WHERE id = 7",
        "SELECT id FROM zm_cost_prune WHERE id BETWEEN 7 AND 9",
    ] {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            panic!("expected SELECT");
        };
        let table = engine.db_for_tests().table("zm_cost_prune").unwrap();
        let preview = engine
            .db_for_tests()
            .zone_map_cost_lower_bound(&table, select.selection.as_ref().unwrap())
            .unwrap();
        crate::storage::reset_zone_map_cost_estimate_calls();
        let physical = plan_select(engine.db_for_tests(), &select)
            .unwrap()
            .physical;
        assert_eq!(
            crate::storage::zone_map_cost_estimate_calls(),
            0,
            "cheap index candidate should prune full Zone Map costing for {sql}; preview={preview:?}"
        );
        let explained = format!("{:?}", explain_rows(&physical));
        assert!(
            explained.contains("idx_id"),
            "expected non-unique index access for {sql}, got {explained}"
        );
    }
}

#[test]
fn zone_map_costing_skips_when_one_matching_zone_outcosts_the_index() {
    // Default cost constants: the metadata-only bound of a two-zone table is
    // negligible, but any zone holding the expected match is scanned whole.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE zm_match_floor (id BIGINT, pad VARCHAR(4096), INDEX idx_id(id))")
        .unwrap();
    let pad = "x".repeat(3000);
    let rows = (0..600)
        .map(|id| format!("({id},'{pad}')"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO zm_match_floor VALUES {rows}"))
        .unwrap();
    engine.execute("ANALYZE TABLE zm_match_floor").unwrap();
    let Statement::Select(select) =
        parse_statement("SELECT id FROM zm_match_floor WHERE id = 7").unwrap()
    else {
        panic!("expected SELECT");
    };
    let table = engine.db_for_tests().table("zm_match_floor").unwrap();
    let preview = engine
        .db_for_tests()
        .zone_map_cost_lower_bound(&table, select.selection.as_ref().unwrap())
        .unwrap();
    assert!(preview.total_pages > u64::from(table.zone_pages()));
    crate::storage::reset_zone_map_cost_estimate_calls();
    let physical = plan_select(engine.db_for_tests(), &select)
        .unwrap()
        .physical;
    assert_eq!(crate::storage::zone_map_cost_estimate_calls(), 0);
    assert!(format!("{:?}", explain_rows(&physical)).contains("idx_id"));
}

#[test]
fn prepared_physical_point_lookup_matches_standalone_planner() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE prepared_plan_eq (id BIGINT PRIMARY KEY, v VARCHAR(20))")
        .unwrap();
    let Statement::Select(select) =
        parse_statement("SELECT v FROM prepared_plan_eq WHERE id = ?").unwrap()
    else {
        panic!("expected SELECT");
    };

    let standalone = plan_select(engine.db_for_tests(), &select).unwrap();
    let prepared = plan_prepared_physical_select(engine.db_for_tests(), &select).unwrap();
    assert_eq!(
        format!("{:?}", explain_rows(&standalone.physical)),
        format!("{:?}", explain_rows(&prepared)),
        "the execution-only entry must preserve the standalone physical plan"
    );
}

#[test]
fn cbo_row_estimate_does_not_treat_updates_as_inserted_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE cbo_rows (id INT)").unwrap();
    let mut schema = (*engine.db_for_tests().table("cbo_rows").unwrap()).clone();
    schema.stats.row_count = 100;
    schema.stats.modified_rows_since_analyze = 400;
    let table = PlanTable::with_sidecar(schema.id, Arc::new(schema), None, 500, 0);
    assert_eq!(
        table_row_estimate(engine.db_for_tests(), &table).unwrap(),
        100.0
    );
}

#[test]
fn cbo_filter_estimation_preserves_zero_and_sql_null_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE cbo_sel (v VARCHAR(20) NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO cbo_sel VALUES ('alpha'),('beta'),('gamma'),('mu'),('omega'),(NULL)")
        .unwrap();
    engine.execute("ANALYZE TABLE cbo_sel").unwrap();
    let schema = engine.db_for_tests().table("cbo_sel").unwrap();

    let duplicate_in = parsed_filter("SELECT * FROM cbo_sel WHERE v IN ('alpha','alpha')");
    let single_eq = parsed_filter("SELECT * FROM cbo_sel WHERE v = 'alpha'");
    assert_eq!(
        estimate_filter_rows(&schema, &duplicate_in, 6.0),
        estimate_filter_rows(&schema, &single_eq, 6.0)
    );

    let not_in_null = parsed_filter("SELECT * FROM cbo_sel WHERE v NOT IN ('alpha', NULL)");
    assert_eq!(estimate_filter_rows(&schema, &not_in_null, 6.0), 0.0);
    assert_eq!(estimate_filter_rows(&schema, &single_eq, 0.0), 0.0);

    let string_range = parsed_filter("SELECT * FROM cbo_sel WHERE v >= 'mu'");
    let range_rows = estimate_filter_rows(&schema, &string_range, 6.0);
    assert!(
        range_rows > 0.0 && range_rows < 6.0,
        "range_rows={range_rows}"
    );
}

#[test]
fn repeated_single_column_domains_do_not_multiply_selectivity() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE domain_sel (v INT, KEY kv(v))")
        .unwrap();
    let values = (0..100)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO domain_sel VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE domain_sel").unwrap();
    let table = engine.db_for_tests().table("domain_sel").unwrap();
    for (predicate, expected) in [
        ("v IN (1,2) AND v IN (2,1)", 2.0),
        ("v IN (1,2,3) AND v IN (2,3,4)", 2.0),
        ("v IN (1,2) AND v = 2", 1.0),
        ("v = 2 AND v = 2", 1.0),
        ("v IN (1,2) AND v IN (3,4)", 0.0),
        ("v IN (1,NULL) AND v IN (NULL,1)", 1.0),
    ] {
        let filter = parsed_filter(&format!("SELECT * FROM domain_sel WHERE {predicate}"));
        let actual = estimate_filter_rows(&table, &filter, 100.0);
        assert!(
            (actual - expected).abs() < 1e-9,
            "{predicate}: {actual} != {expected}"
        );
    }
}

#[test]
fn same_column_date_bounds_are_estimated_as_one_interval() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE date_range_sel (id INT PRIMARY KEY, d DATE NOT NULL)")
        .unwrap();
    let start = chrono::NaiveDate::from_ymd_opt(1999, 1, 1).unwrap();
    let rows = (0..365)
        .map(|offset| {
            let date = start + chrono::Duration::days(offset);
            format!("({offset}, '{}')", date.format("%Y-%m-%d"))
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO date_range_sel VALUES {rows}"))
        .unwrap();
    engine.execute("ANALYZE TABLE date_range_sel").unwrap();

    let table = engine.db_for_tests().table("date_range_sel").unwrap();
    let filter = parsed_filter(
        "SELECT * FROM date_range_sel \
         WHERE d >= '1999-03-01' AND d < '1999-04-01'",
    );
    let estimated = estimate_filter_rows(&table, &filter, 365.0);
    assert!(
        (25.0..=37.0).contains(&estimated),
        "31-day interval should estimate near 31 rows, got {estimated}"
    );

    // A redundant weaker bound must not be multiplied as an independent
    // predicate and must not replace the tighter lower bound.
    let redundant = parsed_filter(
        "SELECT * FROM date_range_sel \
         WHERE d >= '1999-01-01' AND d >= '1999-03-01' AND d < '1999-04-01'",
    );
    let redundant_estimate = estimate_filter_rows(&table, &redundant, 365.0);
    assert!(
        (estimated - redundant_estimate).abs() <= 1.0,
        "redundant bound changed estimate: {estimated} vs {redundant_estimate}"
    );
}

#[test]
fn empty_composite_index_range_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t1 (a INT, b DATE NOT NULL, KEY k1 (a,b))")
        .unwrap();

    let response = engine
        .execute("SELECT MIN(b) FROM t1 WHERE a=1 AND b>'2007-08-01'")
        .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!("expected result set");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![Value::Null]);
}

#[test]
fn row_goal_costing_accepts_fractional_cardinality_estimates() {
    assert_eq!(expected_rows_to_reach_goal(0.05, 0.05, 2), 0.05);
    assert_eq!(access_rows_for_row_goal(0.05, 0.05, Some(2)), 0.05);
}

#[test]
fn low_frequency_equality_uses_collected_frequency() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE skew_frequency (v INT, KEY kv(v))")
        .unwrap();
    engine
        .execute("INSERT INTO skew_frequency VALUES (1),(1),(1),(1),(1),(1),(1),(1),(2),(NULL)")
        .unwrap();
    engine.execute("ANALYZE TABLE skew_frequency").unwrap();
    let schema = engine.db_for_tests().table("skew_frequency").unwrap();
    assert_eq!(estimate_eq_rows(&schema, "v", &Value::Int(2), 10.0), 1.0);
    assert_eq!(estimate_eq_rows(&schema, "v", &Value::Int(1), 10.0), 8.0);
}

#[test]
fn low_frequency_sample_absence_and_collation_estimates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE freq_collation (v VARCHAR(32) COLLATE utf8mb4_0900_ai_ci, KEY kv(v))",
        )
        .unwrap();
    engine.execute("INSERT INTO freq_collation VALUES ('hot'),('hot'),('hot'),('hot'),('hot'),('hot'),('rare'),('RARE'),(NULL)").unwrap();
    engine.execute("ANALYZE TABLE freq_collation").unwrap();
    let mut schema = (*engine.db_for_tests().table("freq_collation").unwrap()).clone();
    assert_eq!(
        estimate_eq_rows(&schema, "v", &Value::String("Rare".into()), 9.0),
        2.0
    );
    assert_eq!(
        estimate_eq_rows(&schema, "v", &Value::String("absent".into()), 9.0),
        0.0
    );
    schema.columns[0].collation = Some("utf8mb4_bin".into());
    assert_eq!(
        estimate_eq_rows(&schema, "v", &Value::String("rare".into()), 9.0),
        1.0
    );
    let stats = schema.stats.columns.get_mut("v").unwrap();
    stats.frequency.source = crate::storage::FrequencySource::Sample;
    // Nine sampled rows expanded to a population of 900.
    stats.frequency.sample_rows = 9;
    for f in &mut stats.top_values {
        f.count *= 100;
    }
    stats.null_count *= 100;
    schema.stats.row_count = 900;
    assert_eq!(
        estimate_eq_rows(&schema, "v", &Value::String("rare".into()), 900.0),
        100.0
    );
    assert_eq!(
        estimate_eq_rows(&schema, "v", &Value::String("absent".into()), 900.0),
        100.0
    );
}

#[test]
fn low_frequency_dense_domain_rejects_noninteger_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for (name, ty, values) in [
        ("text_domain", "VARCHAR(32)", "('key-a',1),('key-b',2),('key-c',2)"),
        ("numeric_text_domain", "VARCHAR(32)", "('01',1),('001',2),('1',2)"),
        ("decimal_domain", "DECIMAL(8,2)", "(0.10,1),(0.20,2),(0.30,2)"),
        ("float_domain", "DOUBLE", "(0.1,1),(0.2,2),(0.3,2)"),
        ("temporal_domain", "DATETIME(6)", "('2026-01-01 00:00:00.000001',1),('2026-01-01 00:00:00.000002',2),('2026-01-01 00:00:00.000003',2)"),
    ] {
        engine.execute(&format!("CREATE TABLE {name} (id {ty} PRIMARY KEY, flag INT, KEY f(flag,id))")).unwrap();
        engine.execute(&format!("INSERT INTO {name} VALUES {values}")).unwrap();
        engine.execute(&format!("ANALYZE TABLE {name}")).unwrap();
        let table = engine.db_for_tests().table(name).unwrap();
        let filter = parsed_filter(&format!("SELECT * FROM {name} WHERE flag=1"));
        assert!(numeric_domain_cardinality_cap(&table, "id").is_none(), "{ty}");
        assert!(filtered_key_domain_for_schema(&table, &filter, "id", 1.0).is_none(), "{ty}");
        let Statement::Select(select) = parse_statement(&format!("SELECT * FROM {name} WHERE flag=1")).unwrap() else { unreachable!() };
        let plan = build_physical_plan(engine.db_for_tests(), &select).unwrap();
        assert!(key_numeric_min_max_in_plan(&plan, "id").is_none(), "{ty}");
        let Statement::Select(unfiltered) = parse_statement(&format!("SELECT * FROM {name}")).unwrap() else { unreachable!() };
        let right = build_physical_plan(engine.db_for_tests(), &unfiltered).unwrap();
        let key = Expr::Identifier("id".into());
        assert!(estimate_equijoin_rows(&plan, &right, &[key.clone()], &[key], 1.0, 3.0) <= 1.0);
    }
}

#[test]
fn low_frequency_bigint_domain_cap_preserves_adjacent_large_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE big_domain (id BIGINT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO big_domain VALUES (9007199254740992),(9007199254740993)")
        .unwrap();
    engine.execute("ANALYZE TABLE big_domain").unwrap();
    let schema = engine.db_for_tests().table("big_domain").unwrap();
    assert_eq!(numeric_domain_cardinality_cap(&schema, "id"), Some(2));
    assert!(dense_integer_column_bounds(&schema, "id").is_none());
}

#[test]
fn composite_constant_frequency_caps_correlated_estimates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE freq_composite (id INT PRIMARY KEY, app VARCHAR(16), state VARCHAR(16), visible INT, KEY dispatch(app,state,id))").unwrap();
    // Preserve the statistics/cardinality; expression evaluation is not under test.
    let db = engine.db_for_tests();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.freq_composite",
        (1..=10_000)
            .map(|n| {
                vec![
                    Value::Int(n),
                    Value::String("worker".into()),
                    Value::String(if n == 1 { "rare" } else { "common" }.into()),
                    Value::Int(1),
                ]
            })
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    engine.execute("ANALYZE TABLE freq_composite").unwrap();
    let schema = engine.db_for_tests().table("freq_composite").unwrap();
    let index = schema
        .indexes
        .iter()
        .find(|i| i.name == "dispatch")
        .unwrap();
    for state in ["rare", "absent"] {
        let key = IndexKey::from(vec![
            KeyPart::String("worker".into()),
            KeyPart::String(state.into()),
        ]);
        let access = estimate_constant_index_lookup_rows(&schema, index, &key, false, 10000.0);
        assert!(
            access <= 1.0,
            "composite access must respect rare marginal: {state}: {access}"
        );
        let filter = parsed_filter(&format!(
            "SELECT * FROM freq_composite WHERE app='worker' AND state='{state}' AND visible=1"
        ));
        let rows = estimate_filter_rows(&schema, &filter, 10000.0);
        assert!(
            rows <= 1.0,
            "correlated estimate must respect rare marginal: {state}: {rows}"
        );
    }
}

#[test]
fn selective_composite_join_considers_multi_lookup_inner() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE jobs (id INT PRIMARY KEY, parent_id INT, app VARCHAR(16), state VARCHAR(16), KEY dispatch(app,state,parent_id), KEY parent_lookup(parent_id))").unwrap();
    engine
        .execute("CREATE TABLE parents (id INT PRIMARY KEY, enabled INT)")
        .unwrap();
    let db = engine.db_for_tests();
    let txn = db.begin_txn().unwrap();
    db.insert_rows(
        txn,
        "test.parents",
        (1..=10_000)
            .map(|n| vec![Value::Int(n), Value::Int(1)])
            .collect(),
    )
    .unwrap();
    db.insert_rows(
        txn,
        "test.jobs",
        (1..=10_000)
            .map(|n| {
                vec![
                    Value::Int(n),
                    Value::Int(n),
                    Value::String("worker".into()),
                    Value::String("done".into()),
                ]
            })
            .collect(),
    )
    .unwrap();
    db.commit_txn(txn.txn_id).unwrap();
    engine.execute("INSERT INTO jobs VALUES (10001,1,'worker','waiting'),(10002,2,'worker','waiting'),(10003,3,'worker','waiting'),(10004,1,'helper','running'),(10005,2,'unrelated','running'),(10006,3,'helper','done')").unwrap();
    engine.execute("ANALYZE TABLE jobs").unwrap();
    engine.execute("ANALYZE TABLE parents").unwrap();
    let sql = "SELECT DISTINCT m.parent_id FROM jobs m JOIN parents s ON s.id=m.parent_id WHERE m.app='worker' AND m.state='waiting' AND s.enabled=1 AND NOT EXISTS (SELECT 1 FROM jobs a FORCE INDEX(dispatch) WHERE a.parent_id=m.parent_id AND a.app IN ('worker','helper') AND a.state IN ('running','stopping')) LIMIT 32";
    let Statement::Select(select) = parse_statement(sql).unwrap() else {
        unreachable!()
    };
    let plan = plan_select(engine.db_for_tests(), &select)
        .unwrap()
        .physical;
    let explained = format!("{:?}", explain_rows(&plan));
    assert!(
        plan.any(|node| matches!(
            node,
            PhysicalPlan::IndexNestedLoopJoin {
                join_type: JoinType::Inner,
                ..
            }
        )),
        "rare outer must use indexed parent lookup: {explained}"
    );
    assert!(
        plan.any(|node| matches!(
            node,
            PhysicalPlan::IndexNestedLoopJoin {
                join_type: JoinType::Anti,
                ..
            }
        )),
        "multi lookup must allow indexed anti join: {explained}"
    );
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    let rows = result.rows;
    assert_eq!(
        rows.len(),
        2,
        "inner residual must retain both IN predicates: {rows:?}"
    );
    assert!(rows.contains(&vec![Value::Int(2)]));
    assert!(rows.contains(&vec![Value::Int(3)]));
}

#[test]
fn low_frequency_constant_lookup_cost_uses_each_key_frequency() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE freq_cost (id INT PRIMARY KEY, v INT, KEY kv(v,id))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO freq_cost VALUES (1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,2),(8,3),(9,NULL)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE freq_cost").unwrap();
    let schema = engine.db_for_tests().table("freq_cost").unwrap();
    let index = schema.indexes.iter().find(|i| i.name == "kv").unwrap();
    let key = |v| IndexKey::from(vec![KeyPart::Int(v)]);
    assert_eq!(
        estimate_constant_index_lookup_rows(&schema, index, &key(2), false, 9.0),
        1.0
    );
    assert_eq!(
        estimate_constant_index_lookup_rows(&schema, index, &key(1), false, 9.0),
        6.0
    );
    assert_eq!(
        estimate_constant_index_multi_lookup_rows(&schema, index, &[key(1), key(2)], false, 9.0),
        7.0
    );
}

#[test]
fn top_value_candidate_cannot_lower_uniform_eq_estimate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE eq_floor (v INT)").unwrap();
    engine.execute("INSERT INTO eq_floor VALUES (1)").unwrap();
    engine.execute("ANALYZE TABLE eq_floor").unwrap();

    let mut schema = (*engine.db_for_tests().table("eq_floor").unwrap()).clone();
    schema.stats.row_count = 150_000;
    {
        let stats = schema.stats.columns.get_mut("v").unwrap();
        stats.null_count = 0;
        stats.distinct_count = 150;
        stats.frequency = Default::default();
        stats.top_values = vec![crate::storage::ValueFrequency {
            value: Value::Int(7),
            count: 8,
        }];
    }

    assert_eq!(
        estimate_eq_rows(&schema, "v", &Value::Int(7), 150_000.0),
        1_000.0
    );

    schema
        .stats
        .columns
        .get_mut("v")
        .unwrap()
        .frequency
        .count_error = Some(12);
    assert_eq!(
        estimate_eq_rows(&schema, "v", &Value::Int(7), 150_000.0),
        20.0
    );
    schema
        .stats
        .columns
        .get_mut("v")
        .unwrap()
        .frequency
        .count_error = None;
    schema.stats.columns.get_mut("v").unwrap().top_values[0].count = 5_000;
    assert_eq!(
        estimate_eq_rows(&schema, "v", &Value::Int(7), 150_000.0),
        5_000.0
    );
    let residual = estimate_eq_rows(&schema, "v", &Value::Int(9), 150_000.0);
    let expected_residual = (150_000.0 - 5_000.0) / 149.0;
    assert!(
        (residual - expected_residual).abs() < 1e-9,
        "non-MCV estimate must retain residual-mass/remaining-NDV semantics: \
         actual={residual} expected={expected_residual}"
    );
}

#[test]
fn decimal_top_value_matches_integer_literal_by_sql_numeric_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE decimal_stats (gmt_offset DECIMAL(5,2))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO decimal_stats VALUES \
             (-5.00),(-5.00),(-5.00),(-5.00),(-5.00),(-5.00), \
             (-6.00),(-6.00),(-6.00),(-6.00),(-6.00),(-6.00)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE decimal_stats").unwrap();

    let schema = engine.db_for_tests().table("decimal_stats").unwrap();
    let estimate = estimate_eq_rows(&schema, "gmt_offset", &Value::Int(-5), 12.0);
    assert_eq!(
        estimate, 6.0,
        "integer SQL literal must hit the DECIMAL TOP-K frequency"
    );
    assert_eq!(
        filter_rows_source(
            &schema,
            &parsed_filter("SELECT * FROM decimal_stats WHERE gmt_offset = -5")
        ),
        crate::engine::cbo::SelectivitySource::Histogram,
        "the same semantic TOP-K match must be reported in estimate provenance"
    );
}

#[test]
fn literal_assignment_dnf_uses_post_pushdown_domain() {
    let expr = parsed_filter(
        "SELECT 1 WHERE \
         (left_rel.code = 'E' AND right_rel.code = 'W') OR \
         (left_rel.code = 'W' AND right_rel.code = 'E')",
    );
    assert_eq!(literal_assignment_dnf_selectivity(&expr), Some(0.5));

    let ordinary_join = parsed_filter("SELECT 1 WHERE left_rel.id = right_rel.id");
    assert_eq!(literal_assignment_dnf_selectivity(&ordinary_join), None);
}

fn outer_join_tables(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE oj_a (id INT PRIMARY KEY, a_val INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE oj_b (id INT PRIMARY KEY, b_val INT)")
        .unwrap();
}

#[test]
fn left_join_with_null_rejecting_where_becomes_inner() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    outer_join_tables(&engine);
    let db = engine.db_for_tests();
    // Comparison on the null-supplied side → null-rejecting → INNER.
    let s = normalized_select(
        db,
        "SELECT a_val FROM oj_a LEFT JOIN oj_b ON oj_a.id = oj_b.id WHERE oj_b.b_val > 5",
    );
    assert_eq!(s.joins[0].join_type, JoinType::Inner);

    // IS NOT NULL on the null-supplied side also rejects.
    let s = normalized_select(
        db,
        "SELECT a_val FROM oj_a LEFT JOIN oj_b ON oj_a.id = oj_b.id WHERE oj_b.b_val IS NOT NULL",
    );
    assert_eq!(s.joins[0].join_type, JoinType::Inner);

    // Equality against another table's column (the q93 shape) rejects too.
    let s = normalized_select(
        db,
        "SELECT a_val FROM oj_a LEFT JOIN oj_b ON oj_a.id = oj_b.id WHERE oj_b.b_val = oj_a.a_val",
    );
    assert_eq!(s.joins[0].join_type, JoinType::Inner);
}

#[test]
fn left_join_without_null_rejecting_where_stays_left() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    outer_join_tables(&engine);
    let db = engine.db_for_tests();

    // No WHERE → must stay LEFT.
    let s = normalized_select(
        db,
        "SELECT a_val FROM oj_a LEFT JOIN oj_b ON oj_a.id = oj_b.id",
    );
    assert_eq!(s.joins[0].join_type, JoinType::Left);

    // Predicate only on the preserved side → not null-rejecting on oj_b.
    let s = normalized_select(
        db,
        "SELECT a_val FROM oj_a LEFT JOIN oj_b ON oj_a.id = oj_b.id WHERE oj_a.a_val > 5",
    );
    assert_eq!(s.joins[0].join_type, JoinType::Left);

    // `IS NULL` on the null side is TRUE under null-extension → keep LEFT.
    let s = normalized_select(
        db,
        "SELECT a_val FROM oj_a LEFT JOIN oj_b ON oj_a.id = oj_b.id WHERE oj_b.b_val IS NULL",
    );
    assert_eq!(s.joins[0].join_type, JoinType::Left);

    // COALESCE is null-tolerant → not provably rejecting → keep LEFT.
    let s = normalized_select(
        db,
        "SELECT a_val FROM oj_a LEFT JOIN oj_b ON oj_a.id = oj_b.id \
         WHERE COALESCE(oj_b.b_val, 0) = 0",
    );
    assert_eq!(s.joins[0].join_type, JoinType::Left);

    // OR with only one branch rejecting → not rejecting → keep LEFT.
    let s = normalized_select(
        db,
        "SELECT a_val FROM oj_a LEFT JOIN oj_b ON oj_a.id = oj_b.id \
         WHERE oj_b.b_val > 5 OR oj_a.a_val > 5",
    );
    assert_eq!(s.joins[0].join_type, JoinType::Left);
}

#[test]
fn outer_join_conversion_recurses_into_derived_subquery() {
    // Mirrors TPC-DS q93: the LEFT join lives inside a derived table and a
    // null-rejecting WHERE predicate elsewhere in that subquery converts it.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    outer_join_tables(&engine);
    let db = engine.db_for_tests();
    let s = normalized_select(
        db,
        "SELECT t.a_val FROM (\
             SELECT oj_a.a_val a_val, oj_b.b_val b_val \
             FROM oj_a LEFT JOIN oj_b ON oj_a.id = oj_b.id \
             WHERE oj_b.b_val > 0\
         ) t",
    );
    let derived = s
        .derived
        .as_ref()
        .and_then(crate::parser::SubQuery::as_select)
        .expect("derived select");
    assert_eq!(derived.joins[0].join_type, JoinType::Inner);
}

#[test]
fn outer_join_conversion_gate_off_keeps_left_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    outer_join_tables(&engine);
    let db = engine.db_for_tests();
    crate::util::env::with_vars(&[("MYDB_OUTER_JOIN_SIMPLIFY", Some("off"))], || {
        let s = normalized_select(
            db,
            "SELECT a_val FROM oj_a LEFT JOIN oj_b ON oj_a.id = oj_b.id WHERE oj_b.b_val > 5",
        );
        assert_eq!(s.joins[0].join_type, JoinType::Left);
    });
}

#[test]
fn sip_pushes_one_hop_dimension_filter_through_comma_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE date_dim (d_date_sk INT PRIMARY KEY, d_year INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE fact_sales (sold_date_sk INT, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO date_dim VALUES (1,1999),(2,2000),(3,2001)")
        .unwrap();
    // The fact must be larger than the dimension for SIP to fire — the
    // whole point is shrinking a big fact via a small filtered dim.
    engine
        .execute(
            "INSERT INTO fact_sales VALUES \
             (1,10),(2,20),(3,30),(1,40),(2,50),(3,60),(1,70),(2,80),(3,90)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE date_dim").unwrap();
    engine.execute("ANALYZE TABLE fact_sales").unwrap();
    let db = engine.db_for_tests();
    let Statement::Select(mut select) = parse_statement(
        "SELECT COUNT(*) \
         FROM fact_sales f, date_dim d \
         WHERE f.sold_date_sk = d.d_date_sk \
           AND d.d_year IN (1999, 2000)",
    )
    .unwrap() else {
        panic!("expected SELECT");
    };
    let sip_tables = collect_sip_tables(db, &select).unwrap_or_else(|| {
        panic!(
            "SIP tables; from={:?} joins={:?}",
            select.from, select.joins
        )
    });
    assert_eq!(
        sip_tables
            .iter()
            .map(|t| (t.alias.as_str(), t.table_name.as_str()))
            .collect::<Vec<_>>(),
        vec![("f", "fact_sales"), ("d", "date_dim")]
    );
    assert!(
        sip_tables.iter().all(|t| t.row_count <= 100_000),
        "unexpected SIP row counts: {:?}",
        sip_tables
            .iter()
            .map(|t| (t.alias.as_str(), t.row_count))
            .collect::<Vec<_>>()
    );

    propagate_sideways_dimension_filters(db, &mut select);
    let mut conjuncts = Vec::new();
    split_conjuncts(select.selection.as_ref().expect("WHERE"), &mut conjuncts);
    let pushed = conjuncts.iter().any(|expr| match expr {
        Expr::InSubquery {
            expr,
            subquery,
            negated: false,
        } => {
            matches!(
                expr.as_ref(),
                Expr::CompoundIdentifier(parts)
                    if parts == &vec!["f".to_string(), "sold_date_sk".to_string()]
            ) && subquery
                .from
                .first()
                .is_some_and(|t| t.name.eq_ignore_ascii_case("date_dim"))
                && matches!(
                    subquery.projection.first(),
                    Some(SelectItem::Expr {
                        expr: Expr::CompoundIdentifier(parts),
                        ..
                    }) if parts == &vec!["d".to_string(), "d_date_sk".to_string()]
                )
        }
        _ => false,
    });
    assert!(
        pushed,
        "SIP should add fact date-key IN-subquery, got {:?}",
        select.selection
    );
}

#[test]
fn sip_skips_low_ndv_filter_that_keeps_entire_dimension() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE store_dim (store_sk INT PRIMARY KEY, gmt_offset INT, \
             INDEX idx_store_offset(gmt_offset))",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE sales_fact (store_sk INT, amount INT)")
        .unwrap();
    engine
        .execute("INSERT INTO store_dim VALUES (1,-5),(2,-5),(3,-5)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO sales_fact VALUES \
             (1,10),(2,20),(3,30),(1,40),(2,50),(3,60),(1,70),(2,80),(3,90)",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE store_dim").unwrap();
    engine.execute("ANALYZE TABLE sales_fact").unwrap();
    let Statement::Select(mut select) = parse_statement(
        "SELECT SUM(amount) \
         FROM sales_fact f, store_dim s \
         WHERE f.store_sk = s.store_sk AND s.gmt_offset = -5",
    )
    .unwrap() else {
        panic!("expected SELECT");
    };

    propagate_sideways_dimension_filters(engine.db_for_tests(), &mut select);
    let mut conjuncts = Vec::new();
    split_conjuncts(select.selection.as_ref().expect("WHERE"), &mut conjuncts);
    assert!(
        !conjuncts.iter().any(|expr| matches!(
            expr,
            Expr::InSubquery {
                expr,
                negated: false,
                ..
            } if matches!(
                expr.as_ref(),
                Expr::CompoundIdentifier(parts)
                    if parts == &vec!["f".to_string(), "store_sk".to_string()]
            )
        )),
        "a source filter estimated at 100% must not add a redundant fact-side Apply: {:?}",
        select.selection
    );
}

#[test]
fn sip_does_not_propagate_through_large_transit_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE date_dim (d_date_sk INT PRIMARY KEY, d_year INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE fact_sales (sold_date_sk INT, cust_sk INT, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE customer (cust_sk INT PRIMARY KEY, name VARCHAR(20))")
        .unwrap();
    engine
        .execute("INSERT INTO date_dim VALUES (1,1999),(2,2000)")
        .unwrap();
    // Fact larger than both the date dimension and customer so the
    // one-hop date→fact SIP push clears the "target must be larger than
    // source" guard; customer stays small (and is blocked by the transit
    // cap regardless).
    engine
        .execute(
            "INSERT INTO fact_sales VALUES \
             (1,10,100),(2,20,200),(1,10,300),(2,20,400),(1,20,500),(2,10,600)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO customer VALUES (10,'a'),(20,'b')")
        .unwrap();
    engine.execute("ANALYZE TABLE date_dim").unwrap();
    engine.execute("ANALYZE TABLE fact_sales").unwrap();
    engine.execute("ANALYZE TABLE customer").unwrap();
    let db = engine.db_for_tests();
    let Statement::Select(mut select) = parse_statement(
        "SELECT c.name \
         FROM customer c, fact_sales f, date_dim d \
         WHERE c.cust_sk = f.cust_sk \
           AND f.sold_date_sk = d.d_date_sk \
           AND d.d_year = 1999",
    )
    .unwrap() else {
        panic!("expected SELECT");
    };

    crate::util::env::with_vars(&[("MYDB_SIP_MAX_TRANSIT_ROWS", Some("1"))], || {
        propagate_sideways_dimension_filters(db, &mut select);
    });
    let mut conjuncts = Vec::new();
    split_conjuncts(select.selection.as_ref().expect("WHERE"), &mut conjuncts);
    let has_fact_date = conjuncts.iter().any(|expr| {
        matches!(
            expr,
            Expr::InSubquery {
                expr,
                negated: false,
                ..
            } if matches!(
                expr.as_ref(),
                Expr::CompoundIdentifier(parts)
                    if parts == &vec!["f".to_string(), "sold_date_sk".to_string()]
            )
        )
    });
    let has_customer = conjuncts.iter().any(|expr| {
        matches!(
            expr,
            Expr::InSubquery {
                expr,
                negated: false,
                ..
            } if matches!(
                expr.as_ref(),
                Expr::CompoundIdentifier(parts)
                    if parts == &vec!["c".to_string(), "cust_sk".to_string()]
            )
        )
    });
    assert!(has_fact_date, "one-hop date→fact SIP should remain");
    assert!(
        !has_customer,
        "SIP must not continue through a transit table above the cap: {:?}",
        select.selection
    );
}

#[test]
fn trimmed_zero_date_stats_keep_bounded_date_range_estimate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE calendar (id INT PRIMARY KEY, d DATE, INDEX idx_d(d))")
        .unwrap();
    engine
        .execute("INSERT INTO calendar VALUES (0, '0000-00-00')")
        .unwrap();
    let start = chrono::NaiveDate::from_ymd_opt(1999, 1, 1).unwrap();
    for id in 1..=365 {
        let d = start + chrono::Duration::days((id - 1) as i64);
        engine
            .execute(&format!(
                "INSERT INTO calendar VALUES ({id}, '{}')",
                d.format("%Y-%m-%d")
            ))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE calendar").unwrap();

    let mut table = (*engine.db_for_tests().table("calendar").unwrap()).clone();
    let d_stats = table.stats.columns.get_mut("d").unwrap();
    assert_eq!(d_stats.min, Some(Value::Date(i32::MIN)));
    d_stats.top_values.clear();
    d_stats.histogram.clear();

    let low = Value::String("1999-05-20".to_string());
    let high = Value::String("1999-07-19".to_string());
    let estimated = estimate_range_rows_for_column(
        &table,
        "d",
        Some((BinaryOp::GtEq, &low)),
        Some((BinaryOp::LtEq, &high)),
        table.stats.row_count as f64,
    );

    assert!(
        (55.0..=67.0).contains(&estimated),
        "trimmed zero-date stats should estimate the 61-day range near reality, got {estimated}"
    );
}

#[test]
fn timestamp_stats_coerce_folded_date_bounds_into_storage_domain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE timestamp_stats_t (\
             id INT PRIMARY KEY, ts TIMESTAMP NOT NULL, INDEX idx_ts(ts))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO timestamp_stats_t VALUES \
             (1,'2023-01-01 00:00:00'),(2,'2023-11-01 00:00:00'),\
             (3,'2023-12-31 00:00:00'),(4,'2024-12-31 00:00:00')",
        )
        .unwrap();
    engine.execute("ANALYZE TABLE timestamp_stats_t").unwrap();
    let table = engine.db_for_tests().table("timestamp_stats_t").unwrap();

    let low_date = Value::Date(crate::storage::parse_date_to_days_bytes(b"2023-11-01").unwrap());
    let high_date = Value::Date(crate::storage::parse_date_to_days_bytes(b"2023-12-31").unwrap());
    let Value::Timestamp(low_timestamp) = crate::engine::expr::coerce_comparison_constant_for_type(
        &low_date,
        &crate::storage::DataTypeKind::Timestamp { fsp: 0 },
    )
    .unwrap() else {
        panic!("low bound did not coerce to timestamp")
    };
    let Value::Timestamp(high_timestamp) =
        crate::engine::expr::coerce_comparison_constant_for_type(
            &high_date,
            &crate::storage::DataTypeKind::Timestamp { fsp: 0 },
        )
        .unwrap()
    else {
        panic!("high bound did not coerce to timestamp")
    };
    let low_timestamp = Value::Timestamp(low_timestamp);
    let high_timestamp = Value::Timestamp(high_timestamp);
    let from_folded_dates = estimate_range_rows_for_column(
        &table,
        "ts",
        Some((BinaryOp::GtEq, &low_date)),
        Some((BinaryOp::LtEq, &high_date)),
        table.stats.row_count as f64,
    );
    let from_typed_timestamps = estimate_range_rows_for_column(
        &table,
        "ts",
        Some((BinaryOp::GtEq, &low_timestamp)),
        Some((BinaryOp::LtEq, &high_timestamp)),
        table.stats.row_count as f64,
    );
    assert_eq!(from_folded_dates, from_typed_timestamps);
    assert!(from_folded_dates > 0.0);
}

#[test]
fn range_index_estimate_resolves_deferred_param_bound() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY,\
             o_orderdate DATE NOT NULL,\
             INDEX idx_orders_orderdate(o_orderdate)\
             )",
        )
        .unwrap();
    for id in 1..=100 {
        engine
            .execute(&format!(
                "INSERT INTO orders VALUES ({id}, '1993-01-{:02}')",
                (id % 28) + 1
            ))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE orders").unwrap();

    let table = engine.db_for_tests().table("orders").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|idx| idx.name == "idx_orders_orderdate")
        .unwrap();
    let high = Some(IndexBound {
        key: IndexKey::from(vec![KeyPart::Param {
            idx: 0,
            target: ParamTarget::Date,
        }]),
        inclusive: false,
    });

    let estimated = with_params(vec![Value::String("1990-03-15".to_string())], || {
        estimate_index_range_access_rows(&table, index, &None, &high, 100.0)
    });

    assert!(
        estimated <= 1.0,
        "parameterized range bound below the column minimum should estimate near-empty access, got {estimated}"
    );
}

#[test]
fn parameterized_tpch_like_join_plans_from_empty_orders_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE customer (\
             c_custkey INT PRIMARY KEY,\
             c_mktsegment VARCHAR(16) NOT NULL\
             )",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY,\
             o_custkey INT NOT NULL,\
             o_orderdate DATE NOT NULL,\
             o_shippriority INT NOT NULL,\
             INDEX idx_orders_orderdate(o_orderdate),\
             INDEX idx_orders_custkey(o_custkey)\
             )",
        )
        .unwrap();
    for id in 1..=100 {
        let segment = if id % 2 == 0 {
            "BUILDING"
        } else {
            "AUTOMOBILE"
        };
        engine
            .execute(&format!("INSERT INTO customer VALUES ({id}, '{segment}')"))
            .unwrap();
        engine
            .execute(&format!(
                "INSERT INTO orders VALUES ({id}, {id}, '1993-01-{:02}', 0)",
                (id % 28) + 1
            ))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE customer").unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();

    let mut statement = parse_statement(
        "SELECT count(*) \
         FROM customer \
         INNER JOIN orders ON c_custkey = o_custkey \
         WHERE c_mktsegment = 'BUILDING' \
           AND o_orderdate < ? \
         GROUP BY o_orderdate, o_shippriority \
         ORDER BY o_orderdate \
         LIMIT 10",
    )
    .unwrap();
    let mut next = 0;
    number_statement_placeholders(&mut statement, &mut next);
    let Statement::Select(select) = statement else {
        panic!("expected SELECT");
    };

    let planned = with_params(vec![Value::String("1990-03-15".to_string())], || {
        plan_select(engine.db_for_tests(), &select)
    })
    .unwrap();
    let debug = format!("{:?}", planned.physical);

    assert!(
        debug.contains("IndexRangeScan")
            && debug.contains("orders")
            && debug.contains("idx_orders_orderdate"),
        "parameterized empty orders date range should drive the join from idx_orders_orderdate, got {debug}"
    );
}

#[test]
fn semi_anti_match_rate_caps_filtered_inner_ndv_by_plan_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE existence_outer (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE existence_inner (\
             id INT PRIMARY KEY, keep_row INT NOT NULL, \
             INDEX idx_existence_keep (keep_row))",
        )
        .unwrap();

    let outer_rows = (1..=100)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(",");
    let inner_rows = (1..=100)
        .map(|id| format!("({id},{})", usize::from(id <= 5)))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO existence_outer VALUES {outer_rows}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO existence_inner VALUES {inner_rows}"))
        .unwrap();
    engine.execute("ANALYZE TABLE existence_outer").unwrap();
    engine.execute("ANALYZE TABLE existence_inner").unwrap();

    let db = engine.db_for_tests();
    let scan = |sql| {
        build_physical_plan(
            db,
            &match parse_statement(sql).unwrap() {
                Statement::Select(select) => select,
                _ => unreachable!(),
            },
        )
        .unwrap()
    };
    let outer = scan("SELECT * FROM existence_outer");
    let filtered_inner = scan("SELECT * FROM existence_inner WHERE keep_row = 1");
    let filtered_rows = estimated_rows(&filtered_inner);
    assert!(
        filtered_rows < estimated_rows(&outer),
        "fixture must retain a selective inner plan, got {filtered_rows} rows: {filtered_inner:?}"
    );

    let match_rate = estimate_join_existence_match_rate(
        &outer,
        &filtered_inner,
        &[Expr::Identifier("id".into())],
        &[Expr::Identifier("id".into())],
    );
    let maximum_rate = filtered_rows / estimated_rows(&outer);
    assert!(
        match_rate <= maximum_rate + f64::EPSILON,
        "a filtered inner cannot expose more distinct join keys than rows: \
         rate={match_rate}, maximum={maximum_rate}, inner={filtered_inner:?}"
    );
}

/// Regression for the TPC-H Q5 hang: when the join key has low NDV
/// relative to both sides (the textbook many-to-many shape, e.g.
/// `customer.c_nationkey = supplier.s_nationkey`), the equijoin row
/// estimate must reflect the Cartesian-style blow-up of
/// `|L|*|R|/ndv`. Capping silently at `max(L,R)*4` flattens the
/// estimate by orders of magnitude, lets the join-order DP pick the
/// blown-up intermediate as a hash-join build side, and stalls the
/// executor in grace-hash NLJ fallback (O(N*M) with no progress).
#[test]
fn equijoin_row_estimate_does_not_cap_low_ndv_many_to_many() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE customer (\
             c_custkey INT PRIMARY KEY, \
             c_nationkey INT NOT NULL, \
             INDEX idx_customer_nation(c_nationkey))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE supplier (\
             s_suppkey INT PRIMARY KEY, \
             s_nationkey INT NOT NULL, \
             INDEX idx_supplier_nation(s_nationkey))",
        )
        .unwrap();
    // 5 nationkeys, 600 customers, 40 suppliers — NDV(nationkey)=5
    // on both sides. Textbook estimate is 600*40/5 = 4800 join rows;
    // the pre-fix cap of `max(L,R)*4 = 2400` cut that in half.
    let customers = (1..=600)
        .map(|i| format!("({i},{})", (i % 5) + 1))
        .collect::<Vec<_>>()
        .join(",");
    let suppliers = (1..=40)
        .map(|i| format!("({i},{})", (i % 5) + 1))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO customer VALUES {customers}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO supplier VALUES {suppliers}"))
        .unwrap();
    engine.execute("ANALYZE TABLE customer").unwrap();
    engine.execute("ANALYZE TABLE supplier").unwrap();

    let db = engine.db_for_tests();
    let customer_scan = build_physical_plan(
        db,
        &match parse_statement("SELECT * FROM customer").unwrap() {
            Statement::Select(s) => s,
            _ => unreachable!(),
        },
    )
    .unwrap();
    let supplier_scan = build_physical_plan(
        db,
        &match parse_statement("SELECT * FROM supplier").unwrap() {
            Statement::Select(s) => s,
            _ => unreachable!(),
        },
    )
    .unwrap();

    let left_keys = vec![Expr::Identifier("c_nationkey".into())];
    let right_keys = vec![Expr::Identifier("s_nationkey".into())];
    let est = estimate_equijoin_rows(
        &customer_scan,
        &supplier_scan,
        &left_keys,
        &right_keys,
        600.0,
        40.0,
    );

    // Pre-fix cap returned ≤ 2400. Post-fix returns ≈ 4800
    // (|L|*|R|/max(NDV)). Anything materially above the old cap
    // is enough to keep the join-order DP from picking the
    // many-to-many intermediate as a hash-join build side.
    assert!(
        est >= 4000.0,
        "single-key low-NDV equijoin must estimate near |L|*|R|/ndv, got {est}"
    );
}

#[test]
fn sip_keyset_prefilter_makes_matching_unique_join_lossless() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE region_sip_ndv (\
             r_regionkey INT PRIMARY KEY, r_name VARCHAR(16) NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE nation_sip_ndv (\
             n_nationkey INT PRIMARY KEY, n_regionkey INT NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE customer_sip_ndv (\
             c_custkey INT PRIMARY KEY, c_nationkey INT NOT NULL, \
             INDEX idx_customer_sip_nation(c_nationkey))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO region_sip_ndv VALUES \
             (1,'ASIA'),(2,'EUROPE'),(3,'AFRICA'),(4,'AMERICA'),(5,'MIDDLE EAST')",
        )
        .unwrap();
    let nations = (1..=25)
        .map(|nation| format!("({nation},{})", ((nation - 1) / 5) + 1))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO nation_sip_ndv VALUES {nations}"))
        .unwrap();
    let customers = (1..=100)
        .map(|customer| format!("({customer},{})", ((customer - 1) % 25) + 1))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO customer_sip_ndv VALUES {customers}"))
        .unwrap();
    engine.execute("ANALYZE TABLE region_sip_ndv").unwrap();
    engine.execute("ANALYZE TABLE nation_sip_ndv").unwrap();
    engine.execute("ANALYZE TABLE customer_sip_ndv").unwrap();

    let db = engine.db_for_tests();
    let scan = |sql| {
        build_physical_plan(
            db,
            &match parse_statement(sql).unwrap() {
                Statement::Select(select) => select,
                _ => unreachable!(),
            },
        )
        .unwrap()
    };
    let nation = scan(
        "SELECT * FROM nation_sip_ndv n \
         WHERE n.n_regionkey IN (\
             SELECT r.r_regionkey FROM region_sip_ndv r WHERE r.r_name='ASIA')",
    );
    let region = scan("SELECT * FROM region_sip_ndv r WHERE r.r_name='ASIA'");
    assert!(
        (estimated_rows(&nation) - 5.0).abs() < 1e-9,
        "one key out of five should retain exactly five nation rows, plan={nation:?}"
    );
    let joined = estimate_equijoin_rows(
        &nation,
        &region,
        &[Expr::CompoundIdentifier(vec![
            "n".into(),
            "n_regionkey".into(),
        ])],
        &[Expr::CompoundIdentifier(vec![
            "r".into(),
            "r_regionkey".into(),
        ])],
        estimated_rows(&nation),
        estimated_rows(&region),
    );
    assert!(
        (joined - estimated_rows(&nation)).abs() < 1e-9,
        "joining the same unique-key source must not charge the SIP keyset twice: joined={joined}, nation={}",
        estimated_rows(&nation)
    );
    let region_filter = Expr::Binary {
        left: Box::new(Expr::CompoundIdentifier(vec!["r".into(), "r_name".into()])),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(crate::parser::Literal::String("ASIA".into()))),
    };
    let inlj_condition = Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::CompoundIdentifier(vec![
                "n".into(),
                "n_regionkey".into(),
            ])),
            op: BinaryOp::Eq,
            right: Box::new(Expr::CompoundIdentifier(vec![
                "r".into(),
                "r_regionkey".into(),
            ])),
        }),
        op: BinaryOp::And,
        right: Box::new(region_filter),
    };
    let region_key = Expr::CompoundIdentifier(vec!["r".into(), "r_regionkey".into()]);
    let (region_table, _) = unique_scan_for_key(&region, &region_key).expect("unique region scan");
    let inlj_joined = apply_inlj_right_only_residual_selectivity(
        estimated_rows(&nation),
        Some(&inlj_condition),
        &nation,
        &region,
        region_table,
        JoinType::Inner,
        estimated_rows(&nation),
    );
    assert!(
        (inlj_joined - estimated_rows(&nation)).abs() < 1e-9,
        "INLJ must mirror hash-join cardinality when its unique-side residual is already represented by the SIP keyset: joined={inlj_joined}, nation={nation:?}, region={region:?}"
    );

    let customer = scan(
        "SELECT * FROM customer_sip_ndv c \
         WHERE c.c_nationkey IN (\
             SELECT n.n_nationkey \
             FROM region_sip_ndv r \
             JOIN nation_sip_ndv n ON n.n_regionkey=r.r_regionkey \
             WHERE r.r_name='ASIA')",
    );
    let nation_region = scan(
        "SELECT * FROM nation_sip_ndv n \
         JOIN region_sip_ndv r ON n.n_regionkey=r.r_regionkey \
         WHERE r.r_name='ASIA'",
    );
    let customer_key = Expr::CompoundIdentifier(vec!["c".into(), "c_nationkey".into()]);
    let nation_key = Expr::CompoundIdentifier(vec!["n".into(), "n_nationkey".into()]);
    assert!(
        keyset_prefilter_guarantees_unique_join(
            &customer,
            &nation_region,
            &customer_key,
            &nation_key,
        ),
        "multi-hop SIP keyset should prove the nation-region subtree key-preserving: customer={customer:?}, nation_region={nation_region:?}"
    );
    let customer_joined = estimate_equijoin_rows(
        &customer,
        &nation_region,
        std::slice::from_ref(&customer_key),
        std::slice::from_ref(&nation_key),
        estimated_rows(&customer),
        estimated_rows(&nation_region),
    );
    assert!(
        (customer_joined - estimated_rows(&customer)).abs() < 1e-9,
        "multi-hop SIP keyset must not be charged again: joined={customer_joined}, customer={}",
        estimated_rows(&customer)
    );

    // The join enumerator also builds unaliased variants whose extracted
    // keys are plain Identifiers. A plain `n_nationkey` must not bind to
    // the first scan in the subtree (region) merely because it is
    // unqualified, and a non-unique FK such as nation.n_regionkey must
    // not be mistaken for a key-preserving unique side.
    let customer_unaliased = scan(
        "SELECT * FROM customer_sip_ndv \
         WHERE customer_sip_ndv.c_nationkey IN (\
             SELECT nation_sip_ndv.n_nationkey \
             FROM region_sip_ndv \
             JOIN nation_sip_ndv \
               ON nation_sip_ndv.n_regionkey=region_sip_ndv.r_regionkey \
             WHERE region_sip_ndv.r_name='ASIA')",
    );
    let nation_region_unaliased = scan(
        "SELECT * FROM region_sip_ndv \
         JOIN nation_sip_ndv \
           ON region_sip_ndv.r_regionkey=nation_sip_ndv.n_regionkey \
         WHERE region_sip_ndv.r_name='ASIA'",
    );
    let unaliased_joined = estimate_equijoin_rows(
        &nation_region_unaliased,
        &customer_unaliased,
        &[Expr::Identifier("n_nationkey".into())],
        &[Expr::Identifier("c_nationkey".into())],
        estimated_rows(&nation_region_unaliased),
        estimated_rows(&customer_unaliased),
    );
    assert!(
        (unaliased_joined - estimated_rows(&customer_unaliased)).abs() < 1e-9,
        "unaliased unique-key subtree must preserve the SIP-filtered side: joined={unaliased_joined}, customer={}",
        estimated_rows(&customer_unaliased)
    );
}

#[test]
fn equijoin_estimate_counts_proven_equivalent_keys_once() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE customer_eq (\
             c_custkey INT PRIMARY KEY, c_nationkey INT NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE nation_eq (\
             n_nationkey INT PRIMARY KEY)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE supplier_eq (\
             s_suppkey INT PRIMARY KEY, s_nationkey INT NOT NULL)",
        )
        .unwrap();
    let customers = (1..=100)
        .map(|i| format!("({i}, {})", (i % 25) + 1))
        .collect::<Vec<_>>()
        .join(",");
    let nations = (1..=25)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    let suppliers = (1..=40)
        .map(|i| format!("({i}, {})", (i % 25) + 1))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO customer_eq VALUES {customers}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO nation_eq VALUES {nations}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO supplier_eq VALUES {suppliers}"))
        .unwrap();
    for table in ["customer_eq", "nation_eq", "supplier_eq"] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }

    let db = engine.db_for_tests();
    let scan = |sql| {
        build_physical_plan(
            db,
            &match parse_statement(sql).unwrap() {
                Statement::Select(select) => select,
                _ => unreachable!(),
            },
        )
        .unwrap()
    };
    let customer = scan("SELECT * FROM customer_eq");
    let nation = scan("SELECT * FROM nation_eq");
    let supplier = scan("SELECT * FROM supplier_eq");
    let left = PhysicalPlan::HashJoin {
        left: Box::new(customer),
        right: Box::new(nation),
        join_type: JoinType::Inner,
        left_keys: vec![Expr::Identifier("c_nationkey".into())],
        right_keys: vec![Expr::Identifier("n_nationkey".into())],
        null_safe: vec![false],
        build_side: BuildSide::Right,
        residual: None,
        output_projection: None,
        estimated_rows: 100.0,
        cost: crate::engine::cbo::Cost::default(),
    };
    let single = estimate_equijoin_rows(
        &left,
        &supplier,
        &[Expr::Identifier("c_nationkey".into())],
        &[Expr::Identifier("s_nationkey".into())],
        100.0,
        40.0,
    );
    let redundant = estimate_equijoin_rows(
        &left,
        &supplier,
        &[
            Expr::Identifier("c_nationkey".into()),
            Expr::Identifier("n_nationkey".into()),
        ],
        &[
            Expr::Identifier("s_nationkey".into()),
            Expr::Identifier("s_nationkey".into()),
        ],
        100.0,
        40.0,
    );
    assert!(
        (single - redundant).abs() < 1e-9,
        "a transitive duplicate equality must not multiply nation NDV twice: single={single}, redundant={redundant}"
    );
}

#[test]
fn equijoin_deduplicates_transitive_keys_before_composite_ndv_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE fact_pair_eq (\
             id INT PRIMARY KEY, f_part INT NOT NULL, f_supp INT NOT NULL, \
             INDEX idx_fact_pair(f_part, f_supp))",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE part_dim_eq (d_part INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE pair_dim_eq (\
             ps_part INT NOT NULL, ps_supp INT NOT NULL, \
             PRIMARY KEY(ps_part, ps_supp))",
        )
        .unwrap();
    let fact_rows = (1..=100)
        .map(|i| format!("({i},{i},{i})"))
        .collect::<Vec<_>>()
        .join(",");
    let part_rows = (1..=100)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    let pair_rows = (1..=100)
        .map(|i| format!("({i},{i})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO fact_pair_eq VALUES {fact_rows}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO part_dim_eq VALUES {part_rows}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO pair_dim_eq VALUES {pair_rows}"))
        .unwrap();
    for table in ["fact_pair_eq", "part_dim_eq", "pair_dim_eq"] {
        engine.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }
    let db = engine.db_for_tests();
    let scan = |sql| {
        build_physical_plan(
            db,
            &match parse_statement(sql).unwrap() {
                Statement::Select(select) => select,
                _ => unreachable!(),
            },
        )
        .unwrap()
    };
    let fact = scan("SELECT * FROM fact_pair_eq");
    let part = scan("SELECT * FROM part_dim_eq");
    let pair = scan("SELECT * FROM pair_dim_eq");
    let left = PhysicalPlan::HashJoin {
        left: Box::new(fact),
        right: Box::new(part),
        join_type: JoinType::Inner,
        left_keys: vec![Expr::Identifier("f_part".into())],
        right_keys: vec![Expr::Identifier("d_part".into())],
        null_safe: vec![false],
        build_side: BuildSide::Right,
        residual: None,
        output_projection: None,
        estimated_rows: 100.0,
        cost: crate::engine::cbo::Cost::default(),
    };
    let estimated = estimate_equijoin_rows(
        &left,
        &pair,
        &[
            Expr::Identifier("f_part".into()),
            Expr::Identifier("f_supp".into()),
            Expr::Identifier("d_part".into()),
        ],
        &[
            Expr::Identifier("ps_part".into()),
            Expr::Identifier("ps_supp".into()),
            Expr::Identifier("ps_part".into()),
        ],
        100.0,
        100.0,
    );
    assert!(
        estimated >= 90.0,
        "the effective two-column key should use its joint NDV, got {estimated}"
    );

    let equal = |left: &str, right: &str| Expr::Binary {
        left: Box::new(Expr::Identifier(left.into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Identifier(right.into())),
    };
    let condition = Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(equal("f_part", "ps_part")),
            op: BinaryOp::And,
            right: Box::new(equal("f_supp", "ps_supp")),
        }),
        op: BinaryOp::And,
        right: Box::new(equal("d_part", "ps_part")),
    };
    let (left_keys, right_keys, null_safe, residual) =
        extract_join_keys_for_plans(&left, &pair, &condition).unwrap();
    assert_eq!(left_keys.len(), 2);
    assert_eq!(right_keys.len(), 2);
    assert_eq!(null_safe, [false, false]);
    assert!(residual.is_none());
}

#[test]
fn broad_noncovering_primary_probe_costs_more_than_heap_scan_hash_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE probe_outer (id INT PRIMARY KEY, inner_id INT NOT NULL, payload INT)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE probe_inner (id INT PRIMARY KEY, payload INT NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO probe_outer VALUES (1,1,10),(2,2,20)")
        .unwrap();
    engine
        .execute("INSERT INTO probe_inner VALUES (1,100),(2,200)")
        .unwrap();
    engine.execute("ANALYZE TABLE probe_outer").unwrap();
    engine.execute("ANALYZE TABLE probe_inner").unwrap();
    let db = engine.db_for_tests();
    let scan = |sql| {
        build_physical_plan(
            db,
            &match parse_statement(sql).unwrap() {
                Statement::Select(select) => select,
                _ => unreachable!(),
            },
        )
        .unwrap()
    };
    let mut outer = scan("SELECT * FROM probe_outer");
    let mut inner = scan("SELECT * FROM probe_inner");
    let ctx = crate::engine::cbo::CboContext::from_db(db);
    let retarget = |plan: &mut PhysicalPlan, rows: u64, pages: u64, key: &str| {
        let leaf = match &mut *plan {
            PhysicalPlan::Projection { input, .. } => input.as_mut(),
            leaf => leaf,
        };
        let (table, estimated_rows, access_rows, cost) = match leaf {
            PhysicalPlan::SeqScan {
                table,
                estimated_rows,
                access_rows,
                cost,
                ..
            }
            | PhysicalPlan::IndexFastScan {
                table,
                estimated_rows,
                access_rows,
                cost,
                ..
            } => (table, estimated_rows, access_rows, cost),
            other => panic!("unexpected tiny-fixture scan: {other:?}"),
        };
        let schema = Arc::make_mut(&mut table.schema);
        schema.stats.row_count = rows;
        schema.stats.heap_page_count = pages;
        schema.stats.columns.get_mut(key).unwrap().distinct_count = rows;
        for index_stats in schema.stats.indexes.values_mut() {
            if let Some(first) = index_stats.prefix_distinct_counts.first_mut() {
                *first = rows;
            }
        }
        *estimated_rows = rows as f64;
        *access_rows = rows as f64;
        *cost = crate::engine::cbo::operator_cost::seq_scan(
            &ctx,
            pages as f64,
            rows as f64,
            rows as f64,
            crate::engine::cbo::operator_cost::estimated_row_width(schema),
            0.0,
        );
        if let PhysicalPlan::Projection {
            input,
            estimated_rows,
            cost,
            ..
        } = plan
        {
            *estimated_rows = rows as f64;
            *cost = plan_cost(input);
        }
    };
    retarget(&mut outer, 150_000, 12_000, "inner_id");
    retarget(&mut inner, 1_500_000, 18_000, "id");
    let condition = Expr::Binary {
        left: Box::new(Expr::CompoundIdentifier(vec![
            "probe_outer".into(),
            "inner_id".into(),
        ])),
        op: BinaryOp::Eq,
        right: Box::new(Expr::CompoundIdentifier(vec![
            "probe_inner".into(),
            "id".into(),
        ])),
    };
    let chosen = choose_join_with_hints(&ctx, outer, inner, JoinType::Inner, Some(condition), &[]);
    assert!(
        matches!(chosen, PhysicalPlan::HashJoin { .. }),
        "a broad non-covering lookup through an unclustered PRIMARY index should yield to a heap scan hash join; got {chosen:?}"
    );
}

#[test]
fn dense_filtered_key_domain_corrects_star_join_estimate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE calendar_dim (\
             date_sk INT PRIMARY KEY, \
             yr INT NOT NULL, \
             INDEX idx_calendar_year(yr))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE sales_fact (\
             id INT PRIMARY KEY, \
             sold_date_sk INT NOT NULL, \
             INDEX idx_sales_date(sold_date_sk))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO calendar_dim VALUES ");
    for day in 1..=1000 {
        if day > 1 {
            sql.push(',');
        }
        let yr = 2000 + ((day - 1) / 100);
        sql.push_str(&format!("({day},{yr})"));
    }
    engine.execute(&sql).unwrap();
    let mut sql = String::from("INSERT INTO sales_fact VALUES ");
    for id in 1..=1000 {
        if id > 1 {
            sql.push(',');
        }
        let sold_date_sk = 201 + ((id - 1) % 100);
        sql.push_str(&format!("({id},{sold_date_sk})"));
    }
    engine.execute(&sql).unwrap();
    engine.execute("ANALYZE TABLE calendar_dim").unwrap();
    engine.execute("ANALYZE TABLE sales_fact").unwrap();

    let db = engine.db_for_tests();
    let calendar_scan = build_physical_plan(
        db,
        &match parse_statement("SELECT * FROM calendar_dim WHERE yr = 2002").unwrap() {
            Statement::Select(s) => s,
            _ => unreachable!(),
        },
    )
    .unwrap();
    let sales_scan = build_physical_plan(
        db,
        &match parse_statement("SELECT * FROM sales_fact").unwrap() {
            Statement::Select(s) => s,
            _ => unreachable!(),
        },
    )
    .unwrap();

    let est = estimate_equijoin_rows(
        &calendar_scan,
        &sales_scan,
        &[Expr::Identifier("date_sk".into())],
        &[Expr::Identifier("sold_date_sk".into())],
        estimated_rows(&calendar_scan),
        estimated_rows(&sales_scan),
    );

    assert!(
        est >= 900.0,
        "dense filtered key domain should estimate the fact-side date fanout, got {est}"
    );
    for predicate in [
        "yr IN (2002,2003)",
        "yr BETWEEN 2002 AND 2003",
        "yr >= 2002 AND yr < 2004",
    ] {
        let Statement::Select(select) =
            parse_statement(&format!("SELECT * FROM calendar_dim WHERE {predicate}")).unwrap()
        else {
            unreachable!()
        };
        let calendar_scan = build_physical_plan(db, &select).unwrap();
        let estimated = estimate_equijoin_rows(
            &calendar_scan,
            &sales_scan,
            &[Expr::Identifier("date_sk".into())],
            &[Expr::Identifier("sold_date_sk".into())],
            estimated_rows(&calendar_scan),
            estimated_rows(&sales_scan),
        );
        assert!(
            estimated >= 900.0,
            "{predicate} lost its conditional key domain: {estimated}"
        );
    }
}

#[test]
fn dense_filtered_domain_inlj_caps_inner_ndv_by_integer_span() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE calendar_dim (\
             date_sk INT PRIMARY KEY, \
             yr INT NOT NULL, \
             INDEX idx_calendar_year(yr))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE sales_fact (\
             id INT PRIMARY KEY, \
             sold_date_sk INT NOT NULL, \
             INDEX idx_sales_date(sold_date_sk))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO calendar_dim VALUES ");
    for day in 1..=1000 {
        if day > 1 {
            sql.push(',');
        }
        let yr = 2000 + ((day - 1) / 100);
        sql.push_str(&format!("({day},{yr})"));
    }
    engine.execute(&sql).unwrap();
    let mut sql = String::from("INSERT INTO sales_fact VALUES ");
    for id in 1..=1000 {
        if id > 1 {
            sql.push(',');
        }
        let sold_date_sk = 201 + ((id - 1) % 100);
        sql.push_str(&format!("({id},{sold_date_sk})"));
    }
    engine.execute(&sql).unwrap();
    engine.execute("ANALYZE TABLE calendar_dim").unwrap();
    engine.execute("ANALYZE TABLE sales_fact").unwrap();

    let db = engine.db_for_tests();
    let calendar_scan = build_physical_plan(
        db,
        &match parse_statement("SELECT * FROM calendar_dim WHERE yr = 2002").unwrap() {
            Statement::Select(s) => s,
            _ => unreachable!(),
        },
    )
    .unwrap();

    let mut sales_table = (*db.table("sales_fact").unwrap()).clone();
    sales_table
        .stats
        .columns
        .get_mut("sold_date_sk")
        .unwrap()
        .distinct_count = 10_000;
    sales_table
        .stats
        .indexes
        .get_mut("idx_sales_date")
        .unwrap()
        .prefix_distinct_counts[0] = 10_000;
    let sales_table = PlanTable::new(sales_table.id, Arc::new(sales_table), None);
    let sales_index = sales_table
        .schema
        .indexes
        .iter()
        .find(|idx| idx.name == "idx_sales_date")
        .unwrap();
    let join_condition = Expr::Binary {
        left: Box::new(Expr::Identifier("date_sk".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Identifier("sold_date_sk".into())),
    };

    let rows = dense_filtered_domain_rows_for_inlj(
        &calendar_scan,
        &sales_table,
        sales_index,
        Some(&join_condition),
        estimated_rows(&calendar_scan),
    )
    .unwrap();

    assert!(
        rows >= 900.0,
        "INLJ dense-domain estimate should cap high inner NDV by integer span, got {rows}"
    );
}

#[test]
fn dense_filtered_domain_uses_integer_span_when_conditional_ndv_is_sampled_low() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE calendar_dim (\
             date_sk INT PRIMARY KEY, \
             yr INT NOT NULL, \
             INDEX idx_calendar_year(yr))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE sales_fact (\
             id INT PRIMARY KEY, \
             sold_date_sk INT NOT NULL, \
             INDEX idx_sales_date(sold_date_sk))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO calendar_dim VALUES ");
    for day in 1..=1000 {
        if day > 1 {
            sql.push(',');
        }
        let yr = 2000 + ((day - 1) / 100);
        sql.push_str(&format!("({day},{yr})"));
    }
    engine.execute(&sql).unwrap();
    let mut sql = String::from("INSERT INTO sales_fact VALUES ");
    for id in 1..=1000 {
        if id > 1 {
            sql.push(',');
        }
        let sold_date_sk = 201 + ((id - 1) % 100);
        sql.push_str(&format!("({id},{sold_date_sk})"));
    }
    engine.execute(&sql).unwrap();
    engine.execute("ANALYZE TABLE calendar_dim").unwrap();
    engine.execute("ANALYZE TABLE sales_fact").unwrap();

    let db = engine.db_for_tests();
    let mut calendar_table = (*db.table("calendar_dim").unwrap()).clone();
    for domain in &mut calendar_table.stats.conditional_key_domains {
        if domain.filter_columns == ["yr"] && domain.key_columns == ["date_sk"] {
            for entry in &mut domain.entries {
                if entry.filter_values == [Value::Int(2002)] {
                    entry.key_distinct_count = 8;
                }
            }
        }
    }
    let calendar_table = PlanTable::new(calendar_table.id, Arc::new(calendar_table), None);
    let calendar_scan = PhysicalPlan::IndexLookup {
        table: calendar_table,
        index: db
            .table("calendar_dim")
            .unwrap()
            .indexes
            .iter()
            .find(|idx| idx.name == "idx_calendar_year")
            .unwrap()
            .clone(),
        key: IndexKey::from(vec![KeyPart::from_value(&Value::Int(2002))]),
        exact: true,
        filter: Some(Expr::Binary {
            left: Box::new(Expr::Identifier("yr".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::BoundValue(Value::Int(2002))),
        }),
        estimated_rows: 100.0,
        access_rows: 100.0,
        cost: crate::engine::cbo::Cost::scalar(0.0),
        possible_keys: Vec::new(),
        covering: false,
        hint_note: None,
        projected_columns: None,
    };
    let sales_scan = build_physical_plan(
        db,
        &match parse_statement("SELECT * FROM sales_fact").unwrap() {
            Statement::Select(s) => s,
            _ => unreachable!(),
        },
    )
    .unwrap();

    let est = estimate_equijoin_rows(
        &calendar_scan,
        &sales_scan,
        &[Expr::Identifier("date_sk".into())],
        &[Expr::Identifier("sold_date_sk".into())],
        estimated_rows(&calendar_scan),
        estimated_rows(&sales_scan),
    );

    assert!(
        est >= 900.0,
        "dense integer key span should protect against sampled-low conditional NDV, got {est}"
    );
}

#[test]
fn sparse_filtered_key_domain_keeps_base_ndv_estimate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE profile_dim (\
             profile_sk INT PRIMARY KEY, \
             bucket INT NOT NULL, \
             INDEX idx_profile_bucket(bucket))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE sparse_fact (\
             id INT PRIMARY KEY, \
             profile_sk INT NOT NULL, \
             INDEX idx_sparse_profile(profile_sk))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO profile_dim VALUES ");
    for sk in 1..=1000 {
        if sk > 1 {
            sql.push(',');
        }
        let bucket = sk % 10;
        sql.push_str(&format!("({sk},{bucket})"));
    }
    engine.execute(&sql).unwrap();
    let mut sql = String::from("INSERT INTO sparse_fact VALUES ");
    for id in 1..=1000 {
        if id > 1 {
            sql.push(',');
        }
        let profile_sk = 1 + ((id - 1) % 100) * 10;
        sql.push_str(&format!("({id},{profile_sk})"));
    }
    engine.execute(&sql).unwrap();
    engine.execute("ANALYZE TABLE profile_dim").unwrap();
    engine.execute("ANALYZE TABLE sparse_fact").unwrap();

    let db = engine.db_for_tests();
    let profile_scan = build_physical_plan(
        db,
        &match parse_statement("SELECT * FROM profile_dim WHERE bucket = 1").unwrap() {
            Statement::Select(s) => s,
            _ => unreachable!(),
        },
    )
    .unwrap();
    let fact_scan = build_physical_plan(
        db,
        &match parse_statement("SELECT * FROM sparse_fact").unwrap() {
            Statement::Select(s) => s,
            _ => unreachable!(),
        },
    )
    .unwrap();

    let est = estimate_equijoin_rows(
        &profile_scan,
        &fact_scan,
        &[Expr::Identifier("profile_sk".into())],
        &[Expr::Identifier("profile_sk".into())],
        estimated_rows(&profile_scan),
        estimated_rows(&fact_scan),
    );

    assert!(
        est < 300.0,
        "sparse surrogate-key domain must not be treated as dense fact coverage, got {est}"
    );
}

#[test]
fn composite_filtered_key_domain_prefers_most_specific_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE profile_dim (\
             profile_sk INT PRIMARY KEY, \
             bucket INT NOT NULL, \
             shard INT NOT NULL, \
             INDEX idx_profile_bucket_shard(bucket, shard))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE sparse_fact (\
             id INT PRIMARY KEY, \
             profile_sk INT NOT NULL, \
             INDEX idx_sparse_profile(profile_sk))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO profile_dim VALUES ");
    for sk in 1..=1000 {
        if sk > 1 {
            sql.push(',');
        }
        let bucket = sk % 2;
        let shard = sk % 100;
        sql.push_str(&format!("({sk},{bucket},{shard})"));
    }
    engine.execute(&sql).unwrap();
    let mut sql = String::from("INSERT INTO sparse_fact VALUES ");
    for id in 1..=1000 {
        if id > 1 {
            sql.push(',');
        }
        let profile_sk = 1 + ((id - 1) % 10) * 100;
        sql.push_str(&format!("({id},{profile_sk})"));
    }
    engine.execute(&sql).unwrap();
    engine.execute("ANALYZE TABLE profile_dim").unwrap();
    engine.execute("ANALYZE TABLE sparse_fact").unwrap();

    let db = engine.db_for_tests();
    let profile_scan = build_physical_plan(
        db,
        &match parse_statement("SELECT * FROM profile_dim WHERE bucket = 1 AND shard = 1").unwrap()
        {
            Statement::Select(s) => s,
            _ => unreachable!(),
        },
    )
    .unwrap();
    let fact_scan = build_physical_plan(
        db,
        &match parse_statement("SELECT * FROM sparse_fact").unwrap() {
            Statement::Select(s) => s,
            _ => unreachable!(),
        },
    )
    .unwrap();

    let est = estimate_equijoin_rows(
        &profile_scan,
        &fact_scan,
        &[Expr::Identifier("profile_sk".into())],
        &[Expr::Identifier("profile_sk".into())],
        estimated_rows(&profile_scan),
        estimated_rows(&fact_scan),
    );

    assert!(
        est < 200.0,
        "composite filter should use the sparse most-specific prefix, got {est}"
    );
}

#[test]
fn composite_pk_leading_column_floor_does_not_hide_high_fanout() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE fact_by_item (\
             item_sk INT NOT NULL, \
             ticket INT NOT NULL, \
             payload INT NOT NULL, \
             PRIMARY KEY(item_sk, ticket), \
             INDEX idx_fact_by_item(item_sk))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO fact_by_item VALUES ");
    let mut first = true;
    for item in 1..=10 {
        for ticket in 1..=100 {
            if !first {
                sql.push(',');
            }
            first = false;
            sql.push_str(&format!("({item},{ticket},{ticket})"));
        }
    }
    engine.execute(&sql).unwrap();
    engine.execute("ANALYZE TABLE fact_by_item").unwrap();

    let mut table = (*engine.db_for_tests().table("fact_by_item").unwrap()).clone();
    table
        .stats
        .indexes
        .get_mut("idx_fact_by_item")
        .unwrap()
        .prefix_distinct_counts[0] = table.stats.row_count / 4;
    let index = table
        .indexes
        .iter()
        .find(|idx| idx.name == "idx_fact_by_item")
        .unwrap();
    let per_lookup =
        estimate_index_lookup_rows(&table, index, 1, false, table.stats.row_count.max(1) as f64);

    assert!(
        per_lookup >= 50.0,
        "high-fanout composite-PK leading column should not be floored to FK-like fanout, got {per_lookup}"
    );
}

#[test]
fn star_join_order_prefers_sparse_dimension_over_dense_date_domain() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE calendar_dim (\
             date_sk INT PRIMARY KEY, \
             yr INT NOT NULL, \
             INDEX idx_calendar_year(yr))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE profile_dim (\
             profile_sk INT PRIMARY KEY, \
             bucket INT NOT NULL, \
             INDEX idx_profile_bucket(bucket))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE sales_fact (\
             id INT PRIMARY KEY, \
             sold_date_sk INT NOT NULL, \
             profile_sk INT NOT NULL, \
             INDEX idx_sales_date(sold_date_sk), \
             INDEX idx_sales_profile(profile_sk))",
        )
        .unwrap();
    let mut sql = String::from("INSERT INTO calendar_dim VALUES ");
    for day in 1..=1000 {
        if day > 1 {
            sql.push(',');
        }
        let yr = 2000 + ((day - 1) / 100);
        sql.push_str(&format!("({day},{yr})"));
    }
    engine.execute(&sql).unwrap();
    let mut sql = String::from("INSERT INTO profile_dim VALUES ");
    for sk in 1..=1000 {
        if sk > 1 {
            sql.push(',');
        }
        let bucket = sk % 10;
        sql.push_str(&format!("({sk},{bucket})"));
    }
    engine.execute(&sql).unwrap();
    let mut sql = String::from("INSERT INTO sales_fact VALUES ");
    for id in 1..=1000 {
        if id > 1 {
            sql.push(',');
        }
        let sold_date_sk = 201 + ((id - 1) % 100);
        sql.push_str(&format!("({id},{sold_date_sk},{id})"));
    }
    engine.execute(&sql).unwrap();
    engine.execute("ANALYZE TABLE calendar_dim").unwrap();
    engine.execute("ANALYZE TABLE profile_dim").unwrap();
    engine.execute("ANALYZE TABLE sales_fact").unwrap();

    let plan = build_physical_plan(
        engine.db_for_tests(),
        &match parse_statement(
            "SELECT COUNT(*) \
             FROM sales_fact, calendar_dim, profile_dim \
             WHERE sold_date_sk = date_sk \
               AND sales_fact.profile_sk = profile_dim.profile_sk \
               AND yr = 2002 \
               AND bucket = 1",
        )
        .unwrap()
        {
            Statement::Select(s) => s,
            _ => unreachable!(),
        },
    )
    .unwrap();
    let first = first_join_tables_containing(&plan, "sales_fact")
        .unwrap_or_else(|| panic!("expected a join containing sales_fact, got {plan:?}"));
    assert!(
        first.iter().any(|name| name == "profile_dim")
            && !first.iter().any(|name| name == "calendar_dim"),
        "first fact join should use the sparse profile dimension, got {first:?}; plan={plan:?}"
    );
}

fn first_join_tables_containing(plan: &PhysicalPlan, table_name: &str) -> Option<Vec<String>> {
    match plan {
        PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
            first_join_tables_containing(left, table_name)
                .or_else(|| first_join_tables_containing(right, table_name))
                .or_else(|| {
                    let names = collect_physical_plan_tables(plan)
                        .into_iter()
                        .map(|table| table.schema.name.clone())
                        .collect::<Vec<_>>();
                    if names.iter().any(|name| name == table_name) {
                        Some(names)
                    } else {
                        None
                    }
                })
        }
        PhysicalPlan::IndexNestedLoopJoin { left, .. } => {
            first_join_tables_containing(left, table_name).or_else(|| {
                let names = collect_physical_plan_tables(plan)
                    .into_iter()
                    .map(|table| table.schema.name.clone())
                    .collect::<Vec<_>>();
                if names.iter().any(|name| name == table_name) {
                    Some(names)
                } else {
                    None
                }
            })
        }
        PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::HashRollupAggregate { input, .. }
        | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
        | PhysicalPlan::StreamingAggregate { input, .. }
        | PhysicalPlan::StreamingRollupAggregate { input, .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::WindowAgg { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::Exchange { input, .. } => first_join_tables_containing(input, table_name),
        _ => None,
    }
}

#[test]
fn in_subquery_lookup_costs_heap_fanout_independently_of_key_count() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    setup_dual_sip_store_sales(&engine, 10, 40);
    let db = engine.db_for_tests();
    let mut schema = db.table("store_sales").unwrap();
    Arc::make_mut(&mut schema).stats.row_count = 4_000_000;
    let table = PlanTable::new(schema.id, schema, None);
    let Statement::Select(subquery) =
        parse_statement("SELECT d_date_sk FROM date_dim WHERE d_year = 2001").unwrap()
    else {
        panic!("expected SELECT");
    };
    let access = InSubqueryIndexAccess {
        index: table
            .schema
            .indexes
            .iter()
            .find(|i| i.name == "idx_ss_sold_date")
            .unwrap()
            .clone(),
        key_prefix: IndexKey::from(Vec::new()),
        in_column: "ss_sold_date_sk".to_string(),
        subquery,
    };
    let mut ctx = crate::engine::cbo::CboContext::from_db(db);
    ctx.effective_cache_pages = 64;
    let costed = cost_in_subquery_index_access(
        db,
        &ctx,
        &table,
        &access,
        &ScanHints::none(),
        None,
        4_000_000.0,
        100_000.0,
        24.0,
    );
    assert!(costed.lookup_count <= 16.0);
    assert!(costed.access_rows >= 100_000.0);
    assert!(
        costed.cost.io_rnd_pages > 100_000.0,
        "few lookup keys must still cost the heap pages their fanout visits: {:?}",
        costed.cost
    );
}

#[test]
fn distinct_set_integer_reduction_preserves_nulls_aliases_and_all_multiplicity() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE set_dim (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    engine.execute("CREATE TABLE set_fact (k INT)").unwrap();
    engine.execute("INSERT INTO set_dim SELECT n, IF(n%7=0,NULL,n%4), n%5 FROM generate_series(0,99) gs(n)").unwrap();
    engine
        .execute("INSERT INTO set_fact SELECT n%100 FROM generate_series(0,59999) gs(n)")
        .unwrap();
    engine
        .execute("INSERT INTO set_fact VALUES (NULL),(1000)")
        .unwrap();
    engine.execute("ANALYZE TABLE set_dim").unwrap();
    engine.execute("ANALYZE TABLE set_fact").unwrap();
    let sql = "SELECT d.a AS x,d.b AS y FROM set_fact f JOIN set_dim d ON f.k=d.id INTERSECT SELECT a,b FROM set_dim WHERE id<80 ORDER BY 1,2";
    let QueryResponse::ResultSet(expected) = engine
        .execute("SELECT DISTINCT a,b FROM set_dim WHERE id<80 ORDER BY 1,2")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("rows")
    };
    let QueryResponse::ResultSet(actual) = engine.execute(sql).unwrap().materialize().unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(actual.rows, expected.rows);
    let QueryResponse::ResultSet(explain) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("plan")
    };
    assert!(explain.rows[0][0]
        .to_display_string()
        .to_ascii_lowercase()
        .contains("aggregate"));
    assert!(
        explain.rows[0][0]
            .to_display_string()
            .to_ascii_lowercase()
            .contains("semi"),
        "distinct dimension projection should use existence: {:?}",
        explain.rows
    );
    let all = sql.replace(" INTERSECT ", " INTERSECT ALL ");
    let QueryResponse::ResultSet(actual) = engine.execute(&all).unwrap().materialize().unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(
        actual.rows.len(),
        80,
        "ALL must preserve every matching copy"
    );
}

#[test]
fn in_subquery_index_access_costs_multiple_sip_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_dual_sip_store_sales(&engine, 10, 40);

    let plan = plan_dual_sip_store_sales(engine.db_for_tests());
    let index =
        find_sip_index_access_for_table(&plan.physical, "store_sales").unwrap_or_else(|| {
            panic!(
                "expected store_sales IN-subquery index access, got {:?}",
                plan.physical
            )
        });
    assert_eq!(
        index, "idx_ss_sold_date",
        "date_dim keyset has far fewer lookup probes than customer_address; plan={:?}",
        plan.physical
    );
}

#[test]
fn in_subquery_index_access_keeps_cheaper_non_date_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Keep enough offset-domain NDV that the desired five-row value stays
    // selective even under the top-values uniform lower bound.
    setup_dual_sip_store_sales_with_address_count(&engine, 90, 5, 100);

    let index = best_dual_in_subquery_index(engine.db_for_tests())
        .unwrap_or_else(|| panic!("expected store_sales IN-subquery index candidate"));
    assert_eq!(
        index, "idx_ss_addr",
        "address keyset is deliberately cheaper and should still win"
    );
}

#[test]
fn in_subquery_index_access_is_conservative_for_missing_keyset_stats() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_dual_sip_store_sales_with_address_count(&engine, 10, 4, 200);

    let index = best_dual_in_subquery_index(engine.db_for_tests())
        .unwrap_or_else(|| panic!("expected store_sales IN-subquery index candidate"));
    assert_eq!(
        index, "idx_ss_sold_date",
        "unindexed keyset filters with missing stats must not be costed as tiny"
    );
}

fn setup_dual_sip_store_sales(
    engine: &StorageQueryEngine,
    matching_date_keys: i32,
    matching_address_keys: i32,
) {
    setup_dual_sip_store_sales_with_address_count(
        engine,
        matching_date_keys,
        matching_address_keys,
        100,
    );
}

fn setup_dual_sip_store_sales_with_address_count(
    engine: &StorageQueryEngine,
    matching_date_keys: i32,
    matching_address_keys: i32,
    address_count: i32,
) {
    engine
        .execute(
            "CREATE TABLE date_dim (\
             d_date_sk INT PRIMARY KEY, \
             d_year INT NOT NULL, \
             d_moy INT NOT NULL, \
             INDEX idx_date_dim_year(d_year))",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE customer_address (\
             ca_address_sk INT PRIMARY KEY, \
             ca_gmt_offset INT NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE store_sales (\
             ss_sold_date_sk INT NOT NULL, \
             ss_addr_sk INT NOT NULL, \
             ss_ext_sales_price INT NOT NULL, \
             INDEX idx_ss_sold_date(ss_sold_date_sk), \
             INDEX idx_ss_addr(ss_addr_sk))",
        )
        .unwrap();

    let dates = (1..=100)
        .map(|i| {
            let year = if i <= matching_date_keys { 2001 } else { 2002 };
            let moy = if i <= matching_date_keys { 3 } else { 4 };
            format!("({i},{year},{moy})")
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO date_dim VALUES {dates}"))
        .unwrap();

    let addresses = (1..=address_count)
        .map(|i| {
            let offset = if i <= matching_address_keys { -5 } else { -8 };
            format!("({i},{offset})")
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO customer_address VALUES {addresses}"))
        .unwrap();

    let sales = (1..=400)
        .map(|i| {
            let date_sk = ((i - 1) % 100) + 1;
            let addr_sk = ((i - 1) % address_count) + 1;
            format!("({date_sk},{addr_sk},1)")
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO store_sales VALUES {sales}"))
        .unwrap();

    engine.execute("ANALYZE TABLE date_dim").unwrap();
    engine.execute("ANALYZE TABLE customer_address").unwrap();
    engine.execute("ANALYZE TABLE store_sales").unwrap();
}

fn plan_dual_sip_store_sales(db: &Database) -> PlannedSelect {
    let Statement::Select(select) = parse_statement(
        "SELECT COUNT(*) \
         FROM store_sales ss, date_dim d, customer_address ca \
         WHERE ss.ss_sold_date_sk = d.d_date_sk \
           AND ss.ss_addr_sk = ca.ca_address_sk \
           AND d.d_year = 2001 \
           AND d.d_moy = 3 \
           AND ca.ca_gmt_offset = -5",
    )
    .unwrap() else {
        panic!("expected SELECT");
    };
    plan_select(db, &select).unwrap()
}

fn best_dual_in_subquery_index(db: &Database) -> Option<String> {
    let Statement::Select(select) = parse_statement(
        "SELECT COUNT(*) \
         FROM store_sales ss \
         WHERE ss.ss_sold_date_sk IN (\
             SELECT d.d_date_sk FROM date_dim d \
             WHERE d.d_year = 2001 AND d.d_moy = 3) \
           AND ss.ss_addr_sk IN (\
             SELECT ca.ca_address_sk FROM customer_address ca \
             WHERE ca.ca_gmt_offset = -5)",
    )
    .unwrap() else {
        panic!("expected SELECT");
    };
    let table = db.table("store_sales").unwrap();
    let plan_table = PlanTable::new(table.id, table, select.from[0].alias.clone());
    let filter = select.selection.clone();
    let expr = filter.as_ref()?;
    let ctx = crate::engine::cbo::CboContext::from_db(db);
    let total_rows = plan_table.schema.stats.row_count.max(1) as f64;
    let row_width = crate::engine::cbo::operator_cost::estimated_row_width(&plan_table.schema);
    let table_pages =
        crate::engine::cbo::operator_cost::estimated_heap_pages(total_rows, row_width);
    best_in_subquery_index_access(
        db,
        &ctx,
        &plan_table,
        expr,
        filter.clone(),
        &ScanHints::none(),
        total_rows,
        table_pages,
        row_width,
    )
    .map(|choice| choice.access.index.name)
}

fn find_sip_index_access_for_table(plan: &PhysicalPlan, table_name: &str) -> Option<String> {
    match plan {
        PhysicalPlan::IndexInLookup { table, index, .. }
            if table.schema.name.eq_ignore_ascii_case(table_name) =>
        {
            Some(index.name.clone())
        }
        PhysicalPlan::IndexNestedLoopJoin {
            right_table,
            right_index,
            condition: Some(condition),
            ..
        } if right_table.schema.name.eq_ignore_ascii_case(table_name)
            && sip_expr_has_subquery(condition) =>
        {
            Some(right_index.name.clone())
        }
        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
            find_sip_index_access_for_table(left, table_name)
                .or_else(|| find_sip_index_access_for_table(right, table_name))
        }
        PhysicalPlan::IndexNestedLoopJoin { left, .. } => {
            find_sip_index_access_for_table(left, table_name)
        }
        PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::HashRollupAggregate { input, .. }
        | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
        | PhysicalPlan::StreamingAggregate { input, .. }
        | PhysicalPlan::StreamingRollupAggregate { input, .. }
        | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::ParallelTopK { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::WindowAgg { input, .. }
        | PhysicalPlan::StreamingDistinct { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Apply { outer: input, .. }
        | PhysicalPlan::Materialize { input, .. }
        | PhysicalPlan::Exchange { input, .. } => {
            find_sip_index_access_for_table(input, table_name)
        }
        _ => None,
    }
}

#[test]
fn expr_refs_only_table_recognizes_inlist_and_function() {
    // Q12-shape predicates on lineitem: the IN list and the
    // DATE_ADD-wrapped comparison must be recognised as single-table
    // residuals, otherwise their selectivity is dropped from the
    // INL right-only estimate and the planner over-estimates rows.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT NOT NULL,\
             l_shipmode VARCHAR(16) NOT NULL,\
             l_commitdate DATE NOT NULL,\
             l_receiptdate DATE NOT NULL,\
             l_shipdate DATE NOT NULL,\
             PRIMARY KEY (l_orderkey))",
        )
        .unwrap();
    let table = engine.db_for_tests().table("lineitem").unwrap();
    let plan_table = PlanTable::new(table.id, table, None);

    // l_shipmode IN ('MAIL','SHIP')
    let in_list = Expr::InList {
        expr: Box::new(Expr::Identifier("l_shipmode".into())),
        list: vec![
            Expr::Literal(crate::parser::Literal::String("MAIL".into())),
            Expr::Literal(crate::parser::Literal::String("SHIP".into())),
        ],
        negated: false,
    };
    assert!(
        expr_refs_only_table(&in_list, &plan_table),
        "IN list against lineitem column must be recognised as right-only"
    );

    // l_receiptdate < DATE_ADD('1994-01-01', INTERVAL '1' YEAR)
    let date_add = Expr::Function {
        name: "DATE_ADD".into(),
        args: vec![
            Expr::Literal(crate::parser::Literal::String("1994-01-01".into())),
            Expr::Function {
                name: "__INTERVAL".into(),
                args: vec![
                    Expr::Literal(crate::parser::Literal::String("1".into())),
                    Expr::Literal(crate::parser::Literal::String("YEAR".into())),
                ],
                distinct: false,
            },
        ],
        distinct: false,
    };
    let date_cmp = Expr::Binary {
        left: Box::new(Expr::Identifier("l_receiptdate".into())),
        op: BinaryOp::Lt,
        right: Box::new(date_add),
    };
    assert!(
        expr_refs_only_table(&date_cmp, &plan_table),
        "column < FUNCTION(literal,...) must be recognised as right-only"
    );

    // Column-vs-column on the same table.
    let col_cmp = Expr::Binary {
        left: Box::new(Expr::Identifier("l_commitdate".into())),
        op: BinaryOp::Lt,
        right: Box::new(Expr::Identifier("l_receiptdate".into())),
    };
    assert!(
        expr_refs_only_table(&col_cmp, &plan_table),
        "column-vs-column on lineitem must be recognised as right-only"
    );

    // A predicate that references a column NOT on lineitem must
    // remain rejected; otherwise we'd misclassify join keys as
    // residuals.
    let cross_ref = Expr::Binary {
        left: Box::new(Expr::Identifier("o_orderkey".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Identifier("l_orderkey".into())),
    };
    assert!(
        !expr_refs_only_table(&cross_ref, &plan_table),
        "join key referencing a non-lineitem column must be rejected"
    );
}

#[test]
fn inlj_scan_filter_residual_ignores_future_table_conjuncts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE store_sales (\
             ss_sold_date_sk INT NOT NULL,\
             ss_store_sk INT NOT NULL,\
             ss_sales_price INT NOT NULL,\
             INDEX idx_ss_sold_date(ss_sold_date_sk))",
        )
        .unwrap();
    let table = engine.db_for_tests().table("store_sales").unwrap();
    let plan_table = PlanTable::new(table.id, table.clone(), None);
    let scan_filter = Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::Identifier("ss_store_sk".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Identifier("s_store_sk".into())),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::Binary {
            left: Box::new(Expr::Identifier("ss_sales_price".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::BoundValue(Value::Int(0))),
        }),
    };
    let scan = PhysicalPlan::SeqScan {
        table: plan_table.clone(),
        filter: Some(scan_filter),
        estimated_rows: 100.0,
        access_rows: 100.0,
        cost: crate::engine::cbo::Cost::scalar(0.0),
        possible_keys: Vec::new(),
        runtime_filters: Vec::new(),
        hint_note: None,
        projected_columns: None,
    };

    let residual = collect_inlj_right_only_residual(None, &scan, &plan_table)
        .expect("right-only scan predicate should survive");
    let rendered = format!("{residual:?}");
    assert!(rendered.contains("ss_sales_price"), "{rendered}");
    assert!(
        !rendered.contains("s_store_sk"),
        "future-table conjunct must not be treated as INLJ right-only residual: {rendered}"
    );
}

#[test]
fn q12_shape_plan_drives_from_filtered_side() {
    // Mimics TPC-H Q12: orders is the large unfiltered side, the
    // selective WHERE shrinks lineitem 100x. The pre-fix planner
    // costed (orders outer × INL into lineitem PK) cheaper than the
    // truly-cheaper (filtered lineitem outer × INL into orders PK),
    // because (1) batched INL amortised inner-side work into a
    // single sequential range scan and (2) the right-only residual
    // selectivity dropped IN/Function predicates. After the fix the
    // planner should prefer driving from the filtered lineitem side
    // — either via INLJ with lineitem as outer, or as a hash join
    // with the filtered lineitem as the build side.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE orders (\
             o_orderkey INT PRIMARY KEY,\
             o_orderdate DATE NOT NULL)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE lineitem (\
             l_orderkey INT NOT NULL,\
             l_linenumber INT NOT NULL,\
             l_shipmode VARCHAR(16) NOT NULL,\
             l_commitdate DATE NOT NULL,\
             l_receiptdate DATE NOT NULL,\
             l_shipdate DATE NOT NULL,\
             PRIMARY KEY (l_orderkey, l_linenumber))",
        )
        .unwrap();
    // 1000 orders, ~4000 lineitems (1-7 per order), only ~1% pass
    // the Q12 filter (l_shipmode IN ('MAIL','SHIP') AND date window).
    let mut order_id = 1;
    let shipmodes = ["TRUCK", "RAIL", "AIR", "REG_AIR", "FOB", "MAIL", "SHIP"];
    let mut lineno: i32;
    let mut batch_inserts: Vec<String> = Vec::new();
    for o in 1..=1000 {
        batch_inserts.push(format!("({o}, '1994-06-{:02}')", (o % 28) + 1));
    }
    engine
        .execute(&format!(
            "INSERT INTO orders VALUES {}",
            batch_inserts.join(", ")
        ))
        .unwrap();
    batch_inserts.clear();
    for o in 1..=1000 {
        let lines = (o % 7) + 1;
        lineno = 1;
        for _ in 0..lines {
            // Spread shipmodes 7-way; MAIL/SHIP each appear ~1/7.
            let mode = shipmodes[((order_id + lineno as i32) as usize) % 7];
            // Most rows fail the Q12 date window or the inequality
            // chain. Force MAIL/SHIP onto 1995-01-01 (outside window)
            // and TRUCK onto 1994-06-01 (inside window) so the
            // shipmode filter is highly selective in combination
            // with the date filter.
            let receipt = if mode == "MAIL" && (order_id % 30 == 0) {
                "1994-06-15"
            } else if mode == "SHIP" && (order_id % 30 == 0) {
                "1994-06-20"
            } else {
                "1995-06-01"
            };
            batch_inserts.push(format!(
                "({o}, {lineno}, '{mode}', '1994-05-01', '{receipt}', '1994-04-01')"
            ));
            lineno += 1;
        }
        order_id += 1;
    }
    engine
        .execute(&format!(
            "INSERT INTO lineitem VALUES {}",
            batch_inserts.join(", ")
        ))
        .unwrap();
    engine.execute("ANALYZE TABLE orders").unwrap();
    engine.execute("ANALYZE TABLE lineitem").unwrap();

    let q12 = parse_statement(
        "SELECT l_shipmode, COUNT(*) FROM lineitem \
         INNER JOIN orders ON o_orderkey = l_orderkey \
         WHERE l_shipmode IN ('MAIL','SHIP') \
           AND l_commitdate < l_receiptdate \
           AND l_shipdate < l_commitdate \
           AND l_receiptdate >= '1994-01-01' \
           AND l_receiptdate < '1995-01-01' \
         GROUP BY l_shipmode \
         ORDER BY l_shipmode",
    )
    .unwrap();
    let select = match q12 {
        Statement::Select(s) => s,
        _ => unreachable!(),
    };
    let plan = build_physical_plan(engine.db_for_tests(), &select).unwrap();

    // Drill into the plan to find the join node.
    fn find_join(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
        match plan {
            PhysicalPlan::IndexNestedLoopJoin { .. }
            | PhysicalPlan::HashJoin { .. }
            | PhysicalPlan::NestedLoopJoin { .. }
            | PhysicalPlan::AdaptiveJoin { .. } => Some(plan),
            PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::HashRollupAggregate { input, .. }
            | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
            | PhysicalPlan::StreamingAggregate { input, .. }
            | PhysicalPlan::StreamingRollupAggregate { input, .. }
            | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
            | PhysicalPlan::ParallelHashAggregate { input, .. }
            | PhysicalPlan::ParallelRollupAggregate { input, .. }
            | PhysicalPlan::ParallelGroupingSetsAggregate { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::TopK { input, .. }
            | PhysicalPlan::ParallelTopK { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::Exchange { input, .. } => find_join(input),
            _ => None,
        }
    }
    let join = find_join(&plan).expect("Q12 plan must contain a join");

    // Helper: collect leaf-table names appearing in a plan subtree.
    fn collect_tables(plan: &PhysicalPlan, out: &mut Vec<String>) {
        match plan {
            PhysicalPlan::SeqScan { table, .. }
            | PhysicalPlan::IndexLookup { table, .. }
            | PhysicalPlan::IndexRangeScan { table, .. }
            | PhysicalPlan::IndexFastScan { table, .. }
            | PhysicalPlan::IndexInLookup { table, .. }
            | PhysicalPlan::ParallelSeqScan { table, .. }
            | PhysicalPlan::ParallelIndexFastScan { table, .. }
            | PhysicalPlan::ParallelIndexRangeScan { table, .. }
            | PhysicalPlan::CoveringIndexAggregate { table, .. }
            | PhysicalPlan::ParallelIndexAggregate { table, .. } => {
                out.push(table.schema.name.clone());
            }
            PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::HashRollupAggregate { input, .. }
            | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
            | PhysicalPlan::StreamingAggregate { input, .. }
            | PhysicalPlan::StreamingRollupAggregate { input, .. }
            | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
            | PhysicalPlan::ParallelHashAggregate { input, .. }
            | PhysicalPlan::ParallelRollupAggregate { input, .. }
            | PhysicalPlan::ParallelGroupingSetsAggregate { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::TopK { input, .. }
            | PhysicalPlan::ParallelTopK { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::Projection { input, .. }
            | PhysicalPlan::Exchange { input, .. } => collect_tables(input, out),
            PhysicalPlan::IndexNestedLoopJoin {
                left, right_table, ..
            } => {
                collect_tables(left, out);
                out.push(right_table.schema.name.clone());
            }
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::AdaptiveJoin { left, right, .. } => {
                collect_tables(left, out);
                collect_tables(right, out);
            }
            _ => {}
        }
    }

    match join {
        PhysicalPlan::IndexNestedLoopJoin {
            left, right_table, ..
        } => {
            let mut outer_tables = Vec::new();
            collect_tables(left, &mut outer_tables);
            let inner = &right_table.schema.name;
            // Either lineitem is on the outer side, or the inner
            // (PK probe) is orders driven by a filtered outer.
            assert!(
                outer_tables.iter().any(|t| t == "lineitem") || inner == "orders",
                "INL plan must drive from filtered lineitem; got outer={outer_tables:?} inner={inner}"
            );
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            build_side,
            ..
        } => {
            // For hash join, the filtered lineitem should be the
            // build side (smaller filtered cardinality wins).
            let build = match build_side {
                BuildSide::Left => left.as_ref(),
                BuildSide::Right => right.as_ref(),
            };
            let mut build_tables = Vec::new();
            collect_tables(build, &mut build_tables);
            assert!(
                build_tables.iter().any(|t| t == "lineitem"),
                "hash-join build side must be the filtered lineitem; got {build_tables:?}"
            );
        }
        other => panic!(
            "Q12 plan must be INL or hash join, got: {}",
            std::any::type_name_of_val(other)
        ),
    }
}

fn col(qual: &str, name: &str) -> EqColumnKey {
    (Some(qual.to_string()), name.to_string())
}

fn extract_eq_pair(expr: &Expr) -> Option<(EqColumnKey, EqColumnKey)> {
    let Expr::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    } = expr
    else {
        return None;
    };
    Some((
        equivalence_column_key(left)?,
        equivalence_column_key(right)?,
    ))
}

#[test]
fn synthesize_transitive_equalities_emits_missing_class_pairs() {
    // Equivalence class {a.k, b.k, c.k} given by `a.k = b.k AND
    // b.k = c.k`. Closure should synthesize `a.k = c.k` so the
    // join-graph builder has a direct edge between a and c.
    let mut adj: HashMap<EqColumnKey, Vec<EqColumnKey>> = HashMap::new();
    adj.entry(col("a", "k")).or_default().push(col("b", "k"));
    adj.entry(col("b", "k")).or_default().push(col("a", "k"));
    adj.entry(col("b", "k")).or_default().push(col("c", "k"));
    adj.entry(col("c", "k")).or_default().push(col("b", "k"));

    let mut existing: HashSet<(EqColumnKey, EqColumnKey)> = HashSet::new();
    existing.insert((col("a", "k"), col("b", "k")));
    existing.insert((col("b", "k"), col("a", "k")));
    existing.insert((col("b", "k"), col("c", "k")));
    existing.insert((col("c", "k"), col("b", "k")));

    let mut out: Vec<Expr> = Vec::new();
    synthesize_transitive_equalities(&adj, &existing, &mut out);

    assert_eq!(out.len(), 1, "expected exactly one synthesized pair");
    let pair = extract_eq_pair(&out[0]).expect("synthesized pred is col=col");
    // Sort the synthesized pair so the assertion is direction-free.
    let mut as_tuple = [pair.0, pair.1];
    as_tuple.sort();
    assert_eq!(as_tuple, [col("a", "k"), col("c", "k")]);
}

#[test]
fn synthesize_transitive_equalities_skips_existing_pairs() {
    // All three pairs already written — closure must be a no-op.
    let mut adj: HashMap<EqColumnKey, Vec<EqColumnKey>> = HashMap::new();
    for (l, r) in [
        (col("a", "k"), col("b", "k")),
        (col("b", "k"), col("c", "k")),
        (col("a", "k"), col("c", "k")),
    ] {
        adj.entry(l.clone()).or_default().push(r.clone());
        adj.entry(r).or_default().push(l);
    }
    let mut existing: HashSet<(EqColumnKey, EqColumnKey)> = HashSet::new();
    for (l, r) in [
        (col("a", "k"), col("b", "k")),
        (col("b", "k"), col("c", "k")),
        (col("a", "k"), col("c", "k")),
    ] {
        existing.insert((l.clone(), r.clone()));
        existing.insert((r, l));
    }
    let mut out: Vec<Expr> = Vec::new();
    synthesize_transitive_equalities(&adj, &existing, &mut out);
    assert!(out.is_empty(), "no new predicates expected, got {out:?}");
}

#[test]
fn synthesize_transitive_equalities_handles_q5_shape() {
    // TPC-H Q5 chain: c_nationkey = s_nationkey AND s_nationkey =
    // n_nationkey. The synthesized `c_nationkey = n_nationkey`
    // edge is the one that unlocks the customer ⋈ nation ⋈ region
    // plan and slashes the lineitem probe size.
    let mut adj: HashMap<EqColumnKey, Vec<EqColumnKey>> = HashMap::new();
    let c = col("customer", "c_nationkey");
    let s = col("supplier", "s_nationkey");
    let n = col("nation", "n_nationkey");
    for (l, r) in [(c.clone(), s.clone()), (s.clone(), n.clone())] {
        adj.entry(l.clone()).or_default().push(r.clone());
        adj.entry(r).or_default().push(l);
    }
    let mut existing: HashSet<(EqColumnKey, EqColumnKey)> = HashSet::new();
    for (l, r) in [(c.clone(), s.clone()), (s.clone(), n.clone())] {
        existing.insert((l.clone(), r.clone()));
        existing.insert((r, l));
    }
    let mut out: Vec<Expr> = Vec::new();
    synthesize_transitive_equalities(&adj, &existing, &mut out);
    assert_eq!(out.len(), 1);
    let pair = extract_eq_pair(&out[0]).unwrap();
    let mut as_tuple = [pair.0, pair.1];
    as_tuple.sort();
    assert_eq!(as_tuple, [c, n]);
}

#[test]
fn synthesize_transitive_equalities_independent_classes_stay_isolated() {
    // Two disconnected pairs `{a.k, b.k}` and `{c.k, d.k}` — neither
    // class has ≥3 members, so no new edges should appear (and
    // certainly no edge between the two classes).
    let mut adj: HashMap<EqColumnKey, Vec<EqColumnKey>> = HashMap::new();
    for (l, r) in [
        (col("a", "k"), col("b", "k")),
        (col("c", "k"), col("d", "k")),
    ] {
        adj.entry(l.clone()).or_default().push(r.clone());
        adj.entry(r).or_default().push(l);
    }
    let mut existing: HashSet<(EqColumnKey, EqColumnKey)> = HashSet::new();
    for (l, r) in [
        (col("a", "k"), col("b", "k")),
        (col("c", "k"), col("d", "k")),
    ] {
        existing.insert((l.clone(), r.clone()));
        existing.insert((r, l));
    }
    let mut out: Vec<Expr> = Vec::new();
    synthesize_transitive_equalities(&adj, &existing, &mut out);
    assert!(out.is_empty(), "no synthesis across disjoint pairs");
}

#[test]
fn heap_fetching_index_parallelism_uses_cost_not_only_row_count() {
    let scattered_range = crate::engine::cbo::Cost::from_components(
        1.19, 10_730.15, 200.05, 37_909.93, 1_583.30, 0.0, 0.0,
    );
    assert!(
        cbo_wants_parallel_for_index_range_scan(67_913.0, 67_913.0, scattered_range, 4),
        "a sub-threshold range with expensive heap probes should use the calibrated cost"
    );

    let tiny_range =
        crate::engine::cbo::Cost::from_components(1.0, 100.0, 0.0, 8.0, 20.0, 0.0, 0.0);
    assert!(
        !cbo_wants_parallel_for_index_range_scan(100.0, 100.0, tiny_range, 4),
        "parallel setup cost must keep genuinely small literal ranges serial"
    );
}

#[test]
fn grace_hash_unique_lookup_risk_is_zero_without_spill_and_scales_by_bytes() {
    let resident = crate::engine::cbo::Cost::scalar(10.0);
    assert_eq!(grace_hash_unique_lookup_risk_surcharge(resident), 0.0);

    let one_page = crate::engine::cbo::Cost {
        spill_bytes: (16 * 1024) as f64,
        ..resident
    };
    let ten_pages = crate::engine::cbo::Cost {
        spill_bytes: (10 * 16 * 1024) as f64,
        ..resident
    };
    let small = grace_hash_unique_lookup_risk_surcharge(one_page);
    let large = grace_hash_unique_lookup_risk_surcharge(ten_pages);
    assert!(small > 0.0);
    assert_eq!(large, small * 10.0);
}

#[test]
fn grace_unique_lookup_risk_requires_severe_build_overflow() {
    let budget = 256_u64 * 1024 * 1024;
    let below = crate::engine::cbo::Cost {
        spill_bytes: budget as f64 * (SEVERE_GRACE_BUILD_OVERFLOW - 0.01),
        ..crate::engine::cbo::Cost::ZERO
    };
    let boundary = crate::engine::cbo::Cost {
        spill_bytes: budget as f64 * SEVERE_GRACE_BUILD_OVERFLOW,
        ..crate::engine::cbo::Cost::ZERO
    };
    assert!(!severe_grace_build_overflow(below, budget));
    assert!(severe_grace_build_overflow(boundary, budget));
}

#[test]
fn equijoin_joint_ndv_missing_on_unindexed_side_is_bounded_by_base_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE tuple_fact (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE TABLE tuple_dim (x INT NOT NULL, y INT NOT NULL, PRIMARY KEY(x,y))")
        .unwrap();
    let facts = (1..=1000)
        .map(|i| format!("({i},{0},{0})", i % 100))
        .collect::<Vec<_>>()
        .join(",");
    let dims = (0..100)
        .map(|i| format!("({i},{i})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO tuple_fact VALUES {facts}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO tuple_dim VALUES {dims}"))
        .unwrap();
    engine.execute("ANALYZE TABLE tuple_fact").unwrap();
    engine.execute("ANALYZE TABLE tuple_dim").unwrap();
    let scan = |sql| {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            unreachable!()
        };
        build_physical_plan(engine.db_for_tests(), &select).unwrap()
    };
    let fact = scan("SELECT * FROM tuple_fact f");
    let dim = scan("SELECT * FROM tuple_dim d");
    let fk = [
        Expr::CompoundIdentifier(vec!["f".into(), "a".into()]),
        Expr::CompoundIdentifier(vec!["f".into(), "b".into()]),
    ];
    let dk = [
        Expr::CompoundIdentifier(vec!["d".into(), "x".into()]),
        Expr::CompoundIdentifier(vec!["d".into(), "y".into()]),
    ];
    assert!(composite_ndv_in_plan(&fact, &fk).is_none());
    assert!(multi_column_ndv_in_plan(&fact, &fk).is_none());
    for (left, right, lk, rk, lr, rr) in [
        (&fact, &dim, &fk, &dk, 1000.0, 100.0),
        (&dim, &fact, &dk, &fk, 100.0, 1000.0),
    ] {
        let rows = estimate_equijoin_rows(left, right, lk, rk, lr, rr);
        // Unknown tuple NDV cannot exceed the 1000 base rows. Do not
        // invent a foreign-key constraint: this is a bound, not an exact
        // claim that all 1000 facts match the dimension's 100 tuples.
        assert!(
            (rows - 100.0).abs() < 1.0,
            "one-sided joint statistics lost: {rows}"
        );
    }
    let filtered = scan("SELECT * FROM tuple_fact f WHERE id <= 10");
    let rows = estimate_equijoin_rows(&filtered, &dim, &fk, &dk, 10.0, 100.0);
    assert!(
        (rows - 1.0).abs() < 0.01,
        "unrelated filter must not shrink the base tuple universe: {rows}"
    );
    let reversed = estimate_equijoin_rows(
        &fact,
        &dim,
        &[fk[1].clone(), fk[0].clone()],
        &[dk[1].clone(), dk[0].clone()],
        1000.0,
        100.0,
    );
    assert!(
        (reversed - 100.0).abs() < 1.0,
        "key order changed joint estimate: {reversed}"
    );
}

#[test]
fn uncollected_joint_ndv_preserves_independence_and_relation_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE grid_fact (id INT PRIMARY KEY, a INT, b VARCHAR(8))")
        .unwrap();
    let values = (0..1000)
        .map(|i| format!("({i},{},'v{}')", i % 10, (i / 10) % 10))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO grid_fact VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE grid_fact").unwrap();
    let scan = |sql| {
        let Statement::Select(select) = parse_statement(sql).unwrap() else {
            unreachable!()
        };
        build_physical_plan(engine.db_for_tests(), &select).unwrap()
    };
    let key =
        |alias: &str, column: &str| Expr::CompoundIdentifier(vec![alias.into(), column.into()]);
    let fact = scan("SELECT * FROM grid_fact f");
    assert_eq!(
        uncollected_joint_ndv_in_plan(&fact, &[key("f", "a"), key("f", "b")]),
        Some(100)
    );
    let cross = scan("SELECT * FROM grid_fact f CROSS JOIN grid_fact g");
    assert_eq!(
        uncollected_joint_ndv_in_plan(&cross, &[key("f", "a"), key("g", "b")]),
        None
    );
    assert_eq!(
        uncollected_joint_ndv_in_plan(
            &fact,
            &[key("f", "a"), Expr::Literal(crate::parser::Literal::Int(1))]
        ),
        None
    );
}

#[path = "column_validation_tests.rs"]
mod column_validation_tests;

#[test]
fn theta_join_range_predicates_use_range_selectivity() {
    let mut model = crate::engine::cbo::CboContext::defaults_for_test().model;
    model.default_join_selectivity = 0.002;
    model.default_range_selectivity = 0.25;
    for predicate in [
        "a.v > b.v",
        "a.v >= b.v",
        "a.v < b.v",
        "a.v <= b.v",
        "a.ship_date > b.sold_date + 5",
        "a.price > ABS(b.cost)",
        "a.label < b.label",
        "CAST(a.v AS DECIMAL(20,2)) <= b.v",
    ] {
        let expr = parsed_filter(&format!("SELECT 1 WHERE {predicate}"));
        assert_eq!(theta_join_selectivity(&expr, &model), 0.25, "{predicate}");
    }
    for predicate in ["a.v=b.v", "a.v<=>b.v", "a.v=b.v AND a.k=b.k"] {
        let expr = parsed_filter(&format!("SELECT 1 WHERE {predicate}"));
        assert_eq!(theta_join_selectivity(&expr, &model), 0.002, "{predicate}");
    }
    let not_equal = parsed_filter("SELECT 1 WHERE a.v<>b.v");
    assert_eq!(theta_join_selectivity(&not_equal, &model), 0.998);
    let dnf = parsed_filter("SELECT 1 WHERE (a.k='E' AND b.k='W') OR (a.k='W' AND b.k='E')");
    assert_eq!(theta_join_selectivity(&dnf, &model), 0.5);
}

#[test]
fn theta_join_cardinality_reaches_parent_costing_and_preserves_outer_floors() {
    let mut ctx = crate::engine::cbo::CboContext::defaults_for_test();
    ctx.model.default_join_selectivity = 0.002;
    ctx.model.default_range_selectivity = 0.25;
    let input = |rows: f64| PhysicalPlan::ValuesScan {
        rows: vec![],
        columns: vec![],
        filter: None,
        estimated_rows: rows,
        cost: crate::engine::cbo::Cost::scalar(rows),
    };
    let predicate = parsed_filter("SELECT 1 WHERE a.x > b.x + 5");
    let inner = choose_join_with_hints(
        &ctx,
        input(100.0),
        input(200.0),
        JoinType::Inner,
        Some(predicate.clone()),
        &[],
    );
    assert_eq!(estimated_rows(&inner), 5_000.0);
    for (kind, left, right, expected) in [
        (JoinType::Left, 100.0, 1.0, 100.0),
        (JoinType::Right, 1.0, 100.0, 100.0),
        (JoinType::Full, 1.0, 100.0, 100.0),
        (JoinType::Inner, 0.0, 100.0, 0.0),
    ] {
        let plan = choose_join_with_hints(
            &ctx,
            input(left),
            input(right),
            kind,
            Some(predicate.clone()),
            &[],
        );
        assert_eq!(estimated_rows(&plan), expected);
    }
}

mod parameter_ranges;
#[test]
fn serial_producer_parallel_reduction_preserves_producer_cost_and_small_plan_policy() {
    crate::util::env::with_vars(
        &[
            ("MYDB_PARALLEL_DEGREE", Some("4")),
            ("MYDB_PARALLEL_DISABLE", Some("0")),
            ("MYDB_PARALLEL_THRESHOLD_ROWS", Some("10000")),
        ],
        || {
            for (rows, producer, total, parallel) in [
                (100_000.0, 10_000.0, 1_000_000.0, true),
                (100_000.0, 1_000_000.0, 1_000_001.0, false),
                (10.0, 1.0, 1_000_000.0, false),
            ] {
                let mut plan = PhysicalPlan::HashAggregate {
                    input: Box::new(PhysicalPlan::ValuesScan {
                        rows: Vec::new(),
                        columns: Vec::new(),
                        filter: None,
                        estimated_rows: rows,
                        cost: crate::engine::cbo::Cost::scalar(producer),
                    }),
                    group_by: vec![Expr::Identifier("k".into())],
                    having: None,
                    aggregates: vec![Expr::Function {
                        name: "AVG".into(),
                        args: vec![Expr::Identifier("v".into())],
                        distinct: false,
                    }],
                    estimated_rows: 100.0,
                    cost: crate::engine::cbo::Cost {
                        startup: total,
                        total,
                        ..crate::engine::cbo::Cost::ZERO
                    },
                };
                super::parallelize_fused_aggregate(&mut plan);
                assert_eq!(
                    matches!(plan, PhysicalPlan::ParallelHashAggregate { degree: 4, .. }),
                    parallel
                );
                assert!(super::plan_cost(&plan).total >= producer);
                if parallel {
                    assert!(super::plan_cost(&plan).total < total * 0.9);
                }
            }
        },
    );
}
