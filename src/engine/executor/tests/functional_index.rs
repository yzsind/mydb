use super::*;

#[test]
fn refactor_functional_index_numeric_types_match_expression_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE numeric_index_types (
        id INT PRIMARY KEY, d DOUBLE, n DECIMAL(12,3), i INT,
        INDEX idx_double ((ABS(d))), INDEX idx_decimal ((ABS(n))),
        INDEX idx_int ((ABS(i))))",
        )
        .unwrap();
    let table = engine
        .db_for_tests()
        .table("test.numeric_index_types")
        .unwrap();
    for (name, expected) in [
        (
            "idx_double",
            crate::storage::DataTypeKind::Double {
                precision: None,
                scale: None,
            },
        ),
        (
            "idx_decimal",
            crate::storage::DataTypeKind::Decimal {
                precision: Some(12),
                scale: Some(3),
            },
        ),
        ("idx_int", crate::storage::DataTypeKind::Int),
    ] {
        let index = table
            .indexes
            .iter()
            .find(|index| index.name == name)
            .unwrap();
        assert_eq!(
            index.key_expression(0).unwrap().data_type,
            expected,
            "{name}"
        );
    }
    engine
        .execute("INSERT INTO numeric_index_types VALUES (1,-1.125,-2.375,-7),(2,1.625,2.875,8)")
        .unwrap();
    for (index, predicate) in [
        ("idx_double", "ABS(d)=1.125"),
        ("idx_decimal", "ABS(n)=2.375"),
        ("idx_int", "ABS(i)=7"),
    ] {
        let sql =
            format!("SELECT id FROM numeric_index_types FORCE INDEX ({index}) WHERE {predicate}");
        assert_eq!(
            rows_to_strings(exec_set(&engine, &sql)),
            vec![vec!["1".to_string()]]
        );
    }
}

fn rows_to_strings(rs: ResultSet) -> Vec<Vec<String>> {
    rs.rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|v| match v {
                    Value::Null => "NULL".to_string(),
                    Value::String(s) => s,
                    other => other.to_display_string(),
                })
                .collect()
        })
        .collect()
}

fn exec_set(engine: &StorageQueryEngine, sql: &str) -> ResultSet {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    rs
}

#[test]
fn startup_rebuilds_missing_functional_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = memory_test_storage_config();
    config.durability = crate::storage::DurabilityMode::Relaxed;
    config.startup_validation = crate::storage::StartupValidationMode::Auto;
    config.startup_validate_indexes = true;
    let open = || {
        StorageQueryEngine::open_with_storage_config_without_background_workers(
            dir.path(),
            config.clone(),
        )
        .unwrap()
    };
    let index_path;
    {
        let engine = open();
        engine
            .execute(
                "CREATE TABLE functional_recovery (
            id INT PRIMARY KEY, int_col INT, integer_col INT, email VARCHAR(64),
            INDEX idx_sum ((int_col + integer_col)),
            INDEX idx_lower ((LOWER(email)), id),
            INDEX idx_unsigned ((CAST(int_col AS UNSIGNED))))",
            )
            .unwrap();
        engine
            .execute(
                "INSERT INTO functional_recovery VALUES
            (1, 10, 20, 'ALICE'), (2, 30, 40, 'BOB'), (3, NULL, 5, NULL)",
            )
            .unwrap();
        let db = engine.db_for_tests();
        let table = db.table("test.functional_recovery").unwrap();
        let index = table.indexes.iter().find(|i| i.name == "idx_sum").unwrap();
        index_path = crate::storage::naming::index_object_path_by_id(
            dir.path(),
            table.id,
            table.active_generation,
            index.index_id,
            "idx",
        );
        db.checkpoint().unwrap();
    }
    std::fs::remove_file(&index_path).unwrap();
    for _ in 0..2 {
        let engine = open();
        assert!(index_path.exists());
        for (index, predicate, expected) in [
            ("idx_sum", "int_col + integer_col = 30", "1"),
            ("idx_lower", "LOWER(email) = 'bob'", "2"),
            ("idx_unsigned", "CAST(int_col AS UNSIGNED) = 30", "2"),
            ("idx_sum", "int_col + integer_col IS NULL", "3"),
        ] {
            let sql = format!(
                "SELECT id FROM functional_recovery FORCE INDEX ({index}) WHERE {predicate}"
            );
            assert_eq!(
                rows_to_strings(exec_set(&engine, &sql)),
                vec![vec![expected.to_string()]]
            );
        }
        engine.db_for_tests().checkpoint().unwrap();
    }
}

#[test]
fn create_table_with_functional_index_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_email_lower ((LOWER(email))))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO users VALUES (1, 'Alice@Example.COM')")
        .unwrap();
    engine
        .execute("INSERT INTO users VALUES (2, 'BOB@example.com')")
        .unwrap();
    let rs = exec_set(
        &engine,
        "SELECT id FROM users WHERE LOWER(email) = 'alice@example.com'",
    );
    assert_eq!(rows_to_strings(rs), vec![vec!["1".to_string()]]);
}

#[test]
fn alter_table_add_functional_index_backfills_existing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, created_at DATETIME)")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1, '2024-01-15 12:00:00')")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (2, '2025-03-20 09:30:00')")
        .unwrap();
    engine
        .execute("ALTER TABLE orders ADD INDEX idx_year ((YEAR(created_at)))")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (3, '2025-11-01 00:00:00')")
        .unwrap();
    let rs = exec_set(
        &engine,
        "SELECT id FROM orders WHERE YEAR(created_at) = 2025 ORDER BY id",
    );
    let actual = rows_to_strings(rs);
    assert_eq!(
        actual,
        vec![vec!["2".to_string()], vec!["3".to_string()]],
        "expected rows 2 and 3 (year 2025)"
    );
}

#[test]
fn create_index_on_functional_expression() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO items VALUES (1, 'WIDGET')")
        .unwrap();
    engine
        .execute("INSERT INTO items VALUES (2, 'gadget')")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_lower_name ON items ((LOWER(name)))")
        .unwrap();
    let rs = exec_set(&engine, "SELECT id FROM items WHERE LOWER(name) = 'widget'");
    assert_eq!(rows_to_strings(rs), vec![vec!["1".to_string()]]);
}

#[test]
fn update_maintains_functional_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE accounts (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO accounts VALUES (1, 'OLD@a.com')")
        .unwrap();
    engine
        .execute("UPDATE accounts SET email = 'New@A.COM' WHERE id = 1")
        .unwrap();
    let rs = exec_set(
        &engine,
        "SELECT id FROM accounts WHERE LOWER(email) = 'new@a.com'",
    );
    assert_eq!(rows_to_strings(rs), vec![vec!["1".to_string()]]);
    let rs = exec_set(
        &engine,
        "SELECT id FROM accounts WHERE LOWER(email) = 'old@a.com'",
    );
    assert!(rs.rows.is_empty(), "old key must be gone");
}

#[test]
fn nondeterministic_function_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    let err = engine
        .execute("CREATE INDEX idx_bad ON t ((NOW()))")
        .unwrap_err();
    assert!(
        err.message().to_lowercase().contains("nondeterministic")
            || err.message().to_lowercase().contains("does not reference"),
        "expected rejection, got {err:?}",
    );
}

#[test]
fn show_index_marks_functional_part() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    let rs = exec_set(&engine, "SHOW INDEX FROM u");
    // Result columns from `show_index_columns`:
    //   [Table, Non_unique, Key_name, Seq_in_index, Column_name, Collation,
    //    Cardinality, Sub_part, Packed, Null, Index_type, Comment,
    //    Index_comment, Visible, Expression]
    let lower_row = rs
        .rows
        .iter()
        .find(|row| matches!(&row[2], Value::String(s) if s == "idx_lower"))
        .expect("idx_lower row missing");
    assert!(
        matches!(lower_row[4], Value::Null),
        "Column_name should be NULL"
    );
    let Value::String(expr) = &lower_row[14] else {
        panic!(
            "Expression column must be a string, got {:?}",
            lower_row[14]
        );
    };
    assert!(
        expr.to_ascii_lowercase().contains("lower(`email`)"),
        "Expression should round-trip LOWER(email), got {expr:?}"
    );
}

#[test]
fn information_schema_statistics_reports_expression() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, name VARCHAR(64), \
             INDEX idx_upper ((UPPER(name))))",
        )
        .unwrap();
    let rs = exec_set(
        &engine,
        "SELECT COLUMN_NAME, EXPRESSION FROM information_schema.statistics \
         WHERE TABLE_NAME = 'u' AND INDEX_NAME = 'idx_upper'",
    );
    assert_eq!(rs.rows.len(), 1, "expected one statistics row");
    let row = &rs.rows[0];
    assert!(matches!(row[0], Value::Null), "COLUMN_NAME should be NULL");
    let Value::String(expr) = &row[1] else {
        panic!("EXPRESSION column must be a string, got {:?}", row[1]);
    };
    assert!(
        expr.to_ascii_lowercase().contains("upper(`name`)"),
        "EXPRESSION should contain UPPER(name), got {expr:?}"
    );
}

#[test]
fn explain_uses_functional_index_for_matching_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE accts (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    // Push enough rows so the optimizer prefers an index lookup over a
    // seq scan even at modest cardinalities. ANALYZE updates the stats
    // the planner reads.
    for i in 0..200 {
        engine
            .execute(&format!(
                "INSERT INTO accts VALUES ({i}, 'user{i}@example.com')"
            ))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE accts").unwrap();
    let rs = exec_set(
        &engine,
        "EXPLAIN SELECT id FROM accts WHERE LOWER(email) = 'user42@example.com'",
    );
    let mut found_index = false;
    for row in &rs.rows {
        for cell in row {
            if let Value::String(s) = cell {
                if s.contains("idx_lower") {
                    found_index = true;
                }
            }
        }
    }
    assert!(
        found_index,
        "expected EXPLAIN to mention idx_lower, got rows: {:?}",
        rs.rows
    );
}

#[test]
fn show_create_table_round_trips_functional_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    let rs = exec_set(&engine, "SHOW CREATE TABLE u");
    let Value::String(ddl) = &rs.rows[0][1] else {
        panic!("expected DDL string");
    };
    assert!(
        ddl.contains("((lower(`email`)))") || ddl.contains("((LOWER(`email`)))"),
        "expected functional index in DDL, got: {ddl}"
    );
}

#[test]
fn explain_uses_functional_index_for_range_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE log (id INT PRIMARY KEY, ts DATETIME, \
             INDEX idx_year ((YEAR(ts))))",
        )
        .unwrap();
    for y in 2018..=2026 {
        for d in 1..=10 {
            engine
                .execute(&format!(
                    "INSERT INTO log VALUES ({}, '{y}-03-{d:02} 00:00:00')",
                    y * 100 + d
                ))
                .unwrap();
        }
    }
    engine.execute("ANALYZE TABLE log").unwrap();
    let rs = exec_set(
        &engine,
        "EXPLAIN SELECT id FROM log WHERE YEAR(ts) BETWEEN 2024 AND 2025",
    );
    let mentions_idx = rs.rows.iter().any(|row| {
        row.iter()
            .any(|cell| matches!(cell, Value::String(s) if s.contains("idx_year")))
    });
    assert!(
        mentions_idx,
        "EXPLAIN should mention idx_year for BETWEEN; rows: {:?}",
        rs.rows
    );
    // Also verify correctness of the answer.
    let rs = exec_set(
        &engine,
        "SELECT COUNT(*) FROM log WHERE YEAR(ts) BETWEEN 2024 AND 2025",
    );
    assert_eq!(
        rs.rows,
        vec![vec![Value::Int(20)]],
        "expected two years × 10 rows = 20"
    );
}

#[test]
fn explain_uses_functional_index_for_in_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    for i in 0..30 {
        engine
            .execute(&format!("INSERT INTO u VALUES ({i}, 'user{i}@x.com')"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE u").unwrap();
    let rs = exec_set(
        &engine,
        "EXPLAIN SELECT id FROM u WHERE LOWER(email) IN ('user1@x.com','user5@x.com')",
    );
    let mentions_idx = rs.rows.iter().any(|row| {
        row.iter()
            .any(|cell| matches!(cell, Value::String(s) if s.contains("idx_lower")))
    });
    assert!(
        mentions_idx,
        "EXPLAIN should mention idx_lower for IN; rows: {:?}",
        rs.rows
    );
    let rs = exec_set(
        &engine,
        "SELECT id FROM u WHERE LOWER(email) IN ('user1@x.com','user5@x.com') ORDER BY id",
    );
    assert_eq!(rs.rows.len(), 2);
}

#[test]
fn unique_functional_index_enforces_uniqueness() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE acct (id INT PRIMARY KEY, email VARCHAR(64), \
             UNIQUE KEY idx_lower_email ((LOWER(email))))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO acct VALUES (1, 'Alice@example.com')")
        .unwrap();
    // Different case but identical under LOWER — must collide.
    let err = engine
        .execute("INSERT INTO acct VALUES (2, 'ALICE@EXAMPLE.COM')")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1062,
        "expected duplicate-entry error, got {err:?}"
    );
}

#[test]
fn alter_table_add_unique_functional_index_rejects_existing_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE acct (id INT PRIMARY KEY, email VARCHAR(64))")
        .unwrap();
    engine
        .execute("INSERT INTO acct VALUES (1, 'a@x.com')")
        .unwrap();
    engine
        .execute("INSERT INTO acct VALUES (2, 'A@x.com')")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE acct ADD UNIQUE KEY idx_lower ((LOWER(email)))")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1062,
        "expected duplicate-entry error, got {err:?}"
    );
}

#[test]
fn drop_column_blocked_by_functional_index_dependency() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    let err = engine
        .execute("ALTER TABLE u DROP COLUMN email")
        .unwrap_err();
    assert_eq!(err.code(), 1553, "expected 1553, got {err:?}");
}

#[test]
fn rename_column_blocked_by_functional_index_dependency() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    let err = engine
        .execute("ALTER TABLE u RENAME COLUMN email TO email_addr")
        .unwrap_err();
    assert_eq!(err.code(), 3855, "expected 3855, got {err:?}");
}

#[test]
fn order_by_uses_functional_index_to_skip_sort() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    for i in 0..50 {
        engine
            .execute(&format!("INSERT INTO u VALUES ({i}, 'X{i}@x.com')"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE u").unwrap();
    let rs = exec_set(
        &engine,
        "EXPLAIN SELECT id FROM u ORDER BY LOWER(email) LIMIT 5",
    );
    let mentions_idx = rs.rows.iter().any(|row| {
        row.iter()
            .any(|cell| matches!(cell, Value::String(s) if s.contains("idx_lower")))
    });
    let has_sort = rs.rows.iter().any(|row| {
        row.iter()
            .any(|cell| matches!(cell, Value::String(s) if s.to_ascii_lowercase().contains("sort")))
    });
    assert!(
        mentions_idx,
        "EXPLAIN should mention idx_lower for ORDER BY; rows: {:?}",
        rs.rows
    );
    assert!(
        !has_sort,
        "ORDER BY on functional index should avoid explicit Sort; rows: {:?}",
        rs.rows
    );
}

#[test]
fn like_prefix_drives_functional_index_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, name VARCHAR(64), \
             INDEX idx_lower ((LOWER(name))))",
        )
        .unwrap();
    let names = [
        "Alpha", "Apple", "Beta", "BRAVO", "Carol", "alpha", "alphabet",
    ];
    for (i, n) in names.iter().enumerate() {
        engine
            .execute(&format!("INSERT INTO u VALUES ({i}, '{n}')"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE u").unwrap();
    let rs = exec_set(
        &engine,
        "EXPLAIN SELECT id FROM u WHERE LOWER(name) LIKE 'a%'",
    );
    let mentions_idx = rs.rows.iter().any(|row| {
        row.iter()
            .any(|cell| matches!(cell, Value::String(s) if s.contains("idx_lower")))
    });
    assert!(
        mentions_idx,
        "EXPLAIN should mention idx_lower for LIKE; rows: {:?}",
        rs.rows
    );
    // 'Alpha','Apple','alpha','alphabet' all lower-case start with 'a'.
    let rs = exec_set(
        &engine,
        "SELECT COUNT(*) FROM u WHERE LOWER(name) LIKE 'a%'",
    );
    assert_eq!(rs.rows, vec![vec![Value::Int(4)]]);
}

#[test]
fn modify_column_blocked_by_functional_index_dependency() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    let err = engine
        .execute("ALTER TABLE u MODIFY COLUMN email VARCHAR(128)")
        .unwrap_err();
    assert_eq!(err.code(), 3855, "expected 3855, got {err:?}");
}

#[test]
fn explain_labels_functional_index_with_expression_text() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    for i in 0..200 {
        engine
            .execute(&format!("INSERT INTO u VALUES ({i}, 'user{i}@x.com')"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE u").unwrap();
    let rs = exec_set(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT id FROM u WHERE LOWER(email) = 'user42@x.com'",
    );
    let has_expr_text = rs.rows.iter().any(|row| {
        row.iter().any(|cell| {
            matches!(cell, Value::String(s) if s.contains("(lower(`email`))=const") || s.contains("(LOWER(`email`))=const"))
        })
    });
    assert!(
        has_expr_text,
        "EXPLAIN FORMAT=TREE index condition should embed the expression SQL; rows: {:?}",
        rs.rows
    );
}

#[test]
fn analyze_table_records_ndv_over_expression_results() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    // 100 rows that collide pair-wise after LOWER (mixed-case dups
    // for every even id). Expected NDV(LOWER(email)) ≈ 50.
    for i in 0..100 {
        let address = if i % 2 == 0 {
            format!("USER{i}@x.com")
        } else {
            format!("user{}@x.com", i - 1)
        };
        engine
            .execute(&format!("INSERT INTO u VALUES ({i}, '{address}')"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE u").unwrap();
    // information_schema.statistics CARDINALITY reflects
    // prefix_distinct_counts[0] — the planner uses the same number
    // through `index_prefix_cardinality`.
    let rs = exec_set(
        &engine,
        "SELECT CARDINALITY FROM information_schema.statistics \
         WHERE INDEX_NAME = 'idx_lower'",
    );
    let Value::Int(card) = rs.rows[0][0] else {
        panic!("expected cardinality int, got {:?}", rs.rows[0][0]);
    };
    // Allow ±5 slop in case sampling kicks in.
    assert!(
        (45..=55).contains(&card),
        "expected ~50 distinct LOWER(email) values, got {card}"
    );
}

#[test]
fn functional_index_does_not_promote_to_covering_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    for i in 0..200 {
        engine
            .execute(&format!("INSERT INTO u VALUES ({i}, 'User{i}@x.com')"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE u").unwrap();
    // Even when the query reads only LOWER(email), the functional
    // index leaf can't satisfy a covering scan today (the executor
    // would still need to rebuild the base row to evaluate the
    // residual filter). EXPLAIN must not advertise "Index-only".
    let rs = exec_set(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT LOWER(email) FROM u WHERE LOWER(email) = 'user42@x.com'",
    );
    let mentions_index_only = rs.rows.iter().any(|row| {
        row.iter().any(|cell| {
            matches!(cell, Value::String(s) if s.to_ascii_lowercase().contains("index-only"))
        })
    });
    assert!(
        !mentions_index_only,
        "functional index must not be picked as covering; rows: {:?}",
        rs.rows
    );
}

#[test]
fn nested_functional_expression_index_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, raw VARCHAR(64), \
             INDEX idx_norm ((LOWER(TRIM(raw)))))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO u VALUES (1, '  Foo  ')")
        .unwrap();
    engine.execute("INSERT INTO u VALUES (2, 'FOO')").unwrap();
    engine.execute("INSERT INTO u VALUES (3, 'bar  ')").unwrap();
    engine.execute("ANALYZE TABLE u").unwrap();
    let rs = exec_set(
        &engine,
        "SELECT id FROM u WHERE LOWER(TRIM(raw)) = 'foo' ORDER BY id",
    );
    assert_eq!(
        rs.rows,
        vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        "nested expression match should return rows 1 and 2"
    );
    // Multi-column functional expression: CONCAT(c1, '.', c2).
    engine.execute("DROP TABLE u").unwrap();
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, a VARCHAR(16), b VARCHAR(16), \
             INDEX idx_concat ((CONCAT(a, '.', b))))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO u VALUES (1, 'x', 'y')")
        .unwrap();
    engine
        .execute("INSERT INTO u VALUES (2, 'a', 'b')")
        .unwrap();
    let rs = exec_set(&engine, "SELECT id FROM u WHERE CONCAT(a, '.', b) = 'a.b'");
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn show_index_marks_not_null_when_inputs_are_not_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, \
             INDEX idx_lower ((LOWER(name))))",
        )
        .unwrap();
    let rs = exec_set(&engine, "SHOW INDEX FROM u");
    // Columns: [Table, Non_unique, Key_name, Seq_in_index, Column_name,
    //   Collation, Cardinality, Sub_part, Packed, Null, Index_type,
    //   Comment, Index_comment, Visible, Expression]
    let lower_row = rs
        .rows
        .iter()
        .find(|row| matches!(&row[2], Value::String(s) if s == "idx_lower"))
        .expect("idx_lower row missing");
    assert_eq!(
        lower_row[9],
        Value::String("".to_string()),
        "Null column should be empty (not nullable) when input is NOT NULL"
    );
}

#[test]
fn show_index_marks_yes_when_inputs_are_nullable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, name VARCHAR(64), \
             INDEX idx_lower ((LOWER(name))))",
        )
        .unwrap();
    let rs = exec_set(&engine, "SHOW INDEX FROM u");
    let lower_row = rs
        .rows
        .iter()
        .find(|row| matches!(&row[2], Value::String(s) if s == "idx_lower"))
        .expect("idx_lower row missing");
    assert_eq!(
        lower_row[9],
        Value::String("YES".to_string()),
        "Null column should be YES when input is nullable"
    );
}

#[test]
fn composite_index_mixing_column_and_expression() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, status VARCHAR(8), email VARCHAR(64), \
             INDEX idx_mix (status, (LOWER(email))))",
        )
        .unwrap();
    for i in 0..50 {
        let status = if i % 2 == 0 { "active" } else { "off" };
        engine
            .execute(&format!(
                "INSERT INTO u VALUES ({i}, '{status}', 'User{i}@x.com')"
            ))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE u").unwrap();
    // Both parts are needed for an exact lookup. The planner should
    // pick `idx_mix` because both equalities collapse to a key prefix.
    let rs = exec_set(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT id FROM u \
         WHERE status = 'active' AND LOWER(email) = 'user10@x.com'",
    );
    let uses_mix = rs.rows.iter().any(|row| {
        row.iter()
            .any(|cell| matches!(cell, Value::String(s) if s.contains("idx_mix")))
    });
    assert!(
        uses_mix,
        "composite index should be picked; rows: {:?}",
        rs.rows
    );
    let rs = exec_set(
        &engine,
        "SELECT id FROM u WHERE status = 'active' AND LOWER(email) = 'user10@x.com'",
    );
    assert_eq!(rs.rows, vec![vec![Value::Int(10)]]);
}

#[test]
fn unique_composite_index_with_expression_enforces_uniqueness() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE m (id INT PRIMARY KEY, tenant INT, email VARCHAR(64), \
             UNIQUE KEY idx_uniq (tenant, (LOWER(email))))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO m VALUES (1, 1, 'Alice@x.com')")
        .unwrap();
    // Same tenant + same LOWER(email) — collide.
    let err = engine
        .execute("INSERT INTO m VALUES (2, 1, 'ALICE@x.com')")
        .unwrap_err();
    assert_eq!(err.code(), 1062, "expected duplicate-entry, got {err:?}");
    // Different tenant — no collide.
    engine
        .execute("INSERT INTO m VALUES (3, 2, 'alice@x.com')")
        .unwrap();
}

#[test]
fn repeated_lookups_against_functional_index_stay_consistent() {
    // Surrogate for "PREPARE … EXECUTE many times": exercise the
    // SELECT path repeatedly with varying literals so any plan-cache
    // regression around expression-keyed predicates would show up as
    // mismatched results. The session-level COM_STMT_PREPARE flow
    // ultimately calls the same dispatcher, so identical results
    // across N runs is the right invariant to lock down here.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    for i in 0..30 {
        engine
            .execute(&format!("INSERT INTO u VALUES ({i}, 'User{i}@x.com')"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE u").unwrap();
    for i in 0..30 {
        let sql = format!("SELECT id FROM u WHERE LOWER(email) = 'user{i}@x.com'");
        let rs = exec_set(&engine, &sql);
        assert_eq!(rs.rows, vec![vec![Value::Int(i as i64)]], "iteration {i}");
    }
}

#[test]
fn invisible_functional_index_is_not_picked_by_planner() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    for i in 0..200 {
        engine
            .execute(&format!("INSERT INTO u VALUES ({i}, 'User{i}@x.com')"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE u").unwrap();
    engine
        .execute("ALTER TABLE u ALTER INDEX idx_lower INVISIBLE")
        .unwrap();
    // Default-format EXPLAIN row layout:
    //   [0]=id, [1]=select_type, [2]=table, [3]=partitions, [4]=type,
    //   [5]=possible_keys, [6]=key, [7]=key_len, [8]=ref, [9]=rows,
    //   [10]=filtered, [11]=Extra.
    // `possible_keys` may still list invisible indexes (mirroring
    // MySQL); the planner-picked index is column 5.
    let rs = exec_set(
        &engine,
        "EXPLAIN SELECT id FROM u WHERE LOWER(email) = 'user42@x.com'",
    );
    let chosen_is_idx = rs
        .rows
        .iter()
        .any(|row| matches!(&row[6], Value::String(s) if s == "idx_lower"));
    assert!(
        !chosen_is_idx,
        "INVISIBLE functional index must not be in `key`; rows: {:?}",
        rs.rows
    );
    // Reactivate and confirm the planner picks it again.
    engine
        .execute("ALTER TABLE u ALTER INDEX idx_lower VISIBLE")
        .unwrap();
    let rs = exec_set(
        &engine,
        "EXPLAIN SELECT id FROM u WHERE LOWER(email) = 'user42@x.com'",
    );
    let chosen_is_idx = rs
        .rows
        .iter()
        .any(|row| matches!(&row[6], Value::String(s) if s == "idx_lower"));
    assert!(
        chosen_is_idx,
        "VISIBLE functional index should be in `key`; rows: {:?}",
        rs.rows
    );
}

#[test]
fn drop_functional_index_removes_it_from_planner() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    for i in 0..50 {
        engine
            .execute(&format!("INSERT INTO u VALUES ({i}, 'X{i}@x.com')"))
            .unwrap();
    }
    engine.execute("DROP INDEX idx_lower ON u").unwrap();
    // After DROP, SHOW INDEX must no longer list the functional part.
    let rs = exec_set(&engine, "SHOW INDEX FROM u");
    let mentions_lower = rs
        .rows
        .iter()
        .any(|row| matches!(&row[2], Value::String(s) if s == "idx_lower"));
    assert!(
        !mentions_lower,
        "idx_lower must be gone: rows: {:?}",
        rs.rows
    );
    // Data is still queryable through the heap.
    let rs = exec_set(&engine, "SELECT id FROM u WHERE LOWER(email) = 'x10@x.com'");
    assert_eq!(rs.rows, vec![vec![Value::Int(10)]]);
}

#[test]
fn order_by_desc_uses_functional_index_reverse_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, name VARCHAR(64), \
             INDEX idx_lower ((LOWER(name))))",
        )
        .unwrap();
    let names = ["Charlie", "alice", "Bob", "delta", "Echo"];
    for (i, n) in names.iter().enumerate() {
        engine
            .execute(&format!("INSERT INTO u VALUES ({i}, '{n}')"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE u").unwrap();
    let rs = exec_set(
        &engine,
        "EXPLAIN FORMAT=TREE SELECT id FROM u ORDER BY LOWER(name) DESC LIMIT 3",
    );
    let mentions_idx = rs.rows.iter().any(|row| {
        row.iter()
            .any(|cell| matches!(cell, Value::String(s) if s.contains("idx_lower")))
    });
    assert!(
        mentions_idx,
        "ORDER BY DESC LIMIT should still drive idx_lower; rows: {:?}",
        rs.rows
    );
    // Result must come out by LOWER(name) descending: echo, delta,
    // charlie (first three).
    let rs = exec_set(
        &engine,
        "SELECT id FROM u ORDER BY LOWER(name) DESC LIMIT 3",
    );
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(4)], // Echo
            vec![Value::Int(3)], // delta
            vec![Value::Int(0)], // Charlie
        ]
    );
}

#[test]
fn join_uses_functional_index_on_inner_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Outer driver: a few canonical user names.
    engine
        .execute("CREATE TABLE drivers (id INT PRIMARY KEY, lookup VARCHAR(64))")
        .unwrap();
    engine
        .execute("INSERT INTO drivers VALUES (1, 'alice@x.com')")
        .unwrap();
    engine
        .execute("INSERT INTO drivers VALUES (2, 'bob@x.com')")
        .unwrap();
    // Inner big table with mixed-case emails and a functional index.
    engine
        .execute(
            "CREATE TABLE accts (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    for i in 0..500 {
        let s = format!("User{i}@X.com");
        engine
            .execute(&format!("INSERT INTO accts VALUES ({i}, '{s}')"))
            .unwrap();
    }
    engine
        .execute("INSERT INTO accts VALUES (1001, 'ALICE@X.com')")
        .unwrap();
    engine
        .execute("INSERT INTO accts VALUES (1002, 'BOB@X.com')")
        .unwrap();
    engine.execute("ANALYZE TABLE accts").unwrap();
    engine.execute("ANALYZE TABLE drivers").unwrap();
    let rs = exec_set(
        &engine,
        "EXPLAIN FORMAT=TREE \
         SELECT /*+ INDEX_NLJ(accts) */ drivers.id, accts.id FROM drivers \
         JOIN accts ON LOWER(accts.email) = drivers.lookup",
    );
    let uses_idx = rs.rows.iter().any(|row| {
        row.iter()
            .any(|cell| matches!(cell, Value::String(s) if s.contains("idx_lower")))
    });
    assert!(
        uses_idx,
        "JOIN with INDEX_NLJ hint should drive accts.idx_lower; rows: {:?}",
        rs.rows
    );
    // Same predicate, without the hint — correctness is independent
    // of whichever physical join shape the optimizer picks.
    let rs = exec_set(
        &engine,
        "SELECT drivers.id, accts.id FROM drivers \
         JOIN accts ON LOWER(accts.email) = drivers.lookup \
         ORDER BY drivers.id",
    );
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(1001)],
            vec![Value::Int(2), Value::Int(1002)],
        ]
    );
}

#[test]
fn multi_expression_part_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, first VARCHAR(32), last VARCHAR(32), \
             INDEX idx_norm ((LOWER(first)), (LOWER(last))))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO u VALUES (1, 'Alice', 'Smith')")
        .unwrap();
    engine
        .execute("INSERT INTO u VALUES (2, 'ALICE', 'JONES')")
        .unwrap();
    engine
        .execute("INSERT INTO u VALUES (3, 'bob', 'smith')")
        .unwrap();
    engine.execute("ANALYZE TABLE u").unwrap();
    // Equality on both parts.
    let rs = exec_set(
        &engine,
        "SELECT id FROM u WHERE LOWER(first) = 'alice' AND LOWER(last) = 'smith'",
    );
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    // Equality on the leading part only.
    let rs = exec_set(
        &engine,
        "SELECT id FROM u WHERE LOWER(first) = 'alice' ORDER BY id",
    );
    assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    // SHOW INDEX must list both expression parts with non-null
    // Expression columns.
    let rs = exec_set(&engine, "SHOW INDEX FROM u");
    let expressions: Vec<String> = rs
        .rows
        .iter()
        .filter(|row| matches!(&row[2], Value::String(s) if s == "idx_norm"))
        .filter_map(|row| match &row[14] {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(expressions.len(), 2, "expected two functional parts");
    assert!(expressions[0]
        .to_ascii_lowercase()
        .contains("lower(`first`)"));
    assert!(expressions[1]
        .to_ascii_lowercase()
        .contains("lower(`last`)"));
}

#[test]
fn inlj_composite_index_combines_eq_and_expr_range() {
    // Exercise the new INLJ path that pairs an equality on the
    // leading column part with a range on a trailing functional
    // part: `INDEX (tenant, (LOWER(name)))` + ON `tenant = ?` plus
    // `LOWER(name) > ?`. The composite shape is the only way to
    // exercise the "range on expression part" branch in
    // `compile_index_lookup_plan` today, because the planner only
    // considers INLJ when at least one equality anchors the prefix.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE drivers (id INT PRIMARY KEY, tenant INT, threshold VARCHAR(64))")
        .unwrap();
    engine
        .execute("INSERT INTO drivers VALUES (1, 1, 'c')")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE accts (id INT PRIMARY KEY, tenant INT, name VARCHAR(64), \
             INDEX idx_mix (tenant, (LOWER(name))))",
        )
        .unwrap();
    let rows = [
        (1, "alpha"),
        (1, "Bravo"),
        (1, "delta"),
        (2, "alpha"),
        (1, "echo"),
    ];
    for (i, (tenant, n)) in rows.iter().enumerate() {
        engine
            .execute(&format!("INSERT INTO accts VALUES ({i}, {tenant}, '{n}')"))
            .unwrap();
    }
    engine.execute("ANALYZE TABLE accts").unwrap();
    engine.execute("ANALYZE TABLE drivers").unwrap();
    // Correctness over the eq+expr-range composite predicate
    // (tenant=1 AND LOWER(name) > 'c'): the cost model may pick
    // Hash join over IndexNestedLoop for this tiny dataset, but the
    // result must be identical to the row-by-row evaluation either
    // way — that's what locks down the INLJ expr-range code path
    // (it's exercised at compile time even when the optimizer
    // ultimately picks a different plan shape).
    let rs = exec_set(
        &engine,
        "SELECT accts.id FROM drivers \
         JOIN accts ON accts.tenant = drivers.tenant \
         AND LOWER(accts.name) > drivers.threshold \
         ORDER BY accts.id",
    );
    assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(4)]]);
}

#[test]
fn async_add_functional_index_completes_and_serves_queries() {
    // Smaller, focused variant of the async-ADD-INDEX test in
    // `online_ddl.rs` — turn async DDL on, push enough rows that
    // backfill is non-trivial, issue ALTER, wait for completion, then
    // confirm every row is queryable through the functional index.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.database().set_async_ddl(true);
    engine
        .execute("CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64))")
        .unwrap();
    for i in 0..100 {
        engine
            .execute(&format!("INSERT INTO u VALUES ({i}, 'User{i}@x.com')"))
            .unwrap();
    }
    engine
        .execute("ALTER TABLE u ADD INDEX idx_lower ((LOWER(email)))")
        .unwrap();
    // Backfill must have populated the functional index for every
    // pre-existing row (BUILDING → PUBLIC).
    for i in (0..100).step_by(7) {
        let rs = exec_set(
            &engine,
            &format!("SELECT id FROM u WHERE LOWER(email) = 'user{i}@x.com'"),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(i as i64)]], "row {i}");
    }
    // SHOW INDEX should now expose the functional index as PUBLIC.
    let rs = exec_set(&engine, "SHOW INDEX FROM u");
    let row = rs
        .rows
        .iter()
        .find(|r| matches!(&r[2], Value::String(s) if s == "idx_lower"))
        .expect("idx_lower missing post-wait");
    assert!(matches!(row[4], Value::Null), "Column_name must be NULL");
    let Value::String(expr) = &row[14] else {
        panic!("Expression should be a string, got {:?}", row[14]);
    };
    assert!(expr.to_ascii_lowercase().contains("lower(`email`)"));
}

#[test]
fn vectorized_path_matches_row_path_for_functional_index() {
    // Lock down that turning on the vectorized executor doesn't
    // miscompile WHERE / ORDER BY against a functional index — the
    // vec path either routes correctly or falls back to row-based,
    // but the result set must match in either mode.
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE u (id INT PRIMARY KEY, email VARCHAR(64), \
             INDEX idx_lower ((LOWER(email))))",
        )
        .unwrap();
    let values = (0..2_000)
        .map(|i| format!("({i}, 'User{i}@X.com')"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&format!("INSERT INTO u VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE u").unwrap();
    // Baseline under VecMode::Off.
    let baseline = with_vec_mode(VecMode::Off, || {
        let QueryResponse::ResultSet(rs) = engine
            .execute("SELECT id FROM u WHERE LOWER(email) = 'user42@x.com'")
            .unwrap()
        else {
            panic!("expected result set");
        };
        rs.rows
    });
    let vec_on = with_vec_mode(VecMode::On, || {
        let QueryResponse::ResultSet(rs) = engine
            .execute("SELECT id FROM u WHERE LOWER(email) = 'user42@x.com'")
            .unwrap()
        else {
            panic!("expected result set");
        };
        rs.rows
    });
    assert_eq!(baseline, vec_on);
    // ORDER BY consistency too.
    let ordered_baseline = with_vec_mode(VecMode::Off, || {
        let QueryResponse::ResultSet(rs) = engine
            .execute("SELECT id FROM u ORDER BY LOWER(email) LIMIT 5")
            .unwrap()
        else {
            panic!("expected result set");
        };
        rs.rows
    });
    let ordered_vec = with_vec_mode(VecMode::On, || {
        let QueryResponse::ResultSet(rs) = engine
            .execute("SELECT id FROM u ORDER BY LOWER(email) LIMIT 5")
            .unwrap()
        else {
            panic!("expected result set");
        };
        rs.rows
    });
    assert_eq!(ordered_baseline, ordered_vec);
}

#[test]
fn scalar_json_extract_functional_index_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE docs (id INT PRIMARY KEY, payload JSON, \
             INDEX idx_x ((CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, '$.x')) AS SIGNED))))",
        )
        .unwrap();
    engine
        .execute(r#"INSERT INTO docs VALUES (1, '{"x": 42, "y": 1}')"#)
        .unwrap();
    engine
        .execute(r#"INSERT INTO docs VALUES (2, '{"x": 7, "y": 2}')"#)
        .unwrap();
    engine
        .execute(r#"INSERT INTO docs VALUES (3, '{"x": 42, "y": 3}')"#)
        .unwrap();
    engine.execute("ANALYZE TABLE docs").unwrap();
    // Equality lookup over a scalar-cast extracted JSON field — must hit the
    // functional index and return both rows whose `$.x` is 42.
    let rs = exec_set(
        &engine,
        "SELECT id FROM docs WHERE CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, '$.x')) AS SIGNED) = 42 ORDER BY id",
    );
    assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
}

#[test]
fn primary_key_cannot_be_expression() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute("CREATE TABLE t (id INT, PRIMARY KEY ((id + 1)))")
        .unwrap_err();
    assert!(
        err.message().to_lowercase().contains("primary key")
            || err.message().to_lowercase().contains("unsupported"),
        "got {err:?}"
    );
}
