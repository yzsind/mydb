use super::*;

fn parameterized_select(sql: &str) -> Select {
    let mut statement = parse_statement(sql).unwrap();
    number_statement_placeholders(&mut statement, &mut 0);
    let Statement::Select(select) = statement else {
        panic!("expected SELECT");
    };
    select
}

#[test]
fn moving_parameter_intervals_reuse_the_same_cost_tier() {
    let dir = tempfile::Builder::new()
        .prefix("parameter-interval-")
        .tempdir_in("/tmp/mydb")
        .unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE moving_interval (id INT PRIMARY KEY)")
        .unwrap();
    let values = (1..=1000)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO moving_interval VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE moving_interval").unwrap();
    let db = engine.db_for_tests();
    for predicate in [
        "id BETWEEN ? AND ?",
        "id >= ? AND id <= ?",
        "? <= id AND ? >= id",
    ] {
        let select =
            parameterized_select(&format!("SELECT id FROM moving_interval WHERE {predicate}"));
        let signature =
            |a, b| parameter_selectivity_signature(db, &select, &[Value::Int(a), Value::Int(b)]);
        assert_eq!(signature(200, 205), signature(800, 805), "{predicate}");
        assert_ne!(
            signature(200, 205),
            signature(200, 900),
            "broad binding: {predicate}"
        );
        assert_ne!(
            signature(200, 205),
            signature(205, 200),
            "empty binding: {predicate}"
        );
        assert_ne!(
            signature(200, 205),
            signature(-10, -5),
            "outside statistics: {predicate}"
        );
    }
    // Disjunctions describe two half-lines, not one closed interval.
    let select = parameterized_select("SELECT id FROM moving_interval WHERE id >= ? OR id <= ?");
    assert!(
        !parameter_selectivity_signature(db, &select, &[Value::Int(200), Value::Int(205)])
            .contains(":r")
    );
    let select =
        parameterized_select("SELECT id FROM moving_interval WHERE NOT (id >= ? AND id <= ?)");
    assert!(
        !parameter_selectivity_signature(db, &select, &[Value::Int(200), Value::Int(205)])
            .contains(":r")
    );
}

#[test]
fn parameter_interval_tiers_preserve_skew_and_character_domains() {
    let dir = tempfile::Builder::new()
        .prefix("parameter-skew-")
        .tempdir_in("/tmp/mydb")
        .unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE skew_interval (id INT PRIMARY KEY, v INT, s VARCHAR(10) COLLATE utf8mb4_0900_ai_ci, KEY kv(v), KEY ks(s))").unwrap();
    let values = (1..=1000)
        .map(|id| {
            if id <= 900 {
                format!("({id},50,'m')")
            } else {
                format!("({id},{},'x')", id - 900)
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO skew_interval VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE skew_interval").unwrap();
    let db = engine.db_for_tests();
    let select = parameterized_select("SELECT id FROM skew_interval WHERE v >= ? AND v <= ?");
    let signature =
        |a, b| parameter_selectivity_signature(db, &select, &[Value::Int(a), Value::Int(b)]);
    assert_ne!(
        signature(49, 51),
        signature(79, 81),
        "hot and cold intervals need distinct costs"
    );
    let select = parameterized_select("SELECT id FROM skew_interval WHERE s BETWEEN ? AND ?");
    let signature = |a: &str, b: &str| {
        parameter_selectivity_signature(
            db,
            &select,
            &[Value::String(a.into()), Value::String(b.into())],
        )
    };
    assert_eq!(
        signature("M", "M"),
        signature("m", "m"),
        "use the column's collation"
    );
    assert_ne!(
        signature("m", "m"),
        signature("x", "x"),
        "retain text value skew"
    );
}

#[test]
fn static_selectivity_matches_every_signature_it_replaces() {
    let dir = tempfile::Builder::new()
        .prefix("parameter-static-")
        .tempdir_in("/tmp/mydb")
        .unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE static_sel (id INT PRIMARY KEY, grp INT, note VARCHAR(8), KEY (grp))",
        )
        .unwrap();
    let values = (1..=1000)
        .map(|id| format!("({id}, {}, 'n')", id % 3))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO static_sel VALUES {values}"))
        .unwrap();
    engine.execute("ANALYZE TABLE static_sel").unwrap();
    let db = engine.db_for_tests();
    let bindings = [
        vec![Value::Int(1), Value::Int(2)],
        vec![Value::Int(-5), Value::Int(5000)],
        vec![Value::Null, Value::Int(0)],
    ];
    // Unique single-column equality never contributes, whatever the
    // bindings.
    for sql in [
        "SELECT note FROM static_sel WHERE id = ? AND id <> 0 AND ? IS NOT NULL",
        "SELECT note FROM static_sel WHERE ? = id OR id = ?",
    ] {
        let select = parameterized_select(sql);
        assert!(parameter_selectivity_is_static(db, &select), "{sql}");
        for params in &bindings {
            assert_eq!(
                parameter_selectivity_signature(db, &select, params),
                "",
                "{sql}"
            );
        }
    }
    // A skewed or ranged statistics-bearing column is parameter dependent.
    for sql in [
        "SELECT id FROM static_sel WHERE grp = ? AND id > ?",
        "SELECT id FROM static_sel WHERE id BETWEEN ? AND ?",
        "SELECT id FROM static_sel WHERE id = ? AND note = ?",
    ] {
        let select = parameterized_select(sql);
        assert!(!parameter_selectivity_is_static(db, &select), "{sql}");
        assert!(bindings
            .iter()
            .any(|params| !parameter_selectivity_signature(db, &select, params).is_empty()));
    }
}
