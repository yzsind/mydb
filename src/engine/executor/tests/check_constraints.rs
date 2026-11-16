use super::support::result_rows as rows;
use super::*;
use crate::engine::params::with_params;

#[test]
fn check_constraints_enforce_and_surface_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE t (\
             id INT PRIMARY KEY, \
             v INT, \
             CONSTRAINT v_nonneg CHECK (v >= 0), \
             CHECK (v IS NULL) NOT ENFORCED)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 0), (2, NULL)")
        .unwrap();

    let err = engine.execute("INSERT INTO t VALUES (3, -1)").unwrap_err();
    assert_eq!(err.code(), 3819);
    assert_eq!(err.sql_state(), "HY000");
    assert!(err.message().contains("v_nonneg"));

    let create_sql = rows(engine.execute("SHOW CREATE TABLE t").unwrap())[0][1].to_display_string();
    assert!(create_sql.contains("CONSTRAINT `v_nonneg` CHECK"));
    assert!(create_sql.contains("CONSTRAINT `t_chk_1` CHECK"));
    assert!(create_sql.contains("NOT ENFORCED"));

    let constraint_rows = rows(
        engine
            .execute(
                "SELECT constraint_name, constraint_type, enforced \
                 FROM information_schema.table_constraints \
                 WHERE table_schema = 'test' AND table_name = 't' \
                   AND constraint_type = 'CHECK' \
                 ORDER BY constraint_name",
            )
            .unwrap(),
    );
    assert_eq!(
        constraint_rows,
        vec![
            vec![
                Value::String("t_chk_1".into()),
                Value::String("CHECK".into()),
                Value::String("NO".into()),
            ],
            vec![
                Value::String("v_nonneg".into()),
                Value::String("CHECK".into()),
                Value::String("YES".into()),
            ],
        ]
    );

    let check_rows = rows(
        engine
            .execute(
                "SELECT constraint_name, check_clause \
                 FROM information_schema.check_constraints \
                 WHERE constraint_schema = 'test' \
                 ORDER BY constraint_name",
            )
            .unwrap(),
    );
    assert_eq!(check_rows.len(), 2);
    assert_eq!(check_rows[0][0], Value::String("t_chk_1".into()));
    assert_eq!(check_rows[1][0], Value::String("v_nonneg".into()));
}

#[test]
fn ignore_check_violations_skip_rows_and_record_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v INT, CONSTRAINT v_pos CHECK (v > 0))",
        )
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                "INSERT IGNORE INTO t VALUES (1, 10), (2, -20), (3, 30)",
            )
            .unwrap(),
        QueryResponse::OkAffected { affected_rows: 2 }
    );
    assert_eq!(session.warning_count(), 1);

    let warning_rows = rows(
        engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap(),
    );
    assert_eq!(warning_rows.len(), 1);
    assert_eq!(warning_rows[0][1], Value::Int(3819));
    assert!(warning_rows[0][2].to_display_string().contains("v_pos"));

    assert_eq!(
        rows(
            engine
                .execute_with_session(&mut session, "SELECT id, v FROM t ORDER BY id")
                .unwrap(),
        ),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(3), Value::Int(30)],
        ]
    );

    assert_eq!(
        engine
            .execute_with_session(&mut session, "UPDATE IGNORE t SET v = -5 WHERE id > 0")
            .unwrap(),
        QueryResponse::OkAffected { affected_rows: 0 }
    );
    assert_eq!(session.warning_count(), 2);
    let warning_rows = rows(
        engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap(),
    );
    assert_eq!(warning_rows.len(), 2);
    assert!(warning_rows.iter().all(|row| row[1] == Value::Int(3819)));
}

#[test]
fn prepared_ignore_check_violation_records_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v INT, CONSTRAINT v_pos CHECK (v > 0))",
        )
        .unwrap();

    let sql = "INSERT IGNORE INTO t VALUES (?, ?)";
    let prepared = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let stmt = (*prepared).clone();
    let response = with_params(vec![Value::Int(1), Value::Int(-10)], || {
        engine
            .execute_prepared_statement(&mut session, sql, stmt)
            .unwrap()
            .unwrap()
    })
    .materialize()
    .unwrap();
    assert_eq!(response, QueryResponse::OkAffected { affected_rows: 0 });
    assert_eq!(session.warning_count(), 1);

    let warning_rows = rows(
        engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap(),
    );
    assert_eq!(warning_rows.len(), 1);
    assert_eq!(warning_rows[0][1], Value::Int(3819));
    assert!(warning_rows[0][2].to_display_string().contains("v_pos"));
}

#[test]
fn load_data_ignore_check_violations_records_warning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT PRIMARY KEY, v INT, CONSTRAINT v_pos CHECK (v > 0))",
        )
        .unwrap();
    let data_path = dir.path().join("checks.tsv");
    std::fs::write(&data_path, "1\t10\n2\t-20\n3\t30\n").unwrap();

    let sql = format!(
        "LOAD DATA INFILE '{}' IGNORE INTO TABLE t",
        crate::util::file::sql_path(&data_path)
    );
    assert_eq!(
        engine.execute_with_session(&mut session, &sql).unwrap(),
        QueryResponse::OkAffected { affected_rows: 2 }
    );
    assert_eq!(session.warning_count(), 1);
    assert_eq!(
        rows(
            engine
                .execute_with_session(&mut session, "SELECT id FROM t ORDER BY id")
                .unwrap(),
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(3)]]
    );
}

#[test]
fn alter_check_validates_existing_rows_and_can_drop() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, -1)").unwrap();

    let err = engine
        .execute("ALTER TABLE t ADD CONSTRAINT v_positive CHECK (v > 0)")
        .unwrap_err();
    assert_eq!(err.code(), 3819);

    engine
        .execute("ALTER TABLE t ADD CONSTRAINT v_positive CHECK (v > 0) NOT ENFORCED")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE t ALTER CHECK v_positive ENFORCED")
        .unwrap_err();
    assert_eq!(err.code(), 3819);

    engine.execute("UPDATE t SET v = 1 WHERE id = 1").unwrap();
    engine
        .execute("ALTER TABLE t ALTER CHECK v_positive ENFORCED")
        .unwrap();
    let err = engine.execute("INSERT INTO t VALUES (2, -2)").unwrap_err();
    assert_eq!(err.code(), 3819);

    engine
        .execute("ALTER TABLE t DROP CHECK v_positive")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (2, -2)").unwrap();
}

#[test]
fn create_table_like_regenerates_check_names() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE src (\
             id INT, \
             CONSTRAINT explicit CHECK (src.id > 0) NOT ENFORCED, \
             CHECK (src.id < 10))",
        )
        .unwrap();
    engine.execute("CREATE TABLE dst LIKE src").unwrap();

    let create_sql =
        rows(engine.execute("SHOW CREATE TABLE dst").unwrap())[0][1].to_display_string();
    assert!(create_sql.contains("CONSTRAINT `dst_chk_1` CHECK"));
    assert!(create_sql.contains("CONSTRAINT `dst_chk_2` CHECK"));
    assert!(!create_sql.contains("explicit"));
    assert!(!create_sql.contains("src_chk"));
    assert!(!create_sql.contains("`src`.`id`"));
    assert!(create_sql.contains("`dst`.`id`"));

    engine.execute("INSERT INTO dst VALUES (-1)").unwrap();
    let err = engine.execute("INSERT INTO dst VALUES (11)").unwrap_err();
    assert_eq!(err.code(), 3819);
    assert!(err.message().contains("dst_chk_2"));
}

#[test]
fn column_check_cannot_reference_other_columns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let err = engine
        .execute("CREATE TABLE bad (a INT CHECK (b > 0), b INT)")
        .unwrap_err();
    assert_eq!(err.code(), 3813);
}

#[test]
fn check_constraint_rejects_unsupported_expressions_and_duplicate_names() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    let err = engine
        .execute("CREATE TABLE bad_auto (id INT AUTO_INCREMENT, CHECK (id > 0), PRIMARY KEY (id))")
        .unwrap_err();
    assert_eq!(err.code(), 3815);

    let err = engine
        .execute("CREATE TABLE bad_func (v INT, CHECK (unknown_func(v) > 0))")
        .unwrap_err();
    assert_eq!(err.code(), 3815);

    let err = engine
        .execute("CREATE TABLE bad_subquery (v INT, CHECK (v > (SELECT 0)))")
        .unwrap_err();
    assert_eq!(err.code(), 3815);

    engine
        .execute("CREATE TABLE a (v INT, CONSTRAINT same_name CHECK (v > 0))")
        .unwrap();
    let err = engine
        .execute("CREATE TABLE b (v INT, CONSTRAINT same_name CHECK (v > 0))")
        .unwrap_err();
    assert_eq!(err.code(), 3822);
}

#[test]
fn check_constraint_like_escape_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (v VARCHAR(20), CHECK (v LIKE 'a!_%' ESCAPE '!'))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES ('a_x')").unwrap();
    let err = engine.execute("INSERT INTO t VALUES ('abx')").unwrap_err();
    assert_eq!(err.code(), 3819);

    let create_sql = rows(engine.execute("SHOW CREATE TABLE t").unwrap())[0][1].to_display_string();
    assert!(create_sql.contains("ESCAPE"));
}
