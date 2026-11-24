use super::*;

fn fixture() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,a INT,b VARCHAR(20),d DATE,h VARBINARY(16))",
        "INSERT INTO t VALUES(1,1,'alpha','2026-01-01',X'AB'),(2,2,'beta','2026-01-02',X'BC'),(3,3,'alpha','2026-01-03',X'CD')",
    ] {
        engine.execute(sql).unwrap();
    }
    (dir, engine)
}

fn result(engine: &StorageQueryEngine, sql: &str) -> ResultSet {
    match engine.execute(sql).unwrap().materialize().unwrap() {
        QueryResponse::ResultSet(rs) => rs,
        other => panic!("{sql}: {other:?}"),
    }
}

#[test]
fn release_audit_1123_grouped_boundaries() {
    let (_dir, e) = fixture();
    for aggregate in [
        "GROUP_CONCAT(b)",
        "GROUP_CONCAT(b ORDER BY id SEPARATOR '|')",
        "GROUP_CONCAT(DISTINCT a,':',b ORDER BY a)",
        "MAX(a)",
        "COUNT(*)",
        "JSON_ARRAYAGG(b)",
    ] {
        e.execute(&format!(
            "CREATE OR REPLACE VIEW v AS SELECT b,{aggregate} gc FROM t GROUP BY b"
        ))
        .unwrap();
        let expected = result(&e, "SELECT * FROM v ORDER BY b");
        for sql in [
            "SELECT * FROM (SELECT * FROM v) d WHERE b IS NOT NULL ORDER BY b",
            "WITH c AS (SELECT * FROM v) SELECT * FROM c WHERE b IS NOT NULL ORDER BY b",
            "SELECT * FROM (SELECT * FROM v) d GROUP BY b,gc HAVING b IS NOT NULL ORDER BY b",
            "SELECT * FROM (SELECT DISTINCT b,gc FROM v) d ORDER BY b",
        ] {
            let actual = result(&e, sql);
            assert_eq!(actual.columns.len(), 2, "{aggregate}: {sql}");
            assert_eq!(actual.rows, expected.rows, "{aggregate}: {sql}");
        }
    }
}

#[test]
fn release_audit_1123_distinct_aggregate_view() {
    let (_dir, e) = fixture();
    let body = "SELECT DISTINCT b,MAX(a) m FROM t GROUP BY b";
    let expected = result(&e, &format!("{body} ORDER BY b")).rows;
    e.execute(&format!("CREATE VIEW v AS {body}")).unwrap();
    assert_eq!(result(&e, "SELECT * FROM v ORDER BY b").rows, expected);
}

#[test]
fn release_audit_1123_shared_cursor() {
    let (_dir, e) = fixture();
    e.execute("CREATE VIEW v AS SELECT id,a FROM t ORDER BY id LIMIT 5 OFFSET 1")
        .unwrap();
    for _ in 0..4 {
        assert_eq!(
            result(
                &e,
                "SELECT id FROM (SELECT * FROM v) d ORDER BY id,a LIMIT 4"
            )
            .rows,
            vec![vec![Value::Int(2)], vec![Value::Int(3)]]
        );
    }
}

#[test]
fn release_audit_1123_view_ctas_types() {
    let (_dir, e) = fixture();
    e.execute("CREATE VIEW v AS SELECT id,DATE_FORMAT(d,'%Y-%m-%d') df,HEX(h) hx FROM t")
        .unwrap();
    e.execute("CREATE TABLE ct AS SELECT * FROM v").unwrap();
    assert_eq!(
        result(&e, "SELECT df,hx FROM ct WHERE id=1").rows,
        vec![vec![
            Value::String("2026-01-01".into()),
            Value::String("AB".into())
        ]]
    );
    e.execute("CREATE VIEW vr AS SELECT id,COUNT(*) n FROM t GROUP BY id WITH ROLLUP")
        .unwrap();
    e.execute("CREATE TABLE ctr AS SELECT * FROM vr").unwrap();
    assert_eq!(
        result(&e, "SELECT n FROM ctr WHERE id IS NULL").rows,
        vec![vec![Value::Int(3)]]
    );
}

#[test]
fn release_audit_1123_lateral_view() {
    let (_dir, e) = fixture();
    let body = "SELECT t.id,s.x FROM t,LATERAL (SELECT a x FROM t u WHERE u.id=t.id LIMIT 1) s";
    let expected = result(&e, body).rows;
    e.execute(&format!("CREATE VIEW v AS {body}")).unwrap();
    assert_eq!(result(&e, "SELECT * FROM v").rows, expected);
}

#[test]
fn release_audit_1123_replace_join_view_is_atomic() {
    let (_dir, e) = fixture();
    e.execute("CREATE VIEW v AS SELECT t.id,t.a,u.b FROM t JOIN t u ON t.id=u.id")
        .unwrap();
    let before = result(&e, "SELECT * FROM t ORDER BY id").rows;
    let err = e.execute("REPLACE INTO v(id,a) VALUES(1,44)").unwrap_err();
    assert_eq!(err.code(), 1395);
    assert_eq!(result(&e, "SELECT * FROM t ORDER BY id").rows, before);
}

#[test]
fn release_audit_1123_comma_join_precedence() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE empty_t(id INT)").unwrap();
    assert!(
        result(&e, "SELECT * FROM empty_t,t RIGHT JOIN t u ON t.id=u.id")
            .rows
            .is_empty()
    );
    assert_eq!(
        result(&e, "SELECT COUNT(*) FROM t CROSS JOIN (t u,t v)").rows,
        vec![vec![Value::Int(27)]]
    );
    assert!(e
        .execute("SELECT t.id FROM t,t u LEFT JOIN t v ON t.id=v.id")
        .is_err());
}

#[test]
fn release_audit_1123_show_metadata_and_diagnostics() {
    let (_dir, e) = fixture();
    let mut session = SessionContext::default();
    for sql in [
        "SHOW GRANTS",
        "SHOW GRANTS FOR CURRENT_USER",
        "SHOW CREATE USER CURRENT_USER",
        "SHOW SEQUENCES",
        "SHOW TABLES LIKE 't%'",
        "SHOW DATABASES LIKE 't%'",
    ] {
        let prepared = e.describe_prepared_statement(&mut session, sql).unwrap();
        assert_eq!(prepared, result(&e, sql).columns, "{sql}");
    }
    for sql in [
        "/*+ NO_RANGE */ SHOW WARNINGS",
        "SHOW COUNT( * ) WARNINGS",
        "SHOW COUNT(*)WARNINGS",
        "SHOW WARNINGS LIMIT 2 OFFSET 0",
        "SHOW/*+ NO_RANGE */WARNINGS",
        "SHOW COUNT/*hint*/( * ) WARNINGS",
    ] {
        e.execute_with_session(&mut session, "SELECT 1/0")
            .unwrap()
            .materialize()
            .unwrap();
        let r = e
            .execute_with_session(&mut session, sql)
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(r) = r else {
            panic!("{sql}")
        };
        assert_eq!(r.rows.len(), 1, "{sql}");
        if r.columns.len() == 1 {
            assert_eq!(r.rows[0][0].to_display_string(), "1", "{sql}");
        }
    }
    for sql in ["SHOW", "SHOW TABLE", "SHOW DATABASE", "SHOW VARIABLES2"] {
        assert_eq!(e.execute(sql).unwrap_err().code(), 1064, "{sql}");
    }
    assert_eq!(e.execute("SHOW PROFILE CPU").unwrap_err().code(), 1235);
}

#[test]
fn release_audit_1123_interval_and_sequence_replay() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE iv(id INT,d DATE,nxt DATE AS (DATE_ADD(d,INTERVAL 1 DAY)),KEY k((DATE_ADD(d,INTERVAL 2 DAY))),CHECK(nxt<DATE_ADD(d,INTERVAL 3 DAY)))").unwrap();
    let ddl = result(&e, "SHOW CREATE TABLE iv").rows[0][1].to_display_string();
    assert!(!ddl.contains("__INTERVAL"), "{ddl}");
    e.execute(
        &ddl.replacen("`iv`", "`iv_copy`", 1)
            .replace("`iv_chk_1`", "`iv_copy_chk_1`"),
    )
    .unwrap();
    e.execute("INSERT INTO iv_copy(id,d) VALUES(1,'2026-01-01')")
        .unwrap();
    assert_eq!(
        result(&e, "SELECT nxt FROM iv_copy").rows[0][0].to_display_string(),
        "2026-01-02"
    );
    e.execute("CREATE SEQUENCE seq OWNED BY t.id").unwrap();
    let ddl = result(&e, "SHOW CREATE SEQUENCE seq").rows[0][1].to_display_string();
    assert!(ddl.contains("OWNED BY"));
    e.execute(&ddl.replacen("`seq`", "`seq_copy`", 1)).unwrap();
    e.execute("DROP TABLE t").unwrap();
    assert!(e.execute("SHOW CREATE SEQUENCE seq_copy").is_err());
}

#[test]
fn release_audit_1123_catalog_indexes_paths_and_locks() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE fi(a INT,b INT,UNIQUE KEY uk((a+1),b),KEY k(a))")
        .unwrap();
    let kcu=result(&e,"SELECT column_name,ordinal_position FROM information_schema.key_column_usage WHERE table_schema='test' AND table_name='fi'").rows;
    assert_eq!(kcu.len(), 1);
    assert_eq!(kcu[0][0].to_display_string(), "b");
    assert_eq!(kcu[0][1].to_display_string(), "2");
    assert_eq!(result(&e,"SELECT COUNT(*) FROM information_schema.table_constraints_extensions WHERE constraint_schema='test' AND table_name='fi'").rows[0][0].to_display_string(),"2");
    for sql in [
        "SELECT FILE_NAME FROM information_schema.FILES",
        "SELECT PATH FROM information_schema.INNODB_DATAFILES",
        "SELECT PATH FROM information_schema.INNODB_TABLESPACES_BRIEF",
    ] {
        for row in result(&e, sql).rows {
            assert!(row[0].to_display_string().starts_with("./"), "{row:?}");
        }
    }
    for (sql, code) in [
        (
            "SELECT * FROM information_schema.tables LIMIT 1 FOR UPDATE",
            1142,
        ),
        (
            "SELECT * FROM information_schema.tables LIMIT 1 FOR SHARE",
            3550,
        ),
        ("LOCK TABLES information_schema.tables READ", 1044),
        ("LOCK TABLES performance_schema.global_status READ", 1142),
        ("SELECT * FROM mysql.user LIMIT 1 FOR UPDATE", 1235),
        ("ALTER DATABASE mysql CHARACTER SET utf8mb4", 3552),
    ] {
        assert_eq!(e.execute(sql).unwrap_err().code(), code, "{sql}");
    }
    assert!(!result(
        &e,
        "SELECT * FROM performance_schema.global_status LIMIT 1 FOR SHARE"
    )
    .rows
    .is_empty());
}

#[test]
fn release_audit_1123_committed_update_time() {
    let (_dir, e) = fixture();
    let time = || {
        result(&e,"SELECT UPDATE_TIME FROM information_schema.tables WHERE table_schema='test' AND table_name='t'").rows[0][0].clone()
    };
    assert!(!matches!(time(), Value::Null));
    let before = time();
    let table_id = e.db.table("test.t").unwrap().id;
    let before_ms = e.db.table_update_time_ms(table_id).unwrap();
    let mut session = SessionContext::default();
    e.execute_with_session(&mut session, "BEGIN").unwrap();
    e.execute_with_session(&mut session, "UPDATE t SET a=a+1 WHERE id=1")
        .unwrap();
    assert_eq!(time(), before);
    e.execute_with_session(&mut session, "ROLLBACK").unwrap();
    assert_eq!(time(), before);
    e.execute("ANALYZE TABLE t").unwrap();
    assert_eq!(time(), before);
    let show = result(&e, "SHOW TABLE STATUS LIKE 't'");
    assert_eq!(show.rows[0][12], before);
    std::thread::sleep(Duration::from_millis(2));
    e.execute("UPDATE t SET a=a+1 WHERE id=1").unwrap();
    let committed = e.db.table_update_time_ms(table_id).unwrap();
    assert!(committed > before_ms);
    e.execute("UPDATE t SET a=a+1 WHERE id=-1").unwrap();
    assert_eq!(e.db.table_update_time_ms(table_id), Some(committed));
}

#[test]
fn release_audit_1123_account_visibility_and_authentication_redaction() {
    let (_dir, e) = fixture();
    e.execute("CREATE USER 'reader'@'%' IDENTIFIED BY 'secret'")
        .unwrap();
    let mut reader = SessionContext::default();
    reader.user = "reader".into();
    reader.login_user = "reader".into();
    reader.authenticated_as = crate::engine::auth::AccountKey::new("reader", "%");
    reader.auth_epoch = e.db.auth_catalog().epoch;
    assert_eq!(
        e.execute_with_session(&mut reader, "SHOW TRIGGERS FROM test")
            .unwrap_err()
            .code(),
        1044
    );
    let read = |session: &mut SessionContext, sql: &str| {
        let QueryResponse::ResultSet(rs) = e
            .execute_with_session(session, sql)
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("{sql}")
        };
        rs
    };
    let ddl = read(&mut reader, "SHOW CREATE USER CURRENT_USER").rows[0][0].to_display_string();
    assert!(ddl.contains("AS <secret>"), "{ddl}");
    assert!(!ddl.contains("$A$"));
    e.execute("CREATE ROLE 'attr_role'@'%'").unwrap();
    e.execute("GRANT 'attr_role'@'%' TO 'reader'@'%'").unwrap();
    e.execute_with_session(&mut reader, "SET ROLE ALL").unwrap();
    assert_eq!(
        read(
            &mut reader,
            "SELECT * FROM information_schema.user_attributes"
        )
        .rows
        .len(),
        1
    );
    e.execute("GRANT SELECT ON mysql.* TO 'reader'@'%'")
        .unwrap();
    assert!(
        read(
            &mut reader,
            "SELECT * FROM information_schema.user_attributes"
        )
        .rows
        .len()
            > 1
    );
    let ddl = read(&mut reader, "SHOW CREATE USER CURRENT_USER").rows[0][0].to_display_string();
    assert!(!ddl.contains("<secret>"));
}

#[test]
fn release_audit_1123_using_join_view_update() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE u(id INT PRIMARY KEY,x INT)")
        .unwrap();
    e.execute("INSERT INTO u VALUES(1,10),(2,20)").unwrap();
    let mut value = 9;
    for join in ["JOIN u USING(id)", "NATURAL JOIN u"] {
        for projection in ["t.id,t.a,u.x", "*"] {
            value += 1;
            e.execute(&format!(
                "CREATE OR REPLACE VIEW v AS SELECT {projection} FROM t {join}"
            ))
            .unwrap();
            assert_eq!(result(&e, "SELECT IS_UPDATABLE FROM information_schema.views WHERE table_schema='test' AND table_name='v'").rows[0][0].to_display_string(), "YES");
            e.execute(&format!("UPDATE v SET a={value} WHERE id=1"))
                .unwrap();
            assert_eq!(
                result(&e, "SELECT a FROM t WHERE id=1").rows[0][0],
                Value::Int(value)
            );
        }
    }
    e.execute("UPDATE v SET id=7 WHERE id=1").unwrap();
    assert_eq!(
        result(&e, "SELECT a FROM t WHERE id=7").rows,
        vec![vec![Value::Int(value)]]
    );
    assert_eq!(
        result(&e, "SELECT x FROM u WHERE id=1").rows,
        vec![vec![Value::Int(10)]]
    );
}

#[test]
fn release_audit_1123_expression_metadata_domains() {
    let (_dir, e) = fixture();
    for (expr, kind, nullable) in [
        ("COUNT(*)", "bigint", "NO"),
        ("COUNT(DISTINCT a)", "bigint", "NO"),
        ("NULL", "binary(0)", "YES"),
        ("CHAR(65)", "varbinary(4)", "YES"),
        ("YEAR(d)", "int", "YES"),
        ("DATEDIFF(d,d)", "int", "YES"),
        ("EXTRACT(YEAR FROM d)", "int", "YES"),
        ("ORD('😀')", "bigint", "NO"),
        ("ROW_NUMBER() OVER(ORDER BY id)", "bigint unsigned", "NO"),
        ("a DIV -1", "bigint", "YES"),
    ] {
        e.execute(&format!(
            "CREATE OR REPLACE VIEW meta AS SELECT {expr} x FROM t"
        ))
        .unwrap();
        let columns = result(&e, "SHOW COLUMNS FROM meta");
        assert_eq!(columns.rows[0][1].to_display_string(), kind, "{expr}");
        assert_eq!(columns.rows[0][2].to_display_string(), nullable, "{expr}");
    }
    e.execute("UPDATE t SET a=-2147483648 WHERE id=1").unwrap();
    e.execute("CREATE VIEW div_v AS SELECT a DIV -1 x FROM t")
        .unwrap();
    e.execute("CREATE TABLE div_ct AS SELECT * FROM div_v")
        .unwrap();
    assert!(result(&e, "SELECT x FROM div_ct")
        .rows
        .iter()
        .any(|r| r[0].to_display_string() == "2147483648"));
}

#[test]
fn release_audit_1123_text_and_binary_batch_boundaries() {
    let (_dir, e) = fixture();
    for (expr, expected) in [
        ("GROUP_CONCAT(b ORDER BY id)", "alpha,alpha"),
        ("GROUP_CONCAT(CAST(b AS BINARY) ORDER BY id)", "alpha,alpha"),
    ] {
        e.execute(&format!(
            "CREATE OR REPLACE VIEW v AS SELECT b,{expr} gc FROM t GROUP BY b"
        ))
        .unwrap();
        let rows = result(&e, "SELECT * FROM (SELECT * FROM v) d WHERE b LIKE 'a%'").rows;
        assert_eq!(rows[0][1].to_display_string(), expected);
    }
    e.execute("CREATE TABLE binary_t(k INT,b VARBINARY(8))")
        .unwrap();
    e.execute("INSERT INTO binary_t VALUES(1,X'00FF'),(1,X'FE00')")
        .unwrap();
    let rows=result(&e,"SELECT HEX(gc) FROM (SELECT k,GROUP_CONCAT(b ORDER BY b SEPARATOR '') gc FROM binary_t GROUP BY k) d WHERE k IS NOT NULL").rows;
    assert_eq!(rows[0][0].to_display_string(), "00FFFE00");
}

#[test]
fn release_audit_1123_lateral_outer_projection_types() {
    let (_dir, e) = fixture();
    for body in [
        "SELECT t.id,s.oid,s.aa FROM t,LATERAL(SELECT t.id oid,t.a+1 aa) s",
        "SELECT t.id,s.oid,s.aa FROM t,LATERAL(SELECT t.id oid,t.a+1 aa UNION ALL SELECT t.id,t.a+2) s",
    ] {
        e.execute(&format!("CREATE OR REPLACE VIEW v AS {body}")).unwrap();
        e.execute("CREATE TABLE lc AS SELECT * FROM v").unwrap();
        let columns=result(&e,"SHOW COLUMNS FROM lc");
        assert_eq!(columns.rows[1][1].to_display_string(),"int");
        assert_eq!(columns.rows[2][1].to_display_string(),"bigint");
        e.execute("DROP TABLE lc").unwrap();
    }
    e.execute("CREATE TABLE lc AS SELECT s.x FROM (SELECT NULL n) t,LATERAL(SELECT t.n x) s")
        .unwrap();
    assert_eq!(
        result(&e, "SHOW COLUMNS FROM lc").rows[0][1].to_display_string(),
        "binary(0)"
    );
    e.execute("DROP TABLE lc").unwrap();
    e.execute("CREATE TABLE lc AS SELECT s.* FROM t,LATERAL(SELECT t.h,t.b) s")
        .unwrap();
    let columns = result(&e, "SHOW COLUMNS FROM lc");
    assert_eq!(columns.rows[0][1].to_display_string(), "varbinary(16)");
    assert_eq!(columns.rows[1][1].to_display_string(), "varchar(20)");
}

#[test]
fn release_audit_1123_view_catalog_and_event_temporals() {
    let (_dir, e) = fixture();
    e.execute("CREATE TABLE zbase(z TINYINT(3) UNSIGNED ZEROFILL)")
        .unwrap();
    e.execute("CREATE VIEW zv AS SELECT z FROM zbase").unwrap();
    e.execute("CREATE VIEW zv2 AS SELECT z renamed FROM zv")
        .unwrap();
    for name in ["zv", "zv2"] {
        assert_eq!(
            result(&e, &format!("SHOW COLUMNS FROM {name}")).rows[0][1].to_display_string(),
            "tinyint(3) unsigned zerofill"
        );
        assert_eq!(result(&e, &format!("SELECT COLUMN_TYPE FROM information_schema.columns WHERE table_schema='test' AND table_name='{name}'")).rows[0][0].to_display_string(), "tinyint(3) unsigned zerofill");
        assert!(!matches!(
            result(&e, &format!("SHOW TABLE STATUS LIKE '{name}'")).rows[0][11],
            Value::Null
        ));
    }
    e.execute("DROP TABLE zbase").unwrap();
    assert!(result(&e, "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema='test' AND table_name IN ('zv','zv2')").rows.is_empty());
    assert!(result(&e, "SHOW EVENTS FROM nonexistent_release_audit")
        .rows
        .is_empty());
    e.execute("CREATE EVENT ev ON SCHEDULE EVERY 1 DAY STARTS '2099-01-01 12:34:56' ENDS '2099-02-01 12:34:56' DISABLE DO SELECT 1").unwrap();
    let events = result(&e, "SHOW EVENTS");
    for index in [5, 8, 9] {
        assert_eq!(events.columns[index].column_type, ColumnType::DateTime);
    }
    assert_eq!(events.rows[0][8].to_display_string(), "2099-01-01 12:34:56");
    assert_eq!(events.rows[0][9].to_display_string(), "2099-02-01 12:34:56");
    let mut session = SessionContext::default();
    assert_eq!(
        e.describe_prepared_statement(&mut session, "SHOW EVENTS")
            .unwrap(),
        events.columns
    );
    assert_eq!(
        e.describe_prepared_statement(&mut session, "SHOW CREATE EVENT ev")
            .unwrap(),
        result(&e, "SHOW CREATE EVENT ev").columns
    );
}

#[test]
fn release_audit_1123_derived_names_and_order_output() {
    let (_dir, e) = fixture();
    assert_eq!(
        result(
            &e,
            "SELECT d.* FROM (SELECT id FROM t) d JOIN t u ON d.id=u.id ORDER BY id DESC"
        )
        .rows,
        vec![
            vec![Value::Int(3)],
            vec![Value::Int(2)],
            vec![Value::Int(1)]
        ]
    );
    for sql in [
        "SELECT * FROM (SELECT id,id FROM t) d",
        "SELECT * FROM t JOIN (SELECT id x,a x FROM t) d ON t.id=d.x",
        "SELECT * FROM (SELECT 1 x,2 X) d",
    ] {
        assert_eq!(e.execute(sql).unwrap_err().code(), 1060, "{sql}");
    }
    assert_eq!(
        result(&e, "SELECT * FROM (SELECT id,id FROM t) d(x,y) ORDER BY x")
            .rows
            .len(),
        3
    );
    assert_eq!(
        e.execute("SELECT t.*,u.* FROM t JOIN t u ON t.id=u.id ORDER BY id")
            .unwrap_err()
            .code(),
        1052
    );
    assert_eq!(
        result(&e, "SELECT * FROM (t,t u) WHERE t.id=u.id")
            .rows
            .len(),
        3
    );
}

#[test]
fn release_audit_1123_constant_folding_preserves_bound_domain() {
    use crate::engine::{expr, expr_type};
    use crate::parser::{BinaryOp, Expr, Literal};
    let source = crate::types::ColumnSource {
        flags: crate::mysql::protocol::NOT_NULL_FLAG,
        ..Default::default()
    };
    let mut expression = Expr::Binary {
        left: Box::new(expr_type::typed_constant(
            Expr::BoundValue(Value::Int(1)),
            ColumnType::Long,
            &source,
        )),
        op: BinaryOp::Add,
        right: Box::new(Expr::Literal(Literal::Int(1))),
    };
    let no_column = |_: &Expr| None;
    let no_parameter = |_: usize| None;
    let context = expr_type::TypeContext::context_free(&no_column, &no_parameter);
    let before = expr_type::infer_expr_type(&expression, &context);
    assert!(expr::expr_is_constant(&expression));
    expr::fold_constant_subexprs(&mut expression);
    assert_eq!(expr_type::infer_expr_type(&expression, &context), before);
    assert_eq!(expr::eval_const(&expression).unwrap(), Value::Int(2));
    assert!(expr::expr_is_constant(&expression));
}

#[test]
fn release_audit_1123_scalar_lateral_matches_query_execution() {
    let (_dir, e) = fixture();
    e.execute("UPDATE t SET a=NULL,b=NULL,d=NULL,h=NULL WHERE id=2")
        .unwrap();
    e.execute("UPDATE t SET b='😀中',h=X'00FF' WHERE id=3")
        .unwrap();
    for expression in [
        "t.id",
        "t.a+1",
        "t.a/3",
        "CAST(t.a AS DECIMAL(20,4))",
        "HEX(t.h)",
        "DATE_FORMAT(t.d,'%Y-%m-%d')",
        "CONCAT(t.b,'!')",
        "JSON_OBJECT('a',t.a)",
        "COERCIBILITY(t.b)",
        "ORD(t.b)",
        "t.b='ALPHA'",
        "CAST(t.b AS UNSIGNED)",
        "1/0",
        "NULL",
    ] {
        let direct = result(
            &e,
            &format!(
                "SELECT t.id,s.x FROM t CROSS JOIN LATERAL(SELECT {expression} x) s ORDER BY t.id"
            ),
        );
        let query = result(&e, &format!("SELECT t.id,s.x FROM t CROSS JOIN LATERAL(SELECT {expression} x LIMIT 1) s ORDER BY t.id"));
        assert_eq!(direct.rows, query.rows, "{expression}");
        assert_eq!(direct.columns, query.columns, "{expression}");
    }
    let mut session = SessionContext::default();
    let run = |session: &mut SessionContext, sql: &str| {
        let QueryResponse::ResultSet(rs) = e
            .execute_with_session(session, sql)
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("{sql}");
        };
        rs
    };
    for expression in ["CAST(t.b AS UNSIGNED)", "1/0"] {
        run(
            &mut session,
            &format!("SELECT s.x FROM t,LATERAL(SELECT {expression} x) s"),
        );
        let direct = run(&mut session, "SHOW COUNT(*) WARNINGS").rows;
        run(
            &mut session,
            &format!("SELECT s.x FROM t,LATERAL(SELECT {expression} x LIMIT 1) s"),
        );
        let fallback = run(&mut session, "SHOW COUNT(*) WARNINGS").rows;
        assert_eq!(direct, fallback, "{expression}");
    }
    for expression in [
        "GREATEST(t.b COLLATE utf8mb4_bin,t.b COLLATE utf8mb4_general_ci)",
        "t.b REGEXP BINARY '^A'",
    ] {
        let direct = e
            .execute(&format!(
                "SELECT s.x FROM t,LATERAL(SELECT {expression} x) s"
            ))
            .and_then(QueryResponse::materialize)
            .unwrap_err();
        let fallback = e
            .execute(&format!(
                "SELECT s.x FROM t,LATERAL(SELECT {expression} x LIMIT 1) s"
            ))
            .and_then(QueryResponse::materialize)
            .unwrap_err();
        assert_eq!(direct.code(), fallback.code(), "{expression}");
    }
    assert_eq!(
        result(
            &e,
            "SELECT t.id,s.x FROM t LEFT JOIN LATERAL(SELECT t.a+1 x) s ON s.x>3 ORDER BY t.id"
        )
        .rows,
        vec![
            vec![Value::Int(1), Value::Null],
            vec![Value::Int(2), Value::Null],
            vec![Value::Int(3), Value::Int(4)]
        ]
    );
    e.execute("DELETE FROM t").unwrap();
    assert!(result(
        &e,
        "SELECT t.id,s.x FROM t CROSS JOIN LATERAL(SELECT t.a+1 x) s"
    )
    .rows
    .is_empty());
}
