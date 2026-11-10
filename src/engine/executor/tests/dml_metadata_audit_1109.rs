use super::support::{execute_materialized as exec, result_rows};
use super::*;

fn fixture() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    (dir, engine, SessionContext::default())
}

fn rows(engine: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    result_rows(exec(engine, s, sql).unwrap())
}

#[test]
fn audit1109_locking_read_applies_offset_once() {
    let (_dir, e, mut s) = fixture();
    exec(&e, &mut s, "CREATE TABLE t(k INT PRIMARY KEY)").unwrap();
    exec(&e, &mut s, "INSERT INTO t VALUES (1),(2),(3),(4),(5)").unwrap();
    for predicate in ["", "WHERE k>0"] {
        for lock in ["FOR UPDATE", "FOR SHARE"] {
            assert_eq!(
                rows(
                    &e,
                    &mut s,
                    &format!("SELECT k FROM t {predicate} ORDER BY k LIMIT 2 OFFSET 1 {lock}")
                ),
                vec![vec![Value::Int(2)], vec![Value::Int(3)]]
            );
            assert_eq!(
                rows(
                    &e,
                    &mut s,
                    &format!("SELECT k FROM t {predicate} ORDER BY k LIMIT 2 OFFSET 4 {lock}")
                ),
                vec![vec![Value::Int(5)]]
            );
        }
    }
    exec(
        &e,
        &mut s,
        "PREPARE p FROM 'SELECT k FROM t WHERE k>0 ORDER BY k LIMIT ?,? FOR UPDATE'",
    )
    .unwrap();
    exec(&e, &mut s, "SET @o=1,@l=2").unwrap();
    assert_eq!(
        rows(&e, &mut s, "EXECUTE p USING @o,@l"),
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT (SELECT k FROM t WHERE k>0 ORDER BY k LIMIT 1 OFFSET 1 FOR UPDATE)"
        ),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn audit1109_dml_in_subquery_rejects_multiple_columns_even_when_empty() {
    let (_dir, e, mut s) = fixture();
    exec(&e, &mut s, "CREATE TABLE t(k INT PRIMARY KEY,v INT)").unwrap();
    exec(&e, &mut s, "CREATE TABLE u(a INT,b INT)").unwrap();
    exec(&e, &mut s, "INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    for populated in [false, true] {
        if populated {
            exec(&e, &mut s, "INSERT INTO u VALUES (1,99)").unwrap();
        }
        for sql in [
            "UPDATE t SET v=99 WHERE k IN (SELECT a,b FROM u)",
            "DELETE FROM t WHERE k NOT IN (SELECT a,b FROM u)",
        ] {
            assert_eq!(exec(&e, &mut s, sql).unwrap_err().code(), 1241, "{sql}");
        }
        assert_eq!(
            rows(&e, &mut s, "SELECT * FROM t ORDER BY k"),
            vec![
                vec![Value::Int(1), Value::Int(10)],
                vec![Value::Int(2), Value::Int(20)]
            ]
        );
    }
}

#[test]
fn audit1109_partition_update_reuses_released_unique_key() {
    let (_dir, e, mut s) = fixture();
    exec(&e, &mut s, "CREATE TABLE t(id INT,a INT,p INT,PRIMARY KEY(id,p),UNIQUE KEY uk(a,p)) PARTITION BY HASH(p) PARTITIONS 4").unwrap();
    exec(&e, &mut s, "INSERT INTO t VALUES (1,10,1),(2,20,1)").unwrap();
    exec(&e, &mut s, "UPDATE t SET p=IF(id=1,3,p),a=IF(id=2,10,a)").unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Int(3)],
            vec![Value::Int(2), Value::Int(10), Value::Int(1)]
        ]
    );
    assert_eq!(
        exec(&e, &mut s, "UPDATE t SET p=3").unwrap_err().code(),
        1062
    );
    assert_eq!(
        rows(&e, &mut s, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn audit1109_multitable_trigger_sees_coerced_new_values() {
    let (_dir, e, mut s) = fixture();
    for sql in ["CREATE TABLE t(id INT PRIMARY KEY,n INT)","CREATE TABLE u(id INT PRIMARY KEY)","CREATE TABLE log(v VARCHAR(30))", "INSERT INTO t VALUES (1,1)","INSERT INTO u VALUES (1)","CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW INSERT INTO log VALUES (CONCAT('n=',NEW.n))", "UPDATE t JOIN u ON t.id=u.id SET t.n='042'"] { exec(&e, &mut s, sql).unwrap(); }
    assert_eq!(
        rows(&e, &mut s, "SELECT v FROM log"),
        vec![vec![Value::String("n=42".into())]]
    );
}

#[test]
fn audit1109_load_quoted_temporals_have_typed_index_keys() {
    let (dir, e, mut s) = fixture();
    let path = dir.path().join("dates.csv");
    std::fs::write(&path, "1,\"2024-03-05\",\"12:34:56\",\"2024-03-05 12:34:56\",\"2024\"\n2,\"2024-03-06\",\"12:34:57\",\"2024-03-06 12:34:57\",\"2025\"\n").unwrap();
    exec(&e, &mut s, "CREATE TABLE t(id INT PRIMARY KEY,d DATE,tm TIME,dt DATETIME,y YEAR,KEY kd(d),KEY kt(tm),KEY kdt(dt),KEY ky(y))").unwrap();
    exec(
        &e,
        &mut s,
        &format!(
            "LOAD DATA INFILE '{}' INTO TABLE t FIELDS TERMINATED BY ',' ENCLOSED BY '\"'",
            crate::util::file::sql_path(&path)
        ),
    )
    .unwrap();
    for (key, predicate) in [
        ("kd", "d='2024-03-05'"),
        ("kt", "tm='12:34:56'"),
        ("kdt", "dt='2024-03-05 12:34:56'"),
        ("ky", "y=2024"),
    ] {
        assert_eq!(
            rows(
                &e,
                &mut s,
                &format!("SELECT id FROM t FORCE INDEX({key}) WHERE {predicate}")
            ),
            vec![vec![Value::Int(1)]]
        );
    }
}

#[test]
fn audit1109_load_escaped_delimiter_generates_identity() {
    let (dir, e, mut s) = fixture();
    let path = dir.path().join("ids.txt");
    let input = (0..3000)
        .map(|i| format!("x{i}\\|7|0\n"))
        .collect::<String>();
    std::fs::write(&path, input).unwrap();
    exec(
        &e,
        &mut s,
        "CREATE TABLE t(v VARCHAR(30),id INT AUTO_INCREMENT PRIMARY KEY)",
    )
    .unwrap();
    exec(
        &e,
        &mut s,
        &format!(
            "LOAD DATA INFILE '{}' INTO TABLE t FIELDS TERMINATED BY '|'",
            crate::util::file::sql_path(&path)
        ),
    )
    .unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT COUNT(*),MIN(id),MAX(id) FROM t"),
        vec![vec![Value::Int(3000), Value::Int(1), Value::Int(3000)]]
    );
    exec(&e, &mut s, "TRUNCATE TABLE t").unwrap();
    let input = (1..3000)
        .map(|i| format!("y{i}|{i}\n"))
        .chain(std::iter::once("x0\\|7|0\n".to_string()))
        .collect::<String>();
    std::fs::write(&path, input).unwrap();
    exec(
        &e,
        &mut s,
        &format!(
            "LOAD DATA INFILE '{}' INTO TABLE t FIELDS TERMINATED BY '|'",
            crate::util::file::sql_path(&path)
        ),
    )
    .unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT COUNT(*) FROM t WHERE id=0"),
        vec![vec![Value::Int(0)]]
    );
}

#[test]
fn audit1109_view_column_metadata_retains_source_and_expression_types() {
    let (_dir, e, mut s) = fixture();
    exec(&e,&mut s,"CREATE TABLE t(id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,name VARCHAR(64) NOT NULL)").unwrap();
    exec(
        &e,
        &mut s,
        "CREATE VIEW v AS SELECT id,id+1 AS n,UPPER(name) AS u FROM t",
    )
    .unwrap();
    assert_eq!(rows(&e,&mut s,"SELECT COLUMN_NAME,COLUMN_DEFAULT,IS_NULLABLE,COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='v' ORDER BY ORDINAL_POSITION"),vec![
        vec![Value::String("id".into()),Value::String("0".into()),Value::String("NO".into()),Value::String("bigint unsigned".into())],
        vec![Value::String("n".into()),Value::String("0".into()),Value::String("NO".into()),Value::String("bigint unsigned".into())],
        vec![Value::String("u".into()),Value::String("".into()),Value::String("NO".into()),Value::String("varchar(64)".into())],
    ]);
}

#[test]
fn audit1109_information_schema_producer_values() {
    let (_dir, e, mut s) = fixture();
    for sql in ["CREATE TABLE t(a MEDIUMINT UNSIGNED,ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)","CREATE VIEW v AS SELECT a FROM t","CREATE PROCEDURE p() SELECT 1"] {exec(&e,&mut s,sql).unwrap();}
    assert_eq!(rows(&e,&mut s,"SELECT NUMERIC_PRECISION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t' AND COLUMN_NAME='a'"),vec![vec![Value::Int(7)]]);
    assert_eq!(rows(&e,&mut s,"SELECT EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='t' AND COLUMN_NAME='ts'"),vec![vec![Value::String("DEFAULT_GENERATED on update CURRENT_TIMESTAMP".into())]]);
    assert_eq!(rows(&e,&mut s,"SELECT DTD_IDENTIFIER FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA='test' AND ROUTINE_NAME='p'"),vec![vec![Value::Null]]);
    assert_eq!(rows(&e,&mut s,"SELECT TABLE_ROWS,CREATE_TIME IS NULL,NODEGROUP FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='v'"),vec![vec![Value::Int(0),Value::Int(0),Value::String("".into())]]);
    assert_eq!(rows(&e,&mut s,"SELECT VERSION FROM information_schema.TABLES WHERE TABLE_SCHEMA='information_schema' AND TABLE_NAME='COLUMNS'"),vec![vec![Value::Int(10)]]);
}

#[test]
fn audit1109_routine_character_metadata_inherits_database_charset() {
    let (_dir, e, mut s) = fixture();
    for sql in [
        "CREATE DATABASE u3 CHARSET utf8mb3",
        "USE u3",
        "CREATE PROCEDURE p(IN s VARCHAR(10),IN b TEXT) SELECT 1",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    assert_eq!(rows(&e,&mut s,"SELECT CHARACTER_OCTET_LENGTH,CHARACTER_SET_NAME,COLLATION_NAME FROM information_schema.PARAMETERS WHERE SPECIFIC_SCHEMA='u3' AND SPECIFIC_NAME='p' ORDER BY ORDINAL_POSITION"),vec![vec![Value::Int(30),Value::String("utf8mb3".into()),Value::String("utf8mb3_general_ci".into())],vec![Value::Int(65535),Value::String("utf8mb3".into()),Value::String("utf8mb3_general_ci".into())]]);
}

#[test]
fn audit1109_fulltext_diagnostics_keep_every_document_token() {
    let (_dir, e, mut s) = fixture();
    for sql in [
        "CREATE TABLE t(id INT PRIMARY KEY,txt TEXT,FULLTEXT KEY ft(txt))",
        "INSERT INTO t VALUES (1,'quick brown fox'),(2,'quick lazy dog')",
        "SET GLOBAL innodb_ft_aux_table='test/t/ft'",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT COUNT(*) FROM information_schema.INNODB_FT_INDEX_TABLE"
        ),
        vec![vec![Value::Int(6)]]
    );
}

#[test]
fn audit1109_explicit_lob_defaults_distinguish_nullable_and_required() {
    let (_dir, e, mut s) = fixture();
    exec(&e, &mut s, "CREATE TABLE n(a TEXT,b BLOB,c JSON)").unwrap();
    exec(&e, &mut s, "INSERT INTO n VALUES(DEFAULT,DEFAULT,DEFAULT)").unwrap();
    assert_eq!(
        rows(&e, &mut s, "SELECT * FROM n"),
        vec![vec![Value::Null, Value::Null, Value::Null]]
    );
    exec(
        &e,
        &mut s,
        "CREATE TABLE t(a TEXT NOT NULL,b BLOB NOT NULL,c JSON NOT NULL)",
    )
    .unwrap();
    assert_eq!(
        exec(&e, &mut s, "INSERT INTO t VALUES(DEFAULT,DEFAULT,DEFAULT)")
            .unwrap_err()
            .code(),
        1364
    );
    exec(&e, &mut s, "SET sql_mode=''").unwrap();
    exec(&e, &mut s, "INSERT INTO t VALUES(DEFAULT,DEFAULT,DEFAULT)").unwrap();
    assert_eq!(s.warnings.len(), 3);
    assert_eq!(
        rows(&e, &mut s, "SELECT a,LENGTH(b),JSON_TYPE(c) FROM t"),
        vec![vec![
            Value::String("".into()),
            Value::Int(0),
            Value::String("NULL".into())
        ]]
    );
}

#[test]
fn audit1109_malformed_xml_row_tag_returns_error() {
    let (dir, e, mut s) = fixture();
    let path = dir.path().join("rows.xml");
    std::fs::write(&path, "<rows><a/></rows>").unwrap();
    exec(&e, &mut s, "CREATE TABLE t(a INT)").unwrap();
    let error = exec(
        &e,
        &mut s,
        &format!(
            "LOAD XML INFILE '{}' INTO TABLE t ROWS IDENTIFIED BY '<a/>'",
            crate::util::file::sql_path(&path)
        ),
    )
    .unwrap_err();
    assert_eq!(error.code(), 1064);
    assert_eq!(
        rows(&e, &mut s, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(0)]]
    );
}

#[test]
fn audit1109_on_update_clock_truncates_to_declared_fractional_precision() {
    let (_dir, e, mut s) = fixture();
    exec(&e, &mut s, "SET time_zone='+00:00'").unwrap();
    for fsp in 0..=6 {
        let sql=format!("CREATE TABLE t{fsp}(id INT PRIMARY KEY,v INT,d DATETIME({fsp}) ON UPDATE CURRENT_TIMESTAMP({fsp}),t TIMESTAMP({fsp}) ON UPDATE CURRENT_TIMESTAMP({fsp}))");
        exec(&e, &mut s, &sql).unwrap();
        exec(
            &e,
            &mut s,
            &format!("INSERT INTO t{fsp} VALUES(1,0,NULL,NULL)"),
        )
        .unwrap();
        exec(&e, &mut s, "SET timestamp=1700000000.123678").unwrap();
        exec(&e, &mut s, &format!("UPDATE t{fsp} SET v=1")).unwrap();
        let quantum = 10_i64.pow(6 - fsp);
        let expected = 123678 / quantum * quantum;
        assert_eq!(
            rows(
                &e,
                &mut s,
                &format!("SELECT MICROSECOND(d),MICROSECOND(t) FROM t{fsp}")
            ),
            vec![vec![Value::Int(expected), Value::Int(expected)]]
        );
    }
}

#[test]
fn audit1109_cross_trigger_cycle_reports_used_table_and_rolls_back() {
    let (_dir, e, mut s) = fixture();
    for sql in [
        "CREATE TABLE a(id INT)",
        "CREATE TABLE b(id INT)",
        "CREATE TRIGGER ta AFTER INSERT ON a FOR EACH ROW INSERT INTO b VALUES(NEW.id)",
        "CREATE TRIGGER tb AFTER INSERT ON b FOR EACH ROW INSERT INTO a VALUES(NEW.id)",
    ] {
        exec(&e, &mut s, sql).unwrap();
    }
    assert_eq!(
        exec(&e, &mut s, "INSERT INTO a VALUES(1)")
            .unwrap_err()
            .code(),
        1442
    );
    assert_eq!(
        rows(
            &e,
            &mut s,
            "SELECT (SELECT COUNT(*) FROM a),(SELECT COUNT(*) FROM b)"
        ),
        vec![vec![Value::Int(0), Value::Int(0)]]
    );
}

#[test]
fn audit1109_view_definition_requires_show_view_and_uses_empty_sentinel() {
    let (_dir, e, mut root) = fixture();
    for sql in [
        "CREATE TABLE t(id INT)",
        "CREATE VIEW v AS SELECT id FROM t",
        "CREATE USER reader",
        "GRANT SELECT ON test.* TO reader",
    ] {
        exec(&e, &mut root, sql).unwrap();
    }
    let mut reader = SessionContext {
        user: "reader".into(),
        login_user: "reader".into(),
        authenticated_as: AccountKey::new("reader", "%"),
        ..Default::default()
    };
    let sql="SELECT VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='v'";
    assert_eq!(
        rows(&e, &mut reader, sql),
        vec![vec![Value::String("".into())]]
    );
    exec(&e, &mut root, "GRANT SHOW VIEW ON test.* TO reader").unwrap();
    let visible = rows(&e, &mut reader, sql);
    assert!(!visible[0][0].to_display_string().is_empty());
}

#[test]
fn audit1109_view_declared_defaults_and_decimal_expression_domains() {
    let (_dir, e, mut s) = fixture();
    exec(&e,&mut s,"CREATE TABLE t(a DECIMAL(10,3) NOT NULL,b VARCHAR(5) NOT NULL,c DECIMAL(10,3) DEFAULT 1.230,d BINARY(1) DEFAULT 0xFF)").unwrap();
    exec(
        &e,
        &mut s,
        "CREATE VIEW v AS SELECT a,b,c,a+1 AS n,CAST(1 AS DECIMAL(8,3)) AS x,d FROM t",
    )
    .unwrap();
    let actual=rows(&e,&mut s,"SELECT COLUMN_DEFAULT,COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='v' ORDER BY ORDINAL_POSITION");
    assert_eq!(
        actual,
        vec![
            vec![Value::Null, Value::String("decimal(10,3)".into())],
            vec![Value::Null, Value::String("varchar(5)".into())],
            vec![
                Value::String("1.230".into()),
                Value::String("decimal(10,3)".into())
            ],
            vec![
                Value::String("0.000".into()),
                Value::String("decimal(11,3)".into())
            ],
            vec![
                Value::String("0.000".into()),
                Value::String("decimal(8,3)".into())
            ],
            vec![
                Value::String("0xFF".into()),
                Value::String("binary(1)".into())
            ],
        ]
    );
}
