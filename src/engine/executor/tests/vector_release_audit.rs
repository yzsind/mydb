//! Regressions for the November vector release audit.
use super::support::query_rows;
use super::vector_type::HnswEnv;
use super::*;

#[test]
fn vector_release_partition_deferred_rows_keep_storage_identity() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut reader = SessionContext::default();
    let mut writer = SessionContext::default();
    engine
        .execute(
            "CREATE TABLE p(id INT PRIMARY KEY,v VECTOR(2)) PARTITION BY HASH(id) PARTITIONS 2",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO p VALUES(0,'[100,100]'),(1,'[0,0]'),(2,'[200,200]'),(3,NULL),(4,'[1,1]')",
        )
        .unwrap();
    let check = |reader: &mut SessionContext| {
        let sql = "SELECT id,L2_DISTANCE(v,'[0,0]') d FROM p ORDER BY d LIMIT 5";
        let QueryResponse::ResultSet(actual) = engine.execute_with_session(reader, sql).unwrap()
        else {
            panic!()
        };
        let QueryResponse::ResultSet(expected) = engine
            .execute_with_session(reader, &sql.replace("ORDER BY d", "ORDER BY d+0"))
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(actual.rows, expected.rows);
    };
    check(&mut reader);
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE p SET v='[9,9]' WHERE id=1")
        .unwrap();
    check(&mut reader);
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
    engine
        .execute_with_session(&mut reader, "SET transaction_isolation='REPEATABLE-READ'")
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "DELETE FROM p WHERE id=1")
        .unwrap();
    check(&mut reader);
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
}

#[test]
fn vector_release_like_initializes_indexes_and_temp_ddl_rejects() {
    let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "paged")]);
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        for algo in ["HNSW", "IVF"] {
            let src = format!("src_{algo}");
            let dst = format!("dst_{algo}");
            engine.execute(&format!("CREATE TABLE {src}(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,INDEX ix(v) USING {algo})")).unwrap();
            engine
                .execute(&format!("CREATE TABLE {dst} LIKE {src}"))
                .unwrap();
            engine
                .execute(&format!("INSERT INTO {dst} VALUES(1,'[1,2]')"))
                .unwrap();
            assert_eq!(query_rows(&engine,&format!("SELECT /*+ HNSW_EF(64) IVF_NPROBE(64) */ id FROM {dst} ORDER BY L2_DISTANCE(v,'[1,2]') LIMIT 1")),vec![vec![Value::Int(1)]]);
            for sql in [format!("CREATE TEMPORARY TABLE tmp LIKE {src}"),format!("CREATE TEMPORARY TABLE tmp(id INT PRIMARY KEY,v VECTOR(2) NOT NULL,INDEX ix(v) USING {algo})")] {
                let err=engine.execute(&sql).unwrap_err();
                assert!(err.is(crate::error::catalog::ER_NOT_SUPPORTED_YET),"{err}");
            }
        }
    }
    let engine = open_strict_test_engine(dir.path());
    for algo in ["HNSW", "IVF"] {
        engine
            .execute(&format!("INSERT INTO dst_{algo} VALUES(2,'[2,3]')"))
            .unwrap();
    }
}

#[test]
fn vector_release_assignment_and_diagnostics() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,b VARBINARY(64),v VECTOR(3))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,UNHEX('5B31302C2032302C2033305D'),NULL)")
        .unwrap();
    assert_eq!(
        query_rows(&engine, "SELECT VECTOR_TO_STRING(CAST(b AS VECTOR)) FROM t")[0][0]
            .to_display_string(),
        "[10,20,30]"
    );
    engine.execute("UPDATE t SET v=b").unwrap();
    assert_eq!(
        query_rows(
            &engine,
            "SELECT COUNT(*) FROM t WHERE v=TO_VECTOR('[10,20,30]')"
        ),
        vec![vec![Value::Int(1)]]
    );
    let err = engine
        .execute("SELECT L2_DISTANCE('[1,2]','[1]')")
        .unwrap_err();
    assert!(err.is(crate::error::catalog::MYDB_ER_VECTOR_DIMENSION_MISMATCH));
    for var in ["mydb_hnsw_rerank_mult", "mydb_ivf_rerank_mult"] {
        for value in ["0", "1025", "-1"] {
            assert!(engine
                .execute(&format!("SET SESSION {var}={value}"))
                .is_err());
        }
    }
    let err = engine
        .execute("CREATE INDEX ix ON t(v) USING HNSW WITH(m=1)")
        .unwrap_err();
    assert!(!err.message().contains("expected []"));
    assert!(!err.message().contains("line 1"));
}

#[test]
fn vector_release_global_defaults_do_not_become_explicit_session_options() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for var in [
        "mydb_hnsw_ef_search",
        "mydb_hnsw_rerank_mult",
        "mydb_ivf_nprobe",
        "mydb_ivf_rerank_mult",
    ] {
        engine
            .execute(&format!("SET GLOBAL {var}=DEFAULT"))
            .unwrap();
    }
    let session = SessionContext::default();
    assert!(!session.sysvars.mydb_hnsw_ef_search_explicit);
    assert!(!session.sysvars.mydb_hnsw_rerank_mult_explicit);
    assert!(!session.sysvars.mydb_ivf_nprobe_explicit);
    assert!(!session.sysvars.mydb_ivf_rerank_mult_explicit);
}

#[test]
fn vector_release_duplicate_cluster_survives_member_deletion() {
    let _env = HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", "paged"),
        ("MYDB_HNSW_COMPACT_DISABLE", "1"),
    ]);
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v VECTOR(8) NOT NULL)")
        .unwrap();
    let mut state = 7_u64;
    let mut next = || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        (state >> 32) as u32 as f64 / u32::MAX as f64 * 2.0 - 1.0
    };
    let duplicate = format!(
        "[{}]",
        (0..8)
            .map(|_| next().to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    for start in (0..5000).step_by(500) {
        let rows = (start..start + 500)
            .map(|id| {
                let vector = if id >= 4800 {
                    duplicate.clone()
                } else {
                    format!(
                        "[{}]",
                        (0..8)
                            .map(|_| next().to_string())
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                };
                format!("({id},'{vector}')")
            })
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO t VALUES {rows}"))
            .unwrap();
    }
    engine
        .execute("CREATE INDEX ix ON t(v) USING HNSW")
        .unwrap();
    engine
        .execute("DELETE FROM t WHERE id>=4800 AND id<4950")
        .unwrap();
    for ef in [60, 240, 1000] {
        let rows=query_rows(&engine,&format!("SELECT /*+ HNSW_EF({ef}) */ id,L2_DISTANCE(v,'{duplicate}') d FROM t ORDER BY d LIMIT 20"));
        assert_eq!(rows.len(), 20);
        assert!(
            rows.iter().all(|row| row[1].to_display_string() == "0"),
            "ef={ef}: {rows:?}"
        );
    }
}
