use super::*;

fn exported(engine: &StorageQueryEngine, sql: &str) -> u64 {
    match engine.execute(sql).unwrap() {
        QueryResponse::OkAffected { affected_rows } => affected_rows,
        other => panic!("expected OK affected response, got {other:?}"),
    }
}

#[test]
fn outfile_matches_mysql80_default_and_csv_bytes() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id BIGINT, n DECIMAL(10,2), s VARCHAR(100), b VARBINARY(20))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES \
             (1,12.30,CONCAT('a',CHAR(9),'b',CHAR(10),'c',CHAR(92),'d',CHAR(34),'e'),X'00ff095c'), \
             (2,NULL,'',NULL)",
        )
        .unwrap();

    let tsv = dir.path().join("default.tsv");
    assert_eq!(
        exported(
            &engine,
            &format!(
                "SELECT id,n,s,b FROM t ORDER BY id INTO OUTFILE '{}'",
                crate::util::file::sql_path(&tsv)
            ),
        ),
        2
    );
    assert_eq!(
        std::fs::read(&tsv).unwrap(),
        [
            b"1\t12.30\ta\\\tb\\\nc\\\\d\"e\t\\0".as_slice(),
            &[0xff],
            b"\\\t\\\\\n2\t\\N\t\t\\N\n",
        ]
        .concat()
    );

    let csv = dir.path().join("csv.out");
    assert_eq!(
        exported(
            &engine,
            &format!(
                "SELECT id,n,s,b FROM t ORDER BY id INTO OUTFILE '{}' \
                 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' \
                 LINES TERMINATED BY '\\r\\n'",
                crate::util::file::sql_path(&csv)
            ),
        ),
        2
    );
    assert_eq!(
        std::fs::read(&csv).unwrap(),
        [
            b"1,12.30,\"a\tb\nc\\\\d\\\"e\",\"\\0".as_slice(),
            &[0xff],
            b"\t\\\\\"\r\n2,\\N,\"\",\\N\r\n",
        ]
        .concat()
    );
}

#[test]
fn dumpfile_is_raw_single_row_and_preserves_partial_file_on_error() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT, b VARBINARY(8))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,X'00ff095c'),(2,NULL)")
        .unwrap();

    let one = dir.path().join("one.bin");
    assert_eq!(
        exported(
            &engine,
            &format!(
                "SELECT b,NULL FROM t WHERE id=1 INTO DUMPFILE '{}'",
                crate::util::file::sql_path(&one)
            ),
        ),
        1
    );
    assert_eq!(std::fs::read(&one).unwrap(), [0, 0xff, 9, 92, 0]);

    let many = dir.path().join("many.bin");
    let err = engine
        .execute(&format!(
            "SELECT b FROM t ORDER BY id INTO DUMPFILE '{}'",
            crate::util::file::sql_path(&many)
        ))
        .unwrap_err();
    assert_eq!(err.code(), 1172);
    assert_eq!(std::fs::read(&many).unwrap(), [0, 0xff, 9, 92]);
}

#[test]
fn outfile_does_not_overwrite_and_uses_mode_0640() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let path = dir.path().join("exclusive.tsv");
    assert_eq!(
        exported(
            &engine,
            &format!(
                "SELECT 1 INTO OUTFILE '{}'",
                crate::util::file::sql_path(&path)
            ),
        ),
        1
    );
    let original = std::fs::read(&path).unwrap();
    let err = engine
        .execute(&format!(
            "SELECT 2 INTO OUTFILE '{}'",
            crate::util::file::sql_path(&path)
        ))
        .unwrap_err();
    assert_eq!(err.code(), 1086);
    assert_eq!(std::fs::read(&path).unwrap(), original);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }
}

#[test]
fn select_into_sysvars_and_set_var_hint_are_effective_without_leaking() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "SET SESSION select_into_buffer_size=8192, \
             select_into_disk_sync=OFF, select_into_disk_sync_delay=0",
        )
        .unwrap();
    assert_eq!(session.sysvars.select_into_buffer_size, 8192);

    let baseline = crate::engine::executor::select_into_file::active_writer_reserved_bytes();
    let path = dir.path().join("hint.tsv");
    let response = engine
        .execute_with_session(
            &mut session,
            &format!(
                "SELECT /*+ SET_VAR(select_into_buffer_size=12288) */ 1 \
                 INTO OUTFILE '{}'",
                crate::util::file::sql_path(&path)
            ),
        )
        .unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 1 }
    ));
    assert_eq!(session.sysvars.select_into_buffer_size, 8192);
    assert_eq!(
        crate::engine::executor::select_into_file::active_writer_reserved_bytes(),
        baseline,
        "writer reservation must return to baseline"
    );
}

#[test]
fn outfile_fixed_width_rows_use_column_display_width() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE fixed(a INT, b CHAR(4), c VARCHAR(4))")
        .unwrap();
    engine
        .execute("INSERT INTO fixed VALUES(1,'xy','z'),(22,NULL,'abcd')")
        .unwrap();
    let path = dir.path().join("fixed.out");
    assert_eq!(
        exported(
            &engine,
            &format!(
                "SELECT a,b,c FROM fixed ORDER BY a INTO OUTFILE '{}' \
                 FIELDS TERMINATED BY '' LINES TERMINATED BY ''",
                crate::util::file::sql_path(&path)
            ),
        ),
        2
    );
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(bytes.len(), 86);
    assert_eq!(&bytes[..11], b"1          ");
    assert_eq!(&bytes[43..54], b"22         ");
}
