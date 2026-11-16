//! MySQL 8.0.46 LOAD_FILE() compatibility regressions.
use super::*;
use crate::engine::auth::AccountKey;

fn sql_path(path: &std::path::Path) -> String {
    path.to_string_lossy()
        .replace('\\', "\\\\")
        .replace('\'', "''")
}

#[cfg(target_os = "linux")]
fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

fn row(engine: &StorageQueryEngine, sql: &str) -> Vec<Value> {
    let QueryResponse::ResultSet(result) = engine.execute(sql).unwrap() else {
        panic!("expected result set for {sql}");
    };
    assert_eq!(result.rows.len(), 1, "expected one row for {sql}");
    result.rows.into_iter().next().unwrap()
}

fn row_with_session(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> Vec<Value> {
    let QueryResponse::ResultSet(result) = engine.execute_with_session(session, sql).unwrap()
    else {
        panic!("expected result set for {sql}");
    };
    assert_eq!(result.rows.len(), 1, "expected one row for {sql}");
    result.rows.into_iter().next().unwrap()
}

struct QueryGlobalsReset;

impl Drop for QueryGlobalsReset {
    fn drop(&mut self) {
        crate::engine::sysvar::builtin::query::reset_query_globals_for_mtr();
        crate::engine::sysvar::builtin::connection::reset_max_allowed_packet_for_mtr();
    }
}

fn reset_query_globals() -> QueryGlobalsReset {
    crate::engine::sysvar::builtin::query::reset_query_globals_for_mtr();
    crate::engine::sysvar::builtin::connection::reset_max_allowed_packet_for_mtr();
    QueryGlobalsReset
}

#[test]
fn load_file_reads_binary_empty_and_reports_binary_metadata() {
    let _guard = test_env_lock();
    let _reset = reset_query_globals();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let binary = dir.path().join("load-file-binary.bin");
    let empty = dir.path().join("load-file-empty.bin");
    let utf8 = dir.path().join("加载文件.bin");
    let payload = [b'a', 0, 0xff, b'\n', 0x80];
    std::fs::write(&binary, payload).unwrap();
    std::fs::write(&empty, []).unwrap();
    std::fs::write(&utf8, b"utf8-name").unwrap();

    let sql = format!(
        "SELECT LOAD_FILE('{}'), HEX(LOAD_FILE('{}')), LENGTH(LOAD_FILE('{}')), \
         CHARSET(LOAD_FILE('{}')), COLLATION(LOAD_FILE('{}')), LOAD_FILE('{}')",
        sql_path(&binary),
        sql_path(&binary),
        sql_path(&binary),
        sql_path(&binary),
        sql_path(&binary),
        sql_path(&empty),
    );
    let values = row(&engine, &sql);
    assert_eq!(values[0], Value::Bytes(payload.to_vec()));
    assert_eq!(values[1].to_display_string(), "6100FF0A80");
    assert_eq!(values[2].to_display_string(), "5");
    assert_eq!(values[3].to_display_string(), "binary");
    assert_eq!(values[4].to_display_string(), "binary");
    assert_eq!(values[5], Value::Bytes(Vec::new()));
    assert_eq!(
        row(&engine, &format!("SELECT LOAD_FILE('{}')", sql_path(&utf8))),
        vec![Value::Bytes(b"utf8-name".to_vec())]
    );
    assert_eq!(
        row(&engine, "SELECT LOAD_FILE('load-file-binary.bin')"),
        vec![Value::Bytes(payload.to_vec())],
        "relative paths resolve against the server data directory"
    );

    let QueryResponse::ResultSet(result) = engine
        .execute(&format!("SELECT LOAD_FILE('{}')", sql_path(&binary)))
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.columns[0].column_type, ColumnType::Blob);
    assert_eq!(result.columns[0].source.collation_id.unwrap_or(63), 63);

    // Darwin filesystems reject ill-formed UTF-8 path components with
    // EILSEQ; exercise raw Unix filename bytes on platforms that admit them.
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};

        let mut raw_path = dir.path().as_os_str().as_bytes().to_vec();
        raw_path.extend_from_slice(b"/non-utf8-\xff.bin");
        let raw_path = std::path::PathBuf::from(std::ffi::OsString::from_vec(raw_path));
        std::fs::write(&raw_path, b"raw").unwrap();
        assert_eq!(
            row(
                &engine,
                &format!(
                    "SELECT LOAD_FILE(X'{}')",
                    hex_bytes(raw_path.as_os_str().as_bytes())
                )
            ),
            vec![Value::Bytes(b"raw".to_vec())]
        );

        let mut latin1_path = dir.path().as_os_str().as_bytes().to_vec();
        latin1_path.extend_from_slice(b"/latin-\xe9.bin");
        let latin1_path = std::path::PathBuf::from(std::ffi::OsString::from_vec(latin1_path));
        std::fs::write(&latin1_path, b"latin1").unwrap();
        let mut fs_session = SessionContext::default();
        engine
            .execute_with_session(&mut fs_session, "SET character_set_filesystem=latin1")
            .unwrap();
        assert_eq!(
            row_with_session(
                &engine,
                &mut fs_session,
                &format!("SELECT LOAD_FILE('{}/latin-é.bin')", sql_path(dir.path()))
            ),
            vec![Value::Bytes(b"latin1".to_vec())]
        );
    }
}

#[test]
fn load_file_arity_null_missing_and_non_regular_semantics() {
    let _guard = test_env_lock();
    let _reset = reset_query_globals();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    assert_eq!(row(&engine, "SELECT LOAD_FILE(NULL)"), vec![Value::Null]);
    assert_eq!(
        row(
            &engine,
            &format!(
                "SELECT LOAD_FILE('{}')",
                sql_path(&dir.path().join("missing"))
            )
        ),
        vec![Value::Null]
    );
    for sql in ["SELECT LOAD_FILE()", "SELECT LOAD_FILE('a','b')"] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(err.code(), 1582, "{sql}: {err}");
        assert_eq!(err.sql_state(), "42000");
    }
    let QueryResponse::ResultSet(result) = engine
        .execute(&format!("SELECT LOAD_FILE('{}')", sql_path(dir.path())))
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.rows[0][0], Value::Null);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let unreadable = dir.path().join("unreadable.bin");
        std::fs::write(&unreadable, b"hidden").unwrap();
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o0)).unwrap();
        // LOAD_FILE follows the OS read result, as MySQL 8.0 does. Root or
        // CAP_DAC_OVERRIDE can read mode-000 files; mode bits alone do not
        // establish that this process lacks access.
        let expected = match std::fs::read(&unreadable) {
            Ok(bytes) => Value::Bytes(bytes),
            Err(err) => {
                assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
                Value::Null
            }
        };
        assert_eq!(
            row(
                &engine,
                &format!("SELECT LOAD_FILE('{}')", sql_path(&unreadable))
            ),
            vec![expected]
        );
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o600)).unwrap();

        let fifo = dir.path().join("load-file.fifo");
        let fifo_c = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
        assert_eq!(
            row(&engine, &format!("SELECT LOAD_FILE('{}')", sql_path(&fifo))),
            vec![Value::Null]
        );
    }
}

#[test]
fn load_file_checks_file_privilege_grant_revoke_and_active_role() {
    let _guard = test_env_lock();
    let _reset = reset_query_globals();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let file = dir.path().join("load-file-auth.txt");
    std::fs::write(&file, b"secret").unwrap();
    let sql = format!("SELECT LOAD_FILE('{}')", sql_path(&file));

    engine
        .execute("CREATE USER 'load_alice'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    let mut alice = SessionContext::default();
    alice.user = "load_alice".into();
    alice.login_user = "load_alice".into();
    alice.authenticated_as = AccountKey::new("load_alice", "%");
    alice.auth_epoch = engine.db.auth_catalog().epoch;
    assert_eq!(
        row_with_session(&engine, &mut alice, &sql),
        vec![Value::Null]
    );

    engine
        .execute("GRANT FILE ON *.* TO 'load_alice'@'%'")
        .unwrap();
    assert_eq!(
        row_with_session(&engine, &mut alice, &sql),
        vec![Value::Bytes(b"secret".to_vec())]
    );
    engine
        .execute("REVOKE FILE ON *.* FROM 'load_alice'@'%'")
        .unwrap();
    assert_eq!(
        row_with_session(&engine, &mut alice, &sql),
        vec![Value::Null]
    );

    engine.execute("CREATE ROLE 'load_reader'@'%'").unwrap();
    engine
        .execute("GRANT FILE ON *.* TO 'load_reader'@'%'")
        .unwrap();
    engine
        .execute("GRANT 'load_reader'@'%' TO 'load_alice'@'%'")
        .unwrap();
    engine
        .execute_with_session(&mut alice, "SET ROLE 'load_reader'@'%'")
        .unwrap();
    assert_eq!(
        row_with_session(&engine, &mut alice, &sql),
        vec![Value::Bytes(b"secret".to_vec())]
    );
}

#[test]
fn load_file_secure_file_priv_and_packet_boundary() {
    let _guard = test_env_lock();
    let _reset = reset_query_globals();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let allowed = dir.path().join("allowed");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&allowed).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let inside_file = allowed.join("inside.bin");
    let outside_file = outside.join("outside.bin");
    std::fs::write(&inside_file, b"inside").unwrap();
    std::fs::write(&outside_file, b"outside").unwrap();

    engine
        .execute(&format!(
            "SET GLOBAL secure_file_priv='{}'",
            sql_path(&allowed)
        ))
        .unwrap();
    assert_eq!(
        row(
            &engine,
            &format!("SELECT LOAD_FILE('{}')", sql_path(&inside_file))
        ),
        vec![Value::Bytes(b"inside".to_vec())]
    );
    assert_eq!(
        row(
            &engine,
            &format!("SELECT LOAD_FILE('{}')", sql_path(&outside_file))
        ),
        vec![Value::Null]
    );
    assert_eq!(
        row(
            &engine,
            &format!(
                "SELECT LOAD_FILE('{}')",
                sql_path(&allowed.join("..").join("outside").join("outside.bin"))
            )
        ),
        vec![Value::Null],
        ".. must not escape secure_file_priv"
    );
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&outside_file, allowed.join("link-out")).unwrap();
        std::os::unix::fs::symlink(&inside_file, outside.join("link-in")).unwrap();
        assert_eq!(
            row(
                &engine,
                &format!(
                    "SELECT LOAD_FILE('{}')",
                    sql_path(&allowed.join("link-out"))
                )
            ),
            vec![Value::Null],
            "symlink target outside the allowed directory must be rejected"
        );
        assert_eq!(
            row(
                &engine,
                &format!("SELECT LOAD_FILE('{}')", sql_path(&outside.join("link-in")))
            ),
            vec![Value::Bytes(b"inside".to_vec())],
            "authorization follows the canonical target"
        );
    }
    engine
        .execute("SET GLOBAL secure_file_priv='NULL'")
        .unwrap();
    assert_eq!(
        row(
            &engine,
            &format!("SELECT LOAD_FILE('{}')", sql_path(&inside_file))
        ),
        vec![Value::Null]
    );
    assert_eq!(
        row(
            &engine,
            &format!(
                "SELECT CHARSET(LOAD_FILE('{}')), COLLATION(LOAD_FILE('{}'))",
                sql_path(&inside_file),
                sql_path(&inside_file)
            )
        ),
        vec![
            Value::String("binary".into()),
            Value::String("binary".into())
        ]
    );

    engine
        .execute("SET GLOBAL secure_file_priv='', max_allowed_packet=1024")
        .unwrap();
    let mut packet_session = SessionContext::default();
    let exact = dir.path().join("exact.bin");
    let below = dir.path().join("below.bin");
    let over = dir.path().join("over.bin");
    let sparse = dir.path().join("sparse-over.bin");
    std::fs::write(&below, vec![6_u8; 1023]).unwrap();
    std::fs::write(&exact, vec![7_u8; 1024]).unwrap();
    std::fs::write(&over, vec![8_u8; 1025]).unwrap();
    std::fs::File::create(&sparse)
        .unwrap()
        .set_len(64 * 1024 * 1024)
        .unwrap();
    assert_eq!(
        row_with_session(
            &engine,
            &mut packet_session,
            &format!("SELECT LENGTH(LOAD_FILE('{}'))", sql_path(&below))
        )[0]
        .to_display_string(),
        "1023"
    );
    assert_eq!(
        row_with_session(
            &engine,
            &mut packet_session,
            &format!("SELECT LENGTH(LOAD_FILE('{}'))", sql_path(&exact))
        )[0]
        .to_display_string(),
        "1024"
    );
    let scratch_before = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[1];
    assert_eq!(
        row_with_session(
            &engine,
            &mut packet_session,
            &format!("SELECT LOAD_FILE('{}')", sql_path(&sparse))
        ),
        vec![Value::Null]
    );
    assert_eq!(
        crate::engine::executor::mem_budget::controlled_memory_snapshot().2[1],
        scratch_before,
        "metadata rejection of a sparse oversized file must not allocate"
    );
    assert_eq!(
        row_with_session(
            &engine,
            &mut packet_session,
            &format!("SELECT LOAD_FILE('{}')", sql_path(&over))
        ),
        vec![Value::Null]
    );
    assert_eq!(
        packet_session.warning_count(),
        1,
        "LOAD_FILE overflow must persist warning 1301"
    );
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut packet_session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!("expected SHOW WARNINGS result set");
    };
    assert_eq!(warnings.rows.len(), 1);
    assert_eq!(warnings.rows[0][1].to_display_string(), "1301");
    assert!(warnings.rows[0][2]
        .to_display_string()
        .contains("larger than max_allowed_packet (1024)"));
}

#[test]
fn load_file_is_volatile_dynamic_and_releases_storage_scratch() {
    let _guard = test_env_lock();
    let _reset = reset_query_globals();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let file = dir.path().join("volatile.bin");
    std::fs::write(&file, b"first").unwrap();
    let sql = format!("SELECT LOAD_FILE('{}')", sql_path(&file));
    let before = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[1];
    assert_eq!(row(&engine, &sql), vec![Value::Bytes(b"first".to_vec())]);
    std::fs::write(&file, b"second").unwrap();
    assert_eq!(row(&engine, &sql), vec![Value::Bytes(b"second".to_vec())]);
    let after = crate::engine::executor::mem_budget::controlled_memory_snapshot().2[1];
    assert_eq!(after, before, "StorageScratch reservation leaked");

    engine.execute("CREATE TABLE load_paths(p TEXT)").unwrap();
    engine
        .execute(&format!(
            "INSERT INTO load_paths VALUES('{}')",
            sql_path(&file)
        ))
        .unwrap();
    assert_eq!(
        row(&engine, "SELECT LOAD_FILE(p) FROM load_paths"),
        vec![Value::Bytes(b"second".to_vec())]
    );
    assert_eq!(
        row(
            &engine,
            &format!(
                "WITH loaded AS (SELECT LOAD_FILE('{}') AS b) \
                 SELECT b FROM loaded",
                sql_path(&file)
            )
        ),
        vec![Value::Bytes(b"second".to_vec())]
    );
    assert_eq!(
        row(
            &engine,
            &format!("SELECT (SELECT LOAD_FILE('{}'))", sql_path(&file))
        ),
        vec![Value::Bytes(b"second".to_vec())]
    );
    engine
        .execute(&format!(
            "CREATE TABLE loaded_copy AS SELECT LOAD_FILE('{}') AS contents",
            sql_path(&file)
        ))
        .unwrap();
    let loaded_copy = engine.database().table("test.loaded_copy").unwrap();
    assert!(matches!(
        loaded_copy.columns[0].data_type,
        crate::storage::DataTypeKind::Blob { max_len }
            if max_len == crate::storage::schema::LONG_LOB_MAX
    ));
    engine
        .execute("ALTER TABLE load_paths ADD COLUMN contents BLOB")
        .unwrap();
    engine
        .execute("UPDATE load_paths SET contents=LOAD_FILE(p)")
        .unwrap();
    assert_eq!(
        row(&engine, "SELECT contents FROM load_paths"),
        vec![Value::Bytes(b"second".to_vec())]
    );

    for ddl in [
        "CREATE TABLE lf_generated(a TEXT, b BLOB AS (LOAD_FILE(a)))",
        "CREATE TABLE lf_index(a TEXT, INDEX ((LOAD_FILE(a))))",
        "CREATE TABLE lf_default(a BLOB DEFAULT (LOAD_FILE('/tmp/x')))",
        "CREATE TABLE lf_check(a TEXT CHECK (LOAD_FILE(a) IS NULL))",
    ] {
        assert!(engine.execute(ddl).is_err(), "{ddl} must reject LOAD_FILE");
    }
}

#[test]
fn load_file_prepared_binding_reloads_and_prepare_does_not_touch_file() {
    let _guard = test_env_lock();
    let _reset = reset_query_globals();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let file = dir.path().join("prepared.bin");
    let sql = "SELECT LOAD_FILE(?)";
    let mut session = SessionContext::default();

    let columns = engine
        .describe_prepared_statement(&mut session, sql)
        .unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].column_type, ColumnType::Blob);
    assert_eq!(
        columns[0].source.length,
        Some(crate::storage::schema::LONG_LOB_MAX)
    );
    let prepared = engine.prepare_cached(&mut session, sql, 1).unwrap();
    engine
        .execute(&format!("EXPLAIN SELECT LOAD_FILE('{}')", sql_path(&file)))
        .unwrap();
    assert!(!file.exists(), "PREPARE/DESCRIBE must not access the file");

    let execute = |session: &mut SessionContext, expected: &[u8]| {
        let response = crate::engine::params::with_params(
            vec![Value::String(file.to_string_lossy().into_owned())],
            || {
                engine
                    .execute_prepared_statement(session, sql, (*prepared).clone())
                    .expect("prepared SELECT must be handled")
                    .unwrap()
            },
        )
        .materialize()
        .unwrap();
        let QueryResponse::ResultSet(result) = response else {
            panic!("expected prepared result set");
        };
        assert_eq!(result.columns[0].column_type, ColumnType::Blob);
        assert_eq!(result.rows, vec![vec![Value::Bytes(expected.to_vec())]]);
    };

    std::fs::write(&file, b"one").unwrap();
    execute(&mut session, b"one");
    std::fs::write(&file, b"two").unwrap();
    execute(&mut session, b"two");
}

#[test]
fn load_file_stored_function_uses_definer_or_invoker_security_context() {
    let _guard = test_env_lock();
    let _reset = reset_query_globals();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let file = dir.path().join("routine-secret.bin");
    std::fs::write(&file, b"routine-secret").unwrap();
    engine
        .execute(
            "CREATE FUNCTION load_definer(p TEXT) RETURNS BLOB \
             SQL SECURITY DEFINER RETURN LOAD_FILE(p)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE FUNCTION load_invoker(p TEXT) RETURNS BLOB \
             SQL SECURITY INVOKER RETURN LOAD_FILE(p)",
        )
        .unwrap();
    engine
        .execute("CREATE USER 'routine_reader'@'%' IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute("GRANT EXECUTE ON FUNCTION test.load_definer TO 'routine_reader'@'%'")
        .unwrap();
    engine
        .execute("GRANT EXECUTE ON FUNCTION test.load_invoker TO 'routine_reader'@'%'")
        .unwrap();

    let mut reader = SessionContext::default();
    reader.user = "routine_reader".into();
    reader.login_user = "routine_reader".into();
    reader.authenticated_as = AccountKey::new("routine_reader", "%");
    reader.auth_epoch = engine.db.auth_catalog().epoch;
    assert_eq!(
        row_with_session(
            &engine,
            &mut reader,
            &format!("SELECT load_definer('{}')", sql_path(&file))
        ),
        vec![Value::Bytes(b"routine-secret".to_vec())]
    );
    assert_eq!(
        row_with_session(
            &engine,
            &mut reader,
            &format!("SELECT load_invoker('{}')", sql_path(&file))
        ),
        vec![Value::Null]
    );
}

#[test]
fn load_file_can_borrow_past_low_soft_target_without_leaking_scratch() {
    let _guard = test_env_lock();
    let _reset = reset_query_globals();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let file = dir.path().join("governor.bin");
    std::fs::write(&file, vec![9_u8; 4 * 1024 * 1024]).unwrap();

    let prior_target = crate::engine::executor::mem_budget::memory_target_bytes();
    let before = crate::engine::executor::mem_budget::controlled_memory_snapshot();
    let tight_mb = before.0.div_ceil(1024 * 1024).max(1);
    crate::engine::executor::mem_budget::set_memory_target_mb(tight_mb).unwrap();
    let result = row(&engine, &format!("SELECT LOAD_FILE('{}')", sql_path(&file)));
    assert!(matches!(&result[0], Value::Bytes(bytes) if bytes.len() == 4 * 1024 * 1024));
    let after = crate::engine::executor::mem_budget::controlled_memory_snapshot();
    assert_eq!(after.2[1], before.2[1], "StorageScratch leaked");

    let prior_mb = prior_target.div_ceil(1024 * 1024).max(1);
    crate::engine::executor::mem_budget::set_memory_target_mb(prior_mb).unwrap();
}
