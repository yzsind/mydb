//! Regressions confirmed while adjudicating the October account/LOB/HNSW audits.
use super::support::execute_materialized as exec;
use super::*;
use crate::engine::auth::{self, AccountKey, Privilege, PrivilegeScope, RequiredPrivilege};

fn user_session(user: &str) -> SessionContext {
    SessionContext {
        user: user.into(),
        login_user: user.into(),
        authenticated_as: AccountKey::new(user, "%"),
        ..SessionContext::default()
    }
}

#[test]
fn audit_1029_function_return_subquery_cannot_borrow_view_definer_privileges() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "CREATE USER fn_weak, view_strong",
        "CREATE TABLE secret(value INT)",
        "INSERT INTO secret VALUES(42)",
        "GRANT SELECT ON test.secret TO view_strong",
        "CREATE DEFINER='fn_weak'@'%' FUNCTION f() RETURNS INT DETERMINISTIC RETURN (SELECT value FROM test.secret LIMIT 1)",
        "GRANT EXECUTE ON FUNCTION test.f TO view_strong, fn_weak",
        "CREATE DEFINER='view_strong'@'%' VIEW v AS SELECT f() AS answer,value FROM test.secret",
        "GRANT SELECT ON test.v TO fn_weak",
    ] { exec(&engine, &mut root, sql).unwrap(); }
    let mut weak = user_session("fn_weak");
    assert_eq!(
        exec(&engine, &mut weak, "SELECT * FROM v")
            .unwrap_err()
            .code(),
        1142
    );
    exec(&engine, &mut root, "GRANT SELECT ON test.secret TO fn_weak").unwrap();
    let QueryResponse::ResultSet(rs) = exec(&engine, &mut weak, "SELECT * FROM v").unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(42), Value::Int(42)]]);
    exec(
        &engine,
        &mut root,
        "REVOKE SELECT ON test.secret FROM fn_weak",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut weak, "SELECT * FROM v")
            .unwrap_err()
            .code(),
        1142
    );
}

#[test]
fn audit_1029_batch_create_binlog_keeps_options_when_last_user_is_skipped() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let source = open_test_engine(first.path());
    let replica = open_test_engine(second.path());
    for engine in [&source, &replica] {
        engine.execute("CREATE ROLE r").unwrap();
        engine
            .execute("CREATE USER kept IDENTIFIED BY 'old' WITH MAX_QUERIES_PER_HOUR 7")
            .unwrap();
    }
    let sql = "CREATE USER IF NOT EXISTS fresh IDENTIFIED BY 'new', kept IDENTIFIED BY 'ignored' WITH MAX_QUERIES_PER_HOUR 99 DEFAULT ROLE r";
    source.execute(sql).unwrap();
    let replay = credential_safe_binlog_sql(&source.db, &SessionContext::default(), sql).unwrap();
    replica.execute(&replay).unwrap();
    let key = AccountKey::new("fresh", "%");
    let copied = replica.db.auth_catalog();
    assert_eq!(copied.accounts[&auth::account_id(&key)].max_questions, 99);
    assert_eq!(
        copied.accounts[&auth::account_id(&AccountKey::new("kept", "%"))].max_questions,
        7
    );
    assert!(copied
        .active_default_roles(&key)
        .contains(&AccountKey::new("r", "%")));
}

#[test]
fn audit_1029_replace_allows_shrinking_a_lob_below_packet_limit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "CREATE TABLE large_source(s TEXT,b BLOB)",
        "INSERT INTO large_source VALUES(REPEAT('a',2000),REPEAT('a',2000))",
        "SET GLOBAL max_allowed_packet=1024",
    ] {
        exec(&engine, &mut root, sql).unwrap();
    }
    let QueryResponse::ResultSet(rs) = exec(
        &engine,
        &mut root,
        "SELECT LENGTH(REPLACE(s,'a','')),LENGTH(REPLACE(b,'a','')) FROM large_source",
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(0), Value::Int(0)]]);
}

#[test]
fn audit_1029_current_user_binlog_targets_bind_without_rewriting_roles_or_objects() {
    let account = AccountKey::new("MixedIssuer", "localhost");
    for sql in [
        "GRANT SELECT ON current_user.to TO CURRENT_USER",
        "GRANT to, current_user TO CURRENT_USER()",
        "REVOKE current_user FROM CURRENT_USER",
        "GRANT SELECT ON test.t TO 'CURRENT_USER'@'%', CURRENT_USER()",
    ] {
        let replay =
            credential_safe_binlog_sql_from_catalog(&auth::AuthCatalog::empty(), &account, sql)
                .unwrap();
        assert!(replay.contains("'MixedIssuer'@'localhost'"), "{replay}");
        parse_statement(&replay).unwrap();
        if sql.contains("current_user.to") {
            assert!(replay.contains("current_user.to"));
        }
        if sql.contains("to, current_user") {
            assert!(replay.contains("to, current_user"));
        }
        if sql.contains("'CURRENT_USER'") {
            assert!(replay.contains("'CURRENT_USER'@'%'"));
        }
    }
}

#[test]
fn audit_1029_component_state_is_database_scoped() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let enforced = open_test_engine(first.path());
    enforced
        .execute("INSTALL COMPONENT 'file://component_validate_password'")
        .unwrap();
    let ordinary = open_test_engine(second.path());
    ordinary
        .execute("CREATE USER simple IDENTIFIED BY 'x'")
        .unwrap();
    assert_eq!(
        enforced
            .execute("CREATE USER simple IDENTIFIED BY 'x'")
            .unwrap_err()
            .code(),
        1819
    );
}

#[test]
fn audit_1029_cipher_requirement_does_not_imply_client_certificate() {
    let mut request = auth::AuthRequest::legacy("u", "127.0.0.1", &[], None);
    request.tls_established = true;
    request.tls_cipher = Some("TLS_AES_128_GCM_SHA256");
    let mut requirement = auth::SslRequirement::Specified {
        cipher: Some("TLS_AES_128_GCM_SHA256".into()),
        issuer: None,
        subject: None,
    };
    enforce_tls_requirement(&requirement, &request).unwrap();
    if let auth::SslRequirement::Specified { issuer, .. } = &mut requirement {
        *issuer = Some("/CN=CA".into());
    }
    assert!(enforce_tls_requirement(&requirement, &request).is_err());
}

#[test]
fn audit_1029_password_rotation_preserves_temporary_login_lock() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(
        &engine,
        &mut root,
        "CREATE USER locked IDENTIFIED BY 'old' FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 1",
    )
    .unwrap();
    let key = AccountKey::new("locked", "%");
    for _ in 0..2 {
        auth::failed_login_record(&key, 2, Some(1), auth::current_time_ms());
    }
    for sql in [
        "ALTER USER locked IDENTIFIED BY 'new'",
        "SET PASSWORD FOR locked = 'newer'",
    ] {
        exec(&engine, &mut root, sql).unwrap();
        let account = engine.db.auth_catalog().accounts[&auth::account_id(&key)].clone();
        assert!(auth::active_login_lock(&account, auth::current_time_ms()).is_some());
    }
    exec(&engine, &mut root, "ALTER USER locked ACCOUNT UNLOCK").unwrap();
    let account = engine.db.auth_catalog().accounts[&auth::account_id(&key)].clone();
    assert!(auth::active_login_lock(&account, auth::current_time_ms()).is_none());
}

#[test]
fn audit_1029_batch_credential_rewrite_is_parseable_and_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    let sql = "CREATE USER BatchOne IDENTIFIED BY 'first', BatchTwo IDENTIFIED BY 'second' WITH MAX_QUERIES_PER_HOUR 50";
    exec(&engine, &mut root, sql).unwrap();
    let replay = credential_safe_binlog_sql(&engine.db, &root, sql).unwrap();
    let Statement::CreateUser(create) = parse_statement(&replay).unwrap() else {
        panic!("{replay}")
    };
    assert_eq!(create.accounts.len(), 2);
    for (spec, name) in create.accounts.iter().zip(["BatchOne", "BatchTwo"]) {
        assert_eq!(spec.account.user, name);
        assert!(spec
            .password
            .as_deref()
            .is_some_and(|hash| hash.starts_with("$A$")));
    }
    assert!(!replay.contains("first"));
    assert!(!replay.contains("second"));
    assert!(credential_safe_binlog_sql(
        &engine.db,
        &root,
        "CREATE USER x IDENTIFIED BY 'secret' INVALID"
    )
    .is_err());
    let before = engine.db.auth_catalog();
    let _context = crate::binlog::enter_ddl_txn_context(
        "test",
        "CREATE USER x IDENTIFIED BY 'secret' INVALID",
        &crate::binlog::BinlogQueryContext::default(),
        &root.authenticated_as,
    );
    assert!(engine
        .db
        .mutate_auth_catalog(|auth| {
            auth.accounts.insert(
                auth::account_id(&AccountKey::new("unpublished", "%")),
                auth::UserAccount::new(
                    AccountKey::new("unpublished", "%"),
                    "mysql_native_password".into(),
                    String::new(),
                ),
            );
            Ok(())
        })
        .is_err());
    assert_eq!(engine.db.auth_catalog().accounts, before.accounts);
    assert_eq!(engine.db.auth_catalog().epoch, before.epoch);
}

#[test]
fn audit_1029_warning_values_and_retention_are_bounded_with_full_count() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in ["SET max_error_count=3", "CREATE TABLE diagnostics(s TEXT)", "INSERT INTO diagnostics VALUES(CONCAT('a',REPEAT('甲',1000))),(CONCAT('b',REPEAT('乙',1000))),(CONCAT('c',REPEAT('丙',1000))),('d'),('e')"] {
        exec(&engine, &mut root, sql).unwrap();
    }
    exec(&engine, &mut root, "SELECT SUM(s) FROM diagnostics").unwrap();
    assert_eq!(root.warning_count(), 5);
    assert_eq!(root.warnings.len(), 3);
    assert!(root
        .warnings
        .iter()
        .all(|warning| warning.message().len() < 256));
    exec(
        &engine,
        &mut root,
        "SELECT COUNT(*) FROM diagnostics WHERE s",
    )
    .unwrap();
    assert_eq!(root.warning_count(), 5);
    assert!(root
        .warnings
        .iter()
        .all(|warning| warning.message().len() < 256));
    exec(&engine, &mut root, "SET max_error_count=0").unwrap();
    exec(&engine, &mut root, "SELECT SUM(s) FROM diagnostics").unwrap();
    assert_eq!(root.warning_count(), 5);
    assert_eq!(root.warnings.len(), 0);
}

#[test]
fn audit_1029_component_installation_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        engine
            .execute("INSTALL COMPONENT 'file://component_validate_password'")
            .unwrap();
        assert_eq!(
            engine
                .execute("CREATE USER weak IDENTIFIED BY 'x'")
                .unwrap_err()
                .code(),
            1819
        );
        engine.shutdown().unwrap();
    }
    crate::engine::password_policy::set_installed(false);
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    assert_eq!(
        engine
            .execute("CREATE USER weak IDENTIFIED BY 'x'")
            .unwrap_err()
            .code(),
        1819
    );
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT component_urn FROM mysql.component")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(rs.rows.len(), 1);
    engine
        .execute("UNINSTALL COMPONENT 'file://component_validate_password'")
        .unwrap();
    engine
        .execute("CREATE USER weak IDENTIFIED BY 'x'")
        .unwrap();
    engine.shutdown().unwrap();
}

#[test]
fn audit_1029_mixed_case_account_and_password_without_reuse_policy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "CREATE USER MixedCase IDENTIFIED BY 'one'",
        "ALTER USER MixedCase IDENTIFIED BY 'one'",
        "SET PASSWORD FOR MixedCase = 'one'",
    ] {
        exec(&engine, &mut root, sql).unwrap();
    }
    exec(
        &engine,
        &mut root,
        "ALTER USER MixedCase PASSWORD HISTORY 2",
    )
    .unwrap();
    assert_eq!(
        exec(
            &engine,
            &mut root,
            "ALTER USER MixedCase IDENTIFIED BY 'one'"
        )
        .unwrap_err()
        .code(),
        3638
    );
}

#[test]
fn audit_1029_global_all_expands_and_revokes_dynamic_privileges() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(&engine, &mut root, "CREATE USER full_admin").unwrap();
    exec(&engine, &mut root, "GRANT ALL ON *.* TO full_admin").unwrap();
    let user = user_session("full_admin");
    for privilege in auth::default_dynamic_privileges() {
        assert!(
            engine.db.auth_catalog().account_has_privilege(
                &user.authorization_context(),
                &RequiredPrivilege {
                    privilege: privilege.clone(),
                    scope: PrivilegeScope::Global,
                },
                false
            ),
            "{}",
            privilege.sql_name()
        );
    }
    exec(&engine, &mut root, "REVOKE ALL ON *.* FROM full_admin").unwrap();
    assert!(!engine
        .db
        .auth_catalog()
        .grants
        .iter()
        .any(|grant| grant.grantee == user.authenticated_as));
}

#[test]
fn audit_1029_partial_revoke_removes_schema_grant_before_restricting() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "SET GLOBAL partial_revokes=ON",
        "CREATE USER restricted",
        "GRANT SELECT ON *.* TO restricted",
        "GRANT SELECT ON test.* TO restricted",
        "REVOKE SELECT ON test.* FROM restricted",
    ] {
        exec(&engine, &mut root, sql).unwrap();
    }
    assert!(engine.db.auth_catalog().restrictions.is_empty());
    exec(
        &engine,
        &mut root,
        "REVOKE SELECT ON test.* FROM restricted",
    )
    .unwrap();
    assert_eq!(engine.db.auth_catalog().restrictions.len(), 1);
    exec(&engine, &mut root, "REVOKE SELECT ON *.* FROM restricted").unwrap();
    assert!(engine.db.auth_catalog().restrictions.is_empty());
    exec(&engine, &mut root, "SET GLOBAL partial_revokes=OFF").unwrap();
}

#[test]
fn audit_1029_missing_revoke_and_invalid_rename_are_atomic_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in ["CREATE USER u1, u2", "GRANT SELECT ON test.* TO u1"] {
        exec(&engine, &mut root, sql).unwrap();
    }
    assert_eq!(
        exec(&engine, &mut root, "REVOKE SELECT ON test.* FROM u1, u2")
            .unwrap_err()
            .code(),
        1141
    );
    assert!(engine
        .db
        .auth_catalog()
        .grants
        .iter()
        .any(|g| g.grantee.user == "u1" && g.privilege == Privilege::Select));
    assert_eq!(
        exec(
            &engine,
            &mut root,
            &format!("RENAME USER u1 TO '{}'", "x".repeat(33))
        )
        .unwrap_err()
        .code(),
        1470
    );
    exec(&engine, &mut root, "DROP USER u1, u2").unwrap();
}

#[test]
fn audit_1029_create_user_can_manage_roles_and_mandatory_default_role() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in ["CREATE USER manager", "GRANT CREATE USER ON *.* TO manager"] {
        exec(&engine, &mut root, sql).unwrap();
    }
    let mut manager = user_session("manager");
    exec(&engine, &mut manager, "CREATE ROLE r").unwrap();
    exec(&engine, &mut root, "SET GLOBAL mandatory_roles='r'").unwrap();
    exec(&engine, &mut root, "SET DEFAULT ROLE r TO manager").unwrap();
    assert!(engine
        .db
        .auth_catalog()
        .active_default_roles(&manager.authenticated_as)
        .contains(&AccountKey::new("r", "%")));
    exec(&engine, &mut root, "SET GLOBAL mandatory_roles=''").unwrap();
    exec(&engine, &mut manager, "DROP ROLE r").unwrap();
}

#[test]
fn audit_1029_dml_system_subqueries_and_column_definer_view() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "CREATE USER colreader",
        "CREATE TABLE t(a INT, secret INT)",
        "GRANT INSERT, UPDATE, DELETE ON test.t TO colreader",
        "GRANT SELECT(a) ON test.t TO colreader",
        "GRANT CREATE VIEW ON test.* TO colreader",
    ] {
        exec(&engine, &mut root, sql).unwrap();
    }
    let mut user = user_session("colreader");
    for sql in [
        "INSERT INTO test.t SELECT 1,2 FROM information_schema.schemata LIMIT 1",
        "UPDATE test.t SET a=2 WHERE EXISTS(SELECT 1 FROM information_schema.schemata)",
        "CREATE VIEW test.v AS SELECT a FROM test.t",
    ] {
        exec(&engine, &mut user, sql).unwrap();
    }
    exec(&engine, &mut root, "GRANT SELECT ON test.v TO colreader").unwrap();
    exec(&engine, &mut user, "SELECT * FROM test.v").unwrap();
    assert!(exec(
        &engine,
        &mut user,
        "CREATE VIEW test.bad AS SELECT secret FROM test.t"
    )
    .is_err());
    exec(
        &engine,
        &mut root,
        "REVOKE SELECT(a) ON test.t FROM colreader",
    )
    .unwrap();
    assert_eq!(
        exec(&engine, &mut user, "SELECT * FROM test.v")
            .unwrap_err()
            .code(),
        1356
    );
}

#[test]
fn audit_1029_routine_grant_requires_existing_target_and_sql_log_off_is_restricted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(&engine, &mut root, "CREATE USER logger").unwrap();
    for kind in ["PROCEDURE", "FUNCTION"] {
        assert_eq!(
            exec(
                &engine,
                &mut root,
                &format!("GRANT EXECUTE ON {kind} test.absent TO logger")
            )
            .unwrap_err()
            .code(),
            1305
        );
    }
    let mut user = user_session("logger");
    assert_eq!(
        exec(&engine, &mut user, "SET SESSION sql_log_off=1")
            .unwrap_err()
            .code(),
        1227
    );
    exec(
        &engine,
        &mut root,
        "GRANT SESSION_VARIABLES_ADMIN ON *.* TO logger",
    )
    .unwrap();
    exec(&engine, &mut user, "SET SESSION sql_log_off=1").unwrap();
}

#[test]
fn audit_1029_expanding_string_functions_check_packet_before_allocating() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    exec(&engine, &mut root, "SET GLOBAL max_allowed_packet=1024").unwrap();
    for expr in [
        "REPLACE(REPEAT('a',100),'a',REPEAT('x',100))",
        "REPLACE(CAST(REPEAT('a',100) AS BINARY),'a',REPEAT('x',100))",
        "REGEXP_REPLACE(REPEAT('a',100),'(a)',REPEAT('x',100))",
        "REGEXP_REPLACE(REPEAT('a',100),'(a)',CONCAT(REPEAT('x',100),'\\\\1'))",
        "INSERT(REPEAT('a',1000),1,1,REPEAT('x',1000))",
    ] {
        let QueryResponse::ResultSet(rs) =
            exec(&engine, &mut root, &format!("SELECT {expr}")).unwrap()
        else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![Value::Null]], "{expr}");
        assert!(root.warnings.iter().any(|w| w.code() == 1301), "{expr}");
    }
}

#[test]
fn audit_1029_truncate_vector_indexes_accepts_new_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for metric in ["l2", "cosine", "dot"] {
        exec(
            &engine,
            &mut root,
            "CREATE TABLE vt(id INT PRIMARY KEY, v VECTOR(4) NOT NULL)",
        )
        .unwrap();
        exec(
            &engine,
            &mut root,
            &format!("CREATE INDEX ix ON vt(v) USING HNSW WITH (metric={metric})"),
        )
        .unwrap();
        for _ in 0..2 {
            exec(&engine, &mut root, "INSERT INTO vt VALUES(1,'[1,0,0,0]')").unwrap();
            exec(&engine, &mut root, "TRUNCATE TABLE vt").unwrap();
        }
        exec(&engine, &mut root, "INSERT INTO vt VALUES(2,'[0,1,0,0]')").unwrap();
        let QueryResponse::ResultSet(rs) = exec(&engine, &mut root, "SELECT id FROM vt").unwrap()
        else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
        let order = match metric {
            "cosine" => "COSINE_DISTANCE(v,'[0,1,0,0]')",
            "dot" => "-DOT_PRODUCT(v,'[0,1,0,0]')",
            _ => "L2_DISTANCE(v,'[0,1,0,0]')",
        };
        exec(&engine, &mut root, "SET mydb_hnsw_brute_threshold=0").unwrap();
        let QueryResponse::ResultSet(rs) = exec(
            &engine,
            &mut root,
            &format!("SELECT id FROM vt ORDER BY {order} LIMIT 1"),
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
        exec(&engine, &mut root, "DROP TABLE vt").unwrap();
    }
}
