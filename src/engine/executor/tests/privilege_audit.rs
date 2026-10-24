//! Regressions for doc_old/privilege-bugs-audit-2026-09-26.md.
use super::*;
use crate::engine::auth::{self, AccountKey};

fn account_session(engine: &StorageQueryEngine, user: &str) -> SessionContext {
    let key = AccountKey::new(user, "%");
    let auth = engine.db.auth_catalog();
    SessionContext {
        user: user.into(),
        login_user: user.into(),
        authenticated_as: key.clone(),
        account_limits: auth.accounts[&auth::account_id(&key)].resource_limits(),
        auth_epoch: auth.epoch,
        ..SessionContext::default()
    }
}

#[test]
fn audit_password_self_service_cannot_change_account_policy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(&mut root, "CREATE USER audit_policy IDENTIFIED BY 'old'")
        .unwrap();
    let mut user = account_session(&engine, "audit_policy");
    engine
        .execute_with_session(&mut root, "ALTER USER audit_policy ACCOUNT LOCK")
        .unwrap();
    let key = auth::account_id(&user.authenticated_as);
    let before = engine.db.auth_catalog().accounts[&key].clone();
    for option in [
        "ACCOUNT UNLOCK",
        "ACCOUNT LOCK",
        "REQUIRE NONE",
        "REQUIRE SSL",
        "WITH MAX_QUERIES_PER_HOUR 0",
        "WITH MAX_UPDATES_PER_HOUR 0",
        "WITH MAX_CONNECTIONS_PER_HOUR 0",
        "WITH MAX_USER_CONNECTIONS 0",
        "FAILED_LOGIN_ATTEMPTS 0",
        "PASSWORD_LOCK_TIME 0",
        "PASSWORD EXPIRE",
        "PASSWORD EXPIRE NEVER",
        "PASSWORD EXPIRE DEFAULT",
        "PASSWORD HISTORY 0",
        "PASSWORD HISTORY DEFAULT",
        "PASSWORD REUSE INTERVAL 0 DAY",
        "PASSWORD REUSE INTERVAL DEFAULT",
        "PASSWORD REQUIRE CURRENT OPTIONAL",
        "PASSWORD REQUIRE CURRENT DEFAULT",
        "COMMENT 'changed'",
        "ATTRIBUTE '{\"changed\":true}'",
    ] {
        for password in ["IDENTIFIED BY 'new'", "IDENTIFIED BY RANDOM PASSWORD"] {
            let sql = format!("ALTER USER CURRENT_USER() {password} {option}");
            let err = engine.execute_with_session(&mut user, &sql).unwrap_err();
            assert_eq!(err.code(), 1142, "{sql}: {err:?}");
            assert_eq!(engine.db.auth_catalog().accounts[&key], before, "{sql}");
        }
    }
    engine
        .execute_with_session(&mut root, "ALTER USER audit_policy ACCOUNT UNLOCK")
        .unwrap();
    engine
        .execute_with_session(&mut user, "ALTER USER CURRENT_USER() IDENTIFIED BY 'new'")
        .unwrap();
    engine
        .execute_with_session(
            &mut user,
            "ALTER USER CURRENT_USER() IDENTIFIED BY RANDOM PASSWORD",
        )
        .unwrap();
}

#[test]
fn audit_dual_password_authority_does_not_exempt_policy_changes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "CREATE USER audit_dual IDENTIFIED BY 'old'",
        "GRANT APPLICATION_PASSWORD_ADMIN ON *.* TO audit_dual",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut user = account_session(&engine, "audit_dual");
    engine
        .execute_with_session(
            &mut user,
            "ALTER USER CURRENT_USER() IDENTIFIED BY 'new' RETAIN CURRENT PASSWORD",
        )
        .unwrap();
    let key = auth::account_id(&user.authenticated_as);
    let before = engine.db.auth_catalog().accounts[&key].clone();
    for sql in [
        "ALTER USER CURRENT_USER() IDENTIFIED BY 'third' RETAIN CURRENT PASSWORD ACCOUNT UNLOCK",
        "ALTER USER CURRENT_USER() DISCARD OLD PASSWORD WITH MAX_USER_CONNECTIONS 0",
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut user, sql)
                .unwrap_err()
                .code(),
            1142
        );
        assert_eq!(engine.db.auth_catalog().accounts[&key], before);
    }
    engine
        .execute_with_session(&mut user, "ALTER USER CURRENT_USER() DISCARD OLD PASSWORD")
        .unwrap();
}

#[test]
fn audit_create_default_role_requires_grant_authority_and_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "CREATE ROLE audit_r1, audit_r2",
        "CREATE USER audit_creator",
        "GRANT CREATE USER ON *.* TO audit_creator",
        "GRANT audit_r1 TO audit_creator WITH ADMIN OPTION",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let mut creator = account_session(&engine, "audit_creator");
    let sql = "CREATE USER audit_created DEFAULT ROLE audit_r1, audit_r2";
    assert_eq!(
        engine
            .execute_with_session(&mut creator, sql)
            .unwrap_err()
            .code(),
        1227
    );
    let auth = engine.db.auth_catalog();
    let key = AccountKey::new("audit_created", "%");
    assert!(!auth.accounts.contains_key(&auth::account_id(&key)));
    assert!(!auth.role_edges.iter().any(|e| e.to_account == key));
    assert!(!auth.default_roles.iter().any(|e| e.to_account == key));
    engine
        .execute_with_session(
            &mut creator,
            "CREATE USER audit_admin_created DEFAULT ROLE audit_r1",
        )
        .unwrap();
    engine
        .execute_with_session(&mut root, "GRANT ROLE_ADMIN ON *.* TO audit_creator")
        .unwrap();
    engine.execute_with_session(&mut creator, sql).unwrap();
    assert_eq!(engine.db.auth_catalog().active_default_roles(&key).len(), 2);
    engine
        .execute_with_session(&mut root, "GRANT SYSTEM_USER ON *.* TO audit_r2")
        .unwrap();
    assert!(engine
        .execute_with_session(
            &mut creator,
            "CREATE USER audit_system_escalation DEFAULT ROLE audit_r2"
        )
        .is_err());
}

#[test]
fn audit_default_role_unknown_target_rolls_back_all_targets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    for sql in [
        "CREATE ROLE audit_default_r",
        "CREATE USER audit_default_u",
        "GRANT audit_default_r TO audit_default_u",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    for roles in ["ALL", "audit_default_r"] {
        let sql = format!("SET DEFAULT ROLE {roles} TO audit_default_u, audit_missing");
        assert_eq!(
            engine
                .execute_with_session(&mut root, &sql)
                .unwrap_err()
                .code(),
            3523
        );
        assert!(engine.db.auth_catalog().default_roles.is_empty());
    }
    engine
        .execute_with_session(&mut root, "SET DEFAULT ROLE NONE TO audit_missing")
        .unwrap();
}

#[test]
fn audit_constant_and_prepared_queries_share_exact_question_budget() {
    for plan_cache_enabled in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = open_test_engine(dir.path());
        engine.plan_cache_enabled = plan_cache_enabled;
        let mut root = SessionContext::default();
        engine
            .execute_with_session(
                &mut root,
                "CREATE USER audit_questions WITH MAX_QUERIES_PER_HOUR 3",
            )
            .unwrap();
        engine
            .execute_with_session(&mut root, "CREATE TABLE audit_question_table (id INT)")
            .unwrap();
        engine
            .execute_with_session(&mut root, "INSERT INTO audit_question_table VALUES (4)")
            .unwrap();
        engine
            .execute_with_session(
                &mut root,
                "GRANT SELECT ON test.audit_question_table TO audit_questions",
            )
            .unwrap();
        let mut user = account_session(&engine, "audit_questions");
        engine.execute_with_session(&mut user, "SELECT 1").unwrap();
        engine
            .execute_with_session(&mut user, "SELECT 2 + 3")
            .unwrap();
        let sql = "SELECT id FROM audit_question_table";
        let parsed = engine.prepare_cached(&mut user, sql, 0).unwrap();
        let mut memo = crate::engine::api::PreparedSelectMemoState::default();
        QueryEngine::execute_prepared_select(&engine, &mut user, &mut memo, &parsed, sql, &[])
            .unwrap()
            .unwrap()
            .materialize()
            .unwrap();
        let err = engine
            .execute_with_session(&mut user, "SELECT 1")
            .unwrap_err();
        assert_eq!(err.code(), 1226);
        assert!(err.message().contains("audit_questions"));
        assert!(!err.message().contains("%s"));
        assert_eq!(
            QueryEngine::execute_prepared_select(&engine, &mut user, &mut memo, &parsed, sql, &[])
                .unwrap()
                .unwrap_err()
                .code(),
            1226
        );
        auth::clear_user_resources(&user.authenticated_as);
    }
}

#[test]
fn audit_password_lock_time_uses_mysql_keyword_and_persists() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(
            &mut root,
            "CREATE USER audit_locktime FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1",
        )
        .unwrap();
    let key = auth::account_id(&AccountKey::new("audit_locktime", "%"));
    assert_eq!(
        engine.db.auth_catalog().accounts[&key].password_lock_time_days,
        Some(1)
    );
    engine
        .execute_with_session(
            &mut root,
            "ALTER USER audit_locktime PASSWORD_LOCK_TIME UNBOUNDED",
        )
        .unwrap();
    assert_eq!(
        engine.db.auth_catalog().accounts[&key].password_lock_time_days,
        Some(u16::MAX)
    );
    for sql in [
        "CREATE USER audit_bad PASSWORD LOCK TIME",
        "CREATE USER audit_bad PASSWORD LOCK TIME 1",
        "ALTER USER audit_locktime PASSWORD LOCK TIME 1",
        "CREATE USER audit_bad PASSWORD_LOCK_TIME",
        "ALTER USER audit_locktime PASSWORD_LOCK_TIME nope",
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut root, sql)
                .unwrap_err()
                .code(),
            1064,
            "{sql}"
        );
    }
}

#[test]
fn audit_history_errors_identify_account_on_both_password_paths() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut root = SessionContext::default();
    engine
        .execute_with_session(
            &mut root,
            "CREATE USER audit_history IDENTIFIED BY 'old' PASSWORD HISTORY 3",
        )
        .unwrap();
    engine
        .execute_with_session(&mut root, "ALTER USER audit_history IDENTIFIED BY 'new'")
        .unwrap();
    for sql in [
        "ALTER USER audit_history IDENTIFIED BY 'old'",
        "SET PASSWORD FOR audit_history = 'old'",
    ] {
        let err = engine.execute_with_session(&mut root, sql).unwrap_err();
        assert_eq!(err.code(), 3638);
        assert!(err.message().contains("audit_history"), "{err:?}");
    }
}

#[test]
fn audit_all_resource_errors_include_account_and_failed_login_does_not_charge() {
    let key = AccountKey::new("audit_resource_messages", "%");
    auth::clear_user_resources(&key);
    auth::charge_question(&key, 1).unwrap();
    auth::charge_update(&key, 1).unwrap();
    auth::charge_new_connection(&key, 2, 1).unwrap();
    for err in [
        auth::charge_question(&key, 1).unwrap_err(),
        auth::charge_update(&key, 1).unwrap_err(),
        auth::charge_new_connection(&key, 2, 1).unwrap_err(),
    ] {
        assert_eq!(err.code(), 1226);
        assert!(err.message().contains("audit_resource_messages"));
    }
    auth::release_connection(&key);
    // The rejected simultaneous connection must not consume the hourly quota.
    auth::charge_new_connection(&key, 2, 1).unwrap();
    auth::release_connection(&key);
    let err = auth::charge_new_connection(&key, 2, 1).unwrap_err();
    assert_eq!(err.code(), 1226);
    assert!(err.message().contains("audit_resource_messages"));
    assert!(err.message().contains("max_connections_per_hour"));
    auth::clear_user_resources(&key);
}
