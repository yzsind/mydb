use super::*;
fn root() -> SessionContext {
    let mut s = SessionContext::default();
    s.mysql_authenticated = true;
    s.ssl_cipher = Some("TLS_AES_256_GCM_SHA384".into());
    s
}
fn rows(engine: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(r) = engine
        .execute_with_session(s, sql)
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("Expected rows")
    };
    r.rows
}
fn temp() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb/rest").unwrap();
    tempfile::tempdir_in("/tmp/mydb/rest").unwrap()
}
#[test]
fn rest_api_key_reveal_recovery_revocation_and_ownership() {
    let dir = temp();
    let token;
    {
        let engine = open_strict_test_engine(dir.path());
        let mut s = root();
        let created = rows(&engine, &mut s, "/* leading */ CREATE API KEY 'backend'");
        assert_eq!(created.len(), 1);
        assert_eq!(created[0][4], Value::Null);
        token = created[0][6].to_display_string();
        assert!(token.starts_with("mydb_ak_"));
        assert_eq!(
            rows(&engine, &mut s, "SHOW API KEY 'backend'")[0][6].to_display_string(),
            token
        );
        let request = crate::engine::auth::AuthRequest::legacy("", "127.0.0.1", &[], None);
        assert!(engine.authenticate_api_key(&token, &request).is_ok());
        assert!(engine
            .authenticate_api_key(&format!("{}x", token), &request)
            .is_err());
        let mut denied = s.clone();
        denied.mysql_authenticated = false;
        assert!(engine
            .execute_with_session(&mut denied, "SHOW API KEY 'backend'")
            .is_err());
        let mut rest = s.clone();
        rest.rest_identity = Some(Arc::new(crate::engine::api::RestSessionIdentity {
            key_id: "fake".into(),
            owner: rest.authenticated_as.clone(),
        }));
        assert!(engine
            .execute_with_session(&mut rest, "CREATE API KEY 'cannot-mint'")
            .is_err());
        engine
            .execute_with_session(&mut s, "CREATE USER 'rename_me'@'%'")
            .unwrap();
        rows(
            &engine,
            &mut s,
            "CREATE API KEY 'owned' FOR 'rename_me'@'%'",
        );
        engine
            .execute_with_session(&mut s, "RENAME USER 'rename_me'@'%' TO 'renamed'@'%'")
            .unwrap();
        assert_eq!(
            rows(&engine, &mut s, "SHOW API KEYS FOR 'renamed'@'%'").len(),
            1
        );
        engine
            .execute_with_session(&mut s, "DROP USER 'renamed'@'%'")
            .unwrap();
        assert_eq!(engine.db.auth_catalog().api_keys.len(), 1);
        engine.db.checkpoint().unwrap();
    }
    {
        let engine = open_strict_test_engine(dir.path());
        let mut s = root();
        assert_eq!(
            rows(&engine, &mut s, "SHOW API KEY 'backend'")[0][6].to_display_string(),
            token
        );
        let request = crate::engine::auth::AuthRequest::legacy("", "127.0.0.1", &[], None);
        assert!(engine.authenticate_api_key(&token, &request).is_ok());
        engine
            .execute_with_session(&mut s, "DROP API KEY 'backend'")
            .unwrap();
        assert!(engine.authenticate_api_key(&token, &request).is_err());
        assert!(engine
            .execute_with_session(&mut s, "SHOW API KEY 'backend'")
            .is_err());
    }
}
#[test]
fn rest_api_key_quota_expiration_and_durable_ciphertext() {
    let dir = temp();
    let engine = open_strict_test_engine(dir.path());
    let mut s = root();
    let r = rows(
        &engine,
        &mut s,
        "CREATE API KEY 'first' EXPIRES AT '2099-01-01 00:00:00'",
    );
    let token = r[0][6].to_display_string();
    assert!(engine
        .execute_with_session(
            &mut s,
            "CREATE API KEY 'past' EXPIRES AT '2000-01-01 00:00:00'"
        )
        .is_err());
    let owner = s.authenticated_as.clone();
    engine
        .db
        .mutate_auth_catalog(|a| {
            let original = a.api_keys.values().next().unwrap().clone();
            for n in 1..1024 {
                let mut r = original.clone();
                r.id = format!("fixture-{n}");
                r.name = format!("fixture-{n}");
                a.api_keys.insert(r.id.clone(), r);
            }
            assert_eq!(
                a.api_keys.values().filter(|k| k.owner == owner).count(),
                1024
            );
            Ok(())
        })
        .unwrap();
    assert!(engine
        .execute_with_session(&mut s, "CREATE API KEY 'overflow'")
        .unwrap_err()
        .message()
        .contains("1024"));
    engine
        .execute_with_session(&mut s, "DROP API KEY 'fixture-1'")
        .unwrap();
    rows(&engine, &mut s, "CREATE API KEY 'replacement'");
    engine.db.checkpoint().unwrap();
    fn scan(path: &std::path::Path, token: &[u8]) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                scan(&path, token)
            } else {
                let bytes = std::fs::read(&path).unwrap();
                assert!(
                    !bytes.windows(token.len()).any(|v| v == token),
                    "plaintext in {}",
                    path.display()
                );
            }
        }
    }
    scan(dir.path(), token.as_bytes());
}
#[test]
fn rest_api_key_parser_rejects_ambiguous_or_multiple_commands() {
    for sql in [
        "CREATE API KEY 'x'; SELECT 1",
        "SHOW API KEYS 'extra'",
        "DROP API KEYS",
        "CREATE API KEY 'x' EXPIRES AT 'invalid'",
        "SHOW API KEY 'x' FOR 'u'",
        "ALTER INSTANCE ROTATE API KEY MASTER KEY garbage",
    ] {
        assert!(parse_statement(sql).is_err(), "{sql}");
    }
    assert!(matches!(
        parse_statement("CREATE API KEY 'x'").unwrap(),
        Statement::ApiKey(_)
    ));
}

#[test]
fn rest_api_key_master_rotation_and_authenticated_envelopes() {
    use base64::Engine;
    let dir = temp();
    let engine = open_strict_test_engine(dir.path());
    let mut s = root();
    let original = rows(&engine, &mut s, "CREATE API KEY 'rotate'")[0][6].to_display_string();
    let path = dir.path().join("rest-keys.mydb");
    let mut file: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let key = [0x79u8; 32];
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"mydb.rest.ak.kek.v1\0");
    hash.update(key);
    let id: String = hash.finalize().iter().map(|b| format!("{b:02x}")).collect();
    file["active_key_id"] = serde_json::json!(id);
    file["keys"][&id] = serde_json::json!(format!(
        "base64:{}",
        base64::engine::general_purpose::STANDARD.encode(key)
    ));
    std::fs::write(&path, serde_json::to_vec(&file).unwrap()).unwrap();
    engine
        .execute_with_session(&mut s, "ALTER INSTANCE ROTATE API KEY MASTER KEY")
        .unwrap();
    assert_eq!(
        rows(&engine, &mut s, "SHOW API KEY 'rotate'")[0][6].to_display_string(),
        original
    );
    crate::engine::access_keys::validate_keys(engine.db_handle()).unwrap();
    engine
        .db
        .mutate_auth_catalog(|a| {
            let mut record = a.api_keys.values().next().unwrap().clone();
            record.envelope.nonce[0] ^= 1;
            a.api_keys.insert(record.id.clone(), record);
            Ok(())
        })
        .unwrap();
    assert!(engine
        .execute_with_session(&mut s, "SHOW API KEY 'rotate'")
        .is_err());
    assert!(crate::engine::access_keys::validate_keys(engine.db_handle()).is_err());
}
