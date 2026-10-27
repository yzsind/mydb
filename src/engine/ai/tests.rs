use super::*;
use crate::parser::{parse_statement, Statement};
use rand::{rngs::OsRng, RngCore};

fn temp() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::Builder::new()
        .prefix("llm-crypto-")
        .tempdir_in("/tmp/mydb")
        .unwrap()
}
#[test]
fn ai_json_rejects_duplicates_depth_and_trailing_input() {
    assert!(parse_json(r#"{"a":1,"a":2}"#, 100).is_err());
    assert!(parse_json(r#"{"a":{"x":1,"x":2}}"#, 100).is_err());
    assert!(parse_json(&format!("{}0{}", "[".repeat(33), "]".repeat(33)), 1000).is_err());
    assert!(parse_json("{} {}", 100).is_err());
    assert_eq!(
        parse_json(r#"{"a":[1,true,null]}"#, 100).unwrap()["a"][0],
        1
    );
}
#[cfg(unix)]
#[test]
fn ai_secret_authentication_and_randomized_envelopes() {
    crate::util::env::with_vars(&[("MYDB_AI_ENCRYPT_KEY", None)], || {
        let dir = temp();
        let mut c = AiCatalog {
            encryption_id: "test-encryption-domain".into(),
            ..Default::default()
        };
        let keys = load_keys(dir.path(), &AiCatalog::default(), true).unwrap();
        c.active_key_id = keys.candidate.clone();
        let a = encrypt_secret(&c, &keys, 1, 1, "dummy-api-key").unwrap();
        let b = encrypt_secret(&c, &keys, 1, 1, "dummy-api-key").unwrap();
        assert_ne!(a.nonce, b.nonce);
        assert_ne!(a.ciphertext, b.ciphertext);
        assert_ne!(a.wrapped_dek, b.wrapped_dek);
        assert_eq!(&*decrypt_secret(&c, &keys, &a).unwrap(), b"dummy-api-key");
        let encoded = serde_json::to_string(&a).unwrap();
        assert!(!encoded.contains("dummy-api-key"));
        let mut wrong = a.clone();
        wrong.id = 2;
        assert!(decrypt_secret(&c, &keys, &wrong).is_err());
        let mut wrong = a.clone();
        wrong.generation += 1;
        assert!(decrypt_secret(&c, &keys, &wrong).is_err());
        let mut wrong = a.clone();
        wrong.ciphertext[0] ^= 1;
        assert!(decrypt_secret(&c, &keys, &wrong).is_err());
        let mut wrong = a.clone();
        wrong.wrapped_dek[0] ^= 1;
        assert!(decrypt_secret(&c, &keys, &wrong).is_err());
        c.secrets.insert("key".into(), a);
        std::fs::remove_file(dir.path().join("apikey.mydb")).unwrap();
        assert!(load_keys(dir.path(), &c, true).is_err());
        assert!(!dir.path().join("apikey.mydb").exists());
    });
}
#[test]
fn ai_environment_key_is_strict_and_never_persisted() {
    let dir = temp();
    for key in [
        "",
        "apikey.mydb",
        "hex:bad",
        "hex:éééééééééééééééééééééééééééééééé",
    ] {
        crate::util::env::with_vars(&[("MYDB_AI_ENCRYPT_KEY", Some(key))], || {
            assert!(load_keys(dir.path(), &AiCatalog::default(), true).is_err())
        });
    }
    let mut key = [0; 32];
    OsRng.fill_bytes(&mut key);
    let encoded = format!(
        "hex:{}",
        key.iter().map(|x| format!("{x:02x}")).collect::<String>()
    );
    crate::util::env::with_vars(&[("MYDB_AI_ENCRYPT_KEY", Some(&encoded))], || {
        let k = load_keys(dir.path(), &AiCatalog::default(), true).unwrap();
        assert_eq!(k.keys.len(), 1);
        assert!(!dir.path().join("apikey.mydb").exists());
    });
}
#[test]
fn ai_parser_numbers_secret_parameters_and_redacts_all_spellings() {
    for sql in [
        "CREATE AI SECRET key VALUE ?",
        "CREATE/**/AI/**/SECRET key VALUE ?",
        "ALTER AI SECRET key VALUE ?",
    ] {
        let mut statement = parse_statement(sql).unwrap();
        let mut count = 0;
        crate::parser::number_statement_placeholders(&mut statement, &mut count);
        assert_eq!(count, 1);
        assert!(crate::engine::logging::redact_sql_for_log(sql).contains("redacted"));
        if let Statement::Ai(a) = statement {
            assert!(matches!(a.value, Some(crate::parser::Expr::Placeholder(0))));
        } else {
            panic!("not AI statement");
        }
    }
    let sql = "CREATE AI SECRET key VALUE 'dummy-secret-not-for-logs'";
    let statement = parse_statement(sql).unwrap();
    assert!(!format!("{statement:?}").contains("dummy-secret"));
    assert!(!serde_json::to_string(&statement)
        .unwrap()
        .contains("dummy-secret"));
    assert!(parse_statement("CREATE AI CONNECTION c WITH CONFIG '{}' unexpected").is_err());
}

#[test]
fn ai_sql_detection_keeps_ordinary_ai_named_relations_on_cached_sql_paths() {
    for sql in [
        "SELECT id FROM ai_vectors WHERE id=?",
        "SELECT id FROM ai.documents WHERE id=?",
        "SELECT AI_GENERATE FROM data WHERE id=?",
        "SELECT id FROM ai_embed WHERE id=?",
    ] {
        assert!(!crate::engine::ai::sql::sql_may_have_ai(sql), "{sql}");
    }
    for sql in [
        "SELECT AI_GENERATE ('m',?)",
        "SELECT `AI_EMBED`('m',?)",
        "CREATE/**/AI/**/SECRET x VALUE 'dummy'",
        "SET @source='CREATE AI SECRET x VALUE ?'",
        "PREPARE x FROM 'CREATE AI SECRECT x VALUE ?'",
    ] {
        assert!(crate::engine::ai::sql::sql_may_have_ai(sql), "{sql}");
    }
}

#[test]
fn ai_json_unicode_duplicates_and_exact_depth_and_size_boundaries() {
    assert!(parse_json(r#"{"a":1,"\u0061":2}"#, 100).is_err());
    let exact = format!("{}0{}", "[".repeat(32), "]".repeat(32));
    assert!(parse_json(&exact, exact.len()).is_ok());
    assert!(parse_json(&exact, exact.len() - 1)
        .unwrap_err()
        .is(crate::error::catalog::ER_AI_INVALID_ARGUMENT));
    let text = r#"{"text":"😀 [ { \\\" ] }"}"#;
    assert!(parse_json(text, text.len()).is_ok());
    assert!(parse_json(text, text.len() - 1).is_err());
}
#[test]
fn ai_default_policy_allows_all_networks_without_local_config() {
    crate::util::env::with_vars(&[("MYDB_AI_LOCAL_CONFIG", None)], || {
        let config = LocalConfig::load().unwrap();
        for host in [
            "8.8.8.8",
            "169.254.169.254",
            "0.0.0.0",
            "239.1.2.3",
            "255.255.255.255",
            "198.18.0.1",
            "192.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.254",
            "192.168.1.10",
            "100.64.0.1",
            "100.127.255.254",
            "[fd00::1]",
            "[fe80::1]",
            "[::]",
            "[ff02::1]",
            "[2001:db8::1]",
            "[::ffff:169.254.169.254]",
            "[::ffff:10.1.2.3]",
        ] {
            for scheme in ["http", "https"] {
                let url = config
                    .check_url(&format!("{scheme}://{host}:11434/v1"))
                    .unwrap();
                let ip = host.trim_matches(['[', ']']).parse().unwrap();
                assert!(config.check_ip(ip).is_ok(), "{url}");
            }
        }
        for hostname in ["models.internal", "models.example", "localhost"] {
            assert!(config
                .check_url(&format!("http://{hostname}:8000/v1"))
                .is_ok());
        }
    });
}

#[test]
fn ai_default_policy_preserves_url_validation_and_reload() {
    crate::util::env::with_vars(&[("MYDB_AI_LOCAL_CONFIG", None)], || {
        let config = LocalConfig::load().unwrap();
        for url in [
            "https://models.example/v1",
            "http://models.internal:8000/v1",
            "http://localhost:11434",
            "http://LOCALHOST.:8080/v1",
            "http://127.0.0.1:11434",
            "http://127.0.0.2:8000",
            "http://[::1]:11434",
            "http://[::ffff:127.0.0.1]:8000",
            "http://8.8.8.8/v1",
            "http://169.254.169.254",
        ] {
            assert!(config.check_url(url).is_ok(), "{url}");
        }
        for url in [
            "https://user:password@models.example/v1",
            "https://models.example/v1?api_key=x",
            "https://models.example/v1#fragment",
            "file:///tmp/mydb/forbidden",
        ] {
            assert!(config.check_url(url).is_err(), "{url}");
        }
        LocalConfig::reload().unwrap();
        assert!(!config.generation_is_current().unwrap());
        assert!(config.current().unwrap().generation_is_current().unwrap());
        assert!(LocalConfig::load()
            .unwrap()
            .generation_is_current()
            .unwrap());
    });
}

#[test]
fn ai_optional_policy_preserves_defaults_and_explicit_errors() {
    let dir = temp();
    let path = dir.path().join("policy.json");
    crate::util::env::with_vars(&[("MYDB_AI_LOCAL_CONFIG", path.to_str())], || {
        assert!(LocalConfig::load().is_err());
        std::fs::write(
            &path,
            r#"{"version":1,"user_limits":[{"user":"app","host":"%","max_inflight":2}]}"#,
        )
        .unwrap();
        let config = LocalConfig::load().unwrap();
        assert_eq!(config.user_limits[0].max_inflight, Some(2));
        assert!(config.check_url("http://localhost:11434").is_ok());
        assert!(config.check_ip("127.0.0.1".parse().unwrap()).is_ok());
        assert!(config.check_url("http://models.internal:8000/v1").is_ok());
        assert!(config.check_url("http://169.254.169.254").is_ok());
        assert!(config.check_ip("169.254.169.254".parse().unwrap()).is_ok());
        std::fs::write(&path, r#"{"version":1,"egress":{"allowed_origins":[]}}"#).unwrap();
        let restricted = LocalConfig::reload().unwrap();
        assert!(restricted.check_url("http://localhost:11434").is_err());
        assert!(restricted.check_url("https://models.example").is_err());
        assert!(restricted.check_ip("127.0.0.1".parse().unwrap()).is_err());
        assert!(restricted.check_url("http://10.1.2.3:8000").is_err());
        assert!(restricted.check_ip("10.1.2.3".parse().unwrap()).is_err());
        for invalid in [
            r#"{"version":2}"#,
            r#"{"version":1,"unknown":true}"#,
            "broken",
        ] {
            std::fs::write(&path, invalid).unwrap();
            assert!(LocalConfig::reload().is_err());
            assert!(LocalConfig::load()
                .unwrap()
                .check_url("https://models.example")
                .is_err());
            assert!(restricted.generation_is_current().unwrap());
        }
        std::fs::remove_file(&path).unwrap();
        assert!(LocalConfig::reload().is_err());
        assert!(restricted.generation_is_current().unwrap());
    });
    crate::util::env::with_vars(&[("MYDB_AI_LOCAL_CONFIG", Some(""))], || {
        assert!(LocalConfig::load().is_err());
    });
}

#[test]
fn ai_egress_checks_canonical_origins_url_components_and_private_ranges() {
    let mut config:LocalConfig=serde_json::from_value(serde_json::json!({"version":1,"egress":{"allowed_origins":["https://models.example","http://127.0.0.1:11434"],"private_destinations":[],"allow_plaintext_origins":[]}})).unwrap();
    assert!(config.check_url("https://MODELS.example:443/v1").is_ok());
    for url in [
        "https://models.example.evil/v1",
        "https://user:password@models.example/v1",
        "https://models.example/v1?api_key=x",
        "https://models.example/v1#fragment",
        "http://models.example/v1",
        "file:///tmp/mydb/forbidden",
        "http://127.0.0.1:11434",
    ] {
        assert!(config.check_url(url).is_err(), "{url}");
    }
    for ip in [
        "127.0.0.1",
        "10.1.2.3",
        "169.254.169.254",
        "100.64.0.1",
        "198.18.0.1",
        "239.1.2.3",
        "255.255.255.255",
        "0.0.0.0",
        "::1",
        "fc00::1",
        "fe80::1",
        "::ffff:127.0.0.1",
    ] {
        assert!(config.check_ip(ip.parse().unwrap()).is_err(), "{ip}");
    }
    assert!(config.check_ip("8.8.8.8".parse().unwrap()).is_ok());
    config
        .egress
        .as_mut()
        .unwrap()
        .private_destinations
        .push("127.0.0.1/32".parse().unwrap());
    assert!(config.check_ip("::ffff:127.0.0.1".parse().unwrap()).is_ok());
    assert!(config.check_ip("127.0.0.2".parse().unwrap()).is_err());
    config
        .egress
        .as_mut()
        .unwrap()
        .allow_plaintext_origins
        .push("http://127.0.0.1:11434".into());
    assert!(config.check_url("http://127.0.0.1:11434/api").is_ok());
}
#[cfg(unix)]
#[test]
fn ai_keyring_rejects_insecure_permissions_and_symlink_substitution() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    crate::util::env::with_vars(&[("MYDB_AI_ENCRYPT_KEY", None)], || {
        let dir = temp();
        let keys = load_keys(dir.path(), &AiCatalog::default(), true).unwrap();
        let path = dir.path().join("apikey.mydb");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load_keys(dir.path(), &AiCatalog::default(), false)
            .err()
            .unwrap()
            .is(crate::error::catalog::ER_AI_ENCRYPTION_KEY_INVALID));
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let original = dir.path().join("original-keyring");
        std::fs::rename(&path, &original).unwrap();
        let before = std::fs::read(&original).unwrap();
        symlink(&original, &path).unwrap();
        assert!(load_keys(dir.path(), &AiCatalog::default(), true).is_err());
        assert_eq!(std::fs::read(&original).unwrap(), before);
        std::fs::remove_file(&path).unwrap();
        std::fs::rename(&original, &path).unwrap();
        assert_eq!(
            load_keys(dir.path(), &AiCatalog::default(), false)
                .unwrap()
                .candidate,
            keys.candidate
        );
    });
}
#[cfg(unix)]
#[test]
fn ai_envelopes_reject_cross_database_replay_and_malformed_metadata() {
    crate::util::env::with_vars(&[("MYDB_AI_ENCRYPT_KEY", None)], || {
        let dir = temp();
        let keys = load_keys(dir.path(), &AiCatalog::default(), true).unwrap();
        let state = AiCatalog {
            encryption_id: "database-one".into(),
            active_key_id: keys.candidate.clone(),
            ..Default::default()
        };
        let secret = encrypt_secret(&state, &keys, 7, 1, "dummy-envelope-key").unwrap();
        let other = AiCatalog {
            encryption_id: "database-two".into(),
            ..state.clone()
        };
        assert!(decrypt_secret(&other, &keys, &secret)
            .unwrap_err()
            .is(crate::error::catalog::ER_AI_SECRET_DECRYPT_FAILED));
        for corrupt in 0..4 {
            let mut value = secret.clone();
            match corrupt {
                0 => value.nonce.pop().map(|_| ()).unwrap(),
                1 => value.ciphertext.clear(),
                2 => value.format_version = 99,
                _ => value.wrapped_dek[0] ^= 1,
            }
            assert!(
                decrypt_secret(&state, &keys, &value).is_err(),
                "corruption {corrupt}"
            );
        }
        let mut value = secret.clone();
        value.key_id = "unknown".into();
        assert!(decrypt_secret(&state, &keys, &value)
            .unwrap_err()
            .is(crate::error::catalog::ER_AI_ENCRYPTION_KEY_MISSING));
        assert_eq!(
            &*decrypt_secret(&state, &keys, &secret).unwrap(),
            b"dummy-envelope-key"
        );
    });
}

#[cfg(unix)]
#[test]
fn ai_failed_master_rotation_does_not_partially_rewrap_any_secret() {
    crate::util::env::with_vars(&[("MYDB_AI_ENCRYPT_KEY", None)], || {
        let dir = temp();
        let old = load_keys(dir.path(), &AiCatalog::default(), true).unwrap();
        let mut catalog = AiCatalog {
            encryption_id: "rotation-atomicity".into(),
            active_key_id: old.candidate.clone(),
            ..Default::default()
        };
        catalog.secrets.insert(
            "first".into(),
            encrypt_secret(&catalog, &old, 1, 1, "dummy-first").unwrap(),
        );
        catalog.secrets.insert(
            "second".into(),
            encrypt_secret(&catalog, &old, 2, 1, "dummy-second").unwrap(),
        );
        let encoded = format!("hex:{}", "a3".repeat(32));
        let mut candidate =
            crate::util::env::with_vars(&[("MYDB_AI_ENCRYPT_KEY", Some(&encoded))], || {
                load_keys(dir.path(), &catalog, false).unwrap()
            });
        candidate.keys.extend(old.keys);
        catalog.secrets.get_mut("second").unwrap().ciphertext[0] ^= 1;
        let before = serde_json::to_value(&catalog).unwrap();
        assert!(super::crypto::rewrap(&mut catalog, &candidate)
            .unwrap_err()
            .is(crate::error::catalog::ER_AI_SECRET_DECRYPT_FAILED));
        assert_eq!(serde_json::to_value(&catalog).unwrap(), before);
        assert_eq!(
            &*decrypt_secret(&catalog, &candidate, &catalog.secrets["first"]).unwrap(),
            b"dummy-first"
        );
    });
}
