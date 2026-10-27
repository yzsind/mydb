use super::*;

fn account(engine: &StorageQueryEngine, root: &mut SessionContext, user: &str) -> SessionContext {
    exec(
        engine,
        root,
        &format!("CREATE USER '{user}'@'localhost' IDENTIFIED BY 'test-only'"),
    );
    exec(
        engine,
        root,
        &format!("GRANT AI_USE ON *.* TO '{user}'@'localhost'"),
    );
    let mut session = SessionContext::default();
    session.authenticated_as = crate::engine::auth::AccountKey::new(user, "localhost");
    session.user = user.into();
    session.login_user = user.into();
    session.current_user_is_system = false;
    session.current_database = "test".into();
    session.connection_id = 20;
    session
}
fn response(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    prompt: &str,
    timeout: u64,
) -> QueryResponse {
    <StorageQueryEngine as QueryEngine>::execute(engine,session,&format!("SELECT AI_GENERATE_DETAIL('test.chat','{prompt}',JSON_OBJECT('timeout_ms',{timeout},'on_error','return'))")).unwrap()
}
fn spawn(
    response: QueryResponse,
) -> thread::JoinHandle<Result<QueryResponse, crate::types::QueryError>> {
    thread::spawn(move || response.materialize())
}
fn pending_count() -> u64 {
    crate::engine::ai::runtime::status()
        .into_iter()
        .find(|(name, _)| *name == "queued_and_running")
        .unwrap()
        .1
}
fn wait_for(mock: &Mock, pending: u64, received: usize) {
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while pending_count() != pending || mock.requests.lock().unwrap().len() != received {
        assert!(
            std::time::Instant::now() < until,
            "expected pending={pending}, requests={received}; got pending={}, requests={}",
            pending_count(),
            mock.requests.lock().unwrap().len()
        );
        thread::sleep(Duration::from_millis(1));
    }
}
fn set_model_limit(engine: &StorageQueryEngine, session: &mut SessionContext, limit: usize) {
    exec(engine,session,&format!("ALTER AI MODEL test.chat WITH CONFIG '{{\"kind\":\"text_generation\",\"remote_model\":\"mock-chat\",\"model_revision\":\"v1\",\"defaults\":{{\"max_output_tokens\":32}},\"limits\":{{\"max_inflight\":{limit}}}}}'"));
}

#[test]
fn ai_queued_user_does_not_hoard_shared_connection_slots() {
    let gate = Arc::new(ResponseGate::default());
    let blocked = gate.clone();
    let mock = Mock::responding(move |_, request| {
        let text = request["messages"][0]["content"].as_str().unwrap();
        if text == "holder" {
            blocked.wait();
        }
        answer(text)
    });
    with_provider(&mock, |mock, dir, engine, root| {
        let mut other = account(engine, root, "independent");
        let config = json!({"protocol":"ollama","base_url":format!("http://127.0.0.1:{}",mock.port),"auth":{"type":"none"},"limits":{"max_inflight":2}});
        exec(
            engine,
            root,
            &format!("ALTER AI CONNECTION local WITH CONFIG '{config}'"),
        );
        set_user_policy(mock, dir, engine, root, json!({"max_inflight":1}));
        let first = spawn(response(engine, root, "holder", 2000));
        wait_for(mock, 1, 1);
        let second = spawn(response(engine, root, "queued", 2000));
        wait_for(mock, 2, 1);
        let independent=engine.execute_with_session(&mut other,"SELECT AI_GENERATE_DETAIL('test.chat','independent',JSON_OBJECT('timeout_ms',120,'on_error','return'))");
        gate.release();
        let a = first.join().unwrap();
        let b = second.join().unwrap();
        assert!(a.is_ok() && b.is_ok());
        assert_idle();
        let independent = json_result(independent.unwrap());
        assert_eq!(
            independent["ok"], true,
            "queued user occupied a shared slot: {independent}"
        );
        assert_eq!(independent["text"], "independent");
    });
}

#[test]
fn ai_tightening_model_concurrency_preserves_existing_inflight_accounting() {
    let gate = Arc::new(ResponseGate::default());
    let blocked = gate.clone();
    let mock = Mock::responding(move |_, request| {
        let text = request["messages"][0]["content"].as_str().unwrap();
        if text == "holder" {
            blocked.wait();
        }
        answer(text)
    });
    with_provider(&mock, |mock, _, engine, root| {
        set_model_limit(engine, root, 2);
        let first = spawn(response(engine, root, "holder", 2000));
        let second = spawn(response(engine, root, "holder", 2000));
        wait_for(mock, 2, 2);
        set_model_limit(engine, root, 1);
        let value=json_result(exec(engine,root,"SELECT AI_GENERATE_DETAIL('test.chat','must-wait',JSON_OBJECT('timeout_ms',80,'on_error','return'))"));
        gate.release();
        gate.release();
        assert!(first.join().unwrap().is_ok());
        assert!(second.join().unwrap().is_ok());
        assert_idle();
        assert_eq!(
            value["ok"], false,
            "new capacity created a second independent gate: {value}"
        );
        assert_eq!(value["error"]["code"], "ER_AI_TIMEOUT");
        assert_eq!(mock.requests.lock().unwrap().len(), 2);
        exec(
            engine,
            root,
            "SELECT AI_GENERATE('test.chat','after release')",
        );
        assert_idle();
    });
}

#[test]
fn ai_rate_waiters_do_not_spend_other_users_shared_tokens() {
    with_mock(|mock, dir, engine, root| {
        let mut other = account(engine, root, "rate_independent");
        exec(
            engine,
            root,
            r#"ALTER AI MODEL test.chat WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1","defaults":{"max_output_tokens":32},"limits":{"requests_per_minute":3}}'"#,
        );
        set_user_policy(mock, dir, engine, root, json!({"requests_per_minute":1}));
        exec(engine, root, "SELECT AI_GENERATE('test.chat','first')");
        let first = spawn(response(engine, root, "queued-a", 350));
        let second = spawn(response(engine, root, "queued-b", 350));
        wait_for(mock, 2, 1);
        let independent=engine.execute_with_session(&mut other,"SELECT AI_GENERATE_DETAIL('test.chat','independent',JSON_OBJECT('timeout_ms',120,'on_error','return'))");
        assert!(first.join().unwrap().is_ok());
        assert!(second.join().unwrap().is_ok());
        assert_idle();
        let value = json_result(independent.unwrap());
        assert_eq!(
            value["ok"], true,
            "canceled user waiters spent shared model tokens: {value}"
        );
        assert_eq!(mock.requests.lock().unwrap().len(), 2);
    });
}

#[cfg(unix)]
#[test]
fn ai_connection_auth_change_cancels_previously_queued_credentials() {
    let gate = Arc::new(ResponseGate::default());
    let blocked = gate.clone();
    let mock = Mock::responding(move |_, request| {
        let text = request["messages"][0]["content"].as_str().unwrap();
        if text == "holder" {
            blocked.wait();
        }
        answer(text)
    });
    with_provider(&mock, |mock, _, engine, root| {
        exec(
            engine,
            root,
            "CREATE AI SECRET old_key VALUE 'dummy-old-credential'",
        );
        exec(
            engine,
            root,
            "CREATE AI SECRET new_key VALUE 'dummy-new-credential'",
        );
        let origin = format!("http://127.0.0.1:{}", mock.port);
        let config = |secret: &str| json!({"protocol":"ollama","base_url":origin,"auth":{"type":"bearer","secret":secret}});
        exec(
            engine,
            root,
            &format!(
                "ALTER AI CONNECTION local WITH CONFIG '{}'",
                config("old_key")
            ),
        );
        set_model_limit(engine, root, 1);
        let first = spawn(response(engine, root, "holder", 2000));
        wait_for(mock, 1, 1);
        let second = spawn(response(engine, root, "queued", 2000));
        wait_for(mock, 2, 1);
        exec(
            engine,
            root,
            &format!(
                "ALTER AI CONNECTION local WITH CONFIG '{}'",
                config("new_key")
            ),
        );
        gate.release();
        let _first = first.join().unwrap();
        let queued = second.join().unwrap();
        assert_idle();
        exec(engine, root, "SELECT AI_GENERATE('test.chat','fresh')");
        let records = mock.requests.lock().unwrap();
        assert!(
            queued.is_err(),
            "queued request used the superseded connection: {queued:?}"
        );
        assert!(queued.unwrap_err().is(ER_AI_DISABLED));
        assert_eq!(records.len(), 2);
        assert_eq!(
            records.last().unwrap()["_test_authorization"],
            "Bearer dummy-new-credential"
        );
    });
}

#[test]
fn ai_authentication_latch_does_not_survive_database_lifetime_restart() {
    let denied = Arc::new(AtomicBool::new(true));
    let status = denied.clone();
    let mock = Mock::responding(move |_, _| {
        if status.load(Ordering::Acquire) {
            reply(401, b"{}", &[])
        } else {
            answer("recovered")
        }
    });
    with_provider(&mock, |mock, dir, _, _| {
        let path = dir.join("reopened");
        let engine = open_strict_test_engine(&path);
        let mut root = SessionContext::default();
        let config = json!({"protocol":"ollama","base_url":format!("http://127.0.0.1:{}",mock.port),"auth":{"type":"none"}});
        exec(
            &engine,
            &mut root,
            &format!("CREATE AI CONNECTION local WITH CONFIG '{config}'"),
        );
        exec(
            &engine,
            &mut root,
            r#"CREATE AI MODEL test.chat USING local WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1"}'"#,
        );
        let value=json_result(exec(&engine,&mut root,"SELECT AI_GENERATE_DETAIL('test.chat','before restart',JSON_OBJECT('on_error','return'))"));
        assert_eq!(value["error"]["code"], "ER_AI_UPSTREAM_AUTH");
        engine.shutdown().unwrap();
        drop(engine);
        denied.store(false, Ordering::Release);
        let reopened = open_strict_test_engine(&path);
        let value=json_result(exec(&reopened,&mut root,"SELECT AI_GENERATE_DETAIL('test.chat','after restart',JSON_OBJECT('on_error','return'))"));
        reopened.shutdown().unwrap();
        assert_idle();
        assert_eq!(
            value["ok"], true,
            "old database lifetime retained an authentication failure: {value}"
        );
        assert_eq!(mock.requests.lock().unwrap().len(), 2);
    });
}

fn completion(
    response: QueryResponse,
) -> (
    thread::JoinHandle<()>,
    std::sync::mpsc::Receiver<Result<QueryResponse, crate::types::QueryError>>,
) {
    let (tx, rx) = std::sync::mpsc::channel();
    (
        thread::spawn(move || {
            let _ = tx.send(response.materialize());
        }),
        rx,
    )
}

#[test]
fn ai_live_grant_and_role_revocation_cancel_inflight_and_queued_calls() {
    for via_role in [false, true] {
        let gate = Arc::new(ResponseGate::default());
        let blocked = gate.clone();
        let mock = Mock::responding(move |_, request| {
            if request["messages"][0]["content"] == "holder" {
                blocked.wait();
            }
            answer("allowed")
        });
        with_provider(&mock, |mock, _, engine, root| {
            let mut user = account(engine, root, "revocable");
            exec(
                engine,
                root,
                "REVOKE AI_USE ON *.* FROM 'revocable'@'localhost'",
            );
            if via_role {
                exec(engine, root, "CREATE ROLE 'live_ai_role'@'%'");
                exec(
                    engine,
                    root,
                    "GRANT AI_USE ON AI MODEL test.chat TO 'live_ai_role'@'%'",
                );
                exec(
                    engine,
                    root,
                    "GRANT 'live_ai_role'@'%' TO 'revocable'@'localhost'",
                );
                exec(engine, &mut user, "SET ROLE 'live_ai_role'@'%'");
            } else {
                exec(
                    engine,
                    root,
                    "GRANT AI_USE ON AI MODEL test.chat TO 'revocable'@'localhost'",
                );
            }
            set_model_limit(engine, root, 1);
            let (a, ra) = completion(response(engine, &mut user, "holder", 2000));
            wait_for(mock, 1, 1);
            let (b, rb) = completion(response(engine, &mut user, "queued", 2000));
            wait_for(mock, 2, 1);
            let revoke = if via_role {
                "REVOKE 'live_ai_role'@'%' FROM 'revocable'@'localhost'"
            } else {
                "REVOKE AI_USE ON AI MODEL test.chat FROM 'revocable'@'localhost'"
            };
            exec(engine, root, revoke);
            let first = ra.recv_timeout(Duration::from_secs(1));
            let second = rb.recv_timeout(Duration::from_secs(1));
            gate.release();
            a.join().unwrap();
            b.join().unwrap();
            assert_idle();
            for result in [first, second] {
                let error = result
                    .expect("revocation did not promptly cancel active call")
                    .unwrap_err();
                assert!(error.is(ER_AI_ACCESS_DENIED), "{error:?}");
            }
            assert_eq!(
                mock.requests.lock().unwrap().len(),
                1,
                "revoked queued call reached the provider"
            );
        });
    }
}

#[test]
fn ai_kill_of_queued_call_releases_admission_without_poisoning_session() {
    let gate = Arc::new(ResponseGate::default());
    let blocked = gate.clone();
    let mock = Mock::responding(move |_, request| {
        if request["messages"][0]["content"] == "holder" {
            blocked.wait();
        }
        answer("allowed")
    });
    with_provider(&mock, |mock, _, engine, root| {
        set_model_limit(engine, root, 1);
        let holder = spawn(response(engine, root, "holder", 2000));
        wait_for(mock, 1, 1);
        let mut victim = root.clone();
        victim.connection_id = 9001;
        let handle =
            engine
                .registry()
                .register(9001, "root".into(), "localhost".into(), "test".into());
        victim.set_cancel_handle(Some(handle.cancel_handle()));
        let (worker, done) = completion(response(engine, &mut victim, "queued", 2000));
        wait_for(mock, 2, 1);
        engine.registry().kill_query(9001).unwrap();
        let result = done.recv_timeout(Duration::from_secs(1));
        gate.release();
        worker.join().unwrap();
        assert!(holder.join().unwrap().is_ok());
        assert_idle();
        assert!(result
            .expect("KILL did not wake queued call")
            .unwrap_err()
            .is(ER_QUERY_INTERRUPTED));
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
        victim.refresh_cancel_generations();
        exec(engine, &mut victim, "SELECT 1");
        exec(engine, root, "SELECT AI_GENERATE('test.chat','after KILL')");
        assert_idle();
    });
}

#[test]
fn ai_shutdown_cancels_inflight_and_queued_http_and_releases_memory() {
    let gate = Arc::new(ResponseGate::default());
    let blocked = gate.clone();
    let mock = Mock::responding(move |_, request| {
        if request["messages"][0]["content"] == "holder" {
            blocked.wait();
        }
        answer("allowed")
    });
    with_provider(&mock, |mock, _, engine, root| {
        set_model_limit(engine, root, 1);
        let first = spawn(response(engine, root, "holder", 2000));
        wait_for(mock, 1, 1);
        let second = spawn(response(engine, root, "queued", 2000));
        wait_for(mock, 2, 1);
        engine.shutdown().unwrap();
        gate.release();
        for result in [first.join().unwrap(), second.join().unwrap()] {
            assert!(result.unwrap_err().is(ER_AI_DISABLED));
        }
        assert_idle();
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
    });
}

#[test]
fn ai_cancellation_while_building_catalog_candidate_cannot_commit_it() {
    with_mock(|mock, _, engine, _| {
        let before = engine.db.ai_catalog().unwrap();
        let token = crate::engine::cancel::QueryCancelToken::noop().fork_local();
        let guard = crate::engine::cancel::enter(token.clone());
        let result = engine.db.mutate_ai_catalog(|candidate| {
            candidate.models.get_mut("test.chat").unwrap().enabled = false;
            token.cancel_local();
            Ok(())
        });
        drop(guard);
        assert!(result.is_err(), "canceled catalog candidate committed");
        assert!(result.unwrap_err().is(ER_QUERY_INTERRUPTED));
        assert!(Arc::ptr_eq(&before, &engine.db.ai_catalog().unwrap()));
        assert!(mock.requests.lock().unwrap().is_empty());
    });
}

#[test]
fn ai_drop_recreate_racing_stale_grant_never_grants_the_new_identity() {
    with_mock(|mock, _, engine, root| {
        let mut user = account(engine, root, "identity_race");
        exec(
            engine,
            root,
            "REVOKE AI_USE ON *.* FROM 'identity_race'@'localhost'",
        );
        for _ in 0..12 {
            let old_id = engine.db.ai_catalog().unwrap().models["test.chat"].id;
            let barrier = std::sync::Barrier::new(2);
            let mut ddl = root.clone();
            let mut grantor = root.clone();
            thread::scope(|scope| {
                let a=scope.spawn(||{barrier.wait();exec(engine,&mut ddl,"DROP AI MODEL test.chat");exec(engine,&mut ddl,r#"CREATE AI MODEL test.chat USING local WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1"}'"#);});
                let b = scope.spawn(|| {
                    barrier.wait();
                    let result = engine.execute_with_session(
                        &mut grantor,
                        &format!(
                            "GRANT AI_MODEL_{old_id}_USE ON *.* TO 'identity_race'@'localhost'"
                        ),
                    );
                    if let Err(error) = result {
                        assert!(error.is(ER_AI_MODEL_NOT_FOUND), "{error:?}");
                    }
                });
                a.join().unwrap();
                b.join().unwrap();
            });
            assert_ne!(
                engine.db.ai_catalog().unwrap().models["test.chat"].id,
                old_id
            );
            assert_error(
                engine,
                &mut user,
                "SELECT AI_GENERATE('test.chat','must stay denied')",
                ER_AI_ACCESS_DENIED,
            );
            assert!(!engine.db.auth_catalog().grants.iter().any(|grant|matches!(&grant.privilege,crate::engine::auth::Privilege::Dynamic(name) if name==&format!("AI_MODEL_{old_id}_USE"))));
        }
        assert!(mock.requests.lock().unwrap().is_empty());
    });
}

#[cfg(unix)]
#[test]
fn ai_catalog_failures_rollback_secret_updates_and_model_grants() {
    with_mock(|mock, dir, _, _| {
        for stage in ["before_dd", "after_dd_rows"] {
            let path = dir.join(stage);
            let engine = open_strict_test_engine(&path);
            let mut root = SessionContext::default();
            root.ssl_cipher = Some("test-TLS".into());
            let config = json!({"protocol":"ollama","base_url":format!("http://127.0.0.1:{}",mock.port),"auth":{"type":"none"}});
            exec(
                &engine,
                &mut root,
                &format!("CREATE AI CONNECTION local WITH CONFIG '{config}'"),
            );
            exec(
                &engine,
                &mut root,
                r#"CREATE AI MODEL test.chat USING local WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1"}'"#,
            );
            exec(
                &engine,
                &mut root,
                "CREATE AI SECRET recovery_key VALUE 'dummy-original-key'",
            );
            let mut user = account(&engine, &mut root, "recovery_user");
            exec(
                &engine,
                &mut root,
                "REVOKE AI_USE ON *.* FROM 'recovery_user'@'localhost'",
            );
            exec(
                &engine,
                &mut root,
                "GRANT AI_USE ON AI MODEL test.chat TO 'recovery_user'@'localhost'",
            );
            for sql in [
                "ALTER AI SECRET recovery_key VALUE 'dummy-replacement-key'",
                "DROP AI MODEL test.chat",
            ] {
                let before = engine.db.ai_catalog().unwrap();
                let auth = engine.db.auth_catalog();
                let result = crate::engine::ai::catalog_faults::with_failure(stage, || {
                    engine.execute_with_session(&mut root, sql)
                });
                assert!(result.unwrap_err().is(ER_GET_ERRNO));
                assert!(Arc::ptr_eq(&before, &engine.db.ai_catalog().unwrap()));
                assert!(Arc::ptr_eq(&auth, &engine.db.auth_catalog()));
            }
            engine.shutdown().unwrap();
            drop(engine);
            let reopened = open_strict_test_engine(&path);
            let state = reopened.db.ai_catalog().unwrap();
            assert!(state.models.contains_key("test.chat"));
            assert_eq!(state.secrets["recovery_key"].generation, 1);
            let keys = crate::engine::ai::load_keys(reopened.db.data_dir(), &state, false).unwrap();
            assert_eq!(
                &*crate::engine::ai::decrypt_secret(&state, &keys, &state.secrets["recovery_key"])
                    .unwrap(),
                b"dummy-original-key"
            );
            exec(
                &reopened,
                &mut user,
                "SELECT AI_GENERATE('test.chat','grant survived rollback')",
            );
            reopened.shutdown().unwrap();
        }
    });
}

#[test]
fn ai_catalog_crash_child() {
    let Ok(path) = std::env::var("MYDB_AI_TEST_CHILD_DIR") else {
        return;
    };
    let engine = open_strict_test_engine(std::path::Path::new(&path));
    let mut root = SessionContext::default();
    root.ssl_cipher = Some("test-TLS".into());
    let stage = std::env::var("MYDB_AI_TEST_CHILD_STAGE").unwrap();
    let operation = std::env::var("MYDB_AI_TEST_CHILD_OPERATION").unwrap();
    std::env::set_var("MYDB_AI_TEST_CRASH_STAGE", stage);
    exec(
        &engine,
        &mut root,
        if operation == "secret" {
            "ALTER AI SECRET recovery_key VALUE 'dummy-replacement-key'"
        } else {
            "DROP AI MODEL test.chat"
        },
    );
    panic!("AI catalog crash boundary did not execute");
}

#[cfg(unix)]
#[test]
fn ai_catalog_crash_recovery_keeps_ciphertext_and_grant_retirement_atomic() {
    with_mock(|mock, dir, _, _| {
        for operation in ["secret", "drop"] {
            for stage in [
                "before_dd",
                "after_dd_rows",
                "after_durable",
                "after_publish",
            ] {
                let path = dir.join(format!("{operation}-{stage}"));
                let engine = open_strict_test_engine(&path);
                let mut root = SessionContext::default();
                root.ssl_cipher = Some("test-TLS".into());
                let config = json!({"protocol":"ollama","base_url":format!("http://127.0.0.1:{}",mock.port),"auth":{"type":"none"}});
                exec(
                    &engine,
                    &mut root,
                    &format!("CREATE AI CONNECTION local WITH CONFIG '{config}'"),
                );
                exec(
                    &engine,
                    &mut root,
                    r#"CREATE AI MODEL test.chat USING local WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1"}'"#,
                );
                exec(
                    &engine,
                    &mut root,
                    "CREATE AI SECRET recovery_key VALUE 'dummy-original-key'",
                );
                let mut user = account(&engine, &mut root, "crash_user");
                exec(
                    &engine,
                    &mut root,
                    "REVOKE AI_USE ON *.* FROM 'crash_user'@'localhost'",
                );
                exec(
                    &engine,
                    &mut root,
                    "GRANT AI_USE ON AI MODEL test.chat TO 'crash_user'@'localhost'",
                );
                let model_id = engine.db.ai_catalog().unwrap().models["test.chat"].id;
                engine.shutdown().unwrap();
                drop(engine);
                let output=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","engine::executor::tests::ai::regression::deep_review::ai_catalog_crash_child","--nocapture"])
                    .env("MYDB_AI_TEST_CHILD_DIR",&path).env("MYDB_AI_TEST_CHILD_STAGE",stage).env("MYDB_AI_TEST_CHILD_OPERATION",operation).env("MYDB_AI_LOCAL_CONFIG",dir.join("policy.json")).env_remove("MYDB_AI_ENCRYPT_KEY").output().unwrap();
                assert_eq!(
                    output.status.code(),
                    Some(74),
                    "{operation}/{stage}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let reopened = open_strict_test_engine(&path);
                let state = reopened.db.ai_catalog().unwrap();
                let durable = matches!(stage, "after_durable" | "after_publish");
                let grant=reopened.db.auth_catalog().grants.iter().any(|grant|grant.grantee==user.authenticated_as && matches!(&grant.privilege,crate::engine::auth::Privilege::Dynamic(name) if name==&format!("AI_MODEL_{model_id}_USE")));
                assert_eq!(
                    state.models.contains_key("test.chat"),
                    operation != "drop" || !durable,
                    "{operation}/{stage}"
                );
                assert_eq!(
                    grant,
                    operation != "drop" || !durable,
                    "{operation}/{stage}"
                );
                let keys =
                    crate::engine::ai::load_keys(reopened.db.data_dir(), &state, false).unwrap();
                let secret = &state.secrets["recovery_key"];
                let expected = if operation == "secret" && durable {
                    b"dummy-replacement-key".as_slice()
                } else {
                    b"dummy-original-key".as_slice()
                };
                assert_eq!(
                    &*crate::engine::ai::decrypt_secret(&state, &keys, secret).unwrap(),
                    expected,
                    "{operation}/{stage}"
                );
                assert!(!serde_json::to_string(&*state).unwrap().contains("dummy-"));
                if operation == "drop" && durable {
                    exec(
                        &reopened,
                        &mut root,
                        r#"CREATE AI MODEL test.chat USING local WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"new-space"}'"#,
                    );
                    assert_error(
                        &reopened,
                        &mut user,
                        "SELECT AI_GENERATE('test.chat','must not inherit retired grant')",
                        ER_AI_ACCESS_DENIED,
                    );
                }
                reopened.shutdown().unwrap();
            }
        }
    });
}

#[test]
fn ai_auth_failure_prevents_waiting_requests_from_reaching_provider() {
    let gate = Arc::new(ResponseGate::default());
    let blocked = gate.clone();
    let mock = Mock::responding(move |_, request| {
        if request["messages"][0]["content"] == "holder" {
            blocked.wait();
        }
        reply(401, b"{}", &[])
    });
    with_provider(&mock, |mock, _, engine, root| {
        set_model_limit(engine, root, 1);
        let first = spawn(response(engine, root, "holder", 2000));
        wait_for(mock, 1, 1);
        let second = spawn(response(engine, root, "waiting", 2000));
        wait_for(mock, 2, 1);
        gate.release();
        for result in [first.join().unwrap(), second.join().unwrap()] {
            let value = json_result(result.unwrap());
            assert_eq!(value["error"]["code"], "ER_AI_UPSTREAM_AUTH");
        }
        assert_idle();
        assert_eq!(
            mock.requests.lock().unwrap().len(),
            1,
            "queued request ignored an authentication failure learned while waiting"
        );
    });
}

#[test]
fn ai_open_circuit_prevents_previously_waiting_requests_from_dispatching() {
    let gate = Arc::new(ResponseGate::default());
    let blocked = gate.clone();
    let mock = Mock::responding(move |_, request| {
        if request["messages"][0]["content"] == "holder" {
            blocked.wait();
        }
        reply(503, b"{}", &[])
    });
    with_provider(&mock, |mock, _, engine, root| {
        set_model_limit(engine, root, 1);
        let mut calls = vec![spawn(response(engine, root, "holder", 2000))];
        wait_for(mock, 1, 1);
        for _ in 0..5 {
            calls.push(spawn(response(engine, root, "waiting", 2000)));
        }
        wait_for(mock, 6, 1);
        gate.release();
        let mut circuit_errors = 0;
        for call in calls {
            match call.join().unwrap() {
                Ok(result) => assert_eq!(json_result(result)["error"]["http_status"], 503),
                Err(error) => {
                    assert!(error.is(ER_AI_CIRCUIT_OPEN), "{error:?}");
                    circuit_errors += 1;
                }
            }
        }
        assert_idle();
        assert_eq!(
            mock.requests.lock().unwrap().len(),
            5,
            "a queued call bypassed the newly opened circuit"
        );
        assert_eq!(circuit_errors, 1);
    });
}
