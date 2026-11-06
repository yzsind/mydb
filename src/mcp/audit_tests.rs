use super::*;

fn items(query: &str) -> Vec<codec::SqlItem> {
    vec![tools::statement(query.into())]
}
async fn wait_query(h: &Harness, query: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if h.engine
                .registry()
                .snapshot()
                .iter()
                .any(|p| p.info.as_deref() == Some(query))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}
async fn wait_closed(session: &Arc<sql::Session<StorageQueryEngine>>) -> bool {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !session.closed.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .is_ok()
}
async fn open(h: &Harness) -> Arc<sql::Session<StorageQueryEngine>> {
    let result = h.call("open_session", json!({"database":"test"})).await;
    let id = result["structuredContent"]["id"].as_str().unwrap();
    h.sql.sessions.lock().get(id).unwrap().clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_closed_session_execution_is_rejected_without_panic() {
    let h = Harness::new(McpConfig::default());
    let session = open(&h).await;
    session.close().await;
    let state = h.sql.clone();
    let survived = tokio::spawn(async move {
        sql::start(
            state.clone(),
            session,
            items("SELECT 1"),
            false,
            false,
            1000,
            "audit_closed_session_001".into(),
            sql::OutputFormat::Json,
            state.memory(1024).unwrap(),
        )
        .await
        .is_err()
    })
    .await;
    h.close().await;
    assert!(
        survived.is_ok_and(|rejected| rejected),
        "closing a session before execution must return an error, not panic"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_registered_request_cannot_lose_early_cancellation() {
    let h = Harness::new(McpConfig::default());
    let session = open(&h).await;
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let held_session = session.clone();
    let holder = std::thread::spawn(move || {
        let _context = held_session.context.lock();
        held_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    held_rx.recv().unwrap();
    let state = h.sql.clone();
    let active = session.clone();
    let id = "audit_early_cancellation_001";
    let running = tokio::spawn(async move {
        let mut execution = sql::start(
            state.clone(),
            active,
            items("SELECT SLEEP(1)"),
            false,
            false,
            3000,
            id.into(),
            sql::OutputFormat::Structured {
                max_rows: 100,
                max_bytes: 8192,
                explain_only: false,
            },
            state.memory(1024).unwrap(),
        )
        .await?;
        let chunk = execution.next().await.unwrap().unwrap();
        let sql::Payload::Structured(value, _) = chunk.payload else {
            panic!()
        };
        Ok::<_, Failure>(value)
    });
    // The original implementation publishes the request while blocked on the
    // context lock, then refreshes generations after accepting its cancellation.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let early = h.sql.request(id, &session.key_id).ok();
    if let Some(record) = &early {
        record.cancel();
    }
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    if early.is_none() {
        wait_query(&h, "SELECT SLEEP(1)").await;
        h.sql.request(id, &session.key_id).unwrap().cancel();
    }
    let value = tokio::time::timeout(Duration::from_secs(3), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    h.close().await;
    assert!(
        value["error"].is_object(),
        "accepted cancellation was lost: {value}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_failed_request_status_is_successful_tool_data() {
    let h = Harness::new(McpConfig::default());
    let id = "audit_failed_request_001";
    let query = h
        .call(
            "execute_sql",
            json!({"sql":"SELECT * FROM no_such_audit_table","request_id":id}),
        )
        .await;
    assert_eq!(query["isError"], true);
    let status = h.call("get_request", json!({"request_id":id})).await;
    let cancel = h.call("cancel_request", json!({"request_id":id})).await;
    h.close().await;
    assert_eq!(
        status["isError"], false,
        "a successful status lookup is not a SQL execution failure: {status}"
    );
    assert_eq!(status["structuredContent"]["state"], "failed");
    assert_eq!(cancel["isError"], false);
    assert_eq!(
        cancel["structuredContent"]["cancel_requested"], false,
        "completed work was not cancelled"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_unpolled_delivery_expires_and_releases_transaction() {
    let h = Harness::new(McpConfig::default());
    h.sql("CREATE TABLE stalled_audit (id INT PRIMARY KEY)")
        .await;
    let session = open(&h).await;
    for query in ["BEGIN", "INSERT INTO stalled_audit VALUES (1)"] {
        assert_eq!(
            h.call("execute_sql", json!({"session_id":session.id,"sql":query}))
                .await["isError"],
            false
        );
    }
    let mut execution = sql::start(
        h.sql.clone(),
        session.clone(),
        items("SELECT 1"),
        false,
        false,
        1000,
        "audit_stalled_delivery_001".into(),
        sql::OutputFormat::Structured {
            max_rows: 100,
            max_bytes: 8192,
            explain_only: false,
        },
        h.sql.memory(1024).unwrap(),
    )
    .await
    .unwrap();
    let chunk = execution.next().await.unwrap().unwrap();
    let sql::Payload::Structured(_, memory) = chunk.payload else {
        panic!()
    };
    let request = h.request("tools/list", json!({}));
    let identity = sql::authenticate(&h.sql, request.headers(), Harness::peer()).unwrap();
    let lease = Arc::new(Lease {
        identity,
        arguments: None,
        execution: Mutex::new(Some(execution)),
        created: Mutex::new(None),
        memory: Mutex::new(vec![memory]),
        permit: Mutex::new(None),
        delivery: AtomicU8::new(DELIVERY_PENDING),
        delivery_frames: AtomicUsize::new(0),
        supervisor: Mutex::new(None),
        persistent_created: AtomicBool::new(false),
        connection: None,
    });
    let delivery = Delivery::new(
        Box::pin(tokio_stream::pending()),
        lease,
        Duration::from_millis(10),
    );
    // A network stack stalled on flow control need not poll the response body.
    let closed_before_drop = wait_closed(&session).await;
    drop(delivery);
    h.close().await;
    assert!(
        closed_before_drop,
        "unpolled response retained its transaction past the delivery deadline"
    );
    assert!(h.engine.registry().snapshot().is_empty());
    assert_eq!(
        h.sql("SELECT COUNT(*) FROM stalled_audit").await["results"][0]["rows"],
        json!([["0"]])
    );
    h.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_revoked_key_cannot_prevent_atomic_rollback() {
    let h = Harness::new(McpConfig::default());
    h.sql("CREATE TABLE revoked_atomic_audit (id INT PRIMARY KEY)")
        .await;
    let request = h.request("tools/call", json!({"name":"execute_sql","arguments":{
        "database":"test","atomic":true,"request_id":"audit_revoked_atomic_001",
        "statements":[{"sql":"INSERT INTO revoked_atomic_audit VALUES (1)"},{"sql":"SELECT SLEEP(10)"}]}}));
    let running = tokio::spawn(h.app.clone().oneshot(request));
    wait_query(&h, "SELECT SLEEP(10)").await;
    let mut root = SessionContext::default();
    root.mysql_authenticated = true;
    h.engine
        .execute_with_session(&mut root, "DROP API KEY 'mcp-test'")
        .unwrap();
    h.sql.sweep().await;
    let (_, value, _) = Harness::response(
        tokio::time::timeout(Duration::from_secs(3), running)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    )
    .await;
    h.close().await;
    let result = &value["result"]["structuredContent"];
    assert_eq!(
        result["error"]["rollback_confirmed"], true,
        "credential revocation blocked internal rollback: {value}"
    );
    assert_eq!(result["transaction_state"], "idle");
    assert_eq!(result["execution_outcome"], "failed");
    let QueryResponse::ResultSet(rows) = h
        .engine
        .execute_with_session(&mut root, "SELECT COUNT(*) FROM test.revoked_atomic_audit")
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(rows.rows[0][0].to_display_string(), "0");
    assert!(h.engine.registry().snapshot().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_other_key_cannot_inspect_cancel_or_close_live_work() {
    let h = Harness::new(McpConfig::default());
    let session = open(&h).await;
    let request = h.request(
        "tools/call",
        json!({"name":"execute_sql","arguments":{
        "session_id":session.id,"sql":"SELECT SLEEP(10)","request_id":"audit_isolated_live_001"}}),
    );
    let running = tokio::spawn(h.app.clone().oneshot(request));
    wait_query(&h, "SELECT SLEEP(10)").await;
    let other = Harness::key(&h.engine, "other-audit-key");
    for (name, args) in [
        (
            "get_request",
            json!({"request_id":"audit_isolated_live_001"}),
        ),
        (
            "cancel_request",
            json!({"request_id":"audit_isolated_live_001"}),
        ),
        ("get_session", json!({"session_id":session.id})),
        ("close_session", json!({"session_id":session.id})),
    ] {
        let mut request = h.request("tools/call", json!({"name":name,"arguments":args}));
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {other}").parse().unwrap());
        let (_, result, _) = Harness::response(h.app.clone().oneshot(request).await.unwrap()).await;
        assert_eq!(
            result["result"]["structuredContent"]["error"]["code"], "NOT_FOUND",
            "{result}"
        );
    }
    assert!(!session.closed.load(Ordering::Acquire));
    assert_eq!(
        h.sql
            .request("audit_isolated_live_001", &session.key_id)
            .unwrap()
            .state
            .lock()["state"],
        "running"
    );
    h.call(
        "cancel_request",
        json!({"request_id":"audit_isolated_live_001"}),
    )
    .await;
    Harness::response(running.await.unwrap().unwrap()).await;
    h.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_rejected_atomic_call_preserves_existing_transaction() {
    let h = Harness::new(McpConfig::default());
    h.sql("CREATE TABLE existing_atomic_audit (id INT PRIMARY KEY)")
        .await;
    let session = open(&h).await;
    for query in ["BEGIN", "INSERT INTO existing_atomic_audit VALUES (1)"] {
        assert_eq!(
            h.call("execute_sql", json!({"session_id":session.id,"sql":query}))
                .await["isError"],
            false
        );
    }
    let rejected = h
        .call(
            "execute_sql",
            json!({"session_id":session.id,"atomic":true,"sql":"SELECT 1"}),
        )
        .await;
    let count = h
        .call(
            "execute_sql",
            json!({"session_id":session.id,"sql":"SELECT COUNT(*) FROM existing_atomic_audit"}),
        )
        .await;
    h.close().await;
    assert_eq!(
        rejected["structuredContent"]["error"]["code"],
        "TRANSACTION_ACTIVE"
    );
    assert_eq!(
        rejected["structuredContent"]["transaction_state"], "active",
        "rejecting an atomic request rolled back a caller-owned transaction: {rejected}"
    );
    assert_eq!(
        count["structuredContent"]["results"][0]["rows"],
        json!([["1"]])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_failed_session_creation_keeps_cleanup_retryable() {
    use crate::engine::auth::AccountKey;
    use std::sync::atomic::AtomicUsize;
    struct FailedCreation {
        inner: Arc<StorageQueryEngine>,
        cleanups: AtomicUsize,
    }
    impl QueryEngine for FailedCreation {
        fn execute(
            &self,
            session: &mut SessionContext,
            query: &str,
        ) -> std::result::Result<QueryResponse, crate::types::QueryError> {
            self.inner.execute_with_session(session, query)
        }
        fn api_key_is_valid(&self, id: &str, owner: &AccountKey) -> bool {
            self.inner.api_key_is_valid(id, owner)
        }
        fn init_session_database(
            &self,
            _: &mut SessionContext,
            _: &str,
        ) -> std::result::Result<(), crate::types::QueryError> {
            Err(crate::error::catalog::ER_BAD_DB_ERROR
                .error("Injected database initialization failure"))
        }
        fn close_session(
            &self,
            session: &mut SessionContext,
        ) -> std::result::Result<(), crate::types::QueryError> {
            if self.cleanups.fetch_add(1, Ordering::AcqRel) == 0 {
                return Err(
                    crate::error::catalog::ER_UNKNOWN_ERROR.error("Injected cleanup failure")
                );
            }
            self.inner.close_session(session)
        }
    }
    let h = Harness::new(McpConfig::default());
    let request = h.request("tools/list", json!({}));
    let identity = sql::authenticate(&h.sql, request.headers(), Harness::peer()).unwrap();
    let engine = Arc::new(FailedCreation {
        inner: h.engine.clone(),
        cleanups: AtomicUsize::new(0),
    });
    let state = sql::State::new(
        RestConfig::from_startup().unwrap(),
        engine.clone(),
        h.engine.registry().clone(),
    );
    assert!(
        sql::create_session(state.clone(), identity, Some("missing".into()))
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            state.sweep().await;
            if h.engine.registry().snapshot().is_empty() && state.used.load(Ordering::Acquire) == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("failed initialization must retain a retryable cleanup owner");
    assert!(
        h.engine.registry().snapshot().is_empty(),
        "failed initialization lost its session, so cleanup could not retry"
    );
    assert!(engine.cleanups.load(Ordering::Acquire) >= 2);
    assert_eq!(state.slots.available_permits(), state.config.max_sessions);
    assert_eq!(state.used.load(Ordering::Acquire), 0);
    h.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_failed_atomic_rollback_closes_session_before_reuse() {
    use crate::engine::auth::AccountKey;
    struct FailedRollback(Arc<StorageQueryEngine>);
    impl QueryEngine for FailedRollback {
        fn execute(
            &self,
            session: &mut SessionContext,
            query: &str,
        ) -> std::result::Result<QueryResponse, crate::types::QueryError> {
            if query == "ROLLBACK" {
                return Err(
                    crate::error::catalog::ER_UNKNOWN_ERROR.error("Injected rollback failure")
                );
            }
            self.0.execute_with_session(session, query)
        }
        fn api_key_is_valid(&self, id: &str, owner: &AccountKey) -> bool {
            self.0.api_key_is_valid(id, owner)
        }
        fn close_session(
            &self,
            session: &mut SessionContext,
        ) -> std::result::Result<(), crate::types::QueryError> {
            self.0.close_session(session)
        }
    }
    let h = Harness::new(McpConfig::default());
    h.sql("CREATE TABLE failed_rollback_audit (id INT PRIMARY KEY)")
        .await;
    let request = h.request("tools/list", json!({}));
    let identity = sql::authenticate(&h.sql, request.headers(), Harness::peer()).unwrap();
    let state = sql::State::new(
        RestConfig::from_startup().unwrap(),
        Arc::new(FailedRollback(h.engine.clone())),
        h.engine.registry().clone(),
    );
    let session = sql::create_session(state.clone(), identity, Some("test".into()))
        .await
        .unwrap();
    state
        .sessions
        .lock()
        .insert(session.id.clone(), session.clone());
    let mut execution = sql::start(
        state.clone(),
        session.clone(),
        vec![
            tools::statement("INSERT INTO failed_rollback_audit VALUES (1)".into()),
            tools::statement("INSERT INTO failed_rollback_audit VALUES (1)".into()),
        ],
        true,
        false,
        3000,
        "audit_failed_rollback_001".into(),
        sql::OutputFormat::Structured {
            max_rows: 100,
            max_bytes: 8192,
            explain_only: false,
        },
        state.memory(1024).unwrap(),
    )
    .await
    .unwrap();
    let chunk = execution.next().await.unwrap().unwrap();
    let sql::Payload::Structured(value, _) = chunk.payload else {
        panic!()
    };
    let rejected_reuse = state.session(&session.id, &session.key_id).is_err();
    drop(execution);
    state.close_all().await;
    h.close().await;
    assert!(
        rejected_reuse,
        "failed atomic rollback left partial writes available to COMMIT: {value}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_cancelled_mcp_worker_retains_its_input_memory_charge() {
    use crate::engine::auth::{AccountKey, AuthOutcome, AuthRequest};
    struct GatedInput {
        inner: Arc<StorageQueryEngine>,
        gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
        entered: AtomicBool,
    }
    impl QueryEngine for GatedInput {
        fn execute(
            &self,
            session: &mut SessionContext,
            query: &str,
        ) -> std::result::Result<QueryResponse, crate::types::QueryError> {
            self.entered.store(true, Ordering::Release);
            let held = self.gate.0.lock().unwrap();
            drop(self.gate.1.wait_while(held, |open| !*open).unwrap());
            self.inner.execute_with_session(session, query)
        }
        fn authenticate_api_key(
            &self,
            token: &str,
            request: &AuthRequest<'_>,
        ) -> std::result::Result<(String, AuthOutcome), crate::types::QueryError> {
            self.inner.authenticate_api_key(token, request)
        }
        fn api_key_is_valid(&self, id: &str, owner: &AccountKey) -> bool {
            self.inner.api_key_is_valid(id, owner)
        }
        fn close_session(
            &self,
            session: &mut SessionContext,
        ) -> std::result::Result<(), crate::types::QueryError> {
            self.inner.close_session(session)
        }
    }
    let h = Harness::new(McpConfig::default());
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let engine = Arc::new(GatedInput {
        inner: h.engine.clone(),
        gate: gate.clone(),
        entered: AtomicBool::new(false),
    });
    let state = sql::State::new(
        RestConfig::from_startup().unwrap(),
        engine.clone(),
        h.engine.registry().clone(),
    );
    let app = router(
        state.clone(),
        McpConfig {
            max_inflight: 1,
            ..McpConfig::default()
        },
    )
    .layer(Extension(Harness::peer()));
    let request = h.request(
        "tools/call",
        json!({"name":"execute_sql","arguments":{"sql":"SELECT ?","params":["x".repeat(65536)]}}),
    );
    let running = tokio::spawn(app.clone().oneshot(request));
    let entered = tokio::time::timeout(Duration::from_secs(3), async {
        while !engine.entered.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .is_ok();
    running.abort();
    let _ = running.await;
    let released_frontend = tokio::time::timeout(Duration::from_secs(1), async {
        while state.used.load(Ordering::Acquire) >= 1024 * 1024 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .is_ok();
    let retained = state.used.load(Ordering::Acquire);
    let retained_slots = state.inflight.available_permits();
    let second = tokio::time::timeout(
        Duration::from_millis(100),
        app.oneshot(h.request(
            "tools/call",
            json!({"name":"execute_sql","arguments":{"sql":"SELECT 2"}}),
        )),
    )
    .await;
    let capacity_rejected = match second {
        Ok(Ok(response)) => {
            Harness::response(response).await.1["result"]["structuredContent"]["error"]["code"]
                == "CAPACITY_EXCEEDED"
        }
        _ => false,
    };
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    state.close_all().await;
    state.requests.lock().clear();
    h.close().await;
    assert!(
        entered && released_frontend,
        "failed to isolate worker ownership from frontend buffers"
    );
    assert_eq!(
        retained_slots + 1,
        state.config.max_inflight,
        "cancelled worker released its global execution slot before exiting"
    );
    assert!(
        capacity_rejected,
        "cancelled worker released its MCP execution slot before exiting"
    );
    assert!(retained>=128*1024,"background worker still owns a 64 KiB parameter but retained only {retained} charged bytes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_cancelled_session_initialization_preserves_cleanup_owner() {
    use crate::engine::auth::AccountKey;
    use std::sync::atomic::AtomicUsize;
    struct GatedCreation {
        inner: Arc<StorageQueryEngine>,
        gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
        entered: AtomicBool,
        cleanups: AtomicUsize,
    }
    impl QueryEngine for GatedCreation {
        fn execute(
            &self,
            session: &mut SessionContext,
            query: &str,
        ) -> std::result::Result<QueryResponse, crate::types::QueryError> {
            self.inner.execute_with_session(session, query)
        }
        fn api_key_is_valid(&self, id: &str, owner: &AccountKey) -> bool {
            self.inner.api_key_is_valid(id, owner)
        }
        fn init_session_database(
            &self,
            session: &mut SessionContext,
            database: &str,
        ) -> std::result::Result<(), crate::types::QueryError> {
            self.entered.store(true, Ordering::Release);
            let held = self.gate.0.lock().unwrap();
            drop(self.gate.1.wait_while(held, |open| !*open).unwrap());
            self.inner.init_session_database(session, database)
        }
        fn close_session(
            &self,
            session: &mut SessionContext,
        ) -> std::result::Result<(), crate::types::QueryError> {
            if self.cleanups.fetch_add(1, Ordering::AcqRel) == 0 {
                return Err(
                    crate::error::catalog::ER_UNKNOWN_ERROR.error("Injected cleanup failure")
                );
            }
            self.inner.close_session(session)
        }
    }
    let h = Harness::new(McpConfig::default());
    let identity = sql::authenticate(
        &h.sql,
        h.request("tools/list", json!({})).headers(),
        Harness::peer(),
    )
    .unwrap();
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let engine = Arc::new(GatedCreation {
        inner: h.engine.clone(),
        gate: gate.clone(),
        entered: AtomicBool::new(false),
        cleanups: AtomicUsize::new(0),
    });
    let state = sql::State::new(
        RestConfig::from_startup().unwrap(),
        engine.clone(),
        h.engine.registry().clone(),
    );
    let worker_state = state.clone();
    let running = tokio::spawn(async move {
        sql::create_session(worker_state, identity, Some("test".into())).await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !engine.entered.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    running.abort();
    let _ = running.await;
    assert_eq!(
        state.slots.available_permits() + 1,
        state.config.max_sessions
    );
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            state.sweep().await;
            if state.registry.snapshot().is_empty() && state.used.load(Ordering::Acquire) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(state.slots.available_permits(), state.config.max_sessions);
    assert!(engine.cleanups.load(Ordering::Acquire) >= 2);
    h.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_malformed_origins_cannot_match_a_trusted_origin() {
    let h = Harness::new(McpConfig {
        allowed_origins: vec!["https://app.example.com:443".into()],
        ..McpConfig::default()
    });
    for origin in [
        "https://app.example.com:65536",
        "https://app.example.com:abc",
        "https://app.example.com:",
        "https://app.example.com:+443",
        "https://app.example.com#fragment",
    ] {
        let mut request = h.request("tools/list", json!({}));
        request
            .headers_mut()
            .insert("origin", origin.parse().unwrap());
        assert_eq!(
            h.app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::FORBIDDEN,
            "{origin}"
        );
        assert!(!same_origin(origin, "https://app.example.com"));
    }
    assert!(same_origin(
        "https://[::1]:443",
        "https://[0:0:0:0:0:0:0:1]"
    ));
    h.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_mcp_row_security_and_revocation_apply_to_reused_session() {
    let mut h = Harness::new(McpConfig::default());
    let mut root = SessionContext::default();
    root.mysql_authenticated = true;
    root.ssl_cipher = Some("TLS_AES_256_GCM_SHA384".into());
    for query in [
        "CREATE TABLE audit_secured(id INT PRIMARY KEY, tenant INT)",
        "INSERT INTO audit_secured VALUES (1,1),(2,2)",
        "CREATE USER 'audit_tenant'@'%'",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON test.audit_secured TO 'audit_tenant'@'%'",
        "ALTER TABLE audit_secured ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY tenant ON audit_secured USING(tenant=1)",
    ] {
        h.engine.execute_with_session(&mut root, query).unwrap();
    }
    let QueryResponse::ResultSet(created) = h
        .engine
        .execute_with_session(&mut root, "CREATE API KEY 'tenant' FOR 'audit_tenant'@'%'")
        .unwrap()
    else {
        panic!()
    };
    h.token = created.rows[0][6].to_display_string();
    let session = open(&h).await;
    let selected = h
        .call(
            "execute_sql",
            json!({"session_id":session.id,"sql":"SELECT id FROM audit_secured ORDER BY id"}),
        )
        .await;
    assert_eq!(
        selected["structuredContent"]["results"][0]["rows"],
        json!([["1"]])
    );
    for query in [
        "INSERT INTO audit_secured VALUES (3,2)",
        "UPDATE audit_secured SET tenant=2 WHERE id=1",
        "ALTER TABLE audit_secured DISABLE ROW LEVEL SECURITY",
    ] {
        assert_eq!(
            h.call("execute_sql", json!({"session_id":session.id,"sql":query}))
                .await["isError"],
            true,
            "{query}"
        );
    }
    h.engine
        .execute_with_session(
            &mut root,
            "REVOKE SELECT ON test.audit_secured FROM 'audit_tenant'@'%'",
        )
        .unwrap();
    assert_eq!(
        h.call(
            "execute_sql",
            json!({"session_id":session.id,"sql":"SELECT * FROM audit_secured"})
        )
        .await["isError"],
        true
    );
    h.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_completed_delivery_cannot_cancel_a_subsequent_transaction() {
    let h = Harness::new(McpConfig::default());
    h.sql("CREATE TABLE delivered_audit(id INT PRIMARY KEY)")
        .await;
    let session = open(&h).await;
    assert_eq!(
        h.call(
            "execute_sql",
            json!({"session_id":session.id,"sql":"BEGIN"})
        )
        .await["isError"],
        false
    );
    let mut execution = sql::start(
        h.sql.clone(),
        session.clone(),
        items("INSERT INTO delivered_audit VALUES(1)"),
        false,
        false,
        1000,
        "audit_delivered_000001".into(),
        sql::OutputFormat::Structured {
            max_rows: 100,
            max_bytes: 8192,
            explain_only: false,
        },
        h.sql.memory(1024).unwrap(),
    )
    .await
    .unwrap();
    let chunk = execution.next().await.unwrap().unwrap();
    let sql::Payload::Structured(_, memory) = chunk.payload else {
        panic!()
    };
    let identity = sql::authenticate(
        &h.sql,
        h.request("tools/list", json!({})).headers(),
        Harness::peer(),
    )
    .unwrap();
    let lease = Arc::new(Lease {
        identity,
        arguments: None,
        execution: Mutex::new(Some(execution)),
        created: Mutex::new(None),
        memory: Mutex::new(vec![memory]),
        permit: Mutex::new(None),
        delivery: AtomicU8::new(DELIVERY_PENDING),
        delivery_frames: AtomicUsize::new(0),
        supervisor: Mutex::new(None),
        persistent_created: AtomicBool::new(false),
        connection: None,
    });
    let completed_lease = lease.clone();
    let mut delivery = Delivery::new(
        Box::pin(tokio_stream::once(Ok(Bytes::from_static(b"result")))),
        lease,
        Duration::from_millis(10),
    );
    let retained_frame = delivery.next().await.unwrap().unwrap();
    assert!(delivery.next().await.is_none());
    assert!(
        session.busy.load(Ordering::Acquire),
        "queued network data must retain session ownership after EOF"
    );
    drop(retained_frame);
    assert_eq!(
        completed_lease.delivery.load(Ordering::Acquire),
        DELIVERY_COMPLETE
    );
    assert_eq!(
        h.call(
            "execute_sql",
            json!({"session_id":session.id,"sql":"INSERT INTO delivered_audit VALUES(2)"})
        )
        .await["isError"],
        false
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    drop(delivery);
    drop(completed_lease);
    assert!(!session.closed.load(Ordering::Acquire));
    assert_eq!(
        h.call(
            "execute_sql",
            json!({"session_id":session.id,"sql":"COMMIT"})
        )
        .await["isError"],
        false
    );
    assert_eq!(
        h.sql("SELECT COUNT(*) FROM delivered_audit").await["results"][0]["rows"],
        json!([["2"]])
    );
    h.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_eof_with_queued_network_frame_still_expires() {
    let h = Harness::new(McpConfig::default());
    h.sql("CREATE TABLE queued_frame_audit(id INT PRIMARY KEY)")
        .await;
    let session = open(&h).await;
    for query in ["BEGIN", "INSERT INTO queued_frame_audit VALUES(1)"] {
        assert_eq!(
            h.call("execute_sql", json!({"session_id":session.id,"sql":query}))
                .await["isError"],
            false
        );
    }
    let mut execution = sql::start(
        h.sql.clone(),
        session.clone(),
        items("SELECT 1"),
        false,
        false,
        1000,
        "audit_queued_frame_0001".into(),
        sql::OutputFormat::Structured {
            max_rows: 100,
            max_bytes: 8192,
            explain_only: false,
        },
        h.sql.memory(1024).unwrap(),
    )
    .await
    .unwrap();
    let chunk = execution.next().await.unwrap().unwrap();
    let sql::Payload::Structured(_, memory) = chunk.payload else {
        panic!()
    };
    let identity = sql::authenticate(
        &h.sql,
        h.request("tools/list", json!({})).headers(),
        Harness::peer(),
    )
    .unwrap();
    let lease = Arc::new(Lease {
        identity,
        arguments: None,
        execution: Mutex::new(Some(execution)),
        created: Mutex::new(None),
        memory: Mutex::new(vec![memory]),
        permit: Mutex::new(None),
        delivery: AtomicU8::new(DELIVERY_PENDING),
        delivery_frames: AtomicUsize::new(0),
        supervisor: Mutex::new(None),
        persistent_created: AtomicBool::new(false),
        connection: None,
    });
    let mut delivery = Delivery::new(
        Box::pin(tokio_stream::once(Ok(Bytes::from_static(b"queued data")))),
        lease,
        Duration::from_millis(10),
    );
    let frame = delivery.next().await.unwrap().unwrap();
    assert!(delivery.next().await.is_none());
    drop(delivery);
    let expired = wait_closed(&session).await;
    drop(frame);
    h.close().await;
    assert!(
        expired,
        "EOF cancelled the deadline while the network still owned an unsent frame"
    );
    assert!(h.engine.registry().snapshot().is_empty());
}
