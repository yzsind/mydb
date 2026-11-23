use super::*;
use crate::{
    engine::{api::QueryEngine, StorageQueryEngine},
    storage::{DurabilityMode, StorageConfig},
};
use runtime::*;
use serde_json::Value;

#[tokio::test]
async fn availability_readiness_rejects_fail_stopped_database() {
    let (_dir, engine) = engine();
    let state = runtime::State::new(
        RestConfig::from_startup().unwrap(),
        engine.clone(),
        engine.registry().clone(),
    );
    let probe = |path: &'static str| {
        endpoint(
            State(state.clone()),
            Extension(Peer {
                host: "127.0.0.1".into(),
                cipher: None,
                version: None,
                subject: None,
                issuer: None,
            }),
            Request::builder().uri(path).body(Body::empty()).unwrap(),
        )
    };
    assert_eq!(
        probe("/health/ready").await.unwrap().status(),
        StatusCode::OK
    );
    engine
        .db_handle()
        .quarantine_mvcc("injected consistency failure");
    let response = probe("/health/ready").await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"ready":false})
    );
    assert_eq!(
        probe("/health/live").await.unwrap().status(),
        StatusCode::OK
    );
    assert!(engine.execute("SELECT 1").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn availability_http_listener_failure_keeps_mysql_running() {
    use mysql::prelude::Queryable;
    let (_dir, engine) = engine();
    let mysql_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mysql_addr = mysql_listener.local_addr().unwrap();
    let mysql_engine = engine.clone();
    let mysql_task = tokio::spawn(async move {
        crate::mysql::server::MysqlServer::with_tls(
            mysql_addr,
            mysql_engine.clone(),
            None,
            mysql_engine.registry().clone(),
        )
        .run_with_listener(mysql_listener)
        .await
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let failure = Arc::new(tokio::sync::Notify::new());
    let mut config = RestConfig::from_startup().unwrap();
    config.tls = false;
    let mut server = RestServer::new(config, engine.clone(), engine.registry().clone(), None);
    server.accept_failure = Some((addr, failure.clone()));
    let late_key = test_key(&engine, "CREATE API KEY 'late'");
    let state = server.state.clone();
    let session = create_session(state.clone(), identity(&engine), Some("test".into()))
        .await
        .unwrap();
    state
        .sessions
        .lock()
        .insert(session.id.clone(), session.clone());
    for sql in [
        "CREATE TABLE pending_http (id INT PRIMARY KEY)",
        "BEGIN",
        "INSERT INTO pending_http VALUES (1)",
    ] {
        assert_eq!(
            query(state.clone(), session.clone(), json!({"sql":sql}), false)
                .await
                .0,
            StatusCode::OK
        );
    }
    let http_task = tokio::spawn(async move { server.run_with_listener(listener).await });
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let health = client
        .get(format!("http://{addr}/health/live"))
        .send()
        .await
        .unwrap();
    assert!(health.status().is_success());
    health.bytes().await.unwrap();
    failure.notify_one();
    assert!(tokio::time::timeout(Duration::from_secs(5), http_task)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert!(
        !crate::mysql::shutdown::is_shutting_down(),
        "HTTP failure must not request global shutdown"
    );
    assert!(!mysql_task.is_finished());
    let late = client
        .post(format!("http://{addr}/api/v1/sessions"))
        .bearer_auth(&late_key)
        .json(&json!({}))
        .send()
        .await;
    assert!(
        match &late {
            Err(_) => true,
            Ok(response) => response.status() == StatusCode::SERVICE_UNAVAILABLE,
        },
        "a retained HTTP connection admitted a session after listener shutdown"
    );
    assert!(session.closed.load(std::sync::atomic::Ordering::Acquire));
    let crate::engine::api::QueryResponse::ResultSet(rows) =
        engine.execute("SELECT COUNT(*) FROM pending_http").unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(
        rows.rows[0][0].to_display_string(),
        "0",
        "failed HTTP service must roll back its sessions"
    );
    tokio::task::spawn_blocking(move || {
        let opts = mysql::OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(mysql_addr.port())
            .user(Some("root"))
            .pass(Some("root"))
            .prefer_socket(false);
        let mut connection = mysql::Conn::new(opts).unwrap();
        connection
            .query_drop("CREATE TABLE test.http_failure (id INT PRIMARY KEY)")
            .unwrap();
        connection
            .query_drop("INSERT INTO test.http_failure VALUES (7)")
            .unwrap();
        assert_eq!(
            connection
                .query_first::<u64, _>("SELECT id FROM test.http_failure")
                .unwrap(),
            Some(7)
        );
    })
    .await
    .unwrap();
    mysql_task.abort();
    let _ = mysql_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn availability_dual_listener_failure_preserves_other_listener_and_transaction() {
    for fail_primary in [true, false] {
        let (_dir, engine) = engine();
        let key = test_key(&engine, "CREATE API KEY 'survivor'");
        let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let secondary = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary.local_addr().unwrap();
        let secondary_addr = secondary.local_addr().unwrap();
        let failed_addr = if fail_primary {
            primary_addr
        } else {
            secondary_addr
        };
        let healthy_url = if fail_primary {
            format!("http://{secondary_addr}")
        } else {
            format!("https://{primary_addr}")
        };
        let failure = Arc::new(tokio::sync::Notify::new());
        let mut config = RestConfig::from_startup().unwrap();
        config.enabled = true;
        config.tls = true;
        config.loopback_http_addr = Some(secondary_addr);
        let tls = crate::mysql::server::load_or_generate_tls_acceptor(None).unwrap();
        let mut server = RestServer::new(config, engine.clone(), engine.registry().clone(), tls);
        server.accept_failure = Some((failed_addr, failure.clone()));
        let task =
            tokio::spawn(async move { server.run_with_listeners(primary, Some(secondary)).await });
        let client = reqwest::Client::builder()
            .no_proxy()
            .danger_accept_invalid_certs(true)
            .pool_max_idle_per_host(0)
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let response = client
            .post(format!("{healthy_url}/api/v1/sessions"))
            .bearer_auth(&key)
            .json(&json!({"database":"test"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let session = response.json::<Value>().await.unwrap();
        let id = session["id"].as_str().unwrap();
        let sql_url = format!("{healthy_url}/api/v1/sessions/{id}/sql");
        for sql in [
            "CREATE TABLE survivor (id INT PRIMARY KEY)",
            "BEGIN",
            "INSERT INTO survivor VALUES (9)",
        ] {
            let response = client
                .post(&sql_url)
                .bearer_auth(&key)
                .json(&json!({"sql":sql}))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{sql}");
        }
        failure.notify_one();
        tokio::time::timeout(Duration::from_secs(5), async {
            while tokio::net::TcpStream::connect(failed_addr).await.is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed listener socket was not closed");
        assert!(!task.is_finished());
        assert!(!crate::mysql::shutdown::is_shutting_down());
        let response = client
            .post(&sql_url)
            .bearer_auth(&key)
            .json(&json!({"sql":"COMMIT"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let crate::engine::api::QueryResponse::ResultSet(rows) =
            engine.execute("SELECT id FROM survivor").unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.rows[0][0], crate::types::Value::Int(9));
        let response = client
            .delete(format!("{healthy_url}/api/v1/sessions/{id}"))
            .bearer_auth(&key)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn availability_http_shutdown_rejects_racing_session_admission() {
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let state = runtime::State::new(
        RestConfig::from_startup().unwrap(),
        engine.clone(),
        engine.registry().clone(),
    );
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let holder = {
        let state = state.clone();
        tokio::task::spawn_blocking(move || {
            let _sessions = state.sessions.lock();
            let _ = locked_tx.send(());
            let _ = release_rx.blocking_recv();
        })
    };
    tokio::time::timeout(Duration::from_secs(5), locked_rx)
        .await
        .expect("session admission lock was not acquired")
        .unwrap();
    let creating = tokio::spawn(create_session(state.clone(), identity.clone(), None));
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.registry.snapshot().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("session admission did not reach registration");
    let closing = {
        let state = state.clone();
        tokio::spawn(async move {
            state.begin_close();
            state.close_all().await;
        })
    };
    state.closing_notified().await;
    release_tx.send(()).unwrap();
    holder.await.unwrap();
    assert!(creating.await.unwrap().is_err());
    closing.await.unwrap();
    assert!(create_session(state.clone(), identity, None).await.is_err());
    tokio::time::timeout(Duration::from_secs(5), async {
        while !state.registry.snapshot().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late admission leaked its authenticated connection");
    assert!(state.sessions.lock().is_empty());
    assert_eq!(state.slots.available_permits(), state.config.max_sessions);
}

fn engine() -> (tempfile::TempDir, Arc<StorageQueryEngine>) {
    std::fs::create_dir_all("/tmp/mydb/rest").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb/rest").unwrap();
    let mut config = StorageConfig::from_env();
    config.durability = DurabilityMode::Memory;
    config.buffer_pool_pages = 512;
    config.buffer_pool_io_threads = 1;
    let engine = Arc::new(
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap(),
    );
    (dir, engine)
}
fn test_key(engine: &StorageQueryEngine, sql: &str) -> String {
    let mut s = SessionContext::default();
    s.mysql_authenticated = true;
    s.ssl_cipher = Some("TLS_AES_256_GCM_SHA384".into());
    let r = engine.execute_with_session(&mut s, sql).unwrap();
    let crate::engine::api::QueryResponse::ResultSet(r) = r else {
        panic!()
    };
    r.rows[0][6].to_display_string()
}
fn identity(engine: &StorageQueryEngine) -> Identity {
    let token = test_key(engine, "CREATE API KEY 'test'");
    let peer = Peer {
        host: "127.0.0.1".into(),
        cipher: None,
        version: None,
        subject: None,
        issuer: None,
    };
    let (key_id, outcome) = engine
        .authenticate_api_key(&token, &peer.auth_request())
        .unwrap();
    Identity {
        key_id,
        outcome,
        peer,
    }
}
async fn query(
    state: Arc<runtime::State<StorageQueryEngine>>,
    session: Arc<Session<StorageQueryEngine>>,
    value: Value,
    stream: bool,
) -> (StatusCode, Vec<u8>) {
    let r: codec::SqlRequest = serde_json::from_value(value).unwrap();
    let (_, items, atomic, _) = r.items().unwrap();
    let response = execute(
        state.clone(),
        session,
        items,
        atomic,
        false,
        5000,
        format!("rq_{}", crate::engine::access_keys::random_id().unwrap()),
        stream,
        state.memory(1024).unwrap(),
        None,
    )
    .await
    .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 32 * 1024 * 1024)
        .await
        .unwrap();
    (status, bytes.to_vec())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_protocol_session_transactions_parameters_and_ndjson() {
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let state = runtime::State::new(
        RestConfig::from_startup().unwrap(),
        engine.clone(),
        engine.registry().clone(),
    );
    let s = create_session(state.clone(), identity, Some("test".into()))
        .await
        .unwrap();
    let(status,bytes)=query(state.clone(),s.clone(),json!({"sql":"SELECT ? AS u, ? AS d, ? AS j","params":[{"type":"uint64","value":"18446744073709551615"},{"type":"decimal","value":"12345678901234567890.123456789"},{"type":"json","value":"null"}]}),false).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        result["results"][0]["rows"][0],
        json!([
            "18446744073709551615",
            "12345678901234567890.123456789",
            "null"
        ])
    );
    for sql in [
        "CREATE TABLE rest_t (id INT PRIMARY KEY)",
        "BEGIN",
        "INSERT INTO rest_t VALUES (1)",
        "ROLLBACK",
    ] {
        let (status, bytes) = query(state.clone(), s.clone(), json!({"sql":sql}), false).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{sql}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    let(status,bytes)=query(state.clone(),s.clone(),json!({"atomic":true,"statements":[{"sql":"INSERT INTO rest_t VALUES (2)"},{"sql":"INSERT INTO rest_t VALUES (2)"}]}),false).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let (_, bytes) = query(
        state.clone(),
        s.clone(),
        json!({"sql":"SELECT COUNT(*) FROM rest_t"}),
        true,
    )
    .await;
    let events: Vec<Value> = String::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(events.last().unwrap()["type"], "complete");
    assert_eq!(
        events.iter().find(|v| v["type"] == "rows").unwrap()["rows"],
        json!([["0"]])
    );
    assert!(!s.closed.load(std::sync::atomic::Ordering::Acquire));
    s.close().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_protocol_busy_cancellation_and_revocation() {
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let state = runtime::State::new(
        RestConfig::from_startup().unwrap(),
        engine.clone(),
        engine.registry().clone(),
    );
    let s = create_session(state.clone(), identity, None).await.unwrap();
    state.sessions.lock().insert(s.id.clone(), s.clone());
    let request: codec::SqlRequest =
        serde_json::from_value(json!({"sql":"SELECT SLEEP(10)"})).unwrap();
    let (_, items, _, _) = request.items().unwrap();
    let id = "cancel_request_000001".to_string();
    let worker_state = state.clone();
    let worker_session = s.clone();
    let worker_id = id.clone();
    let task = tokio::spawn(async move {
        execute(
            worker_state.clone(),
            worker_session,
            items,
            false,
            false,
            20000,
            worker_id,
            false,
            worker_state.memory(1024).unwrap(),
            None,
        )
        .await
    });
    for _ in 0..200 {
        if state.request(&id, &s.key_id).is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let request: codec::SqlRequest = serde_json::from_value(json!({"sql":"SELECT 1"})).unwrap();
    let (_, items, _, _) = request.items().unwrap();
    let error = execute(
        state.clone(),
        s.clone(),
        items,
        false,
        false,
        5000,
        "concurrent_request_001".into(),
        false,
        state.memory(1024).unwrap(),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error.status, StatusCode::CONFLICT);
    state.request(&id, &s.key_id).unwrap().cancel();
    let response = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(result["error"].is_object());
    let mut root = SessionContext::default();
    root.mysql_authenticated = true;
    engine
        .execute_with_session(&mut root, "DROP API KEY 'test'")
        .unwrap();
    state.sweep().await;
    assert!(s.closed.load(std::sync::atomic::Ordering::Acquire));
}
#[test]
fn rest_protocol_strict_json_and_lossless_parameter_codec() {
    let s = SessionContext::default();
    assert!(strict(br#"{"sql":"SELECT 1","sql":"SELECT 2"}"#, 1024).is_err());
    for text in [
        "9007199254740992",
        r#"{"type":"uint64","value":"18446744073709551616"}"#,
        r#"{"type":"binary","value":"not base64"}"#,
    ] {
        let raw: Box<serde_json::value::RawValue> = serde_json::from_str(text).unwrap();
        assert!(codec::param(&raw, &s).is_err(), "{text}");
    }
    let raw: Box<serde_json::value::RawValue> =
        serde_json::from_str("12345678901234567890.1234567890123456789").unwrap();
    let v = codec::param(&raw, &s).unwrap();
    assert_eq!(v.to_display_string(), raw.get());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_protocol_json_ceiling_includes_metadata_and_envelope() {
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let mut config = RestConfig::from_startup().unwrap();
    config.max_json_response_bytes = 1024;
    let state = runtime::State::new(config, engine.clone(), engine.registry().clone());
    let session = create_session(state.clone(), identity, None).await.unwrap();
    let (status, bytes) = query(
        state,
        session.clone(),
        json!({"sql":"SELECT REPEAT('x',700)"}),
        false,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(bytes.len() <= 1024, "{}", bytes.len());
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["error"]["code"], "RESULT_TOO_LARGE");
    assert_eq!(result["execution_outcome"], "succeeded");
    assert_eq!(result["results_omitted"], true);
    session.close().await.unwrap();
}

#[test]
fn rest_protocol_temporal_precision_survives_conservative_expression_metadata() {
    use crate::types::{Column, ColumnType, Value};
    let mut column = Column {
        name: "dt".into(),
        column_type: ColumnType::DateTime,
        source: Default::default(),
    };
    column.source.decimals = Some(0);
    let raw: Box<serde_json::value::RawValue> =
        serde_json::from_str(r#"{"type":"datetime","value":"2026-11-04 01:02:03.123456"}"#)
            .unwrap();
    let value = codec::param(&raw, &SessionContext::default()).unwrap();
    assert_eq!(
        codec::row(vec![value], &[column.clone()]).unwrap(),
        json!(["2026-11-04 01:02:03.123456"])
    );
    column.column_type = ColumnType::Time;
    assert_eq!(
        codec::row(vec![Value::Time(-1)], &[column.clone()]).unwrap(),
        json!(["-00:00:00.000001"])
    );
    column.source.decimals = Some(3);
    assert_eq!(
        codec::row(vec![Value::Time(123000)], &[column]).unwrap(),
        json!(["00:00:00.123"])
    );
}

#[test]
fn rest_protocol_output_memory_follows_network_bytes_ownership() {
    let (_dir, engine) = engine();
    let state = runtime::State::new(
        RestConfig::from_startup().unwrap(),
        engine.clone(),
        engine.registry().clone(),
    );
    let before = state.used.load(std::sync::atomic::Ordering::Acquire);
    let memory = state.memory(4096).unwrap();
    let bytes = runtime::governed_bytes(vec![0; 4096], memory);
    let network_copy = bytes.clone();
    drop(bytes);
    assert_eq!(
        state.used.load(std::sync::atomic::Ordering::Acquire),
        before + 4096
    );
    drop(network_copy);
    assert_eq!(
        state.used.load(std::sync::atomic::Ordering::Acquire),
        before
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_protocol_transport_deadline_retains_worker_until_cleanup() {
    use crate::engine::api::{QueryResponse, SessionContext};
    use crate::engine::auth::AccountKey;
    use crate::types::QueryError;
    struct GatedEngine {
        inner: Arc<StorageQueryEngine>,
        gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    }
    impl QueryEngine for GatedEngine {
        fn execute(
            &self,
            context: &mut SessionContext,
            sql: &str,
        ) -> std::result::Result<QueryResponse, QueryError> {
            let (lock, notify) = &*self.gate;
            let guard = lock.lock().unwrap();
            drop(notify.wait_while(guard, |open| !*open).unwrap());
            self.inner.execute_with_session(context, sql)
        }
        fn api_key_is_valid(&self, id: &str, owner: &AccountKey) -> bool {
            self.inner.api_key_is_valid(id, owner)
        }
        fn close_session(
            &self,
            context: &mut SessionContext,
        ) -> std::result::Result<(), QueryError> {
            self.inner.close_session(context)
        }
    }
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let proxy = Arc::new(GatedEngine {
        inner: engine.clone(),
        gate: gate.clone(),
    });
    let state = runtime::State::new(
        RestConfig::from_startup().unwrap(),
        proxy,
        engine.registry().clone(),
    );
    let session = create_session(state.clone(), identity, None).await.unwrap();
    let request: codec::SqlRequest = serde_json::from_value(json!({"sql":"SELECT 1"})).unwrap();
    let (_, items, _, _) = request.items().unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        execute(
            state.clone(),
            session.clone(),
            items,
            false,
            false,
            500,
            "transport_deadline_0001".into(),
            false,
            state.memory(1024).unwrap(),
            None,
        ),
    )
    .await;
    let was_owned = session.context.lock().is_none();
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    let failure = response
        .expect("transport deadline must not await uncooperative SQL")
        .unwrap_err();
    assert_eq!(failure.status, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(failure.execution_outcome, "unknown");
    assert!(
        was_owned,
        "engine resources remain owned until the worker exits"
    );
    session.close().await.unwrap();
    assert!(state.registry.snapshot().is_empty());
}

#[tokio::test]
async fn rest_protocol_required_tls_never_falls_back_to_plaintext() {
    let (_dir, engine) = engine();
    let mut config = RestConfig::from_startup().unwrap();
    config.tls = true;
    let server = RestServer::new(config, engine.clone(), engine.registry().clone(), None);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    assert_eq!(
        server.run_with_listener(listener).await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_protocol_dual_listeners_share_sql_and_enforce_transport() {
    let (_dir, engine) = engine();
    let key = test_key(&engine, "CREATE API KEY 'loopback'");
    let mut root = SessionContext::default();
    for sql in [
        "CREATE USER 'strict_rest'@'127.0.0.1' REQUIRE SSL",
        "GRANT SELECT ON test.* TO 'strict_rest'@'127.0.0.1'",
    ] {
        engine.execute_with_session(&mut root, sql).unwrap();
    }
    let tls_key = test_key(
        &engine,
        "CREATE API KEY 'strict' FOR 'strict_rest'@'127.0.0.1'",
    );
    let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let secondary = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let primary_addr = primary.local_addr().unwrap();
    let https = format!("https://{primary_addr}");
    let http = format!("http://{}", secondary.local_addr().unwrap());
    let mut config = RestConfig::from_startup().unwrap();
    config.enabled = true;
    config.loopback_http_addr = Some(secondary.local_addr().unwrap());
    config.max_sessions = 1;
    let tls = crate::mysql::server::load_or_generate_tls_acceptor(None).unwrap();
    let server = RestServer::new(config, engine.clone(), engine.registry().clone(), tls);
    let task =
        tokio::spawn(async move { server.run_with_listeners(primary, Some(secondary)).await });
    let client = reqwest::Client::builder()
        .no_proxy()
        .danger_accept_invalid_certs(true)
        .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let session = client
        .post(format!("{http}/api/v1/sessions"))
        .bearer_auth(&key)
        .json(&json!({"database":"test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(session.status(), StatusCode::CREATED);
    let session = session.json::<Value>().await.unwrap();
    let id = session["id"].as_str().unwrap();
    for sql in [
        "CREATE TABLE dual_listener (id INT PRIMARY KEY)",
        "INSERT INTO dual_listener VALUES (42)",
    ] {
        let response = client
            .post(format!("{http}/api/v1/sessions/{id}/sql"))
            .bearer_auth(&key)
            .json(&json!({"sql":sql}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{sql}");
    }
    let foreign_session = client
        .get(format!("{https}/api/v1/sessions/{id}"))
        .bearer_auth(&key)
        .send()
        .await
        .unwrap();
    assert_eq!(foreign_session.status(), StatusCode::NOT_FOUND);
    let session_limit = client
        .post(format!("{https}/api/v1/sessions"))
        .bearer_auth(&tls_key)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(session_limit.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        client
            .delete(format!("{http}/api/v1/sessions/{id}"))
            .bearer_auth(&key)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );

    let query = json!({"sql":"SELECT id FROM test.dual_listener"});
    let denied = client
        .post(format!("{http}/api/v1/sql"))
        .bearer_auth(&tls_key)
        .json(&query)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let result = client
        .post(format!("{https}/api/v1/sql"))
        .bearer_auth(&tls_key)
        .json(&query)
        .send()
        .await
        .unwrap();
    assert_eq!(result.status(), StatusCode::OK);
    assert_eq!(
        result.json::<Value>().await.unwrap()["results"][0]["rows"],
        json!([["42"]])
    );
    assert!(client
        .get(format!("http://{primary_addr}/health/live"))
        .send()
        .await
        .is_err());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn rest_protocol_dual_listeners_share_connection_limit() {
    let (_dir, engine) = engine();
    let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let secondary = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let https = format!("https://{}/health/live", primary.local_addr().unwrap());
    let http = format!("http://{}/health/live", secondary.local_addr().unwrap());
    let mut config = RestConfig::from_startup().unwrap();
    config.loopback_http_addr = Some(secondary.local_addr().unwrap());
    config.max_connections = 1;
    let tls = crate::mysql::server::load_or_generate_tls_acceptor(None).unwrap();
    let server = RestServer::new(config, engine.clone(), engine.registry().clone(), tls);
    let task =
        tokio::spawn(async move { server.run_with_listeners(primary, Some(secondary)).await });
    let client = || {
        reqwest::Client::builder()
            .no_proxy()
            .danger_accept_invalid_certs(true)
            .http1_only()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap()
    };
    let local_client = client();
    let remote_client = client();
    let live = local_client.get(&http).send().await.unwrap();
    assert_eq!(live.status(), StatusCode::OK);
    live.bytes().await.unwrap();
    assert!(remote_client.get(&https).send().await.is_err());
    drop(local_client);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(live) = remote_client.get(&https).send().await {
                assert_eq!(live.status(), StatusCode::OK);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Closing the local connection should release the shared permit");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_protocol_resource_error_uses_reserved_reply_and_retains_execution_outcome() {
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let mut config = RestConfig::from_startup().unwrap();
    config.memory_limit = 65536;
    let state = runtime::State::new(config, engine.clone(), engine.registry().clone());
    let session = create_session(state.clone(), identity, None).await.unwrap();
    let free = 65536 - state.used.load(std::sync::atomic::Ordering::Acquire);
    let pressure = state.memory(free - 6200).unwrap();
    let (status, bytes) = query(
        state.clone(),
        session.clone(),
        json!({"sql":"SELECT REPEAT('x',400)"}),
        false,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["execution_outcome"], "succeeded");
    assert_eq!(result["results_omitted"], true);
    assert!(state.used.load(std::sync::atomic::Ordering::Acquire) <= 65536);
    drop(pressure);
    session.close().await.unwrap();
}
#[test]
fn rest_protocol_cancellation_targets_handle_instead_of_reused_connection_id() {
    let registry = SessionRegistry::new();
    let old = registry.register(7, "old".into(), "127.0.0.1".into(), "test".into());
    registry.unregister(7);
    let current = registry.register(7, "new".into(), "127.0.0.1".into(), "test".into());
    old.cancel_query();
    old.kill_connection();
    assert_eq!(current.query_cancel_generation(), 0);
    assert!(!current
        .kill_flag()
        .load(std::sync::atomic::Ordering::Acquire));
    registry.unregister(7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_protocol_failed_cleanup_retains_context_for_retry() {
    use crate::engine::{
        api::{QueryResponse, SessionContext},
        auth::AccountKey,
    };
    use crate::types::QueryError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct RetryEngine {
        inner: Arc<StorageQueryEngine>,
        attempts: AtomicUsize,
    }
    impl QueryEngine for RetryEngine {
        fn execute(
            &self,
            context: &mut SessionContext,
            sql: &str,
        ) -> std::result::Result<QueryResponse, QueryError> {
            self.inner.execute_with_session(context, sql)
        }
        fn api_key_is_valid(&self, id: &str, owner: &AccountKey) -> bool {
            self.inner.api_key_is_valid(id, owner)
        }
        fn close_session(
            &self,
            context: &mut SessionContext,
        ) -> std::result::Result<(), QueryError> {
            if self.attempts.fetch_add(1, Ordering::AcqRel) == 0 {
                return Err(
                    crate::error::catalog::ER_UNKNOWN_ERROR.error("Injected cleanup failure")
                );
            }
            self.inner.close_session(context)
        }
    }
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let proxy = Arc::new(RetryEngine {
        inner: engine.clone(),
        attempts: AtomicUsize::new(0),
    });
    let state = runtime::State::new(
        RestConfig::from_startup().unwrap(),
        proxy,
        engine.registry().clone(),
    );
    let session = create_session(state.clone(), identity, None).await.unwrap();
    state
        .sessions
        .lock()
        .insert(session.id.clone(), session.clone());
    session.begin_close();
    state.sweep().await;
    assert_eq!(state.sessions.lock().len(), 1);
    assert!(session.context.lock().is_some());
    state.sweep().await;
    assert!(state.sessions.lock().is_empty());
    assert!(state.registry.snapshot().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_protocol_temporary_session_releases_admission_before_reply() {
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let mut config = RestConfig::from_startup().unwrap();
    config.max_sessions = 1;
    let state = runtime::State::new(config, engine.clone(), engine.registry().clone());
    for _ in 0..3 {
        let session = create_session(state.clone(), identity.clone(), None)
            .await
            .unwrap();
        state
            .sessions
            .lock()
            .insert(session.id.clone(), session.clone());
        let request: codec::SqlRequest = serde_json::from_value(json!({"sql":"SELECT 1"})).unwrap();
        let (_, items, _, _) = request.items().unwrap();
        let response = execute(
            state.clone(),
            session,
            items,
            false,
            true,
            5000,
            format!("rq_{}", crate::engine::access_keys::random_id().unwrap()),
            false,
            state.memory(1024).unwrap(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(state.slots.available_permits(), 1);
        assert!(state.sessions.lock().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_audit_rest_bound_dml_and_atomic_rollback() {
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let state = runtime::State::new(
        RestConfig::from_startup().unwrap(),
        engine.clone(),
        engine.registry().clone(),
    );
    let s = create_session(state.clone(), identity, Some("test".into()))
        .await
        .unwrap();
    let requests = [
        json!({"sql":"CREATE TABLE audit_params(id INT PRIMARY KEY,v VARCHAR(80))"}),
        json!({"sql":"INSERT INTO audit_params VALUES (?,?)","params":[1,"first"]}),
        json!({"sql":"INSERT INTO audit_params SELECT ?,?","params":[2,"second"]}),
        json!({"sql":"REPLACE INTO audit_params VALUES (?,?)","params":[1,"replaced"]}),
        json!({"sql":"UPDATE audit_params SET v=? WHERE id=?","params":["bound ' ? \\ text",2]}),
        json!({"sql":"UPDATE audit_params SET v=? WHERE id=?","params":["no match",999]}),
        json!({"sql":"DELETE FROM audit_params WHERE id=?","params":[1]}),
        json!({"sql":"SET @audit_value=?","params":[123]}),
        json!({"atomic":true,"statements":[{"sql":"INSERT INTO audit_params VALUES (?,?)","params":[3,"third"]},{"sql":"UPDATE audit_params SET v=? WHERE id=?","params":["updated",3]}]}),
    ];
    for request in requests {
        let (status, bytes) = query(state.clone(), s.clone(), request.clone(), false).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{request}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    let (_, bytes) = query(
        state.clone(),
        s.clone(),
        json!({"sql":"SELECT id,v,@audit_value FROM audit_params ORDER BY id"}),
        false,
    )
    .await;
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        result["results"][0]["rows"],
        json!([["2", "bound ' ? \\ text", "123"], ["3", "updated", "123"]])
    );
    let (status, bytes) = query(state.clone(), s.clone(), json!({"atomic":true,"statements":[{"sql":"INSERT INTO audit_params VALUES (?,?)","params":[4,"fourth"]},{"sql":"INSERT INTO audit_params VALUES (?,?)","params":[3,"duplicate"]}]}), false).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["error"]["rollback_confirmed"], true);
    let (_, bytes) = query(
        state.clone(),
        s.clone(),
        json!({"sql":"SELECT COUNT(*) FROM audit_params"}),
        false,
    )
    .await;
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["results"][0]["rows"], json!([["2"]]));
    s.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_audit_rest_returning_overflow_confirms_commit_and_batch_rollback() {
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let mut config = RestConfig::from_startup().unwrap();
    config.max_json_response_bytes = 4096;
    let state = runtime::State::new(config, engine.clone(), engine.registry().clone());
    let s = create_session(state.clone(), identity, Some("test".into()))
        .await
        .unwrap();
    query(
        state.clone(),
        s.clone(),
        json!({"sql":"CREATE TABLE audit_overflow(id INT PRIMARY KEY,v TEXT)"}),
        false,
    )
    .await;
    for atomic in [false, true] {
        let id = if atomic { 2 } else { 1 };
        let (status,bytes) = query(state.clone(),s.clone(),json!({"atomic":atomic,"sql":format!("INSERT INTO audit_overflow VALUES ({id},REPEAT('x',5000)) RETURNING *")}),false).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["execution_outcome"], "succeeded", "{result}");
        assert_eq!(result["results_omitted"], true);
        assert_eq!(result["error"]["code"], "RESULT_TOO_LARGE");
    }
    let (status, bytes) = query(state.clone(),s.clone(),json!({"atomic":true,"statements":[{"sql":"INSERT INTO audit_overflow VALUES (3,REPEAT('x',5000)) RETURNING *"},{"sql":"INSERT INTO audit_overflow VALUES (1,'duplicate')"}]}),false).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["error"]["mysql_errno"], 1062);
    assert_eq!(result["error"]["rollback_confirmed"], true);
    let (_, bytes) = query(
        state.clone(),
        s.clone(),
        json!({"sql":"SELECT COUNT(*) FROM audit_overflow"}),
        false,
    )
    .await;
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["results"][0]["rows"], json!([["2"]]));
    s.close().await.unwrap();
}

#[test]
fn release_audit_rest_accept_negotiation() {
    use axum::http::HeaderMap;
    for (accept, expected) in [
        ("APPLICATION/JSON", Some(false)),
        ("application/json; q=0.8", Some(false)),
        ("application/json, application/x-ndjson", Some(false)),
        ("*/*;q=0.8", Some(false)),
        ("application/*", Some(false)),
        ("application/json;q=0, */*;q=1", Some(true)),
        ("application/json;q=0.5, */*", Some(true)),
        (
            "application/json;q=0.2, application/x-ndjson;q=0.8",
            Some(true),
        ),
        ("application/json;q=0, application/x-ndjson;q=0", None),
        ("text/html", None),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert("accept", accept.parse().unwrap());
        assert_eq!(negotiate_format(&headers).ok(), expected, "{accept}");
    }
    let mut headers = HeaderMap::new();
    headers.append("accept", "text/html".parse().unwrap());
    headers.append("accept", "application/x-ndjson".parse().unwrap());
    assert!(negotiate_format(&headers).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_audit_rest_chunked_body_memory_and_transaction_expiry() {
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let mut config = RestConfig::from_startup().unwrap();
    config.memory_limit = 1024 * 1024;
    config.session_idle = Duration::from_secs(600);
    config.transaction_idle = Duration::from_secs(30);
    let state = runtime::State::new(config, engine.clone(), engine.registry().clone());
    let chunks: Vec<_> = (0..64)
        .map(|_| Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"x")))
        .collect();
    let request = Request::builder()
        .header("content-type", "application/json")
        .body(Body::from_stream(tokio_stream::iter(chunks)))
        .unwrap();
    let (bytes, memory) = body(&state, request).await.unwrap();
    assert_eq!(bytes.len(), 64);
    assert!(state.used.load(std::sync::atomic::Ordering::Acquire) < 8192);
    drop(memory);
    assert_eq!(state.used.load(std::sync::atomic::Ordering::Acquire), 0);
    let s = create_session(state.clone(), identity, None).await.unwrap();
    s.transaction
        .store(true, std::sync::atomic::Ordering::Release);
    s.transaction_touched
        .store(1000, std::sync::atomic::Ordering::Release);
    s.touched.store(10000, std::sync::atomic::Ordering::Release);
    assert_eq!(session_info(&s, &state.config)["expires_at_ms"], 31000);
    s.transaction
        .store(false, std::sync::atomic::Ordering::Release);
    assert_eq!(session_info(&s, &state.config)["expires_at_ms"], 610000);
    s.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_audit_no_implicit_database_and_sequential_batch_sql_mode() {
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let state = runtime::State::new(
        RestConfig::from_startup().unwrap(),
        engine.clone(),
        engine.registry().clone(),
    );
    let session = create_session(state.clone(), identity, None).await.unwrap();
    let (_, bytes) = query(
        state.clone(),
        session.clone(),
        json!({"sql":"SELECT DATABASE()"}),
        false,
    )
    .await;
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["results"][0]["rows"], json!([[null]]));
    let (_, bytes) = query(state.clone(), session.clone(), json!({"statements":[{"sql":"SET sql_mode='ANSI_QUOTES'"},{"sql":"SELECT 7 AS \"n\" ORDER BY \"n\""}]}), false).await;
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(result["error"].is_null(), "{result}");
    assert_eq!(result["results"][1]["rows"], json!([["7"]]));
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit1123_persistent_cleanup_failure_is_bounded_and_retriable() {
    use crate::engine::api::{QueryResponse, SessionContext};
    use crate::engine::auth::AccountKey;
    use std::sync::atomic::{AtomicBool, Ordering};
    struct FailingEngine {
        inner: Arc<StorageQueryEngine>,
        fail: AtomicBool,
    }
    impl QueryEngine for FailingEngine {
        fn execute(
            &self,
            context: &mut SessionContext,
            sql: &str,
        ) -> std::result::Result<QueryResponse, crate::types::QueryError> {
            self.inner.execute_with_session(context, sql)
        }
        fn api_key_is_valid(&self, id: &str, owner: &AccountKey) -> bool {
            self.inner.api_key_is_valid(id, owner)
        }
        fn close_session(
            &self,
            context: &mut SessionContext,
        ) -> std::result::Result<(), crate::types::QueryError> {
            if self.fail.load(Ordering::Acquire) {
                return Err(
                    crate::error::catalog::ER_UNKNOWN_ERROR.error("persistent cleanup fault")
                );
            }
            self.inner.close_session(context)
        }
    }
    let (_dir, engine) = engine();
    let identity = identity(&engine);
    let proxy = Arc::new(FailingEngine {
        inner: engine.clone(),
        fail: AtomicBool::new(true),
    });
    let state = runtime::State::new(
        RestConfig::from_startup().unwrap(),
        proxy.clone(),
        engine.registry().clone(),
    );
    let session = create_session(state.clone(), identity, None).await.unwrap();
    state
        .sessions
        .lock()
        .insert(session.id.clone(), session.clone());
    let close = tokio::time::timeout(Duration::from_secs(2), session.close()).await;
    let err = close
        .expect("close must return on persistent storage failure")
        .unwrap_err();
    assert_eq!(err.code, "SESSION_CLEANUP_FAILED");
    assert_eq!(err.execution_outcome, "unknown");
    assert!(session.context.lock().is_some());
    assert_eq!(state.sessions.lock().len(), 1);
    proxy.fail.store(false, Ordering::Release);
    state.sweep().await;
    assert!(state.sessions.lock().is_empty());
    assert!(state.registry.snapshot().is_empty());
}
