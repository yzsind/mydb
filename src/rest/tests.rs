use super::*;
use crate::{
    engine::{api::QueryEngine, StorageQueryEngine},
    storage::{DurabilityMode, StorageConfig},
};
use runtime::*;
use serde_json::Value;
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
fn identity(engine: &StorageQueryEngine) -> Identity {
    let mut s = SessionContext::default();
    s.mysql_authenticated = true;
    s.ssl_cipher = Some("TLS_AES_256_GCM_SHA384".into());
    let r = engine
        .execute_with_session(&mut s, "CREATE API KEY 'test'")
        .unwrap();
    let crate::engine::api::QueryResponse::ResultSet(r) = r else {
        panic!()
    };
    let token = r.rows[0][6].to_display_string();
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
    s.close().await;
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
    session.close().await;
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
    session.close().await;
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
    session.close().await;
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
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(state.slots.available_permits(), 1);
        assert!(state.sessions.lock().is_empty());
    }
}
