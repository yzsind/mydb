use super::*;
use crate::{
    engine::{
        api::{QueryResponse, SessionContext},
        StorageQueryEngine,
    },
    rest::{RestConfig, RestServer},
    storage::{DurabilityMode, StorageConfig},
};
use axum::{body::to_bytes, Extension};
use std::sync::atomic::AtomicU64;
use tower::ServiceExt;

struct Harness {
    _dir: tempfile::TempDir,
    engine: Arc<StorageQueryEngine>,
    sql: Arc<sql::State<StorageQueryEngine>>,
    app: Router,
    token: String,
    sequence: AtomicU64,
}
impl Harness {
    fn new(config: McpConfig) -> Self {
        std::fs::create_dir_all("/tmp/mydb/mcp").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb/mcp").unwrap();
        let mut storage = StorageConfig::from_env();
        storage.durability = DurabilityMode::Memory;
        storage.buffer_pool_pages = 512;
        storage.buffer_pool_io_threads = 1;
        let engine = Arc::new(
            StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                storage,
            )
            .unwrap(),
        );
        let token = Self::key(&engine, "mcp-test");
        let sql = sql::State::new(
            RestConfig::from_startup().unwrap(),
            engine.clone(),
            engine.registry().clone(),
        );
        let app = router(sql.clone(), config).layer(Extension(Self::peer()));
        Self {
            _dir: dir,
            engine,
            sql,
            app,
            token,
            sequence: AtomicU64::new(1),
        }
    }
    fn peer() -> Peer {
        Peer {
            host: "127.0.0.1".into(),
            cipher: None,
            version: None,
            subject: None,
            issuer: None,
        }
    }
    fn key(engine: &StorageQueryEngine, name: &str) -> String {
        let mut ctx = SessionContext::default();
        ctx.mysql_authenticated = true;
        ctx.ssl_cipher = Some("TLS_AES_256_GCM_SHA384".into());
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut ctx, &format!("CREATE API KEY '{}'", name))
            .unwrap()
        else {
            panic!()
        };
        result.rows[0][6].to_display_string()
    }
    fn request(&self, method: &str, mut params: Value) -> Request {
        params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28",
            "io.modelcontextprotocol/clientInfo":{"name":"mydb-test","version":"1"},
            "io.modelcontextprotocol/clientCapabilities":{}});
        let body = json!({"jsonrpc":"2.0","id":self.sequence.fetch_add(1,Ordering::Relaxed),"method":method,"params":params});
        let mut request = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("host", "127.0.0.1")
            .header("authorization", format!("Bearer {}", self.token))
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", method);
        if method == "tools/call" {
            request = request.header("mcp-name", body["params"]["name"].as_str().unwrap());
        }
        request.body(Body::from(body.to_string())).unwrap()
    }
    async fn response(response: Response) -> (StatusCode, Value, usize) {
        let status = response.status();
        let sse = response
            .headers()
            .get("content-type")
            .is_some_and(|v| v.to_str().unwrap().starts_with("text/event-stream"));
        let bytes = to_bytes(response.into_body(), 32 * 1024 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        let value = if sse {
            text.lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .filter_map(|s| serde_json::from_str::<Value>(s.trim()).ok())
                .find(|v| v.get("result").is_some() || v.get("error").is_some())
                .unwrap_or_else(|| panic!("Missing SSE result: {text}"))
        } else {
            serde_json::from_str(&text)
                .unwrap_or_else(|_| panic!("Invalid JSON response {status}: {text}"))
        };
        (status, value, bytes.len())
    }
    async fn rpc(&self, method: &str, params: Value) -> (StatusCode, Value, usize) {
        Self::response(
            self.app
                .clone()
                .oneshot(self.request(method, params))
                .await
                .unwrap(),
        )
        .await
    }
    async fn call(&self, name: &str, args: Value) -> Value {
        let (status, value, _) = self
            .rpc("tools/call", json!({"name":name,"arguments":args}))
            .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert!(value["result"].is_object(), "{value}");
        value["result"].clone()
    }
    async fn sql(&self, query: &str) -> Value {
        let value = self
            .call("execute_sql", json!({"database":"test","sql":query}))
            .await;
        assert_eq!(value["isError"], false, "{query}: {value}");
        value["structuredContent"].clone()
    }
    async fn close(&self) {
        self.sql.close_all().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_discovery_versions_and_tools() {
    let h = Harness::new(McpConfig::default());
    let (_, discovery, _) = h.rpc("server/discover", json!({})).await;
    assert_eq!(
        discovery["result"]["supportedVersions"],
        json!(["2026-07-28", "2025-11-25"])
    );
    assert!(discovery["result"]["capabilities"]["tools"].is_object());
    assert!(discovery["result"]["capabilities"]
        .get("resources")
        .is_none());
    let (_, list, _) = h.rpc("tools/list", json!({})).await;
    assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 10);
    assert_eq!(list["result"]["cacheScope"], "private");
    let mut request = h.request("tools/list", json!({}));
    request
        .headers_mut()
        .insert("mcp-protocol-version", "2099-01-01".parse().unwrap());
    let (status, error, _) = Harness::response(h.app.clone().oneshot(request).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    let body = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"old-client","version":"1"}}});
    let mut request = h.request("initialize", json!({}));
    request.headers_mut().remove("mcp-protocol-version");
    *request.body_mut() = Body::from(body.to_string());
    let (_, legacy, _) = Harness::response(h.app.clone().oneshot(request).await.unwrap()).await;
    assert_eq!(
        legacy["result"]["protocolVersion"], "2025-11-25",
        "{legacy}"
    );
    let mut request = h.request(
        "tools/call",
        json!({"name":"execute_sql","arguments":{"sql":"SELECT 42"}}),
    );
    request
        .headers_mut()
        .insert("mcp-protocol-version", "2025-11-25".parse().unwrap());
    *request.body_mut()=Body::from(json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"execute_sql","arguments":{"sql":"SELECT 42"}}}).to_string());
    let (_, legacy, _) = Harness::response(h.app.clone().oneshot(request).await.unwrap()).await;
    assert_eq!(
        legacy["result"]["structuredContent"]["results"][0]["rows"],
        json!([["42"]]),
        "{legacy}"
    );
    assert!(legacy["result"].get("resultType").is_none());
    h.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_types_metadata_transactions_and_api_key_boundary() {
    let h = Harness::new(McpConfig::default());
    h.sql("CREATE TABLE mcp_t (id INT PRIMARY KEY, v DECIMAL(30,9))")
        .await;
    let result=h.call("execute_sql",json!({"sql":"SELECT ? AS x, ? AS x, ?, ?, ?, ?",
        "params":[{"type":"uint64","value":"18446744073709551615"},{"type":"decimal","value":"12345678901234567890.123456789"},
            {"type":"binary","value":"AP8="},{"type":"json","value":"null"},
            {"type":"vector","value":"[1,2,3]","datatype":"FLOAT32","dimension":3},null]})).await;
    assert_eq!(result["isError"], false, "{result}");
    let rows = &result["structuredContent"]["results"][0]["rows"][0];
    assert_eq!(rows[0], "18446744073709551615");
    assert_eq!(rows[1], "12345678901234567890.123456789");
    assert!(rows[5].is_null());
    let metadata = h
        .call("describe_table", json!({"database":"test","table":"mcp_t"}))
        .await;
    assert_eq!(metadata["isError"], false, "{metadata}");
    assert_eq!(
        metadata["structuredContent"]["results"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    let tables = h.call("list_tables", json!({"database":"test"})).await;
    assert_eq!(tables["isError"], false, "{tables}");
    let open = h.call("open_session", json!({"database":"test"})).await;
    let sid = open["structuredContent"]["id"].as_str().unwrap();
    for query in [
        "BEGIN",
        "INSERT INTO mcp_t VALUES (1,1.23)",
        "SAVEPOINT s",
        "INSERT INTO mcp_t VALUES (2,2.34)",
        "ROLLBACK TO SAVEPOINT s",
        "COMMIT",
    ] {
        let result = h
            .call("execute_sql", json!({"session_id":sid,"sql":query}))
            .await;
        assert_eq!(result["isError"], false, "{query}: {result}");
    }
    assert_eq!(
        h.sql("SELECT COUNT(*) FROM mcp_t").await["results"][0]["rows"],
        json!([["1"]])
    );
    let plan = h
        .call(
            "explain_sql",
            json!({"database":"test","sql":"SELECT * FROM mcp_t"}),
        )
        .await;
    assert_eq!(plan["isError"], false, "{plan}");
    let bad = h
        .call(
            "explain_sql",
            json!({"sql":"EXPLAIN ANALYZE SELECT SLEEP(5)"}),
        )
        .await;
    assert_eq!(bad["isError"], true, "{bad}");
    for query in [
        "CREATE API KEY 'nope'",
        "SHOW API KEY 'mcp-test'",
        "DROP API KEY 'mcp-test'",
    ] {
        let result = h.call("execute_sql", json!({"sql":query})).await;
        assert_eq!(result["isError"], true, "{query}: {result}");
    }
    assert_eq!(
        h.call("close_session", json!({"session_id":sid})).await["isError"],
        false
    );
    h.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_result_limits_preserve_commits_and_later_errors() {
    let h = Harness::new(McpConfig {
        max_result_rows: 1,
        max_result_bytes: 8192,
        ..McpConfig::default()
    });
    h.sql("CREATE TABLE limits_t (id INT PRIMARY KEY)").await;
    let result = h
        .call(
            "execute_sql",
            json!({"database":"test","atomic":true,"statements":[
        {"sql":"INSERT INTO limits_t VALUES (1),(2)"},{"sql":"SELECT * FROM limits_t"}]}),
        )
        .await;
    let data = &result["structuredContent"];
    assert_eq!(result["isError"], true, "{result}");
    assert_eq!(data["error"]["code"], "RESULT_TOO_LARGE");
    assert_eq!(data["execution_outcome"], "succeeded");
    assert_eq!(data["transaction_state"], "idle");
    assert_eq!(data["results_omitted"], true);
    assert_eq!(
        h.sql("SELECT COUNT(*) FROM limits_t").await["results"][0]["rows"],
        json!([["2"]])
    );
    let result=h.call("execute_sql",json!({"database":"test","atomic":true,"statements":[
        {"sql":"INSERT INTO limits_t VALUES (3)"},{"sql":"SELECT * FROM limits_t"},{"sql":"INSERT INTO limits_t VALUES (1)"}]})).await;
    assert_eq!(
        result["structuredContent"]["error"]["mysql_errno"], 1062,
        "{result}"
    );
    assert_eq!(result["structuredContent"]["execution_outcome"], "failed");
    assert_eq!(
        h.sql("SELECT COUNT(*) FROM limits_t").await["results"][0]["rows"],
        json!([["2"]])
    );
    let (_, value, bytes) = h
        .rpc(
            "tools/call",
            json!({"name":"execute_sql","arguments":{"sql":"SELECT REPEAT('x',10000)"}}),
        )
        .await;
    assert!(bytes < 8192, "response length {bytes}");
    assert_eq!(
        value["result"]["structuredContent"]["error"]["code"],
        "RESULT_TOO_LARGE"
    );
    h.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_authentication_origin_validation_and_key_isolation() {
    let h = Harness::new(McpConfig::default());
    let open = h.call("open_session", json!({})).await;
    let sid = open["structuredContent"]["id"].as_str().unwrap();
    let mut request = h.request("tools/list", json!({}));
    request.headers_mut().remove("authorization");
    assert_eq!(
        h.app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let mut request = h.request("tools/list", json!({}));
    request
        .headers_mut()
        .insert("origin", "https://evil.example".parse().unwrap());
    assert_eq!(
        h.app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let other = Harness::key(&h.engine, "another");
    let mut request = h.request(
        "tools/call",
        json!({"name":"get_session","arguments":{"session_id":sid}}),
    );
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {other}").parse().unwrap());
    let (_, result, _) = Harness::response(h.app.clone().oneshot(request).await.unwrap()).await;
    assert_eq!(
        result["result"]["structuredContent"]["error"]["code"],
        "NOT_FOUND"
    );
    let mut ctx = SessionContext::default();
    ctx.mysql_authenticated = true;
    h.engine
        .execute_with_session(&mut ctx, "DROP API KEY 'mcp-test'")
        .unwrap();
    h.sql.sweep().await;
    let request = h.request("tools/list", json!({}));
    assert_eq!(
        h.app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(h.sql.sessions.lock().is_empty());
    h.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_shared_listener_feature_switches() {
    let h = Harness::new(McpConfig::default());
    for rest in [false, true] {
        for mcp in [false, true] {
            let mut config = RestConfig::from_startup().unwrap();
            config.enabled = rest;
            let app = RestServer::new(config, h.engine.clone(), h.engine.registry().clone(), None)
                .with_mcp(McpConfig {
                    enabled: mcp,
                    ..McpConfig::default()
                })
                .router()
                .layer(Extension(Harness::peer()));
            let response = app
                .clone()
                .oneshot(h.request("tools/list", json!({})))
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                if mcp {
                    StatusCode::OK
                } else {
                    StatusCode::NOT_FOUND
                }
            );
            let body = json!({"sql":"SELECT 1"});
            let req = Request::builder()
                .method("POST")
                .uri("/api/v1/sql")
                .header("authorization", format!("Bearer {}", h.token))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap();
            let response = app.oneshot(req).await.unwrap();
            assert_eq!(
                response.status(),
                if rest {
                    StatusCode::OK
                } else {
                    StatusCode::NOT_FOUND
                }
            );
            if rest {
                to_bytes(response.into_body(), 65536).await.unwrap();
            }
        }
    }
    h.close().await;
}

async fn wait_running(h: &Harness, id: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if h.sql
                .request(id, &h.sql.sessions.lock().values().next().unwrap().key_id)
                .is_ok_and(|r| r.state.lock()["state"] == "running")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_busy_cancel_timeout_disconnect_and_admission_cleanup() {
    let h = Harness::new(McpConfig {
        max_inflight: 2,
        ..McpConfig::default()
    });
    let open = h.call("open_session", json!({"database":"test"})).await;
    let sid = open["structuredContent"]["id"].as_str().unwrap();
    let id = "mcp_cancel_request_001";
    let request=h.request("tools/call",json!({"name":"execute_sql","arguments":{"session_id":sid,"sql":"SELECT SLEEP(10)","request_id":id,"timeout_ms":15000}}));
    let task = tokio::spawn(h.app.clone().oneshot(request));
    wait_running(&h, id).await;
    let busy = h
        .call("execute_sql", json!({"session_id":sid,"sql":"SELECT 1"}))
        .await;
    assert_eq!(
        busy["structuredContent"]["error"]["code"], "SESSION_BUSY",
        "{busy}"
    );
    let cancel = h.call("cancel_request", json!({"request_id":id})).await;
    assert_eq!(
        cancel["structuredContent"]["cancel_requested"], true,
        "{cancel}"
    );
    let (_, result, _) = Harness::response(
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    )
    .await;
    assert_eq!(result["result"]["isError"], true, "{result}");
    let info = h.call("get_session", json!({"session_id":sid})).await;
    assert_eq!(info["structuredContent"]["busy"], false, "{info}");
    let timeout = h
        .call(
            "execute_sql",
            json!({"sql":"SELECT SLEEP(2)","timeout_ms":10}),
        )
        .await;
    assert_eq!(timeout["isError"], true, "{timeout}");
    let id = "mcp_disconnect_request_001";
    let request=h.request("tools/call",json!({"name":"execute_sql","arguments":{"session_id":sid,"sql":"SELECT SLEEP(10)","request_id":id,"timeout_ms":15000}}));
    let task = tokio::spawn(h.app.clone().oneshot(request));
    wait_running(&h, id).await;
    task.abort();
    let _ = task.await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let key = h
                .sql
                .sessions
                .lock()
                .get(sid)
                .map(|s| s.key_id.clone())
                .unwrap_or_default();
            if h.sql.session(sid, &key).is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    h.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_raw_decimal_validation_and_network_memory_ownership() {
    let h = Harness::new(McpConfig::default());
    let mut request = h.request(
        "tools/call",
        json!({"name":"execute_sql","arguments":{"sql":"SELECT ?","params":["DECIMAL_LITERAL"]}}),
    );
    let body = to_bytes(std::mem::replace(request.body_mut(), Body::empty()), 65536)
        .await
        .unwrap();
    let body = String::from_utf8(body.to_vec())
        .unwrap()
        .replace("\"DECIMAL_LITERAL\"", "0.123456789123456789123456789");
    *request.body_mut() = Body::from(body);
    let response = h.app.clone().oneshot(request).await.unwrap();
    let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
    assert!(
        h.sql.used.load(Ordering::Acquire) > 3 * 1024 * 1024,
        "SDK buffer accounting must outlive the response body"
    );
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(text.contains("0.123456789123456789123456789"), "{text}");
    drop(bytes);
    let mut request = h.request(
        "tools/call",
        json!({"name":"execute_sql","arguments":{"sql":"SELECT 1","unexpected":true}}),
    );
    let (_, result, _) = Harness::response(h.app.clone().oneshot(request).await.unwrap()).await;
    assert_eq!(result["result"]["isError"], true, "{result}");
    request = h.request(
        "tools/call",
        json!({"name":"execute_sql","arguments":{"sql":"SELECT 1; SELECT 2"}}),
    );
    let (_, result, _) = Harness::response(h.app.clone().oneshot(request).await.unwrap()).await;
    assert_eq!(
        result["result"]["structuredContent"]["error"]["code"],
        "MULTIPLE_STATEMENTS_NOT_ALLOWED"
    );
    request = h.request("tools/call", json!({"name":"execute_sql","arguments":{}}));
    *request.body_mut() = Body::from(
        r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"execute_sql","arguments":{"sql":"SELECT 1","sql":"SELECT 2"}}}"#,
    );
    assert_eq!(
        h.app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    h.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_account_privilege_revocation_and_readonly_metadata() {
    let mut h = Harness::new(McpConfig::default());
    h.sql("CREATE TABLE reader_t (id INT PRIMARY KEY)").await;
    h.sql("INSERT INTO reader_t VALUES (7)").await;
    let mut root = SessionContext::default();
    root.mysql_authenticated = true;
    root.ssl_cipher = Some("TLS_AES_256_GCM_SHA384".into());
    for query in [
        "CREATE USER 'mcp_reader'@'%' IDENTIFIED BY 'secret'",
        "GRANT SELECT ON test.reader_t TO 'mcp_reader'@'%'",
    ] {
        h.engine.execute_with_session(&mut root, query).unwrap();
    }
    let QueryResponse::ResultSet(result) = h
        .engine
        .execute_with_session(&mut root, "CREATE API KEY 'reader' FOR 'mcp_reader'@'%'")
        .unwrap()
    else {
        panic!()
    };
    h.token = result.rows[0][6].to_display_string();
    assert_eq!(
        h.sql("SELECT id FROM reader_t").await["results"][0]["rows"],
        json!([["7"]])
    );
    let write = h
        .call(
            "execute_sql",
            json!({"database":"test","sql":"DELETE FROM reader_t"}),
        )
        .await;
    assert_eq!(write["isError"], true, "{write}");
    assert_eq!(
        h.call(
            "describe_table",
            json!({"database":"test","table":"reader_t"})
        )
        .await["isError"],
        false
    );
    h.engine
        .execute_with_session(
            &mut root,
            "REVOKE SELECT ON test.reader_t FROM 'mcp_reader'@'%'",
        )
        .unwrap();
    let revoked = h
        .call(
            "execute_sql",
            json!({"database":"test","sql":"SELECT id FROM reader_t"}),
        )
        .await;
    assert_eq!(revoked["isError"], true, "{revoked}");
    h.close().await;
}
#[test]
fn mcp_origin_authorities_are_exact() {
    assert!(same_origin(
        "https://APP.example",
        "https://app.example:443"
    ));
    assert!(!same_origin(
        "https://app.example:444",
        "https://app.example"
    ));
    for origin in [
        "null",
        "https://evil.example",
        "https://app.example/path",
        "https://user@app.example",
        "https://app.example:*",
        "https://app.example?x=1",
    ] {
        assert!(!same_origin(origin, "https://app.example"), "{origin}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_http2_cancel_has_transport_capacity_when_sql_slots_are_full() {
    let h = Harness::new(McpConfig::default());
    let mut config = RestConfig::from_startup().unwrap();
    config.tls = false;
    config.max_inflight = 1;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let server = RestServer::new(config, h.engine.clone(), h.engine.registry().clone(), None)
        .with_mcp(McpConfig {
            enabled: true,
            ..McpConfig::default()
        });
    let server = tokio::spawn(async move { server.run_with_listener(listener).await });
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .pool_max_idle_per_host(1)
        .build()
        .unwrap();
    let request = h.request(
        "tools/call",
        json!({"name":"execute_sql","arguments":{
        "sql":"SELECT SLEEP(10)","request_id":"h2_transport_cancel_001","timeout_ms":15000}}),
    );
    let (head, body) = request.into_parts();
    let data = to_bytes(body, 65536).await.unwrap();
    let query = client.post(&url).headers(head.headers).body(data);
    let running = tokio::spawn(async move { query.send().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if h.engine
                .registry()
                .snapshot()
                .iter()
                .any(|p| p.info.as_deref() == Some("SELECT SLEEP(10)"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let request = h.request(
        "tools/call",
        json!({"name":"cancel_request","arguments":{"request_id":"h2_transport_cancel_001"}}),
    );
    let (head, body) = request.into_parts();
    let data = to_bytes(body, 65536).await.unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        client.post(&url).headers(head.headers).body(data).send(),
    )
    .await
    .expect("control requests must not queue behind an occupied SQL stream")
    .unwrap();
    assert_eq!(response.version(), reqwest::Version::HTTP_2);
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response
        .text()
        .await
        .unwrap()
        .contains("\"cancel_requested\":true"));
    let response = tokio::time::timeout(Duration::from_secs(3), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(response.text().await.unwrap().contains("\"isError\":true"));
    drop(client);
    server.abort();
    let _ = server.await;
    h.close().await;
}

#[path = "audit_tests.rs"]
mod audit;
