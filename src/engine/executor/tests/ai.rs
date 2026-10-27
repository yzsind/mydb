use super::*;
use serde_json::{json, Value as Json};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
};

type MockResponder = Arc<dyn Fn(&str, &Json) -> Vec<u8> + Send + Sync>;

struct Mock {
    port: u16,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<Json>>>,
    reading_started: Arc<AtomicUsize>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Mock {
    fn new() -> Self {
        Self::start(None)
    }
    fn responding(reply: impl Fn(&str, &Json) -> Vec<u8> + Send + Sync + 'static) -> Self {
        Self::start(Some(Arc::new(reply)))
    }
    fn start(responder: Option<MockResponder>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let reading_started = Arc::new(AtomicUsize::new(0));
        let reading = reading_started.clone();
        let s = stop.clone();
        let q = requests.clone();
        let handle = thread::spawn(move || {
            let mut workers = Vec::new();
            while !s.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let q = q.clone();
                        let responder = responder.clone();
                        let reading = reading.clone();
                        workers.push(thread::spawn(move || {
                            mock_request(stream, q, responder, reading)
                        }));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(e)
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::Interrupted | std::io::ErrorKind::ConnectionAborted
                        ) =>
                    {
                        continue
                    }
                    Err(e) => panic!("mock listener accept failed: {e}"),
                }
            }
            for worker in workers {
                worker.join().unwrap();
            }
        });
        Self {
            port,
            stop,
            requests,
            reading_started,
            thread: Some(handle),
        }
    }
}
fn mock_request(
    mut stream: std::net::TcpStream,
    requests: Arc<Mutex<Vec<Json>>>,
    responder: Option<MockResponder>,
    reading_started: Arc<AtomicUsize>,
) {
    // BSD/macOS accepted sockets inherit the listener's nonblocking mode.
    // A temporarily empty socket must not be mistaken for request EOF.
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    reading_started.fetch_add(1, Ordering::Release);
    let mut bytes = Vec::new();
    let end = loop {
        let mut buf = [0; 4096];
        let n = match stream.read(&mut buf) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => result.unwrap_or(0),
        };
        if n == 0 {
            return;
        }
        bytes.extend_from_slice(&buf[..n]);
        if let Some(i) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break i + 4;
        }
        if bytes.len() > 1024 * 1024 {
            return;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..end]).into_owned();
    let path = headers
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .to_owned();
    let len = headers
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                .map(|(_, v)| v.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    while bytes.len() < end + len {
        let mut buf = [0; 4096];
        let n = match stream.read(&mut buf) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => result.unwrap_or(0),
        };
        if n == 0 {
            return;
        }
        bytes.extend_from_slice(&buf[..n]);
    }
    let mut request: Json = if len == 0 {
        json!({})
    } else {
        serde_json::from_slice(&bytes[end..end + len]).unwrap()
    };
    if let Some((_, value)) = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.eq_ignore_ascii_case("authorization"))
    {
        request["_test_authorization"] = json!(value.trim());
    }
    let mut recorded = request.clone();
    recorded["_test_path"] = json!(path);
    let mut header_values = std::collections::BTreeMap::<String, Vec<String>>::new();
    for (name, value) in headers.lines().filter_map(|line| line.split_once(':')) {
        header_values
            .entry(name.to_ascii_lowercase())
            .or_default()
            .push(value.trim().to_owned());
    }
    recorded["_test_headers"] = json!(header_values);
    requests.lock().unwrap().push(recorded);
    if let Some(reply) = responder {
        let _ = stream.write_all(&reply(&path, &request));
        return;
    }
    let prompt = request["messages"][0]["content"]
        .as_str()
        .unwrap_or_default();
    if prompt == "__slow__" {
        thread::sleep(Duration::from_secs(2));
    }
    if prompt == "__parallel__" {
        thread::sleep(Duration::from_millis(300));
    }
    let status = if prompt == "__503__" {
        503
    } else if prompt == "__401__" {
        401
    } else {
        200
    };
    let response = if path.ends_with("embed") {
        let count = request["input"].as_array().map_or(1, Vec::len);
        let dim = if request["input"] == "bad-dimension" {
            2
        } else {
            3
        };
        json!({"model":"mock-embed","embeddings":(0..count).map(|_|vec![0.25;dim]).collect::<Vec<_>>(),"prompt_eval_count":count})
    } else if status != 200 {
        json!({"error":"dummy-provider-error-body-must-not-be-exposed"})
    } else {
        json!({"model":"mock-chat","done":true,"done_reason":if prompt=="__length__"{"length"}else{"stop"},"message":{"role":"assistant","content":"mock generated text"},"prompt_eval_count":4,"eval_count":3})
    };
    let body = response.to_string();
    let _=write!(stream,"HTTP/1.1 {} test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nRetry-After: 0\r\nx-request-id: mock-request-id\r\nConnection: close\r\n\r\n{}",status,body.len(),body);
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(t) = self.thread.take() {
            if let Err(panic) = t.join() {
                if !thread::panicking() {
                    std::panic::resume_unwind(panic);
                }
            }
        }
    }
}
fn temp() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    tempfile::Builder::new()
        .prefix("llm-engine-")
        .tempdir_in("/tmp/mydb")
        .unwrap()
}
fn exec(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> QueryResponse {
    engine
        .execute_with_session(session, sql)
        .unwrap_or_else(|e| panic!("{sql}: {}", e.message()))
}
#[test]
fn ai_sql_projection_null_short_circuit_batch_and_contexts() {
    let mock = Mock::new();
    let dir = temp();
    let policy = dir.path().join("policy.json");
    let origin = format!("http://127.0.0.1:{}", mock.port);
    std::fs::write(&policy,json!({"version":1,"egress":{"allowed_origins":[origin],"private_destinations":["127.0.0.1/32"],"allow_plaintext_origins":[origin]}}).to_string()).unwrap();
    crate::util::env::with_vars(
        &[
            ("MYDB_AI_ENABLED", Some("ON")),
            ("MYDB_AI_LOCAL_CONFIG", policy.to_str()),
            ("MYDB_AI_ENCRYPT_KEY", None),
        ],
        || {
            let engine = open_test_engine(&dir.path().join("data"));
            let mut session = SessionContext::default();
            session.current_database = "test".into();
            session.ssl_cipher = Some("test-TLS".into());
            exec(
                &engine,
                &mut session,
                &format!(
                    "CREATE AI CONNECTION local WITH CONFIG '{}'",
                    json!({"protocol":"ollama","base_url":origin,"auth":{"type":"none"}})
                ),
            );
            exec(
                &engine,
                &mut session,
                r#"CREATE AI MODEL test.chat USING local WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1","capabilities":{"messages":true,"temperature":true},"defaults":{"max_output_tokens":32}}'"#,
            );
            exec(
                &engine,
                &mut session,
                r#"CREATE AI MODEL test.embed USING local WITH CONFIG '{"kind":"embedding","remote_model":"mock-embed","model_revision":"v1","embedding":{"dimensions":3}}'"#,
            );
            let result = materialize(exec(
                &engine,
                &mut session,
                "SELECT AI_GENERATE('test.chat','hello') AS answer",
            ));
            assert_eq!(
                result.rows[0][0],
                Value::String("mock generated text".into())
            );
            exec(
                &engine,
                &mut session,
                "SET @q = AI_EMBED('test.embed','hello')",
            );
            assert!(matches!(session.user_variable("q"), Some(Value::Vector(_))));
            let before = mock.requests.lock().unwrap().len();
            exec(&engine,&mut session,"SELECT AI_GENERATE('test.chat',NULL), CASE WHEN 0 THEN AI_GENERATE('test.chat','hidden') ELSE 'skip' END");
            assert_eq!(mock.requests.lock().unwrap().len(), before);
            exec(
                &engine,
                &mut session,
                "CREATE TABLE docs(id INT PRIMARY KEY, body TEXT)",
            );
            exec(
                &engine,
                &mut session,
                "INSERT INTO docs VALUES(1,'one'),(2,'two'),(3,'three')",
            );
            let result=materialize(exec(&engine,&mut session,"SELECT id, AI_GENERATE('test.chat',body) AS text FROM docs ORDER BY id LIMIT 1 OFFSET 1"));
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], Value::Int(2));
            assert_eq!(mock.requests.lock().unwrap().len(), before + 1);
            let result = materialize(exec(
                &engine,
                &mut session,
                "SELECT AI_EMBED_DETAIL('test.embed',JSON_ARRAY('a','b'))",
            ));
            let Value::Json(bytes) = &result.rows[0][0] else {
                panic!()
            };
            assert_eq!(
                crate::engine::json::to_serde(bytes).unwrap()["data"]
                    .as_array()
                    .unwrap()
                    .len(),
                2
            );
            assert!(engine
                .execute_with_session(
                    &mut session,
                    "SELECT AI_EMBED('test.embed','bad-dimension')"
                )
                .unwrap_err()
                .message()
                .contains("DIMENSION"));
            for sql in [
                "SELECT id FROM docs WHERE AI_GENERATE('test.chat',body)='x'",
                "UPDATE docs SET body=AI_GENERATE('test.chat',body)",
                "SELECT DISTINCT AI_GENERATE('test.chat',body) FROM docs",
                "SELECT AI_GENERATE('test.chat',body) AS a FROM docs ORDER BY a",
                "CREATE VIEW forbidden AS SELECT AI_GENERATE('test.chat','x')",
            ] {
                assert!(
                    engine.execute_with_session(&mut session, sql).is_err(),
                    "accepted {sql}"
                );
            }
            exec(&engine, &mut session, "BEGIN");
            assert!(engine
                .execute_with_session(&mut session, "SELECT AI_GENERATE('test.chat','x')")
                .is_err());
            exec(&engine, &mut session, "ROLLBACK");
            exec(
                &engine,
                &mut session,
                "CREATE AI SECRET cloud VALUE 'dummy-regression-key'",
            );
            assert!(dir.path().join("data/apikey.mydb").is_file());
            let bytes = serde_json::to_vec(&*engine.db.ai_catalog().unwrap()).unwrap();
            assert!(!bytes
                .windows(b"dummy-regression-key".len())
                .any(|v| v == b"dummy-regression-key"));
        },
    );
}

fn with_mock(f: impl FnOnce(&Mock, &std::path::Path, &StorageQueryEngine, &mut SessionContext)) {
    let mock = Mock::new();
    with_provider(&mock, f);
}
fn with_provider(
    mock: &Mock,
    f: impl FnOnce(&Mock, &std::path::Path, &StorageQueryEngine, &mut SessionContext),
) {
    let dir = temp();
    let policy = dir.path().join("policy.json");
    let origin = format!("http://127.0.0.1:{}", mock.port);
    std::fs::write(&policy,json!({"version":1,"egress":{"allowed_origins":[origin],"private_destinations":["127.0.0.1/32"],"allow_plaintext_origins":[origin]}}).to_string()).unwrap();
    crate::util::env::with_vars(
        &[
            ("MYDB_AI_ENABLED", Some("ON")),
            ("MYDB_AI_LOCAL_CONFIG", policy.to_str()),
            ("MYDB_AI_ENCRYPT_KEY", None),
        ],
        || {
            let engine = open_test_engine(&dir.path().join("data"));
            let mut session = SessionContext::default();
            session.current_database = "test".into();
            session.ssl_cipher = Some("test-TLS".into());
            exec(
                &engine,
                &mut session,
                &format!(
                    "CREATE AI CONNECTION local WITH CONFIG '{}'",
                    json!({"protocol":"ollama","base_url":origin,"auth":{"type":"none"}})
                ),
            );
            exec(
                &engine,
                &mut session,
                r#"CREATE AI MODEL test.chat USING local WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1","capabilities":{"messages":true,"json_schema":true},"defaults":{"max_output_tokens":32}}'"#,
            );
            exec(
                &engine,
                &mut session,
                r#"CREATE AI MODEL test.embed USING local WITH CONFIG '{"kind":"embedding","remote_model":"mock-embed","model_revision":"v1","embedding":{"dimensions":3}}'"#,
            );
            f(mock, dir.path(), &engine, &mut session)
        },
    );
}

#[test]
fn ai_embedding_batches_and_explain_preserve_occurrence_counts() {
    with_mock(|mock, _, engine, session| {
        exec(
            engine,
            session,
            "CREATE TABLE inputs(id INT PRIMARY KEY,body TEXT)",
        );
        exec(
            engine,
            session,
            "INSERT INTO inputs VALUES(1,'a'),(2,'b'),(3,'c')",
        );
        let before = mock.requests.lock().unwrap().len();
        let rows = materialize(exec(
            engine,
            session,
            "SELECT id,AI_EMBED('test.embed',body) AS v FROM inputs ORDER BY id",
        ));
        assert_eq!(rows.rows.len(), 3);
        assert_eq!(rows.columns[0].name, "id");
        assert_eq!(rows.columns[0].column_type, ColumnType::Long);
        assert_eq!(mock.requests.lock().unwrap().len(), before + 1);
        assert_eq!(
            mock.requests.lock().unwrap().last().unwrap()["input"],
            json!(["a", "b", "c"])
        );
        exec(
            engine,
            session,
            "EXPLAIN SELECT AI_GENERATE('test.chat',body) FROM inputs LIMIT 1",
        );
        assert_eq!(mock.requests.lock().unwrap().len(), before + 1);
        exec(
            engine,
            session,
            "EXPLAIN ANALYZE SELECT AI_GENERATE('test.chat',body) FROM inputs LIMIT 1",
        );
        assert_eq!(mock.requests.lock().unwrap().len(), before + 2);
        let rows=materialize(exec(engine,session,"WITH chosen AS (SELECT body FROM inputs WHERE id=1) SELECT AI_GENERATE('test.chat',body) AS answer FROM chosen"));
        assert_eq!(rows.rows.len(), 1);
        assert!(engine
            .execute_with_session(
                session,
                "SELECT AI_GENERATE('test.missing','x') FROM inputs LIMIT 0"
            )
            .is_err());
        assert!(engine
            .execute_with_session(session, "SELECT @x:=AI_GENERATE('test.chat','x')")
            .is_err());
    });
}

#[test]
fn ai_model_privileges_roles_and_prepared_recheck_revocation() {
    with_mock(|mock, _, engine, root| {
        exec(
            engine,
            root,
            "CREATE USER 'ai_reader'@'localhost' IDENTIFIED BY 'test-only'",
        );
        let mut reader = SessionContext::default();
        reader.current_database = "test".into();
        reader.user = "ai_reader".into();
        reader.login_user = "ai_reader".into();
        reader.authenticated_as = crate::engine::auth::AccountKey::new("ai_reader", "localhost");
        reader.current_user_is_system = false;
        assert!(engine
            .execute_with_session(&mut reader, "SELECT AI_GENERATE('test.chat','x')")
            .is_err());
        exec(
            engine,
            root,
            "GRANT AI_USE ON AI MODEL test.chat TO 'ai_reader'@'localhost'",
        );
        exec(engine, &mut reader, "USE test");
        assert!(engine
            .execute_with_session(&mut reader, "USE mysql")
            .is_err());
        let sql = "SELECT AI_GENERATE('test.chat',?) AS answer";
        let template =
            <StorageQueryEngine as QueryEngine>::prepare_cached(engine, &mut reader, sql, 1)
                .unwrap();
        let before = mock.requests.lock().unwrap().len();
        let columns = <StorageQueryEngine as QueryEngine>::describe_prepared_statement(
            engine,
            &mut reader,
            sql,
        )
        .unwrap();
        assert_eq!(columns[0].name, "answer");
        assert_eq!(mock.requests.lock().unwrap().len(), before);
        let response =
            crate::engine::params::with_params(vec![Value::String("prepared".into())], || {
                <StorageQueryEngine as QueryEngine>::execute_prepared_statement(
                    engine,
                    &mut reader,
                    sql,
                    (*template).clone(),
                )
            })
            .unwrap()
            .unwrap();
        assert_eq!(materialize(response).rows.len(), 1);
        let visible = materialize(exec(
            engine,
            &mut reader,
            "SELECT MODEL_NAME FROM information_schema.AI_MODELS",
        ));
        assert_eq!(visible.rows.len(), 1);
        exec(
            engine,
            root,
            "REVOKE AI_USE ON AI MODEL test.chat FROM 'ai_reader'@'localhost'",
        );
        assert!(engine
            .execute_with_session(&mut reader, "USE test")
            .is_err());
        let before = mock.requests.lock().unwrap().len();
        let result = crate::engine::params::with_params(
            vec![Value::String("must not dispatch".into())],
            || {
                <StorageQueryEngine as QueryEngine>::execute_prepared_statement(
                    engine,
                    &mut reader,
                    sql,
                    (*template).clone(),
                )
            },
        )
        .unwrap();
        assert!(result.is_err());
        assert_eq!(mock.requests.lock().unwrap().len(), before);
        exec(engine, root, "CREATE ROLE 'ai_role'@'%'");
        exec(
            engine,
            root,
            "GRANT AI_USE ON AI MODEL test.chat TO 'ai_role'@'%'",
        );
        exec(
            engine,
            root,
            "GRANT 'ai_role'@'%' TO 'ai_reader'@'localhost'",
        );
        exec(engine, &mut reader, "SET ROLE 'ai_role'@'%'");
        exec(
            engine,
            &mut reader,
            "SELECT AI_GENERATE('test.chat','role')",
        );
        exec(engine, root, "DROP AI MODEL test.chat");
        exec(
            engine,
            root,
            r#"CREATE AI MODEL test.chat USING local WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v2","defaults":{"max_output_tokens":32}}'"#,
        );
        assert!(engine
            .execute_with_session(&mut reader, "SELECT AI_GENERATE('test.chat','old grant')")
            .is_err());
    });
}

#[cfg(unix)]
#[test]
fn ai_encrypted_catalog_reopens_and_master_rotation_only_rewraps() {
    use base64::Engine as _;
    use sha2::{Digest, Sha256};
    let dir = temp();
    let policy = dir.path().join("policy.json");
    std::fs::write(
        &policy,
        json!({"version":1,"egress":{"allowed_origins":[]}}).to_string(),
    )
    .unwrap();
    crate::util::env::with_vars(
        &[
            ("MYDB_AI_ENABLED", Some("ON")),
            ("MYDB_AI_LOCAL_CONFIG", policy.to_str()),
            ("MYDB_AI_ENCRYPT_KEY", None),
        ],
        || {
            let original;
            {
                let engine = open_strict_test_engine(&dir.path().join("data"));
                let mut session = SessionContext::default();
                session.ssl_cipher = Some("test-TLS".into());
                exec(
                    &engine,
                    &mut session,
                    "CREATE AI SECRET persisted VALUE 'unique-dummy-secret-rotation-regression'",
                );
                original = engine.db.ai_catalog().unwrap().secrets["persisted"].clone();
            }
            let engine = open_strict_test_engine(&dir.path().join("data"));
            let mut session = SessionContext::default();
            session.ssl_cipher = Some("test-TLS".into());
            let state = engine.db.ai_catalog().unwrap();
            let keys = crate::engine::ai::load_keys(engine.db.data_dir(), &state, false).unwrap();
            assert_eq!(
                &*crate::engine::ai::decrypt_secret(&state, &keys, &state.secrets["persisted"])
                    .unwrap(),
                b"unique-dummy-secret-rotation-regression"
            );
            let path = engine.db.data_dir().join("apikey.mydb");
            let mut file: Json = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            let candidate = [0x85; 32];
            let mut h = Sha256::new();
            h.update(b"mydb.ai.kek.v1\0");
            h.update(candidate);
            let id = h
                .finalize()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            file["keys"][&id] = json!(format!(
                "base64:{}",
                base64::engine::general_purpose::STANDARD.encode(candidate)
            ));
            file["active_key_id"] = json!(id);
            std::fs::write(&path, file.to_string()).unwrap();
            exec(
                &engine,
                &mut session,
                "ALTER SYSTEM ROTATE AI ENCRYPTION KEY",
            );
            let state = engine.db.ai_catalog().unwrap();
            let rotated = &state.secrets["persisted"];
            assert_eq!(rotated.ciphertext, original.ciphertext);
            assert_eq!(rotated.nonce, original.nonce);
            assert_ne!(rotated.wrapped_dek, original.wrapped_dek);
            assert_eq!(rotated.generation, original.generation);
            assert_eq!(rotated.crypto_generation, original.crypto_generation + 1);
            let keys = crate::engine::ai::load_keys(engine.db.data_dir(), &state, false).unwrap();
            assert_eq!(
                &*crate::engine::ai::decrypt_secret(&state, &keys, rotated).unwrap(),
                b"unique-dummy-secret-rotation-regression"
            );
            std::fs::remove_file(&path).unwrap();
            assert!(engine
                .execute_with_session(
                    &mut session,
                    "ALTER AI SECRET persisted VALUE 'replacement'"
                )
                .is_err());
            assert!(!path.exists());
            exec(&engine, &mut session, "SELECT 1");
        },
    );
}

fn materialize(response: QueryResponse) -> ResultSet {
    match response.materialize().unwrap() {
        QueryResponse::ResultSet(r) => r,
        _ => panic!("expected rows"),
    }
}

#[test]
fn ai_deferred_projection_releases_mdl_and_pins_model_and_variable_state() {
    with_mock(|mock, _, engine, session| {
        exec(
            engine,
            session,
            "CREATE TABLE deferred_docs(id INT PRIMARY KEY,body TEXT)",
        );
        exec(
            engine,
            session,
            "INSERT INTO deferred_docs VALUES(1,'owned before drop')",
        );
        exec(engine, session, "SET @model_name='test.chat'");
        let before = mock.requests.lock().unwrap().len();
        let response = <StorageQueryEngine as QueryEngine>::execute(
            engine,
            session,
            "SELECT AI_GENERATE(@model_name,body) AS answer FROM deferred_docs",
        )
        .unwrap();
        assert_eq!(
            mock.requests.lock().unwrap().len(),
            before,
            "network I/O must be deferred beyond statement MDL release"
        );
        let mut ddl = SessionContext::default();
        ddl.current_database = "test".into();
        ddl.connection_id = 2;
        exec(engine, &mut ddl, "SET SESSION lock_wait_timeout=1");
        let start = std::time::Instant::now();
        exec(engine, &mut ddl, "DROP TABLE deferred_docs");
        assert!(start.elapsed() < Duration::from_millis(500));
        exec(engine, session, "SET @model_name='test.embed'");
        exec(
            engine,
            &mut ddl,
            r#"ALTER AI MODEL test.chat WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1","capabilities":{"messages":true,"json_schema":true},"defaults":{"max_output_tokens":16}}'"#,
        );
        let rows = materialize(response);
        assert_eq!(rows.rows.len(), 1);
        let request = mock.requests.lock().unwrap().last().unwrap().clone();
        assert_eq!(request["messages"][0]["content"], "owned before drop");
        assert_eq!(request["options"]["num_predict"], 32);
        exec(
            engine,
            session,
            "SELECT AI_GENERATE('test.chat','new snapshot')",
        );
        assert_eq!(
            mock.requests.lock().unwrap().last().unwrap()["options"]["num_predict"],
            16
        );
    });
}

#[test]
fn ai_parallel_generation_uses_one_bounded_async_window() {
    with_mock(|mock, _, engine, session| {
        exec(
            engine,
            session,
            "CREATE TABLE parallel_inputs(id INT PRIMARY KEY,body TEXT)",
        );
        exec(engine,session,"INSERT INTO parallel_inputs VALUES(1,'__parallel__'),(2,'__parallel__'),(3,'__parallel__'),(4,'__parallel__')");
        let start = std::time::Instant::now();
        let rows = materialize(exec(
            engine,
            session,
            "SELECT id,AI_GENERATE('test.chat',body) AS answer FROM parallel_inputs ORDER BY id",
        ));
        assert_eq!(rows.rows.len(), 4);
        assert_eq!(mock.requests.lock().unwrap().len(), 4);
        assert!(
            start.elapsed() < Duration::from_millis(1000),
            "four 300ms HTTP calls must overlap"
        );
        for (index, row) in rows.rows.iter().enumerate() {
            assert_eq!(row[0], Value::Int((index + 1) as i64));
        }
    });
}
#[test]
fn ai_timeouts_incomplete_output_and_retry_ambiguity_are_explicit() {
    with_mock(|mock, _, engine, session| {
        let result = materialize(exec(
            engine,
            session,
            "SELECT AI_GENERATE_DETAIL('test.chat','__length__',JSON_OBJECT('on_error','return'))",
        ));
        let Value::Json(bytes) = &result.rows[0][0] else {
            panic!()
        };
        let value = crate::engine::json::to_serde(bytes).unwrap();
        assert_eq!(value["ok"], false);
        assert_eq!(value["error"]["code"], "ER_AI_INCOMPLETE", "{value}");
        assert_eq!(value["finish_reason"], "length", "{value}");
        let before = mock.requests.lock().unwrap().len();
        let result = materialize(exec(
            engine,
            session,
            "SELECT AI_GENERATE_DETAIL('test.chat','__503__',JSON_OBJECT('on_error','return'))",
        ));
        let Value::Json(bytes) = &result.rows[0][0] else {
            panic!()
        };
        let value = crate::engine::json::to_serde(bytes).unwrap();
        assert_eq!(value["attempts"], 1);
        assert_eq!(value["error"]["http_status"], 503);
        assert_eq!(value["error"]["ambiguous"], true);
        assert!(!value.to_string().contains("dummy-provider-error"));
        assert_eq!(mock.requests.lock().unwrap().len(), before + 1);
        exec(
            engine,
            session,
            r#"ALTER AI MODEL test.chat WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1","capabilities":{"messages":true,"json_schema":true},"defaults":{"max_output_tokens":32},"limits":{"max_retries":2,"allow_retry_ambiguous":true}}'"#,
        );
        let before = mock.requests.lock().unwrap().len();
        let result=materialize(exec(engine,session,"SELECT AI_GENERATE_DETAIL('test.chat','__503__',JSON_OBJECT('max_retries',2,'retry_ambiguous',TRUE,'on_error','return'))"));
        let Value::Json(bytes) = &result.rows[0][0] else {
            panic!()
        };
        let value = crate::engine::json::to_serde(bytes).unwrap();
        assert_eq!(value["attempts"], 3);
        assert_eq!(mock.requests.lock().unwrap().len(), before + 3);
        let start = std::time::Instant::now();
        let result=materialize(exec(engine,session,"SELECT AI_GENERATE_DETAIL('test.chat','__slow__',JSON_OBJECT('timeout_ms',20,'on_error','return'))"));
        assert!(start.elapsed() < Duration::from_secs(1));
        let Value::Json(bytes) = &result.rows[0][0] else {
            panic!()
        };
        assert_eq!(
            crate::engine::json::to_serde(bytes).unwrap()["error"]["code"],
            "ER_AI_TIMEOUT"
        );
        exec(engine, session, "SET SESSION max_execution_time=10");
        let error=engine.execute_with_session(session,"SELECT AI_GENERATE_DETAIL('test.chat','__slow__',JSON_OBJECT('on_error','return'))").unwrap_err();
        assert_eq!(
            error.code(),
            crate::error::catalog::ER_QUERY_TIMEOUT.code().get()
        );
        exec(engine, session, "SET SESSION max_execution_time=0");
        exec(engine, session, "SELECT 1");
    });
}
#[cfg(unix)]
#[test]
fn ai_secret_rotation_refreshes_auth_and_policy_reload_fails_closed() {
    with_mock(|mock, dir, engine, session| {
        exec(
            engine,
            session,
            "CREATE AI SECRET token VALUE 'dummy-old-key'",
        );
        let origin = format!("http://127.0.0.1:{}", mock.port);
        exec(
            engine,
            session,
            &format!(
                "ALTER AI CONNECTION local WITH CONFIG '{}'",
                json!({"protocol":"ollama","base_url":origin,"auth":{"type":"bearer","secret":"TOKEN"}})
            ),
        );
        let response = <StorageQueryEngine as QueryEngine>::execute(
            engine,
            session,
            "SELECT AI_GENERATE('test.chat','after rotation')",
        )
        .unwrap();
        exec(
            engine,
            session,
            "ALTER AI SECRET token VALUE 'dummy-new-key'",
        );
        materialize(response);
        assert_eq!(
            mock.requests.lock().unwrap().last().unwrap()["_test_authorization"],
            "Bearer dummy-new-key"
        );
        let before = mock.requests.lock().unwrap().len();
        let policy = dir.join("policy.json");
        std::fs::write(&policy, r#"{"version":1,"egress":{"allowed_origins":[]}}"#).unwrap();
        exec(engine, session, "ALTER SYSTEM RELOAD AI CONFIG");
        assert!(engine
            .execute_with_session(
                session,
                "SELECT AI_GENERATE('test.chat','denied destination')"
            )
            .unwrap_err()
            .message()
            .contains("EGRESS"));
        assert_eq!(mock.requests.lock().unwrap().len(), before);
        std::fs::write(&policy, r#"{"version":1,"unknown":true}"#).unwrap();
        assert!(engine
            .execute_with_session(session, "ALTER SYSTEM RELOAD AI CONFIG")
            .is_err());
        assert!(engine
            .execute_with_session(session, "SELECT AI_GENERATE('test.chat','still denied')")
            .is_err());
        assert_eq!(mock.requests.lock().unwrap().len(), before);
    });
}

#[test]
fn ai_model_grammar_preserves_sql_modes_quoted_names_and_unicode_schemas() {
    for sql in [
        "CREATE;AI SECRET x VALUE 'dummy'",
        "CREATE AI SECRET x; VALUE 'dummy'",
        "CREATE AI MODEL a.b.c USING conn WITH CONFIG '{}'",
    ] {
        assert!(crate::parser::parse_statement(sql).is_err(), "{sql}");
    }
    let _mode = crate::parser::ParserSqlModeGuard::activate(
        crate::engine::api::SQL_MODE_NO_BACKSLASH_ESCAPES,
    );
    let statement = crate::parser::parse_statement(r"CREATE AI SECRET `if` VALUE 'a\b'").unwrap();
    let Statement::Ai(command) = statement else {
        panic!()
    };
    assert_eq!(command.name, "if");
    assert_eq!(
        command.value,
        Some(Expr::Literal(Literal::String(r"a\b".into())))
    );
    drop(_mode);
    with_mock(|_, _, engine, session| {
        exec(engine, session, "CREATE DATABASE `业务.db`");
        exec(
            engine,
            session,
            r#"CREATE AI MODEL `业务.db`.`if` USING local WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1","defaults":{"max_output_tokens":32}}'"#,
        );
        let rows = materialize(exec(
            engine,
            session,
            "SELECT AI_GENERATE('业务.db.if','中文与 UTF-8 😀')",
        ));
        assert_eq!(rows.rows.len(), 1);
        let rows=materialize(exec(engine,session,"SELECT MODEL_SCHEMA,MODEL_NAME FROM information_schema.AI_MODELS WHERE MODEL_SCHEMA='业务.db'"));
        assert_eq!(
            rows.rows,
            vec![vec![
                Value::String("业务.db".into()),
                Value::String("if".into())
            ]]
        );
        assert!(engine
            .execute_with_session(session, "DROP DATABASE `业务.db`")
            .is_err());
        exec(engine, session, "DROP AI MODEL `业务.db`.`if`");
        exec(engine, session, "DROP DATABASE `业务.db`");
    });
}

#[path = "ai_regression.rs"]
mod regression;
