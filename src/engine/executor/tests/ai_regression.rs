use super::*;
use crate::error::catalog::*;
use std::sync::{atomic::AtomicUsize, Condvar};

fn reply(status: u16, body: impl AsRef<[u8]>, headers: &[(&str, String)]) -> Vec<u8> {
    let body = body.as_ref();
    let mut bytes = format!("HTTP/1.1 {status} test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n", body.len()).into_bytes();
    for (name, value) in headers {
        bytes.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    bytes.extend_from_slice(b"\r\n");
    bytes.extend_from_slice(body);
    bytes
}
fn answer(text: &str) -> Vec<u8> {
    reply(200, json!({"model":"mock","done":true,"done_reason":"stop","message":{"role":"assistant","content":text}}).to_string(), &[])
}
fn json_result(response: QueryResponse) -> Json {
    let rows = materialize(response);
    let Value::Json(bytes) = &rows.rows[0][0] else {
        panic!("expected JSON result")
    };
    crate::engine::json::to_serde(bytes).unwrap()
}
fn assert_error(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
    expected: crate::error::ErrorSpec,
) {
    let result = engine.execute_with_session(session, sql);
    let Err(error) = result else {
        panic!("{sql}: expected {} but succeeded", expected.symbol())
    };
    assert!(
        error.is(expected),
        "{sql}: {error:?}; expected {}",
        expected.symbol()
    );
}
fn assert_idle() {
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while crate::engine::ai::admission::memory_used() != 0
        || crate::engine::ai::runtime::status()
            .into_iter()
            .find(|(key, _)| *key == "queued_and_running")
            .unwrap()
            .1
            != 0
    {
        if std::time::Instant::now() >= until {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(crate::engine::ai::admission::memory_used(), 0);
    assert_eq!(
        crate::engine::ai::runtime::status()
            .into_iter()
            .find(|(key, _)| *key == "queued_and_running")
            .unwrap()
            .1,
        0
    );
}
fn set_user_policy(
    mock: &Mock,
    dir: &std::path::Path,
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    limits: Json,
) {
    let origin = format!("http://127.0.0.1:{}", mock.port);
    let mut limit = limits;
    limit["user"] = json!(session.authenticated_as.user);
    limit["host"] = json!(session.authenticated_as.host);
    std::fs::write(dir.join("policy.json"),json!({"version":1,"user_limits":[limit],"egress":{"allowed_origins":[origin],"private_destinations":["127.0.0.1/32"],"allow_plaintext_origins":[origin]}}).to_string()).unwrap();
    exec(engine, session, "ALTER SYSTEM RELOAD AI CONFIG");
}

#[test]
fn ai_invalid_arity_and_nested_calls_never_dispatch() {
    with_mock(|mock, _, engine, session| {
        for function in [
            "AI_GENERATE",
            "AI_GENERATE_DETAIL",
            "AI_EMBED",
            "AI_EMBED_DETAIL",
        ] {
            for args in ["", "'test.chat'", "'test.chat','x','{}','extra'"] {
                assert_error(
                    engine,
                    session,
                    &format!("SELECT {function}({args})"),
                    ER_AI_UNSUPPORTED_CONTEXT,
                );
            }
        }
        for function in [
            "AI_GENERATE",
            "AI_GENERATE_DETAIL",
            "AI_EMBED",
            "AI_EMBED_DETAIL",
        ] {
            assert_error(
                engine,
                session,
                &format!("SELECT {function}(DISTINCT 'test.chat','x')"),
                ER_PARSE_ERROR,
            );
        }
        for sql in [
            "SELECT AI_GENERATE('test.chat',AI_GENERATE('test.chat','nested'))",
            "SELECT AI_GENERATE(CONCAT('test.','chat'),'x')",
            "SELECT AI_EMBED('test.embed',AI_GENERATE('test.chat','nested'))",
        ] {
            assert_error(engine, session, sql, ER_AI_UNSUPPORTED_CONTEXT);
        }
        assert!(mock.requests.lock().unwrap().is_empty());
        assert_idle();
    });
}

#[test]
fn ai_relational_and_persistent_contexts_fail_before_side_effects() {
    with_mock(|mock, _, engine, session| {
        exec(
            engine,
            session,
            "CREATE TABLE ai_context_docs(id INT PRIMARY KEY,body TEXT)",
        );
        exec(
            engine,
            session,
            "INSERT INTO ai_context_docs VALUES(1,'one'),(2,'two')",
        );
        for sql in [
            "SELECT id FROM ai_context_docs WHERE 0 AND AI_GENERATE('test.chat',body)='yes'",
            "SELECT SUM(AI_GENERATE('test.chat',body)) FROM ai_context_docs",
            "SELECT AI_GENERATE('test.chat',body) FROM ai_context_docs GROUP BY body",
            "SELECT AI_GENERATE('test.chat',body) FROM ai_context_docs HAVING 1",
            "SELECT id FROM ai_context_docs ORDER BY AI_GENERATE('test.chat',body)",
            "SELECT AI_GENERATE('test.chat','x') UNION ALL SELECT 'other'",
            "WITH bad AS (SELECT AI_GENERATE('test.chat','x') AS text) SELECT text FROM bad",
            "SELECT id FROM ai_context_docs JOIN (SELECT AI_GENERATE('test.chat','x') AS text) AS bad ON 1",
            "INSERT INTO ai_context_docs VALUES(3,AI_GENERATE('test.chat','x'))",
            "INSERT INTO ai_context_docs SELECT 3,AI_GENERATE('test.chat','x')",
            "UPDATE ai_context_docs SET body=AI_GENERATE('test.chat',body) WHERE 0",
            "DELETE FROM ai_context_docs WHERE AI_GENERATE('test.chat',body)='yes'",
            "CREATE TABLE ai_forbidden AS SELECT AI_GENERATE('test.chat','x') AS text",
            "CREATE VIEW ai_forbidden_view AS SELECT AI_GENERATE('test.chat','x') AS text",
            "SET @a=AI_GENERATE('test.chat','x'), @b=1",
        ] { assert_error(engine,session,sql,ER_AI_UNSUPPORTED_CONTEXT); }
        let rows = materialize(exec(
            engine,
            session,
            "SELECT id,body FROM ai_context_docs ORDER BY id",
        ));
        assert_eq!(
            rows.rows,
            vec![
                vec![Value::Int(1), Value::String("one".into())],
                vec![Value::Int(2), Value::String("two".into())]
            ]
        );
        assert!(mock.requests.lock().unwrap().is_empty());
        assert_idle();
    });
}

#[test]
fn ai_explicit_transaction_and_autocommit_guards_preserve_session() {
    with_mock(|mock, _, engine, session| {
        for start in ["START TRANSACTION", "SET autocommit=0"] {
            exec(engine, session, start);
            assert_error(
                engine,
                session,
                "SELECT AI_GENERATE_DETAIL('test.chat','x',JSON_OBJECT('on_error','return'))",
                ER_AI_UNSUPPORTED_CONTEXT,
            );
            exec(engine, session, "ROLLBACK");
            exec(engine, session, "SET autocommit=1");
        }
        assert!(mock.requests.lock().unwrap().is_empty());
        exec(
            engine,
            session,
            "SELECT AI_GENERATE('test.chat','after transaction')",
        );
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
        assert_idle();
    });
}

#[test]
fn ai_dynamic_branches_preserve_occurrences_and_skip_unreached_inputs() {
    with_mock(|mock, _, engine, session| {
        exec(
            engine,
            session,
            "CREATE TABLE ai_branches(id INT PRIMARY KEY,body TEXT)",
        );
        exec(
            engine,
            session,
            "INSERT INTO ai_branches VALUES(1,'one'),(2,'two'),(3,'three')",
        );
        let rows=materialize(exec(engine,session,"SELECT id,CASE WHEN id=2 THEN AI_GENERATE('test.chat',body) ELSE 'skip' END,IFNULL('keep',AI_GENERATE('test.chat',body)),COALESCE('keep',AI_GENERATE('test.chat',body)) FROM ai_branches ORDER BY id"));
        assert_eq!(rows.rows.len(), 3);
        assert_eq!(rows.rows[1][1], Value::String("mock generated text".into()));
        assert_eq!(rows.rows[0][1], Value::String("skip".into()));
        let requests = mock.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["messages"][0]["content"], "two");
    });
}

#[test]
fn ai_equal_call_positions_and_prepared_reexecution_are_not_cached() {
    let counter = Arc::new(AtomicUsize::new(0));
    let n = counter.clone();
    let mock =
        Mock::responding(move |_, _| answer(&format!("call-{}", n.fetch_add(1, Ordering::SeqCst))));
    with_provider(&mock, |mock, _, engine, session| {
        let sql = "SELECT AI_GENERATE('test.chat',?),AI_GENERATE('test.chat',?)";
        let template =
            <StorageQueryEngine as QueryEngine>::prepare_cached(engine, session, sql, 2).unwrap();
        assert!(mock.requests.lock().unwrap().is_empty());
        let mut texts = std::collections::BTreeSet::new();
        for _ in 0..2 {
            let result =
                crate::engine::params::with_params(vec![Value::String("same".into()); 2], || {
                    <StorageQueryEngine as QueryEngine>::execute_prepared_statement(
                        engine,
                        session,
                        sql,
                        (*template).clone(),
                    )
                })
                .unwrap()
                .unwrap();
            for v in materialize(result).rows.remove(0) {
                texts.insert(v.to_display_string());
            }
        }
        assert_eq!(texts.len(), 4);
        assert_eq!(mock.requests.lock().unwrap().len(), 4);
        assert_idle();
    });
}

#[test]
fn ai_validation_failures_are_not_caught_by_detail_return_mode() {
    with_mock(|mock, _, engine, session| {
        for options in [
            r#"{"on_error":"return","timeout_ms":null}"#,
            r#"{"on_error":"return","timeout_ms":0}"#,
            r#"{"on_error":"return","timeout_ms":60001}"#,
            r#"{"on_error":"return","max_retries":1}"#,
            r#"{"on_error":"return","retry_ambiguous":true}"#,
            r#"{"on_error":"return","temperature":0.2}"#,
            r#"{"on_error":"return","unknown":1}"#,
            r#"{"on_error":"return","timeout_ms":1,"timeout_ms":2}"#,
            "null",
            "[]",
        ] {
            assert_error(
                engine,
                session,
                &format!("SELECT AI_GENERATE_DETAIL('test.chat','x','{options}')"),
                ER_AI_INVALID_ARGUMENT,
            );
        }
        assert_error(
            engine,
            session,
            "SELECT AI_GENERATE('test.chat','x',JSON_OBJECT('on_error','return'))",
            ER_AI_INVALID_ARGUMENT,
        );
        assert_error(
            engine,
            session,
            "SELECT AI_GENERATE_DETAIL('test.chat',123,JSON_OBJECT('on_error','return'))",
            ER_AI_INVALID_ARGUMENT,
        );
        assert_error(engine,session,"SELECT AI_EMBED_DETAIL('test.embed',JSON_ARRAY('ok',''),JSON_OBJECT('on_error','return'))",ER_AI_INVALID_ARGUMENT);
        assert!(mock.requests.lock().unwrap().is_empty());
        assert_idle();
    });
}

#[test]
fn ai_row_and_logical_item_budgets_reject_before_network_dispatch() {
    with_mock(|mock, dir, engine, session| {
        exec(
            engine,
            session,
            "CREATE TABLE ai_budget_docs(id INT PRIMARY KEY,body TEXT)",
        );
        let values = (0..101)
            .map(|i| format!("({i},'text')"))
            .collect::<Vec<_>>()
            .join(",");
        exec(
            engine,
            session,
            &format!("INSERT INTO ai_budget_docs VALUES{values}"),
        );
        assert_error(
            engine,
            session,
            "SELECT AI_GENERATE('test.chat',body) FROM ai_budget_docs",
            ER_AI_BUDGET_EXCEEDED,
        );
        assert!(mock.requests.lock().unwrap().is_empty());
        assert_idle();
        set_user_policy(
            mock,
            dir,
            engine,
            session,
            json!({"max_call_items_per_statement":2}),
        );
        assert_error(
            engine,
            session,
            "SELECT AI_GENERATE('test.chat',body) FROM ai_budget_docs LIMIT 3",
            ER_AI_BUDGET_EXCEEDED,
        );
        assert!(mock.requests.lock().unwrap().is_empty());
        assert_idle();
        exec(
            engine,
            session,
            "SELECT AI_GENERATE('test.chat',body) FROM ai_budget_docs ORDER BY id LIMIT 2",
        );
        assert_eq!(mock.requests.lock().unwrap().len(), 2);
        assert_idle();
    });
}

#[test]
fn ai_http_attempt_budget_cannot_be_bypassed_by_retry_or_detail() {
    with_mock(|mock, dir, engine, session| {
        exec(
            engine,
            session,
            r#"ALTER AI MODEL test.chat WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1","capabilities":{"messages":true,"json_schema":true},"defaults":{"max_output_tokens":32},"limits":{"max_retries":2,"allow_retry_ambiguous":true}}'"#,
        );
        set_user_policy(
            mock,
            dir,
            engine,
            session,
            json!({"max_http_attempts_per_statement":1}),
        );
        assert_error(engine,session,"SELECT AI_GENERATE_DETAIL('test.chat','__503__',JSON_OBJECT('max_retries',2,'retry_ambiguous',TRUE,'on_error','return'))",ER_AI_BUDGET_EXCEEDED);
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
        assert_idle();
    });
}

#[test]
fn ai_memory_rejection_releases_reservations_and_allows_next_query() {
    let mock = Mock::responding(|_, request| {
        if request["messages"][0]["content"] == "dense" {
            reply(200, json!({"extra":vec![0;10000]}).to_string(), &[])
        } else if request["messages"][0]["content"] == "structured" {
            answer(&json!({"data":vec![0; 20000]}).to_string())
        } else {
            answer("healthy")
        }
    });
    with_provider(&mock, |mock, _, engine, session| {
        crate::util::env::with_vars(&[("MYDB_AI_MEMORY_LIMIT_MB", Some("1"))], || {
            assert_error(
                engine,
                session,
                "SELECT AI_GENERATE_DETAIL('test.chat','dense',JSON_OBJECT('on_error','return'))",
                ER_AI_BUDGET_EXCEEDED,
            );
            assert_idle();
            assert_error(
                engine,
                session,
                r#"SELECT AI_GENERATE_DETAIL('test.chat','structured','{"response_format":{"type":"json_schema","name":"array","strict":true,"schema":{"type":"object","properties":{"data":{"type":"array","items":{"type":"integer"}}},"required":["data"],"additionalProperties":false}},"on_error":"return"}')"#,
                ER_AI_BUDGET_EXCEEDED,
            );
            assert_idle();
            exec(
                engine,
                session,
                "SELECT AI_GENERATE('test.chat','after budget rejection')",
            );
        });
        assert_eq!(mock.requests.lock().unwrap().len(), 3);
        assert_idle();
    });
}

#[test]
fn ai_failed_model_changes_leave_identity_and_catalog_unchanged() {
    with_mock(|mock, _, engine, session| {
        let before = engine.db.ai_catalog().unwrap();
        for patch in [
            json!({"remote_model":"other"}),
            json!({"model_revision":"v2"}),
            json!({"capabilities":{"temperature":true}}),
            json!({"defaults":{"unknown":1}}),
            json!({"id":999}),
            json!({"limits":{"max_input_tokens":10}}),
        ] {
            let mut config = json!({"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1","capabilities":{"messages":true,"json_schema":true},"defaults":{"max_output_tokens":32}});
            for (k, v) in patch.as_object().unwrap() {
                config[k] = v.clone();
            }
            assert_error(
                engine,
                session,
                &format!("ALTER AI MODEL test.chat WITH CONFIG '{config}'"),
                ER_AI_INVALID_ARGUMENT,
            );
            assert!(Arc::ptr_eq(&before, &engine.db.ai_catalog().unwrap()));
        }
        assert!(mock.requests.lock().unwrap().is_empty());
        exec(
            engine,
            session,
            "SELECT AI_GENERATE('test.chat','original model')",
        );
        assert_eq!(mock.requests.lock().unwrap()[0]["model"], "mock-chat");
    });
}

#[cfg(unix)]
#[test]
fn ai_secret_connection_dependencies_prevent_orphaned_metadata() {
    with_mock(|mock, _, engine, session| {
        exec(
            engine,
            session,
            "CREATE AI SECRET key_dep VALUE 'dummy-dependency-key'",
        );
        let origin = format!("http://127.0.0.1:{}", mock.port);
        exec(
            engine,
            session,
            &format!(
                "ALTER AI CONNECTION local WITH CONFIG '{}'",
                json!({"protocol":"ollama","base_url":origin,"auth":{"type":"bearer","secret":"key_dep"}})
            ),
        );
        let before = engine.db.ai_catalog().unwrap();
        assert_error(
            engine,
            session,
            "DROP AI SECRET key_dep",
            ER_AI_INVALID_ARGUMENT,
        );
        assert_error(
            engine,
            session,
            "DROP AI CONNECTION local",
            ER_AI_INVALID_ARGUMENT,
        );
        assert!(Arc::ptr_eq(&before, &engine.db.ai_catalog().unwrap()));
        exec(engine, session, "DROP AI MODEL test.chat");
        exec(engine, session, "DROP AI MODEL test.embed");
        exec(engine, session, "DROP AI CONNECTION local");
        exec(engine, session, "DROP AI SECRET key_dep");
        let after = engine.db.ai_catalog().unwrap();
        assert!(
            after.models.is_empty() && after.connections.is_empty() && after.secrets.is_empty()
        );
        assert!(mock.requests.lock().unwrap().is_empty());
    });
}

#[test]
fn ai_catalog_schema_filters_do_not_match_dotted_schema_prefixes() {
    with_mock(|_, _, engine, session| {
        exec(engine, session, "CREATE DATABASE `test.extra`");
        exec(
            engine,
            session,
            r#"CREATE AI MODEL `test.extra`.hidden USING local WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1"}'"#,
        );
        let rows = materialize(exec(engine, session, "SHOW AI MODELS FROM test"));
        let names: Vec<_> = rows
            .rows
            .iter()
            .map(|row| row[0].to_display_string())
            .collect();
        assert_eq!(names, vec!["test.chat", "test.embed"]);
        exec(
            engine,
            session,
            "CREATE USER 'schema_reader'@'localhost' IDENTIFIED BY 'test-only'",
        );
        exec(
            engine,
            session,
            "GRANT AI_USE ON *.* TO 'schema_reader'@'localhost'",
        );
        let mut reader = SessionContext::default();
        reader.authenticated_as =
            crate::engine::auth::AccountKey::new("schema_reader", "localhost");
        reader.user = "schema_reader".into();
        reader.login_user = "schema_reader".into();
        reader.current_user_is_system = false;
        let visible = materialize(exec(engine, &mut reader, "SHOW AI MODELS FROM test"));
        assert_eq!(
            visible.rows.len(),
            2,
            "schema filters must be exact for ordinary model users too"
        );

        let rows = materialize(exec(engine, session, "SHOW AI MODELS FROM `test.extra`"));
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][0], Value::String("test.extra.hidden".into()));
    });
}

#[test]
fn ai_model_drop_after_execute_prevents_deferred_http_dispatch() {
    with_mock(|mock, _, engine, session| {
        let response = <StorageQueryEngine as QueryEngine>::execute(
            engine,
            session,
            "SELECT AI_GENERATE('test.chat','must never dispatch')",
        )
        .unwrap();
        exec(engine, session, "DROP AI MODEL test.chat");
        let error = response.materialize().unwrap_err();
        assert!(error.is(ER_AI_DISABLED), "{error:?}");
        assert!(mock.requests.lock().unwrap().is_empty());
        assert_idle();
    });
}

#[test]
fn ai_redirects_never_contact_an_unapproved_destination() {
    let target = Mock::responding(|_, _| answer("must not be contacted"));
    let destination = format!("http://127.0.0.1:{}/private", target.port);
    let mock =
        Mock::responding(move |_, _| reply(302, b"{}", &[("Location", destination.clone())]));
    with_provider(&mock, |mock, _, engine, session| {
        let result = json_result(exec(
            engine,
            session,
            "SELECT AI_GENERATE_DETAIL('test.chat','x',JSON_OBJECT('on_error','return'))",
        ));
        assert_eq!(result["ok"], false);
        assert_eq!(result["error"]["code"], "ER_AI_UPSTREAM_ERROR");
        assert_eq!(result["error"]["http_status"], 302);
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
        assert!(target.requests.lock().unwrap().is_empty());
        assert_idle();
    });
}

#[test]
fn ai_malformed_http_responses_are_bounded_redacted_and_not_retried() {
    let mock =
        Mock::responding(
            |_, request| match request["messages"][0]["content"].as_str().unwrap() {
                "json" => reply(200, b"not-json-dummy-sensitive-body", &[]),
                "duplicates" => reply(
                    200,
                    br#"{"done":true,"done":false,"sensitive":"dummy-sensitive-body"}"#,
                    &[],
                ),
                "utf8" => reply(200, [255, 254, 253], &[]),
                "oversized" => reply(200, vec![b'x'; 80 * 1024], &[]),
                "truncated" => {
                    b"HTTP/1.1 200 test\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{}"
                        .to_vec()
                }
                _ => answer("healthy"),
            },
        );
    with_provider(&mock, |mock, _, engine, session| {
        for prompt in ["json", "duplicates", "utf8", "oversized", "truncated"] {
            let value=json_result(exec(engine,session,&format!("SELECT AI_GENERATE_DETAIL('test.chat','{prompt}',JSON_OBJECT('on_error','return'))")));
            assert_eq!(value["ok"], false, "{prompt}: {value}");
            assert_eq!(value["error"]["code"], "ER_AI_INVALID_RESPONSE");
            assert_eq!(value["attempts"], 1);
            assert!(!value.to_string().contains("dummy-sensitive-body"));
            assert_idle();
        }
        exec(engine, session, "SELECT AI_GENERATE('test.chat','healthy')");
        assert_eq!(mock.requests.lock().unwrap().len(), 6);
    });
}

#[test]
fn ai_five_transient_failures_open_circuit_until_connection_changes() {
    let failures = Arc::new(AtomicBool::new(true));
    let mode = failures.clone();
    let mock = Mock::responding(move |_, _| {
        if mode.load(Ordering::Acquire) {
            reply(503, b"{}", &[])
        } else {
            answer("recovered")
        }
    });
    with_provider(&mock, |mock, _, engine, session| {
        for _ in 0..5 {
            let value = json_result(exec(
                engine,
                session,
                "SELECT AI_GENERATE_DETAIL('test.chat','x',JSON_OBJECT('on_error','return'))",
            ));
            assert_eq!(value["error"]["http_status"], 503);
        }
        failures.store(false, Ordering::Release);
        assert_error(
            engine,
            session,
            "SELECT AI_GENERATE_DETAIL('test.chat','x',JSON_OBJECT('on_error','return'))",
            ER_AI_CIRCUIT_OPEN,
        );
        assert_eq!(mock.requests.lock().unwrap().len(), 5);
        let origin = format!("http://127.0.0.1:{}", mock.port);
        exec(
            engine,
            session,
            &format!(
                "ALTER AI CONNECTION local WITH CONFIG '{}'",
                json!({"protocol":"ollama","base_url":origin,"auth":{"type":"none"}})
            ),
        );
        let rows = materialize(exec(engine, session, "SELECT AI_GENERATE('test.chat','x')"));
        assert_eq!(rows.rows[0][0], Value::String("recovered".into()));
        assert_eq!(mock.requests.lock().unwrap().len(), 6);
        assert_idle();
    });
}

#[test]
fn ai_rate_limit_responses_do_not_open_a_transport_circuit() {
    let mock = Mock::responding(|_, request| {
        if request["messages"][0]["content"] == "limited" {
            reply(429, b"{}", &[("Retry-After", "0".into())])
        } else {
            answer("healthy")
        }
    });
    with_provider(&mock, |mock, _, engine, session| {
        for _ in 0..6 {
            let value = json_result(exec(
                engine,
                session,
                "SELECT AI_GENERATE_DETAIL('test.chat','limited',JSON_OBJECT('on_error','return'))",
            ));
            assert_eq!(value["error"]["http_status"], 429);
        }
        exec(engine, session, "SELECT AI_GENERATE('test.chat','healthy')");
        assert_eq!(mock.requests.lock().unwrap().len(), 7);
        assert_idle();
    });
}

#[test]
fn ai_authentication_failure_is_latched_until_connection_changes() {
    let denied = Arc::new(AtomicBool::new(true));
    let state = denied.clone();
    let mock = Mock::responding(move |_, _| {
        if state.load(Ordering::Acquire) {
            reply(401, b"{}", &[])
        } else {
            answer("healthy")
        }
    });
    with_provider(&mock, |mock, _, engine, session| {
        for _ in 0..2 {
            let value = json_result(exec(
                engine,
                session,
                "SELECT AI_GENERATE_DETAIL('test.chat','x',JSON_OBJECT('on_error','return'))",
            ));
            assert_eq!(value["error"]["code"], "ER_AI_UPSTREAM_AUTH");
        }
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
        denied.store(false, Ordering::Release);
        let origin = format!("http://127.0.0.1:{}", mock.port);
        exec(
            engine,
            session,
            &format!(
                "ALTER AI CONNECTION local WITH CONFIG '{}'",
                json!({"protocol":"ollama","base_url":origin,"auth":{"type":"none"}})
            ),
        );
        exec(engine, session, "SELECT AI_GENERATE('test.chat','x')");
        assert_eq!(mock.requests.lock().unwrap().len(), 2);
        assert_idle();
    });
}

#[cfg(unix)]
#[test]
fn ai_header_configuration_rejects_case_collisions_and_framing_headers() {
    with_mock(|mock, _, engine, session| {
        exec(
            engine,
            session,
            "CREATE AI SECRET header_key VALUE 'dummy-header-key'",
        );
        let origin = format!("http://127.0.0.1:{}", mock.port);
        for headers in [
            json!({"X-Trace":"a","x-trace":"b"}),
            json!({"Transfer-Encoding":"chunked"}),
            json!({"Connection":"close"}),
            json!({"Content-Type":"text/plain"}),
        ] {
            let config = json!({"protocol":"ollama","base_url":origin,"auth":{"type":"none"},"headers":headers});
            assert_error(
                engine,
                session,
                &format!("CREATE AI CONNECTION conflicting WITH CONFIG '{config}'"),
                ER_AI_INVALID_ARGUMENT,
            );
        }
        let config = json!({"protocol":"ollama","base_url":origin,"auth":{"type":"header","secret":"header_key","header_name":"X-Token"},"headers":{"x-token":"ambiguous"}});
        assert_error(
            engine,
            session,
            &format!("CREATE AI CONNECTION conflicting WITH CONFIG '{config}'"),
            ER_AI_INVALID_ARGUMENT,
        );
        for name in [
            "Transfer-Encoding",
            "Connection",
            "Content-Type",
            "Proxy-Authorization",
        ] {
            let config = json!({"protocol":"ollama","base_url":origin,"auth":{"type":"header","secret":"header_key","header_name":name}});
            assert_error(
                engine,
                session,
                &format!("CREATE AI CONNECTION conflicting WITH CONFIG '{config}'"),
                ER_AI_INVALID_ARGUMENT,
            );
        }
        assert!(!engine
            .db
            .ai_catalog()
            .unwrap()
            .connections
            .contains_key("conflicting"));
        assert!(mock.requests.lock().unwrap().is_empty());
        let config = json!({"protocol":"ollama","base_url":origin,"auth":{"type":"header","secret":"header_key","header_name":"X-Token"},"headers":{"X-Trace":"valid"}});
        exec(
            engine,
            session,
            &format!("CREATE AI CONNECTION valid_headers WITH CONFIG '{config}'"),
        );
        exec(
            engine,
            session,
            r#"CREATE AI MODEL test.valid_headers USING valid_headers WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1"}'"#,
        );
        exec(
            engine,
            session,
            "SELECT AI_GENERATE('test.valid_headers','x')",
        );
        let requests = mock.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0]["_test_headers"]["x-token"],
            json!(["dummy-header-key"])
        );
        assert_eq!(requests[0]["_test_headers"]["x-trace"], json!(["valid"]));
        assert_eq!(
            requests[0]["_test_headers"]["content-type"],
            json!(["application/json"])
        );
    });
}

#[test]
fn ai_openai_batch_reorders_indexes_without_deduplicating_occurrences() {
    let mock = Mock::responding(|path, request| {
        if path != "/v1/embeddings" {
            return reply(404, b"{}", &[]);
        }
        let count = request["input"].as_array().map_or(1, Vec::len);
        reply(200,json!({"model":"remote-embed","data":(0..count).rev().map(|index|json!({"index":index,"embedding":[(index+1) as f32,0.5,-0.25]})).collect::<Vec<_>>(),"usage":{"prompt_tokens":count,"total_tokens":count}}).to_string(),&[])
    });
    with_provider(&mock, |mock, _, engine, session| {
        let config = json!({"protocol":"openai_embeddings","base_url":format!("http://127.0.0.1:{}/v1",mock.port),"auth":{"type":"none"}});
        exec(
            engine,
            session,
            &format!("CREATE AI CONNECTION indexed WITH CONFIG '{config}'"),
        );
        exec(
            engine,
            session,
            r#"CREATE AI MODEL test.indexed USING indexed WITH CONFIG '{"kind":"embedding","remote_model":"remote-embed","model_revision":"v1","embedding":{"dimensions":3}}'"#,
        );
        exec(
            engine,
            session,
            "CREATE TABLE ai_indexed_inputs(id INT PRIMARY KEY,body TEXT)",
        );
        exec(
            engine,
            session,
            "INSERT INTO ai_indexed_inputs VALUES(1,'same'),(2,'other'),(3,'same')",
        );
        let rows = materialize(exec(
            engine,
            session,
            "SELECT id,AI_EMBED('test.indexed',body) FROM ai_indexed_inputs ORDER BY id",
        ));
        for (index, row) in rows.rows.iter().enumerate() {
            assert_eq!(row[0], Value::Int((index + 1) as i64));
            let Value::Vector(vector) = &row[1] else {
                panic!("expected native vector")
            };
            assert_eq!(vector.element(0), (index + 1) as f64);
            assert_eq!(vector.dimension, 3);
        }
        let requests = mock.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["_test_path"], "/v1/embeddings");
        assert_eq!(requests[0]["encoding_format"], "float");
        assert_eq!(requests[0]["input"], json!(["same", "other", "same"]));
    });
}

#[test]
fn ai_openai_text_adapters_execute_their_exact_http_contracts() {
    let mock = Mock::responding(|path, _| {
        match path {
            "/v1/chat/completions" => {

                reply(200,json!({"model":"chat-deploy","choices":[{"index":0,"message":{"role":"assistant","content":"chat answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}).to_string(),&[])
            }
            "/v1/responses" => {

                reply(200,json!({"model":"responses-deploy","status":"completed","output":[{"type":"reasoning","summary":[]},{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"first "},{"type":"output_text","text":"second"}]}],"usage":{"input_tokens":5,"output_tokens":3,"total_tokens":8}}).to_string(),&[])
            }
            _ => reply(404,b"{}",&[]),
        }
    });
    with_provider(&mock, |mock, _, engine, session| {
        for (protocol, expected, total) in [
            ("openai_chat", "chat answer", 6),
            ("openai_responses", "first second", 8),
        ] {
            let config = json!({"protocol":protocol,"base_url":format!("http://127.0.0.1:{}/v1",mock.port),"auth":{"type":"none"}});
            exec(
                engine,
                session,
                &format!("CREATE AI CONNECTION {protocol} WITH CONFIG '{config}'"),
            );
            exec(engine,session,&format!("CREATE AI MODEL test.{protocol} USING {protocol} WITH CONFIG '{{\"kind\":\"text_generation\",\"remote_model\":\"deploy\",\"model_revision\":\"v1\",\"defaults\":{{\"max_output_tokens\":32}}}}'"));
            let value = json_result(exec(
                engine,
                session,
                &format!("SELECT AI_GENERATE_DETAIL('test.{protocol}','x')"),
            ));
            assert_eq!(value["text"], expected);
            assert_eq!(value["usage"]["total_tokens"], total);
            assert_eq!(value["ok"], true);
        }
        let requests = mock.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["stream"], false);
        assert_eq!(requests[1]["stream"], false);
        assert_eq!(requests[0]["max_completion_tokens"], 32);
        assert!(requests[0].get("max_output_tokens").is_none());
        assert_eq!(requests[1]["max_output_tokens"], 32);
        assert_eq!(requests[1]["store"], false);
        assert_idle();
    });
}

#[derive(Default)]
struct ResponseGate {
    permits: Mutex<usize>,
    changed: Condvar,
}
impl ResponseGate {
    fn release(&self) {
        *self.permits.lock().unwrap() += 1;
        self.changed.notify_one();
    }
    fn wait(&self) {
        let (mut permits, timeout) = self
            .changed
            .wait_timeout_while(self.permits.lock().unwrap(), Duration::from_secs(3), |n| {
                *n == 0
            })
            .unwrap();
        if !timeout.timed_out() {
            *permits -= 1;
        }
    }
}
#[test]
fn ai_model_concurrency_limit_queues_without_holding_source_locks() {
    let gate = Arc::new(ResponseGate::default());
    let server_gate = gate.clone();
    let active = Arc::new(AtomicUsize::new(0));
    let running = active.clone();
    let peak = Arc::new(AtomicUsize::new(0));
    let maximum = peak.clone();
    let mock = Mock::responding(move |_, _| {
        let count = running.fetch_add(1, Ordering::AcqRel) + 1;
        maximum.fetch_max(count, Ordering::AcqRel);
        server_gate.wait();
        running.fetch_sub(1, Ordering::AcqRel);
        answer("released")
    });
    with_provider(&mock, |mock, _, engine, session| {
        exec(
            engine,
            session,
            r#"ALTER AI MODEL test.chat WITH CONFIG '{"kind":"text_generation","remote_model":"mock-chat","model_revision":"v1","defaults":{"max_output_tokens":32},"limits":{"max_inflight":1}}'"#,
        );
        exec(
            engine,
            session,
            "CREATE TABLE ai_concurrency_docs(id INT PRIMARY KEY,body TEXT)",
        );
        exec(
            engine,
            session,
            "INSERT INTO ai_concurrency_docs VALUES(1,'first'),(2,'second')",
        );
        exec(engine, session, "SET SESSION lock_wait_timeout=1");
        let first = <StorageQueryEngine as QueryEngine>::execute(
            engine,
            session,
            "SELECT AI_GENERATE('test.chat',body) FROM ai_concurrency_docs WHERE id=1",
        )
        .unwrap();
        let second = <StorageQueryEngine as QueryEngine>::execute(
            engine,
            session,
            "SELECT AI_GENERATE('test.chat',body) FROM ai_concurrency_docs WHERE id=2",
        )
        .unwrap();
        let a = thread::spawn(move || first.materialize());
        let b = thread::spawn(move || second.materialize());
        let until = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let pending = crate::engine::ai::runtime::status()
                .into_iter()
                .find(|(key, _)| *key == "queued_and_running")
                .unwrap()
                .1;
            if pending == 2 && mock.requests.lock().unwrap().len() == 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < until,
                "requests never reached running + queued state"
            );
            thread::sleep(Duration::from_millis(1));
        }
        exec(engine, session, "DROP TABLE ai_concurrency_docs");
        assert_eq!(peak.load(Ordering::Acquire), 1);
        gate.release();
        let until = std::time::Instant::now() + Duration::from_secs(2);
        while mock.requests.lock().unwrap().len() != 2 {
            assert!(
                std::time::Instant::now() < until,
                "queued request did not acquire released permit"
            );
            thread::sleep(Duration::from_millis(1));
        }
        gate.release();
        assert!(a.join().unwrap().is_ok());
        assert!(b.join().unwrap().is_ok());
        assert_eq!(peak.load(Ordering::Acquire), 1);
        assert_idle();
    });
}

#[cfg(unix)]
#[test]
fn ai_secret_tls_and_prepared_value_rules_fail_before_persistence() {
    with_mock(|mock, _, engine, session| {
        session.ssl_cipher = None;
        assert_error(
            engine,
            session,
            "CREATE AI SECRET tls_key VALUE 'dummy-tls-key'",
            ER_AI_ACCESS_DENIED,
        );
        let result = <StorageQueryEngine as QueryEngine>::prepare_cached(
            engine,
            session,
            "CREATE AI SECRET tls_key VALUE ?",
            1,
        );
        assert!(result.err().unwrap().is(ER_AI_ACCESS_DENIED));
        assert!(engine.db.ai_catalog().unwrap().secrets.is_empty());
        assert!(!engine.db.data_dir().join("apikey.mydb").exists());
        session.ssl_cipher = Some("test-TLS".into());
        let result = <StorageQueryEngine as QueryEngine>::prepare_cached(
            engine,
            session,
            "CREATE AI SECRET tls_key VALUE 'must-not-be-a-prepared-literal'",
            0,
        );
        assert!(result.err().unwrap().is(ER_AI_INVALID_ARGUMENT));
        let sql = "CREATE AI SECRET tls_key VALUE ?";
        let statement =
            <StorageQueryEngine as QueryEngine>::prepare_cached(engine, session, sql, 1).unwrap();
        let result = crate::engine::params::with_params(
            vec![Value::String("dummy-prepared-secret".into())],
            || {
                <StorageQueryEngine as QueryEngine>::execute_prepared_statement(
                    engine,
                    session,
                    sql,
                    (*statement).clone(),
                )
            },
        )
        .unwrap()
        .unwrap();
        result.materialize().unwrap();
        let catalog = engine.db.ai_catalog().unwrap();
        assert_eq!(catalog.secrets.len(), 1);
        assert!(!serde_json::to_string(&*catalog)
            .unwrap()
            .contains("dummy-prepared-secret"));
        assert!(mock.requests.lock().unwrap().is_empty());
    });
}

#[test]
fn ai_rate_limited_queue_timeout_releases_permits_for_next_policy() {
    with_mock(|mock, dir, engine, session| {
        set_user_policy(mock, dir, engine, session, json!({"requests_per_minute":1}));
        exec(engine, session, "SELECT AI_GENERATE('test.chat','first')");
        let result=json_result(exec(engine,session,"SELECT AI_GENERATE_DETAIL('test.chat','queued',JSON_OBJECT('timeout_ms',20,'on_error','return'))"));
        assert_eq!(result["error"]["code"], "ER_AI_TIMEOUT");
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
        assert_idle();
        set_user_policy(mock, dir, engine, session, json!({}));
        exec(
            engine,
            session,
            "SELECT AI_GENERATE('test.chat','after queue cancellation')",
        );
        assert_eq!(mock.requests.lock().unwrap().len(), 2);
        assert_idle();
    });
}

#[test]
fn ai_successful_requests_reset_consecutive_transport_failure_count() {
    with_mock(|mock, _, engine, session| {
        for _ in 0..2 {
            for _ in 0..4 {
                let value=json_result(exec(engine,session,"SELECT AI_GENERATE_DETAIL('test.chat','__503__',JSON_OBJECT('on_error','return'))"));
                assert_eq!(value["error"]["http_status"], 503);
            }
            exec(
                engine,
                session,
                "SELECT AI_GENERATE('test.chat','reset failures')",
            );
        }
        assert_eq!(mock.requests.lock().unwrap().len(), 10);
        assert_idle();
    });
}

#[test]
fn ai_stream_preserves_completed_rows_and_never_dispatches_later_windows_after_error() {
    let mock = Mock::responding(|_, request| {
        let prompt = request["messages"][0]["content"].as_str().unwrap();
        if prompt == "fail" {
            reply(503, b"{}", &[])
        } else {
            answer(prompt)
        }
    });
    with_provider(&mock, |mock, _, engine, session| {
        exec(
            engine,
            session,
            "CREATE TABLE ai_stream_inputs(id INT PRIMARY KEY,body TEXT)",
        );
        exec(engine,session,"INSERT INTO ai_stream_inputs VALUES(1,'one'),(2,'two'),(3,'three'),(4,'four'),(5,'fail'),(6,'six'),(7,'seven'),(8,'eight'),(9,'must-not-dispatch')");
        let response = <StorageQueryEngine as QueryEngine>::execute(
            engine,
            session,
            "SELECT id,AI_GENERATE('test.chat',body) FROM ai_stream_inputs ORDER BY id",
        )
        .unwrap();
        let QueryResponse::StreamingResultSet(mut result) = response else {
            panic!("expected external streaming cursor")
        };
        for i in 1..=4 {
            let row = result.cursor.next_row().unwrap().unwrap();
            assert_eq!(row[0], Value::Int(i));
        }
        assert!(result
            .cursor
            .next_row()
            .unwrap_err()
            .is(ER_AI_UPSTREAM_ERROR));
        assert!(result.cursor.next_row().unwrap().is_none());
        drop(result);
        assert_idle();
        let requests = mock.requests.lock().unwrap();
        assert!((5..=8).contains(&requests.len()));
        assert!(requests
            .iter()
            .all(|request| request["messages"][0]["content"] != "must-not-dispatch"));
    });
}

#[test]
fn ai_http_fixture_waits_for_request_bytes_after_accepting_connection() {
    let mock = Mock::new();
    let mut client = std::net::TcpStream::connect(("127.0.0.1", mock.port)).unwrap();
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while mock.reading_started.load(Ordering::Acquire) == 0 {
        assert!(
            std::time::Instant::now() < until,
            "fixture did not begin reading accepted socket"
        );
        thread::sleep(Duration::from_millis(1));
    }
    // Deliberately withhold request data: a healthy server waits rather than closes.
    client
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let error = client
        .peek(&mut [0u8; 1])
        .expect_err("fixture closed a socket before the HTTP request arrived");
    assert!(matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ));
    assert!(mock.requests.lock().unwrap().is_empty());
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let body = json!({"messages":[{"role":"user","content":"delayed"}]}).to_string();
    client.write_all(format!("POST /api/chat HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200"));
    assert_eq!(mock.requests.lock().unwrap().len(), 1);
}

#[path = "ai_concurrency.rs"]
mod deep_review;

#[test]
fn ai_audit_large_embedding_batch() {
    let mock = Mock::responding(|path, body| {
        let count = body["input"].as_array().unwrap().len();
        let dimensions = body["dimensions"].as_u64().unwrap() as usize;
        let vectors = vec![vec![0.123456_f64; dimensions]; count];
        let response = if path.ends_with("/api/embed") {
            json!({"embeddings":vectors})
        } else {
            json!({"data":vectors.into_iter().enumerate().rev().map(|(index,embedding)| json!({"index":index,"embedding":embedding})).collect::<Vec<_>>()})
        };
        reply(200, response.to_string(), &[])
    });
    with_provider(&mock, |mock, _, engine, session| {
        exec(
            engine,
            session,
            &format!(
                "CREATE AI CONNECTION audit_openai WITH CONFIG '{}'",
                json!({"protocol":"openai_embeddings","base_url":format!("http://127.0.0.1:{}",mock.port),"auth":{"type":"none"}})
            ),
        );
        let inputs = vec!["'x'"; 64].join(",");
        for connection in ["local", "audit_openai"] {
            for dimensions in [4096, 8192] {
                let name = format!("test.batch_{connection}_{dimensions}");
                exec(
                    engine,
                    session,
                    &format!(
                        "CREATE AI MODEL {name} USING {connection} WITH CONFIG '{}'",
                        json!({"kind":"embedding","remote_model":"embed","model_revision":"v1","embedding":{"dimensions":dimensions,"request_dimensions":true},"limits":{"max_batch_items":64}})
                    ),
                );
                let result = json_result(exec(
                    engine,
                    session,
                    &format!("SELECT AI_EMBED_DETAIL('{name}',JSON_ARRAY({inputs}))"),
                ));
                let data = result["data"].as_array().unwrap();
                assert_eq!(data.len(), 64);
                for (index, item) in data.iter().enumerate() {
                    assert_eq!(item["index"], index);
                    assert_eq!(item["embedding"].as_array().unwrap().len(), dimensions);
                    assert_eq!(
                        item["embedding"][dimensions - 1].as_f64().unwrap(),
                        0.123456_f32 as f64
                    );
                }
                assert_idle();
            }
        }
        assert_eq!(mock.requests.lock().unwrap().len(), 4);
        crate::util::env::with_vars(&[("MYDB_AI_MEMORY_LIMIT_MB", Some("1"))], || {
            assert_error(
                engine,
                session,
                &format!("SELECT AI_EMBED_DETAIL('test.batch_local_4096',JSON_ARRAY({inputs}))"),
                ER_AI_BUDGET_EXCEEDED,
            );
        });
        assert_eq!(mock.requests.lock().unwrap().len(), 4);
        assert_idle();
    });
}

#[test]
fn ai_audit_routine_creation_rejects_external_calls() {
    with_mock(|mock, _, engine, session| {
        exec(engine, session, "CREATE TABLE audit_source (id INT)");
        for sql in [
            "CREATE PROCEDURE audit_p() BEGIN SELECT AI_GENERATE('test.chat','x'); END",
            "CREATE PROCEDURE audit_p() BEGIN IF 1 THEN SET @x=AI_GENERATE('test.chat','x'); END IF; END",
            "CREATE PROCEDURE audit_p() BEGIN DECLARE c CURSOR FOR SELECT AI_GENERATE('test.chat','x'); END",
            "CREATE PROCEDURE audit_p() BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET @x=AI_GENERATE('test.chat','x'); SELECT 1; END",
            "CREATE TRIGGER audit_t BEFORE INSERT ON audit_source FOR EACH ROW SET @x=AI_GENERATE('test.chat','x')",
            "CREATE EVENT audit_e ON SCHEDULE EVERY 1 DAY DO SELECT AI_GENERATE('test.chat','x')",
            "CREATE FUNCTION audit_f() RETURNS TEXT RETURN AI_GENERATE('test.chat','x')",
            "CREATE PROCEDURE audit_p() BEGIN DECLARE x TEXT DEFAULT AI_GENERATE('test.chat','x'); END",
        ] {
            assert_error(engine,session,sql,ER_AI_UNSUPPORTED_CONTEXT);
        }
        exec(
            engine,
            session,
            "CREATE PROCEDURE audit_ok() SELECT 'AI_GENERATE' AS label",
        );
        exec(engine, session, "CALL audit_ok()");
        exec(engine,session, "CREATE PROCEDURE audit_dynamic() BEGIN SET @sql='SELECT AI_GENERATE(''test.chat'',''x'')'; PREPARE audit_stmt FROM @sql; EXECUTE audit_stmt; END");
        assert_error(
            engine,
            session,
            "CALL audit_dynamic()",
            ER_AI_UNSUPPORTED_CONTEXT,
        );
        assert!(mock.requests.lock().unwrap().is_empty());
    });
}

#[test]
fn ai_audit_incomplete_finish_reason() {
    let cases = [
        ("ollama", json!({"done":false}), "incomplete"),
        (
            "ollama",
            json!({"done":true,"done_reason":"length","message":{"role":"assistant","content":"partial"}}),
            "length",
        ),
        (
            "openai_chat",
            json!({"choices":[{"message":{"role":"assistant","content":"partial"},"finish_reason":"length"}]}),
            "length",
        ),
        (
            "openai_chat",
            json!({"choices":[{"message":{"role":"assistant","content":"partial"},"finish_reason":"unknown"}]}),
            "incomplete",
        ),
        (
            "openai_responses",
            json!({"status":"in_progress"}),
            "incomplete",
        ),
        ("openai_responses", json!({"status":"failed"}), "incomplete"),
        (
            "openai_responses",
            json!({"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}),
            "length",
        ),
        (
            "openai_responses",
            json!({"status":"incomplete","incomplete_details":{"reason":"unknown"}}),
            "incomplete",
        ),
    ];
    for (protocol, response, expected) in cases {
        let mock = Mock::responding(move |_, _| reply(200, response.to_string(), &[]));
        with_provider(&mock, |_, _, engine, session| {
            exec(
                engine,
                session,
                &format!(
                    "CREATE AI CONNECTION reason_conn WITH CONFIG '{}'",
                    json!({"protocol":protocol,"base_url":format!("http://127.0.0.1:{}",mock.port),"auth":{"type":"none"}})
                ),
            );
            exec(
                engine,
                session,
                r#"CREATE AI MODEL test.reason_model USING reason_conn WITH CONFIG '{"kind":"text_generation","remote_model":"mock","model_revision":"v1"}'"#,
            );
            let result = json_result(exec(
                engine,
                session,
                r#"SELECT AI_GENERATE_DETAIL('test.reason_model','x','{"on_error":"return"}')"#,
            ));
            assert_eq!(result["error"]["code"], "ER_AI_INCOMPLETE");
            assert_eq!(result["finish_reason"], expected, "{protocol}");
            assert!(result["text"].is_null());
            assert_idle();
        });
    }
}
