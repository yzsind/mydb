use super::regression::{assert_error, assert_idle, json_result, reply};
use super::*;
use crate::error::catalog::*;

fn configure(
    mock: &Mock,
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    protocol: &str,
    items: usize,
) {
    let config = json!({"protocol":protocol,"base_url":format!("http://127.0.0.1:{}/v1", mock.port),"auth":{"type":"none"}});
    exec(
        engine,
        session,
        &format!("CREATE AI CONNECTION images WITH CONFIG '{config}'"),
    );
    let model = json!({"kind":"embedding","remote_model":"generic-vision","model_revision":"v1","capabilities":{"image":true},"limits":{"max_batch_items":items},"embedding":{"dimensions":3,"request_dimensions":true}});
    exec(
        engine,
        session,
        &format!("CREATE AI MODEL test.images USING images WITH CONFIG '{model}'"),
    );
}
fn provider(path: &str, request: &Json) -> Vec<u8> {
    if path.ends_with("multimodal-embedding") {
        assert_eq!(request["parameters"]["enable_fusion"], false);
        assert_eq!(request["parameters"]["dimension"], 3);
        let items = request["input"]["contents"].as_array().unwrap();
        let vectors: Vec<_> = items
            .iter()
            .enumerate()
            .rev()
            .map(|(i, v)| {
                assert!(v
                    .get("image")
                    .or_else(|| v.get("text"))
                    .unwrap()
                    .is_string());
                json!({"index":i,"embedding":[i as f32 + 1.0, 2.0, 3.0]})
            })
            .collect();
        reply(200, json!({"output":{"embeddings":vectors},"request_id":"dashscope-request","usage":{"input_tokens":7,"image_tokens":8,"total_tokens":15}}).to_string(), &[])
    } else {
        assert_eq!(path, "/v1/embeddings");
        assert!(request.get("input").is_none());
        let content = request["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 1);
        let text = content[0]["image_url"]["url"]
            .as_str()
            .or_else(|| content[0]["text"].as_str())
            .unwrap();
        let n = if text.contains("second") { 2 } else { 1 };
        reply(200, json!({"data":[{"index":0,"embedding":[n,2,3]}],"usage":{"prompt_tokens":3,"total_tokens":3}}).to_string(), &[])
    }
}
#[test]
fn ai_image_url_blob_sql_batch_and_vllm_independent_requests() {
    for protocol in ["dashscope_multimodal_embeddings", "vllm_embeddings"] {
        let mock = Mock::responding(provider);
        with_provider(&mock, |mock, _, engine, session| {
            configure(mock, engine, session, protocol, 5);
            exec(engine, session, "SET @image=AI_EMBED('test.images','https://images.invalid/first.png','{\"type\":\"image\"}')");
            assert!(matches!(
                session.user_variable("image"),
                Some(Value::Vector(_))
            ));
            let value = json_result(exec(engine, session, "SELECT AI_EMBED_DETAIL('test.images',JSON_ARRAY('https://images.invalid/first.png','https://images.invalid/second.png','https://images.invalid/first.png'),JSON_OBJECT('type','image'))"));
            assert_eq!(value["data"].as_array().unwrap().len(), 3);
            for i in 0..3 {
                assert_eq!(value["data"][i]["index"], i);
            }
            assert_eq!(value["data"][0]["embedding"][0], 1.0);
            if protocol == "vllm_embeddings" {
                assert_eq!(value["data"][1]["embedding"][0], 2.0);
                assert_eq!(value["data"][2]["embedding"][0], 1.0);
                assert_eq!(value["attempts"], 3);
                assert_eq!(value["usage"]["total_tokens"], 9);
            } else {
                assert_eq!(value["provider_request_id"], "dashscope-request");
                assert_eq!(value["usage"]["total_tokens"], 15);
            }
            exec(
                engine,
                session,
                "CREATE TABLE image_inputs(id INT PRIMARY KEY, url TEXT, bytes BLOB)",
            );
            exec(engine, session, "INSERT INTO image_inputs VALUES(1,'https://images.invalid/first.png',FROM_BASE64('iVBORw0KGgo=')),(2,'https://images.invalid/second.png',FROM_BASE64('iVBORw0KGgo=')),(3,'https://images.invalid/first.png',FROM_BASE64('iVBORw0KGgo='))");
            for column in ["url", "bytes"] {
                let before = mock.requests.lock().unwrap().len();
                let rows = materialize(exec(engine, session, &format!("SELECT id,AI_EMBED('test.images',{column},'{{\"type\":\"image\"}}') FROM image_inputs ORDER BY id")));
                assert_eq!(rows.rows.len(), 3);
                assert!(matches!(rows.rows[0][1], Value::Vector(_)));
                assert_eq!(
                    mock.requests.lock().unwrap().len() - before,
                    if protocol == "vllm_embeddings" { 3 } else { 1 }
                );
            }
            let sql = "SELECT AI_EMBED('test.images',?,'{\"type\":\"image\"}')";
            let template =
                <StorageQueryEngine as QueryEngine>::prepare_cached(engine, session, sql, 1)
                    .unwrap();
            let result = crate::engine::params::with_params(
                vec![Value::Bytes(b"\x89PNG\r\n\x1a\n".to_vec())],
                || {
                    <StorageQueryEngine as QueryEngine>::execute_prepared_statement(
                        engine,
                        session,
                        sql,
                        (*template).clone(),
                    )
                },
            )
            .unwrap()
            .unwrap();
            assert!(matches!(materialize(result).rows[0][0], Value::Vector(_)));
            let before = mock.requests.lock().unwrap().len();
            exec(engine, session, "SELECT AI_EMBED('test.images',NULL,'{\"type\":\"image\"}'),CASE WHEN 0 THEN AI_EMBED('test.images','invalid','{\"type\":\"image\"}') ELSE NULL END");
            assert_eq!(mock.requests.lock().unwrap().len(), before);
            exec(
                engine,
                session,
                "SELECT AI_EMBED('test.images','plain text','{\"type\":\"text\"}')",
            );
            assert_idle();
        });
    }
}
#[test]
fn ai_image_invalid_input_capability_limits_and_reserved_options() {
    let mock = Mock::responding(provider);
    with_provider(&mock, |mock, _, engine, session| {
        configure(mock, engine, session, "dashscope_multimodal_embeddings", 5);
        for input in [
            "'local.png'",
            "'file:///tmp/picture.png'",
            "'data:image/png;base64,aGVsbG8='",
            "'https://user:password@images.invalid/p.png'",
            "FROM_BASE64('aGVsbG8=')",
            "12",
            "JSON_ARRAY('https://images.invalid/p.png',12)",
        ] {
            assert_error(engine, session, &format!("SELECT AI_EMBED_DETAIL('test.images',{input},'{{\"type\":\"image\",\"on_error\":\"return\"}}')"), ER_AI_INVALID_ARGUMENT);
        }
        for options in [
            "{\"type\":\"audio\"}",
            "{\"type\":\"image\",\"type\":\"text\"}",
        ] {
            assert_error(
                engine,
                session,
                &format!(
                    "SELECT AI_EMBED('test.images','https://images.invalid/p.png','{options}')"
                ),
                ER_AI_INVALID_ARGUMENT,
            );
        }
        assert_error(
            engine,
            session,
            "SELECT AI_EMBED('test.embed','https://images.invalid/p.png','{\"type\":\"image\"}')",
            ER_AI_INVALID_ARGUMENT,
        );
        for field in [
            "input",
            "image",
            "contents",
            "dimensions",
            "parameters",
            "parameters.dimension",
            "parameters.enable_fusion",
            "messages",
        ] {
            let model = json!({"kind":"embedding","remote_model":"generic","model_revision":"v1","embedding":{"dimensions":3},"provider_options_allowed":[field]});
            assert_error(
                engine,
                session,
                &format!("CREATE AI MODEL test.forbidden USING images WITH CONFIG '{model}'"),
                ER_AI_INVALID_ARGUMENT,
            );
        }
        let model = json!({"kind":"embedding","remote_model":"generic-vision","model_revision":"v1","embedding":{"dimensions":3,"request_dimensions":true},"limits":{"max_image_bytes":7}});
        exec(
            engine,
            session,
            &format!("ALTER AI MODEL test.images WITH CONFIG '{model}'"),
        );
        assert_error(
            engine,
            session,
            "SELECT AI_EMBED('test.images','https://images.invalid/p.png','{\"type\":\"image\"}')",
            ER_AI_INVALID_ARGUMENT,
        );
        assert_error(
            engine,
            session,
            &format!(
                "ALTER AI MODEL test.images WITH CONFIG '{}'",
                json!({"kind":"embedding","remote_model":"generic-vision","model_revision":"v1","capabilities":{"image":true},"embedding":{"dimensions":3,"request_dimensions":true}})
            ),
            ER_AI_INVALID_ARGUMENT,
        );
        assert_eq!(mock.requests.lock().unwrap().len(), 0);
        assert_idle();
    });
}

#[test]
fn ai_image_limits_memory_errors_and_recovery() {
    let mock = Mock::responding(provider);
    with_provider(&mock, |mock, dir, engine, session| {
        configure(mock, engine, session, "dashscope_multimodal_embeddings", 5);
        let mut model = json!({"kind":"embedding","remote_model":"generic-vision","model_revision":"v1","capabilities":{"image":true},"limits":{"max_image_bytes":8,"max_batch_items":5},"embedding":{"dimensions":3,"request_dimensions":true}});
        exec(
            engine,
            session,
            &format!("ALTER AI MODEL test.images WITH CONFIG '{model}'"),
        );
        assert_error(
            engine,
            session,
            "SELECT AI_EMBED('test.images',FROM_BASE64('iVBORw0KGgoA'),'{\"type\":\"image\"}')",
            ER_AI_INVALID_ARGUMENT,
        );
        model["limits"]["max_image_bytes"] = json!(5 * 1024 * 1024 + 1);
        assert_error(
            engine,
            session,
            &format!("ALTER AI MODEL test.images WITH CONFIG '{model}'"),
            ER_AI_INVALID_ARGUMENT,
        );
        crate::util::env::with_vars(&[("MYDB_AI_MEMORY_LIMIT_MB", Some("1"))], || {
            let mut data = b"\x89PNG\r\n\x1a\n".to_vec();
            data.resize(5 * 1024 * 1024, 0);
            let model = json!({"kind":"embedding","remote_model":"generic-vision","model_revision":"v1","capabilities":{"image":true},"limits":{"max_batch_items":5},"embedding":{"dimensions":3,"request_dimensions":true}});
            exec(
                engine,
                session,
                &format!("ALTER AI MODEL test.images WITH CONFIG '{model}'"),
            );
            let sql = "SELECT AI_EMBED('test.images',?,'{\"type\":\"image\"}')";
            let template =
                <StorageQueryEngine as QueryEngine>::prepare_cached(engine, session, sql, 1)
                    .unwrap();
            let result = crate::engine::params::with_params(vec![Value::Bytes(data)], || {
                <StorageQueryEngine as QueryEngine>::execute_prepared_statement(
                    engine,
                    session,
                    sql,
                    (*template).clone(),
                )
            });
            assert!(result.unwrap().unwrap_err().is(ER_AI_BUDGET_EXCEEDED));
        });
        assert_eq!(mock.requests.lock().unwrap().len(), 0);
        exec(
            engine,
            session,
            "SELECT AI_EMBED('test.images','https://images.invalid/ok.png','{\"type\":\"image\"}')",
        );
        let create = materialize(exec(engine, session, "SHOW CREATE AI MODEL test.images"));
        assert!(create.rows[0]
            .iter()
            .any(|v| v.to_display_string().contains("image")));
        assert!(dir.join("data").exists());
        assert_idle();
    });
}

#[test]
fn ai_image_vllm_detail_failure_never_exposes_partial_vectors() {
    let mock = Mock::responding(|path, request| {
        if request["messages"][0]["content"][0]["image_url"]["url"]
            == "https://images.invalid/bad.png"
        {
            return reply(400, "{\"message\":\"SECRET_IMAGE_CONTENT\"}", &[]);
        }
        provider(path, request)
    });
    with_provider(&mock, |mock, _, engine, session| {
        configure(mock, engine, session, "vllm_embeddings", 5);
        let value=json_result(exec(engine,session,"SELECT AI_EMBED_DETAIL('test.images',JSON_ARRAY('https://images.invalid/ok.png','https://images.invalid/bad.png'),'{\"type\":\"image\",\"on_error\":\"return\"}')"));
        assert_eq!(value["ok"], false);
        assert!(value["data"].is_null());
        assert_eq!(value["attempts"], 2);
        assert!(!value.to_string().contains("SECRET_IMAGE_CONTENT"));
        assert_eq!(mock.requests.lock().unwrap().len(), 2);
        assert_idle();
    });
}

#[test]
fn ai_image_large_blob_rows_split_before_request_limit() {
    let mock = Mock::responding(provider);
    with_provider(&mock, |mock, _, engine, session| {
        configure(mock, engine, session, "dashscope_multimodal_embeddings", 5);
        let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
        bytes.resize(3 * 1024 * 1024, 0);
        session
            .set_user_variable("large_image", Value::Bytes(bytes))
            .unwrap();
        exec(
            engine,
            session,
            "CREATE TABLE large_images(id INT PRIMARY KEY,image LONGBLOB)",
        );
        exec(
            engine,
            session,
            "INSERT INTO large_images VALUES(1,@large_image),(2,@large_image),(3,@large_image)",
        );
        let result=materialize(exec(engine,session,"SELECT AI_EMBED('test.images',image,'{\"type\":\"image\"}') FROM large_images ORDER BY id"));
        assert_eq!(result.rows.len(), 3);
        let requests = mock.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        for request in requests.iter() {
            assert_eq!(request["input"]["contents"].as_array().unwrap().len(), 1);
            assert!(serde_json::to_vec(request).unwrap().len() < 8 * 1024 * 1024);
        }
        drop(requests);
        assert_idle();
    });
}

#[test]
fn ai_image_model_configuration_survives_restart() {
    let mock = Mock::responding(provider);
    let dir = temp();
    crate::util::env::with_vars(
        &[
            ("MYDB_AI_ENABLED", Some("ON")),
            ("MYDB_AI_LOCAL_CONFIG", None),
        ],
        || {
            let mut session = SessionContext::default();
            session.current_database = "test".into();
            {
                let engine = open_strict_test_engine(&dir.path().join("data"));
                configure(
                    &mock,
                    &engine,
                    &mut session,
                    "dashscope_multimodal_embeddings",
                    5,
                );
                engine.checkpoint().unwrap();
            }
            let engine = open_strict_test_engine(&dir.path().join("data"));
            let result=materialize(exec(&engine,&mut session,"SELECT AI_EMBED('test.images','https://images.invalid/restarted.png','{\"type\":\"image\"}')"));
            assert!(matches!(result.rows[0][0], Value::Vector(_)));
            assert_eq!(mock.requests.lock().unwrap().len(), 1);
            assert_idle();
        },
    );
}

#[test]
fn ai_image_row_batches_include_merged_provider_options_in_request_limit() {
    let mock = Mock::responding(provider);
    with_provider(&mock, |mock, _, engine, session| {
        configure(mock, engine, session, "dashscope_multimodal_embeddings", 5);
        let config = json!({
            "kind":"embedding", "remote_model":"generic-vision", "model_revision":"v1",
            "capabilities":{"image":true}, "limits":{"max_batch_items":5},
            "embedding":{"dimensions":3,"request_dimensions":true},
            "defaults":{"provider_options":{"parameters":{"instruct":"a".repeat(32000)}}},
            "provider_options_allowed":["parameters.instruct","parameters.extra"],
            "provider_options_schema":{
                "parameters.instruct":{"type":"string"},
                "parameters.extra":{"type":"string"}
            }
        });
        exec(
            engine,
            session,
            &format!("ALTER AI MODEL test.images WITH CONFIG '{config}'"),
        );
        let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
        bytes.resize(3_120_000, 0);
        session
            .set_user_variable("near_limit_image", Value::Bytes(bytes))
            .unwrap();
        exec(
            engine,
            session,
            "CREATE TABLE metadata_images(id INT PRIMARY KEY,image LONGBLOB)",
        );
        exec(
            engine,
            session,
            "INSERT INTO metadata_images VALUES(1,@near_limit_image),(2,@near_limit_image)",
        );
        let rows = materialize(exec(engine, session, "SELECT AI_EMBED('test.images',image,JSON_OBJECT('type','image','provider_options',JSON_OBJECT('parameters',JSON_OBJECT('extra',REPEAT('b',60000))))) FROM metadata_images ORDER BY id"));
        assert_eq!(rows.rows.len(), 2);
        let requests = mock.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            assert_eq!(request["input"]["contents"].as_array().unwrap().len(), 1);
            assert_eq!(
                request["parameters"]["instruct"].as_str().unwrap().len(),
                32000
            );
            assert_eq!(
                request["parameters"]["extra"].as_str().unwrap().len(),
                60000
            );
            assert!(serde_json::to_vec(request).unwrap().len() <= 8 * 1024 * 1024);
        }
        drop(requests);
        assert_idle();
    });
}
