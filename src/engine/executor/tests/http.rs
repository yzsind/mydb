//! HTTP SQL integration tests use only bounded loopback fixtures.
use super::*;
use serde_json::{json, Value as Json};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
};

struct Mock {
    port: u16,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<Json>>>,
    connections: Arc<AtomicUsize>,
    worker: Option<thread::JoinHandle<()>>,
}
impl Mock {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(AtomicUsize::new(0));
        let s = stop.clone();
        let q = requests.clone();
        let c = connections.clone();
        let worker = thread::spawn(move || {
            let mut workers = Vec::new();
            while !s.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        c.fetch_add(1, Ordering::Relaxed);
                        let q = q.clone();
                        let s = s.clone();
                        workers.push(thread::spawn(move || serve(stream, q, s)));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(e) => panic!("{e}"),
                }
            }
            for w in workers {
                w.join().unwrap();
            }
        });
        Self {
            port,
            stop,
            requests,
            connections,
            worker: Some(worker),
        }
    }
    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.port)
    }
    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(w) = self.worker.take() {
            w.join().unwrap();
        }
    }
}
fn serve(mut stream: TcpStream, requests: Arc<Mutex<Vec<Json>>>, stop: Arc<AtomicBool>) {
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut buffer = Vec::new();
    while !stop.load(Ordering::Acquire) {
        let header_end = loop {
            if let Some(i) = buffer.windows(4).position(|v| v == b"\r\n\r\n") {
                break i + 4;
            }
            let mut b = [0; 8192];
            match stream.read(&mut b) {
                Ok(0) => return,
                Ok(n) => buffer.extend_from_slice(&b[..n]),
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    if stop.load(Ordering::Acquire) {
                        return;
                    }
                }
                Err(_) => return,
            }
        };
        let head = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
        let mut lines = head.split("\r\n");
        let first = lines.next().unwrap();
        let mut words = first.split_whitespace();
        let method = words.next().unwrap_or("");
        let path = words.next().unwrap_or("");
        let mut headers = serde_json::Map::new();
        let mut length = 0usize;
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                let key = k.to_ascii_lowercase();
                let value = v.trim().to_owned();
                if key == "content-length" {
                    length = value.parse().unwrap();
                }
                headers
                    .entry(key)
                    .or_insert_with(|| json!([]))
                    .as_array_mut()
                    .unwrap()
                    .push(json!(value));
            }
        }
        while buffer.len() < header_end + length {
            let mut b = [0; 8192];
            match stream.read(&mut b) {
                Ok(0) | Err(_) => return,
                Ok(n) => buffer.extend_from_slice(&b[..n]),
            }
        }
        let body = buffer[header_end..header_end + length].to_vec();
        buffer.drain(..header_end + length);
        let record = json!({"method":method,"path":path,"headers":headers,"body":String::from_utf8_lossy(&body),"bytes":body});
        requests.lock().unwrap().push(record.clone());
        let mut status = 200;
        let mut extra = String::new();
        let mut response = serde_json::to_vec(&record).unwrap();
        if path == "/binary" {
            response = vec![0, 255, 128, 10];
        }
        if path == "/empty" {
            status = 204;
            response.clear();
        }
        if path == "/error" {
            status = 503;
            response = b"upstream unavailable".to_vec();
        }
        if path == "/slow" {
            thread::sleep(Duration::from_millis(500));
        }
        if path == "/large" {
            response = vec![b'x'; 65536];
        }
        if path == "/redirect" {
            status = 307;
            extra = "Location: /echo\r\n".into();
            response.clear();
        }
        if path == "/redirect303" {
            status = 303;
            extra = "Location: /echo\r\n".into();
            response.clear();
        }
        if path == "/cross" {
            status = 302;
            extra = "Location: http://localhost:1/never\r\n".into();
            response.clear();
        }
        if path == "/loop" {
            status = 302;
            extra = "Location: /loop\r\n".into();
            response.clear();
        }
        if path == "/gzip" {
            let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            e.write_all(b"compressed text").unwrap();
            response = e.finish().unwrap();
            extra = "Content-Encoding: gzip\r\n".into();
        }
        if path == "/stacked" {
            let mut gzip =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            gzip.write_all(b"stacked payload").unwrap();
            let mut zlib =
                flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            zlib.write_all(&gzip.finish().unwrap()).unwrap();
            response = zlib.finish().unwrap();
            extra = "Content-Encoding: gzip\r\nContent-Encoding: deflate\r\n".into();
        }
        if path == "/broken" {
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort");
            return;
        }
        if path == "/chunked" {
            let _=stream.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n");
            continue;
        }
        let header=format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nContent-Type: application/json\r\nX-Repeat: one\r\nX-Repeat: two\r\n{extra}\r\n",response.len());
        if stream.write_all(header.as_bytes()).is_err() {
            return;
        }
        if method != "HEAD" && stream.write_all(&response).is_err() {
            return;
        }
    }
}
fn temp() -> tempfile::TempDir {
    std::fs::create_dir_all("/tmp/mydb/http-functions").unwrap();
    tempfile::Builder::new()
        .prefix("sql-")
        .tempdir_in("/tmp/mydb/http-functions")
        .unwrap()
}
fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match engine
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .materialize()
        .unwrap()
    {
        QueryResponse::ResultSet(r) => r.rows,
        _ => panic!("expected rows"),
    }
}
fn response(engine: &StorageQueryEngine, sql: &str) -> Json {
    let r = rows(engine, sql);
    let Value::Json(v) = &r[0][0] else {
        panic!("expected JSON, got {:?}", r);
    };
    crate::engine::json::to_serde(v).unwrap()
}
fn options(v: Json) -> String {
    format!(
        "'{}'",
        v.to_string().replace('\\', "\\\\").replace('\'', "''")
    )
}
fn echo(v: &Json) -> Json {
    serde_json::from_str(v["body"].as_str().unwrap()).unwrap()
}

#[test]
fn http_audit_source_free_explain_analyze_executes_once() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let u = m.url("/echo");
    for format in ["", "FORMAT=TREE ", "FORMAT=JSON ", "FORMAT=TRADITIONAL "] {
        for from in ["", " FROM DUAL"] {
            let target = format!("SELECT HTTP_GET('{u}'){from}");
            let before = m.count();
            rows(&e, &format!("EXPLAIN {format}{target}"));
            assert_eq!(m.count(), before);
            let result = rows(&e, &format!("EXPLAIN ANALYZE {format}{target}"));
            assert_eq!(m.count(), before + 1, "format={format}, from={from}");
            if format == "FORMAT=TRADITIONAL " {
                assert_eq!(result[0].len(), 17);
                assert_eq!(result[0][16], Value::Int(1));
            } else if format == "FORMAT=JSON " {
                let Value::String(text) = &result[0][0] else {
                    panic!("expected JSON plan")
                };
                let plan: Json = serde_json::from_str(text).unwrap();
                assert_eq!(plan["query_block"]["execution"]["rows_total"], 1);
            } else {
                let Value::String(text) = &result[0][0] else {
                    panic!("expected tree plan")
                };
                assert!(text.contains("actual time="), "{text}");
                assert!(text.contains("rows=1 loops=1"), "{text}");
            }
        }
    }
    let before = m.count();
    rows(
        &e,
        &format!("EXPLAIN ANALYZE SELECT HTTP_GET('{u}') WHERE 0"),
    );
    rows(
        &e,
        &format!("EXPLAIN ANALYZE SELECT CASE WHEN 0 THEN HTTP_GET('{u}') ELSE 1 END"),
    );
    assert_eq!(m.count(), before);
    let err = e
        .execute("EXPLAIN ANALYZE SELECT HTTP_GET('file:///invalid')")
        .unwrap_err();
    assert!(
        err.is(crate::error::catalog::ER_HTTP_INVALID_ARGUMENT),
        "{err}"
    );
}

#[test]
fn http_audit_query_objects_have_deterministic_order_and_arrays_keep_order() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let opts = r#"'{"query":{"zebra":"1","alpha":["second","first"]},"headers":{"z-last":"z","a-first":["second","first"]}}'"#;
    let v = response(
        &e,
        &format!("SELECT HTTP_GET('{}',{opts})", m.url("/echo?z=0&a=1")),
    );
    assert_eq!(
        echo(&v)["path"],
        "/echo?z=0&a=1&alpha=second&alpha=first&zebra=1"
    );
    assert_eq!(echo(&v)["headers"]["a-first"], json!(["second", "first"]));
    let v = response(
        &e,
        &format!(
            "SELECT HTTP_GET('{}')",
            m.url("/echo?zebra=1&alpha=2&zebra=3")
        ),
    );
    assert_eq!(echo(&v)["path"], "/echo?zebra=1&alpha=2&zebra=3");
}

#[test]
fn http_audit_rate_exhaustion_fails_fast_without_sending() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let policy = dir.path().join("rate-policy.json");
    std::fs::write(&policy, r#"{"version":1,"user_limits":[{"user":"rate_user","host":"localhost","requests_per_minute":2}]}"#).unwrap();
    e.execute("CREATE USER 'rate_user'@'localhost' IDENTIFIED BY 'x'")
        .unwrap();
    e.execute("GRANT HTTP_USE ON *.* TO 'rate_user'@'localhost'")
        .unwrap();
    let mut s = SessionContext::default();
    s.current_database = "test".into();
    s.user = "rate_user".into();
    s.login_user = "rate_user".into();
    s.authenticated_as = crate::engine::auth::AccountKey::new("rate_user", "localhost");
    s.current_user_is_system = false;
    s.internal_system_session = false;
    crate::util::env::with_vars(
        &[("MYDB_HTTP_LOCAL_CONFIG", Some(policy.to_str().unwrap()))],
        || {
            let sql = format!("SELECT HTTP_GET('{}')", m.url("/echo"));
            for _ in 0..2 {
                e.execute_with_session(&mut s, &sql)
                    .unwrap()
                    .materialize()
                    .unwrap();
            }
            assert_eq!(m.count(), 2);
            for mode in ["raise", "return"] {
                let began = std::time::Instant::now();
                let err = e.execute_with_session(&mut s, &format!("SELECT HTTP_GET('{}','{{\"timeout_ms\":1500,\"on_error\":\"{mode}\"}}')", m.url("/echo"))).unwrap_err();
                assert_eq!(err.symbol(), Some("ER_HTTP_RATE_LIMITED"), "{err}");
                assert!(began.elapsed() < Duration::from_millis(750));
                assert_eq!(m.count(), 2);
                e.execute_with_session(&mut s, "SELECT 1").unwrap();
            }
            // An exhausted account does not prevent an unrestricted account from sending.
            response(&e, &sql);
            assert_eq!(m.count(), 3);
        },
    );
}

#[test]
fn http_methods_payloads_and_response_contract() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    for method in ["GET", "HEAD", "OPTIONS", "DELETE"] {
        let v = response(&e, &format!("SELECT HTTP_{method}('{}')", m.url("/echo")));
        assert_eq!(v["status"], 200);
        assert_eq!(v["headers"]["x-repeat"], json!(["one", "two"]));
        if method == "HEAD" {
            assert_eq!(v["body"], "");
        } else {
            assert_eq!(echo(&v)["method"], method);
        }
    }
    for method in ["POST", "PUT", "PATCH"] {
        let v = response(
            &e,
            &format!(
                "SELECT HTTP_{method}('{}','Unicode 中文 😀')",
                m.url("/echo")
            ),
        );
        assert_eq!(echo(&v)["body"], "Unicode 中文 😀");
        assert_eq!(echo(&v)["method"], method);
    }
    let v = response(
        &e,
        &format!(
            "SELECT HTTP_REQUEST('POST','{}',{})",
            m.url("/echo?original=1"),
            options(
                json!({"query":{"q":["a b","中"]},"headers":{"x-test":["a","b"]},"json":{"x":null},"auth":{"type":"bearer","token":"secret"}})
            )
        ),
    );
    let v = echo(&v);
    assert_eq!(v["path"], "/echo?original=1&q=a+b&q=%E4%B8%AD");
    assert_eq!(v["headers"]["x-test"], json!(["a", "b"]));
    assert_eq!(v["headers"]["authorization"], json!(["Bearer secret"]));
    assert_eq!(v["body"], "{\"x\":null}");
    let v = response(
        &e,
        &format!("SELECT HTTP_POST('{}',X'00FF80')", m.url("/echo")),
    );
    assert_eq!(echo(&v)["bytes"], json!([0, 255, 128]));
    let v = response(
        &e,
        &format!("SELECT HTTP_POST('{}',JSON_OBJECT('x',1))", m.url("/echo")),
    );
    assert_eq!(
        echo(&v)["headers"]["content-type"],
        json!(["application/json"])
    );
    let v = response(&e, &format!("SELECT HTTP_GET('{}')", m.url("/binary")));
    assert_eq!(v["body_encoding"], "base64");
    assert_eq!(v["body"], "AP+ACg==");
    assert_eq!(
        response(&e, &format!("SELECT HTTP_GET('{}')", m.url("/error")))["status"],
        503
    );
    assert_eq!(
        response(&e, &format!("SELECT HTTP_GET('{}')", m.url("/empty")))["body"],
        ""
    );
    assert!(
        m.connections.load(Ordering::Relaxed) < m.count(),
        "requests should reuse pooled TCP connections"
    );
}
#[test]
fn http_form_multipart_and_strict_validation() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let v = response(
        &e,
        &format!(
            "SELECT HTTP_REQUEST('POST','{}',{})",
            m.url("/echo"),
            options(json!({"form":[{"name":"a","value":"one two"},{"name":"a","value":"中"}]}))
        ),
    );
    assert_eq!(echo(&v)["body"], "a=one+two&a=%E4%B8%AD");
    let v = response(
        &e,
        &format!(
            "SELECT HTTP_REQUEST('POST','{}',{})",
            m.url("/echo"),
            options(
                json!({"multipart":[{"name":"field","text":"hello"},{"name":"file","filename":"x.bin","base64":"AP8=","content_type":"application/octet-stream"}]})
            )
        ),
    );
    let r = echo(&v);
    assert!(r["body"].as_str().unwrap().contains("filename=\"x.bin\""));
    assert!(r["body"].as_str().unwrap().contains("hello"));
    let before = m.count();
    for o in [
        json!({"unknown":true}),
        json!({"body":"a","json":null}),
        json!({"headers":{"Host":"evil"}}),
        json!({"headers":{"x":"a\r\nb"}}),
        json!({"headers":{"X":"1","x":"2"}}),
        json!({"auth":{"type":"bearer","token":"s"},"headers":{"Authorization":"x"}}),
        json!({"timeout_ms":30001}),
        json!({"response_encoding":"lossy"}),
        json!({"multipart":[{"name":"x","text":"a","base64":"YQ=="}]}),
    ] {
        assert!(e
            .execute(&format!(
                "SELECT HTTP_GET('{}',{})",
                m.url("/echo"),
                options(o)
            ))
            .is_err());
    }
    assert!(e
        .execute("SELECT HTTP_REQUEST('CONNECT','http://127.0.0.1/')")
        .is_err());
    assert_eq!(m.count(), before);
    assert_eq!(
        rows(
            &e,
            "SELECT HTTP_GET(NULL), HTTP_REQUEST(NULL,'http://127.0.0.1/')"
        ),
        vec![vec![Value::Null, Value::Null]]
    );
}
#[test]
fn http_redirect_compression_timeout_and_limits() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    assert_eq!(
        response(&e, &format!("SELECT HTTP_GET('{}')", m.url("/redirect")))["status"],
        307
    );
    let o = options(json!({"follow_redirects":true}));
    let v = response(
        &e,
        &format!("SELECT HTTP_POST('{}','data',{o})", m.url("/redirect")),
    );
    assert_eq!(echo(&v)["method"], "POST");
    assert_eq!(echo(&v)["body"], "data");
    assert_eq!(v["redirect_count"], 1);
    let v = response(
        &e,
        &format!("SELECT HTTP_POST('{}','data',{o})", m.url("/redirect303")),
    );
    assert_eq!(echo(&v)["method"], "GET");
    assert_eq!(echo(&v)["body"], "");
    assert!(e
        .execute(&format!("SELECT HTTP_GET('{}',{o})", m.url("/cross")))
        .unwrap_err()
        .is(crate::error::catalog::ER_HTTP_EGRESS_DENIED));
    assert!(e
        .execute(&format!("SELECT HTTP_GET('{}',{o})", m.url("/loop")))
        .is_err());
    assert_eq!(
        response(&e, &format!("SELECT HTTP_GET('{}')", m.url("/gzip")))["body"],
        "compressed text"
    );
    assert_eq!(
        response(&e, &format!("SELECT HTTP_GET('{}')", m.url("/chunked")))["body"],
        "abcdef"
    );
    let v = response(
        &e,
        &format!(
            "SELECT HTTP_GET('{}',{})",
            m.url("/slow"),
            options(json!({"timeout_ms":30,"on_error":"return"}))
        ),
    );
    assert_eq!(v["error"]["code"], "ER_HTTP_TIMEOUT");
    assert_eq!(v["error"]["may_have_executed"], true);
    assert!(e
        .execute(&format!(
            "SELECT HTTP_GET('{}',{})",
            m.url("/large"),
            options(json!({"max_response_bytes":100,"on_error":"return"}))
        ))
        .unwrap_err()
        .is(crate::error::catalog::ER_HTTP_BUDGET_EXCEEDED));
    let before = m.count();
    assert!(e
        .execute(&format!("SELECT HTTP_POST('{}','data')", m.url("/broken")))
        .is_err());
    assert_eq!(
        m.count(),
        before + 1,
        "no transport retry after ambiguous failure"
    );
}
#[test]
fn http_runtime_sql_contexts_short_circuit_and_rollback() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let url = m.url("/echo");
    e.execute("CREATE TABLE h(id INT PRIMARY KEY, v JSON)")
        .unwrap();
    e.execute("INSERT INTO h(id) VALUES(1),(2),(3)").unwrap();
    let before = m.count();
    rows(&e,&format!("SELECT CASE WHEN 0 THEN HTTP_GET('{url}') ELSE 1 END, IF(1,2,HTTP_GET('{url}')), COALESCE(3,HTTP_GET('{url}'))"));
    assert_eq!(m.count(), before);
    e.execute(&format!("EXPLAIN SELECT HTTP_GET('{url}') FROM h"))
        .unwrap();
    assert_eq!(m.count(), before);
    rows(&e, &format!("SELECT HTTP_GET('{url}') FROM h ORDER BY id"));
    assert_eq!(m.count(), before + 3);
    assert_eq!(
        rows(
            &e,
            &format!(
                "SELECT id FROM h WHERE JSON_EXTRACT(HTTP_GET('{url}'),'$.status')=200 ORDER BY id"
            )
        )
        .len(),
        3
    );
    e.execute(&format!("UPDATE h SET v=HTTP_GET('{url}') WHERE id=1"))
        .unwrap();
    let mut s = SessionContext::default();
    s.current_database = "test".into();
    e.execute_with_session(&mut s, "BEGIN").unwrap();
    let before = m.count();
    e.execute_with_session(
        &mut s,
        &format!("INSERT INTO h VALUES(4,HTTP_POST('{url}','external'))"),
    )
    .unwrap();
    e.execute_with_session(&mut s, "ROLLBACK").unwrap();
    assert_eq!(m.count(), before + 1);
    assert_eq!(rows(&e, "SELECT COUNT(*) FROM h")[0][0], Value::Int(3));
    e.execute(&format!(
        "CREATE FUNCTION http_wrap(p TEXT) RETURNS JSON DETERMINISTIC RETURN HTTP_GET(p)"
    ))
    .unwrap();
    let before = m.count();
    rows(&e, &format!("SELECT http_wrap('{url}') FROM h"));
    assert_eq!(
        m.count(),
        before + 3,
        "declared determinism must not fold HTTP"
    );
    e.execute(&format!(
        "CREATE VIEW hv AS SELECT id,HTTP_GET('{url}') AS r FROM h"
    ))
    .unwrap();
    let before = m.count();
    rows(&e, "SELECT r FROM hv WHERE id=1");
    assert_eq!(m.count(), before + 1);
    e.execute(&format!(
        "CREATE TRIGGER ht BEFORE INSERT ON h FOR EACH ROW SET NEW.v=HTTP_POST('{url}','trigger')"
    ))
    .unwrap();
    let before = m.count();
    e.execute("INSERT INTO h(id) VALUES(5)").unwrap();
    assert_eq!(m.count(), before + 1);
}
#[test]
fn http_persistent_expressions_are_rejected_without_network() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let u = m.url("/echo");
    for ddl in [
        format!("CREATE TABLE hg(a INT,b JSON AS (HTTP_GET('{u}')))"),
        format!("CREATE TABLE hc(a INT CHECK (HTTP_GET('{u}') IS NOT NULL))"),
        format!("CREATE TABLE hd(a JSON DEFAULT (HTTP_GET('{u}')))"),
        format!("CREATE TABLE hi(a TEXT, INDEX ((HTTP_GET(a))))"),
    ] {
        assert!(e.execute(&ddl).is_err(), "{ddl}");
    }
    assert_eq!(m.count(), 0);
}
#[test]
fn http_permissions_and_prepared_parameters() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    e.execute("CREATE USER 'http_reader'@'localhost' IDENTIFIED BY 'x'")
        .unwrap();
    let mut s = SessionContext::default();
    s.current_database = "test".into();
    s.user = "http_reader".into();
    s.login_user = "http_reader".into();
    s.authenticated_as = crate::engine::auth::AccountKey::new("http_reader", "localhost");
    s.current_user_is_system = false;
    s.internal_system_session = false;
    let sql = format!("SELECT HTTP_GET('{}')", m.url("/echo"));
    assert!(e
        .execute_with_session(&mut s, &sql)
        .unwrap_err()
        .is(crate::error::catalog::ER_HTTP_ACCESS_DENIED));
    assert_eq!(m.count(), 0);
    e.execute("GRANT HTTP_USE ON *.* TO 'http_reader'@'localhost'")
        .unwrap();
    let sql = "SELECT HTTP_POST(?, ?, ?)";
    let template = <StorageQueryEngine as QueryEngine>::prepare_cached(&e, &mut s, sql, 3).unwrap();
    let params = vec![
        Value::String(m.url("/echo")),
        Value::Bytes(vec![0, 255]),
        Value::String("{}".into()),
    ];
    let result = crate::engine::params::with_params(params.clone(), || {
        <StorageQueryEngine as QueryEngine>::execute_prepared_statement(
            &e,
            &mut s,
            sql,
            (*template).clone(),
        )
    })
    .unwrap()
    .unwrap()
    .materialize()
    .unwrap();
    assert!(matches!(result, QueryResponse::ResultSet(_)));
    assert_eq!(m.count(), 1);
    e.execute("REVOKE HTTP_USE ON *.* FROM 'http_reader'@'localhost'")
        .unwrap();
    assert!(crate::engine::params::with_params(params, || {
        <StorageQueryEngine as QueryEngine>::execute_prepared_statement(
            &e,
            &mut s,
            sql,
            (*template).clone(),
        )
    })
    .unwrap()
    .is_err());
    assert_eq!(m.count(), 1);
}

#[test]
fn http_kill_cancels_network_wait_and_preserves_session() {
    let m = Mock::new();
    let dir = temp();
    let engine = Arc::new(open_test_engine(dir.path()));
    let mut session = SessionContext::default();
    session.current_database = "test".into();
    session.connection_id = 9201;
    let handle = engine
        .registry()
        .register(9201, "root".into(), "localhost".into(), "test".into());
    session.set_cancel_handle(Some(handle.cancel_handle()));
    let mut victim = session.clone();
    let e = engine.clone();
    let url = m.url("/slow");
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let result = e.execute_with_session(
            &mut victim,
            &format!("SELECT HTTP_GET('{url}',JSON_OBJECT('on_error','return'))"),
        );
        tx.send(result).unwrap();
    });
    let until = std::time::Instant::now() + Duration::from_secs(3);
    while m.count() == 0 {
        assert!(std::time::Instant::now() < until);
        thread::sleep(Duration::from_millis(2));
    }
    engine.registry().kill_query(9201).unwrap();
    let result = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    worker.join().unwrap();
    assert!(result
        .unwrap_err()
        .is(crate::error::catalog::ER_QUERY_INTERRUPTED));
    session.refresh_cancel_generations();
    assert!(engine
        .execute_with_session(&mut session, "SELECT 1")
        .is_ok());
}
#[test]
fn http_shutdown_cancels_active_wait_without_resending() {
    let m = Mock::new();
    let dir = temp();
    let engine = Arc::new(open_test_engine(dir.path()));
    let e = engine.clone();
    let url = m.url("/slow");
    let worker = thread::spawn(move || e.execute(&format!("SELECT HTTP_GET('{url}')")));
    let until = std::time::Instant::now() + Duration::from_secs(3);
    while m.count() == 0 {
        assert!(std::time::Instant::now() < until);
        thread::sleep(Duration::from_millis(2));
    }
    let start = std::time::Instant::now();
    engine.shutdown().unwrap();
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(worker.join().unwrap().is_err());
    assert_eq!(m.count(), 1);
}
#[test]
fn http_send_budget_is_shared_by_rows_and_nested_routines() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let u = m.url("/echo");
    e.execute("CREATE TABLE quota_rows(id INT)").unwrap();
    e.execute("INSERT INTO quota_rows VALUES(1),(2),(3)")
        .unwrap();
    e.execute("CREATE FUNCTION quota_http(p TEXT) RETURNS JSON RETURN HTTP_GET(p)")
        .unwrap();
    crate::util::env::with_vars(&[("MYDB_HTTP_MAX_CALLS_PER_STATEMENT", Some("2"))], || {
        let err = e
            .execute(&format!("SELECT quota_http('{u}') FROM quota_rows"))
            .unwrap_err();
        assert!(
            err.is(crate::error::catalog::ER_HTTP_BUDGET_EXCEEDED),
            "{err:?}"
        );
    });
    assert_eq!(m.count(), 2);
    let before = m.count();
    assert!(e
        .execute(&format!(
            "CREATE TABLE indirect_default(v JSON DEFAULT (quota_http('{u}')))"
        ))
        .is_err());
    assert_eq!(m.count(), before);
}

const HTTP_TEST_FUNCTIONS: &[&str] = &[
    "HTTP_REQUEST",
    "HTTP_GET",
    "HTTP_HEAD",
    "HTTP_OPTIONS",
    "HTTP_DELETE",
    "HTTP_POST",
    "HTTP_PUT",
    "HTTP_PATCH",
];
#[test]
fn http_all_functions_have_native_json_metadata() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    for name in HTTP_TEST_FUNCTIONS {
        let url = m.url("/echo");
        let arguments = if *name == "HTTP_REQUEST" {
            format!("'GET','{url}'")
        } else if matches!(*name, "HTTP_POST" | "HTTP_PUT" | "HTTP_PATCH") {
            format!("'{url}',''")
        } else {
            format!("'{url}'")
        };
        let QueryResponse::ResultSet(r) =
            e.execute(&format!("SELECT {name}({arguments})")).unwrap()
        else {
            panic!("expected result");
        };
        assert_eq!(r.columns[0].column_type, ColumnType::Json, "{name}");
        assert!(matches!(r.rows[0][0], Value::Json(_)), "{name}");
    }
    assert_eq!(m.count(), HTTP_TEST_FUNCTIONS.len());
}
#[test]
fn http_quoted_dependencies_and_optimizer_trace_are_redacted() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    s.current_database = "test".into();
    e.execute("CREATE FUNCTION `quoted wrapper`(p TEXT) RETURNS JSON RETURN HTTP_GET(p)")
        .unwrap();
    e.execute("CREATE FUNCTION outer_wrapper(p TEXT) RETURNS JSON RETURN `quoted wrapper`(p)")
        .unwrap();
    assert!(crate::engine::http::may_call(
        &e.db,
        "SELECT outer_wrapper('secret')"
    ));
    assert!(crate::engine::http::may_call(
        &e.db,
        "SELECT `quoted wrapper`('secret')"
    ));
    assert_eq!(
        crate::engine::logging::redact_sql_for_log("SELECT outer_wrapper('secret')"),
        "[redacted HTTP statement]"
    );
    e.execute_with_session(&mut s, "SET optimizer_trace='enabled=on'")
        .unwrap();
    let u = format!("{}?marker=HTTP_PRIVATE_MARKER", m.url("/echo"));
    e.execute_with_session(&mut s,&format!("SELECT JSON_EXTRACT(HTTP_GET('{u}'),'$.status') ORDER BY JSON_EXTRACT(HTTP_GET('{u}'),'$.elapsed_ms')")).unwrap();
    let result = e
        .execute_with_session(
            &mut s,
            "SELECT QUERY, TRACE FROM information_schema.optimizer_trace",
        )
        .unwrap()
        .materialize()
        .unwrap();
    let QueryResponse::ResultSet(r) = result else {
        panic!("expected trace");
    };
    for row in r.rows {
        for value in row {
            assert!(!value.to_display_string().contains("HTTP_PRIVATE_MARKER"));
        }
    }
}

#[test]
fn http_json_request_numbers_preserve_decimal_precision() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let u = m.url("/echo");
    let number = "123456789012345678901234567890.12345678901234567890";
    let native = format!("JSON_OBJECT('n',CAST('{number}' AS DECIMAL(50,20)))");
    for sql in [
        format!("SELECT HTTP_POST('{u}',{native})"),
        format!("SELECT HTTP_REQUEST('POST','{u}',JSON_OBJECT('json',{native}))"),
        format!("SELECT HTTP_REQUEST('POST','{u}','{{\"json\":{{\"n\":{number}}}}}')"),
    ] {
        let value = response(&e, &sql);
        let payload = echo(&value);
        assert!(
            payload["body"].as_str().unwrap().contains(number),
            "{payload}"
        );
    }
}
#[test]
fn http_union_stream_inherits_effects_authorization_and_budget() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let u = m.url("/echo");
    e.execute("CREATE TABLE union_inputs(id INT)").unwrap();
    e.execute("INSERT INTO union_inputs VALUES(1),(2)").unwrap();
    let mut session = SessionContext::default();
    session.current_database = "test".into();
    let sql=format!("SELECT HTTP_GET('{u}') FROM union_inputs WHERE id=1 UNION ALL SELECT HTTP_GET('{u}') FROM union_inputs WHERE id=2");
    let response = <StorageQueryEngine as QueryEngine>::execute(&e, &mut session, &sql).unwrap();
    let response = response.materialize().unwrap();
    let QueryResponse::ResultSet(r) = response else {
        panic!("expected rows");
    };
    assert_eq!(r.rows.len(), 2);
    assert_eq!(m.count(), 2);
}

#[test]
fn http_stored_function_union_keeps_live_security_context() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let u = m.url("/echo");
    e.execute(
        "CREATE FUNCTION union_http(p TEXT) RETURNS JSON SQL SECURITY DEFINER RETURN HTTP_GET(p)",
    )
    .unwrap();
    let result = rows(
        &e,
        &format!("SELECT union_http('{u}') UNION ALL SELECT union_http('{u}')"),
    );
    assert_eq!(result.len(), 2);
    assert_eq!(m.count(), 2);
}

#[test]
fn http_event_scheduler_installs_definer_context_and_shared_effect_scope() {
    let m = Mock::new();
    let dir = temp();
    let e = open_background_test_engine(dir.path());
    let u = m.url("/echo");
    e.execute("CREATE TABLE event_http_output(id INT PRIMARY KEY,r JSON)")
        .unwrap();
    e.execute(&format!("CREATE EVENT event_http_call ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 SECOND DO INSERT INTO event_http_output VALUES(1,HTTP_GET('{u}'))")).unwrap();
    assert_eq!(m.count(), 0, "definition must not execute HTTP");
    let until = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if rows(&e, "SELECT COUNT(*) FROM event_http_output")[0][0] == Value::Int(1) {
            break;
        }
        assert!(
            std::time::Instant::now() < until,
            "event failed to insert its HTTP result"
        );
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(m.count(), 1);
    e.shutdown().unwrap();
}

#[test]
fn http_trigger_dependencies_only_route_writes_and_writing_wrappers() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let u = m.url("/echo");
    e.execute("CREATE TABLE http_trigger_input(id INT PRIMARY KEY,r JSON)")
        .unwrap();
    e.execute(&format!("CREATE TRIGGER http_trigger BEFORE INSERT ON http_trigger_input FOR EACH ROW SET NEW.r=HTTP_GET('{u}')")).unwrap();
    e.execute("CREATE VIEW trigger_read_view AS SELECT * FROM http_trigger_input")
        .unwrap();
    e.execute("CREATE PROCEDURE trigger_read_proc() SELECT * FROM http_trigger_input")
        .unwrap();
    e.execute("CREATE PROCEDURE trigger_write_proc() INSERT INTO http_trigger_input(id) VALUES(1)")
        .unwrap();
    for sql in [
        "SELECT * FROM http_trigger_input",
        "SELECT * FROM trigger_read_view",
        "CALL trigger_read_proc()",
        "SELECT 'INSERT' FROM http_trigger_input",
    ] {
        assert!(
            !crate::engine::http::may_call(&e.db, sql),
            "read was classified as HTTP: {sql}"
        );
    }
    for sql in [
        "INSERT INTO http_trigger_input(id) VALUES(2)",
        "CALL trigger_write_proc()",
    ] {
        assert!(
            crate::engine::http::may_call(&e.db, sql),
            "write missed HTTP dependency: {sql}"
        );
    }
    assert_eq!(m.count(), 0);
}
#[test]
fn http_sensitivity_memo_follows_dependency_cache_changes() {
    let dir = temp();
    let e = open_test_engine(dir.path());
    let sql = "SELECT later_wrapper('secret')";
    let memo = crate::engine::http::SensitivityMemo::new(sql);
    assert!(!crate::engine::http::may_call(&e.db, sql));
    assert!(!memo.is_sensitive(sql));
    e.execute("CREATE FUNCTION later_wrapper(p TEXT) RETURNS JSON RETURN HTTP_GET(p)")
        .unwrap();
    // Refreshing the dependency cache must invalidate the memoized verdict.
    assert!(crate::engine::http::may_call(&e.db, sql));
    assert!(memo.is_sensitive(sql));
    assert_eq!(memo.is_sensitive(sql), crate::engine::http::sensitive(sql));
    let direct = "SELECT HTTP_GET('http://127.0.0.1/')";
    assert!(crate::engine::http::SensitivityMemo::new(direct).is_sensitive(direct));
}
#[test]
fn http_top_level_batch_statements_have_independent_send_budgets() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let u = m.url("/echo");
    crate::util::env::with_vars(&[("MYDB_HTTP_MAX_CALLS_PER_STATEMENT", Some("1"))], || {
        e.execute(&format!(
            "SELECT HTTP_GET('{u}'); SELECT HTTP_GET('{u}'); SELECT HTTP_GET('{u}')"
        ))
        .unwrap();
    });
    assert_eq!(m.count(), 3);
}

#[test]
fn http_stored_function_effects_survive_projection_pruning_and_cte_reuse() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let u = m.url("/echo");
    e.execute("CREATE TABLE effect_inputs(id INT,url TEXT)")
        .unwrap();
    for id in 1..=3 {
        e.execute(&format!("INSERT INTO effect_inputs VALUES({id},'{u}')"))
            .unwrap();
    }
    e.execute("CREATE FUNCTION effect_http(p TEXT) RETURNS JSON DETERMINISTIC RETURN HTTP_GET(p)")
        .unwrap();
    e.execute(
        "CREATE FUNCTION effect_outer(p TEXT) RETURNS JSON DETERMINISTIC RETURN effect_http(p)",
    )
    .unwrap();
    for name in ["HTTP_GET", "effect_http", "effect_outer"] {
        let before = m.count();
        let result = rows(
            &e,
            &format!("SELECT COUNT(*) FROM (SELECT {name}(url) AS r FROM effect_inputs) d"),
        );
        assert_eq!(result[0][0], Value::Int(3));
        assert_eq!(m.count() - before, 3, "{name} effects were pruned");
    }
    let before = m.count();
    let result=rows(&e,"WITH c AS (SELECT effect_outer(url) AS r FROM effect_inputs) SELECT COUNT(*) FROM c a JOIN c b ON 1=1");
    assert_eq!(result[0][0], Value::Int(9));
    assert_eq!(m.count() - before, 3, "effectful CTE must materialize once");
}

#[test]
fn http_combined_content_codings_are_preserved_without_partial_decoding() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let r = response(&e, &format!("SELECT HTTP_GET('{}')", m.url("/stacked")));
    assert_eq!(r["status"], 200);
    assert_eq!(r["content_encoding"], "gzip,deflate");
    assert_eq!(r["content_decoded"], false);
    assert_eq!(r["headers"]["content-encoding"], json!(["gzip", "deflate"]));
}
#[test]
fn http_sql_prepare_resolves_scheduling_and_redacts_bound_trace() {
    let m = Mock::new();
    let dir = temp();
    let e = open_test_engine(dir.path());
    let mut s = SessionContext::default();
    s.current_database = "test".into();
    let text = format!(
        "SELECT HTTP_GET('{}?secret=BOUND_HTTP_SECRET')",
        m.url("/echo")
    );
    e.execute_with_session(
        &mut s,
        &format!("PREPARE hp FROM '{}'", text.replace('\'', "''")),
    )
    .unwrap();
    assert_eq!(m.count(), 0);
    assert_eq!(crate::engine::http::bound_sql(&s, "EXECUTE HP"), text);
    e.execute_with_session(&mut s, "PREPARE plain FROM 'SELECT 1'")
        .unwrap();
    assert!(!crate::engine::http::mentions(
        crate::engine::http::bound_sql(&s, "EXECUTE plain")
    ));
    e.execute_with_session(&mut s, "SET optimizer_trace='enabled=on'")
        .unwrap();
    e.execute_with_session(&mut s, "EXECUTE hp").unwrap();
    assert_eq!(m.count(), 1);
    let QueryResponse::ResultSet(trace) = e
        .execute_with_session(
            &mut s,
            "SELECT QUERY,TRACE FROM information_schema.optimizer_trace",
        )
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("expected trace");
    };
    assert!(!trace.rows.is_empty());
    for row in trace.rows {
        for v in row {
            assert!(!v.to_display_string().contains("BOUND_HTTP_SECRET"));
        }
    }
}
