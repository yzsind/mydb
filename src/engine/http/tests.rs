//! Boundary tests for HTTP request and node-policy contracts.
use super::*;
use crate::types::Value;
use serde_json::json;
fn request(options: serde_json::Value) -> Result<Option<request::Request>, QueryError> {
    request::prepare(
        "HTTP_REQUEST",
        &[
            Value::String("POST".into()),
            Value::String("http://127.0.0.1/a".into()),
            Value::String(options.to_string()),
        ],
        &config::Limits::load()?,
    )
}
#[test]
fn http_options_preserve_json_null_and_sql_null() {
    let r = request(json!({"json":null})).unwrap().unwrap();
    assert_eq!(r.body.unwrap(), b"null");
    assert!(
        request::prepare("HTTP_GET", &[Value::Null], &config::Limits::load().unwrap())
            .unwrap()
            .is_none()
    );
    assert!(request(json!({"json":null,"body":""})).is_err());
}
#[test]
fn http_strict_json_rejects_duplicate_depth_and_trailing_tokens() {
    for s in ["{\"x\":1,\"x\":2}", "{}{}", "[", "NaN"] {
        assert!(json::parse_json(s, 65536).is_err());
    }
    let s = format!("{}0{}", "[".repeat(33), "]".repeat(33));
    assert!(json::parse_json(&s, 65536).is_err());
    assert!(json::parse_json("{}", 1).is_err());
}
#[test]
fn http_url_validation_never_interprets_non_http_schemes_or_userinfo() {
    for s in [
        "file:///etc/passwd",
        "ftp://example.com/a",
        "http://user:pass@example.com/",
        "http://example.com/#part",
        "http://example.com/\r\nx",
    ] {
        assert!(config::parse_url(s).is_err(), "{s}");
    }
    assert!(config::parse_url("http://[::1]:8000/a?q=1").is_ok());
}
#[test]
fn http_explicit_egress_checks_origin_port_and_mapped_ipv6() {
    let p:config::Policy=serde_json::from_value(json!({"version":1,"egress":{"allowed_origins":["http://127.0.0.1:8000"],"allow_plaintext_origins":["http://127.0.0.1:8000"],"private_destinations":["127.0.0.1/32"]}})).unwrap();
    assert!(p
        .check_url(&config::parse_url("http://127.0.0.1:8000/x?q=secret").unwrap())
        .is_ok());
    assert!(p
        .check_url(&config::parse_url("http://127.0.0.1:8001/").unwrap())
        .is_err());
    assert!(p.check_ip("::ffff:127.0.0.1".parse().unwrap()).is_ok());
    for s in [
        "169.254.169.254",
        "10.0.0.1",
        "::1",
        "::ffff:10.0.0.1",
        "64:ff9b::a00:1",
    ] {
        assert!(p.check_ip(s.parse().unwrap()).is_err(), "{s}");
    }
    let p = config::Policy::default();
    assert!(p.check_ip("169.254.169.254".parse().unwrap()).is_ok());
}
#[test]
fn http_limits_are_strict_and_call_limits_only_tighten() {
    for v in [
        json!({"timeout_ms":0}),
        json!({"connect_timeout_ms":5001}),
        json!({"read_timeout_ms":10001}),
        json!({"max_redirects":6}),
        json!({"max_response_bytes":0}),
        json!({"max_response_bytes":4*1024*1024+1}),
    ] {
        assert!(request(v).is_err());
    }
    crate::util::env::with_vars(&[("MYDB_HTTP_MAX_INFLIGHT", Some("garbage"))], || {
        assert!(config::Limits::load().is_err())
    });
}
#[test]
fn http_body_types_headers_and_auth_are_strict() {
    for v in [
        json!({"body_base64":"???"}),
        json!({"body":2}),
        json!({"query":{"q":3}}),
        json!({"headers":{"X-A":[1]}}),
        json!({"headers":{"Upgrade":"websocket"}}),
        json!({"auth":{"type":"basic","username":"a:b","password":"p"}}),
        json!({"multipart":[{"name":"x\r\ny","text":"data"}]}),
    ] {
        assert!(request(v).is_err());
    }
    let r = request(json!({"auth":{"type":"basic","username":"alice","password":"pw"}}))
        .unwrap()
        .unwrap();
    assert_eq!(
        r.headers.get("authorization").unwrap(),
        "Basic YWxpY2U6cHc="
    );
}
#[test]
fn http_policy_failed_reload_keeps_the_published_generation() {
    std::fs::create_dir_all("/tmp/mydb/http-functions").unwrap();
    let dir = tempfile::Builder::new()
        .prefix("policy-")
        .tempdir_in("/tmp/mydb/http-functions")
        .unwrap();
    let p = dir.path().join("policy.json");
    std::fs::write(&p, r#"{"version":1,"egress":{"allowed_origins":[]}}"#).unwrap();
    crate::util::env::with_vars(
        &[("MYDB_HTTP_LOCAL_CONFIG", Some(p.to_str().unwrap()))],
        || {
            let first = config::reload().unwrap();
            assert!(first.current());
            std::fs::write(&p, "bad").unwrap();
            assert!(config::reload().is_err());
            assert!(first.current());
            std::fs::write(&p, r#"{"version":1}"#).unwrap();
            let next = config::reload().unwrap();
            assert!(!first.current());
            assert!(next.current());
        },
    );
}

#[test]
fn http_tls_errors_survive_io_wrappers_without_message_matching() {
    let tls = tokio_rustls::rustls::Error::InvalidCertificate(
        tokio_rustls::rustls::CertificateError::BadEncoding,
    );
    let nested = std::io::Error::other(std::io::Error::new(std::io::ErrorKind::InvalidData, tls));
    assert!(runtime::tls_source(&nested, 0));
    assert!(!runtime::tls_source(
        &std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
        0
    ));
}
#[test]
fn http_text_detection_keeps_node_controls_outside_request_admission() {
    for sql in [
        "SET GLOBAL mydb_http_enabled=OFF",
        "SHOW HTTP STATUS",
        "SELECT http_get FROM t",
    ] {
        assert!(!mentions(sql), "{sql}");
    }
    for sql in [
        "SELECT HTTP_GET ('x')",
        "SELECT `http_get`('x')",
        "SELECT HTTP_POST/**/('x','y')",
    ] {
        assert!(mentions(sql), "{sql}");
    }
}

#[test]
fn http_null_inputs_need_no_runtime_or_network_admission() {
    crate::util::env::with_vars(&[("MYDB_HTTP_MAX_INFLIGHT", Some("invalid"))], || {
        assert_eq!(call("HTTP_GET", &[Value::Null]).unwrap(), Value::Null);
        assert_eq!(
            call(
                "HTTP_REQUEST",
                &[Value::Null, Value::String("http://127.0.0.1/".into())]
            )
            .unwrap(),
            Value::Null
        );
        assert!(call("HTTP_REQUEST", &[Value::Null]).is_err());
    });
}
