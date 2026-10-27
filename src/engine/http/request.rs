//! Validate and encode requests before any network dispatch.
use super::{
    config::{self, Limits},
    error,
    json::parse_json,
};
use crate::types::{QueryError, Value};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE},
    Method, Url,
};
use serde::Deserialize;
use serde_json::Value as Json;
use std::{collections::BTreeMap, time::Duration};
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    #[serde(default)]
    headers: BTreeMap<String, Json>,
    #[serde(default)]
    query: BTreeMap<String, Json>,
    body: Option<String>,
    // Presence is checked on the original map so JSON null remains a body.
    json: Option<Json>,
    body_base64: Option<String>,
    form: Option<Vec<Pair>>,
    multipart: Option<Vec<Part>>,
    auth: Option<Auth>,
    timeout_ms: Option<u64>,
    connect_timeout_ms: Option<u64>,
    read_timeout_ms: Option<u64>,
    #[serde(default)]
    follow_redirects: bool,
    max_redirects: Option<usize>,
    max_response_bytes: Option<usize>,
    response_encoding: Option<String>,
    on_error: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Pair {
    name: String,
    value: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Part {
    name: String,
    text: Option<String>,
    base64: Option<String>,
    filename: Option<String>,
    content_type: Option<String>,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
enum Auth {
    None,
    Basic { username: String, password: String },
    Bearer { token: String },
}
pub(crate) struct Request {
    pub method: Method,
    pub url: Url,
    pub headers: HeaderMap,
    pub body: Option<Vec<u8>>,
    pub timeout: Duration,
    pub connect: Duration,
    pub read: Duration,
    pub redirects: usize,
    pub response_limit: usize,
    pub encoding: String,
    pub return_errors: bool,
}
fn invalid(message: &str) -> QueryError {
    error("ER_HTTP_INVALID_ARGUMENT", message)
}
fn text(v: &Value) -> Result<&str, QueryError> {
    crate::types::value_str(v).ok_or_else(|| invalid("method and URL require strings"))
}
fn strings(v: &Json) -> Result<Vec<&str>, QueryError> {
    match v {
        Json::String(s) => Ok(vec![s]),
        Json::Array(a) => a
            .iter()
            .map(|x| {
                x.as_str()
                    .ok_or_else(|| invalid("header/query arrays require strings"))
            })
            .collect(),
        _ => Err(invalid(
            "header/query values require strings or string arrays",
        )),
    }
}
fn bounded_decode(s: &str, limit: usize) -> Result<Vec<u8>, QueryError> {
    if s.len() > limit.saturating_add(2) / 3 * 4 {
        return Err(error(
            "ER_HTTP_BUDGET_EXCEEDED",
            "encoded body exceeds request limit",
        ));
    }
    let b = BASE64
        .decode(s)
        .map_err(|_| invalid("invalid standard Base64 body"))?;
    if b.len() > limit {
        return Err(error(
            "ER_HTTP_BUDGET_EXCEEDED",
            "body exceeds request limit",
        ));
    }
    Ok(b)
}
fn duration(v: Option<u64>, default: u64) -> Result<Duration, QueryError> {
    let v = v.unwrap_or(default);
    if v == 0 || v > default {
        return Err(invalid("call timeouts can only tighten defaults"));
    }
    Ok(Duration::from_millis(v))
}
fn quoted(s: &str) -> Result<String, QueryError> {
    if s.bytes().any(|b| b < 32 || b == 127) {
        return Err(invalid("invalid multipart disposition"));
    }
    Ok(s.replace('\\', "\\\\").replace('"', "\\\""))
}
fn append(out: &mut Vec<u8>, s: &[u8], limit: usize) -> Result<(), QueryError> {
    if out.len().saturating_add(s.len()) > limit {
        return Err(error(
            "ER_HTTP_BUDGET_EXCEEDED",
            "request body exceeds limit",
        ));
    }
    out.extend_from_slice(s);
    Ok(())
}
pub(crate) fn null_input(name: &str, args: &[Value]) -> Result<bool, QueryError> {
    let generic = name.eq_ignore_ascii_case("HTTP_REQUEST");
    let body = ["HTTP_POST", "HTTP_PUT", "HTTP_PATCH"]
        .iter()
        .any(|n| name.eq_ignore_ascii_case(n));
    let required = if generic || body { 2 } else { 1 };
    if !(required..=required + 1).contains(&args.len()) {
        return Err(invalid("wrong HTTP function argument count"));
    }
    Ok(matches!(args[usize::from(generic)], Value::Null)
        || (generic && matches!(args[0], Value::Null)))
}

pub(crate) fn prepare(
    name: &str,
    args: &[Value],
    limits: &Limits,
) -> Result<Option<Request>, QueryError> {
    let generic = name.eq_ignore_ascii_case("HTTP_REQUEST");
    let with_body = ["HTTP_POST", "HTTP_PUT", "HTTP_PATCH"]
        .iter()
        .any(|s| name.eq_ignore_ascii_case(s));
    let required = if generic || with_body { 2 } else { 1 };
    if !(required..=required + 1).contains(&args.len()) {
        return Err(invalid("wrong HTTP function argument count"));
    }
    let url_pos = usize::from(generic);
    if matches!(args[url_pos], Value::Null) || (generic && matches!(args[0], Value::Null)) {
        return Ok(None);
    }
    let mut url = config::parse_url(text(&args[url_pos])?)?;
    let method = if generic {
        Method::from_bytes(text(&args[0])?.as_bytes())
            .map_err(|_| invalid("invalid HTTP method"))?
    } else {
        Method::from_bytes(&name.as_bytes()[5..].to_ascii_uppercase())
            .map_err(|_| invalid("invalid HTTP method"))?
    };
    if method == Method::CONNECT {
        return Err(invalid("CONNECT tunnels are unsupported"));
    }
    let options_text: std::borrow::Cow<'_, str> = match args.get(required) {
        None | Some(Value::Null) => std::borrow::Cow::Borrowed("{}"),
        Some(Value::Json(v)) => {
            if v.len() > 65536 {
                return Err(invalid("options exceed 64 KiB"));
            }
            crate::engine::json::JsonbRef::new(v)
                .map_err(|_| invalid("invalid native JSON options"))?;
            std::borrow::Cow::Owned(crate::engine::json::to_text(v))
        }
        Some(v) => std::borrow::Cow::Borrowed(text(v)?),
    };
    let opts = parse_json(&options_text, 65536)?;
    let map = opts
        .as_object()
        .ok_or_else(|| invalid("options require a JSON object"))?;
    let body_count = ["body", "json", "body_base64", "form", "multipart"]
        .iter()
        .filter(|k| map.contains_key(**k))
        .count();
    if body_count > 1 || (with_body && !matches!(args[1], Value::Null) && body_count > 0) {
        return Err(invalid("request body modes are mutually exclusive"));
    }
    for (k, v) in map {
        if v.is_null() && k != "json" {
            return Err(invalid("option fields cannot be JSON null"));
        }
    }
    let o: Options = serde_json::from_value(opts.clone())
        .map_err(|_| invalid("invalid or unknown HTTP option"))?;
    for (name, v) in &o.query {
        for s in strings(v)? {
            url.query_pairs_mut().append_pair(name, s);
        }
    }
    if url.as_str().len() > 16384 {
        return Err(invalid("URL with query exceeds 16 KiB"));
    }
    let mut headers = HeaderMap::new();
    let mut bytes = 0usize;
    for (name, v) in &o.headers {
        let key = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| invalid("invalid HTTP header name"))?;
        if matches!(
            key.as_str(),
            "host"
                | "content-length"
                | "transfer-encoding"
                | "connection"
                | "upgrade"
                | "proxy-authorization"
                | "proxy-connection"
                | "te"
                | "trailer"
                | "keep-alive"
        ) {
            return Err(invalid("transport-owned header cannot be set"));
        }
        if headers.contains_key(&key) {
            return Err(invalid(
                "case-insensitive duplicate header names require one array",
            ));
        }
        for value in strings(v)? {
            if value.bytes().any(|b| b == b'\r' || b == b'\n') {
                return Err(invalid("CR/LF in HTTP header"));
            }
            bytes = bytes.saturating_add(name.len() + value.len() + 4);
            if bytes > limits.headers {
                return Err(error(
                    "ER_HTTP_BUDGET_EXCEEDED",
                    "request headers exceed limit",
                ));
            }
            let mut value =
                HeaderValue::from_str(value).map_err(|_| invalid("invalid HTTP header value"))?;
            value.set_sensitive(true);
            headers.append(key.clone(), value);
        }
    }
    if let Some(auth) = o.auth {
        let value = match auth {
            Auth::None => None,
            Auth::Bearer { token } => Some(format!("Bearer {token}")),
            Auth::Basic { username, password } => {
                if username.contains(':') {
                    return Err(invalid("Basic username cannot contain colon"));
                }
                Some(format!(
                    "Basic {}",
                    BASE64.encode(format!("{username}:{password}"))
                ))
            }
        };
        if let Some(value) = value {
            if headers.contains_key(AUTHORIZATION) {
                return Err(invalid("auth conflicts with Authorization header"));
            }
            let mut h = HeaderValue::from_str(&value)
                .map_err(|_| invalid("invalid authentication header"))?;
            h.set_sensitive(true);
            headers.insert(AUTHORIZATION, h);
        }
    }
    let mut content_type = None;
    let body = if with_body && !matches!(args[1], Value::Null) {
        match &args[1] {
            Value::Json(j) => {
                content_type = Some("application/json".to_owned());
                crate::engine::json::JsonbRef::new(j)
                    .map_err(|_| invalid("invalid native JSON body"))?;
                Some(crate::engine::json::to_text(j).into_bytes())
            }
            Value::Bytes(b) => Some(b.clone()),
            v => Some(text(v)?.as_bytes().to_vec()),
        }
    } else if let Some(s) = o.body {
        Some(s.into_bytes())
    } else if opts.as_object().unwrap().contains_key("json") {
        content_type = Some("application/json".to_owned());
        #[derive(Deserialize)]
        struct RawBody<'a> {
            #[serde(borrow)]
            json: &'a serde_json::value::RawValue,
        }
        // Validation above rejects duplicate keys and excessive nesting;
        // RawValue preserves exact numeric lexemes instead of rounding to f64.
        let raw: RawBody<'_> =
            serde_json::from_str(&options_text).map_err(|_| invalid("invalid JSON body"))?;
        Some(raw.json.get().as_bytes().to_vec())
    } else if let Some(s) = o.body_base64 {
        Some(bounded_decode(&s, limits.request)?)
    } else if let Some(form) = o.form {
        content_type = Some("application/x-www-form-urlencoded".to_owned());
        let mut u = Url::parse("http://form.invalid").unwrap();
        for p in form {
            u.query_pairs_mut().append_pair(&p.name, &p.value);
        }
        Some(u.query().unwrap_or("").as_bytes().to_vec())
    } else if let Some(parts) = o.multipart {
        use rand::RngCore;
        let mut random = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut random);
        let boundary = format!(
            "mydb{}",
            BASE64.encode(random).replace(['+', '/', '='], "x")
        );
        content_type = Some(format!("multipart/form-data; boundary={boundary}"));
        let mut data = Vec::new();
        for p in parts {
            if usize::from(p.text.is_some()) + usize::from(p.base64.is_some()) != 1 {
                return Err(invalid(
                    "multipart part requires exactly one of text/base64",
                ));
            }
            let mut head = format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{}\"",
                quoted(&p.name)?
            );
            if let Some(f) = p.filename {
                head += &format!("; filename=\"{}\"", quoted(&f)?);
            }
            head += "\r\n";
            if let Some(ct) = p.content_type {
                HeaderValue::from_str(&ct).map_err(|_| invalid("invalid part Content-Type"))?;
                if ct.contains(['\r', '\n']) {
                    return Err(invalid("invalid part Content-Type"));
                }
                head += &format!("Content-Type: {ct}\r\n");
            }
            head += "\r\n";
            append(&mut data, head.as_bytes(), limits.request)?;
            let b = if let Some(s) = p.text {
                s.into_bytes()
            } else {
                bounded_decode(p.base64.as_deref().unwrap(), limits.request)?
            };
            append(&mut data, &b, limits.request)?;
            append(&mut data, b"\r\n", limits.request)?;
        }
        append(
            &mut data,
            format!("--{boundary}--\r\n").as_bytes(),
            limits.request,
        )?;
        Some(data)
    } else {
        None
    };
    if body.as_ref().is_some_and(|b| b.len() > limits.request) {
        return Err(error(
            "ER_HTTP_BUDGET_EXCEEDED",
            "request body exceeds limit",
        ));
    }
    if let Some(ct) = content_type {
        if ct.starts_with("multipart/") && headers.contains_key(CONTENT_TYPE) {
            return Err(invalid(
                "multipart Content-Type is generated with its boundary",
            ));
        }
        if !headers.contains_key(CONTENT_TYPE) {
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_str(&ct).map_err(|_| invalid("invalid Content-Type"))?,
            );
        }
    }
    let header_bytes: usize = headers
        .iter()
        .map(|(k, v)| k.as_str().len() + v.as_bytes().len() + 4)
        .sum();
    if header_bytes > limits.headers {
        return Err(error(
            "ER_HTTP_BUDGET_EXCEEDED",
            "request headers exceed limit",
        ));
    }
    let encoding = o.response_encoding.unwrap_or_else(|| "auto".into());
    if !matches!(encoding.as_str(), "auto" | "utf8" | "base64") {
        return Err(invalid("invalid response_encoding"));
    }
    let on_error = o.on_error.unwrap_or_else(|| "raise".into());
    if !matches!(on_error.as_str(), "raise" | "return") {
        return Err(invalid("invalid on_error"));
    }
    let redirects = o.max_redirects.unwrap_or(5);
    if redirects > 5 {
        return Err(invalid("max_redirects exceeds 5"));
    }
    let response_limit = o.max_response_bytes.unwrap_or(limits.response);
    if response_limit == 0 || response_limit > limits.response {
        return Err(invalid("max_response_bytes can only tighten node limit"));
    }
    let _ = o.json;
    Ok(Some(Request {
        method,
        url,
        headers,
        body,
        timeout: duration(o.timeout_ms, 30000)?,
        connect: duration(o.connect_timeout_ms, 5000)?,
        read: duration(o.read_timeout_ms, 10000)?,
        redirects: if o.follow_redirects { redirects } else { 0 },
        response_limit,
        encoding,
        return_errors: on_error == "return",
    }))
}
