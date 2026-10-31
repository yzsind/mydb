//! Bounded asynchronous transport with synchronous SQL cancellation polling.
use super::{
    config::{self, Limits, Policy},
    error,
    request::{self, Request},
    scope,
};
use crate::{
    engine::{
        auth::AuthorizationContext,
        executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge},
    },
    storage::Database,
    types::{QueryError, Value},
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use reqwest::{
    header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, LOCATION},
    Client, Method,
};
use serde_json::{json, Value as Json};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

static MEMORY: AtomicUsize = AtomicUsize::new(0);
static TOTAL: AtomicU64 = AtomicU64::new(0);
static ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static FAILED: AtomicU64 = AtomicU64::new(0);
static ELAPSED: AtomicU64 = AtomicU64::new(0);
static STATUS: [AtomicU64; 6] = [const { AtomicU64::new(0) }; 6];
static ERRORS: OnceLock<Mutex<BTreeMap<&'static str, u64>>> = OnceLock::new();

pub(crate) struct Memory {
    charge: ControlledMemoryCharge,
    limit: usize,
}
impl Memory {
    fn new(limit: usize) -> Result<Self, QueryError> {
        Ok(Self {
            charge: ControlledMemoryCharge::try_new(ControlledMemoryCategory::Executor, 0)?,
            limit,
        })
    }
    fn retain(&mut self, bytes: usize) {
        let released = self.charge.bytes().saturating_sub(bytes);
        self.charge.shrink(released);
        MEMORY.fetch_sub(released, Ordering::AcqRel);
    }
    fn grow(&mut self, bytes: usize) -> Result<(), QueryError> {
        MEMORY
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|n| *n <= self.limit)
            })
            .map_err(|_| error("ER_HTTP_BUDGET_EXCEEDED", "HTTP memory budget exhausted"))?;
        if let Err(e) = self.charge.try_grow(bytes) {
            MEMORY.fetch_sub(bytes, Ordering::AcqRel);
            return Err(e);
        }
        Ok(())
    }
}
impl Drop for Memory {
    fn drop(&mut self) {
        MEMORY.fetch_sub(self.charge.bytes(), Ordering::AcqRel);
    }
}

#[derive(Default)]
struct Gate {
    inflight: usize,
    waiting: usize,
    users: HashMap<String, UserGate>,
}
struct UserGate {
    inflight: usize,
    tokens: f64,
    updated: Instant,
    last: Instant,
}
static GATE: OnceLock<Mutex<Gate>> = OnceLock::new();
fn gate() -> &'static Mutex<Gate> {
    GATE.get_or_init(Default::default)
}
struct Waiting;
impl Drop for Waiting {
    fn drop(&mut self) {
        let mut g = gate().lock().unwrap_or_else(|e| e.into_inner());
        g.waiting -= 1;
    }
}
struct Permit {
    user: String,
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut g = gate().lock().unwrap_or_else(|e| e.into_inner());
        g.inflight -= 1;
        if let Some(u) = g.users.get_mut(&self.user) {
            u.inflight -= 1;
            u.last = Instant::now();
        }
    }
}
async fn acquire(d: &Dispatch) -> Result<Permit, Failure> {
    {
        let mut g = gate().lock().unwrap_or_else(|e| e.into_inner());
        if g.waiting >= d.limits.queue {
            return Err(hard(
                error("ER_HTTP_QUEUE_FULL", "HTTP queue full"),
                "queue",
            ));
        }
        g.waiting += 1;
    }
    let _waiting = Waiting;
    loop {
        d.check()?;
        {
            let mut g = gate().lock().unwrap_or_else(|e| e.into_inner());
            if g.users.len() >= 1024 && !g.users.contains_key(&d.principal) {
                g.users
                    .retain(|_, u| u.inflight > 0 || u.last.elapsed() < Duration::from_secs(60));
                if g.users.len() >= 1024 {
                    return Err(hard(
                        error("ER_HTTP_QUEUE_FULL", "too many active HTTP accounts"),
                        "queue",
                    ));
                }
            }
            let global = g.inflight < d.limits.inflight;
            let now = Instant::now();
            let u = g
                .users
                .entry(d.principal.clone())
                .or_insert_with(|| UserGate {
                    inflight: 0,
                    tokens: d.rpm.unwrap_or(0) as f64,
                    updated: now,
                    last: now,
                });
            if let Some(rpm) = d.rpm {
                u.tokens = (u.tokens
                    + now.duration_since(u.updated).as_secs_f64() * rpm as f64 / 60.0)
                    .min(rpm as f64);
            }
            u.updated = now;
            u.last = now;
            if d.rpm.is_some() && u.tokens < 1.0 {
                return Err(hard(
                    error(
                        "ER_HTTP_RATE_LIMITED",
                        "HTTP account request rate exhausted",
                    ),
                    "admission",
                ));
            }
            if global && u.inflight < d.user_inflight {
                if d.rpm.is_some() {
                    u.tokens -= 1.0;
                }
                u.inflight += 1;
                g.inflight += 1;
                return Ok(Permit {
                    user: d.principal.clone(),
                });
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
struct Control {
    owner: std::sync::Weak<Database>,
    namespace: usize,
    closed: AtomicBool,
    pending: AtomicUsize,
}
static CONTROLS: OnceLock<Mutex<HashMap<usize, Arc<Control>>>> = OnceLock::new();
fn controls() -> &'static Mutex<HashMap<usize, Arc<Control>>> {
    CONTROLS.get_or_init(Default::default)
}
fn control(db: &Arc<Database>) -> Result<Arc<Control>, QueryError> {
    let key = Arc::as_ptr(db) as usize;
    let mut map = controls().lock().unwrap_or_else(|e| e.into_inner());
    if map.len() >= 1024 && !map.contains_key(&key) {
        map.retain(|_, c| c.owner.strong_count() > 0);
    }
    if map.len() >= 1024 && !map.contains_key(&key) {
        return Err(error(
            "ER_HTTP_BUDGET_EXCEEDED",
            "too many HTTP database runtimes",
        ));
    }
    Ok(map
        .entry(key)
        .or_insert_with(|| {
            static NEXT: AtomicUsize = AtomicUsize::new(1);
            Arc::new(Control {
                owner: Arc::downgrade(db),
                namespace: NEXT.fetch_add(1, Ordering::AcqRel),
                closed: AtomicBool::new(false),
                pending: AtomicUsize::new(0),
            })
        })
        .clone())
}
pub(crate) fn shutdown_database(db: &Arc<Database>) -> Result<(), QueryError> {
    super::sql::retire(db);
    let key = Arc::as_ptr(db) as usize;
    let c = controls()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .cloned();
    if let Some(c) = c {
        c.closed.store(true, Ordering::Release);
        let end = Instant::now() + Duration::from_secs(2);
        while c.pending.load(Ordering::Acquire) > 0 && Instant::now() < end {
            std::thread::sleep(Duration::from_millis(5));
        }
        if c.pending.load(Ordering::Acquire) > 0 {
            return Err(error("ER_HTTP_TIMEOUT", "HTTP shutdown did not drain"));
        }
        // Retain the closed lifetime until its Database is dropped. A racing
        // statement must not recreate an enabled runtime during shutdown.
        if let Some(Ok(rt)) = RUNTIME.get() {
            rt.clients
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|k, _| k.namespace != c.namespace);
        }
    }
    Ok(())
}
struct Pending(Arc<Control>);
impl Drop for Pending {
    fn drop(&mut self) {
        self.0.pending.fetch_sub(1, Ordering::AcqRel);
    }
}
#[derive(Hash, PartialEq, Eq)]
struct ClientKey {
    namespace: usize,
    origin: String,
    policy: u64,
    connect: Duration,
    read: Duration,
}
struct Runtime {
    rt: tokio::runtime::Runtime,
    clients: Mutex<HashMap<ClientKey, (Instant, Client, Memory)>>,
}
static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();
fn runtime() -> Result<&'static Runtime, QueryError> {
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("mydb-http-io")
                .enable_all()
                .build()
                .map(|rt| Runtime {
                    rt,
                    clients: Mutex::new(HashMap::new()),
                })
                .map_err(|_| "cannot initialize HTTP runtime".to_owned())
        })
        .as_ref()
        .map_err(|_| error("ER_HTTP_DISABLED", "cannot initialize HTTP runtime"))
}
struct Dispatch {
    db: Arc<Database>,
    auth: AuthorizationContext,
    policy: Arc<Policy>,
    effects: Arc<scope::Effects>,
    control: Arc<Control>,
    limits: Limits,
    principal: String,
    user_inflight: usize,
    rpm: Option<u64>,
    calls: usize,
    switch: u64,
    token: Option<crate::engine::cancel::QueryCancelToken>,
    aborted: Arc<AtomicBool>,
}
impl Dispatch {
    fn check(&self) -> Result<(), Failure> {
        if self.aborted.load(Ordering::Acquire) {
            return Err(hard(crate::engine::cancel::interrupted_error(), "cancel"));
        }
        if let Some(t) = &self.token {
            t.check().map_err(|e| hard(e, "cancel"))?;
        }
        if self.control.closed.load(Ordering::Acquire)
            || !config::enabled()
            || config::switch_generation() != self.switch
            || !self.policy.current()
        {
            return Err(hard(
                error(
                    "ER_HTTP_DISABLED",
                    "HTTP disabled, policy changed, or database closed",
                ),
                "policy",
            ));
        }
        super::authorize(&self.db, &self.auth, "HTTP_USE").map_err(|e| hard(e, "authorization"))
    }
    fn sent(&self) -> Result<(), Failure> {
        self.effects
            .attempts
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.calls).then_some(n + 1)
            })
            .map_err(|_| {
                hard(
                    error(
                        "ER_HTTP_BUDGET_EXCEEDED",
                        "HTTP statement send budget exhausted",
                    ),
                    "dispatch",
                )
            })?;
        ATTEMPTS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
struct Failure {
    error: QueryError,
    code: &'static str,
    stage: &'static str,
    ambiguous: bool,
    returnable: bool,
}
fn hard(e: QueryError, stage: &'static str) -> Failure {
    let code = e.symbol().unwrap_or("ER_HTTP_TRANSPORT");
    Failure {
        error: e,
        code,
        stage,
        ambiguous: false,
        returnable: false,
    }
}
fn fail(code: &'static str, stage: &'static str, ambiguous: bool) -> Failure {
    Failure {
        error: error(code, "HTTP request failed"),
        code,
        stage,
        ambiguous,
        returnable: true,
    }
}
pub(super) fn tls_source(error: &(dyn std::error::Error + 'static), depth: usize) -> bool {
    if depth > 16 {
        return false;
    }
    if error
        .downcast_ref::<tokio_rustls::rustls::Error>()
        .is_some()
    {
        return true;
    }
    // io::Error::source can skip the wrapped error itself. Inspect get_ref
    // too, preserving typed TLS classification through nested I/O wrappers.
    if let Some(inner) = error
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::get_ref)
    {
        if tls_source(inner, depth + 1) {
            return true;
        }
    }
    error
        .source()
        .is_some_and(|inner| tls_source(inner, depth + 1))
}
fn transport(e: reqwest::Error, stage: &'static str) -> Failure {
    let tls = tls_source(&e, 0);
    let code = if e.is_timeout() {
        "ER_HTTP_TIMEOUT"
    } else if tls {
        "ER_HTTP_TLS"
    } else if e.is_connect() {
        "ER_HTTP_CONNECT"
    } else if e.is_decode() {
        "ER_HTTP_INVALID_RESPONSE"
    } else {
        "ER_HTTP_TRANSPORT"
    };
    let stage = if tls {
        "tls"
    } else if e.is_connect() {
        "connect"
    } else {
        stage
    };
    fail(code, stage, !e.is_connect() && !e.is_builder())
}
async fn client(d: &Dispatch, r: &Request, url: &reqwest::Url) -> Result<Client, Failure> {
    d.check()?;
    d.policy.check_url(url).map_err(|e| hard(e, "policy"))?;
    let rt = runtime().map_err(|e| hard(e, "runtime"))?;
    let key = ClientKey {
        namespace: d.control.namespace,
        origin: url.origin().ascii_serialization(),
        policy: d.policy.generation,
        connect: r.connect,
        read: r.read,
    };
    if let Some((_, c, _)) = rt
        .clients
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .filter(|(at, _, _)| at.elapsed() < Duration::from_secs(60))
    {
        return Ok(c.clone());
    }
    let host = url.host_str().unwrap().trim_matches(['[', ']']);
    let addresses: Vec<_> = tokio::net::lookup_host((host, url.port_or_known_default().unwrap()))
        .await
        .map_err(|_| fail("ER_HTTP_DNS", "dns", false))?
        .collect();
    if addresses.is_empty() {
        return Err(fail("ER_HTTP_DNS", "dns", false));
    }
    for a in &addresses {
        d.policy.check_ip(a.ip()).map_err(|e| hard(e, "policy"))?;
    }
    d.check()?;
    let mut builder = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .connect_timeout(r.connect)
        .read_timeout(r.read)
        .resolve_to_addrs(host, &addresses)
        .pool_max_idle_per_host(16)
        .pool_idle_timeout(Duration::from_secs(60))
        .http2_max_header_list_size(d.limits.headers as u32)
        .no_gzip()
        .no_deflate()
        .no_brotli()
        .no_zstd();
    if let Some(certs) = d.policy.certificates.get(&key.origin) {
        for c in certs {
            builder = builder.add_root_certificate(c.clone());
        }
    }
    let mut cache_memory = Memory::new(d.limits.memory).map_err(|e| hard(e, "memory"))?;
    cache_memory
        .grow(256 * 1024)
        .map_err(|e| hard(e, "memory"))?;
    let c = builder.build().map_err(|e| transport(e, "client"))?;
    let mut cache = rt.clients.lock().unwrap_or_else(|e| e.into_inner());
    if cache.len() >= 128 {
        let victim = cache
            .iter()
            .min_by_key(|(_, v)| v.0)
            .map(|(k, _)| (k.namespace, k.origin.clone(), k.policy, k.connect, k.read));
        if let Some((namespace, origin, policy, connect, read)) = victim {
            cache.remove(&ClientKey {
                namespace,
                origin,
                policy,
                connect,
                read,
            });
        }
    }
    cache.insert(key, (Instant::now(), c.clone(), cache_memory));
    Ok(c)
}
async fn execute(d: &Dispatch, r: Request, memory: &mut Memory) -> Result<Json, Failure> {
    let _permit = acquire(d).await?;
    let mut url = r.url.clone();
    let origin = url.origin();
    let mut method = r.method.clone();
    let mut body = r.body.clone();
    let mut headers = r.headers.clone();
    let mut redirects = 0;
    loop {
        let c = client(d, &r, &url).await?;
        d.check()?;
        d.sent()?;
        let mut builder = c
            .request(method.clone(), url.clone())
            .headers(headers.clone());
        if let Some(b) = &body {
            builder = builder.body(b.clone());
        }
        let mut response = builder.send().await.map_err(|e| transport(e, "send"))?;
        let status = response.status();
        if status.as_u16() == 101 {
            return Err(fail("ER_HTTP_INVALID_RESPONSE", "upgrade", true));
        }
        let size: usize = response
            .headers()
            .iter()
            .map(|(k, v)| k.as_str().len() + v.as_bytes().len() + 4)
            .sum();
        if size > d.limits.headers {
            return Err(hard(
                error("ER_HTTP_BUDGET_EXCEEDED", "response headers exceed limit"),
                "headers",
            ));
        }
        memory
            .grow(size.saturating_mul(12))
            .map_err(|e| hard(e, "memory"))?;
        STATUS[usize::from(status.as_u16() / 100).min(5)].fetch_add(1, Ordering::Relaxed);
        if r.redirects > 0 && matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
            if let Some(location) = response.headers().get(LOCATION) {
                if redirects >= r.redirects {
                    return Err(fail("ER_HTTP_INVALID_RESPONSE", "redirect_limit", true));
                }
                let location = location
                    .to_str()
                    .map_err(|_| fail("ER_HTTP_INVALID_RESPONSE", "redirect", true))?;
                let next = url
                    .join(location)
                    .map_err(|_| fail("ER_HTTP_INVALID_RESPONSE", "redirect", true))?;
                config::parse_url(next.as_str()).map_err(|e| hard(e, "redirect"))?;
                if next.origin() != origin || (url.scheme() == "https" && next.scheme() != "https")
                {
                    return Err(hard(
                        error(
                            "ER_HTTP_EGRESS_DENIED",
                            "cross-origin or downgraded redirect denied",
                        ),
                        "redirect",
                    ));
                }
                d.policy.check_url(&next).map_err(|e| hard(e, "policy"))?;
                if (matches!(status.as_u16(), 301 | 302) && method == Method::POST)
                    || (status.as_u16() == 303 && method != Method::HEAD)
                {
                    method = Method::GET;
                    body = None;
                    headers.remove(CONTENT_TYPE);
                    headers.remove(CONTENT_LENGTH);
                }
                redirects += 1;
                url = next;
                continue;
            }
        }
        let version = format!("{:?}", response.version());
        // Multiple coding fields are one ordered coding list. Do not decode
        // just its first item and accidentally return a partially decoded body.
        let mut content_encoding = String::new();
        for (index, value) in response
            .headers()
            .get_all(CONTENT_ENCODING)
            .iter()
            .enumerate()
        {
            if index != 0 {
                content_encoding.push(',');
            }
            content_encoding.push_str(std::str::from_utf8(value.as_bytes()).unwrap_or("<binary>"));
        }
        content_encoding.make_ascii_lowercase();
        let mut hs = serde_json::Map::new();
        for name in response.headers().keys() {
            let values: Vec<_> = response
                .headers()
                .get_all(name)
                .iter()
                .map(|v| match std::str::from_utf8(v.as_bytes()) {
                    Ok(v) => json!(v),
                    Err(_) => json!({"encoding":"base64","value":BASE64.encode(v.as_bytes())}),
                })
                .collect();
            hs.insert(name.as_str().to_owned(), Json::Array(values));
        }
        if method != Method::HEAD
            && response
                .content_length()
                .is_some_and(|n| n > r.response_limit as u64)
        {
            return Err(hard(
                error("ER_HTTP_BUDGET_EXCEEDED", "response body exceeds limit"),
                "body",
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| transport(e, "body"))? {
            d.check()?;
            if bytes.len().saturating_add(chunk.len()) > r.response_limit {
                return Err(hard(
                    error("ER_HTTP_BUDGET_EXCEEDED", "response body exceeds limit"),
                    "body",
                ));
            }
            // Include Vec growth and the transport's transient chunk allocation.
            memory
                .grow(chunk.len().saturating_mul(3))
                .map_err(|e| hard(e, "memory"))?;
            bytes.extend_from_slice(&chunk);
        }
        let mut decoded = false;
        if !bytes.is_empty() && matches!(content_encoding.as_str(), "gzip" | "deflate") {
            use std::io::Read;
            let mut reader: Box<dyn Read> = if content_encoding == "gzip" {
                Box::new(flate2::read::MultiGzDecoder::new(bytes.as_slice()))
            } else {
                Box::new(flate2::read::ZlibDecoder::new(bytes.as_slice()))
            };
            let mut output = Vec::new();
            let mut chunk = [0u8; 8192];
            loop {
                d.check()?;
                let n = reader
                    .read(&mut chunk)
                    .map_err(|_| fail("ER_HTTP_INVALID_RESPONSE", "decompress", true))?;
                if n == 0 {
                    break;
                }
                if output.len().saturating_add(n) > r.response_limit {
                    return Err(hard(
                        error(
                            "ER_HTTP_BUDGET_EXCEEDED",
                            "decompressed response exceeds limit",
                        ),
                        "decompress",
                    ));
                }
                memory
                    .grow(n.saturating_mul(2))
                    .map_err(|e| hard(e, "memory"))?;
                output.extend_from_slice(&chunk[..n]);
            }
            drop(reader);
            bytes = output;
            decoded = true;
        }
        memory
            .grow(bytes.len().saturating_mul(10).saturating_add(4096))
            .map_err(|e| hard(e, "memory"))?;
        let (body, encoding) = match (r.encoding.as_str(), std::str::from_utf8(&bytes)) {
            ("base64", _) => (BASE64.encode(&bytes), "base64"),
            (_, Ok(s)) => (s.to_owned(), "utf8"),
            ("utf8", Err(_)) => return Err(fail("ER_HTTP_INVALID_RESPONSE", "utf8", true)),
            _ => (BASE64.encode(&bytes), "base64"),
        };
        return Ok(
            json!({"status":status.as_u16(),"headers":hs,"body":body,"body_encoding":encoding,"url":url.as_str(),"http_version":version,"redirect_count":redirects,"elapsed_ms":0,"error":null,"content_encoding":content_encoding,"content_decoded":decoded}),
        );
    }
}
pub(crate) fn call(name: &str, args: &[Value]) -> Result<Value, QueryError> {
    crate::engine::cbo::trace::redact_external();
    // SQL NULL propagation needs neither a network lease nor HTTP configuration.
    if request::null_input(name, args)? {
        return Ok(Value::Null);
    }
    let limits = Limits::load()?;
    let mut memory = Memory::new(limits.memory)?;
    let input = args
        .iter()
        .map(crate::engine::executor::mem_budget::estimate_value_bytes)
        .try_fold(0usize, |a, b| a.checked_add(b))
        .ok_or_else(|| error("ER_HTTP_BUDGET_EXCEEDED", "HTTP arguments too large"))?;
    memory.grow(
        input
            .saturating_mul(8)
            .saturating_add(limits.headers.saturating_mul(8)),
    )?;
    let Some(request) = request::prepare(name, args, &limits)? else {
        return Ok(Value::Null);
    };
    if !config::enabled() {
        return Err(error("ER_HTTP_DISABLED", "HTTP calls disabled"));
    }
    let scope = scope::current()?;
    let (auth, _) = crate::engine::expr::current_authorization_context().ok_or_else(|| {
        error(
            "ER_HTTP_ACCESS_DENIED",
            "HTTP requires an authorization context",
        )
    })?;
    super::authorize(&scope.db, &auth, "HTTP_USE")?;
    let policy = config::load()?;
    policy.check_url(&request.url)?;
    let principal = format!(
        "{}:{}@{}",
        Arc::as_ptr(&scope.db) as usize,
        auth.authenticated_as.user,
        auth.authenticated_as.host
    );
    let user = policy
        .user_limits
        .iter()
        .find(|u| u.user == auth.authenticated_as.user && u.host == auth.authenticated_as.host);
    let user_inflight = user
        .and_then(|u| u.max_inflight)
        .unwrap_or(4)
        .min(limits.inflight);
    let rpm = user.and_then(|u| u.requests_per_minute);
    let calls = user
        .and_then(|u| u.max_http_attempts_per_statement)
        .unwrap_or(limits.calls)
        .min(limits.calls);
    let control = control(&scope.db)?;
    let aborted = Arc::new(AtomicBool::new(false));
    let d = Dispatch {
        db: scope.db.clone(),
        auth,
        policy,
        effects: scope.effects.clone(),
        control: control.clone(),
        limits,
        principal,
        user_inflight,
        rpm,
        calls,
        switch: config::switch_generation(),
        token: crate::engine::cancel::current_token(),
        aborted: aborted.clone(),
    };
    d.check().map_err(|e| e.error)?;
    let return_errors = request.return_errors;
    let timeout = request.timeout;
    let began = Instant::now();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    control.pending.fetch_add(1, Ordering::AcqRel);
    let pending = Pending(control);
    TOTAL.fetch_add(1, Ordering::Relaxed);
    let job = runtime()?.rt.spawn(async move {
        let _pending = pending;
        let before = d.effects.attempts.load(Ordering::Acquire);
        let monitored = async {
            let future = execute(&d, request, &mut memory);
            tokio::pin!(future);
            let mut tick = tokio::time::interval(Duration::from_millis(10));
            loop {
                tokio::select! {result=&mut future=>break result, _=tick.tick()=>{d.check()?;}}
            }
        };
        let result = tokio::time::timeout(timeout, monitored)
            .await
            .unwrap_or_else(|_| {
                Err(fail(
                    "ER_HTTP_TIMEOUT",
                    "deadline",
                    d.effects.attempts.load(Ordering::Acquire) > before,
                ))
            });
        let _ = tx.send((result, memory));
    });
    struct Abort {
        job: tokio::task::JoinHandle<()>,
        flag: Arc<AtomicBool>,
    }
    impl Drop for Abort {
        fn drop(&mut self) {
            self.flag.store(true, Ordering::Release);
            self.job.abort();
        }
    }
    let _abort = Abort { job, flag: aborted };
    let (result, mut memory) = loop {
        crate::engine::cancel::check_cancelled()?;
        match rx.recv_timeout(Duration::from_millis(10)) {
            Ok(v) => break v,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(_) => return Err(error("ER_HTTP_TRANSPORT", "HTTP worker stopped")),
        }
    };
    let elapsed = began.elapsed().as_millis().min(u64::MAX as u128) as u64;
    ELAPSED.fetch_add(elapsed, Ordering::Relaxed);
    let mut envelope = match result {
        Ok(v) => v,
        Err(f) => {
            FAILED.fetch_add(1, Ordering::Relaxed);
            *ERRORS
                .get_or_init(Default::default)
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(f.code)
                .or_default() += 1;
            if !return_errors || !f.returnable {
                return Err(f.error);
            }
            json!({"status":null,"headers":null,"body":null,"body_encoding":null,"url":null,"http_version":null,"redirect_count":null,"elapsed_ms":elapsed,"error":{"code":f.code,"stage":f.stage,"may_have_executed":f.ambiguous},"content_encoding":null,"content_decoded":null})
        }
    };
    envelope["elapsed_ms"] = json!(elapsed);
    let value = Value::Json(crate::engine::json::from_serde(&envelope));
    memory.retain(crate::engine::executor::mem_budget::estimate_value_bytes(
        &value,
    ));
    // SQL result owners also account the returned Value. Keep the HTTP lease
    // until the owning statement or next streaming row releases it.
    scope
        .effects
        .outputs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(memory);
    Ok(value)
}
pub(crate) fn status() -> Vec<(String, u64)> {
    let g = gate().lock().unwrap_or_else(|e| e.into_inner());
    let mut rows = vec![
        ("calls".into(), TOTAL.load(Ordering::Relaxed)),
        ("attempts".into(), ATTEMPTS.load(Ordering::Relaxed)),
        ("failed".into(), FAILED.load(Ordering::Relaxed)),
        ("elapsed_ms".into(), ELAPSED.load(Ordering::Relaxed)),
        ("inflight".into(), g.inflight as u64),
        ("waiting".into(), g.waiting as u64),
        ("memory_bytes".into(), MEMORY.load(Ordering::Relaxed) as u64),
    ];
    for (i, s) in STATUS.iter().enumerate().skip(1) {
        rows.push((format!("status_{i}xx"), s.load(Ordering::Relaxed)));
    }
    for (k, v) in ERRORS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
    {
        rows.push(((*k).into(), *v));
    }
    rows
}

/// HTTP SQL waits use an independent, bounded coordinator pool.
pub(crate) fn spawn_coordinator<F, T>(work: F) -> crate::util::blocking_pool::BlockingHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    static POOL: OnceLock<crate::util::blocking_pool::BlockingPool> = OnceLock::new();
    POOL.get_or_init(|| crate::util::blocking_pool::BlockingPool::with_limits(4, 32))
        .spawn(work)
}
pub(crate) fn admit_coordinator() -> Result<tokio::sync::OwnedSemaphorePermit, QueryError> {
    let limits = Limits::load()?;
    static ADMISSION: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    ADMISSION
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(limits.queue + 32)))
        .clone()
        .try_acquire_owned()
        .map_err(|_| error("ER_HTTP_QUEUE_FULL", "HTTP SQL coordinator queue is full"))
}

/// Wait for coordinator capacity without occupying a synchronous SQL worker.
/// Account admission comes first, so one account's network queue cannot pin
/// every coordinator while other accounts still have runnable work.
pub(crate) fn admit_session(
    session: &crate::engine::api::SessionContext,
) -> impl std::future::Future<Output = Result<SessionPermit, QueryError>> + Send + 'static {
    let account = session.authenticated_as.clone();
    let token = session.current_cancel_token().cloned();
    let timeout = session.sysvars.max_execution_time_ms;
    admit_owned_session(account, token, timeout)
}
async fn admit_owned_session(
    account: crate::engine::auth::AccountKey,
    token: Option<crate::engine::cancel::QueryCancelToken>,
    timeout: u64,
) -> Result<SessionPermit, QueryError> {
    struct AccountGate {
        semaphore: Arc<tokio::sync::Semaphore>,
        capacity: usize,
    }
    static ACCOUNTS: OnceLock<Mutex<HashMap<String, AccountGate>>> = OnceLock::new();
    static QUEUED: AtomicUsize = AtomicUsize::new(0);
    struct Queued;
    impl Drop for Queued {
        fn drop(&mut self) {
            QUEUED.fetch_sub(1, Ordering::AcqRel);
        }
    }
    let limits = Limits::load()?;
    let policy = config::load()?;
    let capacity = policy
        .user_limits
        .iter()
        .find(|u| u.user == account.user && u.host == account.host)
        .and_then(|u| u.max_inflight)
        .unwrap_or(4)
        .min(32);
    let key = format!("{}@{}", account.user, account.host);
    let semaphore = {
        let mut accounts = ACCOUNTS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if accounts.len() >= 1024 && !accounts.contains_key(&key) {
            accounts.retain(|_, a| {
                Arc::strong_count(&a.semaphore) > 1 || a.semaphore.available_permits() != a.capacity
            });
            if accounts.len() >= 1024 {
                return Err(error(
                    "ER_HTTP_QUEUE_FULL",
                    "too many HTTP coordinator accounts",
                ));
            }
        }
        let gate = accounts.entry(key).or_insert_with(|| AccountGate {
            semaphore: Arc::new(tokio::sync::Semaphore::new(capacity)),
            capacity,
        });
        // Keep active leases valid. A changed capacity is installed after the
        // prior generation drains instead of manufacturing extra permits.
        if gate.capacity != capacity
            && gate.semaphore.available_permits() == gate.capacity
            && Arc::strong_count(&gate.semaphore) == 1
        {
            gate.semaphore = Arc::new(tokio::sync::Semaphore::new(capacity));
            gate.capacity = capacity;
        }
        gate.semaphore.clone()
    };
    QUEUED
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            (n < limits.queue).then_some(n + 1)
        })
        .map_err(|_| error("ER_HTTP_QUEUE_FULL", "HTTP coordinator queue full"))?;
    let _queued = Queued;
    let began = Instant::now();
    let wait = async {
        let account = semaphore
            .acquire_owned()
            .await
            .map_err(|_| error("ER_HTTP_DISABLED", "HTTP coordinator stopped"))?;
        // The synchronous pool is bounded independently of request I/O.
        static GLOBAL: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
        let global = GLOBAL
            .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(32)))
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| error("ER_HTTP_DISABLED", "HTTP coordinator stopped"))?;
        Ok(SessionPermit {
            _account: account,
            _global: global,
        })
    };
    tokio::pin!(wait);
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    loop {
        tokio::select! {
            result=&mut wait=>return result,
            _=tick.tick()=>{
                if let Some(token) = &token { token.check()?; }
                if timeout>0 && began.elapsed()>=Duration::from_millis(timeout) {
                    return Err(crate::engine::cancel::execution_time_exceeded_error());
                }
                if !policy.current() {return Err(error("ER_HTTP_DISABLED","HTTP policy changed while queued"));}
            }
        }
    }
}
pub(crate) struct SessionPermit {
    _account: tokio::sync::OwnedSemaphorePermit,
    _global: tokio::sync::OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_admission_refills_tokens_and_releases_capacity_waits() {
        std::fs::create_dir_all("/tmp/mydb/http-functions").unwrap();
        let dir = tempfile::Builder::new()
            .prefix("admission-")
            .tempdir_in("/tmp/mydb/http-functions")
            .unwrap();
        let mut storage = crate::storage::StorageConfig::from_env();
        storage.durability = crate::storage::DurabilityMode::Memory;
        storage.buffer_pool_pages = 512;
        let db = Arc::new(Database::open_with_config(dir.path(), storage).unwrap());
        let auth = crate::engine::api::SessionContext::default().authorization_context();
        let principal = format!("rate-refill-{}", Arc::as_ptr(&db) as usize);
        let d = Dispatch {
            control: control(&db).unwrap(),
            db,
            auth,
            policy: config::load().unwrap(),
            effects: Arc::new(scope::Effects::default()),
            limits: Limits::load().unwrap(),
            principal: principal.clone(),
            user_inflight: 1,
            rpm: Some(2),
            calls: 100,
            switch: config::switch_generation(),
            token: None,
            aborted: Arc::new(AtomicBool::new(false)),
        };
        // Advance only the bucket's accounting time, avoiding a 30-second test.
        let now = Instant::now();
        gate().lock().unwrap().users.insert(
            principal.clone(),
            UserGate {
                inflight: 0,
                tokens: 0.0,
                updated: now - Duration::from_secs(31),
                last: now,
            },
        );
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            let permit = acquire(&d).await.unwrap_or_else(|f| panic!("{}", f.error));
            let failure = match acquire(&d).await {
                Err(f) => f,
                Ok(_) => panic!("refill must not restore the full initial burst"),
            };
            assert!(failure
                .error
                .is(crate::error::catalog::ER_HTTP_RATE_LIMITED));
            assert!(!failure.returnable);
            assert!(!failure.ambiguous);
            assert_eq!(d.effects.attempts.load(Ordering::Acquire), 0);
            drop(permit);
            {
                let mut g = gate().lock().unwrap();
                assert_eq!(g.waiting, 0);
                assert_eq!(g.inflight, 0);
                let user = g.users.get_mut(&principal).unwrap();
                user.tokens = 2.0;
                user.updated = Instant::now();
            }
            let first = acquire(&d).await.unwrap_or_else(|f| panic!("{}", f.error));
            // A token remains, so the concurrency gate must wait rather than
            // report a rate error or consume the remaining token while queued.
            assert!(tokio::time::timeout(Duration::from_millis(30), acquire(&d))
                .await
                .is_err());
            {
                let g = gate().lock().unwrap();
                assert_eq!(g.waiting, 0);
                assert_eq!(g.inflight, 1);
                assert!(g.users[&principal].tokens >= 1.0);
            }
            drop(first);
            let second = acquire(&d).await.unwrap_or_else(|f| panic!("{}", f.error));
            drop(second);
        });
        let mut g = gate().lock().unwrap();
        assert_eq!(g.waiting, 0);
        assert_eq!(g.inflight, 0);
        assert_eq!(g.users.remove(&principal).unwrap().inflight, 0);
    }
}

#[cfg(test)]
mod audit1031_shutdown_tests {
    use super::*;
    #[test]
    fn audit1031_shutdown_finishes_checkpoint_after_http_drain_error() {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let mut config = crate::storage::StorageConfig::from_env();
        config.durability = crate::storage::DurabilityMode::Memory;
        config.buffer_pool_pages = 512;
        let engine =
            crate::engine::StorageQueryEngine::open_with_storage_config_without_background_workers(
                dir.path(),
                config,
            )
            .unwrap();
        engine
            .execute("CREATE TABLE shutdown_probe (a INT)")
            .unwrap();
        let c = control(engine.db_handle()).unwrap();
        c.pending.store(1, Ordering::Release);
        let checkpoints = engine.db_handle().storage_status().checkpoints_total;
        assert!(engine.shutdown().is_err());
        // Shutdown must report the drain failure, but still join workers and
        // establish a durable checkpoint for already completed database work.
        assert!(engine.db_handle().storage_status().checkpoints_total > checkpoints);
        c.pending.store(0, Ordering::Release);
    }
}
