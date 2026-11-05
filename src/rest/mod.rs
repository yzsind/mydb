//! HTTP SQL transport sharing the database engine and SQL session lifecycle.
mod codec;
pub mod config;
mod failure;
mod runtime;
use crate::engine::{
    api::{QueryEngine, SessionContext},
    session_registry::SessionRegistry,
};
#[cfg(test)]
use axum::body::to_bytes;
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
pub use config::RestConfig;
use failure::{Failure, Result};
use runtime::{Identity, Peer};
use serde::Deserialize;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
pub struct RestServer<E: QueryEngine> {
    state: Arc<runtime::State<E>>,
    tls: Option<tokio_rustls::TlsAcceptor>,
}
impl<E: QueryEngine> RestServer<E> {
    pub fn new(
        config: RestConfig,
        engine: Arc<E>,
        registry: Arc<SessionRegistry>,
        tls: Option<tokio_rustls::TlsAcceptor>,
    ) -> Self {
        Self {
            state: runtime::State::new(config, engine, registry),
            tls,
        }
    }
    pub async fn run_with_listener(&self, listener: TcpListener) -> std::io::Result<()> {
        if self.state.config.tls != self.tls.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "REST TLS configuration and acceptor must agree",
            ));
        }
        if self.tls.is_none() && !listener.local_addr()?.ip().is_loopback() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Plaintext REST requires an actual loopback listener",
            ));
        }
        let sockets = Arc::new(tokio::sync::Semaphore::new(
            self.state.config.max_connections,
        ));
        let router = Router::new()
            .route("/health/live", get(endpoint::<E>))
            .route("/health/ready", get(endpoint::<E>))
            .route("/api/v1/status", get(endpoint::<E>))
            .route("/api/v1/sql", post(endpoint::<E>))
            .route("/api/v1/sessions", post(endpoint::<E>))
            .route(
                "/api/v1/sessions/{id}",
                get(endpoint::<E>).delete(endpoint::<E>),
            )
            .route("/api/v1/sessions/{id}/sql", post(endpoint::<E>))
            .route("/api/v1/sessions/{id}/touch", post(endpoint::<E>))
            .route("/api/v1/requests/{id}", get(endpoint::<E>))
            .route("/api/v1/requests/{id}/cancel", post(endpoint::<E>))
            .fallback(|| async { Failure::not_found() })
            .method_not_allowed_fallback(|| async {
                Failure::new(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "METHOD_NOT_ALLOWED",
                    "HTTP method is not supported for this resource",
                )
            })
            .layer(axum::middleware::map_response(
                |mut response: Response| async move {
                    response.headers_mut().insert(
                        "cache-control",
                        axum::http::HeaderValue::from_static("no-store"),
                    );
                    response
                },
            ))
            .with_state(self.state.clone());
        let mut sweep = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
             _=crate::mysql::shutdown::shutdown_notified()=>break,
             _=sweep.tick()=>self.state.sweep().await,
             accepted=listener.accept()=>{
              let(stream,addr)=match accepted{Ok(v)=>v,Err(e)if matches!(e.kind(),std::io::ErrorKind::Interrupted|std::io::ErrorKind::ConnectionAborted)=>continue,Err(e)=>{crate::mysql::shutdown::request_shutdown(crate::mysql::shutdown::ShutdownSource::Signal("REST listener failure"),crate::mysql::shutdown::ShutdownIntent::Halt);return Err(e)}};
              let Ok(permit)=sockets.clone().try_acquire_owned()else{drop(stream);continue};
              let tls=self.tls.clone();let app=router.clone();let config=self.state.config.clone();
              crate::util::connection_dispatch::spawn(move||async move{
               let _permit=permit;let _=stream.set_nodelay(true);
               let mut peer=Peer{host:addr.ip().to_string(),cipher:None,version:None,subject:None,issuer:None};
               if let Some(tls)=tls{
                let Ok(Ok(stream))=tokio::time::timeout(Duration::from_secs(5),tls.accept(stream)).await else{return};
                let connection=stream.get_ref().1;
                peer.cipher=connection.negotiated_cipher_suite().map(|v|crate::mysql::session::tls_names::cipher_name(v.suite()));
                peer.version=connection.protocol_version().map(|v|format!("{v:?}"));
                if let Some(cert)=connection.peer_certificates().and_then(|v|v.first()){if let Ok((_,cert))=x509_parser::parse_x509_certificate(cert.as_ref()){peer.subject=Some(crate::mysql::session::tls_names::distinguished_name(cert.subject()));peer.issuer=Some(crate::mysql::session::tls_names::distinguished_name(cert.issuer()));}}
                serve(stream,app.layer(Extension(peer)),config).await;
               }else{serve(stream,app.layer(Extension(peer)),config).await;}
              })?;
             }
            }
        }
        self.state.close_all().await;
        Ok(())
    }
}
async fn serve<I>(io: I, app: Router, config: RestConfig)
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let io = hyper_util::rt::TokioIo::new(io);
    let service = hyper_util::service::TowerToHyperService::new(app);
    let mut builder =
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
    builder
        .http1()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(Duration::from_secs(5))
        .max_buf_size(config.max_header_bytes.max(8192));
    builder
        .http2()
        .max_concurrent_streams(config.max_inflight.min(u32::MAX as usize) as u32)
        .max_header_list_size(config.max_header_bytes.min(u32::MAX as usize) as u32);
    let connection = builder.serve_connection_with_upgrades(io, service);
    tokio::pin!(connection);
    tokio::select! {_=&mut connection=>{},_=crate::mysql::shutdown::shutdown_notified()=>{connection.as_mut().graceful_shutdown();let _=tokio::time::timeout(crate::mysql::shutdown::shutdown_timeout(),connection).await;}}
}
async fn endpoint<E: QueryEngine>(
    State(state): State<Arc<runtime::State<E>>>,
    Extension(peer): Extension<Peer>,
    request: Request<Body>,
) -> Result<Response> {
    let path = request.uri().path().to_owned();
    let method = request.method().clone();
    if path == "/health/live" {
        return Ok(Json(json!({"status":"live"})).into_response());
    }
    if path == "/health/ready" {
        return Ok((
            if crate::mysql::shutdown::is_shutting_down() {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::OK
            },
            Json(json!({"ready":!crate::mysql::shutdown::is_shutting_down()})),
        )
            .into_response());
    }
    if request.uri().query().is_some() {
        return Err(Failure::bad("Query parameters are not supported; send SQL and credentials in their designated fields"));
    }
    let header_bytes: usize = request
        .headers()
        .iter()
        .map(|(k, v)| k.as_str().len() + v.len() + 4)
        .sum();
    if header_bytes > state.config.max_header_bytes {
        return Err(Failure::bad("HTTP header limit exceeded"));
    }
    if request.headers().contains_key("origin") {
        return Err(Failure::new(
            StatusCode::FORBIDDEN,
            "CORS_DISABLED",
            "Browser origins are disabled",
        ));
    }
    let identity = runtime::authenticate(&state, request.headers(), peer)?;
    if path == "/api/v1/status" {
        let mut ctx = SessionContext::default();
        ctx.authenticated_as = identity.outcome.context.authenticated_as;
        ctx.active_roles = identity.outcome.context.active_roles;
        state.engine.authorize_global_privilege(
            &ctx,
            crate::engine::auth::Privilege::Dynamic("API_KEY_ADMIN".into()),
        )?;
        return Ok(Json(state.summary()).into_response());
    }
    let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
    if parts.get(2) == Some(&"requests") {
        let record = state.request(parts[3], &identity.key_id)?;
        if parts.len() == 5 {
            record.cancel();
            state
                .cancelled
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok((StatusCode::ACCEPTED, Json(record.state.lock().clone())).into_response());
        }
        let value = record.state.lock().clone();
        return Ok(Json(value).into_response());
    }
    if parts.get(2) == Some(&"sessions") && parts.len() >= 4 {
        let session = state.session(parts[3], &identity.key_id)?;
        if method == axum::http::Method::DELETE {
            session.close().await;
            state.sessions.lock().remove(&session.id);
            return Ok(StatusCode::NO_CONTENT.into_response());
        }
        if parts.len() == 4 {
            return Ok(Json(session_info(&session, &state.config)).into_response());
        }
        if parts[4] == "touch" {
            session
                .touched
                .store(runtime::now(), std::sync::atomic::Ordering::Release);
            return Ok(Json(session_info(&session, &state.config)).into_response());
        }
        return sql(state, request, identity, Some(session)).await;
    }
    if path == "/api/v1/sessions" {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Create {
            database: Option<String>,
        }
        let (bytes, _memory) = body(&state, request).await?;
        strict(&bytes, state.config.max_request_bytes)?;
        let create: Create = serde_json::from_slice(&bytes)
            .map_err(|_| Failure::bad("Invalid session creation request"))?;
        let session = runtime::create_session(state.clone(), identity, create.database).await?;
        state
            .sessions
            .lock()
            .insert(session.id.clone(), session.clone());
        let mut response = (
            StatusCode::CREATED,
            Json(session_info(&session, &state.config)),
        )
            .into_response();
        response.headers_mut().insert(
            "location",
            format!("/api/v1/sessions/{}", session.id).parse().unwrap(),
        );
        return Ok(response);
    }
    sql(state, request, identity, None).await
}
fn session_info<E: QueryEngine>(
    session: &runtime::Session<E>,
    config: &RestConfig,
) -> serde_json::Value {
    let mut info = session.info.lock().clone();
    info["busy"] = json!(session.busy.load(std::sync::atomic::Ordering::Acquire));
    let transaction = session
        .transaction
        .load(std::sync::atomic::Ordering::Acquire);
    let (clock, limit) = if transaction {
        (&session.transaction_touched, config.transaction_idle)
    } else {
        (&session.touched, config.session_idle)
    };
    info["expires_at_ms"] = json!(clock
        .load(std::sync::atomic::Ordering::Acquire)
        .saturating_add(limit.as_millis() as u64));
    info
}
fn strict(bytes: &[u8], max: usize) -> Result<()> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| Failure::bad("Request must contain UTF-8 JSON"))?;
    crate::engine::http::parse_strict_json(text, max)
        .map_err(|_| Failure::bad("Invalid JSON, duplicate fields or excessive nesting"))?;
    Ok(())
}
async fn body<E: QueryEngine>(
    state: &runtime::State<E>,
    request: Request<Body>,
) -> Result<(axum::body::Bytes, runtime::Memory)> {
    if !request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case("application/json")
        })
    {
        return Err(Failure::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UNSUPPORTED_MEDIA_TYPE",
            "Content-Type must be application/json",
        ));
    }
    if request.headers().contains_key("content-encoding") {
        return Err(Failure::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "UNSUPPORTED_CONTENT_ENCODING",
            "Compressed request bodies are unsupported",
        ));
    }
    let len = request
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    if len > state.config.max_request_bytes {
        return Err(Failure::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "REQUEST_TOO_LARGE",
            "Request body exceeds limit",
        ));
    }
    let mut memory = state.memory(4096)?;
    let bytes = tokio::time::timeout(Duration::from_secs(10), async {
        let mut stream = request.into_body().into_data_stream();
        let mut bytes = Vec::new();
        let mut charged_capacity = 0usize;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| Failure::bad("Invalid request body"))?;
            let required = bytes.len().saturating_add(chunk.len());
            if required > state.config.max_request_bytes {
                return Err(Failure::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "REQUEST_TOO_LARGE",
                    "Request body exceeds limit",
                ));
            }
            if required > charged_capacity {
                let capacity = required
                    .next_power_of_two()
                    .min(state.config.max_request_bytes);
                state.grow_memory(&mut memory, (capacity - charged_capacity).saturating_mul(4))?;
                bytes.reserve_exact(capacity - bytes.len());
                charged_capacity = capacity;
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok::<_, Failure>(axum::body::Bytes::from(bytes))
    })
    .await
    .map_err(|_| Failure::bad("Request body read timeout"))??;
    state
        .received_bytes
        .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
    Ok((bytes, memory))
}
fn negotiate_format(headers: &axum::http::HeaderMap) -> Result<bool> {
    if !headers.contains_key("accept") {
        return Ok(false);
    }
    // Specific exclusions override wildcard ranges (for example json;q=0,
    // */*;q=1). Prefer JSON when both representations have equal quality.
    let mut formats: [Option<(u8, u16)>; 2] = [None, None];
    for value in headers.get_all("accept") {
        let value = value
            .to_str()
            .map_err(|_| Failure::bad("Invalid Accept header"))?;
        for range in value.split(',') {
            let mut parts = range.split(';');
            let media = parts.next().unwrap_or("").trim();
            let mut quality = 1000;
            for parameter in parts {
                if let Some((key, value)) = parameter.trim().split_once('=') {
                    if key.trim().eq_ignore_ascii_case("q") {
                        let value = value.trim();
                        let parsed = value
                            .parse::<f64>()
                            .ok()
                            .filter(|q| (0.0..=1.0).contains(q));
                        quality = parsed.map(|q| (q * 1000.0).round() as u16).unwrap_or(0);
                    }
                }
            }
            for (index, offered) in ["application/json", "application/x-ndjson"]
                .iter()
                .enumerate()
            {
                let specificity = if media.eq_ignore_ascii_case(offered) {
                    2
                } else if media.eq_ignore_ascii_case("application/*") {
                    1
                } else if media == "*/*" {
                    0
                } else {
                    continue;
                };
                if formats[index]
                    .is_none_or(|(s, q)| specificity > s || (specificity == s && quality > q))
                {
                    formats[index] = Some((specificity, quality));
                }
            }
        }
    }
    let quality = |i: usize| formats[i].map_or(0, |(_, q)| q);
    if quality(0) == 0 && quality(1) == 0 {
        return Err(Failure::new(
            StatusCode::NOT_ACCEPTABLE,
            "UNSUPPORTED_FORMAT",
            "Accept must allow application/json or application/x-ndjson",
        ));
    }
    Ok(quality(1) > quality(0))
}

async fn sql<E: QueryEngine>(
    state: Arc<runtime::State<E>>,
    request: Request<Body>,
    identity: Identity,
    session: Option<Arc<runtime::Session<E>>>,
) -> Result<Response> {
    let stream = negotiate_format(request.headers())?;
    if request.headers().get_all("x-request-id").iter().count() > 1 {
        return Err(Failure::bad("Duplicate request ID header"));
    }
    let id = if let Some(v) = request.headers().get("x-request-id") {
        let v = v.to_str().map_err(|_| Failure::bad("Invalid request ID"))?;
        if !(16..=64).contains(&v.len())
            || !v
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        {
            return Err(Failure::bad(
                "Request ID must contain 16 through 64 ASCII letters, digits, '-' or '_'",
            ));
        }
        v.to_owned()
    } else {
        format!("rq_{}", crate::engine::access_keys::random_id()?)
    };
    let (bytes, memory) = body(&state, request).await?;
    strict(&bytes, state.config.max_request_bytes)?;
    let parsed: codec::SqlRequest =
        serde_json::from_slice(&bytes).map_err(|_| Failure::bad("Invalid SQL request fields"))?;
    let (database, items, atomic, timeout) = parsed.items()?;
    let timeout = timeout.unwrap_or(state.config.default_timeout_ms);
    if timeout == 0 || timeout > state.config.max_timeout_ms {
        return Err(Failure::bad("timeout_ms exceeds configured range"));
    }
    let temporary = session.is_none();
    let session = if let Some(session) = session {
        if database.is_some() {
            return Err(Failure::bad(
                "Persistent sessions select their database with USE",
            ));
        }
        session
    } else {
        let session = runtime::create_session(state.clone(), identity, database).await?;
        state
            .sessions
            .lock()
            .insert(session.id.clone(), session.clone());
        session
    };
    let response = runtime::execute(
        state.clone(),
        session.clone(),
        items,
        atomic,
        temporary,
        timeout,
        id.clone(),
        stream,
        memory,
    )
    .await;
    if temporary && response.is_err() {
        session.close().await;
        state.sessions.lock().remove(&session.id);
    }
    response.map_err(|error| error.with_request_id(id))
}

/// Validate independently provisioned master keys before accepting REST traffic.
pub fn validate_credentials(
    engine: &crate::engine::StorageQueryEngine,
) -> std::result::Result<(), crate::types::QueryError> {
    crate::engine::access_keys::validate_keys(engine.db_handle())
}

#[cfg(test)]
mod tests;
