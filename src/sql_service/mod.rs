//! Shared API-key SQL sessions, execution, and bounded result delivery.
pub(crate) mod codec;
pub(crate) mod failure;
use self::{
    codec::SqlItem,
    failure::{Failure, Result},
};
use crate::rest::RestConfig;
use crate::{
    engine::{
        api::{self, QueryEngine, QueryResponse, ResultCursor, SessionContext, VecResultCursor},
        auth::{self, AccountKey, AuthOutcome},
        executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge},
        session_registry::{SessionHandle, SessionRegistry},
    },
    mysql::connection_manager::ConnectionSlot,
};
use axum::{body::Bytes, http::StatusCode};
use parking_lot::Mutex;
use serde_json::{json, Value as J};
use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Weak,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_stream::Stream;

pub fn now() -> u64 {
    auth::current_time_ms()
}
pub struct Memory {
    used: Arc<AtomicUsize>,
    bytes: usize,
    _charge: ControlledMemoryCharge,
}
impl Drop for Memory {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl Memory {
    fn split(&mut self, bytes: usize) -> Self {
        let charge = self._charge.split(bytes);
        self.bytes -= bytes;
        Self {
            used: self.used.clone(),
            bytes,
            _charge: charge,
        }
    }
    fn grow(&mut self, bytes: usize, limit: usize) -> Result<()> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(bytes).filter(|n| *n <= limit)
            })
            .map_err(|_| Failure::unavailable("REST memory limit exceeded"))?;
        if let Err(error) = self._charge.try_grow(bytes) {
            self.used.fetch_sub(bytes, Ordering::AcqRel);
            return Err(error.into());
        }
        self.bytes += bytes;
        Ok(())
    }
}
/// Ownership carried through classification and SQL even if the caller stops
/// awaiting. An optional adapter admission lease also covers detached workers.
pub struct ExecutionResources {
    _memory: Memory,
    _admission: Option<Arc<OwnedSemaphorePermit>>,
}
impl From<Memory> for ExecutionResources {
    fn from(memory: Memory) -> Self {
        Self {
            _memory: memory,
            _admission: None,
        }
    }
}
impl ExecutionResources {
    pub fn with_admission(memory: Memory, admission: Arc<OwnedSemaphorePermit>) -> Self {
        Self {
            _memory: memory,
            _admission: Some(admission),
        }
    }
}
pub struct State<E: QueryEngine> {
    pub config: RestConfig,
    pub engine: Arc<E>,
    pub registry: Arc<SessionRegistry>,
    pub sessions: Mutex<HashMap<String, Arc<Session<E>>>>,
    pub requests: Mutex<HashMap<String, Arc<RequestState<E>>>>,
    completed_requests: Arc<Mutex<VecDeque<Weak<RequestState<E>>>>>,
    pub inflight: Arc<Semaphore>,
    pub slots: Arc<Semaphore>,
    pub used: Arc<AtomicUsize>,
    pub executed: AtomicU64,
    pub failed: AtomicU64,
    pub cancelled: AtomicU64,
    pub received_bytes: AtomicU64,
    pub sent_bytes: AtomicU64,
    pub elapsed_us: AtomicU64,
}
impl<E: QueryEngine> State<E> {
    pub fn new(config: RestConfig, engine: Arc<E>, registry: Arc<SessionRegistry>) -> Arc<Self> {
        Arc::new(Self {
            inflight: Arc::new(Semaphore::new(config.max_inflight)),
            slots: Arc::new(Semaphore::new(config.max_sessions)),
            config,
            engine,
            registry,
            sessions: Mutex::new(HashMap::new()),
            requests: Mutex::new(HashMap::new()),
            completed_requests: Arc::new(Mutex::new(VecDeque::new())),
            used: Arc::new(AtomicUsize::new(0)),
            executed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            received_bytes: AtomicU64::new(0),
            sent_bytes: AtomicU64::new(0),
            elapsed_us: AtomicU64::new(0),
        })
    }
    fn try_memory(&self, bytes: usize) -> Result<Memory> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value
                    .checked_add(bytes)
                    .filter(|value| *value <= self.config.memory_limit)
            })
            .map_err(|_| Failure::unavailable("REST memory limit exceeded"))?;
        match ControlledMemoryCharge::try_new(ControlledMemoryCategory::ProtocolTransient, bytes) {
            Ok(charge) => Ok(Memory {
                used: self.used.clone(),
                bytes,
                _charge: charge,
            }),
            Err(error) => {
                self.used.fetch_sub(bytes, Ordering::AcqRel);
                Err(error.into())
            }
        }
    }
    pub fn memory(&self, bytes: usize) -> Result<Memory> {
        if bytes > self.config.memory_limit {
            return Err(Failure::unavailable("REST memory limit exceeded"));
        }
        loop {
            let result = self.try_memory(bytes);
            if result.is_ok() || !self.evict_completed_request() {
                return result;
            }
        }
    }
    pub(super) fn grow_memory(&self, memory: &mut Memory, extra: usize) -> Result<()> {
        if memory.bytes.saturating_add(extra) > self.config.memory_limit {
            return Err(Failure::unavailable("REST memory limit exceeded"));
        }
        loop {
            let result = memory.grow(extra, self.config.memory_limit);
            if result.is_ok() || !self.evict_completed_request() {
                return result;
            }
        }
    }
    fn evict_completed_request(&self) -> bool {
        let mut requests = self.requests.lock();
        let mut completed = self.completed_requests.lock();
        Self::evict_oldest(&mut requests, &mut completed)
    }
    fn evict_oldest(
        requests: &mut HashMap<String, Arc<RequestState<E>>>,
        completed: &mut VecDeque<Weak<RequestState<E>>>,
    ) -> bool {
        while let Some(weak) = completed.pop_front() {
            let Some(request) = weak.upgrade() else {
                continue;
            };
            let key = request.cache_key();
            if requests
                .get(&key)
                .is_some_and(|entry| Arc::ptr_eq(entry, &request))
            {
                requests.remove(&key);
                return true;
            }
        }
        false
    }
    fn register_request(&self, request: Arc<RequestState<E>>) -> Result<()> {
        let key = request.cache_key();
        let mut requests = self.requests.lock();
        if requests.contains_key(&key) {
            return Err(Failure::new(
                StatusCode::CONFLICT,
                "DUPLICATE_REQUEST_ID",
                "Request ID already retained; execution was not started",
            ));
        }
        if requests.len() >= 10000 {
            let mut completed = self.completed_requests.lock();
            if !Self::evict_oldest(&mut requests, &mut completed) {
                return Err(Failure::unavailable("Request status capacity exceeded"));
            }
        }
        requests.insert(key, request);
        Ok(())
    }
    fn expire_requests(&self) {
        let mut requests = self.requests.lock();
        let mut completed = self.completed_requests.lock();
        while let Some(weak) = completed.front() {
            let Some(request) = weak.upgrade() else {
                completed.pop_front();
                continue;
            };
            let key = request.cache_key();
            let retained = requests
                .get(&key)
                .is_some_and(|entry| Arc::ptr_eq(entry, &request));
            if retained && now().saturating_sub(request.completed.load(Ordering::Acquire)) < 300000
            {
                break;
            }
            completed.pop_front();
            if retained {
                requests.remove(&key);
            }
        }
    }
    pub fn session(&self, id: &str, key: &str) -> Result<Arc<Session<E>>> {
        self.sessions
            .lock()
            .get(id)
            .filter(|s| s.key_id == key && !s.closed.load(Ordering::Acquire))
            .cloned()
            .ok_or_else(Failure::not_found)
    }
    pub fn request(&self, id: &str, key: &str) -> Result<Arc<RequestState<E>>> {
        self.requests
            .lock()
            .get(&format!("{key}:{id}"))
            .filter(|s| s.key_id == key)
            .cloned()
            .ok_or_else(Failure::not_found)
    }
    pub async fn sweep(self: &Arc<Self>) {
        let sessions: Vec<_> = self.sessions.lock().values().cloned().collect();
        for s in sessions {
            let limit = if s.transaction.load(Ordering::Acquire) {
                self.config.transaction_idle
            } else {
                self.config.session_idle
            };
            if crate::mysql::shutdown::is_shutting_down()
                || s.handle.kill_flag().load(Ordering::Acquire)
                || !self.engine.api_key_is_valid(&s.key_id, &s.owner)
                || (!s.busy.load(Ordering::Acquire)
                    && now().saturating_sub(if s.transaction.load(Ordering::Acquire) {
                        s.transaction_touched.load(Ordering::Acquire)
                    } else {
                        s.touched.load(Ordering::Acquire)
                    }) > limit.as_millis() as u64)
            {
                s.begin_close();
            }
            if s.closed.load(Ordering::Acquire) {
                s.cleanup_idle().await;
                if s.slot.lock().is_none() {
                    self.sessions.lock().remove(&s.id);
                }
            }
        }
        self.expire_requests();
    }
    pub async fn close_all(self: &Arc<Self>) {
        let sessions: Vec<_> = self.sessions.lock().values().cloned().collect();
        for s in &sessions {
            s.begin_close();
        }
        for s in sessions {
            s.close().await;
        }
        self.sessions.lock().clear();
    }
    pub fn summary(&self) -> J {
        json!({"connections_limit":self.config.max_connections,"sessions":self.sessions.lock().len(),"inflight":self.config.max_inflight-self.inflight.available_permits(),"requests":self.requests.lock().len(),"memory_bytes":self.used.load(Ordering::Acquire),"executed":self.executed.load(Ordering::Acquire),"failed":self.failed.load(Ordering::Acquire),"cancelled":self.cancelled.load(Ordering::Acquire),"received_bytes":self.received_bytes.load(Ordering::Relaxed),"sent_bytes":self.sent_bytes.load(Ordering::Relaxed),"request_elapsed_us":self.elapsed_us.load(Ordering::Relaxed)})
    }
}
#[derive(Clone)]
pub struct Peer {
    pub host: String,
    pub cipher: Option<String>,
    pub version: Option<String>,
    pub subject: Option<String>,
    pub issuer: Option<String>,
}
impl Peer {
    pub fn auth_request(&self) -> auth::AuthRequest<'_> {
        auth::AuthRequest {
            replacing_account: None,
            username: "",
            host: &self.host,
            auth_response: &[],
            auth_plugin: None,
            nonce: &[],
            client_capabilities: 0,
            tls_established: self.cipher.is_some(),
            tls_cipher: self.cipher.as_deref(),
            tls_version: self.version.as_deref(),
            x509_subject: self.subject.as_deref(),
            x509_issuer: self.issuer.as_deref(),
        }
    }
}
#[derive(Clone)]
pub struct Identity {
    pub key_id: String,
    pub outcome: AuthOutcome,
    pub peer: Peer,
}
pub fn authenticate<E: QueryEngine>(
    state: &State<E>,
    headers: &axum::http::HeaderMap,
    peer: Peer,
) -> Result<Identity> {
    if crate::mysql::shutdown::is_shutting_down() {
        return Err(Failure::unavailable("Server is shutting down"));
    }
    if headers.get_all("authorization").iter().count() != 1 {
        return Err(Failure::new(
            StatusCode::UNAUTHORIZED,
            "INVALID_API_KEY",
            "Exactly one Bearer credential is required",
        ));
    }
    let value = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            Failure::new(
                StatusCode::UNAUTHORIZED,
                "INVALID_API_KEY",
                "Bearer credential required",
            )
        })?;
    let (scheme, token) = value.split_once(' ').ok_or_else(|| {
        Failure::new(
            StatusCode::UNAUTHORIZED,
            "INVALID_API_KEY",
            "Bearer credential required",
        )
    })?;
    if !scheme.eq_ignore_ascii_case("Bearer") || token.len() > 256 {
        return Err(Failure::new(
            StatusCode::UNAUTHORIZED,
            "INVALID_API_KEY",
            "Invalid Bearer credential",
        ));
    }
    let (key_id, outcome) = state
        .engine
        .authenticate_api_key(token, &peer.auth_request())
        .map_err(|_| {
            Failure::new(
                StatusCode::UNAUTHORIZED,
                "INVALID_API_KEY",
                "Invalid API key or account unavailable",
            )
        })?;
    Ok(Identity {
        key_id,
        outcome,
        peer,
    })
}
pub struct Session<E: QueryEngine> {
    pub id: String,
    pub key_id: String,
    pub owner: AccountKey,
    pub engine: Arc<E>,
    pub registry: Arc<SessionRegistry>,
    pub handle: Arc<SessionHandle>,
    pub context: Mutex<Option<SessionContext>>,
    slot: Mutex<Option<ConnectionSlot>>,
    txn_admission: Mutex<Option<crate::util::exec_permit::AdmissionPermit>>,
    pub closed: AtomicBool,
    pub busy: AtomicBool,
    pub touched: AtomicU64,
    pub transaction: AtomicBool,
    pub transaction_touched: AtomicU64,
    pub info: Mutex<J>,
    _permit: Mutex<Option<OwnedSemaphorePermit>>,
    owner_state: Weak<State<E>>,
    _memory: Memory,
}
impl<E: QueryEngine> Session<E> {
    fn finish_cleanup(&self, connection_id: u32) {
        self.registry.unregister(connection_id);
        self.slot.lock().take();
        self.txn_admission.lock().take();
        self._permit.lock().take();
        if let Some(state) = self.owner_state.upgrade() {
            state.sessions.lock().remove(&self.id);
        }
    }
    pub fn begin_close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.handle.kill_connection();
        self.handle.mark_killed_state();
        crate::engine::cancel::record_killed_connection();
        self.engine.notify_lock_waiters_for_kill();
    }
    pub async fn cleanup_idle(self: &Arc<Self>) {
        if self.busy.load(Ordering::Acquire) {
            return;
        }
        let ctx = {
            let mut context = self.context.lock();
            // Serialize the ownership check with execution admission. A close
            // which observed an idle session must not steal a newly busy context.
            if self.busy.load(Ordering::Acquire) {
                return;
            }
            context.take()
        };
        if let Some(mut ctx) = ctx {
            let s = self.clone();
            let _ = crate::util::blocking_pool::spawn(move || {
                let result = s.engine.close_session(&mut ctx);
                if result.is_ok() {
                    s.finish_cleanup(ctx.connection_id);
                } else {
                    *s.context.lock() = Some(ctx);
                }
                result
            })
            .await;
        }
    }
    pub async fn close(self: &Arc<Self>) {
        self.begin_close();
        loop {
            self.cleanup_idle().await;
            if self.slot.lock().is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    fn release(self: &Arc<Self>, delivered: bool) {
        if !delivered {
            self.begin_close();
        }
        self.busy.store(false, Ordering::Release);
        self.touched.store(now(), Ordering::Release);
        self.transaction_touched.store(now(), Ordering::Release);
        if self.closed.load(Ordering::Acquire) && self.slot.lock().is_some() {
            let s = self.clone();
            tokio::spawn(async move {
                s.cleanup_idle().await;
            });
        }
    }
}
impl<E: QueryEngine> Drop for Session<E> {
    fn drop(&mut self) {
        let ctx = self.context.get_mut().take();
        let slot = self.slot.get_mut().take();
        let admission = self.txn_admission.get_mut().take();
        let rest_permit = self._permit.get_mut().take();
        let engine = self.engine.clone();
        let registry = self.registry.clone();
        if let Some(mut ctx) = ctx {
            drop(crate::util::blocking_pool::spawn(move || {
                if engine.close_session(&mut ctx).is_ok() {
                    registry.unregister(ctx.connection_id);
                    drop(slot);
                    drop(admission);
                    drop(rest_permit);
                } else {
                    std::mem::forget(slot);
                    std::mem::forget(admission);
                    std::mem::forget(rest_permit);
                    crate::util::logging::log_message(
                        "REST session cleanup failed; retaining connection charge",
                    );
                }
            }));
        }
    }
}
struct SessionCreation<E: QueryEngine> {
    session: Mutex<Option<Arc<Session<E>>>>,
    abandoned: AtomicBool,
}
struct CreationGuard<E: QueryEngine> {
    creation: Arc<SessionCreation<E>>,
    runtime: tokio::runtime::Handle,
    armed: bool,
}
impl<E: QueryEngine> Drop for CreationGuard<E> {
    fn drop(&mut self) {
        if self.armed {
            self.creation.abandoned.store(true, Ordering::Release);
            let session = self.creation.session.lock().take();
            if let Some(session) = session {
                session.begin_close();
                self.runtime.spawn(async move {
                    session.cleanup_idle().await;
                });
            }
        }
    }
}
pub async fn create_session<E: QueryEngine>(
    state: Arc<State<E>>,
    identity: Identity,
    database: Option<String>,
) -> Result<Arc<Session<E>>> {
    let permit = state
        .slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| Failure::unavailable("REST session limit exceeded"))?;
    let memory = state.memory(std::mem::size_of::<Session<E>>() + 16384)?;
    let creation = Arc::new(SessionCreation {
        session: Mutex::new(None),
        abandoned: AtomicBool::new(false),
    });
    let runtime = tokio::runtime::Handle::current();
    let mut guard = CreationGuard {
        creation: creation.clone(),
        runtime: runtime.clone(),
        armed: true,
    };
    let worker_creation = creation.clone();
    let result = crate::util::blocking_pool::spawn(move || {
        let _runtime = runtime.enter();
        let slot = ConnectionSlot::acquire_with_admin_reserve(
            crate::mysql::connection_manager::effective_max_connections(),
        ).ok_or_else(|| Failure::unavailable("SQL connection limit exceeded"))?;
        if slot.requires_connection_admin() {
            let mut check = SessionContext::default();
            check.authenticated_as = identity.outcome.context.authenticated_as.clone();
            check.active_roles = identity.outcome.context.active_roles.clone();
            state.engine.authorize_global_privilege(&check, auth::Privilege::Dynamic("CONNECTION_ADMIN".into()))?;
        }
        let id = format!("ss_{}", crate::engine::access_keys::random_id()?);
        let status = crate::engine::status_counters::SessionStatus::try_new_governed()?;
        let account = identity.outcome.context.authenticated_as.clone();
        let limits = &identity.outcome.resource_limits;
        auth::charge_reauthenticated_connection(&account, None, limits.max_connections, limits.max_user_connections)?;
        let mut ctx = SessionContext::default();
        ctx.authenticated_as = account.clone();
        ctx.user = account.user.clone();
        ctx.login_user = account.user.clone();
        ctx.host = identity.peer.host;
        ctx.active_roles = identity.outcome.context.active_roles;
        ctx.auth_epoch = identity.outcome.context.epoch;
        ctx.auth_factors_done = 1;
        ctx.rest_identity = Some(Arc::new(api::RestSessionIdentity {key_id:identity.key_id.clone(),owner:account.clone()}));
        ctx.account_limits = identity.outcome.resource_limits;
        ctx.current_user_is_system = identity.outcome.current_user_is_system;
        ctx.ssl_cipher = identity.peer.cipher;
        ctx.ssl_version = identity.peer.version;
        ctx.x509_subject = identity.peer.subject;
        ctx.x509_issuer = identity.peer.issuer;
        ctx.connection_id = state.registry.allocate_thread_id();
        ctx.status = Some(status.clone());
        let handle = state.registry.register_authenticated(ctx.connection_id, ctx.user.clone(), ctx.host.clone(), ctx.current_database.clone(), status, account.clone());
        ctx.set_cancel_handle(Some(handle.cancel_handle()));
        let info = json!({"id":id,"connection_id":ctx.connection_id,"database":ctx.current_database,"autocommit":ctx.autocommit,"in_transaction":false,"closed":false});
        let session = Arc::new(Session {
            id, key_id: identity.key_id, owner: account,
            engine: state.engine.clone(), registry: state.registry.clone(), handle,
            context: Mutex::new(Some(ctx)), slot: Mutex::new(Some(slot)),
            txn_admission: Mutex::new(None),
            closed: AtomicBool::new(false), busy: AtomicBool::new(true),
            touched: AtomicU64::new(now()), transaction: AtomicBool::new(false),
            transaction_touched: AtomicU64::new(now()), info: Mutex::new(info),
            _permit: Mutex::new(Some(permit)), owner_state:Arc::downgrade(&state), _memory: memory,
        });
        *worker_creation.session.lock() = Some(session.clone());
        state.sessions.lock().insert(session.id.clone(), session.clone());
        let mut context = session.context.lock().take().expect("new session owns its context");
        let initialized = if worker_creation.abandoned.load(Ordering::Acquire) || session.closed.load(Ordering::Acquire) {
            Err(Failure::not_found())
        } else if let Some(database) = database {
            state.engine.init_session_database(&mut context, &database).map_err(Failure::from)
        } else { Ok(()) };
        session.info.lock()["database"] = json!(context.current_database);
        *session.context.lock() = Some(context);
        session.busy.store(false, Ordering::Release);
        if initialized.is_err() || worker_creation.abandoned.load(Ordering::Acquire) || session.closed.load(Ordering::Acquire) {
            session.begin_close();
            let cleanup = session.clone();
            runtime.spawn(async move { cleanup.cleanup_idle().await; });
            initialized?;
            return Err(Failure::not_found());
        }
        Ok(session)
    }).await.map_err(|_| Failure::internal("Session admission worker failed"))?;
    if result.is_ok() {
        guard.armed = false;
        creation.session.lock().take();
    }
    result
}

pub struct RequestState<E: QueryEngine> {
    pub id: String,
    pub key_id: String,
    pub session: Weak<Session<E>>,
    pub state: Mutex<J>,
    pub completed: AtomicU64,
    completion_queue: Arc<Mutex<VecDeque<Weak<RequestState<E>>>>>,
    _memory: Mutex<Memory>,
}
impl<E: QueryEngine> RequestState<E> {
    fn cache_key(&self) -> String {
        format!("{}:{}", self.key_id, self.id)
    }
    fn finish(self: &Arc<Self>) {
        let mut queue = self.completion_queue.lock();
        if self.completed.load(Ordering::Acquire) == 0 {
            self.completed.store(now(), Ordering::Release);
            queue.push_back(Arc::downgrade(self));
        }
    }
    pub fn cancel(&self) -> bool {
        let mut status = self.state.lock();
        if self.completed.load(Ordering::Acquire) != 0 {
            return false;
        }
        let Some(session) = self.session.upgrade() else {
            return false;
        };
        session.handle.cancel_query();
        session.handle.mark_killed_state();
        crate::engine::cancel::record_killed_query();
        session.engine.notify_lock_waiters_for_kill();
        status["state"] = json!("cancel_requested");
        true
    }
}
struct GovernedPayload {
    bytes: Vec<u8>,
    _memory: Memory,
}
impl AsRef<[u8]> for GovernedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
/// Keep protocol accounting attached through Hyper/TLS buffer ownership.
pub(super) fn governed_bytes(bytes: Vec<u8>, memory: Memory) -> Bytes {
    Bytes::from_owner(GovernedPayload {
        bytes,
        _memory: memory,
    })
}
pub enum Payload {
    Encoded(Bytes),
    Structured(J, Memory),
}
#[derive(Clone, Copy)]
pub enum OutputFormat {
    Json,
    Ndjson,
    Structured {
        max_rows: usize,
        max_bytes: usize,
        explain_only: bool,
    },
}
impl OutputFormat {
    fn streaming(self) -> bool {
        matches!(self, Self::Ndjson)
    }
    fn max_bytes(self, default: usize) -> usize {
        match self {
            Self::Structured { max_bytes, .. } => max_bytes,
            _ => default,
        }
    }
}
pub struct Chunk {
    pub payload: Payload,
    pub terminal: bool,
    pub status: StatusCode,
    permit: Option<OwnedSemaphorePermit>,
}
pub struct ExecutionStream<E: QueryEngine> {
    first: Option<Chunk>,
    rx: mpsc::Receiver<Chunk>,
    session: Arc<Session<E>>,
    terminal: bool,
    temporary: bool,
    _permit: Option<OwnedSemaphorePermit>,
    deadline: Pin<Box<tokio::time::Sleep>>,
    timed_out: bool,
}
impl<E: QueryEngine> ExecutionStream<E> {
    pub fn abandon(&self) {
        self.session.begin_close();
    }
    pub fn status(&self) -> StatusCode {
        self.first.as_ref().map_or(StatusCode::OK, |c| c.status)
    }
}
impl<E: QueryEngine> Stream for ExecutionStream<E> {
    type Item = std::result::Result<Chunk, std::io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(mut chunk) = self.first.take() {
            self.terminal |= chunk.terminal;
            if let Some(permit) = chunk.permit.take() {
                self._permit = Some(permit);
            }
            return Poll::Ready(Some(Ok(chunk)));
        }
        if !self.terminal
            && !self.timed_out
            && std::future::Future::poll(self.deadline.as_mut(), cx).is_ready()
        {
            self.timed_out = true;
            self.session.begin_close();
            self.rx.close();
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "REST execution deadline exceeded; SQL outcome may be unknown",
            ))));
        }
        if self.timed_out {
            return Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(mut chunk)) => {
                self.terminal |= chunk.terminal;
                if let Some(permit) = chunk.permit.take() {
                    self._permit = Some(permit);
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
impl<E: QueryEngine> Drop for ExecutionStream<E> {
    fn drop(&mut self) {
        if self.temporary {
            self.session.closed.store(true, Ordering::Release);
        }
        self.session.release(self.terminal);
    }
}
struct AdmissionGuard<E: QueryEngine> {
    session: Arc<Session<E>>,
    armed: bool,
    started: bool,
    executing: bool,
    request: Option<Arc<RequestState<E>>>,
}
impl<E: QueryEngine> Drop for AdmissionGuard<E> {
    fn drop(&mut self) {
        if self.armed {
            if let Some(request) = &self.request {
                let mut value = request.state.lock();
                if request.completed.load(Ordering::Acquire) == 0 {
                    *value = json!({"request_id":request.id,"state":if self.executing{"cancel_requested"}else{"failed"},"execution_outcome":if self.executing{"unknown"}else{"not_started"}});
                    if !self.executing {
                        request.finish();
                    }
                }
            }
            if self.started {
                self.session.begin_close();
                self.session.release(false);
            } else {
                self.session.busy.store(false, Ordering::Release);
            }
        }
    }
}
struct ContextGuard<E: QueryEngine> {
    session: Arc<Session<E>>,
    ctx: Option<SessionContext>,
    returned: bool,
}
impl<E: QueryEngine> Drop for ContextGuard<E> {
    fn drop(&mut self) {
        if !self.returned {
            self.session.begin_close();
        }
        if let Some(ctx) = self.ctx.take() {
            *self.session.context.lock() = Some(ctx);
        }
    }
}
pub struct Output<E: QueryEngine> {
    state: Arc<State<E>>,
    tx: mpsc::Sender<Chunk>,
    stream: bool,
    format: OutputFormat,
    rows: usize,
    pub results: Vec<J>,
    pub bytes: usize,
    results_omitted: bool,
    _memory: Memory,
    deadline: Instant,
    cancel: crate::engine::cancel::QueryCancelToken,
    status: StatusCode,
    permit: Option<OwnedSemaphorePermit>,
}
impl<E: QueryEngine> Output<E> {
    fn send(&mut self, value: J, terminal: bool) -> Result<()> {
        if matches!(self.format, OutputFormat::Structured { .. }) {
            let memory = std::mem::replace(&mut self._memory, self.state.memory(0)?);
            let permit = if terminal { self.permit.take() } else { None };
            return self.deliver(Chunk {
                payload: Payload::Structured(value, memory),
                terminal,
                status: self.status,
                permit,
            });
        }
        let required = codec::encoded_len(&value)? + usize::from(self.stream);
        let mut bytes = Vec::with_capacity(required);
        serde_json::to_writer(&mut bytes, &value)
            .map_err(|_| Failure::internal("Result encoding failed"))?;
        if self.stream {
            bytes.push(b'\n');
        }
        drop(value);
        if terminal && !self.stream {
            self._memory = self.state.memory(0)?;
        }
        let memory = self.state.memory(bytes.capacity() + 128)?;
        let permit = if terminal { self.permit.take() } else { None };
        self.deliver(Chunk {
            payload: Payload::Encoded(governed_bytes(bytes, memory)),
            terminal,
            status: self.status,
            permit,
        })
    }
    fn send_control(
        &mut self,
        request: &Arc<RequestState<E>>,
        value: J,
        status: StatusCode,
    ) -> Result<()> {
        if matches!(self.format, OutputFormat::Structured { .. }) {
            let memory = request._memory.lock().split(2048);
            let permit = self.permit.take();
            return self.deliver(Chunk {
                payload: Payload::Structured(value, memory),
                terminal: true,
                status,
                permit,
            });
        }
        let mut bytes =
            serde_json::to_vec(&value).map_err(|_| Failure::internal("Error encoding failed"))?;
        if self.stream {
            bytes.push(b'\n');
        }
        let required = bytes.capacity() + 128;
        let memory = {
            let mut reservation = request._memory.lock();
            if required > reservation.bytes {
                return Err(Failure::unavailable(
                    "Control response exceeds its reserved capacity",
                ));
            }
            reservation.split(required)
        };
        let permit = self.permit.take();
        self.deliver(Chunk {
            payload: Payload::Encoded(governed_bytes(bytes, memory)),
            terminal: true,
            status,
            permit,
        })
    }
    fn deliver(&self, chunk: Chunk) -> Result<()> {
        let terminal = chunk.terminal;
        if let Payload::Encoded(bytes) = &chunk.payload {
            self.state
                .sent_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        let remaining = if terminal {
            Duration::from_secs(1)
        } else {
            self.deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_secs(30))
        };
        if remaining.is_zero() {
            return Err(Failure::new(
                StatusCode::GATEWAY_TIMEOUT,
                "TIMEOUT",
                "REST response deadline exceeded",
            ));
        }
        let tx = self.tx.clone();
        let cancel = self.cancel.clone();
        tokio::runtime::Handle::current().block_on(async move {
            let mut tick=tokio::time::interval(Duration::from_millis(50));
            let send=tx.send(chunk); tokio::pin!(send);
            let timeout=tokio::time::sleep(remaining); tokio::pin!(timeout);
            loop { tokio::select! {
                result=&mut send=>return result.map_err(|_|Failure::new(StatusCode::CONFLICT,"DISCONNECTED","Result consumer disconnected")),
                _=&mut timeout=>return Err(Failure::new(StatusCode::GATEWAY_TIMEOUT,"SLOW_CONSUMER","Result consumer exceeded deadline")),
                _=tick.tick(), if !terminal=>{cancel.check()?;}
            }}
        })
    }
    fn event(&mut self, value: J) -> Result<()> {
        if self.stream {
            self.send(value, false)
        } else {
            Ok(())
        }
    }
    fn push_result(&mut self, result: J, charged_row_bytes: usize) -> Result<()> {
        if self.stream || self.results_omitted {
            return Ok(());
        }
        if matches!(self.format, OutputFormat::Json) {
            self.results.push(result);
            return Ok(());
        }
        let overhead = codec::encoded_len(&result)?
            .saturating_sub(charged_row_bytes)
            .saturating_add(1);
        if self.bytes.saturating_add(overhead)
            > self
                .format
                .max_bytes(self.state.config.max_json_response_bytes)
        {
            self.results_omitted = true;
            self.results.clear();
            self._memory = self.state.memory(0)?;
            return Ok(());
        }
        self.bytes += overhead;
        self.state.grow_memory(
            &mut self._memory,
            overhead.saturating_mul(3).saturating_add(128),
        )?;
        self.results.push(result);
        Ok(())
    }
    fn row(&mut self, row: J, rows: &mut Vec<J>, bytes: usize) -> Result<()> {
        if !self.stream && self.results_omitted {
            return Ok(());
        }
        self.rows = self.rows.saturating_add(1);
        if !self.stream
            && (self.bytes.saturating_add(bytes)
                > self
                    .format
                    .max_bytes(self.state.config.max_json_response_bytes)
                || matches!(self.format, OutputFormat::Structured { max_rows, .. } if self.rows > max_rows))
        {
            // Drain the cursor and finish execution/COMMIT before reporting the
            // output failure. A response limit must not obscure a known outcome.
            self.results_omitted = true;
            self.results.clear();
            rows.clear();
            self._memory = self.state.memory(0)?;
            return Ok(());
        }
        if bytes
            > self
                .format
                .max_bytes(self.state.config.max_json_response_bytes)
        {
            return Err(Failure::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "RESULT_TOO_LARGE",
                "Single result row exceeds response limit",
            ));
        }
        if !self.stream {
            self.bytes = self.bytes.saturating_add(bytes);
            if self.bytes
                > self
                    .format
                    .max_bytes(self.state.config.max_json_response_bytes)
            {
                return Err(Failure::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "RESULT_TOO_LARGE",
                    "JSON result exceeds limit; request NDJSON",
                ));
            }
            let cells = row.as_array().map_or(0, Vec::len);
            self.state.grow_memory(
                &mut self._memory,
                bytes * 2 + cells * std::mem::size_of::<J>() + 64,
            )?;
        }
        rows.push(row);
        Ok(())
    }
}
fn diagnostics(ctx: &SessionContext) -> J {
    let warnings:Vec<_>=ctx.warnings.iter().take(64).map(|w|json!({"level":format!("{:?}",w.level()),"mysql_errno":w.code(),"sqlstate":w.sql_state().as_str(),"message":w.message()})).collect();
    json!({"warning_count":ctx.warnings.total_len(),"warnings_truncated":ctx.warnings.total_len()>warnings.len(),"warnings":warnings,"autocommit":ctx.autocommit,"in_transaction":ctx.in_transaction(),"time_zone":ctx.sysvars.time_zone})
}
fn emit<E: QueryEngine>(
    output: &mut Output<E>,
    ctx: &mut SessionContext,
    response: QueryResponse,
    statement: usize,
    index: &mut usize,
    meta: &J,
) -> Result<()> {
    if let QueryResponse::MultiResult(results) = response {
        for result in results {
            emit(output, ctx, result, statement, index, meta)?;
        }
        return Ok(());
    }
    let current = *index;
    *index += 1;
    let (columns, mut cursor): (Vec<crate::types::Column>, Option<Box<dyn ResultCursor>>) =
        match response {
            QueryResponse::ResultSet(r) => {
                (r.columns, Some(Box::new(VecResultCursor::new(r.rows))))
            }
            QueryResponse::SharedResultSet(r) => (
                r.columns.to_vec(),
                Some(Box::new(VecResultCursor::new(r.rows))),
            ),
            QueryResponse::StreamingResultSet(r) => (r.columns, Some(r.cursor)),
            QueryResponse::Ok | QueryResponse::OkAffected { .. } => {
                let affected = if let QueryResponse::OkAffected { affected_rows } = response {
                    affected_rows
                } else {
                    0
                };
                let result = json!({"statement_index":statement,"result_index":current,"kind":"ok","affected_rows":affected.to_string(),"matched_rows":meta["matched_rows"],"last_insert_id":meta["last_insert_id"],"info":meta["info"],"diagnostics":diagnostics(ctx)});
                output.event(json!({"type":"result_end","result":result}))?;
                output.push_result(result, 0)?;
                return Ok(());
            }
            QueryResponse::MultiResult(_) => unreachable!(),
        };
    let metadata: Vec<_> = columns.iter().map(codec::column).collect();
    output.event(json!({"type":"result_start","statement_index":statement,"result_index":current,"columns":metadata}))?;
    let mut rows = Vec::new();
    let mut row_count = 0u64;
    let mut batch_bytes = 0usize;
    let mut batch_memory = output.state.memory(0)?;
    let cursor = cursor.as_mut().unwrap();
    loop {
        crate::engine::cancel::check_cancelled()?;
        let Some(row) = cursor.next_row()? else { break };
        if !output.stream && output.results_omitted {
            row_count += 1;
            continue;
        }
        let row = codec::row(row, &columns)?;
        let size = codec::encoded_len(&row)?;
        if output.stream {
            output.state.grow_memory(
                &mut batch_memory,
                size * 2 + columns.len() * std::mem::size_of::<J>() + 64,
            )?;
        }
        output.row(row, &mut rows, size)?;
        batch_bytes += size;
        row_count += 1;
        if output.stream && batch_bytes >= 32768 {
            output.event(json!({"type":"rows","statement_index":statement,"result_index":current,"rows":std::mem::take(&mut rows)}))?;
            batch_memory = output.state.memory(0)?;
            batch_bytes = 0;
        }
    }
    ctx.warnings.extend(cursor.take_warnings());
    if output.stream && !rows.is_empty() {
        output.event(json!({"type":"rows","statement_index":statement,"result_index":current,"rows":std::mem::take(&mut rows)}))?;
    }
    let end = json!({"statement_index":statement,"result_index":current,"row_count":row_count.to_string(),"diagnostics":diagnostics(ctx)});
    output.event(json!({"type":"result_end","result":end}))?;
    if !output.stream && !output.results_omitted {
        output.push_result(json!({"statement_index":statement,"result_index":current,"kind":"result_set","columns":metadata,"rows":rows,"row_count":row_count.to_string(),"diagnostics":diagnostics(ctx)}), batch_bytes)?;
    }
    Ok(())
}
fn parse_item(
    item: &SqlItem,
    ctx: &SessionContext,
) -> Result<(crate::parser::Statement, Vec<crate::types::Value>)> {
    let _mode = crate::parser::ParserSqlModeGuard::activate(api::parse_temporal_sql_mode_bits(
        &ctx.sql_mode,
    ));
    let (_, ranges) = crate::parser::split_query_statements(
        &item.sql,
        api::parse_temporal_sql_mode_bits(&ctx.sql_mode),
    );
    if ranges.len() != 1 {
        return Err(Failure::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "MULTIPLE_STATEMENTS_NOT_ALLOWED",
            "Each SQL item must contain exactly one statement",
        ));
    }
    let mut statement = crate::parser::parse_statement(&item.sql)
        .map_err(|e| Failure::from(e.spec.error(e.to_string())))?;
    if matches!(statement, crate::parser::Statement::MultiStatement { .. }) {
        return Err(Failure::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "MULTIPLE_STATEMENTS_NOT_ALLOWED",
            "Each SQL item must contain exactly one statement",
        ));
    }
    if matches!(&statement, crate::parser::Statement::LoadData(load) if load.local) {
        return Err(Failure::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "UNSUPPORTED_CLIENT_UPLOAD",
            "LOAD DATA LOCAL requires the MySQL client upload protocol",
        ));
    }
    let mut count = 0;
    crate::parser::number_statement_placeholders(&mut statement, &mut count);
    if count != item.params.len() {
        return Err(Failure::bad("SQL placeholder count does not match params"));
    }
    let params = item
        .params
        .iter()
        .map(|p| codec::param(p, ctx))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok((statement, params))
}
fn atomic_allowed(s: &crate::parser::Statement) -> bool {
    use crate::parser::Statement as S;
    matches!(
        s,
        S::Select(_) | S::SetOperation(_) | S::Insert(_) | S::Update(_) | S::Delete(_)
    ) || matches!(s,S::WithCte(w)if atomic_allowed(&w.body))
}
fn temporary_forbidden(s: &crate::parser::Statement) -> bool {
    use crate::parser::Statement as S;
    matches!(
        s,
        S::Begin { .. }
            | S::Commit { .. }
            | S::Rollback { .. }
            | S::Savepoint(_)
            | S::RollbackToSavepoint(_)
            | S::ReleaseSavepoint(_)
            | S::SetAutocommit(false)
            | S::XaStart { .. }
            | S::XaEnd { .. }
            | S::XaPrepare { .. }
            | S::XaCommit { .. }
            | S::XaRollback { .. }
    )
}
fn perform<E: QueryEngine>(
    state: &State<E>,
    session: &Session<E>,
    ctx: &mut SessionContext,
    items: &[SqlItem],
    atomic: bool,
    temporary: bool,
    request: &RequestState<E>,
    output: &mut Output<E>,
    failed_at: &mut usize,
    started: &mut bool,
    owns_atomic_transaction: &mut bool,
) -> Result<()> {
    if session.closed.load(Ordering::Acquire)
        || !state
            .engine
            .api_key_is_valid(&session.key_id, &session.owner)
    {
        return Err(Failure::new(
            StatusCode::UNAUTHORIZED,
            "INVALID_API_KEY",
            "API key or session unavailable",
        ));
    }
    if atomic && (ctx.in_transaction() || !ctx.autocommit) {
        return Err(Failure::new(
            StatusCode::CONFLICT,
            "TRANSACTION_ACTIVE",
            "Atomic batch requires autocommit and no transaction",
        ));
    }
    // Decode all parameter values before the first side effect.
    let mut prepared = Vec::new();
    for (index, item) in items.iter().enumerate() {
        *failed_at = index;
        let parsed = parse_item(item, ctx)?;
        if matches!(
            output.format,
            OutputFormat::Structured {
                explain_only: true,
                ..
            }
        ) {
            use crate::parser::Statement as S;
            if !matches!(&parsed.0, S::Explain { analyze: false, target, .. }
                if matches!(target.as_ref(), S::Select(_) | S::WithCte(_) | S::SetOperation(_)
                    | S::Insert(_) | S::Update(_) | S::Delete(_)))
            {
                return Err(Failure::bad(
                    "explain_sql accepts an explainable SQL statement, without EXPLAIN or ANALYZE",
                ));
            }
        }
        if atomic && !atomic_allowed(&parsed.0) {
            return Err(Failure::bad(
                "Atomic batch accepts transactional query and DML statements only",
            ));
        }
        if temporary && temporary_forbidden(&parsed.0) {
            return Err(Failure::bad(
                "Explicit transactions require a persistent session",
            ));
        }
        prepared.push(parsed);
    }
    output.event(json!({"type":"request_start","request_id":request.id,"session_id":if temporary{None}else{Some(&session.id)}}))?;
    if atomic {
        // Precondition/argument failures must never roll back a transaction
        // which was already owned by the caller before this request.
        *owns_atomic_transaction = true;
        state.engine.execute(ctx, "BEGIN")?;
    }
    for (i, item) in items.iter().enumerate() {
        *failed_at = i;
        crate::engine::cancel::check_cancelled()?;
        if !state
            .engine
            .api_key_is_valid(&session.key_id, &session.owner)
        {
            return Err(Failure::new(
                StatusCode::UNAUTHORIZED,
                "INVALID_API_KEY",
                "API key expired or revoked",
            ));
        }
        // Reparse subsequent statements under the SQL mode changed by previous SQL.
        let (statement, params) = if i == 0 || atomic {
            prepared[i].clone()
        } else {
            parse_item(item, ctx)?
        };
        if temporary && temporary_forbidden(&statement) {
            return Err(Failure::bad(
                "Explicit transactions require a persistent session",
            ));
        }
        session.handle.mark_busy(
            "Query",
            Some(crate::engine::logging::redact_sql_for_log(&item.sql)),
            &ctx.current_database,
        );
        crate::mysql::connection_manager::record_statement();
        state.executed.fetch_add(1, Ordering::Relaxed);
        *started = true;
        let response = state.engine.execute_parsed_with_params(
            ctx,
            &item.sql,
            Arc::new(statement),
            params,
            Arc::from(item.sql.as_str()),
        )?;
        let insert = api::take_statement_ok_insert_id().unwrap_or(ctx.statement_insert_id);
        let meta = json!({"last_insert_id":insert.to_string(),"matched_rows":api::take_statement_matched_rows().map(|v|v.to_string()),"info":api::take_statement_info()});
        let mut index = 0;
        emit(output, ctx, response, i, &mut index, &meta)?;
        if temporary && !atomic && (!ctx.autocommit || ctx.in_transaction()) {
            return Err(Failure::bad(
                "SQL left an open transaction; use a persistent session",
            ));
        }
    }
    if atomic {
        *failed_at = items.len();
        state.engine.execute(ctx, "COMMIT")?;
    }
    Ok(())
}
pub async fn start<E: QueryEngine>(
    state: Arc<State<E>>,
    session: Arc<Session<E>>,
    items: Vec<SqlItem>,
    atomic: bool,
    temporary: bool,
    timeout_ms: u64,
    id: String,
    format: OutputFormat,
    request_memory: impl Into<ExecutionResources> + Send,
) -> Result<ExecutionStream<E>> {
    let request_memory = request_memory.into();
    let stream = format.streaming();
    let admitted_at = Instant::now();
    let absolute_deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let permit = state
        .inflight
        .clone()
        .try_acquire_owned()
        .map_err(|_| Failure::unavailable("REST execution limit exceeded"))?;
    if session
        .busy
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(Failure::new(
            StatusCode::CONFLICT,
            "SESSION_BUSY",
            "Session is executing or sending a result",
        ));
    }
    let mut guard = AdmissionGuard {
        session: session.clone(),
        armed: true,
        started: false,
        executing: false,
        request: None,
    };
    let (mode, in_transaction, token) = {
        let mut context = session.context.lock();
        if session.closed.load(Ordering::Acquire) {
            return Err(Failure::not_found());
        }
        let context = context.as_mut().ok_or_else(Failure::not_found)?;
        // Snapshot before publishing a cancellable request. Otherwise an early
        // cancellation can be swallowed by this generation refresh.
        context.refresh_cancel_generations();
        let token = context
            .current_cancel_token()
            .ok_or_else(|| Failure::internal("Missing SQL cancellation context"))?
            .clone();
        if session.closed.load(Ordering::Acquire) {
            return Err(Failure::not_found());
        }
        (
            api::parse_temporal_sql_mode_bits(&context.sql_mode),
            context.in_transaction(),
            token,
        )
    };
    let memory = state.memory(4096)?;
    let request = Arc::new(RequestState {
        id: id.clone(),
        key_id: session.key_id.clone(),
        session: Arc::downgrade(&session),
        state: Mutex::new(
            json!({"request_id":id,"state":"accepted","execution_outcome":"not_started"}),
        ),
        completed: AtomicU64::new(0),
        completion_queue: state.completed_requests.clone(),
        _memory: Mutex::new(memory),
    });
    state.register_request(request.clone())?;
    guard.request = Some(request.clone());
    // Admission waits run on the reactor. Open transactions retain their
    // scheduling permit so COMMIT/ROLLBACK can always make progress.
    guard.started = true;
    let source: Vec<_> = items.iter().map(|item| item.sql.clone()).collect();
    let classification_token = token.clone();
    let classification = crate::util::blocking_pool::spawn(move || {
        let _cancel = crate::engine::cancel::enter(classification_token);
        let _deadline = crate::engine::cancel::install_statement_deadline(
            absolute_deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_nanos(1)),
        );
        let _mode = crate::parser::ParserSqlModeGuard::activate(mode);
        let needs_admission = in_transaction
            || source.iter().any(|sql| {
                crate::parser::parse_statement(sql).is_ok_and(|statement| {
                    !crate::mysql::session::statement_is_lock_safe_read(&statement)
                        || crate::parser::collect_statement_function_names(&statement)
                            .iter()
                            .any(|name| {
                                !crate::engine::cbo::function_cost::is_known_builtin_function(name)
                            })
                })
            });
        // Transfer the input charge through classification, not merely through
        // its awaiting future. Dropping a BlockingHandle does not stop its job.
        (needs_admission, request_memory, permit)
    });
    let (needs_admission, request_memory, permit) = tokio::time::timeout_at(
        tokio::time::Instant::from_std(absolute_deadline),
        classification,
    )
    .await
    .map_err(|_| {
        Failure::new(
            StatusCode::GATEWAY_TIMEOUT,
            "TIMEOUT",
            "SQL classification deadline exceeded",
        )
    })?
    .map_err(|_| Failure::internal("SQL classification worker failed"))?;
    if needs_admission && session.txn_admission.lock().is_none() {
        let acquire = crate::util::exec_permit::acquire();
        tokio::pin!(acquire);
        let remaining = absolute_deadline.saturating_duration_since(Instant::now());
        let deadline = tokio::time::sleep(remaining);
        tokio::pin!(deadline);
        let mut tick = tokio::time::interval(Duration::from_millis(25));
        loop {
            tokio::select! {
                permit = &mut acquire => { *session.txn_admission.lock() = permit; break; },
                _ = &mut deadline => {
                    *request.state.lock() = json!({"request_id":request.id,"state":"failed","execution_outcome":"not_started","error":{"code":"TIMEOUT","message":"Transaction admission deadline exceeded"}});
                    request.finish();
                    return Err(Failure::new(StatusCode::GATEWAY_TIMEOUT,"TIMEOUT","Transaction admission deadline exceeded"));
                },
                _ = tick.tick() => { token.check()?; }
            }
        }
    }
    let (tx, mut rx) = mpsc::channel(2);
    let worker_state = state.clone();
    let worker_session = session.clone();
    let runtime = tokio::runtime::Handle::current();
    let supervision_request = request.clone();
    let task = crate::util::blocking_pool::spawn(move || {
        let _runtime = runtime.enter();
        let _request_memory = request_memory;
        let Some(ctx) = worker_session.context.lock().take() else {
            *request.state.lock() = json!({"request_id":request.id,"state":"cancelled","execution_outcome":"not_started","http_status":504});
            request.finish();
            return;
        };
        let mut owned = ContextGuard {
            session: worker_session.clone(),
            ctx: Some(ctx),
            returned: false,
        };
        let ctx = owned.ctx.as_mut().unwrap();
        let _cancel = crate::engine::cancel::install_session_cancel_context(ctx);
        let deadline = absolute_deadline;
        let _deadline = crate::engine::cancel::install_statement_deadline(
            deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_nanos(1)),
        );
        *request.state.lock() =
            json!({"request_id":request.id,"state":"running","execution_outcome":"unknown"});
        let mut output = Output {
            state: worker_state.clone(),
            tx,
            stream,
            format,
            rows: 0,
            results: Vec::new(),
            bytes: 0,
            results_omitted: false,
            _memory: worker_state.memory(0).expect("zero-byte protocol charge"),
            deadline,
            cancel: ctx.current_cancel_token().unwrap().clone(),
            status: StatusCode::OK,
            permit: Some(permit),
        };
        let mut failed_at = 0;
        let mut started = false;
        let mut owns_atomic_transaction = false;
        let result = perform(
            &worker_state,
            &worker_session,
            ctx,
            &items,
            atomic,
            temporary,
            &request,
            &mut output,
            &mut failed_at,
            &mut started,
            &mut owns_atomic_transaction,
        );
        let commit_failed = atomic && failed_at == items.len() && result.is_err();
        if commit_failed {
            worker_session.begin_close();
        }
        let mut rolled_back = false;
        let mut cleanup_failed = false;
        if !commit_failed
            && result.is_err()
            && (owns_atomic_transaction || temporary)
            && ctx.in_transaction()
        {
            // Cleanup must remain possible after the query deadline or cancellation fired.
            ctx.refresh_cancel_generations();
            let _cleanup_cancel = crate::engine::cancel::install_session_cancel_context(ctx);
            let _cleanup_deadline =
                crate::engine::cancel::install_statement_deadline(Duration::ZERO);
            rolled_back = worker_state
                .engine
                .rollback_session_for_cleanup(ctx)
                .is_ok();
            if !rolled_back {
                // A caller must never be able to COMMIT a partially failed
                // atomic batch after its rollback failed. Retain ownership for
                // close_session cleanup and report the outcome as unknown.
                cleanup_failed = true;
                worker_session.begin_close();
            }
        }
        let mut final_error = None;
        let mut status = match result {
            Ok(()) => StatusCode::OK,
            Err(e) => {
                let status = e.status;
                final_error = Some(e);
                worker_state.failed.fetch_add(1, Ordering::Relaxed);
                status
            }
        };
        let execution = if final_error.is_none() {
            "succeeded"
        } else if !started {
            "not_started"
        } else if rolled_back {
            "failed"
        } else {
            "unknown"
        };
        let transaction = if commit_failed || cleanup_failed {
            "unknown"
        } else if ctx.in_transaction() {
            "active"
        } else {
            "idle"
        };
        let info = json!({"id":worker_session.id,"connection_id":ctx.connection_id,"database":ctx.current_database,"autocommit":ctx.autocommit,"in_transaction":ctx.in_transaction(),"closed":temporary||ctx.close_after_response||worker_session.closed.load(Ordering::Acquire),"time_zone":ctx.sysvars.time_zone});
        *worker_session.info.lock() = info.clone();
        worker_session
            .transaction
            .store(ctx.in_transaction(), Ordering::Release);
        worker_session
            .transaction_touched
            .store(now(), Ordering::Release);
        worker_session.handle.mark_idle(&ctx.current_database);
        let error = final_error.as_ref().map(|e| {
            let mut value = e.value();
            value["statement_index"] = json!(if commit_failed { None } else { Some(failed_at) });
            value["stage"] = json!(if commit_failed { "commit" } else { "execute" });
            value["request_id"] = json!(request.id);
            value["execution_outcome"] = json!(execution);
            value["transaction_state"] = json!(transaction);
            value["rollback_confirmed"] = json!(rolled_back);
            value
        });
        let mut terminal = if stream {
            if let Some(e) = error {
                json!({"type":"error","error":e,"session":info})
            } else {
                json!({"type":"complete","request_id":request.id,"session":info})
            }
        } else {
            json!({"request_id":request.id,"session_id":if temporary{None}else{Some(&worker_session.id)},"results":std::mem::take(&mut output.results),"session":info,"error":error,"execution_outcome":execution,"transaction_state":transaction})
        };
        if !stream
            && (output.results_omitted
                || codec::encoded_len(&terminal).unwrap_or(usize::MAX)
                    > format.max_bytes(worker_state.config.max_json_response_bytes))
        {
            let failure = Failure::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "RESULT_TOO_LARGE",
                "Complete result exceeds configured response limit",
            );
            terminal["results"] = json!([]);
            terminal["results_omitted"] = json!(true);
            let mut detail = failure.value();
            detail["execution_outcome"] = json!(execution);
            detail["transaction_state"] = json!(transaction);
            detail["rollback_confirmed"] = json!(rolled_back);
            if final_error.is_none() {
                terminal["error"] = detail;
                status = failure.status;
                final_error = Some(failure);
            }
        }
        {
            let mut record = request.state.lock();
            let mut cached_error = if stream {
                terminal.get("error").cloned().unwrap_or(J::Null)
            } else {
                terminal["error"].clone()
            };
            if let Some(error) = cached_error.as_object_mut() {
                error.remove("message");
                error.remove("message_truncated");
            }
            *record = json!({"request_id":request.id,"state":if final_error.is_none(){"completed"}else{"failed"},"http_status":status.as_u16(),"execution_outcome":execution,"transaction_state":transaction,"rollback_confirmed":rolled_back,"error":cached_error});
            request.finish();
        }
        worker_state.elapsed_us.fetch_add(
            admitted_at.elapsed().as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        // Publish the context before exposing the terminal frame: a fast client
        // may immediately submit the next request on another HTTP connection.
        if owned
            .ctx
            .as_ref()
            .is_some_and(|ctx| ctx.current_txn.is_none() && ctx.xa_branch.is_none())
        {
            worker_session.txn_admission.lock().take();
        }
        if temporary {
            worker_session.closed.store(true, Ordering::Release);
            if let Some(context) = owned.ctx.as_mut() {
                if worker_state.engine.close_session(context).is_ok() {
                    worker_session.finish_cleanup(context.connection_id);
                    owned.ctx.take();
                }
            }
        }
        *worker_session.context.lock() = owned.ctx.take();
        owned.returned = true;
        output.status = status;
        if let Err(error) = output.send(terminal, true) {
            if error.status == StatusCode::SERVICE_UNAVAILABLE {
                output._memory = worker_state.memory(0).expect("zero-byte protocol charge");
                let mut detail = error.value();
                detail["execution_outcome"] = json!(execution);
                detail["transaction_state"] = json!(transaction);
                detail["rollback_confirmed"] = json!(rolled_back);
                let fallback = json!({"request_id":request.id,"session_id":if temporary{None}else{Some(&worker_session.id)},"results":[],"results_omitted":true,"session":{"closed":temporary||worker_session.closed.load(Ordering::Acquire)},"error":detail,"execution_outcome":execution,"transaction_state":transaction});
                {
                    let mut summary = request.state.lock();
                    summary["state"] = json!("failed");
                    summary["http_status"] = json!(error.status.as_u16());
                    summary["error"] = json!({"code":error.code});
                }
                if output
                    .send_control(&request, fallback, error.status)
                    .is_err()
                {
                    worker_session.begin_close();
                }
            } else {
                worker_session.begin_close();
            }
        }
        if temporary
            || worker_session
                .context
                .lock()
                .as_ref()
                .is_some_and(|c| c.close_after_response)
        {
            worker_session.closed.store(true, Ordering::Release);
        }
    });
    guard.executing = true;
    let cleanup_session = session.clone();
    tokio::spawn(async move {
        if task.await.is_err() {
            cleanup_session.begin_close();
            *supervision_request.state.lock() = json!({"request_id":supervision_request.id,"state":"failed","execution_outcome":"unknown","http_status":500,"error":{"code":"INTERNAL_ERROR","message":"SQL execution worker failed"}});
            supervision_request.finish();
        }
        if cleanup_session.closed.load(Ordering::Acquire) {
            cleanup_session.busy.store(false, Ordering::Release);
            cleanup_session.cleanup_idle().await;
        }
    });
    let transport_deadline =
        tokio::time::Instant::from_std(absolute_deadline + Duration::from_secs(1));
    let first = tokio::time::timeout_at(transport_deadline, rx.recv())
        .await
        .map_err(|_| {
            Failure::new(
                StatusCode::GATEWAY_TIMEOUT,
                "TIMEOUT",
                "Cancellation requested; SQL execution outcome may be unknown",
            )
            .with_outcome("unknown")
            .with_request_id(id.clone())
        })?
        .ok_or_else(|| {
            Failure::internal("SQL execution ended without a terminal result")
                .with_outcome("unknown")
                .with_request_id(id.clone())
        })?;
    let response_stream = ExecutionStream {
        first: Some(first),
        rx,
        session: session.clone(),
        terminal: false,
        temporary,
        _permit: None,
        deadline: Box::pin(tokio::time::sleep_until(transport_deadline)),
        timed_out: false,
    };
    guard.armed = false;
    Ok(response_stream)
}

#[cfg(test)]
mod history_tests {
    use super::*;
    struct Engine;
    impl QueryEngine for Engine {
        fn execute(
            &self,
            _: &mut SessionContext,
            _: &str,
        ) -> std::result::Result<QueryResponse, crate::types::QueryError> {
            Ok(QueryResponse::Ok)
        }
    }
    fn record(state: &State<Engine>, id: &str, charge: usize) -> Arc<RequestState<Engine>> {
        Arc::new(RequestState {
            id: id.into(),
            key_id: "test-key".into(),
            session: Weak::new(),
            state: Mutex::new(J::Null),
            completed: AtomicU64::new(0),
            completion_queue: state.completed_requests.clone(),
            _memory: Mutex::new(state.memory(charge).unwrap()),
        })
    }
    #[test]
    fn rest_protocol_history_eviction_preserves_active_requests_and_reuses_expired_ids() {
        let state = State::new(
            RestConfig::from_startup().unwrap(),
            Arc::new(Engine),
            Arc::new(SessionRegistry::new()),
        );
        let active = record(&state, "active-request-0001", 0);
        state.register_request(active.clone()).unwrap();
        for n in 0..10002 {
            let request = record(&state, &format!("completed-{n:08}"), 0);
            state.register_request(request.clone()).unwrap();
            request.finish();
        }
        assert_eq!(state.requests.lock().len(), 10000);
        assert!(state.request("active-request-0001", "test-key").is_ok());
        assert!(state.request("completed-00000000", "test-key").is_err());
        assert_eq!(
            state
                .register_request(record(&state, "active-request-0001", 0))
                .unwrap_err()
                .status,
            StatusCode::CONFLICT
        );
        state
            .register_request(record(&state, "completed-00000000", 0))
            .unwrap();
        assert!(state.request("completed-00000000", "test-key").is_ok());
    }
    #[test]
    fn rest_protocol_history_yields_memory_to_active_work() {
        let mut config = RestConfig::from_startup().unwrap();
        config.memory_limit = 16384;
        let state = State::new(config, Arc::new(Engine), Arc::new(SessionRegistry::new()));
        for n in 0..4 {
            let request = record(&state, &format!("old-request-{n:04}"), 4096);
            state.register_request(request.clone()).unwrap();
            request.finish();
        }
        assert_eq!(state.used.load(Ordering::Acquire), 16384);
        let active = state.memory(8192).unwrap();
        assert_eq!(state.requests.lock().len(), 2);
        assert_eq!(state.used.load(Ordering::Acquire), 16384);
        drop(active);
        assert_eq!(state.used.load(Ordering::Acquire), 8192);
    }
}
