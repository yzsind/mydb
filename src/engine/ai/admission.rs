//! Atomic, work-conserving admission across connection, model and account limits.
use super::*;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

const MAX_SCOPES: usize = 1024;
const MAX_WAITERS: usize = 4352;
type MetadataCharge = crate::engine::executor::mem_budget::ControlledMemoryCharge;
fn metadata_charge(bytes: usize) -> Result<MetadataCharge, QueryError> {
    MetadataCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
        bytes,
    )
}
#[derive(Clone)]
pub(crate) struct Scope {
    key: String,
    generation: u64,
    capacity: usize,
    rpm: Option<u64>,
}
pub(crate) fn scopes(
    namespace: &str,
    principal: &str,
    connection: &Connection,
    model: &Model,
    user: Option<&UserLimit>,
    policy_generation: u64,
    instance_capacity: usize,
) -> Vec<Scope> {
    vec![
        Scope {
            key: "instance".into(),
            generation: 0,
            capacity: instance_capacity,
            rpm: None,
        },
        Scope {
            key: format!("{namespace}:connection:{}", connection.id),
            generation: connection.generation,
            capacity: connection.limits.max_inflight.unwrap_or(16).min(256),
            rpm: connection.limits.requests_per_minute,
        },
        Scope {
            key: format!("{namespace}:model:{}", model.id),
            generation: model.generation,
            capacity: model.limits.max_inflight.unwrap_or(16).min(256),
            rpm: model.limits.requests_per_minute,
        },
        Scope {
            key: format!("{namespace}:user:{principal}"),
            generation: policy_generation,
            capacity: user.and_then(|u| u.max_inflight).unwrap_or(4).min(256),
            rpm: user.and_then(|u| u.requests_per_minute),
        },
    ]
}
struct Bucket {
    tokens: f64,
    last: Instant,
    rate: u64,
}
impl Bucket {
    fn refill(&mut self, now: Instant) {
        self.tokens = (self.tokens
            + now.duration_since(self.last).as_secs_f64() * self.rate as f64 / 60.0)
            .min(self.rate as f64);
        self.last = now;
    }
}
struct Gate {
    generation: u64,
    capacity: usize,
    in_flight: usize,
    used: Instant,
    bucket: Option<Bucket>,
    _charge: MetadataCharge,
}
impl Gate {
    fn update(&mut self, scope: &Scope, now: Instant) {
        if let Some(bucket) = &mut self.bucket {
            bucket.refill(now);
        }
        if scope.generation > self.generation {
            self.generation = scope.generation;
            self.capacity = scope.capacity;
            match (self.bucket.as_mut(), scope.rpm) {
                (Some(bucket), Some(rate)) => {
                    bucket.rate = rate;
                    bucket.tokens = bucket.tokens.min(rate as f64);
                }
                (_, None) => self.bucket = None,
                (None, Some(rate)) => {
                    self.bucket = Some(Bucket {
                        tokens: rate as f64,
                        last: now,
                        rate,
                    })
                }
            }
        }
        self.used = now;
    }
}
struct Waiter {
    id: u64,
    keys: Vec<String>,
    _charge: MetadataCharge,
}
#[derive(Default)]
struct State {
    gates: HashMap<String, Gate>,
    waiters: VecDeque<Waiter>,
    next_waiter: u64,
}
fn state() -> &'static Mutex<State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(State::default()))
}
fn changed() -> &'static tokio::sync::Notify {
    static CHANGED: OnceLock<tokio::sync::Notify> = OnceLock::new();
    CHANGED.get_or_init(tokio::sync::Notify::new)
}
impl State {
    fn register(&mut self, scopes: &[Scope], now: Instant) -> Result<(), QueryError> {
        let needed = scopes
            .iter()
            .filter(|scope| !self.gates.contains_key(&scope.key))
            .count();
        if needed > 0 && self.gates.len() + needed > MAX_SCOPES {
            let queued: HashSet<&str> = self
                .waiters
                .iter()
                .flat_map(|w| w.keys.iter().map(String::as_str))
                .chain(scopes.iter().map(|scope| scope.key.as_str()))
                .collect();
            self.gates.retain(|key, gate| {
                gate.in_flight > 0
                    || queued.contains(key.as_str())
                    || gate.used.elapsed() < Duration::from_secs(60)
            });
            if self.gates.len() + needed > MAX_SCOPES {
                return Err(error("ER_AI_QUEUE_FULL", "too many active AI quota scopes"));
            }
        }
        for scope in scopes {
            if scope.capacity == 0 || scope.rpm == Some(0) {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "quota limits must be positive",
                ));
            }
            if let Some(gate) = self.gates.get_mut(&scope.key) {
                gate.update(scope, now);
            } else {
                let charge = metadata_charge(
                    (scope.key.capacity() + std::mem::size_of::<Gate>() + 128).saturating_mul(4),
                )?;
                self.gates.insert(
                    scope.key.clone(),
                    Gate {
                        generation: scope.generation,
                        capacity: scope.capacity,
                        in_flight: 0,
                        used: now,
                        bucket: scope.rpm.map(|rate| Bucket {
                            tokens: rate as f64,
                            last: now,
                            rate,
                        }),
                        _charge: charge,
                    },
                );
            }
        }
        Ok(())
    }
    fn ready(gates: &mut HashMap<String, Gate>, keys: &[String], now: Instant) -> bool {
        keys.iter().all(|key| {
            gates.get_mut(key).is_some_and(|gate| {
                if let Some(bucket) = &mut gate.bucket {
                    bucket.refill(now);
                }
                gate.in_flight < gate.capacity
                    && gate
                        .bucket
                        .as_ref()
                        .is_none_or(|bucket| bucket.tokens >= 1.0)
            })
        })
    }
    fn remove_waiter(&mut self, id: u64) -> bool {
        if let Some(index) = self.waiters.iter().position(|w| w.id == id) {
            self.waiters.remove(index);
            true
        } else {
            false
        }
    }
    fn enqueue(&mut self, keys: Vec<String>) -> Result<u64, QueryError> {
        if self.waiters.len() >= MAX_WAITERS {
            return Err(error(
                "ER_AI_QUEUE_FULL",
                "AI admission waiter limit reached",
            ));
        }
        let id = self.next_waiter.checked_add(1).ok_or_else(|| {
            error(
                "ER_AI_BUDGET_EXCEEDED",
                "AI admission ticket space exhausted",
            )
        })?;
        let charge = metadata_charge(
            (keys.iter().map(String::capacity).sum::<usize>() + 256).saturating_mul(4),
        )?;
        self.next_waiter = id;
        self.waiters.push_back(Waiter {
            id,
            keys,
            _charge: charge,
        });
        Ok(id)
    }
    fn attempt(&mut self, scopes: &[Scope], ticket: Option<u64>) -> Result<Decision, QueryError> {
        let now = Instant::now();
        self.register(scopes, now)?;
        let keys: Vec<_> = scopes.iter().map(|scope| scope.key.clone()).collect();
        let own_ready = Self::ready(&mut self.gates, &keys, now);
        if own_ready {
            // Prefer an older runnable contender, without blocking unrelated scopes.
            let earlier = self
                .waiters
                .iter()
                .take_while(|w| Some(w.id) != ticket)
                .any(|w| {
                    w.keys.iter().any(|key| keys.contains(key))
                        && Self::ready(&mut self.gates, &w.keys, now)
                });
            if !earlier {
                let mut rates = Vec::new();
                for key in &keys {
                    let gate = self.gates.get_mut(key).unwrap();
                    gate.in_flight += 1;
                    if let Some(bucket) = &mut gate.bucket {
                        bucket.tokens -= 1.0;
                        rates.push((key.clone(), gate.generation));
                    }
                }
                if let Some(id) = ticket {
                    self.remove_waiter(id);
                }
                return Ok(Decision::Admitted(Grant { keys, rates }));
            }
        }
        let mut delay = None;
        for key in &keys {
            let gate = &self.gates[key];
            if let Some(bucket) = &gate.bucket {
                if bucket.tokens < 1.0 {
                    let wait =
                        Duration::from_secs_f64((1.0 - bucket.tokens) * 60.0 / bucket.rate as f64)
                            .max(Duration::from_nanos(1));
                    delay = Some(delay.map_or(wait, |old: Duration| old.max(wait)));
                }
            }
        }
        Ok(Decision::Wait(delay, keys))
    }
}
struct Grant {
    keys: Vec<String>,
    rates: Vec<(String, u64)>,
}
enum Decision {
    Admitted(Grant),
    Wait(Option<Duration>, Vec<String>),
}
struct Waiting(u64);
impl Drop for Waiting {
    fn drop(&mut self) {
        let removed = state()
            .lock()
            .map(|mut state| state.remove_waiter(self.0))
            .unwrap_or(false);
        if removed {
            changed().notify_waiters();
        }
    }
}
pub(crate) struct Permit {
    grant: Grant,
    dispatched: bool,
}
impl Permit {
    pub(crate) fn mark_dispatched(&mut self) {
        self.dispatched = true;
    }
}
impl State {
    fn release(&mut self, grant: &Grant, dispatched: bool) {
        for key in &grant.keys {
            if let Some(gate) = self.gates.get_mut(key) {
                gate.in_flight = gate.in_flight.saturating_sub(1);
            }
        }
        if !dispatched {
            let now = Instant::now();
            for (key, generation) in &grant.rates {
                if let Some(gate) = self
                    .gates
                    .get_mut(key)
                    .filter(|gate| gate.generation == *generation)
                {
                    if let Some(bucket) = &mut gate.bucket {
                        bucket.refill(now);
                        bucket.tokens = (bucket.tokens + 1.0).min(bucket.rate as f64);
                    }
                }
            }
        }
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        if let Ok(mut state) = state().lock() {
            state.release(&self.grant, self.dispatched);
        }
        changed().notify_waiters();
    }
}

pub(crate) async fn acquire(
    current: impl Fn() -> Result<Vec<Scope>, QueryError>,
) -> Result<Permit, QueryError> {
    let mut waiting: Option<Waiting> = None;
    loop {
        let notification = changed().notified();
        tokio::pin!(notification);
        notification.as_mut().enable();
        let scopes = current()?;
        let decision = {
            let mut state = state()
                .lock()
                .map_err(|_| error("ER_AI_DISABLED", "admission state poisoned"))?;
            let decision = state.attempt(&scopes, waiting.as_ref().map(|w| w.0))?;
            if let Decision::Wait(_, keys) = &decision {
                if waiting.is_none() {
                    waiting = Some(Waiting(state.enqueue(keys.clone())?));
                }
            }
            decision
        };
        match decision {
            Decision::Admitted(grant) => {
                changed().notify_waiters();
                return Ok(Permit {
                    grant,
                    dispatched: false,
                });
            }
            Decision::Wait(Some(delay), _) => {
                tokio::select! {_=notification=>{},_=tokio::time::sleep(delay)=>{}}
            }
            Decision::Wait(None, _) => notification.await,
        }
    }
}

pub(crate) fn retire_namespace(namespace: &str) {
    let prefix = format!("{namespace}:");
    if let Ok(mut state) = state().lock() {
        state
            .gates
            .retain(|key, gate| !key.starts_with(&prefix) || gate.in_flight > 0);
        state
            .waiters
            .retain(|waiter| !waiter.keys.iter().any(|key| key.starts_with(&prefix)));
    }
    if let Ok(mut entries) = breakers().lock() {
        entries.retain(|key, _| !key.starts_with(&prefix));
    }
    changed().notify_waiters();
}

static MEMORY: AtomicUsize = AtomicUsize::new(0);
pub(crate) struct Memory {
    bytes: usize,
    response: Mutex<Option<Arc<Memory>>>,
    _charge: crate::engine::executor::mem_budget::ScopedCharge,
}
impl Memory {
    pub(crate) fn reserve_response(&self, bytes: usize) -> Result<(), QueryError> {
        let memory = Self::reserve_with_charge(bytes, self._charge.sibling())?;
        *self
            .response
            .lock()
            .map_err(|_| error("ER_AI_DISABLED", "AI memory state poisoned"))? = Some(memory);
        Ok(())
    }

    pub(crate) fn grow(&mut self, bytes: usize) -> Result<(), QueryError> {
        let cap = env_limit("MYDB_AI_MEMORY_LIMIT_MB", 128).saturating_mul(1024 * 1024);
        MEMORY
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|v| *v <= cap)
            })
            .map_err(|_| error("ER_AI_BUDGET_EXCEEDED", "AI input memory budget exceeded"))?;
        if let Err(e) = self._charge.add_force(bytes) {
            MEMORY.fetch_sub(bytes, Ordering::AcqRel);
            return Err(e);
        }
        self.bytes += bytes;
        Ok(())
    }

    pub(crate) fn reserve(bytes: usize) -> Result<Arc<Self>, QueryError> {
        Self::reserve_with_charge(
            bytes,
            crate::engine::executor::mem_budget::ScopedCharge::new(0),
        )
    }

    fn reserve_with_charge(
        bytes: usize,
        mut charge: crate::engine::executor::mem_budget::ScopedCharge,
    ) -> Result<Arc<Self>, QueryError> {
        let cap = env_limit("MYDB_AI_MEMORY_LIMIT_MB", 128).saturating_mul(1024 * 1024);
        MEMORY
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|next| *next <= cap)
            })
            .map_err(|_| {
                error(
                    "ER_AI_BUDGET_EXCEEDED",
                    "instance AI memory budget exceeded",
                )
            })?;
        match charge.add_force(bytes) {
            Ok(()) => Ok(Arc::new(Self {
                bytes,
                response: Mutex::new(None),
                _charge: charge,
            })),
            Err(e) => {
                MEMORY.fetch_sub(bytes, Ordering::AcqRel);
                Err(e)
            }
        }
    }
}
impl Drop for Memory {
    fn drop(&mut self) {
        MEMORY.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
pub(crate) fn memory_used() -> usize {
    MEMORY.load(Ordering::Acquire)
}

#[derive(Default)]
struct Breaker {
    revision: Option<(u64, u64, u64)>,
    active: usize,
    used: Option<Instant>,
    _charge: Option<MetadataCharge>,
    failures: u32,
    until: Option<Instant>,
    half_open: bool,
    auth_failed: bool,
}
fn breakers() -> &'static Mutex<HashMap<String, Breaker>> {
    static B: OnceLock<Mutex<HashMap<String, Breaker>>> = OnceLock::new();
    B.get_or_init(|| Mutex::new(HashMap::new()))
}
pub(crate) struct BreakerPermit {
    key: String,
    revision: (u64, u64, u64),
    half_open: bool,
}
struct BreakerEntry {
    key: String,
    half_open: bool,
}
fn enter_breaker(
    all: &mut HashMap<String, Breaker>,
    scope: &str,
    revision: (u64, u64, u64),
) -> Result<BreakerEntry, QueryError> {
    let key = scope.to_owned();
    if all.len() >= 1024 && !all.contains_key(&key) {
        all.retain(|_, b| {
            b.active > 0
                || b.auth_failed
                || b.half_open
                || b.until.is_some()
                || b.used
                    .is_some_and(|used| used.elapsed() < Duration::from_secs(60))
        });
        if all.len() >= 1024 {
            return Err(error(
                "ER_AI_QUEUE_FULL",
                "too many active circuit breakers",
            ));
        }
    }
    if !all.contains_key(&key) {
        let charge = metadata_charge(
            (key.capacity() + std::mem::size_of::<Breaker>() + 128).saturating_mul(4),
        )?;
        all.insert(
            key.clone(),
            Breaker {
                _charge: Some(charge),
                ..Default::default()
            },
        );
    }
    let b = all.get_mut(&key).unwrap();
    if b.revision.is_some_and(|current| revision < current) {
        return Err(error("ER_AI_DISABLED", "obsolete connection generation"));
    }
    if b.revision != Some(revision) {
        let active = b.active;
        let charge = b._charge.take();
        *b = Breaker {
            revision: Some(revision),
            active,
            _charge: charge,
            ..Default::default()
        };
    }
    b.used = Some(Instant::now());
    if b.auth_failed {
        return Err(error(
            "ER_AI_UPSTREAM_AUTH",
            "connection authentication failed; rotate credentials or configuration",
        ));
    }
    if let Some(until) = b.until {
        if Instant::now() < until || b.half_open {
            return Err(error(
                "ER_AI_CIRCUIT_OPEN",
                "connection circuit breaker is open",
            ));
        }
        b.half_open = true;
        b.active += 1;
        return Ok(BreakerEntry {
            key,
            half_open: true,
        });
    }
    b.active += 1;
    Ok(BreakerEntry {
        key,
        half_open: false,
    })
}
pub(crate) fn breaker(
    scope: String,
    revision: (u64, u64, u64),
) -> Result<BreakerPermit, QueryError> {
    let mut all = breakers()
        .lock()
        .map_err(|_| error("ER_AI_DISABLED", "breaker state poisoned"))?;
    let entered = enter_breaker(&mut all, &scope, revision)?;
    Ok(BreakerPermit {
        key: entered.key,
        revision,
        half_open: entered.half_open,
    })
}

impl BreakerPermit {
    pub(crate) fn check(&self) -> Result<(), QueryError> {
        let entries = breakers()
            .lock()
            .map_err(|_| error("ER_AI_DISABLED", "breaker state poisoned"))?;
        let entry = entries
            .get(&self.key)
            .filter(|entry| entry.revision == Some(self.revision))
            .ok_or_else(|| error("ER_AI_DISABLED", "connection health generation changed"))?;
        if entry.auth_failed {
            return Err(error(
                "ER_AI_UPSTREAM_AUTH",
                "connection authentication failed while request was waiting",
            ));
        }
        if let Some(until) = entry.until {
            if Instant::now() < until || !self.half_open || !entry.half_open {
                return Err(error(
                    "ER_AI_CIRCUIT_OPEN",
                    "connection circuit opened while request was waiting",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn record(&self, success: bool, transient: bool, auth_failed: bool) {
        if let Ok(mut all) = breakers().lock() {
            record_breaker(
                &mut all,
                &self.key,
                self.revision,
                self.half_open,
                success,
                transient,
                auth_failed,
            );
        }
    }
}
fn record_breaker(
    all: &mut HashMap<String, Breaker>,
    key: &str,
    revision: (u64, u64, u64),
    half_open: bool,
    success: bool,
    transient: bool,
    auth_failed: bool,
) {
    if let Some(b) = all.get_mut(key).filter(|b| b.revision == Some(revision)) {
        if success {
            b.failures = 0;
            b.until = None;
        } else if auth_failed {
            b.auth_failed = true;
        } else if transient {
            b.failures = b.failures.saturating_add(1);
            if b.failures >= 5 || half_open {
                b.until = Some(Instant::now() + Duration::from_secs(30));
            }
        }
    }
}

fn release_breaker(
    all: &mut HashMap<String, Breaker>,
    key: &str,
    revision: (u64, u64, u64),
    half_open: bool,
) {
    if let Some(entry) = all.get_mut(key) {
        entry.active = entry.active.saturating_sub(1);
        if entry.revision == Some(revision) && half_open {
            entry.half_open = false;
        }
        entry.used = Some(Instant::now());
    }
}
impl Drop for BreakerPermit {
    fn drop(&mut self) {
        if let Ok(mut all) = breakers().lock() {
            release_breaker(&mut all, &self.key, self.revision, self.half_open);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        task::{Context, Poll, Wake},
    };
    fn spec(key: &str, generation: u64, capacity: usize, rpm: Option<u64>) -> Scope {
        Scope {
            key: key.into(),
            generation,
            capacity,
            rpm,
        }
    }
    #[test]
    fn ai_existing_quota_scopes_remain_usable_when_metadata_registry_is_full() {
        let mut state = State::default();
        for i in 0..MAX_SCOPES {
            state
                .register(&[spec(&format!("scope-{i}"), 1, 1, None)], Instant::now())
                .unwrap();
        }
        assert!(matches!(
            state.attempt(&[spec("scope-0", 1, 1, None)], None).unwrap(),
            Decision::Admitted(_)
        ));
        let error = state
            .attempt(&[spec("overflow", 1, 1, None)], None)
            .err()
            .unwrap();
        assert!(error.is(crate::error::catalog::ER_AI_QUEUE_FULL));
        assert_eq!(state.gates.len(), MAX_SCOPES);
    }
    #[test]
    fn ai_old_quota_generations_cannot_restore_a_superseded_capacity() {
        let mut state = State::default();
        state
            .register(&[spec("model", 3, 2, None)], Instant::now())
            .unwrap();
        state
            .register(&[spec("model", 2, 16, None)], Instant::now())
            .unwrap();
        assert_eq!(state.gates["model"].capacity, 2);
        assert!(matches!(
            state.attempt(&[spec("model", 3, 2, None)], None).unwrap(),
            Decision::Admitted(_)
        ));
        state
            .register(&[spec("model", 4, 1, None)], Instant::now())
            .unwrap();
        assert!(matches!(
            state.attempt(&[spec("model", 2, 16, None)], None).unwrap(),
            Decision::Wait(_, _)
        ));
        state.gates.get_mut("model").unwrap().in_flight = 0;
        assert!(matches!(
            state.attempt(&[spec("model", 5, 4, None)], None).unwrap(),
            Decision::Admitted(_)
        ));
        assert_eq!(state.gates["model"].capacity, 4);
    }
    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn ai_admission_wakes_ready_followers_when_older_waiter_is_admitted() {
        let capacity = AtomicUsize::new(1);
        let current = || {
            Ok(vec![spec(
                "wake-test:model",
                capacity.load(Ordering::Acquire) as u64,
                capacity.load(Ordering::Acquire),
                None,
            )])
        };
        let holder = acquire(current).await.unwrap();
        let first = acquire(current);
        let second = acquire(current);
        tokio::pin!(first);
        tokio::pin!(second);
        let a = Arc::new(Wakes(AtomicUsize::new(0)));
        let b = Arc::new(Wakes(AtomicUsize::new(0)));
        let wa = std::task::Waker::from(a);
        let wb = std::task::Waker::from(b.clone());
        let mut ca = Context::from_waker(&wa);
        let mut cb = Context::from_waker(&wb);
        assert!(first.as_mut().poll(&mut ca).is_pending());
        assert!(second.as_mut().poll(&mut cb).is_pending());
        capacity.store(2, Ordering::Release);
        drop(holder);
        // Poll the younger waiter first so it must defer to the runnable older ticket.
        assert!(second.as_mut().poll(&mut cb).is_pending());
        b.0.store(0, Ordering::Release);
        let permit = match first.as_mut().poll(&mut ca) {
            Poll::Ready(Ok(p)) => p,
            _ => panic!("older runnable waiter did not acquire"),
        };
        assert!(
            b.0.load(Ordering::Acquire) > 0,
            "ready follower was not notified when queue precedence changed"
        );
        let follower = match second.as_mut().poll(&mut cb) {
            Poll::Ready(Ok(p)) => p,
            _ => panic!("free second slot was not usable"),
        };
        drop(permit);
        drop(follower);
    }
    #[test]
    fn ai_authentication_generation_changes_cannot_exhaust_breaker_recovery_capacity() {
        let mut entries = HashMap::new();
        for generation in 1..=1025 {
            let entered = enter_breaker(&mut entries, "one-connection", (generation, 1, 0));
            assert!(
                entered.is_ok(),
                "configuration recovery stopped at generation {generation}"
            );
            let entered = entered.unwrap();
            entries.get_mut(&entered.key).unwrap().auth_failed = true;
            release_breaker(
                &mut entries,
                &entered.key,
                (generation, 1, 0),
                entered.half_open,
            );
        }
    }
    #[test]
    fn ai_successful_configuration_revisions_cannot_exhaust_breaker_recovery_capacity() {
        let mut entries = HashMap::new();
        for generation in 1..=1025 {
            let entered = enter_breaker(&mut entries, "one-connection", (generation, 1, 0));
            assert!(
                entered.is_ok(),
                "healthy configuration revisions exhausted cache at generation {generation}"
            );
            let entered = entered.unwrap();
            release_breaker(
                &mut entries,
                &entered.key,
                (generation, 1, 0),
                entered.half_open,
            );
            assert_eq!(entries.len(), 1);
        }
    }
    #[test]
    fn ai_stale_breaker_completions_cannot_poison_new_connection_revision() {
        let mut entries = HashMap::new();
        let old = enter_breaker(&mut entries, "connection", (1, 1, 1)).unwrap();
        let new = enter_breaker(&mut entries, "connection", (2, 1, 1)).unwrap();
        record_breaker(&mut entries, &old.key, (1, 1, 1), false, false, false, true);
        for _ in 0..5 {
            record_breaker(&mut entries, &old.key, (1, 1, 1), false, false, true, false);
        }
        assert!(!entries["connection"].auth_failed);
        assert_eq!(entries["connection"].failures, 0);
        let stale = enter_breaker(&mut entries, "connection", (1, 1, 1))
            .err()
            .unwrap();
        assert!(stale.is(crate::error::catalog::ER_AI_DISABLED));
        release_breaker(&mut entries, &old.key, (1, 1, 1), false);
        assert_eq!(entries["connection"].active, 1);
        release_breaker(&mut entries, &new.key, (2, 1, 1), false);
        assert_eq!(entries["connection"].active, 0);
    }
    #[test]
    fn ai_old_half_open_probe_cannot_release_new_revision_probe() {
        let mut entries = HashMap::new();
        let first = enter_breaker(&mut entries, "connection", (1, 1, 1)).unwrap();
        release_breaker(&mut entries, &first.key, (1, 1, 1), false);
        entries.get_mut("connection").unwrap().until =
            Some(Instant::now() - Duration::from_secs(1));
        let old_probe = enter_breaker(&mut entries, "connection", (1, 1, 1)).unwrap();
        assert!(old_probe.half_open);
        let updated = enter_breaker(&mut entries, "connection", (2, 1, 1)).unwrap();
        release_breaker(&mut entries, &updated.key, (2, 1, 1), false);
        entries.get_mut("connection").unwrap().until =
            Some(Instant::now() - Duration::from_secs(1));
        let new_probe = enter_breaker(&mut entries, "connection", (2, 1, 1)).unwrap();
        assert!(new_probe.half_open);
        release_breaker(&mut entries, &old_probe.key, (1, 1, 1), true);
        let error = enter_breaker(&mut entries, "connection", (2, 1, 1))
            .err()
            .unwrap();
        assert!(error.is(crate::error::catalog::ER_AI_CIRCUIT_OPEN));
        record_breaker(
            &mut entries,
            &new_probe.key,
            (2, 1, 1),
            true,
            true,
            false,
            false,
        );
        release_breaker(&mut entries, &new_probe.key, (2, 1, 1), true);
        assert!(enter_breaker(&mut entries, "connection", (2, 1, 1)).is_ok());
    }
    #[test]
    fn ai_pre_dispatch_cancellation_refunds_rate_reservation_without_refunding_sent_attempts() {
        let mut state = State::default();
        let scope = spec("rate", 1, 4, Some(1));
        let grant = match state.attempt(&[scope.clone()], None).unwrap() {
            Decision::Admitted(grant) => grant,
            _ => panic!("initial rate reservation failed"),
        };
        state.release(&grant, false);
        let grant = match state.attempt(&[scope.clone()], None).unwrap() {
            Decision::Admitted(grant) => grant,
            _ => panic!("unused rate credit was not refunded"),
        };
        state.release(&grant, true);
        assert!(matches!(
            state.attempt(&[scope], None).unwrap(),
            Decision::Wait(_, _)
        ));
    }
}
