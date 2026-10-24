//! Process-wide bulk-work admission and explicit first-release backup budgets.
use super::*;
use std::sync::{Condvar, OnceLock};

#[derive(Debug, Serialize)]
pub(super) struct Limits {
    pub workers: usize,
    pub queue_capacity: usize,
    pub copy_threads: usize,
    pub open_files: usize,
    pub bandwidth_bytes_per_sec: u64,
    pub chunk_memory_bytes: usize,
    pub spool_limit_bytes: u64,
    pub freeze_budget_ms: u64,
    pub foreground_commit_target_us: u64,
}
fn number(name: &str, default: u64, min: u64, max: u64) -> Result<u64, String> {
    match crate::startup_config::var(name) {
        Ok(raw) => raw
            .parse::<u64>()
            .ok()
            .filter(|v| *v >= min && *v <= max)
            .ok_or_else(|| format!("{name} must be in {min}..={max}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(format!("{name} is not valid UTF-8")),
    }
}
impl Limits {
    fn load() -> Result<Self, String> {
        let cpus = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .div_ceil(2)
            .min(64);
        let open_files = number("MYDB_BACKUP_OPEN_FILES", 64, 16, 4096)? as usize;
        let copy_threads = (number("MYDB_BACKUP_COPY_THREADS", cpus as u64, 1, 64)? as usize)
            .min(open_files / 4)
            .max(1);
        Ok(Self {
            workers: number("MYDB_BACKUP_WORKERS", 2, 1, 8)? as usize,
            queue_capacity: number("MYDB_BACKUP_QUEUE_CAPACITY", 64, 1, 256)? as usize,
            copy_threads,
            open_files,
            bandwidth_bytes_per_sec: number("MYDB_BACKUP_BANDWIDTH_BYTES_PER_SEC", 0, 0, u64::MAX)?,
            chunk_memory_bytes: number(
                "MYDB_BACKUP_CHUNK_MEMORY_BYTES",
                128 << 20,
                8 << 20,
                1 << 30,
            )? as usize,
            spool_limit_bytes: number(
                "MYDB_BACKUP_SPOOL_LIMIT_BYTES",
                16 << 30,
                4 << 20,
                u64::MAX,
            )?,
            freeze_budget_ms: number("MYDB_BACKUP_FREEZE_BUDGET_MS", 1000, 1, 30000)?,
            foreground_commit_target_us: number(
                "MYDB_BACKUP_FOREGROUND_COMMIT_TARGET_US",
                0,
                0,
                60_000_000,
            )?,
        })
    }
}
pub(super) fn limits() -> Result<&'static Limits, QueryError> {
    static LIMITS: OnceLock<Result<Limits, String>> = OnceLock::new();
    LIMITS
        .get_or_init(Limits::load)
        .as_ref()
        .map_err(|s| backup_err(s.clone()))
}
struct Slots {
    used: Mutex<usize>,
    ready: Condvar,
}
fn slots() -> &'static Slots {
    static S: OnceLock<Slots> = OnceLock::new();
    S.get_or_init(|| Slots {
        used: Mutex::new(0),
        ready: Condvar::new(),
    })
}
#[derive(Clone, Default)]
pub(super) struct Context {
    pub cancel: Option<Arc<AtomicBool>>,
    pub control: bool,
}
thread_local! {static CONTEXT:std::cell::RefCell<Context>=const{std::cell::RefCell::new(Context{cancel:None,control:false})};}
pub(super) struct ContextScope(Context);
impl ContextScope {
    pub fn enter(context: Context) -> Self {
        Self(CONTEXT.with(|slot| slot.replace(context)))
    }
    pub fn worker(cancel: Arc<AtomicBool>) -> Self {
        Self::enter(Context {
            cancel: Some(cancel),
            control: false,
        })
    }
    pub fn control() -> Self {
        Self::enter(Context {
            cancel: None,
            control: true,
        })
    }
}
impl Drop for ContextScope {
    fn drop(&mut self) {
        CONTEXT.with(|slot| {
            slot.replace(std::mem::take(&mut self.0));
        });
    }
}
pub(super) fn context() -> Context {
    CONTEXT.with(|slot| slot.borrow().clone())
}
fn cancelled() -> bool {
    CONTEXT.with(|slot| {
        slot.borrow()
            .cancel
            .as_ref()
            .is_some_and(|cancel| cancel.load(Ordering::Acquire))
    })
}
fn control_slots() -> &'static Slots {
    static S: OnceLock<Slots> = OnceLock::new();
    S.get_or_init(|| Slots {
        used: Mutex::new(0),
        ready: Condvar::new(),
    })
}
pub(super) struct BulkPermit {
    control: bool,
}
impl BulkPermit {
    pub fn acquire(wait: bool) -> Result<Option<Self>, QueryError> {
        let control = context().control;
        let limit = if control { 1 } else { limits()?.copy_threads };
        let slots = if control { control_slots() } else { slots() };
        let mut used = slots.used.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if cancelled() {
                return Err(backup_err(
                    "backup cancelled while waiting for bulk resources",
                ));
            }
            if *used < limit {
                *used += 1;
                return Ok(Some(Self { control }));
            }
            if !wait {
                return Ok(None);
            }
            used = slots
                .ready
                .wait_timeout(used, Duration::from_millis(50))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
}
impl Drop for BulkPermit {
    fn drop(&mut self) {
        let slots = if self.control {
            control_slots()
        } else {
            slots()
        };
        let mut used = slots.used.lock().unwrap_or_else(|e| e.into_inner());
        *used -= 1;
        slots.ready.notify_one();
    }
}

/// Rate control applies to bulk bytes only. Writer/rollback/control paths never
/// acquire these slots or sleep on this limiter.
pub(super) fn throttle(bytes: u64) -> Result<(), QueryError> {
    if cancelled() {
        return Err(backup_err("backup cancelled while waiting for bandwidth"));
    }
    let rate = limits()?.bandwidth_bytes_per_sec;
    if rate == 0 || bytes == 0 || context().control {
        return Ok(());
    }
    struct Bucket {
        tokens: u128,
        last: Instant,
    }
    static BUCKET: OnceLock<Mutex<Bucket>> = OnceLock::new();
    let capacity = u128::from((rate / 20).max(1).min(CHUNK_BYTES as u64)) * 1_000_000_000;
    let bucket = BUCKET.get_or_init(|| {
        Mutex::new(Bucket {
            tokens: capacity,
            last: Instant::now(),
        })
    });
    let mut left = u128::from(bytes) * 1_000_000_000;
    let mut reserved = 0;
    while left > 0 {
        if cancelled() {
            let mut state = bucket.lock().unwrap_or_else(|e| e.into_inner());
            state.tokens = state.tokens.saturating_add(reserved).min(capacity);
            return Err(backup_err("backup cancelled while waiting for bandwidth"));
        }
        let delay = {
            let mut state = bucket.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            state.tokens = state
                .tokens
                .saturating_add(
                    now.duration_since(state.last)
                        .as_nanos()
                        .saturating_mul(u128::from(rate)),
                )
                .min(capacity);
            state.last = now;
            let take = left.min(state.tokens);
            state.tokens -= take;
            left -= take;
            reserved += take;
            if left == 0 {
                Duration::ZERO
            } else {
                Duration::from_nanos(
                    ((left.min(capacity) + u128::from(rate) - 1) / u128::from(rate)).min(50_000_000)
                        as u64,
                )
            }
        };
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
    }
    Ok(())
}
pub(super) fn check_spool(bytes: u64) -> Result<(), QueryError> {
    if bytes > limits()?.spool_limit_bytes {
        return Err(backup_err("backup operation spool budget exceeded"));
    }
    Ok(())
}

fn task_slots() -> &'static Slots {
    static S: OnceLock<Slots> = OnceLock::new();
    S.get_or_init(|| Slots {
        used: Mutex::new(0),
        ready: Condvar::new(),
    })
}
pub(super) struct TaskPermit;
impl TaskPermit {
    pub fn acquire() -> Result<Self, QueryError> {
        let limit = limits()?.workers;
        let slots = task_slots();
        let mut used = slots.used.lock().unwrap_or_else(|e| e.into_inner());
        while *used >= limit {
            if cancelled() {
                return Err(backup_err("backup cancelled before task admission"));
            }
            used = slots
                .ready
                .wait_timeout(used, Duration::from_millis(50))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        if cancelled() {
            return Err(backup_err("backup cancelled before task admission"));
        }
        *used += 1;
        Ok(Self)
    }
}
impl Drop for TaskPermit {
    fn drop(&mut self) {
        let slots = task_slots();
        let mut used = slots.used.lock().unwrap_or_else(|e| e.into_inner());
        *used -= 1;
        slots.ready.notify_one();
    }
}

pub(super) fn seal_grants(maximum: usize) -> Result<Vec<BulkPermit>, QueryError> {
    let mut grants = vec![BulkPermit::acquire(true)?.unwrap()];
    while grants.len() < limits()?.copy_threads.min(maximum.max(1)) {
        let Some(grant) = BulkPermit::acquire(false)? else {
            break;
        };
        grants.push(grant);
    }
    Ok(grants)
}

pub(super) fn copy_file(source: &Path, destination: &Path, length: u64) -> Result<(), QueryError> {
    if limits()?.bandwidth_bytes_per_sec == 0 {
        fs::copy(source, destination).map_err(|e| io_err("copy image file", e))?;
        return Ok(());
    }
    let _memory =
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, CHUNK_BYTES)?;
    let mut input = std::fs::File::open(source).map_err(|e| io_err("open throttled copy", e))?;
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .map_err(|e| io_err("create throttled copy", e))?;
    let mut buffer = vec![0; CHUNK_BYTES];
    let mut left = length;
    while left > 0 {
        let take = left.min(CHUNK_BYTES as u64) as usize;
        throttle(take as u64)?;
        input
            .read_exact(&mut buffer[..take])
            .map_err(|e| io_err("read throttled copy", e))?;
        output
            .write_all(&buffer[..take])
            .map_err(|e| io_err("write throttled copy", e))?;
        left -= take as u64;
    }
    Ok(())
}

pub(super) fn feedback_delay(target: u64, ewma: u64, age_ms: u64) -> Duration {
    if target == 0 || age_ms > 1000 {
        Duration::ZERO
    } else {
        Duration::from_micros(ewma.saturating_sub(target).min(10_000))
    }
}
pub(super) fn feedback(state: &crate::storage::capture::CaptureState) -> Result<(), QueryError> {
    let pressure = &state.commit_pressure;
    let delay = feedback_delay(
        limits()?.foreground_commit_target_us,
        pressure.ewma_us.load(Ordering::Relaxed),
        now_ms().saturating_sub(pressure.last_unix_ms.load(Ordering::Relaxed)),
    );
    if !delay.is_zero() {
        std::thread::sleep(delay);
    }
    Ok(())
}

#[derive(Default)]
pub(super) struct IoCounters {
    pub image_read_bytes: std::sync::atomic::AtomicU64,
    pub image_read_calls: std::sync::atomic::AtomicU64,
    pub image_write_bytes: std::sync::atomic::AtomicU64,
    pub catalog_read_bytes: std::sync::atomic::AtomicU64,
    pub checkpoint_read_bytes: std::sync::atomic::AtomicU64,
    pub lineage_read_bytes: std::sync::atomic::AtomicU64,
    pub blob_read_bytes: std::sync::atomic::AtomicU64,
    pub blob_decodes: std::sync::atomic::AtomicU64,
    pub repository_frame_decodes: std::sync::atomic::AtomicU64,
    pub spool_write_calls: std::sync::atomic::AtomicU64,
    pub spool_read_calls: std::sync::atomic::AtomicU64,
    pub spool_frame_decodes: std::sync::atomic::AtomicU64,
    pub helper_sync_calls: std::sync::atomic::AtomicU64,
}
pub(super) fn io() -> &'static IoCounters {
    static IO: OnceLock<IoCounters> = OnceLock::new();
    IO.get_or_init(IoCounters::default)
}
pub(super) fn io_snapshot() -> serde_json::Value {
    let io = io();
    serde_json::json!({"process_id":std::process::id(),"image_read_bytes":io.image_read_bytes.load(Ordering::Relaxed),"image_read_calls":io.image_read_calls.load(Ordering::Relaxed),"image_write_bytes":io.image_write_bytes.load(Ordering::Relaxed),"catalog_read_bytes":io.catalog_read_bytes.load(Ordering::Relaxed),"checkpoint_read_bytes":io.checkpoint_read_bytes.load(Ordering::Relaxed),"lineage_read_bytes":io.lineage_read_bytes.load(Ordering::Relaxed),"blob_read_bytes":io.blob_read_bytes.load(Ordering::Relaxed),"blob_decodes":io.blob_decodes.load(Ordering::Relaxed),"repository_frame_decodes":io.repository_frame_decodes.load(Ordering::Relaxed),"spool_frame_decodes":io.spool_frame_decodes.load(Ordering::Relaxed),"spool_write_calls":io.spool_write_calls.load(Ordering::Relaxed),"spool_read_calls":io.spool_read_calls.load(Ordering::Relaxed),"helper_sync_calls":io.helper_sync_calls.load(Ordering::Relaxed),"scope":"Process-local backup/image helper counters; engine recovery/checkpoint I/O and child-process counters are separate."})
}
