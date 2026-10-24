//! A source owns policy, durable receipts and all worker lifetimes.
use super::repository::{Base, Lease, Position, Publication, Range, Repository};
use super::*;
use crate::storage::capture::WalLease;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Policy {
    Disabled,
    Manual,
    Continuous,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Pending {
    pub base_id: String,
    pub stream_id: String,
    pub end: u64,
    pub time: u64,
    pub hint: Option<(u64, u64)>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Control {
    pub version: u32,
    pub repository: PathBuf,
    pub repository_id: String,
    pub source_id: String,
    pub identity_id: String,
    pub timeline_id: String,
    pub requested: Policy,
    pub effective: Policy,
    pub lifecycle: String,
    pub stream_id: Option<String>,
    pub cursor: u64,
    pub covered_time: u64,
    pub position: Position,
    pub latest_base: Option<String>,
    #[serde(default)]
    pub last_range: Option<(String, String)>,
    #[serde(default)]
    pub latest_base_lsn: u64,
    #[serde(default)]
    pub latest_base_time: u64,
    #[serde(default)]
    pub next_retry_unix_ms: u64,
    #[serde(default)]
    pub drain: Option<(u64, u64)>,
    #[serde(default)]
    pub close_publication_pending: bool,
    #[serde(default)]
    pub close_discarded: bool,
    #[serde(default)]
    pub fence_time: Option<u64>,
    pub pending: Option<Pending>,
    pub max_pending_wal_bytes: u64,
    pub hint: Option<(u64, u64)>,
    #[serde(default)]
    pub consecutive_errors: u64,
    #[serde(default)]
    pub last_error_unix_ms: u64,
    pub error: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Job {
    pub id: String,
    pub kind: String,
    pub state: String,
    pub phase: String,
    pub base_id: Option<String>,
    pub requested_lsn: u64,
    pub requested_source_time: u64,
    pub durable_through_lsn: u64,
    pub covered_time: u64,
    pub catalog_generation: u64,
    #[serde(default)]
    pub repository_id: Option<String>,
    pub bytes: u64,
    #[serde(default)]
    pub files_done: u64,
    /// Remaining bytes in the currently fixed copy phase; unknown in other phases.
    #[serde(default)]
    pub phase_remaining_bytes: Option<u64>,
    #[serde(default)]
    pub sequence: u64,
    #[serde(default)]
    pub created_unix_ms: u64,
    #[serde(default)]
    pub started_unix_ms: u64,
    #[serde(default)]
    pub completed_unix_ms: u64,
    #[serde(default)]
    pub phase_times_us: BTreeMap<String, u64>,
    #[serde(default)]
    pub failure_class: String,
    pub error: String,
}
type QueuedWork = Box<dyn FnOnce() + Send + 'static>;

struct JobEntry {
    job: Job,
    cancel: Arc<AtomicBool>,
    phase_started: Instant,
}
pub(crate) struct Coordinator {
    db: Weak<Database>,
    root: PathBuf,
    pub control: Mutex<Option<Control>>,
    serial: Mutex<()>,
    archive: Mutex<()>,
    capturing: AtomicBool,
    closing: AtomicBool,
    control_directory_confirmed: AtomicBool,
    confirmed_head_verified: AtomicBool,
    shutdown_lock: Mutex<()>,
    jobs: Mutex<BTreeMap<String, JobEntry>>,
    workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
    queue: Mutex<Option<mpsc::SyncSender<QueuedWork>>>,
    background: Mutex<Option<std::thread::JoinHandle<()>>>,
    stop: AtomicBool,
}
#[derive(Serialize, Deserialize)]
struct StateEnvelope {
    version: u32,
    payload: Vec<u8>,
    digest: String,
}
fn state_path(root: &Path) -> PathBuf {
    root.join("system/backup-control/state")
}
pub(crate) fn read_control(root: &Path) -> Result<Option<Control>, QueryError> {
    let path = state_path(root);
    reject_symlink_path(root, Path::new("system/backup-control/state"))?;
    if !path.exists() {
        if root.join("system/backup-control/identity").exists() {
            return Err(backup_err("backup control state is missing"));
        }
        return Ok(None);
    }
    let envelope: StateEnvelope = serde_json::from_slice(&read_bounded(&path, 64 * 1024)?)
        .map_err(|e| backup_err(format!("invalid source backup control: {e}")))?;
    if envelope.version != 1 || sha256_hex(&envelope.payload) != envelope.digest {
        return Err(backup_err(
            "source backup control checksum/version mismatch",
        ));
    }
    let state: Control =
        serde_json::from_slice(&envelope.payload).map_err(|e| backup_err(e.to_string()))?;
    if ![
        "UNCONFIGURED",
        "WAITING_BASE",
        "PREPARING_BASE",
        "ACTIVE",
        "DRAINING",
        "CLOSED",
        "BROKEN",
    ]
    .contains(&state.lifecycle.as_str())
        || !state.repository.is_absolute()
    {
        return Err(backup_err(
            "unknown or invalid source backup lifecycle/path",
        ));
    }
    for id in [
        &state.repository_id,
        &state.source_id,
        &state.timeline_id,
        &state.identity_id,
    ] {
        validate_id(id)?;
    }
    if let Some(id) = &state.stream_id {
        validate_id(id)?;
    }
    if state.version != 1 || state.max_pending_wal_bytes == 0 {
        return Err(backup_err("invalid backup policy state"));
    }
    let id = String::from_utf8(read_bounded(
        &root.join("system/backup-control/identity"),
        128,
    )?)
    .map_err(|e| backup_err(e.to_string()))?;
    if id != state.identity_id {
        return Err(backup_err("source backup identity mismatch"));
    }
    Ok(Some(state))
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct ArchiveIdentity {
    repository_id: String,
    source_id: String,
    timeline_id: String,
    stream_id: Option<String>,
}
impl From<&Control> for ArchiveIdentity {
    fn from(state: &Control) -> Self {
        Self {
            repository_id: state.repository_id.clone(),
            source_id: state.source_id.clone(),
            timeline_id: state.timeline_id.clone(),
            stream_id: state.stream_id.clone(),
        }
    }
}
fn verify_receipt(repo: &Repository, state: &Control) -> Result<(), QueryError> {
    if repo.header.id != state.repository_id {
        return Err(backup_err("acknowledged repository identity changed"));
    }
    let stream = state
        .stream_id
        .as_ref()
        .ok_or_else(|| backup_err("acknowledged archive has no stream"))?;
    let descriptor = repo.stream(stream)?;
    if descriptor.source_id != state.source_id || descriptor.timeline_id != state.timeline_id {
        return Err(backup_err("acknowledged source/timeline changed"));
    }
    if state.latest_base.is_none() {
        return Err(backup_err("active stream has no confirmed baseline"));
    }
    if let Some(base) = &state.latest_base {
        let (base, _) = repo.base_for_open(base)?;
        if base.source_id != state.source_id
            || base.timeline_id != state.timeline_id
            || base.stream_id != state.stream_id
        {
            return Err(backup_err("acknowledged baseline changed source/stream"));
        }
    }
    let Some((id, digest)) = state.last_range.as_ref() else {
        return Ok(());
    };
    let stream = state
        .stream_id
        .as_ref()
        .ok_or_else(|| backup_err("acknowledged WAL has no stream"))?;
    validate_id(id)?;
    let path = repo
        .root
        .join("streams")
        .join(stream)
        .join("ranges")
        .join(id)
        .join("manifest");
    reject_symlink_path(&repo.root, path.strip_prefix(&repo.root).unwrap())?;
    let bytes = read_bounded(&path, MAX_METADATA_BYTES)?;
    if sha256_hex(&bytes) != *digest {
        return Err(backup_err("acknowledged range identity changed"));
    }
    let range: Range = serde_json::from_slice(&bytes).map_err(|e| backup_err(e.to_string()))?;
    if range.last_lsn != state.cursor
        || Some(&range.stream_id) != state.stream_id.as_ref()
        || range.source_id != state.source_id
        || range.timeline_id != state.timeline_id
    {
        return Err(backup_err("acknowledged range position/branch changed"));
    }
    super::restore::verify_range(&repo, &range)
}
struct CapturedTarget {
    identity: ArchiveIdentity,
    lsn: u64,
    time: u64,
}
impl Coordinator {
    pub(crate) fn get(db: &Arc<Database>) -> Result<Arc<Self>, QueryError> {
        db.services.get_or_init(|| {
            db.capture.commit_pressure.enabled.store(
                super::resources::limits()?.foreground_commit_target_us > 0,
                Ordering::Relaxed,
            );
            DirectoryOperation::cleanup(
                &db.data_dir().join("system/backup-control"),
                "control-init",
            )?;
            let control = read_control(db.data_dir())?;
            if let Some(state) = &control {
                db.capture
                    .budget
                    .limit
                    .store(state.max_pending_wal_bytes, Ordering::Release);
            }
            let this = Arc::new(Self {
                db: Arc::downgrade(db),
                root: db.data_dir().to_path_buf(),
                control: Mutex::new(control),
                serial: Mutex::new(()),
                archive: Mutex::new(()),
                capturing: AtomicBool::new(false),
                closing: AtomicBool::new(false),
                control_directory_confirmed: AtomicBool::new(false),
                confirmed_head_verified: AtomicBool::new(false),
                shutdown_lock: Mutex::new(()),
                jobs: Mutex::new(BTreeMap::new()),
                workers: Mutex::new(Vec::new()),
                queue: Mutex::new(None),
                background: Mutex::new(None),
                stop: AtomicBool::new(false),
            });
            if let Some(state) = this
                .control
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_mut()
            {
                if state.effective != Policy::Disabled
                    && state.stream_id.is_some()
                    && load_or_create_source_id(db.data_dir())? != state.source_id
                {
                    state.lifecycle = "BROKEN".into();
                    state.error = "source timeline changed while protected".into();
                    this.save(state)?;
                }
            }
            this.load_jobs()?;
            for name in db
                .capture
                .leases()
                .keys()
                .filter(|name| name.starts_with("capture/"))
            {
                db.capture.set_lease(name, None)?;
            }
            if let Err(error) = this.reconcile() {
                this.record_error(&error);
            }
            if let Err(error) = this.verify_confirmed_head() {
                this.record_error(&error);
            }
            let draining = this
                .control
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .and_then(|s| s.drain);
            if let Some((lsn, time)) = draining {
                let mut target = this.capture_target()?;
                if target.lsn < lsn {
                    return Err(backup_err("durable stop target exceeds source WAL"));
                }
                target.lsn = lsn;
                target.time = time;
                this.spawn_drain(target)?;
            }
            if this
                .control
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .is_some_and(|s| s.effective == Policy::Continuous && s.lifecycle == "ACTIVE")
            {
                this.start_scheduler()?;
            }
            Ok(this)
        })
    }
    fn start_scheduler(self: &Arc<Self>) -> Result<(), QueryError> {
        #[cfg(test)]
        super::faults::hit("scheduler_before_start", &self.root)?;
        let mut owner = self.background.lock().unwrap_or_else(|e| e.into_inner());
        if self.closing.load(Ordering::Acquire) {
            return Ok(());
        }
        if owner.as_ref().is_some_and(|worker| !worker.is_finished()) {
            worker_unpark(owner.as_ref());
            return Ok(());
        }
        if let Some(worker) = owner.take() {
            let _ = worker.join();
        }
        self.stop.store(false, Ordering::Release);
        let weak = Arc::downgrade(self);
        *owner = Some(
            std::thread::Builder::new()
                .name("mydb-wal-archive".into())
                .spawn(move || loop {
                    std::thread::park_timeout(Duration::from_secs(1));
                    let Some(this) = weak.upgrade() else { break };
                    if this.stop.load(Ordering::Acquire) {
                        break;
                    }
                    let continuous = this
                        .control
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_ref()
                        .is_some_and(|s| {
                            s.effective == Policy::Continuous && s.lifecycle == "ACTIVE"
                        });
                    if !continuous {
                        break;
                    }
                    let pending = this
                        .jobs
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .values()
                        .any(|entry| entry.job.kind == "WAL" && entry.job.state == "RUNNING");
                    if pending {
                        continue;
                    }
                    let repository = this
                        .control
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_ref()
                        .unwrap()
                        .repository
                        .clone();
                    if let Err(error) = this.enqueue_wal(&repository, true) {
                        if !this.closing.load(Ordering::Acquire)
                            && this
                                .control
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .as_ref()
                                .is_some_and(|state| {
                                    state.effective == Policy::Continuous
                                        && state.lifecycle == "ACTIVE"
                                })
                        {
                            this.record_error(&error);
                        }
                    }
                })
                .map_err(|e| io_err("spawn WAL scheduler", e))?,
        );
        Ok(())
    }
    fn db(&self) -> Result<Arc<Database>, QueryError> {
        self.db
            .upgrade()
            .ok_or_else(|| backup_err("source database is closed"))
    }
    fn save(&self, state: &Control) -> Result<(), QueryError> {
        let payload = serde_json::to_vec(state).map_err(|e| backup_err(e.to_string()))?;
        let envelope = StateEnvelope {
            version: 1,
            digest: sha256_hex(&payload),
            payload,
        };
        let bytes = serde_json::to_vec(&envelope).map_err(|e| backup_err(e.to_string()))?;
        if bytes.len() > 64 * 1024 {
            return Err(backup_err("source control state exceeds budget"));
        }
        let directory = self.root.join("system/backup-control");
        if !directory.exists()
            || fs::read_dir(&directory)
                .map_err(|e| io_err("inspect source control", e))?
                .next()
                .is_none()
        {
            let operation = DirectoryOperation::new(&directory, "control-init")?;
            write_sync(
                &operation.data.join("identity"),
                state.identity_id.as_bytes(),
            )?;
            #[cfg(test)]
            super::faults::hit("control_identity_published", &self.root)?;
            write_sync(&operation.data.join("state"), &bytes)?;
            let result = operation.install();
            if result.is_ok() {
                self.control_directory_confirmed
                    .store(true, Ordering::Release);
            }
            return result;
        }
        let identity = read_bounded(&directory.join("identity"), 128)?;
        if identity != state.identity_id.as_bytes() {
            return Err(backup_err("source control identity changed"));
        }
        atomic_replace(&state_path(&self.root), &bytes)?;
        if !self.control_directory_confirmed.load(Ordering::Acquire) {
            sync_dir(directory.parent().unwrap())?;
            self.control_directory_confirmed
                .store(true, Ordering::Release);
        }
        Ok(())
    }
    fn record_error(&self, error: &QueryError) {
        if let Some(state) = self
            .control
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
        {
            state.error = error_detail(error).chars().take(2048).collect();
            state.consecutive_errors = state.consecutive_errors.saturating_add(1);
            state.last_error_unix_ms = now_ms();
            state.next_retry_unix_ms =
                if state.effective == Policy::Continuous && state.lifecycle == "ACTIVE" {
                    now_ms().saturating_add(1000)
                } else {
                    0
                };
            let _ = self.save(state);
        }
    }
    pub(crate) fn configure(
        self: &Arc<Self>,
        repository: &Path,
        policy: Policy,
        discard: bool,
        budget: Option<u64>,
    ) -> Result<Option<Job>, QueryError> {
        self.configure_patch(repository, Some(policy), discard, budget)
    }
    pub(crate) fn configure_patch(
        self: &Arc<Self>,
        repository: &Path,
        policy: Option<Policy>,
        discard: bool,
        budget: Option<u64>,
    ) -> Result<Option<Job>, QueryError> {
        let _control_context = super::resources::ContextScope::control();
        if self.closing.load(Ordering::Acquire) {
            return Err(backup_err("database is shutting down"));
        }
        if policy.is_none() && budget.is_none() {
            return Err(backup_err("empty backup configuration patch"));
        }
        if let Some(limit) = budget {
            if limit == 0 {
                return Err(backup_err("MAX_PENDING_WAL_BYTES must be positive"));
            }
            if !discard {
                let selected = resolved_destination(repository)?;
                let mut guard = self.control.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(current) = guard.as_ref().filter(|state| {
                    state.repository == selected
                        && (policy.is_none()
                            || (Some(state.requested) == policy && Some(state.effective) == policy))
                }) {
                    let db = self.db()?;
                    db.ensure_writable()?;
                    if db.physical_replica_mode() || db.durability_mode() == DurabilityMode::Memory
                    {
                        return Err(backup_err("WAL budget requires a persistent source"));
                    }
                    let mut next = current.clone();
                    next.max_pending_wal_bytes = limit;
                    self.save(&next)?;
                    db.capture.budget.limit.store(limit, Ordering::Release);
                    *guard = Some(next);
                    return Ok(None);
                }
            }
        }
        let _serial = self.serial.lock().unwrap_or_else(|e| e.into_inner());
        let policy = policy.unwrap_or_else(|| {
            self.control
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .map_or(Policy::Disabled, |state| state.effective)
        });
        let _archive = self.archive.lock().unwrap_or_else(|e| e.into_inner());
        if self.capturing.load(Ordering::Acquire) {
            return Err(backup_err(
                "policy change conflicts with an active full backup",
            ));
        }
        if discard && policy != Policy::Disabled {
            return Err(backup_err("DISCARD_PENDING applies only to DISABLED"));
        }
        let db = self.db()?;
        db.ensure_writable()?;
        if db.physical_replica_mode() || db.durability_mode() == DurabilityMode::Memory {
            return Err(backup_err(
                "WAL protection requires a persistent standalone/master source",
            ));
        }
        let repository = resolved_destination(repository)?;
        reject_overlap(db.data_dir(), &repository)?;
        let mut guard = self.control.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(state) = guard.as_ref().cloned() {
            if state.lifecycle == "DRAINING" && !(discard && policy == Policy::Disabled) {
                if policy == Policy::Disabled && state.repository == repository {
                    if let Some(limit) = budget {
                        let mut next = state.clone();
                        next.max_pending_wal_bytes = limit;
                        self.save(&next)?;
                        db.capture.budget.limit.store(limit, Ordering::Release);
                        *guard = Some(next);
                    }
                    if let Some(job) = self
                        .jobs
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .values()
                        .find(|e| e.job.kind == "STOP" && e.job.state == "RUNNING")
                        .map(|e| e.job.clone())
                    {
                        return Ok(Some(job));
                    }
                    let (lsn, time) = state
                        .drain
                        .ok_or_else(|| backup_err("missing durable drain target"))?;
                    drop(guard);
                    let mut target = self.capture_target()?;
                    target.lsn = lsn;
                    target.time = time;
                    return self.spawn_drain(target).map(Some);
                }
                return Err(backup_err("archive stream is draining"));
            }
            if state.repository != repository && state.effective != Policy::Disabled {
                return Err(backup_err("only one enabled WAL archive target is allowed"));
            }
        }
        if policy == Policy::Disabled
            && guard.as_ref().is_some_and(|s| {
                s.repository == repository
                    && s.stream_id.is_some()
                    && s.effective != Policy::Disabled
            })
        {
            let mut next = guard.as_ref().unwrap().clone();
            if let Some(bytes) = budget {
                if bytes == 0 {
                    return Err(backup_err("WAL budget must be positive"));
                }
                next.max_pending_wal_bytes = bytes;
            }
            if discard {
                next.requested = policy;
                next.effective = policy;
                next.lifecycle = "CLOSED".into();
                next.pending = None;
                next.drain = None;
                next.close_publication_pending = next.stream_id.is_some();
                next.close_discarded = true;
                self.save(&next)?;
                db.capture
                    .budget
                    .limit
                    .store(next.max_pending_wal_bytes, Ordering::Release);
                db.capture.set_lease("archive", None)?;
                *guard = Some(next);
                drop(guard);
                let _control = super::resources::ContextScope::control();
                if let Err(error) = self.reconcile() {
                    self.record_error(&error);
                }
                return Ok(None);
            }
            drop(guard);
            let target = self.capture_target()?;
            let mut guard = self.control.lock().unwrap_or_else(|e| e.into_inner());
            next = guard.as_ref().unwrap().clone();
            if let Some(bytes) = budget {
                next.max_pending_wal_bytes = bytes;
            }
            next.requested = policy;
            next.lifecycle = "DRAINING".into();
            next.drain = Some((target.lsn, target.time));
            self.save(&next)?;
            db.capture
                .budget
                .limit
                .store(next.max_pending_wal_bytes, Ordering::Release);
            *guard = Some(next);
            drop(guard);
            return self.spawn_drain(target).map(Some);
        }
        let mut repo = if repository.join("repository.json").exists() {
            Repository::open(&repository, None)?
        } else {
            Repository::init(&repository, None, None)?
        };
        if policy != Policy::Disabled {
            repo.require_key(None)?;
        }
        if guard.as_ref().is_some_and(|old| {
            old.effective != Policy::Disabled && old.repository_id != repo.header.id
        }) {
            return Err(backup_err("active repository UUID changed"));
        }
        let mut state = match guard.as_ref() {
            Some(old) if old.repository_id == repo.header.id => old.clone(),
            _ => Control {
                version: 1,
                repository: repo.root.clone(),
                repository_id: repo.header.id.clone(),
                source_id: load_or_create_source_id(db.data_dir())?,
                identity_id: if self.root.join("system/backup-control/identity").exists() {
                    String::from_utf8(read_bounded(
                        &self.root.join("system/backup-control/identity"),
                        128,
                    )?)
                    .map_err(|_| backup_err("invalid source control identity"))?
                } else {
                    load_or_create_source_id(db.data_dir())?
                },
                timeline_id: load_or_create_source_id(db.data_dir())?,
                requested: Policy::Disabled,
                effective: Policy::Disabled,
                lifecycle: "UNCONFIGURED".into(),
                stream_id: None,
                cursor: 0,
                covered_time: 0,
                position: Position::default(),
                latest_base: None,
                last_range: None,
                latest_base_lsn: 0,
                latest_base_time: 0,
                next_retry_unix_ms: 0,
                drain: None,
                close_publication_pending: false,
                close_discarded: false,
                fence_time: None,
                pending: None,
                max_pending_wal_bytes: 16 * 1024 * 1024 * 1024,
                hint: None,
                error: String::new(),
                consecutive_errors: 0,
                last_error_unix_ms: 0,
            },
        };
        let mut rearm_lease = None;
        let rearming = policy != Policy::Disabled
            && state.stream_id.is_some()
            && (state.lifecycle == "BROKEN" || !state.error.is_empty());
        if rearming {
            drop(guard);
            let _control = super::resources::ContextScope::control();
            if repo.position()? != state.position {
                self.reconcile()?;
                state = self
                    .control
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .ok_or_else(|| backup_err("source control disappeared"))?
                    .clone();
            }
            rearm_lease = Some(Lease::acquire(&repo.root, true)?);
            let current_source = load_or_create_source_id(db.data_dir())?;
            if state.source_id != current_source || state.timeline_id != current_source {
                return Err(backup_err("rearming cannot change source lineage"));
            }
            let position = repo.position()?;
            repo.visit_since(&position, &state.position, |_| Ok(()))?;
            repo.verify_coverage(&position)?;
            repo.verify_history()?;
            let mut closed = false;
            let mut base_present = state.latest_base.is_none();
            let mut head_present = state.last_range.is_none();
            repo.visit_live(&position, |_, event| {
                match event {
                    Publication::Base { base } => {
                        base_present |= state.latest_base.as_deref() == Some(base.id.as_str());
                        repo.base(&base.id)?;
                    }
                    Publication::Range { range } => {
                        head_present |= state
                            .last_range
                            .as_ref()
                            .is_some_and(|(id, _)| id == &range.id);
                        super::restore::verify_range(&repo, &range)?;
                    }
                    Publication::Close { stream_id, .. }
                        if Some(&stream_id) == state.stream_id.as_ref() =>
                    {
                        closed = true
                    }
                    _ => {}
                }
                Ok(())
            })?;
            if !base_present || !head_present {
                return Err(backup_err(
                    "acknowledged head is outside the retained catalog",
                ));
            }
            if closed {
                return Err(backup_err(
                    "a closed archive stream requires a new baseline/stream",
                ));
            }
            verify_receipt(&repo, &state)?;
            sync_tree(&repo.root)?;
            sync_dir(repo.root.parent().unwrap())?;
            guard = self.control.lock().unwrap_or_else(|e| e.into_inner());
            let latest = guard
                .as_ref()
                .ok_or_else(|| backup_err("source control disappeared while rearming"))?;
            if ArchiveIdentity::from(latest) != ArchiveIdentity::from(&state) {
                return Err(backup_err("archive identity changed while rearming"));
            }
            if budget.is_none() {
                state.max_pending_wal_bytes = latest.max_pending_wal_bytes;
            }
            state.lifecycle = "ACTIVE".into();
            state.drain = None;
            state.close_publication_pending = false;
        }
        if let Some(bytes) = budget {
            if bytes == 0 {
                return Err(backup_err("WAL budget must be positive"));
            }
            state.max_pending_wal_bytes = bytes;
        }
        if state.effective == Policy::Disabled && policy != Policy::Disabled {
            state.source_id = load_or_create_source_id(db.data_dir())?;
            state.timeline_id = state.source_id.clone();
            state.stream_id = None;
            state.fence_time = None;
            state.last_range = None;
            state.close_publication_pending = false;
            state.close_discarded = false;
            state.drain = None;
            state.lifecycle = "WAITING_BASE".into();
            state.cursor = 0;
            state.covered_time = 0;
            state.hint = None;
            state.position = repo.position()?;
        }
        if policy == Policy::Disabled {
            state.lifecycle = "CLOSED".into();
        }
        state.effective = policy;
        state.requested = policy;
        state.error.clear();
        state.consecutive_errors = 0;
        state.last_error_unix_ms = 0;
        state.next_retry_unix_ms = 0;
        self.save(&state)?;
        if rearming {
            self.confirmed_head_verified.store(true, Ordering::Release);
        }
        db.capture
            .budget
            .limit
            .store(state.max_pending_wal_bytes, Ordering::Release);
        let active = state.effective == Policy::Continuous && state.lifecycle == "ACTIVE";
        *guard = Some(state);
        drop(guard);
        drop(rearm_lease);
        if policy == Policy::Disabled {
            let _control = super::resources::ContextScope::control();
            let _ = self.reconcile();
        }
        if active {
            self.start_scheduler()?;
        } else {
            worker_unpark(
                self.background
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref(),
            );
        }
        Ok(None)
    }
    fn spawn_drain(self: &Arc<Self>, target: CapturedTarget) -> Result<Job, QueryError> {
        let requested = Some((target.lsn, target.time));
        self.spawn("STOP", None, requested, move |this, id, cancel| {
            let _archive = this.archive.lock().unwrap_or_else(|e| e.into_inner());
            let result = (|| {
                this.job_target(id, target.lsn, target.time);
                check_cancelled(cancel)?;
                let end = target.lsn;
                let time = target.time;
                this.archive_to(target, id, cancel)?;
                check_cancelled(cancel)?;
                this.phase(id, "PUBLISHING");
                let mut guard = this.control.lock().unwrap_or_else(|e| e.into_inner());
                let mut state = guard.as_ref().unwrap().clone();
                if state.lifecycle != "DRAINING" {
                    return Err(backup_err("stop intent was superseded"));
                }
                let repo = Repository::open(&state.repository, None)?;
                let _lease = Lease::acquire(&repo.root, false)?;
                state.close_publication_pending = true;
                this.save(&state)?;
                *guard = Some(state.clone());
                let close_position = repo.publish(Publication::Close {
                    stream_id: state.stream_id.clone().unwrap(),
                    lsn: end,
                    time,
                    discarded: false,
                })?;
                state.position = close_position;
                state.effective = Policy::Disabled;
                state.requested = Policy::Disabled;
                state.lifecycle = "CLOSED".into();
                state.pending = None;
                state.drain = None;
                state.close_publication_pending = false;
                this.save(&state)?;
                this.db()?.capture.set_lease("archive", None)?;
                this.receipt(id, &state)?;
                *guard = Some(state);
                Ok(())
            })();
            if let Err(error) = &result {
                // A visible Close is reconciled before deciding whether the
                // effective policy may be restored. On uncertainty keep pins.
                if this.reconcile().is_ok() {
                    let mut guard = this.control.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(state) = guard.as_mut() {
                        if state.lifecycle == "DRAINING" && !state.close_publication_pending {
                            state.lifecycle = "ACTIVE".into();
                            state.requested = state.effective;
                            state.drain = None;
                            state.error = error_detail(error).chars().take(2048).collect();
                            this.save(state)?;
                        }
                    }
                }
            }
            if result.is_err()
                && this
                    .control
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .is_some_and(|s| s.effective == Policy::Continuous && s.lifecycle == "ACTIVE")
            {
                this.start_scheduler()?;
            }
            result
        })
    }
    pub(crate) fn backup(self: &Arc<Self>, repository: &Path) -> Result<Job, QueryError> {
        if self
            .control
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
        {
            self.configure(repository, Policy::Disabled, false, None)?;
        }
        let _serial = self.serial.lock().unwrap_or_else(|e| e.into_inner());
        if self.closing.load(Ordering::Acquire) {
            return Err(backup_err("database is shutting down"));
        }
        let path = resolved_destination(repository)?;
        if self.capturing.swap(true, Ordering::AcqRel) {
            return Err(backup_err("another full backup is active"));
        }
        let result = self.spawn(
            "DATABASE",
            Some(random_uuid()),
            None,
            move |this, id, cancel| {
                struct CaptureDone<'a>(&'a AtomicBool);
                impl Drop for CaptureDone<'_> {
                    fn drop(&mut self) {
                        self.0.store(false, Ordering::Release);
                    }
                }
                let _capture = CaptureDone(&this.capturing);
                let db = this.db()?;
                reject_overlap(db.data_dir(), &path)?;
                if db.physical_replica_mode() || db.durability_mode() == DurabilityMode::Memory {
                    return Err(backup_err(
                        "full backup requires persistent standalone/master source",
                    ));
                }
                let repo = if path.join("repository.json").exists() {
                    Repository::open(&path, None)?
                } else {
                    Repository::init(&path, None, None)?
                };
                if let Some(entry) = this
                    .jobs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get_mut(id)
                {
                    entry.job.repository_id = Some(repo.header.id.clone());
                }
                let _lease = Lease::acquire(&repo.root, false)?;
                let base_id = this
                    .jobs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(id)
                    .unwrap()
                    .job
                    .base_id
                    .clone()
                    .unwrap();
                let operation = repo.root.join("staging").join(&base_id);
                fs::create_dir(&operation).map_err(|e| io_err("create base operation", e))?;
                let source_id = load_or_create_source_id(db.data_dir())?;
                let mut stream_id = None;
                {
                    let state = this.control.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(state) = state.as_ref() {
                        if state.repository_id == repo.header.id
                            && state.effective != Policy::Disabled
                        {
                            if state.lifecycle == "DRAINING" || state.lifecycle == "BROKEN" {
                                return Err(backup_err("archive state does not allow capture"));
                            }
                            stream_id = Some(state.stream_id.clone().unwrap_or_else(random_uuid));
                        }
                    }
                }
                let result = (|| {
                    let request = BackupRequest {
                        repository: operation.join("data"),
                    };
                    let manifest = capture_database(
                        &db,
                        &request,
                        &base_id,
                        cancel,
                        |phase, bytes, files, remaining| {
                            this.phase(id, phase);
                            if let Some(job) = this
                                .jobs
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .get_mut(id)
                            {
                                job.job.bytes = bytes;
                                job.job.files_done = files;
                                job.job.phase_remaining_bytes = remaining;
                            }
                        },
                        |end, time, source_hint| {
                            this.job_target(id, end, time);
                            if let Some(stream) = &stream_id {
                                let mut guard =
                                    this.control.lock().unwrap_or_else(|e| e.into_inner());
                                let state = guard
                                    .as_mut()
                                    .ok_or_else(|| backup_err("missing source archive control"))?;
                                if state.stream_id.is_none() {
                                    db.capture.set_lease(
                                        "archive",
                                        Some(WalLease {
                                            history_retention_ms: this.db()?.undo_retention_ms(),
                                            floor: end + 1,
                                            proof: serde_json::to_string(&(
                                                repo.root.clone(),
                                                base_id.clone(),
                                                stream.clone(),
                                                end,
                                                time,
                                            ))
                                            .map_err(|e| backup_err(e.to_string()))?,
                                        }),
                                    )?;
                                    state.pending = Some(Pending {
                                        base_id: base_id.clone(),
                                        stream_id: stream.clone(),
                                        end,
                                        time,
                                        hint: source_hint,
                                    });
                                    state.lifecycle = "PREPARING_BASE".into();
                                    this.save(state)?;
                                }
                            }
                            Ok(())
                        },
                    )?;
                    check_cancelled(cancel)?;
                    this.phase(id, "VALIDATING_CONTINUATION");
                    let image = native::verify(&operation.join("data"))?;
                    let marker = read_bounded(
                        &operation.join("data").join(native::MARKER),
                        MAX_METADATA_BYTES,
                    )?;
                    let seal =
                        serde_json::to_vec(&image.seal).map_err(|e| backup_err(e.to_string()))?;
                    let base = Base {
                        id: base_id.clone(),
                        repository_id: repo.header.id.clone(),
                        source_id: source_id.clone(),
                        timeline_id: source_id.clone(),
                        stream_id: stream_id.clone(),
                        source_end_lsn: manifest.end_lsn,
                        source_end_time: manifest.end_commit_unix_us,
                        marker_digest: sha256_hex(&marker),
                        seal_digest: sha256_hex(&seal),
                    };
                    this.phase(id, "PUBLISHING");
                    check_cancelled(cancel)?;
                    if let Some(stream) = &stream_id {
                        if !repo
                            .root
                            .join("streams")
                            .join(stream)
                            .join("descriptor")
                            .exists()
                        {
                            repo.register_stream(&super::repository::StreamDescriptor {
                                version: 1,
                                repository_id: repo.header.id.clone(),
                                stream_id: stream.clone(),
                                source_id: source_id.clone(),
                                timeline_id: source_id.clone(),
                                anchor_base: base_id.clone(),
                                anchor_lsn: base.source_end_lsn,
                                anchor_time: base.source_end_time,
                            })?;
                        }
                    }
                    let position = repo.publish_base(&operation, base.clone(), &image)?;
                    if let Some(entry) = this
                        .jobs
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .get_mut(id)
                    {
                        entry.job.durable_through_lsn = base.source_end_lsn;
                        entry.job.covered_time = base.source_end_time;
                        entry.job.catalog_generation = position.generation;
                    }
                    let mut guard = this.control.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(state) = guard.as_mut() {
                        if state.repository_id == repo.header.id {
                            state.latest_base = Some(base_id.clone());
                            state.latest_base_lsn = base.source_end_lsn;
                            state.latest_base_time = base.source_end_time;
                            if state.pending.as_ref().is_some_and(|p| p.base_id == base_id) {
                                state.stream_id = stream_id.clone();
                                state.fence_time = None;
                                state.last_range = None;
                                state.cursor = manifest.end_lsn;
                                state.covered_time = manifest.end_commit_unix_us;
                                state.position = position;
                                state.hint = state.pending.as_ref().and_then(|p| p.hint);
                                state.pending = None;
                                state.lifecycle = "ACTIVE".into();
                                this.save(state)?;
                                db.capture.set_lease(
                                    "archive",
                                    Some(WalLease {
                                        history_retention_ms: this.db()?.undo_retention_ms(),
                                        floor: state.cursor + 1,
                                        proof: state.position.digest.clone(),
                                    }),
                                )?;
                            } else {
                                this.save(state)?;
                            }
                        }
                    }
                    drop(guard);
                    if this
                        .control
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_ref()
                        .is_some_and(|s| {
                            s.effective == Policy::Continuous && s.lifecycle == "ACTIVE"
                        })
                    {
                        this.start_scheduler()?;
                    }
                    this.phase(id, "QUERY_READY");
                    Ok(())
                })();
                if result.is_err() {
                    if Repository::open(&repo.root, None).is_ok()
                        && !repo.root.join("bases").join(&base_id).exists()
                    {
                        let mut guard = this.control.lock().unwrap_or_else(|e| e.into_inner());
                        if let Some(state) = guard.as_mut() {
                            if state.pending.as_ref().is_some_and(|p| p.base_id == base_id) {
                                state.pending = None;
                                state.lifecycle = "WAITING_BASE".into();
                                this.save(state)?;
                                db.capture.set_lease("archive", None)?;
                            }
                        }
                        let _ = fs::remove_dir_all(&operation);
                    }
                }
                result
            },
        );
        if result.is_err() {
            self.capturing.store(false, Ordering::Release);
        }
        result
    }
    pub(crate) fn backup_wal(self: &Arc<Self>, repository: &Path) -> Result<Job, QueryError> {
        self.enqueue_wal(repository, false)?
            .ok_or_else(|| backup_err("WAL request was not admitted"))
    }
    fn enqueue_wal(
        self: &Arc<Self>,
        repository: &Path,
        automatic: bool,
    ) -> Result<Option<Job>, QueryError> {
        let _serial = self.serial.lock().unwrap_or_else(|e| e.into_inner());
        let path = resolved_destination(repository)?;
        {
            let guard = self.control.lock().unwrap_or_else(|e| e.into_inner());
            let state = guard
                .as_ref()
                .ok_or_else(|| backup_err("WAL archiving is not configured"))?;
            if automatic && (state.effective != Policy::Continuous || state.lifecycle != "ACTIVE") {
                return Ok(None);
            }
            if state.repository != path
                || state.effective == Policy::Disabled
                || state.lifecycle != "ACTIVE"
            {
                return Err(backup_err(
                    "BACKUP WAL requires an active MANUAL/CONTINUOUS stream",
                ));
            }
        }
        let target = self.capture_target()?;
        let requested = Some((target.lsn, target.time));
        self.spawn("WAL", None, requested, move |this, id, cancel| {
            #[cfg(test)]
            super::faults::hit("wal_before_execute", &this.root)?;
            let _lock = this.archive.lock().unwrap_or_else(|e| e.into_inner());
            this.archive_to(target, id, cancel)
        })
        .map(Some)
    }
    fn capture_target(&self) -> Result<CapturedTarget, QueryError> {
        let state = self
            .control
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| backup_err("archive policy is not configured"))?;
        let db = self.db()?;
        let log = db.wal_log();
        let target = {
            let log = log.lock();
            CapturedTarget {
                identity: ArchiveIdentity::from(&state),
                lsn: log.last_lsn(),
                time: db.backup_time_fence(),
            }
        };
        crate::wal::LogManager::sync_durable_to(&log, target.lsn)?;
        Ok(target)
    }
    fn archive_to(
        &self,
        target: CapturedTarget,
        job: &str,
        cancel: &AtomicBool,
    ) -> Result<(), QueryError> {
        check_cancelled(cancel)?;
        {
            let control = self.control.lock().unwrap_or_else(|e| e.into_inner());
            let state = control
                .as_ref()
                .ok_or_else(|| backup_err("missing archive control"))?;
            if target.identity != ArchiveIdentity::from(state) {
                return Err(backup_err(
                    "archive request belongs to an obsolete repository/source/stream",
                ));
            }
            if state.effective == Policy::Disabled
                || !["ACTIVE", "DRAINING"].contains(&state.lifecycle.as_str())
            {
                return Err(backup_err("archive stream is closed or unavailable"));
            }
        }
        if !self.confirmed_head_verified.load(Ordering::Acquire) {
            self.verify_confirmed_head()?;
        }
        self.reconcile()?;
        let mut state = self
            .control
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| backup_err("missing archive control"))?;
        #[cfg(test)]
        if let Some(hook) = ARCHIVE_SNAPSHOT_HOOK.lock().unwrap().take() {
            hook();
        }
        if !["ACTIVE", "DRAINING"].contains(&state.lifecycle.as_str())
            || state.effective == Policy::Disabled
        {
            return Err(backup_err("archive stream is closed or unavailable"));
        }
        if target.identity != ArchiveIdentity::from(&state) {
            return Err(backup_err(
                "archive request belongs to an obsolete repository/source/stream",
            ));
        }
        let db = self.db()?;
        let mut repo = Repository::open(&state.repository, None)?;
        repo.require_key(None)?;
        if repo.header.id != state.repository_id {
            return Err(backup_err("repository UUID changed"));
        }
        let _lease = Lease::acquire(&repo.root, false)?;
        if target.lsn < state.cursor
            || (target.lsn == state.cursor && target.time <= state.covered_time)
        {
            return self.receipt(job, &state);
        }
        crate::wal::persist_source_clock(db.data_dir(), target.time)?;
        check_cancelled(cancel)?;
        if target.lsn == state.cursor {
            self.phase(job, "PUBLISHING");
            check_cancelled(cancel)?;
            let position =
                repo.publish_fence(state.stream_id.as_ref().unwrap(), state.cursor, target.time)?;
            test_fault(4)?;
            let mut guard = self.control.lock().unwrap_or_else(|e| e.into_inner());
            let mut next = state;
            if let Some(current) = guard.as_ref() {
                next.latest_base = current.latest_base.clone();
                next.latest_base_lsn = current.latest_base_lsn;
                next.latest_base_time = current.latest_base_time;
                next.max_pending_wal_bytes = current.max_pending_wal_bytes;
            }
            next.fence_time = Some(target.time);
            next.position = position;
            next.covered_time = next.covered_time.max(target.time);
            next.error.clear();
            next.consecutive_errors = 0;
            next.last_error_unix_ms = 0;
            self.save(&next)?;
            *guard = Some(next.clone());
            drop(guard);
            return self.receipt(job, &next);
        }
        let prefixes = db
            .wal_log()
            .lock()
            .pin_prefix_after(state.cursor, state.hint)?;
        let mut expected = state.cursor + 1;
        let mut frames = 0;
        let mut bytes = 0;
        let mut chunks = Vec::new();
        let mut buffer = Vec::with_capacity(CHUNK_BYTES);
        let mut hint = state.hint;
        let mut last_frame_time = state.covered_time;
        let mut source_spans: Vec<super::repository::SourceSpan> = Vec::new();
        let _charge = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::StorageScratch,
            CHUNK_BYTES * 3,
        )?;
        for prefix in &prefixes {
            super::resources::feedback(&db.capture)?;
            if expected > target.lsn {
                break;
            }
            if source_spans.len() == 256 {
                if !buffer.is_empty() {
                    chunks.push(repo.store_blob(&buffer)?);
                    buffer.clear();
                }
                check_cancelled(cancel)?;
                self.phase(job, "PUBLISHING");
                let range = Range {
                    id: random_uuid(),
                    stream_id: state.stream_id.clone().unwrap(),
                    source_id: state.source_id.clone(),
                    timeline_id: state.timeline_id.clone(),
                    first_lsn: state.cursor + 1,
                    last_lsn: expected - 1,
                    covered_time: last_frame_time,
                    frame_count: frames,
                    raw_bytes: bytes,
                    source_spans: std::mem::take(&mut source_spans),
                    chunks: std::mem::take(&mut chunks),
                };
                self.publish_archive_range(&repo, &mut state, range, hint, job)?;
                bytes = 0;
                frames = 0;
                self.phase(job, "ARCHIVING");
            }
            let after = expected - 1;
            crate::wal::visit_pinned_range(
                prefix,
                after,
                target.lsn,
                |first, last, source_time, offset, raw| {
                    if last > target.lsn {
                        return Ok(());
                    }
                    check_cancelled(cancel)?;
                    if source_time <= last_frame_time {
                        return Err(backup_err("archive source decision time regressed"));
                    }
                    last_frame_time = source_time;
                    if first != expected {
                        return Err(backup_err("archive source contains an LSN gap"));
                    }
                    for part in raw.chunks(CHUNK_BYTES) {
                        check_cancelled(cancel)?;
                        let mut part = part;
                        while !part.is_empty() {
                            let take = (CHUNK_BYTES - buffer.len()).min(part.len());
                            buffer.extend_from_slice(&part[..take]);
                            part = &part[take..];
                            if buffer.len() == CHUNK_BYTES {
                                chunks.push(repo.store_blob(&buffer)?);
                                buffer.clear();
                            }
                        }
                    }
                    if source_spans
                        .last()
                        .is_some_and(|span| span.segment_id == prefix.segment_id)
                    {
                        let span = source_spans.last_mut().unwrap();
                        span.last_lsn = last;
                        span.end_offset = offset;
                    } else {
                        source_spans.push(super::repository::SourceSpan {
                            segment_id: prefix.segment_id,
                            first_lsn: first,
                            last_lsn: last,
                            start_offset: offset - raw.len() as u64,
                            end_offset: offset,
                            captured_valid_len: prefix.len,
                        });
                    }
                    bytes += raw.len() as u64;
                    frames += 1;
                    expected = last + 1;
                    hint = Some((prefix.segment_id, offset));
                    Ok(())
                },
            )?;
        }
        if expected != target.lsn + 1 {
            return Err(backup_err(
                "archive target is not a complete covered WAL boundary",
            ));
        }
        if !buffer.is_empty() {
            chunks.push(repo.store_blob(&buffer)?);
        }
        check_cancelled(cancel)?;
        self.phase(job, "PUBLISHING");
        check_cancelled(cancel)?;
        let range = Range {
            id: random_uuid(),
            stream_id: state.stream_id.clone().unwrap(),
            source_id: state.source_id.clone(),
            timeline_id: state.timeline_id.clone(),
            first_lsn: state.cursor + 1,
            last_lsn: target.lsn,
            covered_time: target.time,
            frame_count: frames,
            raw_bytes: bytes,
            source_spans,
            chunks,
        };
        self.publish_archive_range(&repo, &mut state, range, hint, job)
    }
    fn publish_archive_range(
        &self,
        repo: &Repository,
        state: &mut Control,
        range: Range,
        hint: Option<(u64, u64)>,
        job: &str,
    ) -> Result<(), QueryError> {
        let db = self.db()?;
        let last_lsn = range.last_lsn;
        let covered_time = range.covered_time;
        let bytes = range.raw_bytes;
        let range_proof = (
            range.id.clone(),
            sha256_hex(&serde_json::to_vec(&range).map_err(|e| backup_err(e.to_string()))?),
        );
        #[cfg(test)]
        super::faults::hit("archive_before_publish", &repo.root)?;
        let position = repo.publish_range(range)?;
        test_fault(2)?;
        let mut guard = self.control.lock().unwrap_or_else(|e| e.into_inner());
        let mut next = state.clone();
        if let Some(current) = guard.as_ref() {
            next.latest_base = current.latest_base.clone();
            next.latest_base_lsn = current.latest_base_lsn;
            next.latest_base_time = current.latest_base_time;
            next.max_pending_wal_bytes = current.max_pending_wal_bytes;
        }
        next.fence_time = None;
        next.last_range = Some(range_proof);
        next.cursor = last_lsn;
        next.covered_time = covered_time;
        next.position = position;
        next.hint = hint;
        next.error.clear();
        next.consecutive_errors = 0;
        next.last_error_unix_ms = 0;
        next.next_retry_unix_ms = 0;
        self.save(&next)?;
        #[cfg(test)]
        super::faults::hit("source_ack_saved", &self.root)?;
        test_fault(3)?;
        db.capture.set_lease(
            "archive",
            Some(WalLease {
                history_retention_ms: db.undo_retention_ms(),
                floor: next.cursor + 1,
                proof: next.position.digest.clone(),
            }),
        )?;
        db.capture.budget.acknowledged(bytes);
        *guard = Some(next.clone());
        drop(guard);
        if let Some(entry) = self
            .jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(job)
        {
            entry.job.bytes = entry.job.bytes.saturating_add(bytes);
        }
        *state = next.clone();
        self.receipt(job, &next)
    }
    fn receipt(&self, id: &str, state: &Control) -> Result<(), QueryError> {
        if let Some(entry) = self
            .jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(id)
        {
            entry.job.durable_through_lsn = state.cursor;
            entry.job.covered_time = state.covered_time;
            entry.job.catalog_generation = state.position.generation;
        }
        Ok(())
    }
    fn reconcile(&self) -> Result<(), QueryError> {
        let _database = self.db()?;
        let mut guard = self.control.lock().unwrap_or_else(|e| e.into_inner());
        let Some(state) = guard.as_mut() else {
            return Ok(());
        };
        if state.effective == Policy::Disabled && state.lifecycle == "CLOSED" {
            if _database.capture.leases().contains_key("archive") {
                _database.capture.set_lease("archive", None)?;
            }
            if state.close_publication_pending {
                let repo = Repository::open(&state.repository, None)?;
                if repo.header.id != state.repository_id {
                    return Err(backup_err("discarded-close repository identity changed"));
                }
                let _lease = Lease::acquire(&repo.root, false)?;
                let mut position = repo.position()?;
                let mut closed = false;
                repo.visit_live(&position, |_, event| {
                    if let Publication::Close {
                        stream_id,
                        lsn,
                        time,
                        discarded,
                    } = event
                    {
                        closed |= Some(&stream_id) == state.stream_id.as_ref()
                            && lsn == state.cursor
                            && time >= state.covered_time
                            && discarded == state.close_discarded;
                    }
                    Ok(())
                })?;
                if !closed {
                    position = repo.publish(Publication::Close {
                        stream_id: state
                            .stream_id
                            .clone()
                            .ok_or_else(|| backup_err("discarded close has no stream"))?,
                        lsn: state.cursor,
                        time: state.covered_time,
                        discarded: state.close_discarded,
                    })?;
                } else {
                    repo.confirm_position(&position)?;
                }
                state.position = position;
                state.close_publication_pending = false;
                state.error.clear();
                state.consecutive_errors = 0;
                state.last_error_unix_ms = 0;
                state.next_retry_unix_ms = 0;
                self.save(state)?;
            }
            return Ok(());
        }
        let mut repo = match Repository::open(&state.repository, None) {
            Ok(repo) => repo,
            Err(error) => {
                state.error = error_detail(&error);
                if state.close_publication_pending {
                    return Err(error);
                }
                return Ok(());
            }
        };
        if repo.header.id != state.repository_id {
            state.lifecycle = "BROKEN".into();
            self.save(state)?;
            return Err(backup_err("repository identity changed"));
        }
        let _lease = Lease::acquire(&repo.root, false)?;
        repo.require_key(None)?;
        // A completed immutable base may survive a failed CURRENT update.
        // Verify and republish that same operation before canceling any pin.
        if let Some(pending) = state.pending.clone() {
            let path = repo.root.join("bases").join(&pending.base_id);
            if path.exists() {
                let (base, _) = repo.base(&pending.base_id)?;
                let mut published = false;
                repo.visit_live(&repo.position()?, |_, event| {
                    if matches!(event,Publication::Base{base:b}if b.id==base.id) {
                        published = true;
                    }
                    Ok(())
                })?;
                if !published {
                    sync_tree(&path)?;
                    sync_dir(&repo.root.join("bases"))?;
                    repo.publish(Publication::Base { base })?;
                }
            } else {
                // Captures do not resume against a changing live source.
                state.pending = None;
                state.lifecycle = "WAITING_BASE".into();
                self.save(state)?;
                self.db()?.capture.set_lease("archive", None)?;
            }
        } else if state.stream_id.is_none() {
            if let Some(lease) = self.db()?.capture.leases().get("archive") {
                let proof: (PathBuf, String, String, u64, u64) = serde_json::from_str(&lease.proof)
                    .map_err(|_| backup_err("unknown archive lease proof; protection retained"))?;
                if proof.0 != state.repository || lease.floor != proof.3 + 1 {
                    return Err(backup_err("candidate archive lease identity mismatch"));
                }
                if repo.root.join("bases").join(&proof.1).exists() {
                    let (base, _) = repo.base(&proof.1)?;
                    if base.stream_id.as_deref() != Some(&proof.2) {
                        return Err(backup_err("candidate stream mismatch"));
                    }
                    state.pending = Some(Pending {
                        base_id: proof.1,
                        stream_id: proof.2,
                        end: proof.3,
                        time: proof.4,
                        hint: None,
                    });
                    self.save(state)?;
                    repo.publish(Publication::Base { base })?;
                } else {
                    state.lifecycle = "WAITING_BASE".into();
                    self.save(state)?;
                    self.db()?.capture.set_lease("archive", None)?;
                }
            }
        }
        let position = repo.position()?;
        if position != state.position {
            repo.confirm_position(&position)?;
        }
        if position.generation < state.position.generation {
            state.lifecycle = "BROKEN".into();
            self.save(state)?;
            return Err(backup_err("repository catalog rollback"));
        }
        let mut publications = Vec::new();
        let mut metadata =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)?;
        repo.visit_since(&position, &state.position, |publication| {
            let relevant = match &publication {
                Publication::Base { base } => {
                    state.pending.as_ref().is_some_and(|p| p.base_id == base.id)
                        || (base.stream_id == state.stream_id
                            && base.source_id == state.source_id
                            && base.source_end_lsn > state.latest_base_lsn)
                }
                Publication::Range { range } => Some(&range.stream_id) == state.stream_id.as_ref(),
                Publication::Close { stream_id, .. } | Publication::Fence { stream_id, .. } => {
                    Some(stream_id) == state.stream_id.as_ref()
                }
                Publication::Prune { .. } | Publication::Checkpoint { .. } => false,
            };
            if !relevant {
                return Ok(());
            }
            let bytes = serde_json::to_vec(&publication).map_err(|e| backup_err(e.to_string()))?;
            metadata.try_grow(bytes.len().saturating_mul(16))?;
            if publications.len() >= 1024 {
                return Err(backup_err(
                    "unacknowledged catalog backlog exceeds reconciliation budget",
                ));
            }
            publications.push(publication);
            Ok(())
        })?;
        for publication in publications.into_iter().rev() {
            match publication {
                Publication::Base { base }
                    if state.pending.as_ref().is_some_and(|p| p.base_id == base.id) =>
                {
                    repo.base(&base.id)?;
                    let expected = state.pending.as_ref().unwrap();
                    if base.source_id != state.source_id
                        || base.timeline_id != state.timeline_id
                        || base.stream_id.as_ref() != Some(&expected.stream_id)
                        || base.source_end_lsn != expected.end
                        || base.source_end_time != expected.time
                    {
                        state.lifecycle = "BROKEN".into();
                        self.save(state)?;
                        return Err(backup_err(
                            "published baseline differs from the protected capture boundary",
                        ));
                    }
                    let pending = state.pending.take().unwrap();
                    state.stream_id = Some(pending.stream_id);
                    state.cursor = pending.end;
                    state.hint = pending.hint;
                    state.covered_time = pending.time;
                    state.latest_base_lsn = base.source_end_lsn;
                    state.latest_base_time = base.source_end_time;
                    state.latest_base = Some(base.id);
                    state.lifecycle = "ACTIVE".into();
                }
                Publication::Base { base }
                    if base.stream_id == state.stream_id
                        && base.source_id == state.source_id
                        && base.source_end_lsn > state.latest_base_lsn =>
                {
                    repo.base_for_open(&base.id)?;
                    state.latest_base_lsn = base.source_end_lsn;
                    state.latest_base_time = base.source_end_time;
                    state.latest_base = Some(base.id);
                }
                Publication::Range { range }
                    if Some(&range.stream_id) == state.stream_id.as_ref()
                        && range.last_lsn > state.cursor =>
                {
                    if range.source_id != state.source_id || range.timeline_id != state.timeline_id
                    {
                        state.lifecycle = "BROKEN".into();
                        self.save(state)?;
                        return Err(backup_err("unacknowledged range changed source branch"));
                    }
                    if range.first_lsn != state.cursor + 1 {
                        return Err(backup_err("unacknowledged archive gap"));
                    }
                    // Full payload validation is performed by the range reader before ACK.
                    super::restore::verify_range(&repo, &range)?;
                    state.last_range = Some((
                        range.id.clone(),
                        sha256_hex(
                            &serde_json::to_vec(&range).map_err(|e| backup_err(e.to_string()))?,
                        ),
                    ));
                    state.cursor = range.last_lsn;
                    state.covered_time = state.covered_time.max(range.covered_time);
                    state.hint = None;
                }
                Publication::Close {
                    stream_id,
                    lsn,
                    time,
                    ..
                } if Some(&stream_id) == state.stream_id.as_ref() => {
                    if lsn > state.cursor {
                        return Err(backup_err("stream close exceeds durable archive"));
                    }
                    state.covered_time = state.covered_time.max(time);
                    state.effective = Policy::Disabled;
                    state.requested = Policy::Disabled;
                    state.lifecycle = "CLOSED".into();
                    state.drain = None;
                    state.close_publication_pending = false;
                }
                Publication::Fence {
                    stream_id,
                    lsn,
                    time,
                } if Some(&stream_id) == state.stream_id.as_ref() && lsn == state.cursor => {
                    state.covered_time = state.covered_time.max(time);
                }
                _ => {}
            }
        }
        if let Some(stream) = state
            .stream_id
            .as_ref()
            .filter(|_| state.fence_time.is_some())
        {
            let fence = repo.fence_at(stream, &position)?;
            if state
                .fence_time
                .is_some_and(|t| fence.as_ref().is_none_or(|f| f.time < t))
            {
                return Err(backup_err(
                    "acknowledged time fence is missing or rolled back",
                ));
            }
            // A later visible fence may have failed its parent sync. Only
            // the saved ACK authorizes receipts; a retry publishes a new fence.
        }
        if position != state.position {
            state.position = position;
            self.save(state)?;
        }
        self.repair_retention(state, &repo)?;
        if state.lifecycle == "CLOSED" {
            self.save(state)?;
            self.db()?.capture.set_lease("archive", None)?;
        }
        Ok(())
    }
    /// Validate the exact acknowledged head once on source startup. Ordinary
    /// archive rounds inspect only the unacknowledged catalog suffix.
    fn verify_confirmed_head(&self) -> Result<(), QueryError> {
        self.confirmed_head_verified.store(false, Ordering::Release);
        let mut guard = self.control.lock().unwrap_or_else(|e| e.into_inner());
        let Some(state) = guard.as_mut() else {
            return Ok(());
        };
        if state.effective == Policy::Disabled || state.stream_id.is_none() {
            self.confirmed_head_verified.store(true, Ordering::Release);
            return Ok(());
        }
        let mut repo = match Repository::open(&state.repository, None) {
            Ok(repo) => repo,
            Err(error) => {
                state.error = error_detail(&error);
                return Ok(());
            }
        };
        let _lease = Lease::acquire(&repo.root, false)?;
        repo.require_key(None)?;
        let verify = verify_receipt(&repo, state);
        if let Err(error) = &verify {
            if !["RESOURCE_LIMIT", "CANCELLED", "KEY_UNAVAILABLE"]
                .contains(&failure_class(&error_detail(error)))
            {
                state.lifecycle = "BROKEN".into();
            }
            state.error = error_detail(error);
            self.save(state)?;
        }
        verify?;
        self.repair_retention(state, &repo)?;
        self.confirmed_head_verified.store(true, Ordering::Release);
        Ok(())
    }
    fn repair_retention(&self, state: &Control, repo: &Repository) -> Result<(), QueryError> {
        if state.effective == Policy::Disabled
            || state.stream_id.is_none()
            || !["ACTIVE", "DRAINING"].contains(&state.lifecycle.as_str())
        {
            return Ok(());
        }
        let db = self.db()?;
        let lease = db
            .capture
            .leases()
            .get("archive")
            .cloned()
            .ok_or_else(|| backup_err("confirmed archive has no durable source lease"))?;
        let floor = state
            .cursor
            .checked_add(1)
            .ok_or_else(|| backup_err("archive cursor overflow"))?;
        if lease.floor > floor {
            return Err(backup_err(
                "source lease advanced past the confirmed archive",
            ));
        }
        if lease.floor == floor {
            if lease.proof != state.position.digest {
                self.save(state)?;
                db.capture.set_lease(
                    "archive",
                    Some(WalLease {
                        proof: state.position.digest.clone(),
                        ..lease
                    }),
                )?;
            }
            return Ok(());
        }
        verify_receipt(repo, state)?;
        repo.verify_coverage(&state.position)?;
        repo.visit_live(&state.position, |_, event| {
            if let Publication::Range { range } = event {
                if Some(&range.stream_id) == state.stream_id.as_ref()
                    && range.last_lsn >= lease.floor
                    && range.first_lsn <= state.cursor
                {
                    super::restore::verify_range(repo, &range)?;
                }
            }
            Ok(())
        })?;
        let log = db.wal_log();
        let prefixes = log.lock().pin_prefix_after(lease.floor - 1, None)?;
        let mut acknowledged = 0u64;
        for prefix in &prefixes {
            let after = prefix.first_lsn.max(lease.floor) - 1;
            crate::wal::visit_pinned_range(prefix, after, state.cursor, |_, last, _, _, raw| {
                if last > state.cursor {
                    return Err(backup_err("confirmed cursor splits a source frame"));
                }
                acknowledged = acknowledged
                    .checked_add(raw.len() as u64)
                    .ok_or_else(|| backup_err("retention byte accounting overflow"))?;
                Ok(())
            })?;
        }
        // A prior failed save may already have changed the in-memory cursor.
        // Reconfirm the source ACK before advancing its independently persisted pin.
        self.save(state)?;
        db.capture.set_lease(
            "archive",
            Some(WalLease {
                floor,
                proof: state.position.digest.clone(),
                ..lease
            }),
        )?;
        db.capture.budget.acknowledged(acknowledged);
        Ok(())
    }
    fn save_jobs(&self, jobs: &BTreeMap<String, JobEntry>) -> Result<(), QueryError> {
        let values: Vec<&Job> = jobs.values().map(|j| &j.job).collect();
        let payload = serde_json::to_vec(&values).map_err(|e| backup_err(e.to_string()))?;
        if payload.len() > 1024 * 1024 {
            return Err(backup_err("job history exceeds budget"));
        }
        let envelope = StateEnvelope {
            version: 1,
            digest: sha256_hex(&payload),
            payload,
        };
        let directory = self.root.join("system/backup-control");
        if self.control_directory_confirmed.load(Ordering::Acquire) {
            // save() proved this namespace and its parent durable. The atomic
            // replacement below still syncs the new file and final directory;
            // another pre-sync cannot strengthen that publication proof.
            reject_directory_links(&directory)?;
            if !directory.is_dir() {
                return Err(backup_err("confirmed backup control directory disappeared"));
            }
        } else {
            create_directory_durable(&directory)?;
        }
        atomic_replace(
            &self.root.join("system/backup-control/jobs"),
            &serde_json::to_vec(&envelope).map_err(|e| backup_err(e.to_string()))?,
        )
    }
    fn load_jobs(&self) -> Result<(), QueryError> {
        let path = self.root.join("system/backup-control/jobs");
        if !path.exists() {
            return Ok(());
        }
        let value: StateEnvelope = serde_json::from_slice(&read_bounded(&path, 4 * 1024 * 1024)?)
            .map_err(|e| backup_err(e.to_string()))?;
        if value.version != 1 || sha256_hex(&value.payload) != value.digest {
            return Err(backup_err("invalid durable job history"));
        }
        let values: Vec<Job> =
            serde_json::from_slice(&value.payload).map_err(|e| backup_err(e.to_string()))?;
        if values.len() > 256 {
            return Err(backup_err("job history exceeds limit"));
        }
        let mut jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        for mut job in values {
            if job.state == "RUNNING" {
                job.state = "FAILED".into();
                job.error =
                    "process interrupted; committed artifacts are reconciled independently".into();
                job.completed_unix_ms = now_ms();
                job.failure_class = "PROCESS_INTERRUPTED".into();
            }
            jobs.insert(
                job.id.clone(),
                JobEntry {
                    job,
                    cancel: Arc::new(AtomicBool::new(false)),
                    phase_started: Instant::now(),
                },
            );
        }
        self.save_jobs(&jobs)
    }
    fn phase(&self, id: &str, phase: &str) {
        if let Some(entry) = self
            .jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(id)
        {
            if entry.job.phase != phase {
                let elapsed = entry
                    .phase_started
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64;
                *entry
                    .job
                    .phase_times_us
                    .entry(entry.job.phase.clone())
                    .or_default() += elapsed;
                entry.phase_started = Instant::now();
            }
            entry.job.phase = phase.into();
        }
    }
    fn job_target(&self, id: &str, lsn: u64, time: u64) {
        if let Some(entry) = self
            .jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(id)
        {
            entry.job.requested_lsn = lsn;
            entry.job.requested_source_time = time;
        }
    }
    fn spawn(
        self: &Arc<Self>,
        kind: &str,
        base_id: Option<String>,
        requested: Option<(u64, u64)>,
        work: impl FnOnce(&Arc<Self>, &str, &AtomicBool) -> Result<(), QueryError> + Send + 'static,
    ) -> Result<Job, QueryError> {
        let mut workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
        if self.closing.load(Ordering::Acquire) {
            return Err(backup_err("database is shutting down"));
        }
        let receipt_identity = self
            .control
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(ArchiveIdentity::from);
        let mut job = Job {
            id: random_uuid(),
            kind: kind.into(),
            state: "RUNNING".into(),
            phase: "QUEUED".into(),
            base_id,
            requested_lsn: requested.map_or(0, |p| p.0),
            requested_source_time: requested.map_or(0, |p| p.1),
            durable_through_lsn: 0,
            covered_time: 0,
            catalog_generation: 0,
            repository_id: if kind == "DATABASE" {
                None
            } else {
                receipt_identity
                    .as_ref()
                    .map(|identity| identity.repository_id.clone())
            },
            bytes: 0,
            files_done: 0,
            phase_remaining_bytes: None,
            error: String::new(),
            sequence: 0,
            created_unix_ms: now_ms(),
            started_unix_ms: 0,
            completed_unix_ms: 0,
            phase_times_us: BTreeMap::new(),
            failure_class: String::new(),
        };
        let cancel = Arc::new(AtomicBool::new(false));
        let mut jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        job.sequence = jobs
            .values()
            .map(|e| e.job.sequence)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| backup_err("job sequence overflow"))?;
        if jobs.len() >= 256 {
            if let Some(id) = jobs
                .values()
                .filter(|e| e.job.state != "RUNNING")
                .min_by_key(|e| e.job.sequence)
                .map(|e| e.job.id.clone())
            {
                jobs.remove(&id);
            } else {
                return Err(backup_err("job history budget exceeded"));
            }
        }
        jobs.insert(
            job.id.clone(),
            JobEntry {
                job: job.clone(),
                cancel: cancel.clone(),
                phase_started: Instant::now(),
            },
        );
        if let Err(error) = self.save_jobs(&jobs) {
            if let Some(entry) = jobs.get_mut(&job.id) {
                entry.job.state = "FAILED".into();
                entry.job.error = error_detail(&error);
            }
            return Err(error);
        }
        drop(jobs);
        let this = self.clone();
        let id = job.id.clone();
        #[cfg(test)]
        super::faults::hit("job_before_spawn", &self.root)?;
        if workers.is_empty() {
            let create = (|| -> Result<mpsc::SyncSender<QueuedWork>, QueryError> {
                let (sender, receiver) =
                    mpsc::sync_channel::<QueuedWork>(super::resources::limits()?.queue_capacity);
                let receiver = Arc::new(Mutex::new(receiver));
                for _ in 0..super::resources::limits()?.workers {
                    let receiver = receiver.clone();
                    let worker = std::thread::Builder::new()
                        .name("mydb-backup-worker".into())
                        .spawn(move || loop {
                            let work = receiver.lock().unwrap_or_else(|e| e.into_inner()).recv();
                            match work {
                                Ok(work) => work(),
                                Err(_) => break,
                            }
                        })
                        .map_err(|e| io_err("spawn backup worker", e))?;
                    workers.push(worker);
                }
                Ok(sender)
            })();
            match create {
                Ok(sender) => *self.queue.lock().unwrap_or_else(|e| e.into_inner()) = Some(sender),
                Err(error) => {
                    for worker in workers.drain(..) {
                        let _ = worker.join();
                    }
                    let mut jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(entry) = jobs.get_mut(&job.id) {
                        entry.job.state = "FAILED".into();
                        entry.job.error = error_detail(&error);
                        entry.job.failure_class = "WORKER_START".into();
                        entry.job.completed_unix_ms = now_ms();
                    }
                    let _ = self.save_jobs(&jobs);
                    return Err(error);
                }
            }
        }
        let is_full = kind == "DATABASE";
        let task: QueuedWork = Box::new(move || {
            let _context = super::resources::ContextScope::worker(cancel.clone());
            this.phase(&id, "WAITING_RESOURCES");
            let mut work_started = false;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _process_task = super::resources::TaskPermit::acquire()?;
                this.phase(&id, "STARTING");
                {
                    let mut jobs = this.jobs.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(entry) = jobs.get_mut(&id) {
                        entry.job.started_unix_ms = now_ms();
                    }
                    this.save_jobs(&jobs)?;
                }
                let _database = this.db()?;
                check_cancelled(&cancel)?;
                work_started = true;
                work(&this, &id, &cancel)
            }))
            .unwrap_or_else(|_| Err(backup_err("backup worker panicked")));
            if is_full && !work_started {
                this.capturing.store(false, Ordering::Release);
            }
            if let Err(error) = &result {
                let actual_repository = this
                    .jobs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&id)
                    .and_then(|entry| entry.job.repository_id.clone());
                let relevant = this
                    .control
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .is_some_and(|state| {
                        receipt_identity.as_ref() == Some(&ArchiveIdentity::from(state))
                            && actual_repository.as_deref() == Some(state.repository_id.as_str())
                    });
                if relevant
                    && !(cancel.load(Ordering::Acquire) && error_detail(error).contains("cancel"))
                {
                    this.record_error(error);
                }
            }
            if result.is_err() {
                let _control = super::resources::ContextScope::control();
                let _archive = this.archive.lock().unwrap_or_else(|e| e.into_inner());
                let _ = this.reconcile();
            }
            let control = this
                .control
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let mut jobs = this.jobs.lock().unwrap_or_else(|e| e.into_inner());
            let entry = jobs.get_mut(&id).unwrap();
            if let Some(state) = control.filter(|state| {
                entry.job.kind != "DATABASE"
                    && entry.job.repository_id.as_deref() == Some(state.repository_id.as_str())
                    && receipt_identity.as_ref() == Some(&ArchiveIdentity::from(state))
            }) {
                entry.job.durable_through_lsn = entry.job.durable_through_lsn.max(state.cursor);
                entry.job.covered_time = entry.job.covered_time.max(state.covered_time);
                entry.job.catalog_generation =
                    entry.job.catalog_generation.max(state.position.generation);
            }
            entry
                .job
                .phase_times_us
                .entry(entry.job.phase.clone())
                .and_modify(|n| {
                    *n = n.saturating_add(
                        entry
                            .phase_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    )
                })
                .or_insert(
                    entry
                        .phase_started
                        .elapsed()
                        .as_micros()
                        .min(u128::from(u64::MAX)) as u64,
                );
            entry.job.completed_unix_ms = now_ms();
            match result {
                Ok(()) => {
                    entry.job.phase_remaining_bytes = Some(0);
                    entry.job.state = "COMPLETED".into();
                }
                Err(error) => {
                    entry.job.state = if cancel.load(Ordering::Acquire)
                        && error_detail(&error).contains("cancel")
                    {
                        "CANCELLED"
                    } else {
                        "FAILED"
                    }
                    .into();
                    entry.job.error = error_detail(&error).chars().take(8192).collect();
                    entry.job.failure_class = failure_class(&entry.job.error).into();
                }
            }
            if let Err(error) = this.save_jobs(&jobs) {
                if let Some(entry) = jobs.get_mut(&id) {
                    entry.job.state = "FAILED".into();
                    entry.job.error =
                        format!("job history persistence failed: {}", error_detail(&error));
                }
            }
        });
        let send = self
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .ok_or_else(|| backup_err("backup queue is closed"))?
            .try_send(task);
        if send.is_err() {
            let mut jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = jobs.get_mut(&job.id) {
                entry.job.state = "FAILED".into();
                entry.job.completed_unix_ms = now_ms();
                entry.job.error = "backup queue capacity exhausted".into();
                entry.job.failure_class = "RESOURCE_LIMIT".into();
            }
            self.save_jobs(&jobs)?;
            return Err(backup_err("backup queue capacity exhausted"));
        }
        Ok(job)
    }
    pub(crate) fn jobs(&self) -> Vec<Job> {
        let mut jobs = self
            .jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|entry| entry.job.clone())
            .collect::<Vec<_>>();
        jobs.sort_unstable_by_key(|job| std::cmp::Reverse(job.sequence));
        jobs
    }
    pub(crate) fn cancel(&self, id: &str) -> Result<(), QueryError> {
        let jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        let entry = jobs.get(id).ok_or_else(|| backup_err("job not found"))?;
        if entry.job.state != "RUNNING" || entry.job.phase.starts_with("PUBLISH") {
            return Err(backup_err(format!(
                "job cannot be cancelled in {} / {}",
                entry.job.state, entry.job.phase
            )));
        }
        entry.cancel.store(true, Ordering::Release);
        Ok(())
    }
    pub(crate) fn shutdown(&self) -> Result<(), QueryError> {
        let _shutdown = self.shutdown_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.closing.store(true, Ordering::Release);
        self.stop.store(true, Ordering::Release);
        #[cfg(test)]
        super::faults::hit("shutdown_admission_closed", &self.root)?;
        for entry in self.jobs.lock().unwrap_or_else(|e| e.into_inner()).values() {
            entry.cancel.store(true, Ordering::Release);
        }
        let background = self
            .background
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let mut error = None;
        if let Some(worker) = background {
            worker.thread().unpark();
            if worker.join().is_err() {
                error = Some(backup_err("archive scheduler panicked"));
            }
        }
        let mut registered = self.workers.lock().unwrap_or_else(|e| e.into_inner());
        self.queue.lock().unwrap_or_else(|e| e.into_inner()).take();
        let workers = std::mem::take(&mut *registered);
        drop(registered);
        for worker in workers {
            if worker.join().is_err() && error.is_none() {
                error = Some(backup_err("backup worker panicked during shutdown"));
            }
        }
        if let Some(error) = error {
            return Err(error);
        }
        Ok(())
    }
}
impl Drop for Coordinator {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.background.get_mut().unwrap().as_ref() {
            worker.thread().unpark();
        }
    }
}

fn worker_unpark(worker: Option<&std::thread::JoinHandle<()>>) {
    if let Some(worker) = worker {
        worker.thread().unpark();
    }
}

#[cfg(test)]
pub(crate) static TEST_FAULT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
fn test_fault(point: u8) -> Result<(), QueryError> {
    #[cfg(test)]
    if TEST_FAULT
        .compare_exchange(point, 0, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        return Err(backup_err(format!(
            "injected archive interruption at {point}"
        )));
    }
    let _ = point;
    Ok(())
}

#[cfg(test)]
impl Coordinator {
    pub(crate) fn test_start_scheduler(self: &Arc<Self>) -> Result<(), QueryError> {
        self.start_scheduler()
    }
    pub(crate) fn test_verify_head(&self) -> Result<(), QueryError> {
        self.verify_confirmed_head()
    }
    pub(crate) fn test_hold_archive(
        self: &Arc<Self>,
        entered: mpsc::SyncSender<()>,
        release: mpsc::Receiver<()>,
    ) -> Result<Job, QueryError> {
        self.spawn("WAL", None, None, move |this, _, _| {
            let _archive = this.archive.lock().unwrap();
            entered.send(()).unwrap();
            release.recv().unwrap();
            Ok(())
        })
    }
    pub(crate) fn test_blocked_job(
        self: &Arc<Self>,
        entered: mpsc::SyncSender<()>,
        release: mpsc::Receiver<()>,
    ) -> Result<Job, QueryError> {
        self.spawn("WAL", None, None, move |_, _, _| {
            entered.send(()).unwrap();
            release.recv().unwrap();
            Ok(())
        })
    }
    pub(crate) fn test_noop(self: &Arc<Self>) -> Result<Job, QueryError> {
        self.spawn("WAL", None, None, |_, _, cancel| check_cancelled(cancel))
    }
    pub(crate) fn test_has_scheduler(&self) -> bool {
        self.background.lock().unwrap().is_some()
    }
    pub(crate) fn test_begin_drain(&self) -> Result<(u64, u64), QueryError> {
        let _archive = self.archive.lock().unwrap();
        let target = self.capture_target()?;
        let mut state = self.control.lock().unwrap();
        let state = state.as_mut().unwrap();
        state.requested = Policy::Disabled;
        state.lifecycle = "DRAINING".into();
        state.drain = Some((target.lsn, target.time));
        self.save(state)?;
        Ok((target.lsn, target.time))
    }
}

#[cfg(test)]
impl Coordinator {
    pub(crate) fn test_reversed_idle_targets(&self) -> Result<(), QueryError> {
        let _archive = self.archive.lock().unwrap();
        let first = self.capture_target()?;
        let second = self.capture_target()?;
        self.archive_to(second, "", &AtomicBool::new(false))?;
        self.archive_to(first, "", &AtomicBool::new(false))
    }
}

#[cfg(test)]
impl Coordinator {
    pub(crate) fn test_closed_before_lease_release(&self) -> Result<(), QueryError> {
        let mut guard = self.control.lock().unwrap();
        let state = guard.as_mut().unwrap();
        state.effective = Policy::Disabled;
        state.requested = Policy::Disabled;
        state.lifecycle = "CLOSED".into();
        state.drain = None;
        state.close_publication_pending = false;
        self.save(state)
    }
}

#[cfg(test)]
pub(crate) static ARCHIVE_SNAPSHOT_HOOK: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

fn failure_class(message: &str) -> &'static str {
    let message = message.to_ascii_lowercase();
    if message.contains("budget")
        || message.contains("disk space")
        || message.contains("capacity")
        || message.contains("memory")
    {
        "RESOURCE_LIMIT"
    } else if message.contains("checksum")
        || message.contains("mismatch")
        || message.contains("gap")
        || message.contains("corrupt")
    {
        "INTEGRITY"
    } else if message.contains("key") {
        "KEY_UNAVAILABLE"
    } else if message.contains("cancel") {
        "CANCELLED"
    } else if message.contains("obsolete") || message.contains("stream") {
        "STALE_TARGET"
    } else {
        "IO_OR_EXECUTION"
    }
}
