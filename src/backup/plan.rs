//! One fixed catalog and target-selection contract for recovery and export.
use super::repository::{Base, Lease, Position, Publication, Repository};
use super::restore::{Spool, Target};
use super::*;

pub(super) struct PinnedCatalog {
    pub repo: Repository,
    pub position: Position,
    _lease: Lease,
}
impl PinnedCatalog {
    pub fn open(path: &Path, key: Option<&Path>) -> Result<Self, QueryError> {
        let repo = Repository::open(path, key)?;
        let lease = Lease::acquire(&repo.root, false)?;
        let position = repo.position()?;
        Ok(Self {
            repo,
            position,
            _lease: lease,
        })
    }
}
#[derive(Default)]
struct Coverage {
    lsn: u64,
    time: u64,
}

pub(super) struct Selection {
    pub base: Base,
    pub image: native::Snapshot,
    pub fixed_lsn: u64,
    pub covered_time: u64,
    pub exact_base: bool,
}
impl Selection {
    pub fn new(
        repo: &Repository,
        position: &Position,
        base_id: Option<&str>,
        stream: Option<&str>,
        target: Target,
    ) -> Result<Self, QueryError> {
        let mut deleted_bases = BTreeSet::new();
        let mut deleted_ranges = BTreeSet::new();
        let mut branches = BTreeSet::new();
        let mut selected: Option<Base> = None;
        let mut coverage: BTreeMap<String, Coverage> = BTreeMap::new();
        repo.visit_live(&position, |_, event| {
            match event {
                Publication::Prune { bases, ranges } => {
                    deleted_bases.extend(bases);
                    deleted_ranges.extend(ranges);
                    if deleted_bases.len() + deleted_ranges.len() > 100_000 {
                        return Err(backup_err("retention metadata exceeds planning budget"));
                    }
                }
                Publication::Base { base } if !deleted_bases.contains(&base.id) => {
                    if stream.is_some_and(|id| base.stream_id.as_deref() != Some(id))
                        || base_id.is_some_and(|id| base.id != id)
                    {
                        return Ok(());
                    }
                    let branch = base
                        .stream_id
                        .clone()
                        .unwrap_or_else(|| format!("standalone-{}", base.source_id));
                    branches.insert(branch.clone());
                    if branches.len() > 64 {
                        return Err(backup_err("too many branches; specify a stream/base"));
                    }
                    let eligible = match target {
                        Target::Lsn(lsn) => base.source_end_lsn <= lsn,
                        Target::Time(time) => base.source_end_time <= time,
                        _ => true,
                    };
                    // A snapshot after the requested point cannot prove the WAL
                    // interval needed to reach that earlier point from another base.
                    if eligible {
                        let cover = coverage.entry(branch).or_default();
                        cover.lsn = cover.lsn.max(base.source_end_lsn);
                        cover.time = cover.time.max(base.source_end_time);
                    }
                    if eligible
                        && selected
                            .as_ref()
                            .is_none_or(|old| base.source_end_lsn > old.source_end_lsn)
                    {
                        selected = Some(base);
                    }
                }
                Publication::Range { range } if !deleted_ranges.contains(&range.id) => {
                    let cover = coverage.entry(range.stream_id).or_default();
                    cover.lsn = cover.lsn.max(range.last_lsn);
                    cover.time = cover.time.max(range.covered_time);
                }
                Publication::Fence {
                    stream_id,
                    lsn,
                    time,
                } => {
                    let cover = coverage.entry(stream_id).or_default();
                    cover.lsn = cover.lsn.max(lsn);
                    cover.time = cover.time.max(time);
                }
                _ => {}
            }
            Ok(())
        })?;
        if branches.len() > 1 && base_id.is_none() && stream.is_none() {
            return Err(backup_err(
                "restore branch is ambiguous; specify --stream or --base",
            ));
        }
        let base = selected.ok_or_else(|| backup_err("no eligible published base for target"))?;
        let exact_base = target.is_base(&base);
        if let Some(stream) = base.stream_id.as_ref().filter(|_| !exact_base) {
            let descriptor = repo.stream(stream)?;
            if descriptor.source_id != base.source_id || descriptor.timeline_id != base.timeline_id
            {
                return Err(backup_err("base/stream identity mismatch"));
            }
            if let Some(fence) = repo.fence_at(stream, &position)? {
                let cover = coverage.entry(stream.clone()).or_default();
                if fence.lsn > cover.lsn {
                    return Err(backup_err("time fence exceeds committed WAL coverage"));
                }
                cover.time = cover.time.max(fence.time);
            }
        }
        let branch = base
            .stream_id
            .clone()
            .unwrap_or_else(|| format!("standalone-{}", base.source_id));
        let covered = coverage
            .get(&branch)
            .ok_or_else(|| backup_err("missing branch coverage"))?;
        let fixed_lsn = match target {
            Target::Latest => covered.lsn,
            Target::Base => base.source_end_lsn,
            Target::Lsn(lsn) => {
                if lsn > covered.lsn {
                    return Err(backup_err("requested LSN exceeds confirmed coverage"));
                }
                lsn
            }
            Target::Time(_) if exact_base => base.source_end_lsn,
            Target::Time(time) => {
                if time > covered.time {
                    return Err(backup_err("requested time exceeds source time fence"));
                }
                covered.lsn
            }
        };
        if fixed_lsn > base.source_end_lsn && base.stream_id.is_none() {
            return Err(backup_err("standalone base has no archived WAL stream"));
        }
        let (stored, image) = repo.base_for_open(&base.id)?;
        if serde_json::to_vec(&stored).map_err(|e| backup_err(e.to_string()))?
            != serde_json::to_vec(&base).map_err(|e| backup_err(e.to_string()))?
        {
            return Err(backup_err("catalog/base identity mismatch"));
        }
        let covered_time = covered.time;
        Ok(Self {
            base,
            image,
            fixed_lsn,
            covered_time,
            exact_base,
        })
    }
    pub fn ranges(
        &self,
        repo: &Repository,
        position: &Position,
        root: &Path,
    ) -> Result<Spool, QueryError> {
        let mut plan = Spool::new(root)?;
        if self.fixed_lsn > self.base.source_end_lsn {
            repo.visit_live(position, |_, event| {
                if let Publication::Range { range } = event {
                    if Some(&range.stream_id) == self.base.stream_id.as_ref()
                        && range.last_lsn > self.base.source_end_lsn
                        && range.first_lsn <= self.fixed_lsn
                    {
                        if range.source_id != self.base.source_id
                            || range.timeline_id != self.base.timeline_id
                        {
                            return Err(backup_err("WAL branch/source mismatch"));
                        }
                        plan.push(&range)?;
                    }
                }
                Ok(())
            })?;
        }
        Ok(plan)
    }
}

#[derive(Debug, Serialize)]
pub(super) struct ResolvedTarget {
    pub end: u64,
    pub record_time: u64,
    pub frames: u64,
    pub raw_bytes: u64,
}
impl Selection {
    pub fn resolve(
        &self,
        repo: &Repository,
        plan: &mut Spool,
        target: Target,
        mut accept: impl FnMut(&[u8]) -> Result<(), QueryError>,
    ) -> Result<ResolvedTarget, QueryError> {
        let base = &self.base;
        let fixed_lsn = self.fixed_lsn;
        let mut frames = 0;
        let mut raw_bytes = 0;
        let mut end = base.source_end_lsn;
        let mut time = base.source_end_time;
        let mut prefix_end = base.source_end_lsn;
        let mut frame_time = base.source_end_time;
        let mut reached_time = false;
        plan.chronological(|range| {
            if reached_time {
                return Ok(());
            }
            super::restore::visit_range(&repo, &range, |first, frame, raw| {
                if frame.header.lsn <= base.source_end_lsn {
                    return Ok(());
                }
                if first != prefix_end + 1 {
                    return Err(backup_err(format!(
                        "restore dependencies contain a WAL gap/overlap: expected {}, found {}",
                        prefix_end + 1,
                        first
                    )));
                }
                if frame.header.source_time_us <= frame_time {
                    return Err(backup_err("source decision time regressed across ranges"));
                }
                frame_time = frame.header.source_time_us;
                prefix_end = frame.header.lsn;
                if first <= fixed_lsn && frame.header.lsn > fixed_lsn {
                    return Err(backup_err("LSN target splits a physical frame/TxnBatch"));
                }
                if frame.header.lsn <= fixed_lsn
                    && match target {
                        Target::Time(t) => frame.header.source_time_us <= t,
                        _ => true,
                    }
                {
                    end = frame.header.lsn;
                    time = frame.header.source_time_us;
                    accept(raw)?;
                    frames += 1;
                    raw_bytes += raw.len() as u64;
                } else if matches!(target, Target::Time(_)) {
                    reached_time = true;
                }
                Ok(())
            })
        })?;
        if !matches!(target, Target::Time(_)) && end != fixed_lsn {
            return Err(backup_err(format!(
                "target LSN {} is not covered by actual WAL records; reached {}",
                fixed_lsn, end
            )));
        }
        Ok(ResolvedTarget {
            end,
            record_time: time,
            frames,
            raw_bytes,
        })
    }
}

#[derive(Debug, Serialize)]
pub(super) struct PreflightReport {
    pub repository_id: String,
    pub catalog: Position,
    pub source_id: String,
    pub timeline_id: String,
    pub stream_id: Option<String>,
    pub base_id: String,
    pub base_end_lsn: u64,
    pub requested_target: String,
    pub actual_source_lsn: u64,
    pub actual_source_record_time: u64,
    pub confirmed_time_fence: u64,
    pub prepared_xa: usize,
    pub base_bytes: u64,
    pub dependency_ranges: u64,
    pub dependency_digest: String,
    pub wal_frames: u64,
    pub wal_bytes: u64,
    pub minimum_work_bytes: u64,
    pub maximum_work_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
    pub validation: String,
    pub wal_key_id: Option<String>,
    pub baseline_encrypted: bool,
    pub exact_base: bool,
    pub warnings: Vec<String>,
}
pub(super) fn preflight(
    repository: &Path,
    base: Option<&str>,
    stream: Option<&str>,
    target: Target,
    destination: &Path,
    key: Option<&Path>,
) -> Result<PreflightReport, QueryError> {
    let mut pin = PinnedCatalog::open(repository, key)?;
    let destination = resolved_destination(destination)?;
    super::restore::validate_destination(&pin.repo.root, &destination)?;
    let selection = Selection::new(&pin.repo, &pin.position, base, stream, target)?;
    // This command changes neither repository nor destination. All disposable
    // planning files belong to the process's separate scratch tree.
    #[cfg(not(windows))]
    let scratch_root = PathBuf::from("/tmp/mydb");
    #[cfg(windows)]
    let scratch_root = std::env::temp_dir().join("mydb");
    fs::create_dir_all(&scratch_root).map_err(|e| io_err("create planning scratch root", e))?;
    let scratch = ScratchDirectory::new(&scratch_root, "recovery-plan")?;
    if selection.fixed_lsn > selection.base.source_end_lsn {
        pin.repo.require_key(key)?;
    }
    pin.repo.base(&selection.base.id)?;
    let mut ranges = selection.ranges(&pin.repo, &pin.position, &scratch.0)?;
    let dependency_ranges = ranges.len();
    let resolved = selection.resolve(&pin.repo, &mut ranges, target, |_| Ok(()))?;
    let base_bytes = selection
        .image
        .manifest
        .files
        .iter()
        .try_fold(0u64, |n, f| {
            n.checked_add(f.len)
                .ok_or_else(|| backup_err("base size overflow"))
        })?;
    let mut parent = destination.as_path();
    while !parent.is_dir() {
        parent = parent
            .parent()
            .ok_or_else(|| backup_err("no destination ancestor"))?;
    }
    Ok(PreflightReport {
        repository_id:pin.repo.header.id.clone(),catalog:pin.position.clone(),source_id:selection.base.source_id.clone(),timeline_id:selection.base.timeline_id.clone(),stream_id:selection.base.stream_id.clone(),base_id:selection.base.id.clone(),base_end_lsn:selection.base.source_end_lsn,
        requested_target:format!("{target:?}"),actual_source_lsn:resolved.end,actual_source_record_time:resolved.record_time,
        confirmed_time_fence:match target { Target::Time(t)=>t,Target::Latest=>selection.covered_time,_=>resolved.record_time },
        prepared_xa:selection.image.seal.as_ref().map_or(0,|s|s.visibility_state.prepared.len()),
        base_bytes,dependency_ranges,dependency_digest:ranges.digest(),wal_frames:resolved.frames,wal_bytes:resolved.raw_bytes,
        minimum_work_bytes:base_bytes.saturating_add(resolved.raw_bytes),maximum_work_bytes:None,
        available_bytes:crate::engine::executor::spill_quota::available_bytes(parent),validation:"BASE_SHA256_AND_WAL_AUTH_CRC".into(),
        wal_key_id:pin.repo.header.wal_key_id.clone(),baseline_encrypted:false,exact_base:selection.exact_base,
        warnings:vec!["Recovery, undo and index rebuilding can grow beyond the minimum space estimate; free space is not a reservation.".into()],
    })
}
