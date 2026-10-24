//! Read-only operation, recovery-interval and encryption dependency reports.
use super::repository::{Position, Publication};
use super::*;

#[derive(Debug, Serialize)]
pub(super) struct Interval {
    pub from_lsn: u64,
    pub through_lsn: u64,
    pub from_time: u64,
    pub through_time: u64,
}
#[derive(Debug, Serialize, Default)]
pub(super) struct StreamReport {
    pub stream_id: String,
    pub source_id: String,
    pub timeline_id: String,
    pub intervals: Vec<Interval>,
    pub closed_at_lsn: Option<u64>,
    pub closed_at_time: Option<u64>,
    pub discarded: bool,
}
#[derive(Debug, Serialize)]
pub(super) struct RecoveryCatalog {
    pub repository_id: String,
    pub catalog: Position,
    pub checked_unix_ms: u64,
    pub validation: String,
    pub streams: Vec<StreamReport>,
    pub standalone_bases: Vec<super::repository::Base>,
    #[serde(skip)]
    _memory: ControlledMemoryCharge,
}
struct Piece {
    first: u64,
    last: u64,
    time: u64,
    base: bool,
}

pub(super) fn recoverability(
    path: &Path,
    key: Option<&Path>,
    verify: bool,
) -> Result<RecoveryCatalog, QueryError> {
    let mut pin = super::plan::PinnedCatalog::open(path, key)?;
    if verify {
        if pin.repo.needs_wal_key(&pin.position)? {
            pin.repo.require_key(key)?;
        }
        pin.repo.verify_history()?;
    }
    let mut streams = BTreeMap::<String, StreamReport>::new();
    let mut pieces = BTreeMap::<String, Vec<Piece>>::new();
    let mut standalone = Vec::new();
    let mut memory = ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)?;
    pin.repo.visit_live(&pin.position, |_, event| {
        match event {
            Publication::Base { base } => {
                memory.try_grow(1024)?;
                if verify {
                    pin.repo.base(&base.id)?;
                } else {
                    pin.repo.base_for_open(&base.id)?;
                }
                if let Some(id) = &base.stream_id {
                    let descriptor = pin.repo.stream(id)?;
                    if descriptor.source_id != base.source_id
                        || descriptor.timeline_id != base.timeline_id
                    {
                        return Err(backup_err("base changed stream identity"));
                    }
                    let report = streams.entry(id.clone()).or_default();
                    report.stream_id = id.clone();
                    report.source_id = base.source_id.clone();
                    report.timeline_id = base.timeline_id.clone();
                    pieces.entry(id.clone()).or_default().push(Piece {
                        first: base.source_end_lsn,
                        last: base.source_end_lsn,
                        time: base.source_end_time,
                        base: true,
                    });
                } else {
                    standalone.push(base);
                }
            }
            Publication::Range { range } => {
                super::restore::ValidatedRange::new(&range)?;
                let descriptor = pin.repo.stream(&range.stream_id)?;
                if descriptor.source_id != range.source_id
                    || descriptor.timeline_id != range.timeline_id
                {
                    return Err(backup_err("range changed stream identity"));
                }
                if verify {
                    super::restore::verify_range(&pin.repo, &range)?;
                }
                memory.try_grow(256)?;
                pieces.entry(range.stream_id).or_default().push(Piece {
                    first: range.first_lsn,
                    last: range.last_lsn,
                    time: range.covered_time,
                    base: false,
                });
            }
            Publication::Close {
                stream_id,
                lsn,
                time,
                discarded,
            } => {
                let report = streams.entry(stream_id).or_default();
                if report.closed_at_lsn.is_none() {
                    report.closed_at_lsn = Some(lsn);
                    report.closed_at_time = Some(time);
                    report.discarded = discarded;
                }
            }
            _ => {}
        }
        Ok(())
    })?;
    for (id, mut events) in pieces {
        let report = streams.entry(id.clone()).or_default();
        let descriptor = pin.repo.stream(&id)?;
        report.stream_id = id.clone();
        report.source_id = descriptor.source_id;
        report.timeline_id = descriptor.timeline_id;
        events.sort_unstable_by_key(|p| (p.first, !p.base));
        for event in events.iter().filter(|p| p.base) {
            if report
                .intervals
                .last()
                .is_some_and(|last| last.from_lsn == event.first)
            {
                continue;
            }
            report.intervals.push(Interval {
                from_lsn: event.first,
                through_lsn: event.last,
                from_time: event.time,
                through_time: event.time,
            });
        }
        for event in events.into_iter().filter(|p| !p.base) {
            let index = report
                .intervals
                .partition_point(|p| p.through_lsn.saturating_add(1) < event.first);
            if index == report.intervals.len() || report.intervals[index].from_lsn > event.last {
                continue;
            }
            let last = &mut report.intervals[index];
            last.through_lsn = last.through_lsn.max(event.last);
            last.through_time = last.through_time.max(event.time);
            let mut merged = index + 1;
            while merged < report.intervals.len()
                && report.intervals[merged].from_lsn <= report.intervals[index].through_lsn
            {
                report.intervals[index].through_time = report.intervals[index]
                    .through_time
                    .max(report.intervals[merged].through_time);
                report.intervals[index].through_lsn = report.intervals[index]
                    .through_lsn
                    .max(report.intervals[merged].through_lsn);
                merged += 1;
            }
            report.intervals.drain(index + 1..merged);
        }
        if let Some(fence) = pin.repo.fence_at(&id, &pin.position)? {
            if let Some(last) = report
                .intervals
                .last_mut()
                .filter(|last| last.through_lsn == fence.lsn)
            {
                last.through_time = last.through_time.max(fence.time);
            }
        }
    }
    standalone.sort_unstable_by_key(|b| b.source_end_lsn);
    Ok(RecoveryCatalog {
        repository_id: pin.repo.header.id.clone(),
        catalog: pin.position.clone(),
        checked_unix_ms: now_ms(),
        validation: if verify {
            "BASE_SHA256_AND_WAL_AUTH_CRC"
        } else {
            "CATALOG_AND_IMAGE_METADATA"
        }
        .into(),
        streams: streams.into_values().collect(),
        standalone_bases: standalone,
        _memory: memory,
    })
}

pub(super) fn keys(path: &Path, key: Option<&Path>) -> Result<serde_json::Value, QueryError> {
    let mut pin = super::plan::PinnedCatalog::open(path, None)?;
    let key_error = pin.repo.require_key(key).err().as_ref().map(error_detail);
    let mut ranges = 0u64;
    let mut bases = 0u64;
    let mut bytes = 0u64;
    pin.repo.visit_live(&pin.position, |_, event| {
        match event {
            Publication::Range { range } => {
                ranges += 1;
                bytes = bytes.saturating_add(range.raw_bytes);
            }
            Publication::Base { .. } => bases += 1,
            _ => {}
        }
        Ok(())
    })?;
    Ok(
        serde_json::json!({"repository_id":pin.repo.header.id,"catalog":pin.position,"wal_key_id":pin.repo.header.wal_key_id,"key_fingerprint":pin.repo.header.key_fingerprint,"wal_key_available":pin.repo.key.is_some(),"key_error":key_error,"dependent_ranges":ranges,"dependent_raw_bytes":bytes,"baseline_count":bases,"baseline_encrypted":false,"bundle_contains_plaintext_baseline":true,"rotation":"Use a new repository/key and complete a new full baseline before retiring the previous repository and its key. Retained ranges keep their original key dependency."}),
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckRecord {
    payload: serde_json::Value,
    digest: String,
}
pub(super) fn latest(path: &Path) -> Result<serde_json::Value, QueryError> {
    let pin = super::plan::PinnedCatalog::open(path, None)?;
    let mut result = serde_json::json!({"repository_id":pin.repo.header.id,"catalog":pin.position});
    for kind in ["VERIFY", "RECOVERY_DRILL"] {
        let relative = format!("catalog/diagnostics/{kind}.json");
        reject_symlink_path(&pin.repo.root, Path::new(&relative))?;
        let path = pin.repo.root.join(relative);
        if !path.exists() {
            result[kind] = serde_json::Value::Null;
            continue;
        }
        let input = read_bounded(&path, 65536)?;
        let mut record: CheckRecord =
            serde_json::from_slice(&input).map_err(|e| backup_err(e.to_string()))?;
        if record.payload["version"] != 1
            || record.payload["repository_id"] != pin.repo.header.id
            || record.payload["kind"] != kind
            || sha256_hex(
                &serde_json::to_vec(&record.payload).map_err(|e| backup_err(e.to_string()))?,
            ) != record.digest
        {
            return Err(backup_err("diagnostic record identity/checksum mismatch"));
        }
        record.payload["stale"] = serde_json::json!(
            record.payload["catalog"]
                != serde_json::to_value(&pin.position).map_err(|e| backup_err(e.to_string()))?
        );
        result[kind] = record.payload;
    }
    Ok(result)
}
