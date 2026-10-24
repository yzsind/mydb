//! Bounded range validation and source-domain continuation in a private image.
use super::repository::{Base, Range, Repository};
use super::*;
use std::io::{Seek, SeekFrom};

pub(crate) struct BlobReader<'a> {
    repo: &'a Repository,
    chunks: std::slice::Iter<'a, ChunkRef>,
    buffer: Vec<u8>,
    offset: usize,
}
impl<'a> BlobReader<'a> {
    fn new(repo: &'a Repository, range: &'a Range) -> Self {
        Self {
            repo,
            chunks: range.chunks.iter(),
            buffer: Vec::new(),
            offset: 0,
        }
    }
}
impl Read for BlobReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.offset == self.buffer.len() {
            let Some(chunk) = self.chunks.next() else {
                return Ok(0);
            };
            self.buffer = self
                .repo
                .read_blob(chunk)
                .map_err(|e| std::io::Error::other(error_detail(&e)))?;
            self.offset = 0;
        }
        let size = out.len().min(self.buffer.len() - self.offset);
        out[..size].copy_from_slice(&self.buffer[self.offset..self.offset + size]);
        self.offset += size;
        Ok(size)
    }
}
/// Structural proof shared by all consumers before indexing range metadata.
pub(crate) struct ValidatedRange<'a>(&'a Range);
impl<'a> ValidatedRange<'a> {
    pub(crate) fn new(range: &'a Range) -> Result<Self, QueryError> {
        validate_id(&range.id)?;
        validate_id(&range.stream_id)?;
        if range.first_lsn == 0
            || range.last_lsn < range.first_lsn
            || range.last_lsn == u64::MAX
            || range.covered_time == u64::MAX
            || range.frame_count == 0
        {
            return Err(backup_err("invalid WAL range bounds"));
        }
        if range.source_spans.is_empty() || range.source_spans.len() > 512 {
            return Err(backup_err("invalid WAL source span count"));
        }
        let mut span_lsn = range.first_lsn;
        let mut span_bytes = 0u64;
        let mut segment = 0;
        for span in &range.source_spans {
            if span.segment_id <= segment
                || span.first_lsn != span_lsn
                || span.last_lsn < span.first_lsn
                || span.start_offset < crate::wal::WAL_HEADER_LEN as u64
                || span.end_offset <= span.start_offset
                || span.end_offset > span.captured_valid_len
            {
                return Err(backup_err("invalid WAL source generation/offset proof"));
            }
            segment = span.segment_id;
            span_lsn = span
                .last_lsn
                .checked_add(1)
                .ok_or_else(|| backup_err("source span overflow"))?;
            span_bytes = span_bytes
                .checked_add(span.end_offset - span.start_offset)
                .ok_or_else(|| backup_err("source span size overflow"))?;
        }
        if span_lsn != range.last_lsn + 1 || span_bytes != range.raw_bytes {
            return Err(backup_err("WAL source spans disagree with range coverage"));
        }
        validate_id(&range.source_id)?;
        validate_id(&range.timeline_id)?;
        let mut bytes = 0u64;
        for chunk in &range.chunks {
            validate_hash(&chunk.hash)?;
            if chunk.len == 0 || chunk.len > CHUNK_BYTES as u64 {
                return Err(backup_err("invalid WAL chunk size"));
            }
            bytes = bytes
                .checked_add(chunk.len)
                .ok_or_else(|| backup_err("range size overflow"))?;
        }
        if bytes != range.raw_bytes || bytes == 0 {
            return Err(backup_err("WAL chunks disagree with range size"));
        }
        Ok(Self(range))
    }
    pub(crate) fn first_offset(&self) -> u64 {
        self.0.source_spans[0].start_offset
    }
}

pub(crate) fn visit_range(
    repo: &Repository,
    range: &Range,
    mut visit: impl FnMut(u64, &crate::wal::ReplicationFrame, &[u8]) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    let _validated = ValidatedRange::new(range)?;
    let mut span_index = 0usize;
    let mut span_offset = range.source_spans[0].start_offset;
    let path = repo
        .root
        .join("streams")
        .join(&range.stream_id)
        .join("ranges")
        .join(&range.id);
    reject_symlink_path(&repo.root, path.strip_prefix(&repo.root).unwrap())?;
    let manifest = read_bounded(&path.join("manifest"), MAX_METADATA_BYTES)?;
    let commit: String = serde_json::from_slice(&read_bounded(&path.join("commit"), 4096)?)
        .map_err(|e| backup_err(e.to_string()))?;
    if sha256_hex(&manifest) != commit
        || serde_json::to_vec(range).map_err(|e| backup_err(e.to_string()))? != manifest
    {
        return Err(backup_err("WAL range commit mismatch"));
    }
    let _buffer_charge =
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, CHUNK_BYTES * 3)?;
    let mut input = BlobReader::new(repo, range);
    let mut expected = range.first_lsn;
    let mut count = 0;
    let mut total = 0;
    let mut previous_time = 0;
    let mut memory = ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, 0)?;
    let mut bytes = Vec::new();
    loop {
        let mut header = [0; crate::wal::WAL_FRAME_HEADER_LEN];
        let n = input
            .read(&mut header[..1])
            .map_err(|e| io_err("read WAL range", e))?;
        if n == 0 {
            break;
        }
        input
            .read_exact(&mut header[1..])
            .map_err(|e| io_err("read range frame header", e))?;
        let payload = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let size = header
            .len()
            .checked_add(payload)
            .ok_or_else(|| backup_err("WAL frame size overflow"))?;
        let required = size
            .checked_mul(32)
            .ok_or_else(|| backup_err("WAL decoding budget overflow"))?;
        memory.try_grow(required.saturating_sub(memory.bytes()))?;
        bytes.resize(size, 0);
        bytes[..header.len()].copy_from_slice(&header);
        input
            .read_exact(&mut bytes[header.len()..])
            .map_err(|e| io_err("read range payload", e))?;
        let frame = crate::wal::decode_frame_for_replication(&bytes)?;
        super::resources::io()
            .repository_frame_decodes
            .fetch_add(1, Ordering::Relaxed);
        let span = match &frame.payload {
            crate::wal::WalPayload::TxnBatch { records } => records.len() as u64,
            _ => 1,
        };
        let first = frame
            .header
            .lsn
            .checked_add(1)
            .and_then(|n| n.checked_sub(span))
            .ok_or_else(|| backup_err("invalid range frame span"))?;
        if first != expected || frame.header.lsn > range.last_lsn {
            return Err(backup_err("WAL range gap/overlap or forged coverage"));
        }
        if frame.header.source_time_us == 0 || frame.header.source_time_us <= previous_time {
            return Err(backup_err("unordered/missing source frame time"));
        }
        previous_time = frame.header.source_time_us;
        let span = range
            .source_spans
            .get(span_index)
            .ok_or_else(|| backup_err("WAL frame exceeds source spans"))?;
        span_offset = span_offset
            .checked_add(size as u64)
            .ok_or_else(|| backup_err("source offset overflow"))?;
        if frame.header.lsn > span.last_lsn || span_offset > span.end_offset {
            return Err(backup_err("WAL frame splits a source generation"));
        }
        if frame.header.lsn == span.last_lsn {
            if span_offset != span.end_offset {
                return Err(backup_err("WAL source offset does not end at a frame"));
            }
            span_index += 1;
            if let Some(next) = range.source_spans.get(span_index) {
                span_offset = next.start_offset;
            }
        }
        visit(first, &frame, &bytes)?;
        count += 1;
        total += size as u64;
        expected = frame.header.lsn + 1;
    }
    if expected != range.last_lsn + 1
        || count != range.frame_count
        || total != range.raw_bytes
        || previous_time > range.covered_time
    {
        return Err(backup_err("WAL range does not prove its declared coverage"));
    }
    Ok(())
}
pub(crate) fn verify_range(repo: &Repository, range: &Range) -> Result<(), QueryError> {
    visit_range(repo, range, |_, _, _| Ok(()))
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Target {
    Latest,
    Lsn(u64),
    Time(u64),
    Base,
}
impl Target {
    pub(crate) fn is_base(self, base: &Base) -> bool {
        match self {
            Self::Base => true,
            Self::Lsn(lsn) => lsn == base.source_end_lsn,
            Self::Time(time) => time == base.source_end_time,
            Self::Latest => false,
        }
    }
}
#[derive(Debug, Serialize)]
pub(crate) struct Report {
    pub base_id: String,
    pub stream_id: Option<String>,
    pub base_end: u64,
    pub restored_source_lsn: u64,
    pub source_time: u64,
    pub local_checkpoint: u64,
    pub read_only: bool,
    pub catalog_generation: u64,
    pub catalog_digest: String,
    pub source_id: String,
    pub timeline_id: String,
    pub requested_target: String,
    pub actual_source_record_time: u64,
    pub operation_id: String,
    pub dependency_digest: String,
    pub prepared_xa: usize,
    pub validation: String,
    pub image_version: u32,
    pub reused_base_bytes: u64,
    pub process_io: serde_json::Value,
}
pub(super) struct Spool {
    data: std::fs::File,
    index: std::fs::File,
    count: u64,
    digest: Sha256,
}
impl Spool {
    pub(super) fn digest(&self) -> String {
        encode_hex(&self.digest.clone().finalize())
    }
    fn storage_bytes(&self) -> Result<u64, QueryError> {
        self.data
            .metadata()
            .and_then(|data| {
                self.index
                    .metadata()
                    .map(|index| data.len().saturating_add(index.len()))
            })
            .map_err(|error| io_err("inspect restore plan size", error))
    }
    pub(super) fn len(&self) -> u64 {
        self.count
    }
    pub(super) fn new(root: &Path) -> Result<Self, QueryError> {
        Ok(Self {
            data: std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(root.join("ranges.plan"))
                .map_err(|e| io_err("create restore plan", e))?,
            index: std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(root.join("ranges.index"))
                .map_err(|e| io_err("create plan index", e))?,
            count: 0,
            digest: Sha256::new(),
        })
    }
    pub(super) fn push(&mut self, range: &Range) -> Result<(), QueryError> {
        let bytes = serde_json::to_vec(range).map_err(|e| backup_err(e.to_string()))?;
        if bytes.len() > MAX_METADATA_BYTES {
            return Err(backup_err("range metadata exceeds limit"));
        }
        let offset = self
            .data
            .stream_position()
            .map_err(|e| io_err("index restore plan", e))?;
        super::resources::check_spool(
            offset
                .saturating_add(bytes.len() as u64)
                .saturating_add(4)
                .saturating_add((self.count + 1).saturating_mul(8)),
        )?;
        self.index
            .write_all(&offset.to_le_bytes())
            .map_err(|e| io_err("write plan index", e))?;
        self.data
            .write_all(&(bytes.len() as u64).to_le_bytes())
            .and_then(|_| self.data.write_all(&bytes))
            .map_err(|e| io_err("write restore plan", e))?;
        self.digest.update((bytes.len() as u64).to_le_bytes());
        self.digest.update(&bytes);
        self.count += 1;
        Ok(())
    }
    pub(super) fn chronological(
        &mut self,
        mut visit: impl FnMut(Range) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        for i in (0..self.count).rev() {
            self.index
                .seek(SeekFrom::Start(i * 8))
                .map_err(|e| io_err("read plan index", e))?;
            let mut bytes = [0; 8];
            self.index
                .read_exact(&mut bytes)
                .map_err(|e| io_err("read plan offset", e))?;
            self.data
                .seek(SeekFrom::Start(u64::from_le_bytes(bytes)))
                .map_err(|e| io_err("seek restore plan", e))?;
            self.data
                .read_exact(&mut bytes)
                .map_err(|e| io_err("read plan length", e))?;
            let len = u64::from_le_bytes(bytes);
            if len > MAX_METADATA_BYTES as u64 {
                return Err(backup_err("invalid plan size"));
            }
            let _memory = ControlledMemoryCharge::try_new(
                ControlledMemoryCategory::SystemMetadata,
                (len as usize).saturating_mul(16),
            )?;
            let mut bytes = vec![0; len as usize];
            self.data
                .read_exact(&mut bytes)
                .map_err(|e| io_err("read restore plan", e))?;
            visit(serde_json::from_slice(&bytes).map_err(|e| backup_err(e.to_string()))?)?;
        }
        Ok(())
    }
}
pub(crate) fn restore(
    repository: &Path,
    base_id: Option<&str>,
    stream: Option<&str>,
    target: Target,
    destination: &Path,
    key_file: Option<&Path>,
    writable: bool,
) -> Result<Report, QueryError> {
    restore_with_resume(
        repository,
        base_id,
        stream,
        target,
        destination,
        key_file,
        writable,
        false,
    )
}
pub(crate) fn restore_with_resume(
    repository: &Path,
    base_id: Option<&str>,
    stream: Option<&str>,
    target: Target,
    destination: &Path,
    key_file: Option<&Path>,
    writable: bool,
    resume: bool,
) -> Result<Report, QueryError> {
    if resume && (base_id.is_none() || matches!(target, Target::Latest)) {
        return Err(backup_err(
            "restore --resume requires --base and a fixed base/LSN/time target",
        ));
    }
    let mut pinned = super::plan::PinnedCatalog::open(repository, key_file)?;
    let position = pinned.position.clone();
    let repo = &mut pinned.repo;
    let destination = resolved_destination(destination)?;
    validate_destination(&repo.root, &destination)?;
    let selection = super::plan::Selection::new(&repo, &position, base_id, stream, target)?;
    let base = &selection.base;
    let fixed_lsn = selection.fixed_lsn;
    let parent = destination
        .parent()
        .ok_or_else(|| backup_err("restore target has no parent"))?;
    create_directory_durable(parent)?;
    let identity = sha256_hex(
        &serde_json::to_vec(&(
            repo.header.id.clone(),
            base,
            format!("{target:?}"),
            writable,
        ))
        .map_err(|e| backup_err(e.to_string()))?,
    );
    let work = if resume {
        DirectoryOperation::resume(&destination, "restore-work", identity)?
    } else {
        DirectoryOperation::new(&destination, "restore-work")?
    };
    if resume {
        for name in ["ranges.plan", "ranges.index", "verified.frames"] {
            let path = work.data.join(name);
            reject_symlink_path(&work.data, Path::new(name))?;
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(io_err("discard previous verified plan", error)),
            }
        }
        let image = work.data.join("image");
        reject_symlink_path(&work.data, Path::new("image"))?;
        if image.exists() {
            fs::remove_dir_all(&image)
                .map_err(|e| io_err("discard interrupted recovery image", e))?;
        }
    }
    if fixed_lsn > base.source_end_lsn {
        repo.require_key(key_file)?;
    }
    let mut plan = selection.ranges(&repo, &position, &work.data)?;
    let base_bytes = selection
        .image
        .manifest
        .files
        .iter()
        .try_fold(0u64, |bytes, file| {
            bytes
                .checked_add(file.len)
                .ok_or_else(|| backup_err("restore size overflow"))
        })?;
    let plan_bytes = plan.storage_bytes()?;
    let fixed_bytes = base_bytes
        .checked_mul(if resume { 2 } else { 1 })
        .and_then(|bytes| bytes.checked_add(plan_bytes))
        .ok_or_else(|| backup_err("restore size overflow"))?;
    super::resources::check_spool(fixed_bytes)?;
    let mut frames = VerifiedFrames::new(&work.data, fixed_bytes)?;
    let resolved = selection.resolve(&repo, &mut plan, target, |raw| frames.push(raw))?;
    let end = resolved.end;
    let time = resolved.record_time;
    let source = repo.root.join("bases").join(&base.id).join("data");
    let mut reused = 0;
    let cached = if resume {
        let cache = work.data.join("base-cache");
        reused = native::cache_base(&source, &cache)?;
        Some(cache)
    } else {
        None
    };
    #[cfg(test)]
    super::faults::hit("restore_base_cached", &destination)?;
    let image_dir = work.data.join("image");
    let image_time = match target {
        Target::Time(t) => t,
        Target::Latest => selection.covered_time,
        _ => time,
    };
    let image = native::restore_image(
        cached.as_deref().unwrap_or(&source),
        &image_dir,
        end,
        image_time,
        writable,
        |log| {
            frames.consume(|frame, bytes| {
                let first = frame.header.lsn + 1
                    - match &frame.payload {
                        crate::wal::WalPayload::TxnBatch { records } => records.len() as u64,
                        _ => 1,
                    };
                if first != log.next_lsn() {
                    return Err(backup_err("source continuation gap"));
                }
                log.append_replication_frame(bytes, frame)?;
                Ok(())
            })
        },
    )?;
    if destination.exists() {
        fs::remove_dir(&destination).map_err(|e| io_err("remove empty restore target", e))?;
    }
    native::rename_new_directory(&image_dir, &destination)?;
    #[cfg(test)]
    super::faults::hit("restore_installed", &destination)?;
    sync_dir(parent).map_err(|error| backup_err(format!("complete restored image is visible at {}; publication durability is uncertain: {}; use mydb-backup confirm --data-dir PATH", destination.display(), error_detail(&error))))?;
    let report = Report {
        base_id: base.id.clone(),
        stream_id: base.stream_id.clone(),
        base_end: base.source_end_lsn,
        restored_source_lsn: end,
        source_time: image_time,
        local_checkpoint: image.seal.as_ref().unwrap().image_checkpoint,
        read_only: !writable,
        catalog_generation: position.generation,
        catalog_digest: position.digest.clone(),
        source_id: base.source_id.clone(),
        timeline_id: base.timeline_id.clone(),
        requested_target: format!("{target:?}"),
        actual_source_record_time: time,
        operation_id: work.id.clone(),
        dependency_digest: plan.digest(),
        prepared_xa: image
            .seal
            .as_ref()
            .map_or(0, |s| s.visibility_state.prepared.len()),
        validation: "COPIED_BASE_SHA256_AND_AUTHENTICATED_WAL".into(),
        image_version: image.version,
        reused_base_bytes: reused,
        process_io: super::resources::io_snapshot(),
    };
    work.finish();
    Ok(report)
}

/// An authenticated private spool avoids a second read/decrypt of repository
/// objects. Its in-memory digest binds record order, lengths and bytes.
struct VerifiedFrames {
    file: std::fs::File,
    digest: Sha256,
    count: u64,
    bytes: u64,
    fixed_bytes: u64,
    write_buffer: Vec<u8>,
    _io_memory: ControlledMemoryCharge,
    disk: crate::engine::executor::spill_quota::SpillCharger,
}
impl VerifiedFrames {
    fn new(root: &Path, fixed_bytes: u64) -> Result<Self, QueryError> {
        let io_memory =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, CHUNK_BYTES)?;
        let disk = crate::engine::executor::spill_quota::charger_for_new_dir(root)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(root.join("verified.frames"))
            .map_err(|e| io_err("create verified frame spool", e))?;
        Ok(Self {
            file,
            digest: Sha256::new(),
            count: 0,
            bytes: 0,
            fixed_bytes,
            write_buffer: Vec::with_capacity(CHUNK_BYTES),
            _io_memory: io_memory,
            disk,
        })
    }
    fn flush_buffer(&mut self) -> Result<(), QueryError> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }
        // Reserve immediately before each physical batch, including short writes.
        let _pending = self
            .disk
            .reserve_physical_write(self.write_buffer.len() as u64)?;
        let mut bytes = self.write_buffer.as_slice();
        while !bytes.is_empty() {
            super::resources::io()
                .spool_write_calls
                .fetch_add(1, Ordering::Relaxed);
            match self.file.write(bytes) {
                Ok(0) => {
                    return Err(io_err(
                        "write verified spool",
                        std::io::Error::from(std::io::ErrorKind::WriteZero),
                    ))
                }
                Ok(count) => bytes = &bytes[count..],
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(io_err("write verified spool", error)),
            }
        }
        self.write_buffer.clear();
        Ok(())
    }
    fn write_buffered(&mut self, mut bytes: &[u8]) -> Result<(), QueryError> {
        while !bytes.is_empty() {
            if self.write_buffer.len() == CHUNK_BYTES {
                self.flush_buffer()?;
            }
            let count = bytes.len().min(CHUNK_BYTES - self.write_buffer.len());
            self.write_buffer.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
        }
        Ok(())
    }
    fn push(&mut self, raw: &[u8]) -> Result<(), QueryError> {
        // The raw prefix exists both in this spool and in the recovery WAL.
        // Include the planned image/cache and metadata before writing the spool.
        let required = self
            .bytes
            .checked_add(raw.len() as u64)
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add((self.count + 1).checked_mul(8)?))
            .and_then(|bytes| bytes.checked_add(self.fixed_bytes))
            .ok_or_else(|| backup_err("restore spool size overflow"))?;
        super::resources::check_spool(required)?;
        super::resources::throttle(raw.len() as u64)?;
        let length = (raw.len() as u64).to_le_bytes();
        self.write_buffered(&length)?;
        self.write_buffered(raw)?;
        self.digest.update(length);
        self.digest.update(raw);
        self.count += 1;
        self.bytes += raw.len() as u64;
        Ok(())
    }
    fn consume(
        &mut self,
        mut visit: impl FnMut(&crate::wal::ReplicationFrame, &[u8]) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        self.flush_buffer()?;
        // Release the write allocation before allocating the read buffer. The
        // same memory reservation covers both mutually exclusive phases.
        self.write_buffer = Vec::new();
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|e| io_err("rewind verified WAL", e))?;
        struct CountedRead<'a>(&'a mut std::fs::File);
        impl Read for CountedRead<'_> {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                super::resources::io()
                    .spool_read_calls
                    .fetch_add(1, Ordering::Relaxed);
                self.0.read(bytes)
            }
        }
        let mut reader =
            std::io::BufReader::with_capacity(CHUNK_BYTES, CountedRead(&mut self.file));
        let mut digest = Sha256::new();
        let mut buffer = Vec::new();
        let mut memory =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, 0)?;
        for _ in 0..self.count {
            let mut length = [0; 8];
            reader
                .read_exact(&mut length)
                .map_err(|e| io_err("read verified WAL length", e))?;
            let size = u64::from_le_bytes(length);
            if size < crate::wal::WAL_FRAME_HEADER_LEN as u64 || size > self.bytes {
                return Err(backup_err("invalid verified frame length"));
            }
            let size =
                usize::try_from(size).map_err(|_| backup_err("verified frame length overflow"))?;
            let required = size
                .checked_mul(32)
                .ok_or_else(|| backup_err("verified frame budget overflow"))?;
            memory.try_grow(required.saturating_sub(memory.bytes()))?;
            buffer.resize(size, 0);
            reader
                .read_exact(&mut buffer)
                .map_err(|e| io_err("read verified WAL", e))?;
            digest.update(length);
            digest.update(&buffer);
            let frame = crate::wal::decode_frame_for_replication(&buffer)?;
            super::resources::io()
                .spool_frame_decodes
                .fetch_add(1, Ordering::Relaxed);
            visit(&frame, &buffer)?;
        }
        let mut extra = [0];
        if reader
            .read(&mut extra)
            .map_err(|e| io_err("check verified WAL EOF", e))?
            != 0
            || digest.finalize() != self.digest.clone().finalize()
        {
            return Err(backup_err("authenticated frame spool changed"));
        }
        Ok(())
    }
}

pub(super) fn validate_destination(
    repository: &Path,
    destination: &Path,
) -> Result<(), QueryError> {
    reject_overlap(repository, destination)?;
    if destination.exists()
        && fs::read_dir(&destination)
            .map_err(|e| io_err("inspect restore target", e))?
            .next()
            .is_some()
    {
        return Err(backup_err("restore target must be empty or absent"));
    }
    for parent in destination.ancestors().skip(1) {
        if parent.join("schemas").is_dir()
            && parent.join("objects").is_dir()
            && parent.join("system").is_dir()
        {
            return Err(backup_err("restore target is inside a database directory"));
        }
    }
    Ok(())
}
