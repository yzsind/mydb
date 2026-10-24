//! Coverage-aware deletion plans and bounded, self-contained repository bundles.
use super::repository::{Base, Lease, Publication, Range, Repository};
use super::restore::Target;
use super::*;
use std::io::Seek;
fn json<T: Serialize>(value: &T) -> Result<Vec<u8>, QueryError> {
    serde_json::to_vec(value).map_err(|e| backup_err(e.to_string()))
}

pub(crate) fn prune(
    path: &Path,
    stream: Option<&str>,
    window: Option<&str>,
    apply: bool,
    key_file: Option<&Path>,
) -> Result<(), QueryError> {
    let mut repo = Repository::open(path, key_file)?;
    repo.require_key(key_file)?;
    let _lease = Lease::acquire(&repo.root, apply)?;
    let root = repo.position()?;
    repo.verify_coverage(&root)?;
    let duration = window.map(retention_duration).transpose()?;
    let mut bases = Vec::new();
    let mut upper: BTreeMap<String, u64> = BTreeMap::new();
    let mut latest_ranges = BTreeMap::new();
    repo.visit_live(&root, |_, event| {
        match event {
            Publication::Base { base } => {
                repo.base(&base.id)?;
                if bases.len() >= 100_000 {
                    return Err(backup_err("base GC metadata budget exceeded"));
                }
                if let Some(id) = &base.stream_id {
                    upper
                        .entry(id.clone())
                        .and_modify(|t| *t = (*t).max(base.source_end_time))
                        .or_insert(base.source_end_time);
                }
                bases.push(base);
            }
            Publication::Range { range } => {
                super::restore::verify_range(&repo, &range)?;
                upper
                    .entry(range.stream_id.clone())
                    .and_modify(|t| *t = (*t).max(range.covered_time))
                    .or_insert(range.covered_time);
                latest_ranges
                    .entry(range.stream_id.clone())
                    .or_insert(range.id);
            }
            Publication::Fence {
                stream_id, time, ..
            } => {
                upper
                    .entry(stream_id)
                    .and_modify(|t| *t = (*t).max(time))
                    .or_insert(time);
            }
            _ => {}
        }
        Ok(())
    })?;
    for (stream, time) in &mut upper {
        if let Some(fence) = repo.fence_at(stream, &root)? {
            *time = (*time).max(fence.time);
        }
    }
    let selected = if duration.is_some() {
        if let Some(id) = stream {
            if !upper.contains_key(id) {
                return Err(backup_err("unknown retention stream"));
            }
            Some(id.to_string())
        } else if upper.len() == 1 {
            upper.keys().next().cloned()
        } else {
            return Err(backup_err("retention requires an unambiguous --stream"));
        }
    } else {
        None
    };
    let start = selected
        .as_ref()
        .zip(duration)
        .map(|(id, window)| upper[id].saturating_sub(window));
    let predecessor = selected.as_ref().zip(start).and_then(|(id, start)| {
        bases
            .iter()
            .filter(|base| base.stream_id.as_ref() == Some(id) && base.source_end_time <= start)
            .max_by_key(|base| base.source_end_lsn)
            .map(|base| base.id.clone())
    });
    let mut deleted_bases = Vec::new();
    let mut floors: BTreeMap<String, u64> = BTreeMap::new();
    for base in &bases {
        let remove = start.is_some_and(|start| {
            base.stream_id.as_ref() == selected.as_ref()
                && base.source_end_time < start
                && predecessor.as_ref() != Some(&base.id)
        });
        if remove {
            deleted_bases.push(base.id.clone());
        } else if let Some(id) = &base.stream_id {
            floors
                .entry(id.clone())
                .and_modify(|floor| *floor = (*floor).min(base.source_end_lsn))
                .or_insert(base.source_end_lsn);
        }
    }
    let mut deleted_ranges = Vec::new();
    let mut referenced = BTreeSet::new();
    let mut metadata =
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)?;
    repo.visit_live(&root, |_, event| {
        if let Publication::Range { range } = event {
            let remove = selected.as_ref() == Some(&range.stream_id)
                && range.last_lsn <= *floors.get(&range.stream_id).unwrap_or(&0)
                && latest_ranges.get(&range.stream_id) != Some(&range.id);
            if remove {
                deleted_ranges.push((range.stream_id, range.id));
            } else {
                for chunk in range.chunks {
                    if referenced.insert(chunk.hash) {
                        metadata.try_grow(160)?;
                    }
                }
            }
        }
        Ok(())
    })?;
    if deleted_bases.len() + deleted_ranges.len() > 100_000 {
        return Err(backup_err(
            "retention deletion plan exceeds metadata budget",
        ));
    }
    let mut orphan_blobs = Vec::new();
    let namespace = repo
        .root
        .join("blobs")
        .join(repo.header.wal_key_id.as_deref().unwrap_or("plain"));
    if namespace.exists() {
        for entry in fs::read_dir(&namespace).map_err(|e| io_err("enumerate WAL blobs", e))? {
            let entry = entry.map_err(|e| io_err("read WAL blob entry", e))?;
            let id = entry
                .file_name()
                .to_str()
                .ok_or_else(|| backup_err("non-UTF8 WAL blob"))?
                .to_string();
            let temporary = super::gc::temporary_blob_name(&id);
            if !temporary {
                validate_hash(&id)?;
            }
            let ty = entry
                .file_type()
                .map_err(|e| io_err("inspect WAL blob", e))?;
            if ty.is_symlink() {
                return Err(backup_err("symlink WAL blob"));
            }
            if !ty.is_file()
                || (temporary
                    && entry
                        .metadata()
                        .map_err(|e| io_err("inspect temporary blob", e))?
                        .len()
                        > (CHUNK_BYTES + 1024) as u64)
            {
                return Err(backup_err("invalid temporary/physical blob object"));
            }
            if !referenced.contains(&id) {
                metadata.try_grow(160)?;
                orphan_blobs.push(entry.path());
            }
        }
    }
    let mut garbage = super::gc::candidates(&repo, &root, &referenced)?;
    for id in &deleted_bases {
        garbage.insert(format!("bases/{id}"))?;
    }
    for (stream, id) in &deleted_ranges {
        garbage.insert(format!("streams/{stream}/ranges/{id}"))?;
    }
    for path in &orphan_blobs {
        garbage.insert(
            path.strip_prefix(&repo.root)
                .map_err(|_| backup_err("GC path escaped repository"))?
                .to_string_lossy()
                .replace('\\', "/"),
        )?;
    }
    println!("{}",serde_json::to_string_pretty(&serde_json::json!({"catalog_generation":root.generation,"apply":apply,"requested_window_start":start,"actual_history_start":bases.iter().filter(|b|b.stream_id.as_ref()==selected.as_ref()&&!deleted_bases.contains(&b.id)).map(|b|b.source_end_time).min(),"delete_bases":deleted_bases,"delete_ranges":deleted_ranges,"orphan_wal_blobs":orphan_blobs.len()})).map_err(|e|backup_err(e.to_string()))?);
    if apply {
        if !deleted_bases.is_empty() || !deleted_ranges.is_empty() {
            repo.publish(Publication::Prune {
                bases: deleted_bases.clone(),
                ranges: deleted_ranges.iter().map(|(_, id)| id.clone()).collect(),
            })?;
        }
        super::gc::sweep(&repo, garbage)?;
        repo.gc_history_locked()?;
        DirectoryOperation::cleanup(&repo.root.join("staging/range-work"), "range-work")?;
    }
    Ok(())
}
const BUNDLE: &[u8; 16] = super::bundle::MAGIC;
fn safe_path(path: &str) -> Result<(), QueryError> {
    if path.split('/').count() > 64
        || path.is_empty()
        || path.len() > 4096
        || path.contains(['\\', ':', '\0'])
        || Path::new(path).is_absolute()
        || path
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
    {
        return Err(backup_err("unsafe bundle path"));
    }
    if !["repository.json", "gc.lock", "publish.lock"].contains(&path)
        && !["catalog/", "bases/", "streams/", "blobs/"]
            .iter()
            .any(|prefix| path.starts_with(prefix))
    {
        return Err(backup_err("unknown bundle object class"));
    }
    Ok(())
}
fn walk(
    root: &Path,
    path: &Path,
    visit: &mut impl FnMut(String, PathBuf) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    for entry in fs::read_dir(path).map_err(|e| io_err("enumerate bundle files", e))? {
        let entry = entry.map_err(|e| io_err("read bundle entry", e))?;
        let ty = entry
            .file_type()
            .map_err(|e| io_err("inspect bundle entry", e))?;
        // Runtime workspaces and their stable guard are never bundle roots.
        if path == root && entry.file_name() == "staging" {
            if !ty.is_dir() {
                return Err(backup_err("invalid repository staging directory"));
            }
            continue;
        }
        if ty.is_symlink() {
            return Err(backup_err("symlink in bundle tree"));
        }
        if ty.is_dir() {
            walk(root, &entry.path(), visit)?;
        } else if ty.is_file() {
            let relative = entry
                .path()
                .strip_prefix(root)
                .unwrap()
                .to_str()
                .ok_or_else(|| backup_err("invalid bundle filename"))?
                .replace('\\', "/");
            visit(relative, entry.path())?;
        } else {
            return Err(backup_err("unsupported bundle object"));
        }
    }
    Ok(())
}
fn append_file(
    output: &mut super::bundle::Writer,
    relative: &str,
    path: &Path,
    write_root: &Path,
) -> Result<(), QueryError> {
    append_checked_file(output, relative, path, write_root, None)
}
fn append_checked_file(
    output: &mut super::bundle::Writer,
    relative: &str,
    path: &Path,
    write_root: &Path,
    expected: Option<&BackupFile>,
) -> Result<(), QueryError> {
    safe_path(relative)?;
    let mut input = std::fs::File::open(path).map_err(|e| io_err("open export file", e))?;
    let length = input
        .metadata()
        .map_err(|e| io_err("inspect export file", e))?
        .len();
    output
        .write_all(&(relative.len() as u32).to_le_bytes())
        .and_then(|_| output.write_all(relative.as_bytes()))
        .and_then(|_| output.write_all(&length.to_le_bytes()))
        .map_err(|e| io_err("write export header", e))?;
    let digest = output.copy_payload(&mut input, length, write_root, expected)?;
    output.objects += 1;
    output
        .write_all(&digest)
        .map_err(|e| io_err("write bundle checksum", e))
}
fn append_bytes(
    output: &mut super::bundle::Writer,
    relative: &str,
    bytes: &[u8],
    write_root: &Path,
) -> Result<(), QueryError> {
    safe_path(relative)?;
    output
        .write_all(&(relative.len() as u32).to_le_bytes())
        .and_then(|_| output.write_all(relative.as_bytes()))
        .and_then(|_| output.write_all(&(bytes.len() as u64).to_le_bytes()))
        .map_err(|e| io_err("write export header", e))?;
    output.write_payload(bytes, write_root)?;
    output.objects += 1;
    output
        .write_all(&Sha256::digest(bytes))
        .map_err(|e| io_err("write bundle checksum", e))
}
pub(crate) fn export(
    path: &Path,
    base_id: Option<&str>,
    stream: Option<&str>,
    target: Target,
    output: &Path,
    key_file: Option<&Path>,
) -> Result<(), QueryError> {
    let mut source = Repository::open(path, key_file)?;
    let _lease = Lease::acquire(&source.root, false)?;
    let position = source.position()?;
    reject_overlap(&source.root, output)?;
    if output.exists() {
        return Err(backup_err("export destination already exists"));
    }
    let selection = super::plan::Selection::new(&source, &position, base_id, stream, target)?;
    let base = selection.base.clone();
    let end = selection.fixed_lsn;
    let covered_time = selection.covered_time;
    let parent = output
        .parent()
        .ok_or_else(|| backup_err("export path has no parent"))?;
    create_directory_durable(parent)?;
    let scratch = DirectoryOperation::new(output, "export-work")?;
    let root = scratch.data.join("repository");
    // This owned, unpublished scaffold is disposable. Only the completed bundle
    // is durably published; syncing every empty scaffold directory adds no proof.
    fs::create_dir_all(&root).map_err(|e| io_err("create export scaffold", e))?;
    for name in [
        "bases",
        "streams",
        "blobs",
        "staging",
        "catalog/generations",
        "catalog/checkpoints",
        "catalog/history/packs",
        "catalog/history/indexes",
    ] {
        fs::create_dir_all(root.join(name)).map_err(|e| io_err("create export namespace", e))?;
    }
    fs::create_dir_all(
        &root
            .join("blobs")
            .join(source.header.wal_key_id.as_deref().unwrap_or("plain")),
    )
    .map_err(|e| io_err("create private blob namespace", e))?;
    for name in ["gc.lock", "publish.lock"] {
        write_sync(&root.join(name), b"")?;
    }
    let mut private_header = source.header.clone();
    private_header.format = "MYDB_PRIVATE_EXPORT/1".into();
    write_sync(&root.join("repository.json"), &json(&private_header)?)?;
    write_sync(
        &root.join("catalog/CURRENT"),
        &json(&super::repository::Position::default())?,
    )?;
    if end > base.source_end_lsn {
        source.require_key(key_file)?;
    }
    let export = Repository {
        root: root.clone(),
        header: source.header.clone(),
        key: source.key.clone(),
        history_index_cache: Mutex::new(None),
        history_pack_cache: Mutex::new(None),
    };
    let base_dir = root.join("bases").join(&base.id);
    fs::create_dir_all(&base_dir).map_err(|e| io_err("create exported base", e))?;
    for name in ["manifest", "seal", "commit"] {
        fs::copy(
            source.root.join("bases").join(&base.id).join(name),
            base_dir.join(name),
        )
        .map_err(|e| io_err("copy export metadata", e))?;
    }
    if let Some(stream) = &base.stream_id {
        export.register_stream(&source.stream(stream)?)?;
    }
    export.publish(Publication::Base { base: base.clone() })?;
    let mut needed = selection.ranges(&source, &position, &scratch.data)?;
    let mut expected = base.source_end_lsn + 1;
    let mut actual = base.source_end_lsn;
    let mut frame_time = base.source_end_time;
    let mut source_blobs = BTreeMap::<String, u64>::new();
    let mut reference_memory =
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)?;
    needed.chronological(|range| {
        super::restore::ValidatedRange::new(&range)?;
        if range.source_id != base.source_id || range.timeline_id != base.timeline_id {
            return Err(backup_err("export range changed source/timeline"));
        }
        let whole = range.first_lsn > base.source_end_lsn
            && range.last_lsn <= end
            && !matches!(target, Target::Time(t) if range.covered_time > t);
        if whole {
            // Validate every frame, then stream the original immutable encoded
            // objects into the bundle. Header identity and AEAD binding stay unchanged.
            super::restore::visit_range(&source, &range, |first, frame, _| {
                if first != expected || frame.header.source_time_us <= frame_time {
                    return Err(backup_err("export range continuity/time mismatch"));
                }
                expected = frame.header.lsn + 1;
                actual = frame.header.lsn;
                frame_time = frame.header.source_time_us;
                Ok(())
            })?;
            for chunk in &range.chunks {
                if !source_blobs.contains_key(&chunk.hash) {
                    reference_memory.try_grow(192)?;
                    source_blobs.insert(chunk.hash.clone(), chunk.len);
                }
            }
            export.publish_range(range)?;
            return Ok(());
        }
        let mut chunks = Vec::new();
        let mut buffer = Vec::with_capacity(CHUNK_BYTES);
        let mut first = 0;
        let mut last = 0;
        let mut time = 0;
        let mut count = 0;
        let mut bytes = 0;
        let mut source_spans: Vec<super::repository::SourceSpan> = Vec::new();
        let mut source_index = 0usize;
        let mut source_offset = super::restore::ValidatedRange::new(&range)?.first_offset();
        super::restore::visit_range(&source, &range, |start, frame, raw| {
            let original = &range.source_spans[source_index];
            let frame_offset = source_offset;
            let frame_end = source_offset + raw.len() as u64;
            source_offset = frame_end;
            if frame.header.lsn == original.last_lsn {
                source_index += 1;
                if let Some(next) = range.source_spans.get(source_index) {
                    source_offset = next.start_offset;
                }
            }

            if frame.header.lsn <= base.source_end_lsn {
                return Ok(());
            }
            if start != expected {
                return Err(backup_err("export dependencies contain an LSN gap"));
            }
            if frame.header.source_time_us <= frame_time {
                return Err(backup_err("export source time regressed"));
            }
            frame_time = frame.header.source_time_us;
            expected = frame.header.lsn + 1;
            if start <= end && frame.header.lsn > end {
                return Err(backup_err("export LSN splits a frame"));
            }
            if frame.header.lsn > end
                || matches!(target,Target::Time(t)if frame.header.source_time_us>t)
            {
                return Ok(());
            }
            if source_spans
                .last()
                .is_some_and(|span| span.segment_id == original.segment_id)
            {
                let span = source_spans.last_mut().unwrap();
                span.last_lsn = frame.header.lsn;
                span.end_offset = frame_end;
            } else {
                source_spans.push(super::repository::SourceSpan {
                    segment_id: original.segment_id,
                    first_lsn: start,
                    last_lsn: frame.header.lsn,
                    start_offset: frame_offset,
                    end_offset: frame_end,
                    captured_valid_len: original.captured_valid_len,
                });
            }
            if first == 0 {
                first = start;
            }
            last = frame.header.lsn;
            actual = last;
            time = frame.header.source_time_us;
            count += 1;
            bytes += raw.len() as u64;
            let mut raw = raw;
            while !raw.is_empty() {
                let take = (CHUNK_BYTES - buffer.len()).min(raw.len());
                buffer.extend_from_slice(&raw[..take]);
                raw = &raw[take..];
                if buffer.len() == CHUNK_BYTES {
                    chunks.push(export.store_blob(&buffer)?);
                    buffer.clear();
                }
            }
            Ok(())
        })?;
        if first != 0 {
            if !buffer.is_empty() {
                chunks.push(export.store_blob(&buffer)?);
            }
            export.publish_range(Range {
                id: random_uuid(),
                stream_id: range.stream_id,
                source_id: range.source_id,
                timeline_id: range.timeline_id,
                first_lsn: first,
                last_lsn: last,
                covered_time: time,
                frame_count: count,
                raw_bytes: bytes,
                source_spans,
                chunks,
            })?;
        }
        Ok(())
    })?;
    if !matches!(target, Target::Time(_)) && actual != end {
        return Err(backup_err("export target is not covered"));
    }
    if let Some(stream) = &base.stream_id {
        let fence_time = match target {
            Target::Time(time) => Some(time),
            Target::Latest => Some(covered_time),
            _ => None,
        };
        if let Some(time) = fence_time {
            export.publish_fence(stream, actual, time)?;
        }
    }
    let file_path = scratch.data.join("bundle");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&file_path)
        .map_err(|e| io_err("create bundle", e))?;
    let mut file = super::bundle::Writer::new(file)?;
    file.write_all(BUNDLE)
        .map_err(|e| io_err("write bundle magic", e))?;
    walk(&root, &root, &mut |relative, path| {
        if relative == "repository.json" {
            append_bytes(&mut file, &relative, &json(&source.header)?, &scratch.data)
        } else {
            append_file(&mut file, &relative, &path, &scratch.data)
        }
    })?;
    for (hash, len) in source_blobs {
        let reference = ChunkRef { hash, len };
        let private = export.encoded_blob_path(&reference)?;
        if !private.exists() {
            let relative = relative_path(&export.root, &private)?;
            append_file(
                &mut file,
                &relative,
                &source.encoded_blob_path(&reference)?,
                &scratch.data,
            )?;
        }
    }
    let source_data = source.root.join("bases").join(&base.id).join("data");
    let _manifest_memory = ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::SystemMetadata,
        selection
            .image
            .manifest
            .files
            .len()
            .checked_mul(64)
            .ok_or_else(|| backup_err("export manifest index overflow"))?,
    )?;
    let expected = selection
        .image
        .manifest
        .files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect::<HashMap<_, _>>();
    walk(&source_data, &source_data, &mut |relative, path| {
        append_checked_file(
            &mut file,
            &format!("bases/{}/data/{relative}", base.id),
            &path,
            &scratch.data,
            expected.get(relative.as_str()).copied(),
        )
    })?;
    file.finish()?;
    fs::hard_link(&file_path, output).map_err(|e| io_err("publish bundle without overwrite", e))?;
    sync_dir(parent)?;
    println!("exported base {} through source LSN {actual}", base.id);
    Ok(())
}
pub(crate) fn import(
    destination: &Path,
    input: &Path,
    key_file: Option<&Path>,
) -> Result<(), QueryError> {
    import_with_resume(destination, input, key_file, false)
}
pub(crate) fn import_with_resume(
    destination: &Path,
    input: &Path,
    key_file: Option<&Path>,
    resume: bool,
) -> Result<(), QueryError> {
    let destination = resolved_destination(destination)?;
    let parent = destination
        .parent()
        .ok_or_else(|| backup_err("import target has no parent"))?;
    create_directory_durable(parent)?;
    if destination.exists()
        && fs::read_dir(&destination)
            .map_err(|e| io_err("inspect import destination", e))?
            .next()
            .is_some()
    {
        return Err(backup_err("import destination exists; verify/confirm a visible completed installation before retrying"));
    }
    let file = std::fs::File::open(input).map_err(|e| io_err("open import bundle", e))?;
    let mut source = super::bundle::Reader::new(file)?;
    super::resources::check_spool(source.input_len)?;
    let scratch = if resume {
        DirectoryOperation::resume(&destination, "import-work", source.identity.clone())?
    } else {
        DirectoryOperation::new(&destination, "import-work")?
    };
    let root = scratch.data.join("repository");
    if !root.exists() {
        fs::create_dir(&root).map_err(|e| io_err("create import staging", e))?;
    }
    reject_symlink_path(&scratch.data, Path::new("repository"))?;
    let mut magic = [0; 16];
    source
        .read_exact(&mut magic)
        .map_err(|e| io_err("read bundle magic", e))?;
    if &magic != BUNDLE {
        return Err(backup_err("unsupported bundle format"));
    }
    let _charge =
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, CHUNK_BYTES)?;
    let mut buffer = vec![0; CHUNK_BYTES];
    let mut files = 0;
    let mut seen = BTreeSet::new();
    let mut inventory_memory =
        ControlledMemoryCharge::try_new(ControlledMemoryCategory::SystemMetadata, 0)?;
    let _resume_memory = ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::StorageScratch,
        if resume { CHUNK_BYTES } else { 0 },
    )?;
    let mut previous = if resume {
        vec![0; CHUNK_BYTES]
    } else {
        Vec::new()
    };
    loop {
        let mut size = [0; 4];
        source
            .read_exact(&mut size)
            .map_err(|e| io_err("read bundle entry", e))?;
        let length = u32::from_le_bytes(size) as usize;
        if length == 0 {
            break;
        }
        if length > 4096 {
            return Err(backup_err("bundle path exceeds limit"));
        }
        let mut bytes = vec![0; length];
        source
            .read_exact(&mut bytes)
            .map_err(|e| io_err("read bundle path", e))?;
        let relative =
            String::from_utf8(bytes).map_err(|_| backup_err("invalid bundle UTF8 path"))?;
        safe_path(&relative)?;
        inventory_memory.try_grow(relative.len() + 192)?;
        if !seen.insert(relative.clone()) {
            return Err(backup_err("duplicate bundle object path"));
        }
        let mut length = [0; 8];
        source
            .read_exact(&mut length)
            .map_err(|e| io_err("read bundle length", e))?;
        let mut left = u64::from_le_bytes(length);
        let data_file =
            relative.starts_with("bases/") && relative.split('/').nth(2) == Some("data");
        let max = if relative.starts_with("blobs/") {
            (CHUNK_BYTES + 1024) as u64
        } else {
            MAX_METADATA_BYTES as u64
        };
        if !data_file && left > max {
            return Err(backup_err("bundle object length exceeds its class limit"));
        }
        let remaining = source.remaining()?;
        if left > remaining.saturating_sub(32) {
            return Err(backup_err("bundle file length exceeds remaining input"));
        }
        let target = root.join(&relative);
        fs::create_dir_all(target.parent().unwrap())
            .map_err(|e| io_err("create import path", e))?;
        reject_symlink_path(&root, Path::new(&relative))?;
        let (mut output, _) = open_private_output(&target, resume, resume)?;
        let total = left;
        let mut offset = 0u64;
        let mut hash = Sha256::new();
        while left > 0 {
            let take = left.min(CHUNK_BYTES as u64) as usize;
            super::resources::throttle(take as u64)?;
            source
                .read_payload_exact(&mut buffer[..take])
                .map_err(|e| io_err("read bundle content", e))?;
            hash.update(&buffer[..take]);
            let identical = if resume {
                output
                    .seek(std::io::SeekFrom::Start(offset))
                    .map_err(|e| io_err("seek resumed block", e))?;
                match output.read_exact(&mut previous[..take]) {
                    Ok(()) => previous[..take] == buffer[..take],
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => false,
                    Err(error) => return Err(io_err("verify resumed block", error)),
                }
            } else {
                false
            };
            if !identical {
                let _disk = reserve_disk_write(&root, take as u64)?;
                output
                    .seek(std::io::SeekFrom::Start(offset))
                    .and_then(|_| output.write_all(&buffer[..take]))
                    .map_err(|e| io_err("write import content", e))?;
            }
            offset += take as u64;
            left -= take as u64;
            #[cfg(test)]
            super::faults::hit("import_block_written", &destination)?;
        }
        let mut digest = [0; 32];
        source
            .read_exact(&mut digest)
            .map_err(|e| io_err("read bundle digest", e))?;
        if hash.finalize().as_slice() != digest {
            return Err(backup_err("bundle file checksum mismatch"));
        }
        output
            .set_len(total)
            .map_err(|e| io_err("finish imported file length", e))?;
        // Resume rechecks every block after interruption. The final sync_tree
        // makes every file and namespace durable before the directory publish.
        files += 1;
        if files > 1_000_000 {
            return Err(backup_err("bundle file count exceeds budget"));
        }
    }
    source.finish(files)?;
    walk(&root, &root, &mut |relative, _| {
        if !seen.contains(&relative) {
            return Err(backup_err("unknown file in resumed operation"));
        }
        Ok(())
    })?;
    if root.join("staging").exists()
        && fs::read_dir(root.join("staging"))
            .map_err(|e| io_err("inspect resumed staging", e))?
            .next()
            .is_some()
    {
        return Err(backup_err("unknown runtime payload in resumed import"));
    }
    for directory in [
        "bases",
        "streams",
        "blobs",
        "staging",
        "catalog/generations",
        "catalog/checkpoints",
        "catalog/history/packs",
        "catalog/history/indexes",
    ] {
        fs::create_dir_all(root.join(directory))
            .map_err(|e| io_err("create import namespace", e))?;
    }
    let header: super::repository::Header =
        serde_json::from_slice(&read_bounded(&root.join("repository.json"), 4096)?)
            .map_err(|e| backup_err(e.to_string()))?;
    let namespace = header.wal_key_id.as_deref().unwrap_or("plain");
    validate_id(namespace)?;
    fs::create_dir_all(root.join("blobs").join(namespace))
        .map_err(|e| io_err("create import blob namespace", e))?;
    let mut imported = Repository::open(&root, key_file)?;
    let position = imported.position()?;
    if imported.needs_wal_key(&position)? {
        imported.require_key(key_file)?;
    }
    imported.verify_coverage(&position)?;
    imported.visit_live(&position, |_, event| {
        match event {
            Publication::Base { base } => {
                imported.base(&base.id)?;
            }
            Publication::Range { range } => super::restore::verify_range(&imported, &range)?,
            _ => {}
        }
        Ok(())
    })?;
    if destination.exists()
        && fs::read_dir(&destination)
            .map_err(|e| io_err("inspect import target", e))?
            .next()
            .is_some()
    {
        return Err(backup_err(
            "import requires an empty destination; existing repositories are never overwritten",
        ));
    }
    if destination.exists() {
        fs::remove_dir(&destination).map_err(|e| io_err("remove empty import target", e))?;
    }
    sync_tree(&root)?;
    native::rename_new_directory(&root, &destination)?;
    sync_dir(parent)?;
    scratch.finish();
    println!("imported {files} verified files");
    Ok(())
}

fn retention_duration(value: &str) -> Result<u64, QueryError> {
    let unit = match value.as_bytes().last() {
        Some(b's') => 1,
        Some(b'h') => 3600,
        Some(b'd') => 86400,
        _ => return Err(backup_err("retention window requires s/h/d suffix")),
    };
    let count = value[..value.len() - 1]
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| backup_err("invalid retention window"))?;
    count
        .checked_mul(unit)
        .and_then(|n| n.checked_mul(1_000_000))
        .ok_or_else(|| backup_err("retention window overflow"))
}

pub(crate) fn export_window(
    path: &Path,
    stream: Option<&str>,
    window: &str,
    output: &Path,
    key_file: Option<&Path>,
) -> Result<(), QueryError> {
    let duration = retention_duration(window)?;
    let repo = Repository::open(path, key_file)?;
    let _lease = Lease::acquire(&repo.root, false)?;
    let position = repo.position()?;
    let mut upper: BTreeMap<String, u64> = BTreeMap::new();
    repo.visit_live(&position, |_, event| {
        let value = match event {
            Publication::Base { base } => base.stream_id.map(|id| (id, base.source_end_time)),
            Publication::Range { range } => Some((range.stream_id, range.covered_time)),
            _ => None,
        };
        if let Some((id, time)) = value {
            if stream.is_none_or(|selected| id == selected) {
                upper
                    .entry(id)
                    .and_modify(|n| *n = (*n).max(time))
                    .or_insert(time);
                if upper.len() > 64 {
                    return Err(backup_err("export window branch budget exceeded"));
                }
            }
        }
        Ok(())
    })?;
    if upper.len() != 1 {
        return Err(backup_err("export window requires one eligible --stream"));
    }
    let (stream, mut end) = upper.into_iter().next().unwrap();
    if let Some(fence) = repo.fence_at(&stream, &position)? {
        end = end.max(fence.time);
    }
    let start = end.saturating_sub(duration);
    let mut predecessor: Option<Base> = None;
    let mut earliest: Option<Base> = None;
    repo.visit_live(&position, |_, event| {
        if let Publication::Base { base } = event {
            if base.stream_id.as_deref() == Some(&stream) {
                if earliest
                    .as_ref()
                    .is_none_or(|old| base.source_end_time < old.source_end_time)
                {
                    earliest = Some(base.clone());
                }
                if base.source_end_time <= start
                    && predecessor
                        .as_ref()
                        .is_none_or(|old| base.source_end_time > old.source_end_time)
                {
                    predecessor = Some(base);
                }
            }
        }
        Ok(())
    })?;
    let base = predecessor
        .or(earliest)
        .ok_or_else(|| backup_err("export window has no baseline"))?;
    export(
        path,
        Some(&base.id),
        Some(&stream),
        Target::Time(end),
        output,
        key_file,
    )?;
    println!(
        "window requested from {start} through {end}; actual baseline begins at {}",
        base.source_end_time
    );
    Ok(())
}
