// Page ownership is independent of SQL names and survives backup relocation.
// Classic page containers reserve bytes 40..64 for owner and physical page id.
fn owner_context(path: &Path) -> (PathBuf, String) {
    let relative = crate::storage::naming::wal_relative_object_path(path);
    if !["schemas/", "objects/", "system/"]
        .iter()
        .any(|prefix| relative.starts_with(prefix))
    {
        return (
            path.parent().unwrap_or_else(|| Path::new(".")).to_owned(),
            path.file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned(),
        );
    }
    let mut root = path.to_owned();
    for _ in Path::new(&relative).components() {
        root.pop();
    }
    (root, relative)
}

fn owner_proof_path(root: &Path, relative: &str) -> PathBuf {
    use sha2::{Digest, Sha256};
    root.join("system/object-owners").join(format!(
        "{}.owner",
        crate::util::hex::encode_lower(&Sha256::digest(relative.as_bytes()))
    ))
}

fn page_owner_result(path: &Path) -> Result<[u8; 16], QueryError> {
    use sha2::{Digest, Sha256};
    let (root, relative) = owner_context(path);
    let uuid =
        crate::storage::control::Control::load(&root)?.map_or([0; 16], |control| control.uuid);
    let mut hash = Sha256::new();
    hash.update(b"mydb.page.owner.v1\0");
    hash.update(uuid);
    hash.update(relative.as_bytes());
    let canonical: [u8; 16] = hash.finalize()[..16].try_into().unwrap();
    if uuid == [0; 16] {
        return Ok(canonical);
    }
    let proof = match std::fs::File::open(owner_proof_path(&root, &relative)) {
        Ok(file) => {
            use std::io::Read;
            let mut proof = Vec::with_capacity(85);
            file.take(85).read_to_end(&mut proof).map_err(io_err)?;
            proof
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(canonical),
        Err(err) => return Err(io_err(err)),
    };
    if proof.len() != 84
        || &proof[..8] != b"MYDBOWN1"
        || read_u32(&proof, 8) != crate::storage::format_versions::PAGE_OWNER_PROOF_VERSION
        || proof[12..16] != [0; 4]
        || proof[16..32] != canonical
        || proof[48..64] != uuid
        || proof[64..80] != [0; 16]
        || read_u32(&proof, 80) != crc32fast::hash(&proof[..80])
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "corrupt or incompatible page-owner transfer proof",
        ));
    }
    Ok(proof[32..48].try_into().unwrap())
}

fn page_owner(path: &Path) -> [u8; 16] {
    // File keys retain the error and reject I/O; the sentinel is never trusted.
    page_owner_result(path).unwrap_or([0; 16])
}

fn publish_page_owner(path: &Path, owner: &[u8; 16]) -> std::io::Result<()> {
    use sha2::{Digest, Sha256};
    let (root, relative) = owner_context(path);
    let Some(control) = crate::storage::control::Control::load(&root)
        .map_err(|err| std::io::Error::other(err.to_string()))?
    else {
        return Ok(());
    };
    let mut hash = Sha256::new();
    hash.update(b"mydb.page.owner.v1\0");
    hash.update(control.uuid);
    hash.update(relative.as_bytes());
    let mut proof = [0; 84];
    proof[..8].copy_from_slice(b"MYDBOWN1");
    write_u32(
        &mut proof,
        8,
        crate::storage::format_versions::PAGE_OWNER_PROOF_VERSION,
    );
    proof[16..32].copy_from_slice(&hash.finalize()[..16]);
    proof[32..48].copy_from_slice(owner);
    proof[48..64].copy_from_slice(&control.uuid);
    let checksum = crc32fast::hash(&proof[..80]);
    write_u32(&mut proof, 80, checksum);
    let target = owner_proof_path(&root, &relative);
    fs::create_dir_all(target.parent().unwrap())?;
    if fs::read(&target).ok().as_deref() == Some(&proof) {
        return Ok(());
    }
    crate::repl::durable::atomic_write_sync(&target, &target.with_extension("owner.tmp"), &proof)?;
    crate::platform::fs::sync_path(&root.join("system"))
}

fn transfer_page_owner(source: &Path, target: &Path) -> std::io::Result<()> {
    let name = source.file_name().unwrap_or_default().to_string_lossy();
    if ![".heap", ".idx", ".lob", ".rtree"]
        .iter()
        .any(|suffix| name.ends_with(suffix) || name.contains(&format!("{suffix}.alter-")))
    {
        return Ok(());
    }
    let (root, _) = owner_context(source);
    if crate::storage::control::Control::load(&root)
        .map_err(|e| std::io::Error::other(e.to_string()))?
        .is_none()
    {
        return Ok(());
    }
    let file = std::fs::File::open(source)?;
    if !file.metadata()?.is_file() || file.metadata()?.len() < PAGE_SIZE as u64 {
        return Ok(());
    }
    let mut header = [0; 64];
    use std::io::Read;
    (&file).read_exact(&mut header)?;
    if !matches!(
        read_u32(&header, 0),
        HEAP_PAGE_MAGIC
            | HEAP_META_MAGIC
            | INDEX_PAGE_MAGIC
            | INDEX_META_MAGIC
            | OVFL_MAGIC
            | spatial_index::MAGIC
    ) {
        return Ok(());
    }
    let owner = page_owner_result(source).map_err(|err| std::io::Error::other(err.to_string()))?;
    if header[40..56] != owner {
        return Err(std::io::Error::other(
            "source page-owner mismatch before file transfer",
        ));
    }
    publish_page_owner(target, &owner)
}

fn retire_page_owner_proof(path: &Path) {
    let (root, relative) = owner_context(path);
    let proof = owner_proof_path(&root, &relative);
    match fs::remove_file(&proof) {
        Ok(()) => {
            if let Err(err) = crate::platform::fs::sync_path(proof.parent().unwrap()) {
                crate::util::logging::log_message(format_args!(
                    "page-owner proof cleanup sync pending: {err}"
                ));
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => crate::util::logging::log_message(format_args!(
            "page-owner proof cleanup pending: {err}"
        )),
    }
}

fn with_owner_transfer(
    source: &Path,
    target: &Path,
    operation: impl FnOnce() -> std::io::Result<()>,
) -> std::io::Result<()> {
    let name = source.file_name().unwrap_or_default().to_string_lossy();
    if ![".heap", ".idx", ".lob", ".rtree"]
        .iter()
        .any(|suffix| name.ends_with(suffix) || name.contains(&format!("{suffix}.alter-")))
    {
        return operation();
    }
    let (root, relative) = owner_context(target);
    let proof = owner_proof_path(&root, &relative);
    let previous = match std::fs::File::open(&proof) {
        Ok(file) => {
            use std::io::Read;
            let mut bytes = Vec::with_capacity(85);
            file.take(85).read_to_end(&mut bytes)?;
            if bytes.len() != 84 {
                return Err(std::io::Error::other(
                    "invalid destination owner proof length",
                ));
            }
            Some(bytes)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => return Err(err),
    };
    let result = transfer_page_owner(source, target).and_then(|()| operation());
    if let Err(operation_error) = result {
        // A failed namespace operation must leave an existing destination
        // readable under its original proof. DDL intents cover crash recovery.
        let restore = if let Some(bytes) = previous {
            crate::repl::durable::atomic_write_sync(
                &proof,
                &proof.with_extension("owner.tmp"),
                &bytes,
            )
        } else {
            match fs::remove_file(&proof) {
                Ok(()) => crate::platform::fs::sync_path(proof.parent().unwrap()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err),
            }
        };
        if let Err(restore_error) = restore {
            return Err(std::io::Error::other(format!(
                "{operation_error}; owner-proof rollback failed: {restore_error}"
            )));
        }
        return Err(operation_error);
    }
    Ok(())
}

fn rename_owned_file(source: impl AsRef<Path>, target: impl AsRef<Path>) -> std::io::Result<()> {
    with_owner_transfer(source.as_ref(), target.as_ref(), || {
        fs::rename(source.as_ref(), target.as_ref())
    })?;
    if source.as_ref().try_exists().is_ok_and(|exists| !exists) {
        retire_page_owner_proof(source.as_ref());
    }
    Ok(())
}

fn hard_link_owned_file(source: impl AsRef<Path>, target: impl AsRef<Path>) -> std::io::Result<()> {
    with_owner_transfer(source.as_ref(), target.as_ref(), || {
        fs::hard_link(source.as_ref(), target.as_ref())
    })
}

fn has_classic_identity(bytes: &[u8]) -> bool {
    bytes.len() == PAGE_SIZE
        && matches!(
            read_u32(bytes, 0),
            HEAP_PAGE_MAGIC
                | HEAP_META_MAGIC
                | INDEX_PAGE_MAGIC
                | INDEX_META_MAGIC
                | OVFL_MAGIC
                | spatial_index::MAGIC
        )
}

fn stamp_page_owner(bytes: &mut [u8], owner: &[u8; 16], page_id: u64) {
    if has_classic_identity(bytes) {
        bytes[40..56].copy_from_slice(owner);
        write_u64(bytes, 56, page_id);
    }
}

fn validate_page_owner(bytes: &[u8], owner: &[u8; 16], page_id: u64) -> Result<(), QueryError> {
    if has_classic_identity(bytes) && (&bytes[40..56] != owner || read_u64(bytes, 56) != page_id) {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("persistent page owner or position mismatch at page {page_id}"),
        ));
    }
    Ok(())
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_validate_storage_pages(bytes: &[u8]) {
    if bytes.len() != PAGE_SIZE {
        return;
    }
    let mut image = bytes.to_vec();
    // Exercise structural checks as well as the checksum rejection path.
    refresh_page_checksum(&mut image);
    let _ = validate_heap_page_header(&image);
    let _ = decode_heap_meta(&image, &image[40..56].try_into().unwrap());
    let _ = decode_index_meta(&image);
    let _ = decode_index_tree_page(&image);
    let _ = decode_vector_meta(&image);
    let _ = spatial_index::validate_page(&image);
    let identity = zone_map_page::Identity {
        storage: read_u64(&image, 24),
        generation: read_u64(&image, 32),
        page: read_u64(&image, 40),
    };
    let _ = zone_map_page::View::open(&image, identity);
}

#[cfg(test)]
fn bind_test_file_pages(path: &Path) {
    let (root, _) = owner_context(path);
    crate::storage::control::Control::admit(&root).unwrap();
    let owner = page_owner_result(path).unwrap();
    let mut bytes = fs::read(path).unwrap();
    for (page_id, image) in bytes.chunks_exact_mut(PAGE_SIZE).enumerate() {
        stamp_page_owner(image, &owner, page_id as u64);
        refresh_page_checksum(image);
    }
    fs::write(path, bytes).unwrap();
}

#[cfg(test)]
mod owner_transfer_tests {
    use super::*;
    #[test]
    fn release_format_failed_owner_transfer_preserves_destination() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        crate::storage::control::Control::admit(root.path()).unwrap();
        let a = root.path().join("a.heap");
        let b = root.path().join("b.heap");
        fs::write(&a, encode_heap_meta(&a, 1)).unwrap();
        fs::write(&b, encode_heap_meta(&b, 1)).unwrap();
        let before = page_owner_result(&b).unwrap();
        assert!(with_owner_transfer(&a, &b, || Err(std::io::Error::other(
            "injected rename failure"
        )))
        .is_err());
        assert_eq!(page_owner_result(&b).unwrap(), before);
        let alias = root.path().join("alias.heap");
        hard_link_owned_file(&b, &alias).unwrap();
        let prior = page_owner_result(&alias).unwrap();
        assert!(hard_link_owned_file(&a, &alias).is_err());
        assert_eq!(page_owner_result(&alias).unwrap(), prior);
        rename_owned_file(&alias, &alias).unwrap();
        assert_eq!(page_owner_result(&alias).unwrap(), prior);
    }
}
