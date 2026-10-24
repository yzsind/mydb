//! Checked source-clock watermarks synchronized before fences or WAL reclamation.
use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[derive(Serialize, Deserialize)]
struct Watermark {
    version: u32,
    unix_us: u64,
    digest: String,
}
fn digest(time: u64) -> String {
    crate::util::hex::encode_lower(&Sha256::digest(time.to_le_bytes()))
}
pub(crate) fn read_source_clock(root: &Path) -> Result<u64, QueryError> {
    let path = root.join("system/commit-clock.v1");
    if !path.exists() {
        return Ok(0);
    }
    if fs::symlink_metadata(&path)
        .map_err(io_err)?
        .file_type()
        .is_symlink()
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "source clock cannot be a symlink",
        ));
    }
    let mut bytes = Vec::new();
    File::open(&path)
        .map_err(io_err)?
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(io_err)?;
    if bytes.len() > 4096 {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "source clock exceeds metadata budget",
        ));
    }
    let value: Watermark = serde_json::from_slice(&bytes).map_err(|e| {
        error(
            crate::error::catalog::ER_GET_ERRNO,
            format!("invalid source clock: {e}"),
        )
    })?;
    if value.version != crate::storage::format_versions::COMMIT_CLOCK_VERSION
        || value.unix_us == u64::MAX
        || value.digest != digest(value.unix_us)
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "source clock version/checksum mismatch",
        ));
    }
    Ok(value.unix_us)
}
pub(crate) fn persist_source_clock(root: &Path, time: u64) -> Result<(), QueryError> {
    if time == u64::MAX {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "source clock overflow",
        ));
    }
    let path = root.join("system/commit-clock.v1");
    let parent = path.parent().unwrap();
    fs::create_dir_all(parent).map_err(io_err)?;
    if fs::symlink_metadata(parent)
        .map_err(io_err)?
        .file_type()
        .is_symlink()
    {
        return Err(error(
            crate::error::catalog::ER_GET_ERRNO,
            "source clock parent cannot be a symlink",
        ));
    }
    let old = read_source_clock(root)?;
    if old >= time && path.exists() {
        File::open(&path)
            .and_then(|f| sync_before_directory_publication(&f, &path))
            .map_err(io_err)?;
        crate::platform::fs::sync_path(parent).map_err(io_err)?;
        return Ok(());
    }
    let bytes = serde_json::to_vec(&Watermark {
        version: crate::storage::format_versions::COMMIT_CLOCK_VERSION,
        unix_us: time,
        digest: digest(time),
    })
    .map_err(|e| error(crate::error::catalog::ER_GET_ERRNO, e.to_string()))?;
    let temp = parent.join(format!(".source-clock-{:032x}", rand::random::<u128>()));
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(io_err)?;
        file.write_all(&bytes)
            .and_then(|_| sync_before_directory_publication(&file, &temp))
            .map_err(io_err)?;
    }
    fs::rename(&temp, &path).map_err(io_err)?;
    crate::platform::fs::sync_path(parent).map_err(io_err)
}
