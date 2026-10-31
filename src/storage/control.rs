//! Data-directory identity and the pre-recovery format admission boundary.
use std::{
    fs,
    io::{self, Read},
    path::Path,
};

use rand::RngCore;
use sha2::{Digest, Sha256};

use super::{error, format_versions as versions};
use crate::types::QueryError;

pub(crate) const PATH: &str = "system/storage.control";
const SIZE: usize = 128;
const MAGIC: &[u8; 8] = b"MYDBCTL2";

#[derive(Clone, Copy, Debug)]
pub(crate) struct Control {
    pub uuid: [u8; 16],
    pub initialized: bool,
}

pub(crate) fn uuid_for_system_file(path: &Path) -> Result<Option<[u8; 16]>, QueryError> {
    for directory in path.ancestors().skip(1) {
        if directory.file_name().is_some_and(|name| name == "system") {
            if let Some(root) = directory.parent() {
                return Control::load(root).map(|control| control.map(|control| control.uuid));
            }
        }
    }
    Ok(None)
}

fn corrupt(message: impl std::fmt::Display) -> QueryError {
    error(
        crate::error::catalog::ER_GET_ERRNO,
        format!("storage control: {message}"),
    )
}

impl Control {
    fn encode(self) -> [u8; SIZE] {
        let mut bytes = [0; SIZE];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..12].copy_from_slice(&versions::STORAGE_CONTROL_VERSION.to_le_bytes());
        bytes[12..16].copy_from_slice(&(SIZE as u32).to_le_bytes());
        bytes[16..20].copy_from_slice(&16384u32.to_le_bytes());
        bytes[20..24].copy_from_slice(&u32::from(self.initialized).to_le_bytes());
        // Reader/writer required-feature masks at 24..40 are initially zero.
        bytes[40..56].copy_from_slice(&self.uuid);
        bytes[56..88]
            .copy_from_slice(&Sha256::digest(versions::native_replication_compatibility()));
        bytes[88..92].copy_from_slice(&versions::CATALOG_VERSION.to_le_bytes());
        let checksum = crc32fast::hash(&bytes[..124]);
        bytes[124..].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    pub(crate) fn load(root: &Path) -> Result<Option<Self>, QueryError> {
        let file = match std::fs::File::open(root.join(PATH)) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(corrupt(err)),
        };
        let mut bytes = Vec::with_capacity(SIZE + 1);
        file.take(SIZE as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(corrupt)?;
        Self::decode(&bytes).map(Some)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, QueryError> {
        if bytes.len() != SIZE || &bytes[..8] != MAGIC {
            return Err(corrupt("invalid length or magic"));
        }
        let uuid: [u8; 16] = bytes[40..56].try_into().unwrap();
        let value = Self {
            uuid,
            initialized: bytes[20] == 1,
        };
        if uuid == [0; 16] || value.encode().as_slice() != bytes {
            return Err(corrupt(
                "unsupported format/features or invalid checksum; rebuild development data",
            ));
        }
        Ok(value)
    }

    fn persist(self, root: &Path) -> Result<(), QueryError> {
        fs::create_dir_all(root.join("system")).map_err(corrupt)?;
        let path = root.join(PATH);
        super::super::repl::durable::atomic_write_sync(
            &path,
            &path.with_extension("control.tmp"),
            &self.encode(),
        )
        .map_err(corrupt)?;
        crate::platform::fs::sync_path(root).map_err(corrupt)?;
        if let Some(parent) = root
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            crate::platform::fs::sync_path(parent).map_err(corrupt)?;
        }
        Ok(())
    }

    pub(crate) fn admit(root: &Path) -> Result<Self, QueryError> {
        if let Some(value) = Self::load(root)? {
            return Ok(value);
        }
        // Empty service directories and node-local replication configuration
        // can precede initial storage creation. Existing database bytes cannot.
        for name in [
            "objects",
            "schemas",
            "system/catalog",
            "system/wal",
            "system/undo",
            "system/object-owners",
        ] {
            if contains_file(&root.join(name)).map_err(corrupt)? {
                return Err(corrupt(
                    "missing control file in an existing data directory; rebuild development data",
                ));
            }
        }
        for name in ["system/commit-ledger.v1", "system/history-index.v1"] {
            if root.join(name).try_exists().map_err(corrupt)? {
                return Err(corrupt(
                    "missing control file with existing temporal metadata",
                ));
            }
        }
        let mut uuid = [0; 16];
        rand::rngs::OsRng.fill_bytes(&mut uuid);
        let value = Self {
            uuid,
            initialized: false,
        };
        value.persist(root)?;
        Ok(value)
    }

    pub(crate) fn finish_initialization(root: &Path) -> Result<(), QueryError> {
        let mut value =
            Self::load(root)?.ok_or_else(|| corrupt("missing initialization identity"))?;
        if !value.initialized {
            value.initialized = true;
            value.persist(root)?;
        }
        Ok(())
    }
}

fn contains_file(path: &Path) -> io::Result<bool> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() || contains_file(&entry.path())? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn release_format_golden_control_file() {
        let bytes = Control {
            uuid: [0xa5; 16],
            initialized: true,
        }
        .encode();
        crate::storage::format_golden::golden!("control", &bytes);
    }

    #[test]
    fn release_format_control_rejects_missing_corrupt_and_future_roots() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let value = Control::admit(dir.path()).unwrap();
        assert_ne!(value.uuid, [0; 16]);
        Control::finish_initialization(dir.path()).unwrap();
        let original = fs::read(dir.path().join(PATH)).unwrap();
        for offset in [8, 16, 24, 32, 40, 56, 124] {
            let mut bytes = original.clone();
            bytes[offset] ^= 1;
            fs::write(dir.path().join(PATH), &bytes).unwrap();
            assert!(Control::admit(dir.path()).is_err());
            assert_eq!(fs::read(dir.path().join(PATH)).unwrap(), bytes);
        }
        fs::remove_file(dir.path().join(PATH)).unwrap();
        fs::create_dir_all(dir.path().join("objects/tables/1")).unwrap();
        fs::write(dir.path().join("objects/tables/1/1.heap"), b"existing").unwrap();
        assert!(Control::admit(dir.path()).is_err());
    }
}
