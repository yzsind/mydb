//! Versioned JSON envelopes for small state files and relay-log lines.
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::{self, Write};

pub(crate) const PROGRESS: u16 = 1;
pub(crate) const SLOT: u16 = 2;
pub(crate) const CHANNEL: u16 = 3;
pub(crate) const RELAY: u16 = 4;
pub(crate) const UNDO_MANIFEST: u16 = 5;
pub(crate) const ROLE: u16 = 6;
fn limit(kind: u16) -> usize {
    // A 1 GiB MySQL row event can expand under JSON byte-array escaping.
    if kind == RELAY || kind == UNDO_MANIFEST {
        (8u64 * 1024 * 1024 * 1024).min((usize::MAX - 256) as u64) as usize
    } else {
        64 * 1024 * 1024
    }
}

fn crc(kind: u16, version: u16) -> crc32fast::Hasher {
    let mut crc = crc32fast::Hasher::new();
    crc.update(b"MYDBMETA1\0");
    crc.update(&kind.to_le_bytes());
    crc.update(&version.to_le_bytes());
    crc.update(&0u64.to_le_bytes());
    crc
}

struct PayloadWriter<'a, W> {
    out: &'a mut W,
    crc: crc32fast::Hasher,
    len: usize,
    limit: usize,
}
impl<W: Write> Write for PayloadWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.len) {
            return Err(io::Error::other("persistent metadata exceeds format limit"));
        }
        let n = self.out.write(bytes)?;
        self.crc.update(&bytes[..n]);
        self.len += n;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}

pub(crate) fn write<W: Write, T: Serialize>(
    mut out: W,
    kind: u16,
    version: u16,
    value: &T,
) -> serde_json::Result<()> {
    write!(out, "{{\"magic\":\"MYDBMETA1\",\"kind\":{kind},\"version\":{version},\"required_features\":0,\"payload\":")
        .map_err(serde_json::Error::io)?;
    let mut payload = PayloadWriter {
        out: &mut out,
        crc: crc(kind, version),
        len: 0,
        limit: limit(kind),
    };
    serde_json::to_writer(&mut payload, value)?;
    let checksum = payload.crc.finalize();
    write!(out, ",\"checksum\":{checksum}}}").map_err(serde_json::Error::io)
}

pub(crate) fn encode<T: Serialize>(
    kind: u16,
    version: u16,
    value: &T,
) -> serde_json::Result<Vec<u8>> {
    let mut out = Vec::new();
    write(&mut out, kind, version, value)?;
    Ok(out)
}

pub(crate) fn decode<T: DeserializeOwned>(
    kind: u16,
    version: u16,
    bytes: &[u8],
) -> serde_json::Result<T> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Envelope<'a> {
        magic: &'a str,
        kind: u16,
        version: u16,
        required_features: u64,
        #[serde(borrow)]
        payload: &'a serde_json::value::RawValue,
        checksum: u32,
    }
    let invalid = || {
        serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported metadata version/features or checksum mismatch",
        ))
    };
    if bytes.len() > limit(kind) + 256 {
        return Err(invalid());
    }
    let envelope: Envelope<'_> = serde_json::from_slice(bytes)?;
    let mut checksum = crc(kind, version);
    checksum.update(envelope.payload.get().as_bytes());
    if envelope.magic != "MYDBMETA1"
        || envelope.kind != kind
        || envelope.version != version
        || envelope.required_features != 0
    {
        return Err(serde_json::Error::io(io::Error::new(
            io::ErrorKind::Unsupported,
            "unsupported persistent metadata version or required features",
        )));
    }
    if checksum.finalize() != envelope.checksum {
        return Err(invalid());
    }
    serde_json::from_str(envelope.payload.get())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn release_format_golden_native_replication_state() {
        use crate::storage::format_golden::golden;
        let mut role = crate::repl::role::RoleState::new_standalone();
        role.node_uuid = "00000000-0000-4000-8000-000000000001".into();
        let bytes = encode(
            ROLE,
            crate::storage::format_versions::NATIVE_REPLICATION_METADATA_VERSION as u16,
            &role,
        )
        .unwrap();
        golden!("replication-role", &bytes);
        assert_eq!(
            decode::<crate::repl::role::RoleState>(ROLE, 3, &bytes).unwrap(),
            role
        );
        let progress = crate::repl::replica::progress::ReplicaProgress {
            received_lsn: 19,
            durable_lsn: 17,
            applied_lsn: 13,
            durable_applied_lsn: 11,
            last_master_uuid: role.node_uuid.clone(),
            last_master_epoch: 2,
            last_update_ms: 17,
            ..Default::default()
        };
        let bytes = encode(
            PROGRESS,
            crate::storage::format_versions::REPLICA_PROGRESS_VERSION,
            &progress,
        )
        .unwrap();
        golden!("replication-progress", &bytes);
        assert_eq!(
            decode::<crate::repl::replica::progress::ReplicaProgress>(PROGRESS, 1, &bytes).unwrap(),
            progress
        );
        let slot = crate::repl::master::slot::SlotState::new(role.node_uuid, 17);
        let bytes = encode(
            SLOT,
            crate::storage::format_versions::REPLICATION_SLOT_VERSION,
            &slot,
        )
        .unwrap();
        golden!("replication-slot", &bytes);
        assert_eq!(
            decode::<crate::repl::master::slot::SlotState>(SLOT, 1, &bytes).unwrap(),
            slot
        );
    }

    #[test]
    fn release_format_metadata_rejects_future_features_and_payload_corruption() {
        let bytes = encode(PROGRESS, 1, &vec![1, 2, 3]).unwrap();
        crate::storage::format_golden::golden!("metadata-envelope", &bytes);
        assert_eq!(
            decode::<Vec<u32>>(PROGRESS, 1, &bytes).unwrap(),
            vec![1, 2, 3]
        );
        assert!(decode::<Vec<u32>>(PROGRESS, 2, &bytes).is_err());
        assert!(decode::<Vec<u32>>(SLOT, 1, &bytes).is_err());
        for field in ["version", "required_features", "checksum"] {
            let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            value[field] = 999.into();
            assert!(decode::<Vec<u32>>(PROGRESS, 1, &serde_json::to_vec(&value).unwrap()).is_err());
        }
    }
}
