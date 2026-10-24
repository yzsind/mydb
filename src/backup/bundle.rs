//! Streaming bundle framing with per-object SHA-256 and a transcript digest.
//! The transcript binds magic, ordered headers, verified object digests and the
//! final count. Payload bytes are bound transitively without hashing them twice.
use super::*;
use std::io::{Read, Seek, Write};
pub(super) const MAGIC: &[u8; 16] = &{
    let version = crate::storage::format_versions::BACKUP_BUNDLE_VERSION;
    assert!(version < 10);
    let mut magic = *b"MYDB_BUNDLE_V0\0\0";
    magic[13] = b'0' + version as u8;
    magic
};
pub(super) struct Writer {
    pub file: std::fs::File,
    pub objects: u64,
    hash: Sha256,
    buffer: Vec<u8>,
    _memory: ControlledMemoryCharge,
    pub bytes: u64,
}
impl Writer {
    pub fn new(file: std::fs::File) -> Result<Self, QueryError> {
        let memory =
            ControlledMemoryCharge::try_new(ControlledMemoryCategory::StorageScratch, CHUNK_BYTES)?;
        Ok(Self {
            file,
            objects: 0,
            hash: Sha256::new(),
            bytes: 0,
            buffer: vec![0; CHUNK_BYTES],
            _memory: memory,
        })
    }
    pub fn copy_payload(
        &mut self,
        input: &mut std::fs::File,
        length: u64,
        root: &Path,
        expected: Option<&BackupFile>,
    ) -> Result<[u8; 32], QueryError> {
        if expected.is_some_and(|file| file.len != length) {
            return Err(backup_err("export base file length mismatch"));
        }
        let mut hash = Sha256::new();
        let mut left = length;
        let mut chunk_index = 0;
        while left > 0 {
            let take = if let Some(file) = expected {
                let chunk = file
                    .chunks
                    .get(chunk_index)
                    .ok_or_else(|| backup_err("export base chunk count mismatch"))?;
                if chunk.len == 0 || chunk.len > left || chunk.len > CHUNK_BYTES as u64 {
                    return Err(backup_err("export base chunk length mismatch"));
                }
                chunk.len as usize
            } else {
                left.min(CHUNK_BYTES as u64) as usize
            };
            super::resources::throttle(take as u64)?;
            input
                .read_exact(&mut self.buffer[..take])
                .map_err(|e| io_err("read export file", e))?;
            super::resources::check_spool(self.bytes.saturating_add(take as u64))?;
            let _disk = reserve_disk_write(root, take as u64)?;
            self.file
                .write_all(&self.buffer[..take])
                .map_err(|e| io_err("write bundle payload", e))?;
            hash.update(&self.buffer[..take]);
            if let Some(file) = expected {
                let chunk = file
                    .chunks
                    .get(chunk_index)
                    .ok_or_else(|| backup_err("export base chunk count mismatch"))?;
                if chunk.len != take as u64
                    || (file.chunks.len() > 1 && sha256_hex(&self.buffer[..take]) != chunk.hash)
                {
                    return Err(backup_err("export base chunk checksum/length mismatch"));
                }
            }
            chunk_index += 1;
            self.bytes += take as u64;
            left -= take as u64;
        }
        let mut extra = [0];
        if input
            .read(&mut extra)
            .map_err(|e| io_err("check export file EOF", e))?
            != 0
        {
            return Err(backup_err("export source length changed"));
        }
        let digest: [u8; 32] = hash.finalize().into();
        if let Some(file) = expected {
            if chunk_index != file.chunks.len()
                || (file.chunks.len() == 1 && encode_hex(&digest) != file.chunks[0].hash)
            {
                return Err(backup_err("export base file checksum/count mismatch"));
            }
        }
        Ok(digest)
    }
    /// Small metadata payload; headers and the returned object digest are
    /// written through `Write`, while raw payload is excluded from the transcript.
    pub fn write_payload(&mut self, bytes: &[u8], root: &Path) -> Result<(), QueryError> {
        super::resources::check_spool(self.bytes.saturating_add(bytes.len() as u64))?;
        let _disk = reserve_disk_write(root, bytes.len() as u64)?;
        self.file
            .write_all(bytes)
            .map_err(|e| io_err("write bundle payload", e))?;
        self.bytes += bytes.len() as u64;
        Ok(())
    }
    pub fn finish(&mut self) -> Result<(), QueryError> {
        self.write_all(&0u32.to_le_bytes())
            .and_then(|_| self.write_all(&self.objects.to_le_bytes()))
            .map_err(|e| io_err("write bundle terminator", e))?;
        let digest = self.hash.clone().finalize();
        self.file
            .write_all(&digest)
            .and_then(|_| self.file.sync_all())
            .map_err(|e| io_err("sync complete bundle", e))
    }
}
impl Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        super::resources::check_spool(self.bytes.saturating_add(bytes.len() as u64))
            .map_err(|e| std::io::Error::other(error_detail(&e)))?;
        let count = self.file.write(bytes)?;
        self.hash.update(&bytes[..count]);
        self.bytes += count as u64;
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
pub(super) struct Reader {
    pub file: std::fs::File,
    hash: Sha256,
    pub input_len: u64,
    pub identity: String,
}
impl Reader {
    pub fn read_payload_exact(&mut self, bytes: &mut [u8]) -> std::io::Result<()> {
        self.file.read_exact(bytes)
    }
    pub fn new(mut file: std::fs::File) -> Result<Self, QueryError> {
        use std::io::{Seek, SeekFrom};
        let len = file
            .metadata()
            .map_err(|e| io_err("inspect immutable bundle", e))?
            .len();
        if len < 60 {
            return Err(backup_err("incomplete bundle footer"));
        }
        file.seek(SeekFrom::End(-32))
            .map_err(|e| io_err("seek bundle footer", e))?;
        let mut digest = [0; 32];
        file.read_exact(&mut digest)
            .map_err(|e| io_err("read bundle identity", e))?;
        let identity = sha256_hex(&[len.to_le_bytes().as_slice(), digest.as_slice()].concat());
        file.seek(SeekFrom::Start(0))
            .map_err(|e| io_err("rewind bundle", e))?;
        Ok(Self {
            file,
            hash: Sha256::new(),
            input_len: len,
            identity,
        })
    }
    pub fn remaining(&mut self) -> Result<u64, QueryError> {
        Ok(self.input_len.saturating_sub(
            self.file
                .stream_position()
                .map_err(|e| io_err("read bundle position", e))?,
        ))
    }
    pub fn finish(&mut self, objects: u64) -> Result<(), QueryError> {
        let mut count = [0; 8];
        self.read_exact(&mut count)
            .map_err(|e| io_err("read bundle final count", e))?;
        if u64::from_le_bytes(count) != objects {
            return Err(backup_err("bundle object set/count mismatch"));
        }
        let mut digest = [0; 32];
        self.file
            .read_exact(&mut digest)
            .map_err(|e| io_err("read final bundle digest", e))?;
        let expected = self.hash.clone().finalize();
        if digest.as_slice() != expected.as_slice() || self.remaining()? != 0 {
            return Err(backup_err("bundle final digest/length mismatch"));
        }
        let mut extra = [0];
        if self
            .file
            .read(&mut extra)
            .map_err(|e| io_err("read bundle final EOF", e))?
            != 0
        {
            return Err(backup_err("bundle changed after identity capture"));
        }
        let identity =
            sha256_hex(&[self.input_len.to_le_bytes().as_slice(), digest.as_slice()].concat());
        if identity != self.identity {
            return Err(backup_err("immutable bundle identity changed"));
        }
        Ok(())
    }
}
impl Read for Reader {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let count = self.file.read(bytes)?;
        self.hash.update(&bytes[..count]);
        Ok(count)
    }
}
