//! Checked streaming snapshots for resident vector index state.
use crate::{storage::RowId, types::QueryError, vector::VectorValue};
use std::io::{self, Write};

pub(crate) const HEADER_LEN: usize = 8 + 2 + 1;
pub(crate) const EMPTY_COUNT_FILE_LEN: usize = HEADER_LEN + 4 + 4;

pub(crate) fn corrupt() -> QueryError {
    crate::storage::error(
        crate::error::catalog::ER_INDEX_CORRUPT,
        "corrupt persistent vector index state",
    )
}

pub(crate) fn io_error(err: io::Error) -> QueryError {
    crate::storage::error(crate::error::catalog::ER_GET_ERRNO, err.to_string())
}

pub(crate) struct Writer<W> {
    out: W,
    crc: crc32fast::Hasher,
}

impl<W: Write> Writer<W> {
    pub(crate) fn new(out: W, kind: u8) -> io::Result<Self> {
        let mut writer = Self {
            out,
            crc: crc32fast::Hasher::new(),
        };
        writer.put(b"MYDBVEC2")?;
        writer.put(&crate::storage::format_versions::VECTOR_STATE_VERSION.to_le_bytes())?;
        writer.u8(kind)?;
        Ok(writer)
    }
    fn put(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.out.write_all(bytes)?;
        self.crc.update(bytes);
        Ok(())
    }
    pub(crate) fn u8(&mut self, value: u8) -> io::Result<()> {
        self.put(&[value])
    }
    pub(crate) fn u32(&mut self, value: u32) -> io::Result<()> {
        self.put(&value.to_le_bytes())
    }
    pub(crate) fn u64(&mut self, value: u64) -> io::Result<()> {
        self.put(&value.to_le_bytes())
    }
    pub(crate) fn count(&mut self, value: usize) -> io::Result<()> {
        self.u32(u32::try_from(value).map_err(io::Error::other)?)
    }
    pub(crate) fn bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.count(bytes.len())?;
        self.put(bytes)
    }
    pub(crate) fn row(&mut self, row: RowId) -> io::Result<()> {
        self.u32(row.page_id)?;
        self.u32(u32::from(row.slot_id))
    }
    pub(crate) fn vector(&mut self, value: &VectorValue) -> io::Result<()> {
        self.u8(value.datatype as u8)?;
        self.u32(value.dimension)?;
        self.bytes(&value.payload)
    }
    pub(crate) fn finish(mut self) -> io::Result<()> {
        self.out.write_all(&self.crc.finalize().to_le_bytes())
    }
}

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8], kind: u8) -> Result<Self, QueryError> {
        if bytes.len() < HEADER_LEN + 4 {
            return Err(corrupt());
        }
        let (data, crc) = bytes.split_at(bytes.len() - 4);
        if &data[..8] != b"MYDBVEC2"
            || u16::from_le_bytes(data[8..10].try_into().map_err(|_| corrupt())?)
                != crate::storage::format_versions::VECTOR_STATE_VERSION
            || data[10] != kind
            || crc32fast::hash(data) != u32::from_le_bytes(crc.try_into().map_err(|_| corrupt())?)
        {
            return Err(corrupt());
        }
        Ok(Self {
            bytes: data,
            offset: HEADER_LEN,
        })
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], QueryError> {
        let end = self.offset.checked_add(len).ok_or_else(corrupt)?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(corrupt)?;
        self.offset = end;
        Ok(bytes)
    }
    pub(crate) fn u8(&mut self) -> Result<u8, QueryError> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u32(&mut self) -> Result<u32, QueryError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().map_err(|_| corrupt())?,
        ))
    }
    pub(crate) fn u64(&mut self) -> Result<u64, QueryError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| corrupt())?,
        ))
    }
    pub(crate) fn count(&mut self, minimum_bytes: usize) -> Result<usize, QueryError> {
        let count = self.u32()? as usize;
        if minimum_bytes == 0
            || count > self.bytes.len().saturating_sub(self.offset) / minimum_bytes
        {
            return Err(corrupt());
        }
        Ok(count)
    }
    pub(crate) fn bytes(&mut self) -> Result<&'a [u8], QueryError> {
        let len = self.count(1)?;
        self.take(len)
    }
    pub(crate) fn row(&mut self) -> Result<RowId, QueryError> {
        Ok(RowId {
            page_id: self.u32()?,
            slot_id: u16::try_from(self.u32()?).map_err(|_| corrupt())?,
        })
    }
    pub(crate) fn vector(&mut self) -> Result<VectorValue, QueryError> {
        let datatype = crate::vector::VectorDataType::from_tag(self.u8()?).ok_or_else(corrupt)?;
        let dimension = self.u32()?;
        let payload = self.bytes()?;
        if dimension == 0 || datatype.payload_len(dimension) != payload.len() {
            return Err(corrupt());
        }
        Ok(VectorValue::new(datatype, dimension, payload.to_vec()))
    }
    pub(crate) fn finish(self) -> Result<(), QueryError> {
        if self.offset != self.bytes.len() {
            return Err(corrupt());
        }
        Ok(())
    }
}
