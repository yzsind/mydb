//! XA / distributed-transaction primitives shared across parser, executor,
//! storage and WAL. The `Xid` type carries the MySQL XA branch identifier:
//! `(format_id, gtrid, bqual)`. mydb only acts as the resource manager — an
//! external coordinator (JTA, XA driver, etc.) issues the `XA …` SQL.

use serde::{Deserialize, Serialize};

use crate::types::QueryError;

pub const XID_GTRID_MAX: usize = 64;
pub const XID_BQUAL_MAX: usize = 64;
pub const XID_FORMAT_ID_DEFAULT: i64 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Xid {
    pub format_id: i64,
    pub gtrid: Vec<u8>,
    pub bqual: Vec<u8>,
}

impl Xid {
    pub fn new(format_id: i64, gtrid: Vec<u8>, bqual: Vec<u8>) -> Result<Self, QueryError> {
        let xid = Self {
            format_id,
            gtrid,
            bqual,
        };
        xid.validate()?;
        Ok(xid)
    }

    pub(crate) fn validate(&self) -> Result<(), QueryError> {
        if self.gtrid.is_empty() || self.gtrid.len() > XID_GTRID_MAX {
            return Err(xa_error(
                crate::error::catalog::ER_XAER_INVAL,
                format!(
                    "XAER_INVAL: gtrid length must be 1..{} bytes (got {})",
                    XID_GTRID_MAX,
                    self.gtrid.len()
                ),
            ));
        }
        if self.bqual.len() > XID_BQUAL_MAX {
            return Err(xa_error(
                crate::error::catalog::ER_XAER_INVAL,
                format!(
                    "XAER_INVAL: bqual length must be 0..{} bytes (got {})",
                    XID_BQUAL_MAX,
                    self.bqual.len()
                ),
            ));
        }
        Ok(())
    }

    pub fn gtrid_len(&self) -> i64 {
        self.gtrid.len() as i64
    }

    pub fn bqual_len(&self) -> i64 {
        self.bqual.len() as i64
    }

    /// MySQL `XA RECOVER` DATA column rendering. Plain `XA RECOVER` emits
    /// the gtrid+bqual bytes literally when every byte is printable ASCII;
    /// otherwise (or with `CONVERT XID`) it emits `0xHEX` covering the
    /// concatenated gtrid||bqual bytes.
    pub fn render_data(&self, convert: bool) -> String {
        if convert || !self.is_printable_ascii() {
            let mut hex = String::with_capacity(2 + 2 * (self.gtrid.len() + self.bqual.len()));
            hex.push_str("0x");
            crate::util::hex::append_upper(&mut hex, &self.gtrid);
            crate::util::hex::append_upper(&mut hex, &self.bqual);
            hex
        } else {
            let mut out = String::with_capacity(self.gtrid.len() + self.bqual.len());
            out.push_str(std::str::from_utf8(&self.gtrid).unwrap_or(""));
            out.push_str(std::str::from_utf8(&self.bqual).unwrap_or(""));
            out
        }
    }

    fn is_printable_ascii(&self) -> bool {
        self.gtrid
            .iter()
            .chain(self.bqual.iter())
            .all(|&b| (0x20..=0x7e).contains(&b))
    }
}

fn xa_error(spec: crate::error::ErrorSpec, message: impl Into<String>) -> QueryError {
    spec.error(message)
}

/// Branch state attached to a session while an XA transaction is owned by it.
/// PREPARED is intentionally absent here: once an XA branch reaches PREPARED
/// it is detached from any session and lives in the engine-global registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum XaBranchState {
    Active,
    Idle,
}

impl XaBranchState {
    pub fn as_str(self) -> &'static str {
        match self {
            XaBranchState::Active => "ACTIVE",
            XaBranchState::Idle => "IDLE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XaBranch {
    pub xid: Xid,
    pub state: XaBranchState,
    pub txn_id: u64,
}

/// Engine-global descriptor for a PREPARED XA branch that is no longer
/// owned by any session. Persisted in the WAL via `PrepareTxn` and
/// rebuilt at recovery; queryable via `XA RECOVER`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedXa {
    pub xid: Xid,
    pub txn_id: u64,
    pub prepare_scn: u64,
    pub started_unix_ms: u64,
    pub(crate) binlog: Option<crate::wal::PreparedBinlogRef>,
    pub(crate) recovered: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xid_construction_enforces_lengths() {
        assert!(Xid::new(1, vec![], vec![]).is_err());
        assert!(Xid::new(1, vec![0; 65], vec![]).is_err());
        assert!(Xid::new(1, vec![b'g'], vec![0; 65]).is_err());
        let xid = Xid::new(0, b"tx1".to_vec(), vec![]).unwrap();
        assert_eq!(xid.gtrid_len(), 3);
        assert_eq!(xid.bqual_len(), 0);
    }

    #[test]
    fn render_data_printable_emits_literal() {
        let xid = Xid::new(1, b"tx1".to_vec(), b"br".to_vec()).unwrap();
        assert_eq!(xid.render_data(false), "tx1br");
    }

    #[test]
    fn render_data_nonprintable_emits_hex() {
        let xid = Xid::new(1, vec![0x01, 0x02], b"x".to_vec()).unwrap();
        assert_eq!(xid.render_data(false), "0x010278");
    }

    #[test]
    fn render_data_convert_always_hex() {
        let xid = Xid::new(1, b"ab".to_vec(), b"c".to_vec()).unwrap();
        assert_eq!(xid.render_data(true), "0x616263");
    }
}
