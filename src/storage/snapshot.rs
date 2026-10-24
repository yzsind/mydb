//! Durable source visibility imported by a sealed image, independent of local redo.
use super::txn::{TransactionManager, TxnStatus};
use crate::types::QueryError;
use crate::wal::{WalPayload, WalRecordSource};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SourceVisibility {
    pub next_txn_id: u64,
    pub next_scn: u64,
    pub prepared: Vec<PreparedDependency>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PreparedDependency {
    pub txn_id: u64,
    pub xid: crate::xa::Xid,
    pub begin_lsn: u64,
    pub undo_head: Option<u64>,
}

impl SourceVisibility {
    pub(crate) fn from_wal(records: &(impl WalRecordSource + ?Sized)) -> Result<Self, QueryError> {
        let (txns, _) = TransactionManager::from_wal_source(records)?;
        if txns
            .slots
            .values()
            .any(|slot| slot.status == TxnStatus::Active && slot.is_writer)
        {
            return Err(super::error(
                crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                "sealed boundary contains an unfinished ordinary writer",
            ));
        }
        let mut prepared = txns
            .slots
            .values()
            .filter(|slot| slot.status == TxnStatus::Prepared)
            .map(|slot| {
                Ok(PreparedDependency {
                    txn_id: slot.txn_id,
                    xid: slot.xa_xid.clone().ok_or_else(|| {
                        super::error(
                            crate::error::catalog::MYDB_ER_BACKUP_FAILED,
                            "prepared branch has no XID",
                        )
                    })?,
                    begin_lsn: slot.begin_lsn,
                    undo_head: slot.undo_head.get(),
                })
            })
            .collect::<Result<Vec<_>, QueryError>>()?;
        prepared.sort_by_key(|p| p.txn_id);
        Ok(Self {
            next_txn_id: txns.next_txn_id,
            next_scn: txns.next_scn,
            prepared,
        })
    }

    pub(crate) fn dependency_floor(&self) -> Option<u64> {
        self.prepared.iter().map(|branch| branch.begin_lsn).min()
    }

    pub(crate) fn checkpoint(&self, source_end: u64) -> WalPayload {
        WalPayload::CheckpointEnd {
            redo_lsn: source_end.saturating_add(1),
            next_txn_id: self.next_txn_id,
            next_scn: self.next_scn,
            auto_increment_covered_lsn: source_end.saturating_add(1),
            sequence_covered_lsn: source_end.saturating_add(1),
            gc_safe_txn_frontier: self
                .prepared
                .iter()
                .map(|p| p.txn_id.saturating_sub(1))
                .min()
                .unwrap_or(self.next_txn_id.saturating_sub(1)),
        }
    }
}
