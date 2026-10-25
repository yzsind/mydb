//! Recovery rules belong to one database open, including its parallel workers.
//! A restore must never change redo or salvage rules for another database.

use crate::types::QueryError;
use crate::wal::{WalReadMode, WalRecordSource};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DatabaseOpenMode {
    #[default]
    ReadWrite,
    SealBaseline,
    RestoreWorkspace,
    ReadOnlyImage,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RecoveryContext {
    source_range: Option<(u64, u64)>,
    pub(crate) continuation_base: Option<u64>,
    pub(crate) mode: DatabaseOpenMode,
    pub(crate) target_time_us: Option<u64>,
}

impl RecoveryContext {
    pub(crate) fn restore(start: u64, end: u64) -> Result<Self, QueryError> {
        if start == 0 || start > end || end == u64::MAX {
            return Err(invalid("invalid restore WAL range"));
        }
        Ok(Self {
            source_range: Some((start, end)),
            continuation_base: None,
            mode: DatabaseOpenMode::RestoreWorkspace,
            target_time_us: None,
        })
    }

    pub(crate) fn seal(start: u64, end: u64) -> Result<Self, QueryError> {
        let mut context = Self::restore(start, end)?;
        context.mode = DatabaseOpenMode::SealBaseline;
        Ok(context)
    }

    pub(crate) fn continuation(base: u64, end: u64) -> Result<Self, QueryError> {
        let mut context = Self::restore(base, end)?;
        context.continuation_base = Some(base);
        Ok(context)
    }

    pub(crate) fn at_source_time(mut self, time: u64) -> Self {
        self.target_time_us = Some(time);
        self
    }

    pub(crate) fn is_restore(self) -> bool {
        self.source_range.is_some()
    }

    pub(crate) fn source_end(self) -> Option<u64> { self.source_range.map(|(_, end)| end) }

    pub(crate) fn redo_floor(self) -> Option<u64> {
        self.source_range.map(|(start, _)| start)
    }

    pub(crate) fn must_redo(self, lsn: u64) -> bool {
        self.continuation_base.is_none() && self.redo_floor().is_some_and(|start| lsn >= start)
    }

    pub(crate) fn wal_read_mode(self) -> WalReadMode {
        if self.is_restore() {
            WalReadMode::Strict
        } else {
            WalReadMode::Recovery
        }
    }

    pub(crate) fn validate_source_range(
        self,
        records: &impl WalRecordSource,
    ) -> Result<(), QueryError> {
        let Some((start, end)) = self.source_range else {
            return Ok(());
        };
        let mut previous = 0u64;
        for index in 0..records.len() {
            // The scanner has already verified CRCs and complete TxnBatch frames.
            let lsn = records.header(index)?.lsn;
            if lsn == 0
                || (index == 0 && lsn > start)
                || (index != 0 && previous.checked_add(1) != Some(lsn))
            {
                return Err(invalid(
                    "captured WAL has an LSN gap or does not cover the recovery start",
                ));
            }
            previous = lsn;
        }
        if previous != end {
            return Err(invalid(
                "captured WAL does not reach its declared recovery end",
            ));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> QueryError {
    super::error(crate::error::catalog::MYDB_ER_BACKUP_FAILED, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{WalPayload, WalRecord};

    fn records(lsns: &[u64]) -> Vec<WalRecord> {
        lsns.iter()
            .map(|&lsn| WalRecord {
                source_time_us: 0,
                lsn,
                txn_id: 0,
                prev_txn_lsn: 0,
                payload: WalPayload::CheckpointBegin,
            })
            .collect()
    }

    #[test]
    fn restore_requires_real_contiguous_source_coverage() {
        let context = RecoveryContext::restore(10, 12).unwrap();
        assert!(context
            .validate_source_range(&records(&[9, 10, 11, 12]))
            .is_ok());
        for lsns in [
            &[][..],
            &[0, 10, 11, 12],
            &[10, 12],
            &[11, 12],
            &[10, 11],
            &[10, 11, 12, 13],
            &[10, 10, 11, 12],
        ] {
            assert!(
                context.validate_source_range(&records(lsns)).is_err(),
                "{lsns:?}"
            );
        }
        assert!(RecoveryContext::restore(0, 12).is_err());
        assert!(RecoveryContext::restore(12, 11).is_err());
        assert!(RecoveryContext::restore(1, u64::MAX).is_err());
    }

    #[test]
    fn recovery_rules_are_independent_values() {
        let restore = RecoveryContext::restore(10, 12).unwrap();
        std::thread::scope(|scope| {
            scope.spawn(move || {
                assert!(restore.must_redo(10));
                assert!(!restore.must_redo(9));
                assert_eq!(restore.wal_read_mode(), WalReadMode::Strict);
            });
            scope.spawn(|| {
                let crash = RecoveryContext::default();
                assert!(!crash.must_redo(10));
                assert_eq!(crash.wal_read_mode(), WalReadMode::Recovery);
                assert!(crash.validate_source_range(&records(&[100])).is_ok());
            });
        });
    }
}
