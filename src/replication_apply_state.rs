// Atomic apply receipts share the catalog/storage transaction they describe.
thread_local! {
    static DDL_RECEIPT: std::cell::RefCell<Option<(String, DdlReceipt)>> = const { std::cell::RefCell::new(None) };
}

#[derive(Clone, Serialize, Deserialize)]
struct DdlReceipt {
    boundary: String,
    ordinal: usize,
}

pub(crate) struct DdlReceiptGuard(Option<(String, DdlReceipt)>);
impl Drop for DdlReceiptGuard {
    fn drop(&mut self) {
        DDL_RECEIPT.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}

pub(crate) fn suspend_ddl_receipt() -> DdlReceiptGuard {
    DdlReceiptGuard(DDL_RECEIPT.with(|slot| slot.take()))
}

pub(crate) fn current_ddl_receipt() -> Result<Option<(String, String)>, QueryError> {
    DDL_RECEIPT.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|(key, value)| {
                serde_json::to_string(value)
                    .map(|value| (key.clone(), value))
                    .map_err(|err| {
                        error(
                            crate::error::catalog::ER_GET_ERRNO,
                            format!("serialize replication DDL receipt: {err}"),
                        )
                    })
            })
            .transpose()
    })
}

fn replication_property_key(domain: &str, channel: &str, identity: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for component in [domain.as_bytes(), channel.as_bytes(), identity] {
        digest.update((component.len() as u64).to_le_bytes());
        digest.update(component);
    }
    crate::util::hex::encode_lower(&digest.finalize())
}

impl ChannelRunner {
    fn apply_records_at_boundary(
        &self,
        ops: Vec<RelayOp>,
        gtid: Option<String>,
        record: &RelayRecord,
    ) -> Result<(), QueryError> {
        let channel = self.status_snapshot().channel;
        let receipt = DdlReceipt {
            boundary: serde_json::to_string(&(
                record.source_file.as_str(),
                record.source_pos,
                &gtid,
            ))
            .map_err(|err| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    format!("encode replication boundary: {err}"),
                )
            })?,
            ordinal: 0,
        };
        let key = replication_property_key("ddl", &channel, b"");
        let _guard = DdlReceiptGuard(DDL_RECEIPT.with(|slot| slot.replace(Some((key, receipt)))));
        self.apply_records(ops, gtid)
    }
}

const REPLICA_XA_FORMAT_ID: i64 = 0x4d594442;

#[derive(Serialize, Deserialize)]
struct ReplicaXaReceipt {
    internal_xid: crate::xa::Xid,
    token: String,
}

fn xa_relay_error(message: impl Into<String>) -> QueryError {
    error(
        crate::error::catalog::ER_NOT_SUPPORTED_YET,
        format!("MySQL XA replication: {}", message.into()),
    )
}

fn decode_xa_prepare_event(bytes: &[u8]) -> Result<(bool, crate::xa::Xid), QueryError> {
    // XA_PREPARE_LOG_EVENT: one_phase:u8, formatID:u32, gtrid/bqual
    // lengths:u32, then raw XID bytes; all integers are little-endian.
    if bytes.len() < 13 || bytes[0] > 1 {
        return Err(xa_relay_error("malformed XA_PREPARE_LOG_EVENT header"));
    }
    let format_id = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as i64;
    let gtrid_len = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let bqual_len = u32::from_le_bytes(bytes[9..13].try_into().unwrap()) as usize;
    if gtrid_len > 64 || bqual_len > 64 || bytes.len() != 13 + gtrid_len + bqual_len {
        return Err(xa_relay_error("malformed XA_PREPARE_LOG_EVENT XID lengths"));
    }
    // Source MySQL permits empty gtrids. The native prepared branch gets
    // a separate nonempty namespaced XID, so preserve source bytes as-is.
    let xid = crate::xa::Xid {
        format_id,
        gtrid: bytes[13..13 + gtrid_len].to_vec(),
        bqual: bytes[13 + gtrid_len..].to_vec(),
    };
    Ok((bytes[0] != 0, xid))
}

impl ChannelRunner {
    fn handle_xa_query(
        &self,
        sql: &str,
        file: &str,
        pos: u64,
        txn: &mut TxnBuffer,
    ) -> Result<(), QueryError> {
        let statement = crate::parser::parse_replication_xa_statement(sql).ok_or_else(|| {
            mark_io_error_fatal();
            xa_relay_error("cannot parse XA QUERY_EVENT")
        })?;
        if let Statement::XaStart { xid }
        | Statement::XaEnd { xid }
        | Statement::XaPrepare { xid }
        | Statement::XaCommit { xid, .. }
        | Statement::XaRollback { xid } = &statement
        {
            if xid.gtrid.len() > 64
                || xid.bqual.len() > 64
                || !(0..=u32::MAX as i64).contains(&xid.format_id)
            {
                mark_io_error_fatal();
                return Err(xa_relay_error(
                    "invalid source XA identifier lengths or format ID",
                ));
            }
        }
        let commit = matches!(&statement, Statement::XaCommit { .. });
        match statement {
            Statement::XaStart { xid } if !txn.active && txn.event_count == 0 => {
                txn.active = true;
                txn.xa_xid = Some(xid);
                txn.xa_ended = false;
            }
            Statement::XaEnd { xid } if txn.xa_xid.as_ref() == Some(&xid) && !txn.xa_ended => {
                txn.xa_ended = true;
            }
            Statement::XaCommit {
                xid,
                one_phase: true,
            } if txn.xa_xid.as_ref() == Some(&xid) && txn.xa_ended => {
                self.io_commit_txn(file, pos, txn)?;
            }
            Statement::XaCommit {
                xid,
                one_phase: false,
            }
            | Statement::XaRollback { xid }
                if !txn.active && txn.event_count == 0 =>
            {
                self.append_relay(
                    file,
                    pos,
                    txn.gtid.clone(),
                    &RelayOp::XaFinish {
                        xid,
                        commit,
                        source_ts: txn.last_ts,
                    },
                )?;
                self.persist_io_progress(file, pos, txn.gtid.take());
                *txn = TxnBuffer::default();
            }
            _ => {
                mark_io_error_fatal();
                return Err(xa_relay_error("unsupported or out-of-order XA QUERY_EVENT; expected XA START/END, a binary prepare event, or a detached XA COMMIT/ROLLBACK"));
            }
        }
        Ok(())
    }

    fn xa_property_keys(&self, xid: &crate::xa::Xid) -> Result<(String, String), QueryError> {
        let channel = self.status_snapshot().channel;
        let identity = serde_json::to_vec(xid).map_err(|err| xa_relay_error(err.to_string()))?;
        Ok((
            replication_property_key("xa-pending", &channel, &identity),
            replication_property_key("xa-committed", &channel, &identity),
        ))
    }

    fn apply_xa_prepare(
        &self,
        xid: &crate::xa::Xid,
        records: &[RelayRecord],
        boundary: &RelayRecord,
    ) -> Result<(), QueryError> {
        let channel = self.status_snapshot().channel;
        let (pending_key, commit_key) = self.xa_property_keys(xid)?;
        let token =
            serde_json::to_string(&(&boundary.source_file, boundary.source_pos, &boundary.gtid))
                .map_err(|err| xa_relay_error(err.to_string()))?;
        let identity =
            serde_json::to_vec(&(xid, &token)).map_err(|err| xa_relay_error(err.to_string()))?;
        let receipt = ReplicaXaReceipt {
            internal_xid: crate::xa::Xid::new(
                REPLICA_XA_FORMAT_ID,
                replication_property_key("xa-branch", &channel, &identity).into_bytes(),
                replication_property_key("xa-channel", &channel, b"").into_bytes(),
            )?,
            token,
        };
        let encoded =
            serde_json::to_string(&receipt).map_err(|err| xa_relay_error(err.to_string()))?;
        let db = self.engine.database_for_replication();
        let gate = crate::storage::capture::logical_apply_gate(db)?;
        let _apply = gate.read_arc();
        if let Some(previous) = db.replication_property(&pending_key)? {
            let previous: ReplicaXaReceipt = serde_json::from_str(&previous)
                .map_err(|err| xa_relay_error(format!("invalid prepared receipt: {err}")))?;
            if previous.token != receipt.token {
                return Err(xa_relay_error("source reused an unresolved XA identifier"));
            }
        }
        if !db.prepared_xa_contains(&receipt.internal_xid) {
            if db.replication_property(&commit_key)?.as_deref() == Some(&receipt.token) {
                return Err(xa_relay_error(
                    "XA PREPARE replay crossed an already committed boundary",
                ));
            }
            let ops = records
                .iter()
                .map(|record| self.decode_relay_rows(record.op.clone()))
                .collect::<Result<Vec<_>, _>>()?;
            if ops.iter().any(|op| {
                !matches!(
                    op,
                    RelayOp::Noop
                        | RelayOp::WriteRows { .. }
                        | RelayOp::UpdateRows { .. }
                        | RelayOp::DeleteRows { .. }
                )
            }) {
                return Err(xa_relay_error("XA PREPARE contains non-row operations"));
            }
            let txn = db.begin_txn()?;
            let result = with_replication_applier(|| {
                apply_ops_to_storage(&self.engine, db, txn, &ops)?;
                // This receipt becomes visible only if the prepared transaction
                // commits, including after recovery before cursor persistence.
                db.write_replication_property(txn, &commit_key, Some(&receipt.token))?;
                db.prepare_xa(txn.txn_id, 0, receipt.internal_xid.clone())
                    .map(|_| ())
            });
            if let Err(err) = result {
                if !db.prepared_xa_contains(&receipt.internal_xid) {
                    let _ = db.rollback_txn(txn.txn_id);
                    db.release_xa(&receipt.internal_xid, txn.txn_id);
                }
                return Err(err);
            }
        }
        // PREPARE is WAL-durable before its source identifier mapping. A crash
        // in between replays PREPARE and finds the deterministic native XID.
        db.set_replication_property(&pending_key, Some(&encoded))
    }

    fn apply_xa_finish(&self, xid: &crate::xa::Xid, commit: bool) -> Result<(), QueryError> {
        let (pending_key, commit_key) = self.xa_property_keys(xid)?;
        let db = self.engine.database_for_replication();
        let gate = crate::storage::capture::logical_apply_gate(db)?;
        let _apply = gate.read_arc();
        let encoded = db.replication_property(&pending_key)?.ok_or_else(|| xa_relay_error("XA completion has no replicated PREPARE; start from a source position containing the entire XA branch"))?;
        let receipt: ReplicaXaReceipt = serde_json::from_str(&encoded)
            .map_err(|err| xa_relay_error(format!("invalid prepared receipt: {err}")))?;
        if db.prepared_xa_contains(&receipt.internal_xid) {
            with_replication_applier(|| {
                if commit {
                    db.commit_prepared(&receipt.internal_xid).map(|_| ())
                } else {
                    db.rollback_prepared(&receipt.internal_xid)
                }
            })
        } else {
            let was_committed =
                db.replication_property(&commit_key)?.as_deref() == Some(&receipt.token);
            if was_committed != commit {
                return Err(xa_relay_error(
                    "source XA outcome conflicts with the durable local outcome",
                ));
            }
            Ok(())
        }
    }

    fn cleanup_xa_receipts(&self, xid: &crate::xa::Xid) -> Result<(), QueryError> {
        let (pending_key, commit_key) = self.xa_property_keys(xid)?;
        let db = self.engine.database_for_replication();
        let txn = db.begin_txn()?;
        let result = db
            .write_replication_property(txn, &pending_key, None)
            .and_then(|_| db.write_replication_property(txn, &commit_key, None));
        if let Err(err) = result {
            let _ = db.rollback_txn(txn.txn_id);
            return Err(err);
        }
        db.commit_txn(txn.txn_id).map(|_| ())
    }
}
