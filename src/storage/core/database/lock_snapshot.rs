use crate::util::fast_hash::fnv1a_64;
// Read-only lock-manager snapshots for
// `performance_schema.data_locks` / `performance_schema.data_lock_waits`.
//
// mydb's transaction manager tracks row locks (shared/exclusive, Oracle-TX
// style), table intent locks (IS/IX/S/SIX/X), unique-key locks, and a
// wait-for graph between transactions. These methods take a single read lock
// on the manager and project that live state into the column shapes the two
// performance_schema lock tables expose. They never mutate and never block a
// writer for longer than building owned `Vec`s, so they are safe to call from
// the SELECT path.
//
// Honesty notes (per CLAUDE.md):
// * `ENGINE` is reported as `MYDB` — these are mydb's own locks, not an
//   InnoDB emulation.
// * `ENGINE_TRANSACTION_ID` is the authoritative identity; mapping a lock back
//   to the owning *connection* (`THREAD_ID`) happens one layer up, in the
//   performance_schema producer, by joining transaction ids against the
//   session registry.
// * `INDEX_NAME` is `NULL` for row locks (mydb locks heap rows, not index
//   records); `LOCK_DATA` carries the physical row locator
//   (`page:slot:generation`).

/// One granted-or-waiting lock, projected for `performance_schema.data_locks`.
#[derive(Debug, Clone)]
pub struct DataLockRow {
    /// Stable, opaque per-lock identifier (`ENGINE_LOCK_ID`).
    pub engine_lock_id: String,
    /// Owning transaction (`ENGINE_TRANSACTION_ID`).
    pub transaction_id: u64,
    pub object_schema: Option<String>,
    pub object_name: Option<String>,
    pub index_name: Option<String>,
    /// Non-null synthetic instance id (`OBJECT_INSTANCE_BEGIN`).
    pub object_instance: u64,
    /// `RECORD` or `TABLE`.
    pub lock_type: &'static str,
    /// Lock mode string (`S`/`X` for rows, `IS`/`IX`/`S`/`SIX`/`X` for tables).
    pub lock_mode: String,
    /// `GRANTED` or `WAITING`.
    pub lock_status: &'static str,
    pub lock_data: Option<String>,
}

/// One edge of the wait-for graph, for `performance_schema.data_lock_waits`.
#[derive(Debug, Clone)]
pub struct DataLockWaitRow {
    pub requesting_lock_id: String,
    pub requesting_transaction_id: u64,
    pub requesting_object_instance: u64,
    pub blocking_lock_id: String,
    pub blocking_transaction_id: u64,
    pub blocking_object_instance: u64,
    pub object_schema: Option<String>,
    pub object_name: Option<String>,
}

/// One active storage transaction, projected for
/// `information_schema.INNODB_TRX`.
#[derive(Debug, Clone)]
pub struct ActiveTransactionRow {
    pub transaction_id: u64,
    pub state: &'static str,
    pub started_unix_ms: u64,
    pub requested_lock_id: Option<String>,
    pub wait_started_unix_ms: Option<u64>,
    pub read_only: bool,
    pub is_writer: bool,
    pub has_undo: bool,
}

/// Bounded monitor details. Totals count all candidates, including omitted rows.
pub(crate) struct EngineMonitorDetails {
    pub transactions: Vec<ActiveTransactionRow>,
    pub transaction_count: usize,
    pub waits: Vec<EngineMonitorWait>,
    pub wait_edge_count: usize,
}

pub(crate) struct EngineMonitorWait {
    pub requesting_transaction_id: u64,
    pub blocking_transaction_id: u64,
    pub started_unix_ms: u64,
    pub table_id: u64,
    pub object: Option<(String, String)>,
    pub resource: String,
}

impl Database {
    /// The graph records pending resources; actual holders live in the row
    /// and table shards. Probe one resource in the diagnostic lock order
    /// (graph -> shard), also excluding edges whose owners already released.
    fn diagnostic_wait_blockers(
        &self,
        locks: &TxnLockManager,
        waiter: u64,
        wait: &crate::storage::txn::LockWait,
    ) -> LockBlockers {
        match &wait.resource {
            LockWaitResource::Row(table, row) if row_lock_sharding_enabled() => {
                self.row_locks_sharded.diagnostic_blockers(
                    waiter,
                    *table,
                    *row,
                    wait.mode.unwrap_or(LockMode::Exclusive),
                )
            }
            resource => wait
                .owners
                .iter()
                .copied()
                .filter(|&owner| {
                    matches!(resource, LockWaitResource::Table(table)
                    if self.table_locks_sharded.holds(owner, *table))
                        || locks.lock_wait_owner_still_blocks(waiter, wait, owner)
                })
                .collect(),
        }
    }

    /// Select oldest transactions and wait edges before allocating strings.
    /// Never enumerate granted locks or open catalog table definitions.
    pub(crate) fn engine_monitor_details(&self, limit: usize) -> EngineMonitorDetails {
        let mut details = EngineMonitorDetails {
            transactions: Vec::with_capacity(limit),
            transaction_count: 0,
            waits: Vec::with_capacity(limit),
            wait_edge_count: 0,
        };
        if let Ok(txns) = self.txns_read() {
            let mut selected = Vec::with_capacity(limit);
            for slot in txns.in_flight_slots() {
                details.transaction_count += 1;
                let key = (slot.started_unix_ms, slot.txn_id);
                let position = selected.partition_point(|&(other, _)| other < key);
                if position < limit {
                    selected.insert(position, (key, slot));
                    selected.truncate(limit);
                }
            }
            details.transactions = selected
                .into_iter()
                .map(|(_, slot)| ActiveTransactionRow {
                    transaction_id: slot.txn_id,
                    state: if slot.status == TxnStatus::Prepared {
                        "PREPARED"
                    } else {
                        "RUNNING"
                    },
                    started_unix_ms: slot.started_unix_ms,
                    requested_lock_id: None,
                    wait_started_unix_ms: None,
                    read_only: slot.read_only,
                    is_writer: slot.is_writer,
                    has_undo: slot.undo_head.get().is_some(),
                })
                .collect();
        }
        {
            let lm = self.lock_manager_read();
            for txn in &mut details.transactions {
                if let Some(wait) = lm.waits_for.get(&txn.transaction_id).filter(|wait| {
                    !self
                        .diagnostic_wait_blockers(&lm, txn.transaction_id, wait)
                        .is_empty()
                }) {
                    txn.state = "LOCK WAIT";
                    txn.wait_started_unix_ms = Some(wait.started_unix_ms);
                }
            }
            let mut selected = Vec::with_capacity(limit);
            for (&waiter, wait) in &lm.waits_for {
                for owner in self.diagnostic_wait_blockers(&lm, waiter, wait) {
                    details.wait_edge_count += 1;
                    let key = (wait.started_unix_ms, waiter, owner);
                    let position = selected.partition_point(|&(other, _)| other < key);
                    if position < limit {
                        selected.insert(position, (key, wait));
                        selected.truncate(limit);
                    }
                }
            }
            details.waits = selected
                .into_iter()
                .map(|((started, waiter, owner), wait)| {
                    let (table_id, _, _, resource) = describe_resource(&wait.resource);
                    EngineMonitorWait {
                        requesting_transaction_id: waiter,
                        blocking_transaction_id: owner,
                        started_unix_ms: started,
                        table_id,
                        object: None,
                        resource,
                    }
                })
                .collect();
        }
        // Resolve only the selected resources, after releasing transaction and
        // lock-manager guards. Empty wait graphs do not inspect the catalog.
        if !details.waits.is_empty() {
            if let Ok(catalog) = self.catalog.read() {
                for table in catalog.tables.values() {
                    let partition_ids = table.partitioning.as_ref().map(|part| part.storage_ids());
                    for wait in &mut details.waits {
                        if table.id == wait.table_id
                            || partition_ids
                                .as_ref()
                                .is_some_and(|ids| ids.contains(&wait.table_id))
                        {
                            wait.object = Some((table.database.clone(), table.name.clone()));
                        }
                    }
                }
            }
        }
        details
    }

    /// Snapshot active and prepared transaction slots for
    /// `information_schema.INNODB_TRX`. The transaction slot state is the
    /// source of truth; the lock manager is consulted only to mark active
    /// lock waits and expose the requested lock id.
    pub fn active_transaction_snapshot(&self) -> Vec<ActiveTransactionRow> {
        let mut rows: Vec<ActiveTransactionRow> = self
            .txns_read()
            .map(|txns| {
                txns.slots
                    .values()
                    .filter(|slot| slot.status.is_in_flight())
                    .map(|slot| ActiveTransactionRow {
                        transaction_id: slot.txn_id,
                        state: "RUNNING",
                        started_unix_ms: slot.started_unix_ms,
                        requested_lock_id: None,
                        wait_started_unix_ms: None,
                        read_only: slot.read_only,
                        is_writer: slot.is_writer,
                        has_undo: slot.undo_head.get().is_some(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        if rows.is_empty() {
            return rows;
        }

        let lm = self.lock_manager_read();
        for row in &mut rows {
            if let Some(wait) = lm.waits_for.get(&row.transaction_id).filter(|wait| {
                !self
                    .diagnostic_wait_blockers(&lm, row.transaction_id, wait)
                    .is_empty()
            }) {
                let (_table_id, _lock_type, _data, key) = describe_resource(&wait.resource);
                row.state = "LOCK WAIT";
                row.requested_lock_id = Some(lock_id(row.transaction_id, &key));
                row.wait_started_unix_ms = Some(wait.started_unix_ms);
            }
        }
        rows
    }

    /// Snapshot every lock the transaction manager currently tracks: granted
    /// row / table / unique-key locks plus the locks transactions are waiting
    /// to acquire (`LOCK_STATUS = WAITING`).
    pub fn data_lock_snapshot(&self) -> Vec<DataLockRow> {
        let names = self.lock_object_names();
        let lm = self.lock_manager_read();
        let mut rows = Vec::new();

        // Granted row locks — one row per (resource, owner). In sharded mode
        // (P3) the row locks live in `row_locks_sharded`, not `lock_manager`.
        let granted_rows: Vec<((u64, RowId), LockMode, Vec<u64>)> = if row_lock_sharding_enabled() {
            self.row_locks_sharded.snapshot_locks()
        } else {
            lm.locks
                .iter()
                .map(|((t, r), s)| ((*t, *r), s.mode, s.owners.as_slice().to_vec()))
                .collect()
        };
        for ((table_id, row_id), lock_mode, owners) in &granted_rows {
            let key = row_resource_key(*table_id, row_id);
            let (schema, name) = resolve_object(&names, *table_id);
            let mode = lock_mode.as_str().to_string();
            let data = Some(format_row_id(row_id));
            for &owner in owners {
                let id = lock_id(owner, &key);
                rows.push(DataLockRow {
                    object_instance: fnv1a_64(&id),
                    engine_lock_id: id,
                    transaction_id: owner,
                    object_schema: schema.clone(),
                    object_name: name.clone(),
                    index_name: None,
                    lock_type: "RECORD",
                    lock_mode: mode.clone(),
                    lock_status: "GRANTED",
                    lock_data: data.clone(),
                });
            }
        }

        // Granted table (intent) locks — one row per (table, holder). Table
        // locks live in the sharded store now (P2), not in `lock_manager`.
        for (table_id, owner, mode) in self.table_locks_sharded.snapshot_holders() {
            let key = table_resource_key(table_id);
            let (schema, name) = resolve_object(&names, table_id);
            let id = lock_id(owner, &key);
            rows.push(DataLockRow {
                object_instance: fnv1a_64(&id),
                engine_lock_id: id,
                transaction_id: owner,
                object_schema: schema,
                object_name: name,
                index_name: None,
                lock_type: "TABLE",
                lock_mode: mode.as_str().to_string(),
                lock_status: "GRANTED",
                lock_data: None,
            });
        }

        // Granted unique-key locks (exclusive; held while a unique insert is
        // in flight). Reported as RECORD locks on the backing index.
        let mut add_unique = |table_id: u64, index_name: &str, owner: u64| {
            let key = unique_resource_key(table_id, index_name);
            let (schema, name) = resolve_object(&names, table_id);
            let id = lock_id(owner, &key);
            rows.push(DataLockRow {
                object_instance: fnv1a_64(&id),
                engine_lock_id: id,
                transaction_id: owner,
                object_schema: schema,
                object_name: name,
                index_name: Some(index_name.to_string()),
                lock_type: "RECORD",
                lock_mode: "X".to_string(),
                lock_status: "GRANTED",
                lock_data: None,
            });
        };
        for (uk, &owner) in &lm.unique_locks {
            add_unique(uk.table_id(), uk.index_name(), owner);
        }
        for (range, owner) in lm.unique_range_locks() {
            for _ in 0..range.len() {
                add_unique(range.table_id, &range.index_name, owner);
            }
        }

        // Waiting locks — the resource each blocked transaction is trying to
        // acquire. Mirrors MySQL, which lists WAITING rows in data_locks.
        for (&waiter, wait) in &lm.waits_for {
            if self.diagnostic_wait_blockers(&lm, waiter, wait).is_empty() {
                continue;
            }
            let (table_id, lock_type, data, key) = describe_resource(&wait.resource);
            let (schema, name) = resolve_object(&names, table_id);
            let id = lock_id(waiter, &key);
            let mode = wait
                .mode
                .map(|m| m.as_str().to_string())
                .unwrap_or_else(|| "X".to_string());
            rows.push(DataLockRow {
                object_instance: fnv1a_64(&id),
                engine_lock_id: id,
                transaction_id: waiter,
                object_schema: schema,
                object_name: name,
                index_name: None,
                lock_type,
                lock_mode: mode,
                lock_status: "WAITING",
                lock_data: data,
            });
        }

        rows
    }

    /// Snapshot the wait-for graph as `(requesting, blocking)` lock pairs. The
    /// blocking lock id matches a `GRANTED` row in [`Self::data_lock_snapshot`]
    /// and the requesting id matches its `WAITING` row, so the two tables join
    /// on `ENGINE_LOCK_ID` exactly as in MySQL.
    pub fn data_lock_wait_snapshot(&self) -> Vec<DataLockWaitRow> {
        let names = self.lock_object_names();
        let lm = self.lock_manager_read();
        lm.waits_for
            .iter()
            .flat_map(|(&waiter, wait)| {
                let (table_id, _lock_type, _data, key) = describe_resource(&wait.resource);
                let (schema, name) = resolve_object(&names, table_id);
                let requesting_lock_id = lock_id(waiter, &key);
                self.diagnostic_wait_blockers(&lm, waiter, wait)
                    .into_iter()
                    .map(move |owner| {
                        let blocking_lock_id = lock_id(owner, &key);
                        DataLockWaitRow {
                            requesting_object_instance: fnv1a_64(&requesting_lock_id),
                            blocking_object_instance: fnv1a_64(&blocking_lock_id),
                            requesting_lock_id: requesting_lock_id.clone(),
                            requesting_transaction_id: waiter,
                            blocking_lock_id,
                            blocking_transaction_id: owner,
                            object_schema: schema.clone(),
                            object_name: name.clone(),
                        }
                    })
            })
            .collect()
    }

    /// Build a `table_id → (schema, name)` map from the catalog, including
    /// partition storage ids (a partitioned table's row locks are keyed by the
    /// partition's storage id, not the logical table id). Acquired and released
    /// before the transaction-manager lock, so the two locks are never held at
    /// once.
    fn lock_object_names(&self) -> HashMap<u64, (String, String)> {
        let mut map = HashMap::new();
        if let Ok(catalog) = self.catalog.read() {
            for table in catalog.tables.values() {
                let object = (table.database.clone(), table.name.clone());
                map.insert(table.id, object.clone());
                if let Some(part) = table.partitioning.as_ref() {
                    for storage_id in part.storage_ids() {
                        map.insert(storage_id, object.clone());
                    }
                }
            }
        }
        map
    }
}

fn resolve_object(
    names: &HashMap<u64, (String, String)>,
    table_id: u64,
) -> (Option<String>, Option<String>) {
    match names.get(&table_id) {
        Some((schema, name)) => (Some(schema.clone()), Some(name.clone())),
        None => (None, None),
    }
}

/// Decompose a wait resource into `(table_id, lock_type, lock_data, key)`.
/// `key` is the resource-identity suffix used to build matching lock ids on
/// both the requesting and blocking sides.
fn describe_resource(resource: &LockWaitResource) -> (u64, &'static str, Option<String>, String) {
    match resource {
        LockWaitResource::Row(table_id, row_id) => (
            *table_id,
            "RECORD",
            Some(format_row_id(row_id)),
            row_resource_key(*table_id, row_id),
        ),
        LockWaitResource::Table(table_id) => {
            (*table_id, "TABLE", None, table_resource_key(*table_id))
        }
        LockWaitResource::Unique(uk) => (
            uk.table_id(),
            "RECORD",
            None,
            unique_resource_key(uk.table_id(), uk.index_name()),
        ),
    }
}

fn row_resource_key(table_id: u64, row_id: &RowId) -> String {
    format!("R:{}:{}:{}", table_id, row_id.page_id, row_id.slot_id)
}

fn table_resource_key(table_id: u64) -> String {
    format!("T:{table_id}")
}

fn unique_resource_key(table_id: u64, index_name: &str) -> String {
    format!("U:{table_id}:{index_name}")
}

/// `ENGINE_LOCK_ID` = `<txn>:<resource-key>`, deterministic so the same
/// (transaction, resource) always maps to the same id across snapshots and
/// across the two lock tables.
fn lock_id(txn: u64, resource_key: &str) -> String {
    format!("{txn}:{resource_key}")
}

fn format_row_id(row_id: &RowId) -> String {
    format!("{}:{}", row_id.page_id, row_id.slot_id)
}
