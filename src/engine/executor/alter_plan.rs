use super::*;

/// One read-only capability decision for algorithm validation, execution locks,
/// and atomic dispatch. The caller holds definition-stabilizing MDL while
/// planning. No mutation or data scan is allowed here.
pub(super) struct AlterExecutionPlan {
    pub(super) required: AlterAlgorithm,
    pub(super) mdl: MdlLockType,
    pub(super) physical_copy: bool,
    pub(super) online_copy: bool,
    pub(super) constraints: bool,
    pub(super) metadata_atomic: bool,
    pub(super) read_compatible_metadata: bool,
}

/// One immutable catalog replacement, with no row-layout or resource-lifetime
/// change. Compound ALTER still uses the stronger protocol until its in-memory
/// candidate is published atomically as a whole, rather than action by action.
fn read_compatible_metadata(stmt: &AlterTable) -> bool {
    if !stmt.partition_actions.is_empty() || stmt.actions.len() != 1 {
        return false;
    }
    match &stmt.actions[0] {
        AlterAction::SetColumnComment { .. }
        | AlterAction::SetColumnDefault { .. }
        | AlterAction::SetColumnVisibility { .. }
        | AlterAction::AlterIndexVisibility { .. }
        | AlterAction::SetTableOptions { .. }
        | AlterAction::SetStorageProperties { .. }
        | AlterAction::SetAutoIncrement(_)
        | AlterAction::Noop { .. } => true,
        _ => false,
    }
}

fn read_compatible_column_action(stmt: &AlterTable) -> Option<&AlterAction> {
    let action = stmt.actions.first()?;
    let column = match action {
        AlterAction::AddColumn(column) | AlterAction::ModifyColumn(column) => column,
        AlterAction::ChangeColumn { old_name, new_def }
            if old_name.eq_ignore_ascii_case(&new_def.name) =>
        {
            new_def
        }
        _ => return None,
    };
    match stmt.actions.as_slice() {
        [_] => Some(action),
        [_, AlterAction::MoveColumn { column: name, .. }]
            if name.eq_ignore_ascii_case(&column.name) && column.checks.is_empty() =>
        {
            Some(action)
        }
        _ => None,
    }
}

pub(super) fn plan_persistent_alter(
    db: &Database,
    session: &SessionContext,
    stmt: &AlterTable,
) -> Result<AlterExecutionPlan, QueryError> {
    let table = qualify_persistent_table_name(session, &stmt.table);
    let mut constraints = stmt.partition_actions.is_empty()
        && Database::constraint_alter_supported(&stmt.actions)
        && stmt.algorithm != AlterAlgorithm::Copy;
    let indexes = !stmt.actions.is_empty()
        && stmt
            .actions
            .iter()
            .all(|a| matches!(a, AlterAction::AddIndex(_)))
        && stmt.partition_actions.is_empty()
        && stmt.algorithm != AlterAlgorithm::Copy;
    let btree_indexes = indexes
        && stmt.actions.iter().all(
            |a| matches!(a, AlterAction::AddIndex(i) if i.index_type.eq_ignore_ascii_case("BTREE")),
        );
    // A later action can bind a column introduced by an earlier action. Such
    // actions cannot be classified against the live source descriptor. The
    // private compound candidate owns their type/dependency validation and is
    // conservatively executed as one physical rewrite.
    let mut introduced = HashSet::new();
    let mut candidate_binding = false;
    for action in &stmt.actions {
        match action {
            AlterAction::AddColumn(column) => {
                introduced.insert(column.name.to_ascii_lowercase());
            }
            AlterAction::ModifyColumn(column) => {
                candidate_binding |= introduced.contains(&column.name.to_ascii_lowercase());
            }
            AlterAction::ChangeColumn { old_name, new_def } => {
                candidate_binding |= introduced.remove(&old_name.to_ascii_lowercase());
                if !old_name.eq_ignore_ascii_case(&new_def.name) {
                    introduced.insert(new_def.name.to_ascii_lowercase());
                }
            }
            AlterAction::DropColumn(name) => {
                candidate_binding |= introduced.remove(&name.to_ascii_lowercase());
            }
            _ => {}
        }
    }
    let mut required = if candidate_binding {
        AlterAlgorithm::Copy
    } else {
        AlterAlgorithm::Instant
    };
    let mut validation_scan = false;
    for action in &stmt.actions {
        if candidate_binding {
            continue;
        }
        let work =
            if session.is_strict_sql_mode() && db.alter_action_validation_only(&table, action)? {
                validation_scan = true;
                AlterAlgorithm::Inplace
            } else if db.alter_action_requires_copy(&table, action)? {
                AlterAlgorithm::Copy
            } else if let AlterAction::DropColumn(name) = action {
                if db.alter_drop_column_rebuilds_index(&table, name)? {
                    AlterAlgorithm::Inplace
                } else {
                    algorithm_for_action(action)
                }
            } else {
                algorithm_for_action(action)
            };
        if alter_algorithm_rank(work) > alter_algorithm_rank(required) {
            required = work;
        }
    }
    for action in &stmt.partition_actions {
        let work = algorithm_for_partition_action(action);
        if alter_algorithm_rank(work) > alter_algorithm_rank(required) {
            required = work;
        }
    }
    let metadata_atomic = required != AlterAlgorithm::Copy
        && !stmt.actions.is_empty()
        && stmt.partition_actions.is_empty()
        && stmt.actions.iter().all(|a| {
            db.alter_action_is_metadata_reversible(&table, a)
                .unwrap_or(false)
        });
    let column_validation = validation_scan
        && metadata_atomic
        && stmt.algorithm != AlterAlgorithm::Copy
        && stmt.partition_actions.is_empty()
        && stmt.actions.iter().all(|action| match action {
            AlterAction::ModifyColumn(column) => column.checks.is_empty(),
            AlterAction::ChangeColumn { old_name, new_def } => {
                old_name.eq_ignore_ascii_case(&new_def.name) && new_def.checks.is_empty()
            }
            AlterAction::MoveColumn { .. } => true,
            _ => false,
        });
    constraints |= column_validation;
    let fused_shape = stmt.actions.iter().all(|a| {
        matches!(
            a,
            AlterAction::Rebuild
                | AlterAction::AddColumn(_)
                | AlterAction::DropColumn(_)
                | AlterAction::ModifyColumn(_)
                | AlterAction::ChangeColumn { .. }
                | AlterAction::AddIndex(_)
                | AlterAction::DropIndex(_)
                | AlterAction::DropPrimaryKey
                | AlterAction::ConvertToCharset { .. }
                | AlterAction::MoveColumn { .. }
        )
    });
    if indexes && !btree_indexes && stmt.actions.len() > 1 {
        return Err(error(
            crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
            "atomic multi-index ALTER currently requires BTREE indexes",
        ));
    }
    let compound = stmt.actions.len() + stmt.partition_actions.len() > 1;
    let compound_copy = compound
        && !metadata_atomic
        && !constraints
        && !indexes
        && stmt.partition_actions.is_empty()
        && fused_shape;
    if compound && !metadata_atomic && !constraints && !indexes && !compound_copy {
        return Err(error(
            crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
            "this ALTER action mix has no atomic execution plan; split it into separate statements",
        ));
    }
    if compound_copy {
        required = AlterAlgorithm::Copy;
    }
    let physical_copy = required == AlterAlgorithm::Copy || stmt.algorithm == AlterAlgorithm::Copy;
    let online_copy = physical_copy
        && stmt.partition_actions.is_empty()
        && stmt.lock != AlterLock::Exclusive
        && db.online_type_copy_supported(
            &table,
            &stmt.actions,
            session.is_strict_sql_mode(),
            stmt.lock,
            physical_copy,
        );
    let partition_shared_build = stmt.actions.is_empty()
        && stmt.partition_actions.len() == 1
        && db.partition_action_shared_build_supported(&stmt.partition_actions[0]);
    let partition_online = partition_shared_build
        && db.partition_action_online_supported(&table, &stmt.partition_actions[0])?;
    let scan = physical_copy || required != AlterAlgorithm::Instant;
    if stmt.lock == AlterLock::None
        && scan
        && !constraints
        && !btree_indexes
        && !online_copy
        && !partition_online
    {
        return Err(error(crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
            "LOCK=NONE is not supported by this ALTER execution plan; use LOCK=SHARED where supported, otherwise LOCK=EXCLUSIVE"));
    }
    if stmt.lock == AlterLock::Shared
        && !partition_shared_build
        && scan
        && !constraints
        && !btree_indexes
        && !online_copy
        && !(validation_scan && metadata_atomic && read_compatible_column_action(stmt).is_some())
    {
        return Err(error(
            crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
            "LOCK=SHARED is not supported by this ALTER execution plan",
        ));
    }
    // Stable column IDs/physical slots and retained row layouts let existing
    // readers keep decoding across an append or encoding-compatible MODIFY.
    // Positioned ADD/MODIFY builds one combined candidate, so FIRST/AFTER
    // cannot expose a partially changed definition to concurrent readers.
    let read_compatible_column = !physical_copy
        && !column_validation
        && metadata_atomic
        && stmt.partition_actions.is_empty()
        && read_compatible_column_action(stmt).is_some();
    let read_compatible_metadata = !physical_copy
        && (read_compatible_metadata(stmt) || read_compatible_column)
        && stmt.lock != AlterLock::Exclusive;
    let mdl = if read_compatible_metadata {
        MdlLockType::SharedNoWrite
    } else if partition_shared_build && stmt.lock != AlterLock::Exclusive {
        // A separate transaction-duration SNW ticket is acquired by the
        // partition executor; retain SU to serialize competing DDL.
        MdlLockType::SharedUpgradable
    } else if constraints || btree_indexes || online_copy {
        match stmt.lock {
            AlterLock::Exclusive => MdlLockType::Exclusive,
            AlterLock::Shared => MdlLockType::SharedNoWrite,
            AlterLock::Default | AlterLock::None => MdlLockType::SharedUpgradable,
        }
    } else if validation_scan
        && metadata_atomic
        && stmt.actions.len() == 1
        && stmt.lock == AlterLock::Shared
    {
        MdlLockType::SharedNoWrite
    } else if indexes && stmt.lock != AlterLock::Exclusive {
        MdlLockType::SharedNoWrite
    } else {
        MdlLockType::Exclusive
    };
    Ok(AlterExecutionPlan {
        required,
        mdl,
        physical_copy,
        online_copy,
        constraints,
        metadata_atomic,
        read_compatible_metadata,
    })
}

/// Acquire a definition-stabilizing ticket before inspecting the live schema.
/// The execution plan strengthens it after semantic validation. Choosing the
/// initial ticket from data-dependent capabilities could become stale while
/// waiting (e.g. a previous ALTER changes COPY into metadata-only work).
pub(super) fn alter_table_statement_mdl(
    db: &Database,
    _session: &SessionContext,
    stmt: &AlterTable,
) -> Option<MdlLockType> {
    Some(match stmt.lock {
        AlterLock::Default | AlterLock::Shared if stmt.actions.is_empty()
            && stmt.partition_actions.len() == 1
            && db.partition_action_shared_build_supported(&stmt.partition_actions[0]) =>
                MdlLockType::SharedUpgradable,
        AlterLock::Exclusive => MdlLockType::Exclusive,
        // Acquire DDL ownership first; execution adds SNW as an incomparable
        // ticket. Starting with SNW would let two LOCK=SHARED DDLs race.
        AlterLock::Shared => MdlLockType::SharedUpgradable,
        AlterLock::Default if stmt.actions.iter().any(|action|
            matches!(action, AlterAction::AddIndex(i) if !i.index_type.eq_ignore_ascii_case("BTREE"))) => MdlLockType::SharedNoWrite,
        AlterLock::Default | AlterLock::None => MdlLockType::SharedUpgradable,
    })
}
