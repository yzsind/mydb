//! Snapshot-specific index and nullability facts for online additive DDL.
//!
//! This scope never changes the catalog. A read gets a private descriptor
//! only when its snapshot predates index coverage or NOT NULL validation.
use std::cell::Cell;
use std::sync::Arc;

use super::schema::{IndexState, TableSchema};

thread_local! {
    static SNAPSHOT: Cell<Option<u64>> = const { Cell::new(None) };
}

pub(crate) struct ReadSchemaScope {
    previous: Option<u64>,
    _thread_bound: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl ReadSchemaScope {
    pub(crate) fn enter(snapshot: Option<u64>) -> Self {
        Self {
            previous: SNAPSHOT.with(|slot| slot.replace(snapshot)),
            _thread_bound: std::marker::PhantomData,
        }
    }
}

impl Drop for ReadSchemaScope {
    fn drop(&mut self) {
        SNAPSHOT.with(|slot| slot.set(self.previous));
    }
}

pub(crate) fn for_read(mut table: Arc<TableSchema>, highwater: u64) -> Arc<TableSchema> {
    if highwater == 0 {
        return table;
    }
    let Some(snapshot) = SNAPSHOT.with(Cell::get) else {
        return table;
    };
    if snapshot >= highwater {
        return table;
    }
    let historical_nulls = table
        .columns
        .iter()
        .any(|c| !c.nullable && c.not_null_from_scn > snapshot);
    if historical_nulls
        || table
            .indexes
            .iter()
            .any(|i| i.state == IndexState::Public && i.readable_from_scn > snapshot)
    {
        let table = Arc::make_mut(&mut table);
        if table
            .indexes
            .iter()
            .any(|index| index.primary && index.readable_from_scn > snapshot)
        {
            table.primary_key.clear();
            // This private view still uses the unchanged heap generation;
            // it simply cannot use the new primary-key optimizer facts.
            table.primary_definition_scn = 0;
        }
        for column in &mut table.columns {
            if column.not_null_from_scn > snapshot {
                column.nullable = true;
                column.not_null_from_scn = 0;
            }
        }
        for index in &mut table.indexes {
            if index.readable_from_scn > snapshot {
                // Preserve the definition for name/hint validation, but do not
                // advertise an incomplete historical access path to the CBO.
                index.state = IndexState::Building;
                index.unique = false;
                index.primary = false;
            }
        }
    }
    table
}
