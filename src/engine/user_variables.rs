//! Session variables share governed immutable payloads with expression/PFS snapshots.
use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::executor::mem_budget::{self, ControlledMemoryCategory, ControlledMemoryCharge};
use crate::types::{QueryError, Value};

#[derive(Debug)]
struct Entry {
    value: Value,
    _charge: ControlledMemoryCharge,
}

#[derive(Debug, Clone, Default)]
pub struct UserVariables {
    values: Arc<HashMap<String, Arc<Entry>>>,
}

impl UserVariables {
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.values.get(name).map(|entry| &entry.value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.values.iter().map(|(name, entry)| (name, &entry.value))
    }

    pub(crate) fn insert(&mut self, name: String, value: Value) -> Result<(), QueryError> {
        // Cover the owned payload, key, map slot and the small shared-entry
        // headers. Snapshots retain this lease for exactly the payload lifetime.
        let bytes = mem_budget::estimate_value_bytes(&value)
            .saturating_add(name.capacity())
            .saturating_add(192);
        let charge = ControlledMemoryCharge::try_new_recoverable(
            ControlledMemoryCategory::ProtocolTransient,
            bytes,
        )?;
        let values = Arc::make_mut(&mut self.values);
        values
            .try_reserve(1)
            .map_err(|_| mem_budget::allocation_error("user variable map"))?;
        values.insert(
            name,
            Arc::new(Entry {
                value,
                _charge: charge,
            }),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit1106_user_variable_snapshots_share_and_release_payloads() {
        let before = mem_budget::controlled_memory_snapshot().0;
        let mut vars = UserVariables::default();
        vars.insert("large".into(), Value::String("x".repeat(1024 * 1024)))
            .unwrap();
        let charged = mem_budget::controlled_memory_snapshot().0;
        assert!(charged >= before + 1024 * 1024);
        let snapshot = vars.clone();
        assert_eq!(mem_budget::controlled_memory_snapshot().0, charged);
        vars.insert("large".into(), Value::Int(3)).unwrap();
        assert!(matches!(snapshot.get("large"), Some(Value::String(s)) if s.len() == 1024 * 1024));
        assert_eq!(vars.get("large"), Some(&Value::Int(3)));
        drop(snapshot);
        assert!(mem_budget::controlled_memory_snapshot().0 < charged);
        drop(vars);
        assert_eq!(mem_budget::controlled_memory_snapshot().0, before);
    }
}
