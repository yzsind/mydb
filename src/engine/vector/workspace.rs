//! Query-owned vector workspace, with global admission for internal callers.
use crate::engine::executor::mem_budget::{
    self, ControlledMemoryCategory, ControlledMemoryCharge, MemBudget,
};
use crate::types::QueryError;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct VectorQueryCharge {
    budget: Option<Arc<MemBudget>>,
    internal: Option<ControlledMemoryCharge>,
    bytes: usize,
}
impl VectorQueryCharge {
    pub(crate) fn try_new(bytes: usize) -> Result<Self, QueryError> {
        Self::try_new_recoverable(bytes)
    }
    pub(crate) fn try_new_recoverable(bytes: usize) -> Result<Self, QueryError> {
        let budget = mem_budget::current_budget();
        let internal = if let Some(budget) = &budget {
            budget.charge(bytes)?;
            None
        } else {
            Some(ControlledMemoryCharge::try_new_recoverable(
                ControlledMemoryCategory::StorageScratch,
                bytes,
            )?)
        };
        Ok(Self {
            budget,
            internal,
            bytes,
        })
    }
    pub(crate) fn try_grow(&mut self, bytes: usize) -> Result<(), QueryError> {
        self.try_grow_recoverable(bytes)
    }
    pub(crate) fn try_grow_recoverable(&mut self, bytes: usize) -> Result<(), QueryError> {
        if let Some(budget) = &self.budget {
            budget.charge(bytes)?;
        } else if let Some(internal) = &mut self.internal {
            internal.try_grow_recoverable(bytes)?;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
    pub(crate) fn shrink(&mut self, bytes: usize) {
        let bytes = bytes.min(self.bytes);
        if let Some(budget) = &self.budget {
            budget.release(bytes);
        } else if let Some(internal) = &mut self.internal {
            internal.shrink(bytes);
        }
        self.bytes -= bytes;
    }
    pub(crate) fn absorb(&mut self, other: &mut Self) -> Result<(), QueryError> {
        match (&self.budget, &other.budget) {
            (Some(a), Some(b)) if Arc::ptr_eq(a, b) => {
                self.bytes = self.bytes.saturating_add(other.bytes);
                other.bytes = 0;
            }
            (None, None) => {
                self.internal
                    .as_mut()
                    .unwrap()
                    .absorb(other.internal.as_mut().unwrap());
                self.bytes = self.bytes.saturating_add(other.bytes);
                other.bytes = 0;
            }
            _ => {
                let bytes = other.bytes;
                self.try_grow_recoverable(bytes)?;
                other.shrink(bytes);
            }
        }
        Ok(())
    }
}
impl Drop for VectorQueryCharge {
    fn drop(&mut self) {
        if let Some(budget) = &self.budget {
            budget.release(self.bytes);
        }
    }
}
