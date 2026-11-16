//! Master-side subsystems for mydb-native physical replication:
//! replication slots (per-replica acked-LSN tracking), the WAL retention
//! policy that turns the slots' progress into a concrete pruning
//! decision, plus (later phases) the per-replica WAL sender task and
//! the public listening service.

pub mod retention;
pub mod sender;
pub mod service;
pub mod slot;

pub use retention::{RetentionDecision, RetentionPolicy};
pub use sender::{SenderEnv, SenderTracker, SenderTrackerSnapshot};
pub use service::{
    start as start_master_service, start_bound as start_bound_master_service, MasterService,
    MasterServiceInit,
};
pub use slot::{SlotHandle, SlotRegistry, SlotState, SlotStatus};
