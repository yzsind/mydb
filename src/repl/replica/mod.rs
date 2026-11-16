//! Replica-side subsystems: the receiver that connects to the master's
//! WAL stream, the applier that writes incoming frames to the local
//! WAL (and — in later phases — into the running storage layer), and
//! the persistence of `last_applied_lsn`.

pub mod applier;
pub mod progress;
pub mod receiver;

pub use applier::{Applier, ApplyOutcome};
pub use progress::{load_progress, save_progress, ReplicaProgress, PROGRESS_FILE};
pub use receiver::{Receiver, ReceiverEvent, ReceiverHandle};
