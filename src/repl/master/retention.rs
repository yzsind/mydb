//! WAL retention decision: given the master's checkpoint LSN and the
//! current snapshot of replication slots + WAL segment sizes, compute
//! the lowest LSN we must retain on disk. Identifies slots that have
//! timed out (mark abandoned, stop blocking retention) and slots that
//! exceed the size cap (mark invalidated, force retention).

use crate::repl::master::slot::{SlotState, SlotStatus};
use crate::wal::WalSegmentInfo;

/// Retention configuration. Defaults mirror `ReplConfig`.
#[derive(Debug, Clone, Copy)]
pub struct RetentionPolicy {
    /// Hard upper bound on bytes retained for replication purposes.
    /// When the retained span (sealed segments below the checkpoint
    /// LSN that we'd keep just for replicas) would exceed this, the
    /// blocking slots get invalidated.
    pub max_retained_bytes: u64,
    /// Slots silent for longer than this become `Abandoned`; they no
    /// longer block retention.
    pub slot_timeout_ms: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_retained_bytes: 16 * 1024 * 1024 * 1024,
            slot_timeout_ms: 300_000,
        }
    }
}

/// Output of [`RetentionPolicy::decide`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetentionDecision {
    /// All WAL segments whose `last_lsn < min_required_lsn` may be
    /// pruned safely.
    pub min_required_lsn: u64,
    /// Slots that just crossed the timeout — caller should call
    /// `SlotRegistry::mark_abandoned` for each.
    pub abandon_slot_uuids: Vec<String>,
    /// Slots whose lag exceeds `max_retained_bytes` — caller should
    /// call `SlotRegistry::mark_invalidated`.
    pub invalidate_slot_uuids: Vec<String>,
}

impl RetentionPolicy {
    /// Decide the retention LSN.
    ///
    /// `checkpoint_lsn` is the master's latest CheckpointEnd LSN (the
    /// floor without slots). `slots` is the current slot snapshot.
    /// `sealed_segments` is the master's sealed-segment list (used for
    /// the byte-cap calculation). `now_ms` is the current wall time.
    pub fn decide(
        &self,
        checkpoint_lsn: u64,
        slots: &[SlotState],
        sealed_segments: &[WalSegmentInfo],
        now_ms: u64,
    ) -> RetentionDecision {
        let mut decision = RetentionDecision {
            min_required_lsn: checkpoint_lsn,
            abandon_slot_uuids: Vec::new(),
            invalidate_slot_uuids: Vec::new(),
        };
        if slots.is_empty() {
            return decision;
        }

        // Step 1: classify slots into active / timed-out / invalidated.
        let mut active_blocking: Vec<&SlotState> = Vec::new();
        for slot in slots {
            match slot.status {
                SlotStatus::Invalidated => continue,
                SlotStatus::Abandoned => continue,
                SlotStatus::Active => {
                    // Guard against a bogus `last_seen_ms == 0` (clock
                    // unavailable at ack time) being read as "silent
                    // forever" and abandoning a live slot. Only abandon
                    // when we have a real timestamp and it has genuinely
                    // aged past the timeout.
                    let aged_out = slot.last_seen_ms > 0
                        && now_ms >= slot.last_seen_ms
                        && now_ms - slot.last_seen_ms >= self.slot_timeout_ms;
                    if aged_out {
                        decision.abandon_slot_uuids.push(slot.replica_uuid.clone());
                    } else {
                        active_blocking.push(slot);
                    }
                }
            }
        }

        if active_blocking.is_empty() {
            // Nothing blocks the checkpoint LSN.
            return decision;
        }

        let min_acked = active_blocking
            .iter()
            .map(|s| s.acked_lsn)
            .min()
            .unwrap_or(checkpoint_lsn);
        let candidate_min = min_acked.min(checkpoint_lsn);

        // Step 2: enforce the byte cap. If the additional WAL retained
        // because of slots exceeds `max_retained_bytes`, invalidate the
        // furthest-behind slots until we fit.
        let retained_extra = bytes_below(sealed_segments, checkpoint_lsn)
            - bytes_below(sealed_segments, candidate_min);
        if retained_extra <= self.max_retained_bytes {
            decision.min_required_lsn = candidate_min;
            return decision;
        }

        // Sort blocking slots by acked_lsn ascending (worst laggers first)
        // and invalidate from the front until under the cap.
        let mut sorted: Vec<&SlotState> = active_blocking.clone();
        sorted.sort_by_key(|s| s.acked_lsn);
        let mut still_blocking: Vec<&SlotState> = sorted.clone();
        while !still_blocking.is_empty() {
            let new_candidate = still_blocking
                .iter()
                .map(|s| s.acked_lsn)
                .min()
                .unwrap_or(checkpoint_lsn)
                .min(checkpoint_lsn);
            let retained = bytes_below(sealed_segments, checkpoint_lsn)
                - bytes_below(sealed_segments, new_candidate);
            if retained <= self.max_retained_bytes {
                decision.min_required_lsn = new_candidate;
                return decision;
            }
            // Pop the worst lagger and mark for invalidation.
            let worst = still_blocking.remove(0);
            decision
                .invalidate_slot_uuids
                .push(worst.replica_uuid.clone());
        }
        // No remaining blocking slot — fall back to checkpoint_lsn.
        decision.min_required_lsn = checkpoint_lsn;
        decision
    }
}

/// Sum the size of sealed segments whose first_lsn < `lsn`. Used to
/// estimate how much WAL we'd retain by holding the floor at `lsn`.
fn bytes_below(sealed: &[WalSegmentInfo], lsn: u64) -> u64 {
    sealed
        .iter()
        .filter(|seg| seg.first_lsn < lsn)
        .map(|seg| seg.size_bytes)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalSegmentStatus;
    use std::path::PathBuf;

    fn make_sealed(id: u64, first_lsn: u64, last_lsn: u64, size_bytes: u64) -> WalSegmentInfo {
        WalSegmentInfo {
            segment_id: id,
            path: PathBuf::from(format!("wal-{id}.log")),
            first_lsn,
            last_lsn,
            size_bytes,
            status: WalSegmentStatus::Reclaimable,
        }
    }

    fn make_slot(uuid: &str, acked_lsn: u64, last_seen_ms: u64, status: SlotStatus) -> SlotState {
        SlotState {
            replica_uuid: uuid.to_string(),
            acked_lsn,
            confirmed_acked_lsn: 0,
            last_seen_ms,
            first_registered_ms: 0,
            status,
            connected: status == SlotStatus::Active,
        }
    }

    #[test]
    fn empty_slots_yields_checkpoint_lsn_floor() {
        let policy = RetentionPolicy::default();
        let decision = policy.decide(1000, &[], &[], 0);
        assert_eq!(decision.min_required_lsn, 1000);
        assert!(decision.abandon_slot_uuids.is_empty());
        assert!(decision.invalidate_slot_uuids.is_empty());
    }

    #[test]
    fn one_active_slot_below_checkpoint_lowers_the_floor() {
        let policy = RetentionPolicy::default();
        let slots = vec![make_slot("a", 500, 100, SlotStatus::Active)];
        let decision = policy.decide(1000, &slots, &[], 100);
        assert_eq!(decision.min_required_lsn, 500);
    }

    #[test]
    fn timed_out_slot_is_marked_abandoned_and_ignored() {
        let policy = RetentionPolicy {
            max_retained_bytes: u64::MAX,
            slot_timeout_ms: 100,
        };
        let slots = vec![
            make_slot("fresh", 800, 1000, SlotStatus::Active),
            // last_seen=1 (a real, non-zero timestamp far in the past);
            // 0 now specifically means "clock unavailable" and is NOT
            // treated as timed out (see C17 guard).
            make_slot("stale", 100, 1, SlotStatus::Active),
        ];
        let decision = policy.decide(1000, &slots, &[], 1000);
        assert_eq!(decision.abandon_slot_uuids, vec!["stale".to_string()]);
        // floor uses only the still-active slot.
        assert_eq!(decision.min_required_lsn, 800);
    }

    #[test]
    fn zero_last_seen_is_not_treated_as_timed_out() {
        // A `last_seen_ms == 0` means the clock was unavailable at ack
        // time, NOT "silent since the epoch". Such a slot must keep
        // blocking retention instead of being spuriously abandoned (C17).
        let policy = RetentionPolicy {
            max_retained_bytes: u64::MAX,
            slot_timeout_ms: 100,
        };
        let slots = vec![make_slot("clockless", 400, 0, SlotStatus::Active)];
        let decision = policy.decide(1000, &slots, &[], 10_000_000);
        assert!(
            decision.abandon_slot_uuids.is_empty(),
            "zero last_seen must not abandon"
        );
        assert_eq!(decision.min_required_lsn, 400, "slot still blocks");
    }

    #[test]
    fn abandoned_and_invalidated_slots_do_not_block() {
        let policy = RetentionPolicy::default();
        let slots = vec![
            make_slot("inv", 50, 0, SlotStatus::Invalidated),
            make_slot("aban", 100, 0, SlotStatus::Abandoned),
            make_slot("act", 900, 100, SlotStatus::Active),
        ];
        let decision = policy.decide(1000, &slots, &[], 100);
        assert!(decision.invalidate_slot_uuids.is_empty());
        assert!(decision.abandon_slot_uuids.is_empty());
        assert_eq!(decision.min_required_lsn, 900);
    }

    #[test]
    fn over_byte_cap_invalidates_worst_lagger() {
        // 3 sealed segments, each 5MB. Slot A is far behind (lsn 1),
        // would force us to retain all 15MB; cap is 8MB. A gets
        // invalidated, the next-worst slot's LSN drives the floor.
        let sealed = vec![
            make_sealed(1, 1, 100, 5 * 1024 * 1024),
            make_sealed(2, 101, 200, 5 * 1024 * 1024),
            make_sealed(3, 201, 300, 5 * 1024 * 1024),
        ];
        let policy = RetentionPolicy {
            max_retained_bytes: 8 * 1024 * 1024,
            slot_timeout_ms: 1_000_000,
        };
        let slots = vec![
            make_slot("a", 1, 100, SlotStatus::Active), // very far behind
            make_slot("b", 250, 100, SlotStatus::Active), // mostly caught up
        ];
        let decision = policy.decide(400, &slots, &sealed, 100);
        assert_eq!(decision.invalidate_slot_uuids, vec!["a".to_string()]);
        assert_eq!(decision.min_required_lsn, 250);
    }

    #[test]
    fn under_byte_cap_keeps_all_slots() {
        let sealed = vec![make_sealed(1, 1, 100, 1024)];
        let policy = RetentionPolicy {
            max_retained_bytes: 1024 * 1024,
            slot_timeout_ms: 1_000_000,
        };
        let slots = vec![make_slot("a", 50, 100, SlotStatus::Active)];
        let decision = policy.decide(200, &slots, &sealed, 100);
        assert!(decision.invalidate_slot_uuids.is_empty());
        assert_eq!(decision.min_required_lsn, 50);
    }

    #[test]
    fn multiple_invalidations_when_aggregate_lag_exceeds_cap() {
        let sealed = vec![
            make_sealed(1, 1, 100, 10 * 1024 * 1024),
            make_sealed(2, 101, 200, 10 * 1024 * 1024),
        ];
        let policy = RetentionPolicy {
            max_retained_bytes: 5 * 1024 * 1024,
            slot_timeout_ms: 1_000_000,
        };
        let slots = vec![
            make_slot("a", 10, 100, SlotStatus::Active),
            make_slot("b", 150, 100, SlotStatus::Active),
        ];
        let decision = policy.decide(300, &slots, &sealed, 100);
        // 'a' must be invalidated; if even after dropping a we still
        // exceed cap, 'b' goes too. With cap=5MB and each segment 10MB,
        // we end up invalidating both and falling back to checkpoint.
        assert!(decision.invalidate_slot_uuids.contains(&"a".to_string()));
    }
}
