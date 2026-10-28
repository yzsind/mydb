// Flat-combining publication of concurrently written undo frames.
//
// Every row-level DML publishes one undo record into the shared undo
// directory under its exclusive latch. The critical section is a few
// microseconds, but tens of thousands of publications per second turn each
// latch hand-off (park, wake, reschedule) into the dominant cost: waiters
// spent far longer queued than the directory was actually held.
//
// Publishers instead enqueue their request and one combiner at a time takes
// the latch and publishes every queued request in a single hold, then wakes
// their owners directly. Each request keeps its own result, so admission
// failures still reach the statement that caused them.

type UndoPublishResult = Result<(u64, Vec<u8>, bool), QueryError>;

struct UndoPublishRequest {
    record: crate::storage::undo::UndoRecord,
    encoded: Vec<u8>,
    ptr: u64,
    segment_id: u32,
    segment_end: u64,
    reservation: crate::storage::undo::UndoResidentReservation,
}

enum UndoPublishState {
    Queued(UndoPublishRequest),
    Taken,
    Done(UndoPublishResult),
}

struct UndoPublishSlot {
    state: parking_lot::Mutex<UndoPublishState>,
    done: AtomicBool,
    owner: std::thread::Thread,
}

#[derive(Default)]
pub(crate) struct UndoPublishQueue {
    pending: parking_lot::Mutex<Vec<Arc<UndoPublishSlot>>>,
    /// Set while one publisher acts as the combiner for the queue.
    combining: AtomicBool,
}

impl std::fmt::Debug for UndoPublishQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UndoPublishQueue")
            .field("pending", &self.pending.lock().len())
            .field("combining", &self.combining.load(Ordering::Relaxed))
            .finish()
    }
}

/// Upper bound on one combiner's hold so readers and purge are not starved
/// behind an unbounded stream of queued publications.
const UNDO_PUBLISH_COMBINE_LIMIT: usize = 256;

/// Busy-wait budget (a few microseconds) before a waiter parks.
const UNDO_PUBLISH_SPINS: u32 = 256;

/// Backstop for a waiter whose combiner cannot reach it; normal completion
/// unparks the owner directly.
const UNDO_PUBLISH_PARK: Duration = Duration::from_micros(200);

impl Database {
    /// Publish a frame already written to its rollback segment, combining
    /// with concurrent publishers under one exclusive directory hold.
    #[allow(clippy::too_many_arguments)]
    fn publish_concurrent_undo_frame(
        &self,
        record: crate::storage::undo::UndoRecord,
        encoded: Vec<u8>,
        ptr: u64,
        segment_id: u32,
        segment_end: u64,
        reservation: crate::storage::undo::UndoResidentReservation,
    ) -> UndoPublishResult {
        let slot = Arc::new(UndoPublishSlot {
            state: parking_lot::Mutex::new(UndoPublishState::Queued(UndoPublishRequest {
                record,
                encoded,
                ptr,
                segment_id,
                segment_end,
                reservation,
            })),
            done: AtomicBool::new(false),
            owner: std::thread::current(),
        });
        let queue = &self.undo_publish;
        queue.pending.lock().push(Arc::clone(&slot));
        loop {
            if slot.done.load(Ordering::Acquire) {
                break;
            }
            if queue
                .combining
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.combine_undo_publications();
                continue;
            }
            // A running combiner finishes a batch within microseconds. Spin
            // briefly before parking: a park/unpark round trip costs two
            // context switches, more than the wait it replaces.
            let mut spins = 0;
            while spins < UNDO_PUBLISH_SPINS
                && !slot.done.load(Ordering::Acquire)
                && queue.combining.load(Ordering::Relaxed)
            {
                std::hint::spin_loop();
                spins += 1;
            }
            if spins == UNDO_PUBLISH_SPINS {
                std::thread::park_timeout(UNDO_PUBLISH_PARK);
            }
        }
        let state = std::mem::replace(&mut *slot.state.lock(), UndoPublishState::Taken);
        match state {
            UndoPublishState::Done(result) => result,
            UndoPublishState::Queued(_) | UndoPublishState::Taken => Err(error(
                crate::error::catalog::ER_GET_ERRNO,
                "undo publication completed without a result",
            )),
        }
    }

    /// Run as the combiner (the caller won `combining`): drain and publish
    /// until the queue is empty, then hand the role back. Re-checking the
    /// queue after releasing the role closes the window in which a publisher
    /// enqueued after the last drain but saw the role still taken.
    fn combine_undo_publications(&self) {
        let queue = &self.undo_publish;
        loop {
            let mut completed = Vec::new();
            {
                let mut undo = self.undo.write();
                loop {
                    let batch = {
                        let mut pending = queue.pending.lock();
                        if pending.is_empty() {
                            break;
                        }
                        let take = pending.len().min(UNDO_PUBLISH_COMBINE_LIMIT);
                        pending.drain(..take).collect::<Vec<_>>()
                    };
                    for slot in batch {
                        let state =
                            std::mem::replace(&mut *slot.state.lock(), UndoPublishState::Taken);
                        let request = match state {
                            UndoPublishState::Queued(request) => request,
                            other => {
                                *slot.state.lock() = other;
                                continue;
                            }
                        };
                        let result = undo.finish_concurrent_persistent_append(
                            request.record,
                            request.encoded,
                            request.ptr,
                            request.segment_id,
                            request.segment_end,
                            request.reservation,
                        );
                        *slot.state.lock() = UndoPublishState::Done(result);
                        completed.push(slot);
                    }
                    if completed.len() >= UNDO_PUBLISH_COMBINE_LIMIT {
                        break;
                    }
                }
            }
            for slot in completed {
                slot.done.store(true, Ordering::Release);
                slot.owner.unpark();
            }
            queue.combining.store(false, Ordering::SeqCst);
            if queue.pending.lock().is_empty()
                || queue
                    .combining
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
            {
                return;
            }
        }
    }
}
