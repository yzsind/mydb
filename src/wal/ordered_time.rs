//! Assign decision times at the WAL append serialization point. Wide payloads
//! are hashed outside the mutex; only an eight-byte patch is hashed inside it.
use super::*;

pub(crate) struct TimedRecord {
    record: PreEncodedRecord,
    offset: usize,
    checksum: Option<(crc32fast::Hasher, PayloadChecksum, CrcShift)>,
}

impl TimedRecord {
    #[inline]
    pub(crate) fn new(record: PreEncodedRecord) -> Result<Self, QueryError> {
        let offset = match &record.payload {
            WalPayload::CommitTxn { .. } => WAL_FRAME_HEADER_LEN + 8,
            WalPayload::SequenceState { .. } => WAL_FRAME_HEADER_LEN + 25,
            WalPayload::TxnBatch { records } => {
                if !matches!(records.last(), Some(WalPayload::CommitTxn { .. })) {
                    return Err(error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "timed batch must end with CommitTxn",
                    ));
                }
                if read_u32(&record.bytes, WAL_FRAME_HEADER_LEN) & COMPRESSED_TXN_BATCH != 0 {
                    WAL_FRAME_HEADER_LEN + 8
                } else {
                    let mut offset = WAL_FRAME_HEADER_LEN + 4;
                    for _ in 0..records.len() - 1 {
                        offset += 6 + read_u32(&record.bytes, offset + 2) as usize;
                    }
                    offset + 6 + 8
                }
            }
            _ => {
                return Err(error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "unsupported timed WAL record",
                ))
            }
        };
        // Match ordinary pre-encoding: a tiny frame is cheaper to hash once
        // after patching its header than to combine three separate CRCs.
        let checksum = (record.bytes.len() - WAL_FRAME_HEADER_LEN > 256).then(|| {
            let mut prefix = crc32fast::Hasher::new();
            prefix.update(&record.bytes[WAL_FRAME_HEADER_LEN..offset]);
            let suffix = PayloadChecksum::new(&record.bytes[offset + 8..]);
            let shift = CrcShift::for_len(record.bytes.len() - WAL_FRAME_HEADER_LEN);
            (prefix, suffix, shift)
        });
        Ok(Self {
            record,
            offset,
            checksum,
        })
    }
}

/// Called while holding the database's WAL mutex, including snapshot fences.
#[inline]
pub(crate) fn advance_ordered_time(clock: &AtomicU64) -> u64 {
    advance_time(clock, None)
}

#[inline]
fn wall_time_unix_us() -> u64 {
    #[cfg(all(unix, target_pointer_width = "64"))]
    {
        // Read the precise microsecond realtime domain directly. Converting
        // through SystemTime's nanosecond representation adds work inside the
        // append mutex without adding precision to the persisted timestamp.
        let mut time = std::mem::MaybeUninit::<libc::timeval>::uninit();
        // SAFETY: the output pointer is valid; a null timezone requests no
        // timezone output. Success initializes both timeval fields.
        if unsafe { libc::gettimeofday(time.as_mut_ptr(), std::ptr::null_mut()) } == 0 {
            // SAFETY: gettimeofday succeeded above.
            let time = unsafe { time.assume_init() };
            if time.tv_sec < 0 {
                return 0;
            }
            return (time.tv_sec as u64)
                .saturating_mul(1_000_000)
                .saturating_add(time.tv_usec as u64);
        }
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64
}

#[inline]
fn advance_time(clock: &AtomicU64, rollbacks: Option<&AtomicU64>) -> u64 {
    let wall = wall_time_unix_us();
    let observed = clock.load(Ordering::Relaxed);
    let next = wall.max(observed.saturating_add(1));
    if wall <= observed {
        if let Some(counter) = rollbacks {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }
    clock.store(next, Ordering::Release);
    next
}

impl LogManager {
    #[cfg(test)]
    #[inline]
    pub(crate) fn append_timed(
        &mut self,
        mut timed: TimedRecord,
        clock: &AtomicU64,
        rollbacks: Option<&AtomicU64>,
    ) -> Result<(u64, u64), QueryError> {
        self.append_timed_ref(&mut timed, clock, rollbacks)
    }

    pub(crate) fn append_timed_to(
        log: &Arc<LogMutex>,
        mut timed: TimedRecord,
        clock: &AtomicU64,
        rollbacks: Option<&AtomicU64>,
    ) -> Result<(u64, u64), QueryError> {
        let result = {
            let mut guard = log.lock();
            guard.append_timed_ref(&mut timed, clock, rollbacks)
        };
        drop(timed);
        result
    }

    fn append_timed_ref(
        &mut self,
        timed: &mut TimedRecord,
        clock: &AtomicU64,
        rollbacks: Option<&AtomicU64>,
    ) -> Result<(u64, u64), QueryError> {
        // An eager fallback may still owe its transaction's lazy BEGIN.
        // Publish it before assigning the decision time, and fence that time
        // above every preceding source frame even during a clock rollback.
        self.publish_buffered_begin(timed.record.txn_id)?;
        if !std::ptr::eq(&*self.source_clock, clock) {
            clock.fetch_max(self.source_clock.load(Ordering::Acquire), Ordering::Release);
        }
        let time = advance_time(clock, rollbacks);
        timed.record.bytes[timed.offset..timed.offset + 8].copy_from_slice(&time.to_le_bytes());
        timed.record.payload_checksum = timed.checksum.take().map(|(mut prefix, suffix, shift)| {
            prefix.update(&time.to_le_bytes());
            PayloadChecksum::from_crc(suffix.with_prefix(prefix.finalize()), shift)
        });
        if !std::ptr::eq(&*self.source_clock, clock) {
            self.source_clock.fetch_max(time, Ordering::Relaxed);
        }
        timed.record.source_time_us = time;
        let lsn = self.append_pre_encoded_ref(&mut timed.record)?;
        Ok((lsn, time))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_append_releases_the_wal_before_record_drop_on_success_and_error() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(root.path()).unwrap(),
        ));
        let observed = Arc::new(AtomicU64::new(0));
        let observe = |record: &mut PreEncodedRecord| {
            let log = Arc::clone(&log);
            let observed = Arc::clone(&observed);
            record.drop_observer = Some(Box::new(move || {
                if log.try_lock().is_some() {
                    observed.fetch_add(1, Ordering::Relaxed);
                }
            }));
        };
        let mut begin =
            LogManager::pre_encode_record(7, WalPayload::BeginTxn { start_scn: 1 }).unwrap();
        observe(&mut begin);
        let begin_lsn = LogManager::append_pre_encoded_to(&log, begin).unwrap();
        assert_eq!(observed.load(Ordering::Relaxed), 1);
        let mut tail = LogManager::pre_encode_timed_record(
            7,
            WalPayload::TxnBatch {
                records: vec![
                    WalPayload::FileCreate {
                        path: "owned.data".into(),
                        after: vec![7; 8192],
                    },
                    WalPayload::CommitTxn {
                        commit_scn: 2,
                        commit_unix_us: 0,
                        binlog: None,
                    },
                ],
            },
        )
        .unwrap();
        observe(&mut tail.record);
        let clock = AtomicU64::new(0);
        let (commit_lsn, time) = LogManager::append_timed_to(&log, tail, &clock, None).unwrap();
        assert!(commit_lsn > begin_lsn && time > 0);
        assert_eq!(observed.load(Ordering::Relaxed), 2);
        LogManager::sync_durable_to(&log, commit_lsn).unwrap();
        let records = LogManager::scan(root.path()).unwrap().records;
        assert_eq!(records.len(), 3);
        assert!(
            matches!(&records[1].payload, WalPayload::FileCreate { after, .. } if after == &vec![7; 8192])
        );
        assert!(
            matches!(records[2].payload, WalPayload::CommitTxn { commit_unix_us, .. } if commit_unix_us == time)
        );

        log.lock().read_only = true;
        let mut rejected = LogManager::pre_encode_record(0, WalPayload::CheckpointBegin).unwrap();
        observe(&mut rejected);
        assert!(LogManager::append_pre_encoded_to(&log, rejected).is_err());
        assert_eq!(observed.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn wall_time_uses_the_current_realtime_microsecond_domain() {
        let reference = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64
        };
        let before = reference();
        let wall = wall_time_unix_us();
        let after = reference();
        assert!(
            before <= wall && wall <= after,
            "{before} <= {wall} <= {after}"
        );
    }

    #[test]
    fn decision_time_fences_a_later_nondecision_source_clock() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut log = LogManager::open(root.path()).unwrap();
        let future = wall_time_unix_us() + 10_000_000;
        log.source_clock.store(future, Ordering::Release);
        let clock = AtomicU64::new(0);
        let timed = LogManager::pre_encode_timed_record(
            1,
            WalPayload::CommitTxn {
                commit_scn: 1,
                commit_unix_us: 0,
                binlog: None,
            },
        )
        .unwrap();
        let (_, decision) = log.append_timed(timed, &clock, None).unwrap();
        assert_eq!(decision, future + 1);
        log.flush().unwrap();
        let records = LogManager::scan(root.path()).unwrap().records;
        assert_eq!(records.last().unwrap().source_time_us, decision);
        assert!(matches!(records.last().unwrap().payload,
            WalPayload::CommitTxn { commit_unix_us, .. } if commit_unix_us == decision));
    }

    #[test]
    fn decision_times_follow_append_order_even_when_encoding_is_reversed() {
        let root = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let mut log = LogManager::open(root.path()).unwrap();
        let clock = AtomicU64::new(0);
        let a = TimedRecord::new(
            LogManager::pre_encode_record(
                1,
                WalPayload::CommitTxn {
                    commit_scn: 1,
                    commit_unix_us: 1,
                    binlog: None,
                },
            )
            .unwrap(),
        )
        .unwrap();
        // Keep this payload above the incremental-CRC threshold even when
        // transaction compression is enabled, while still compressing its
        // zero suffix. The tiny compressed checksum path is covered above.
        let mut random_state = 0x123456789abcdef0u64;
        let mut mixed_payload: Vec<u8> = (0..4096)
            .map(|_| {
                random_state ^= random_state << 13;
                random_state ^= random_state >> 7;
                random_state ^= random_state << 17;
                random_state as u8
            })
            .collect();
        mixed_payload.extend_from_slice(&vec![0; 2048]);
        let b = TimedRecord::new(
            LogManager::pre_encode_record(
                2,
                WalPayload::TxnBatch {
                    records: vec![
                        WalPayload::BeginTxn { start_scn: 0 },
                        WalPayload::FileCreate {
                            path: "objects/tables/1/1.heap".into(),
                            after: mixed_payload,
                        },
                        WalPayload::CommitTxn {
                            commit_scn: 2,
                            commit_unix_us: 2,
                            binlog: None,
                        },
                    ],
                },
            )
            .unwrap(),
        )
        .unwrap();
        assert!(a.checksum.is_none());
        assert!(b.checksum.is_some());
        let (_, bt) = log.append_timed(b, &clock, None).unwrap();
        let (_, at) = log.append_timed(a, &clock, None).unwrap();
        let fence = advance_ordered_time(&clock);
        assert!(bt < at && at < fence);
        log.flush().unwrap();
        let records = LogManager::scan(root.path()).unwrap().records;
        let times = records
            .iter()
            .filter_map(|r| match r.payload {
                WalPayload::CommitTxn { commit_unix_us, .. } => Some(commit_unix_us),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(times, vec![bt, at]);
        let rollbacks = AtomicU64::new(0);
        clock.store(u64::MAX - 10, Ordering::Relaxed);
        assert_eq!(advance_time(&clock, Some(&rollbacks)), u64::MAX - 9);
        assert_eq!(rollbacks.load(Ordering::Relaxed), 1);
    }
}
