//! Recovery-floor progress of the buffer pool: recLSN bookkeeping across
//! write-back, the oldest-dirty queue, the redo age target and deferred-pin
//! drain requests. Hooks run in the test thread only, outside every latch.

use super::*;

thread_local! {
    static AFTER_WRITEBACK: std::cell::RefCell<Option<Box<dyn FnOnce(&PageKey)>>> =
        std::cell::RefCell::new(None);
}

/// Fired after a single-page write-back reached the file, before the frame is
/// settled. Lets a test re-dirty the page inside the write-back window.
pub(super) fn after_writeback(key: &PageKey) {
    let hook = AFTER_WRITEBACK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook(key);
    }
}

fn quiet_pool(dir: &Path, capacity: u64) -> Arc<BufferPool> {
    let log = Arc::new(parking_lot::Mutex::new(LogManager::open(dir).unwrap()));
    let pool =
        BufferPool::new_with_options(capacity, log, DurabilityMode::Strict, Duration::ZERO, 1, 0);
    // Every cleaner step below is driven explicitly by the test.
    pool.shutdown_flusher();
    for _ in 0..200 {
        pool.log
            .lock()
            .append(0, WalPayload::CheckpointBegin)
            .unwrap();
    }
    pool
}

fn page(file: &Arc<BufferPageFileKey>, page_id: u64) -> PageKey {
    PageKey {
        file: Arc::clone(file),
        page_id,
    }
}

fn heap_file(dir: &Path, name: &str) -> Arc<BufferPageFileKey> {
    let path = dir.join(name);
    File::create(&path).unwrap();
    Arc::new(BufferPool::heap_file_key(path, 1))
}

fn rec_lsn(pool: &BufferPool, key: &PageKey) -> u64 {
    pool.lookup_frame(key)
        .unwrap()
        .unwrap()
        .first_dirty_lsn
        .load(Ordering::Acquire)
}

#[test]
fn writeback_with_concurrent_redirty_advances_rec_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let pool = quiet_pool(dir.path(), 16);
    let file = heap_file(dir.path(), "redirty-window.heap");
    let key = page(&file, 0);
    pool.store_page(key.clone(), vec![1; PAGE_SIZE], 10).unwrap();
    pool.store_page(key.clone(), vec![2; PAGE_SIZE], 20).unwrap();
    assert_eq!(rec_lsn(&pool, &key), 10);

    let writer = Arc::clone(&pool);
    AFTER_WRITEBACK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move |key| {
            writer
                .store_page(key.clone(), vec![3; PAGE_SIZE], 40)
                .unwrap();
            writer
                .store_page(key.clone(), vec![4; PAGE_SIZE], 30)
                .unwrap();
        }))
    });
    let frame = pool.lookup_frame(&key).unwrap().unwrap();
    assert!(pool.flush_frame(&frame).unwrap(), "the snapshot image is written");
    assert!(frame.dirty.load(Ordering::Acquire), "re-dirtied page stays dirty");
    // The image with every change up to LSN 20 is on disk. Only the changes
    // published after the snapshot (LSN 30 and 40) still need redo.
    assert_eq!(rec_lsn(&pool, &key), 30);
    assert_eq!(pool.checkpoint_dirty_snapshot().unwrap(), (Some(30), 1));
    assert_eq!(std::fs::read(&file.path).unwrap()[100], 2);
    // The advanced recLSN is queued: the age cleaner still sees the page.
    let popped = pool.pop_oldest_dirty_frames_striped(0, 1, 4, 4, 0).unwrap();
    assert_eq!(popped.len(), 1);
    assert_eq!(popped[0].key, key);
}

#[test]
fn writeback_redirty_by_deferred_change_waits_for_its_real_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let pool = quiet_pool(dir.path(), 16);
    let file = heap_file(dir.path(), "redirty-deferred.heap");
    let key = page(&file, 0);
    pool.store_page(key.clone(), vec![1; PAGE_SIZE], 10).unwrap();
    let writer = Arc::clone(&pool);
    AFTER_WRITEBACK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move |key| {
            // A txn-buffered change: page bytes change, WAL LSN not known yet.
            assert!(writer.add_deferred_pin(key));
            writer
                .store_page(key.clone(), vec![2; PAGE_SIZE], 0)
                .unwrap();
        }))
    });
    let frame = pool.lookup_frame(&key).unwrap().unwrap();
    assert!(pool.flush_frame(&frame).unwrap());
    assert!(frame.dirty.load(Ordering::Acquire));
    // No logged change follows the written image yet. The pin keeps the page
    // unflushable and the transaction floor covers its pending WAL.
    assert_eq!(rec_lsn(&pool, &key), 0);
    pool.finalize_deferred(&key, 50);
    assert_eq!(rec_lsn(&pool, &key), 50);
    let popped = pool.pop_oldest_dirty_frames_striped(0, 1, 4, 4, 0).unwrap();
    assert_eq!(popped.len(), 1, "finalized recLSN must be queued");
}

#[test]
fn anchored_page_kinds_keep_their_epoch_anchor_across_redirty() {
    let dir = tempfile::tempdir().unwrap();
    let pool = quiet_pool(dir.path(), 16);
    let zone_path = dir.path().join("t.zm");
    File::create(&zone_path).unwrap();
    let zone = Arc::new(BufferPool::zone_map_file_key(zone_path, 1, 1));
    let heap = heap_file(dir.path(), "plain.heap");
    for (file, anchored) in [(zone, true), (heap, false)] {
        let key = page(&file, 0);
        pool.store_page(key.clone(), vec![1; PAGE_SIZE], 10).unwrap();
        let frame = pool.lookup_frame(&key).unwrap().unwrap();
        frame.begin_writeback_window();
        let epoch = frame.dirty_epoch.load(Ordering::Acquire);
        let generation = frame.generation.load(Ordering::Acquire);
        // A writer re-dirties the frame while its image is being written.
        let mark = frame.mark_dirty(40);
        assert_eq!(mark, DirtyMark::Unchanged);
        let outcome = pool.complete_writeback(&frame, epoch, generation, 1);
        if anchored {
            // Dependent zone-map redo starts each dirty epoch at a full
            // image; a later delta alone cannot rebuild a torn page.
            assert_eq!(outcome, WritebackOutcome::Stale);
            assert_eq!(rec_lsn(&pool, &key), 10);
        } else {
            assert_eq!(outcome, WritebackOutcome::Advanced(40));
            assert_eq!(rec_lsn(&pool, &key), 40);
        }
        assert!(frame.dirty.load(Ordering::Acquire));
    }
}

#[test]
fn rec_lsn_set_while_dirty_keeps_the_page_in_the_oldest_queue() {
    let dir = tempfile::tempdir().unwrap();
    let pool = quiet_pool(dir.path(), 16);
    let file = heap_file(dir.path(), "late-lsn.heap");
    let key = page(&file, 0);
    // Deferred first dirtying: queued with recLSN 0.
    pool.store_page(key.clone(), vec![1; PAGE_SIZE], 0).unwrap();
    assert!(pool.add_deferred_pin(&key));
    // An eager change gives the dirty epoch its first real LSN.
    pool.store_page(key.clone(), vec![2; PAGE_SIZE], 60).unwrap();
    pool.finalize_deferred(&key, 70);
    assert_eq!(rec_lsn(&pool, &key), 60);
    // The original zero-LSN entry is stale now. Before the fix it was the
    // page's only entry, so popping it dropped the page from age cleaning
    // while the non-empty queue suppressed the full-scan fallback.
    let other = page(&file, 1);
    pool.store_page(other.clone(), vec![3; PAGE_SIZE], 100)
        .unwrap();
    let popped = pool.pop_oldest_dirty_frames_striped(0, 1, 1, 1, 0).unwrap();
    assert_eq!(popped.len(), 1);
    assert_eq!(popped[0].key, key, "the oldest recLSN must be selected");
}

#[test]
fn age_target_widens_the_maintenance_batch() {
    let dir = tempfile::tempdir().unwrap();
    let pool = quiet_pool(dir.path(), 128);
    let file = heap_file(dir.path(), "age-target.heap");
    for page_id in 0..40 {
        pool.store_page(page(&file, page_id), vec![page_id as u8; PAGE_SIZE], 1 + page_id)
            .unwrap();
    }
    // Without a target only the per-tick maintenance budget is written.
    pool.flush_oldest_dirty_striped_limited(0, 1, 4, true)
        .unwrap();
    assert_eq!(pool.dirty_pages.load(Ordering::Relaxed), 36);
    // Pages 4..=19 (recLSN 5..=20) are older than the target.
    pool.redo_age_target_lsn.store(21, Ordering::Release);
    pool.flush_oldest_dirty_striped_limited(0, 1, 4, true)
        .unwrap();
    assert_eq!(pool.dirty_pages.load(Ordering::Relaxed), 20);
    assert_eq!(pool.checkpoint_dirty_snapshot().unwrap(), (Some(21), 20));
    // At or above the target the budget falls back to maintenance.
    pool.flush_oldest_dirty_striped_limited(0, 1, 4, true)
        .unwrap();
    assert_eq!(pool.dirty_pages.load(Ordering::Relaxed), 16);
}

#[test]
fn blocked_age_flush_requests_a_txn_wal_drain() {
    let dir = tempfile::tempdir().unwrap();
    let pool = quiet_pool(dir.path(), 16);
    let file = heap_file(dir.path(), "hot-deferred.heap");
    let key = page(&file, 0);
    pool.store_page(key.clone(), vec![1; PAGE_SIZE], 5).unwrap();
    assert!(pool.add_deferred_pin(&key));
    btree_logical_wal::take_txn_wal_drain_request();
    pool.redo_age_target_lsn.store(100, Ordering::Release);

    pool.flush_oldest_dirty_striped_limited(0, 1, 4, true)
        .unwrap();
    let frame = pool.lookup_frame(&key).unwrap().unwrap();
    assert!(frame.dirty.load(Ordering::Acquire), "pinned page cannot be written");
    assert!(frame.flush_wanted.load(Ordering::Acquire));
    assert_eq!(pool.deferred_drain_requests.load(Ordering::Relaxed), 1);
    // A repeated blocked attempt does not count a second request.
    pool.flush_oldest_dirty_striped_limited(0, 1, 4, true)
        .unwrap();
    assert_eq!(pool.deferred_drain_requests.load(Ordering::Relaxed), 1);

    // The next transaction pinning this frame is asked to drain early.
    assert!(!btree_logical_wal::take_txn_wal_drain_request());
    assert!(pool.add_deferred_pin(&key));
    assert!(btree_logical_wal::take_txn_wal_drain_request());

    pool.finalize_deferred(&key, 0);
    pool.finalize_deferred(&key, 0);
    pool.flush_oldest_dirty_striped_limited(0, 1, 4, true)
        .unwrap();
    assert!(!frame.dirty.load(Ordering::Acquire));
    assert!(!frame.flush_wanted.load(Ordering::Acquire), "write-back clears the request");
    assert!(pool.add_deferred_pin(&key));
    assert!(!btree_logical_wal::take_txn_wal_drain_request());
    pool.finalize_deferred(&key, 0);
}

#[test]
fn redo_age_target_trails_the_wal_head_by_the_lag() {
    let dir = tempfile::tempdir().unwrap();
    let pool = quiet_pool(dir.path(), 16);
    pool.observe_redo_age(100, Duration::from_secs(3600));
    assert_eq!(pool.redo_age_target_lsn.load(Ordering::Acquire), 0);
    pool.observe_redo_age(200, Duration::ZERO);
    assert_eq!(pool.redo_age_target_lsn.load(Ordering::Acquire), 200);
    // The target never moves backwards and the sample ring stays bounded.
    pool.observe_redo_age(150, Duration::ZERO);
    assert_eq!(pool.redo_age_target_lsn.load(Ordering::Acquire), 200);
    for lsn in 300..400 {
        pool.observe_redo_age(lsn, Duration::ZERO);
    }
    assert_eq!(pool.redo_age_target_lsn.load(Ordering::Acquire), 399);
    assert!(pool.redo_age_samples.lock().len() <= 2);
}
