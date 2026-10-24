//! Bounded Top-K storage, ordering and budget ownership shared by executors.
use super::{compare_sort_keys, SortField};
use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::executor::memory_footprint::AllocatedBytes;
use crate::types::{QueryError, Value};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

/// Static tie policies keep the hot comparison path monomorphised.
pub(crate) trait TiePolicy<T> {
    const COMPARE_PAYLOAD: bool;
    fn compare(left: &T, right: &T) -> Ordering;
}

pub(crate) struct ArrivalOrder;
impl<T> TiePolicy<T> for ArrivalOrder {
    const COMPARE_PAYLOAD: bool = false;
    #[inline]
    fn compare(_: &T, _: &T) -> Ordering {
        Ordering::Equal
    }
}

pub(crate) struct PayloadThenOrdinal;
impl TiePolicy<Vec<Value>> for PayloadThenOrdinal {
    const COMPARE_PAYLOAD: bool = true;
    fn compare(left: &Vec<Value>, right: &Vec<Value>) -> Ordering {
        super::compare_payload_rows(left, right)
    }
}

struct HeapEntry<T, P: TiePolicy<T>> {
    keys: Vec<Value>,
    ordinal: u64,
    owned_bytes: usize,
    item: T,
    fields: std::sync::Arc<[SortField]>,
    policy: std::marker::PhantomData<P>,
}

impl<T, P: TiePolicy<T>> PartialEq for HeapEntry<T, P> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T, P: TiePolicy<T>> Eq for HeapEntry<T, P> {}

impl<T, P: TiePolicy<T>> PartialOrd for HeapEntry<T, P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T, P: TiePolicy<T>> Ord for HeapEntry<T, P> {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_sort_keys(&self.fields, &self.keys, &other.keys)
            .then_with(|| P::compare(&self.item, &other.item))
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }
}

pub(crate) struct BoundedTopK<T, P: TiePolicy<T> = ArrivalOrder> {
    fields: std::sync::Arc<[SortField]>,
    heap: BinaryHeap<HeapEntry<T, P>>,
    k: usize,
    next_ordinal: u64,
    charge: ScopedCharge,
    credit: usize,
    live_payload: usize,
}

/// A small, bounded startup grant leaves room for this consumer before an
/// upstream blocking operator sizes its work area. The heap really owns the
/// initial slots; payload credit is reusable and never proportional to a huge K.
fn initial_topk_heap<T: Ord>(
    k: usize,
    charge: &mut ScopedCharge,
) -> Result<(BinaryHeap<T>, usize), QueryError> {
    let mut heap = BinaryHeap::new();
    let slots = k
        .min(64)
        .min(charge.remaining() / 8 / std::mem::size_of::<T>().max(1));
    let bytes = slots.saturating_mul(std::mem::size_of::<T>());
    if bytes > 0 && charge.add(bytes).is_ok() {
        if heap.try_reserve_exact(slots).is_err() {
            charge.release_bytes(bytes);
            return Err(mem_budget::allocation_error(
                "out of memory reserving initial TopK slots",
            ));
        }
        let actual = heap.capacity().saturating_mul(std::mem::size_of::<T>());
        if actual > bytes {
            charge.add_non_spillable(actual - bytes)?;
        }
    }
    let credit = if k == 0 {
        0
    } else {
        (charge.remaining() / 8).min(4096)
    };
    let credit = if charge.add(credit).is_ok() {
        credit
    } else {
        0
    };
    Ok((heap, credit))
}

/// Amortize governor admission over small payload grants. Unused credit is
/// bounded by 4 KiB and a fraction of available space, independently of K.
/// Exact admission is retried if only the speculative slack was denied.
fn charge_topk_payload(
    charge: &mut ScopedCharge,
    credit: &mut usize,
    live: &mut usize,
    bytes: usize,
) -> Result<(), QueryError> {
    if bytes > *credit {
        let needed = bytes - *credit;
        let request = needed.max((charge.remaining() / 8).min(4096));
        if request > needed && charge.add(request).is_ok() {
            *credit = credit.saturating_add(request);
        } else {
            charge.add_non_spillable(needed)?;
            *credit = credit.saturating_add(needed);
        }
    }
    *credit -= bytes;
    *live = live.saturating_add(bytes);
    Ok(())
}

fn recycle_topk_payload(
    charge: &mut ScopedCharge,
    credit: &mut usize,
    live: &mut usize,
    bytes: usize,
) {
    *live = live.saturating_sub(bytes);
    *credit = credit.saturating_add(bytes);
    let keep = (*live / 8).min(4096);
    if *credit > keep {
        charge.release_bytes(*credit - keep);
        *credit = keep;
    }
}

/// Fallible geometric heap growth. Account for both arrays during reallocation.
fn reserve_topk_heap<T: Ord>(
    heap: &mut BinaryHeap<T>,
    k: usize,
    charge: &mut ScopedCharge,
    credit: &mut usize,
    bounded: bool,
) -> Result<(), QueryError> {
    if heap.len() >= k || heap.len() < heap.capacity() {
        return Ok(());
    }
    charge.release_bytes(std::mem::take(credit));
    let old = heap.capacity();
    let target = old.saturating_mul(2).max(4).min(k);
    let bytes = target.saturating_mul(std::mem::size_of::<T>());
    if bounded {
        // Adaptive callers have already bounded the entire heap by their
        // share of the query grant. A concurrent producer can cross the soft
        // watermark between payload admission and this backing-array grow;
        // only the ordinary hard gate applies to that bounded overlap.
        charge.add_force(bytes)?;
    } else {
        charge.add_non_spillable(bytes)?;
    }
    if heap.try_reserve_exact(target - heap.len()).is_err() {
        charge.release_bytes(bytes);
        return Err(mem_budget::allocation_error(
            "out of memory while growing TopK heap",
        ));
    }
    charge.release_bytes(old.saturating_mul(std::mem::size_of::<T>()));
    let actual = heap.capacity().saturating_mul(std::mem::size_of::<T>());
    if actual > bytes {
        if bounded {
            charge.add_force(actual - bytes)?;
        } else {
            charge.add_non_spillable(actual - bytes)?;
        }
    }
    Ok(())
}

/// DISTINCT membership storage grows with admitted tuples, never with LIMIT.
/// The reservation includes spare buckets and control bytes. During rehash,
/// both tables can coexist, so reserve the new table before releasing the old.
pub(crate) struct TopKSet<T> {
    set: hashbrown::HashSet<T, ahash::RandomState>,
    charge: ScopedCharge,
}

impl<T: Eq + std::hash::Hash> TopKSet<T> {
    pub(crate) fn new() -> Self {
        Self {
            set: hashbrown::HashSet::with_hasher(ahash::RandomState::new()),
            charge: ScopedCharge::new(0),
        }
    }
    pub(crate) fn contains(&self, value: &T) -> bool {
        self.set.contains(value)
    }
    pub(crate) fn len(&self) -> usize {
        self.set.len()
    }
    pub(crate) fn remove(&mut self, value: &T) {
        self.set.remove(value);
    }
    pub(crate) fn insert(&mut self, value: T) -> Result<(), QueryError> {
        if self.set.len() == self.set.capacity() {
            let old_bytes = self.charge.charged();
            let buckets = self
                .set
                .len()
                .saturating_add(1)
                .saturating_mul(2)
                .max(4)
                .checked_next_power_of_two()
                .ok_or_else(|| mem_budget::allocation_error("TopK DISTINCT capacity overflow"))?;
            let new_bytes = buckets
                .saturating_mul(std::mem::size_of::<T>() + 1)
                .saturating_add(std::mem::align_of::<T>().max(16));
            self.charge.add_non_spillable(new_bytes)?;
            if self.set.try_reserve(1).is_err() {
                self.charge.release_bytes(new_bytes);
                return Err(mem_budget::allocation_error(
                    "out of memory growing TopK DISTINCT set",
                ));
            }
            self.charge.release_bytes(old_bytes);
        }
        self.set.insert(value);
        Ok(())
    }
}

/// Sorted worker candidates keep both their backing array and payloads charged.
/// A candidate transfers its reservation to the receiving heap on admission.
pub(crate) struct TopKCandidates<T, P: TiePolicy<T> = ArrivalOrder> {
    entries: std::vec::IntoIter<HeapEntry<T, P>>,
    charge: ScopedCharge,
}

pub(crate) struct TopKCandidate<T> {
    pub(crate) keys: Vec<Value>,
    pub(crate) item: T,
    pub(crate) ordinal: u64,
    charge: ScopedCharge,
}

impl<T> TopKCandidate<T> {
    pub(crate) fn into_parts(self) -> (Vec<Value>, T, u64, ScopedCharge) {
        (self.keys, self.item, self.ordinal, self.charge)
    }
}

impl<T, P: TiePolicy<T>> Iterator for TopKCandidates<T, P> {
    type Item = TopKCandidate<T>;
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entries.size_hint()
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next().map(|entry| TopKCandidate {
            keys: entry.keys,
            item: entry.item,
            ordinal: entry.ordinal,
            charge: self.charge.split(entry.owned_bytes),
        })
    }
}

impl<T, P: TiePolicy<T>> ExactSizeIterator for TopKCandidates<T, P> {}

impl<T: AllocatedBytes> BoundedTopK<T> {
    /// The caller admits payload copies before allocating them. Keep that
    /// owner through insertion and preserve the original input ordinal when
    /// candidates subsequently migrate to an external sort.
    pub(crate) fn push_reserved(
        &mut self,
        item: T,
        keys: Vec<Value>,
        ordinal: u64,
        charge: ScopedCharge,
    ) -> Result<(), QueryError> {
        let mut owned = TopKCandidate {
            keys,
            item,
            ordinal,
            charge,
        };
        if !self.would_accept(&owned.keys) {
            return Ok(());
        }
        self.reserve_bounded_slot()?;
        let bytes = owned
            .item
            .allocated_bytes()
            .saturating_add(owned.keys.allocated_bytes());
        if bytes > owned.charge.charged() {
            owned.charge.add_force(bytes - owned.charge.charged())?;
        } else {
            owned.charge.release_bytes(owned.charge.charged() - bytes);
        }
        self.charge.absorb(&mut owned.charge);
        self.live_payload = self.live_payload.saturating_add(bytes);
        if let Some(old) = self.insert_charged_at(owned.item, owned.keys, bytes, ordinal) {
            let bytes = old.owned_bytes;
            drop(old);
            recycle_topk_payload(
                &mut self.charge,
                &mut self.credit,
                &mut self.live_payload,
                bytes,
            );
        }
        Ok(())
    }

    pub(crate) fn new(fields: &[SortField], k: usize) -> Result<Self, QueryError> {
        Self::from_fields(fields.to_vec(), k)
    }

    pub(crate) fn from_fields(fields: Vec<SortField>, k: usize) -> Result<Self, QueryError> {
        let mut charge = ScopedCharge::new(0);
        charge.add_non_spillable(
            fields
                .len()
                .saturating_mul(std::mem::size_of::<SortField>())
                .saturating_add(2 * std::mem::size_of::<usize>()),
        )?;
        let (heap, credit) = initial_topk_heap(k, &mut charge)?;
        Ok(Self {
            fields: Arc::from(fields),
            heap,
            k,
            next_ordinal: 0,
            charge,
            credit,
            live_payload: 0,
        })
    }

    /// Returns `true` when a row with these sort keys would be admitted
    /// to the top-K window: either the heap is not yet full, or the
    /// candidate strictly beats the current worst entry. Lets callers
    /// skip the allocation that `push` would otherwise consume (cloning
    /// the scratch key buffer and the row payload) on rejections.
    ///
    /// Mirrors `push`: ties on keys are rejected because `push` itself
    /// only accepts on `Ordering::Less`. Since the ordinal counter is
    /// monotonically increasing, the candidate's ordinal is always
    /// greater than the heap-top's ordinal, so a tie on keys would lose
    /// the tiebreak in `HeapEntry::cmp` and `push` would no-op. By
    /// rejecting ties here we save the row payload clone in that case.
    pub(crate) fn would_accept(&self, keys: &[Value]) -> bool {
        if self.k == 0 {
            return false;
        }
        if self.heap.len() < self.k {
            return true;
        }
        let Some(worst) = self.heap.peek() else {
            return true;
        };
        debug_assert_eq!(keys.len(), worst.keys.len());
        matches!(
            compare_sort_keys(&self.fields, keys, &worst.keys),
            Ordering::Less
        )
    }

    /// Drain to a Vec ordered ascending by sort fields.
    ///
    /// The parallel TopK shuffle path needs to ship
    /// each worker's local top-K to the leader as a sorted vector. The
    /// existing `into_sorted_vec` strips keys, but the leader needs
    /// them again to merge — so this variant keeps `(keys, item)` pairs.
    pub(crate) fn push(&mut self, item: T, keys: Vec<Value>) -> Result<(), QueryError> {
        self.push_lazy(keys, || item)
    }

    pub(crate) fn push_with_evicted(
        &mut self,
        item: T,
        keys: Vec<Value>,
    ) -> Result<(bool, Option<TopKCandidate<T>>), QueryError> {
        if !self.would_accept(&keys) {
            return Ok((false, None));
        }
        self.reserve_slot()?;
        let bytes = item
            .allocated_bytes()
            .saturating_add(keys.allocated_bytes());
        charge_topk_payload(
            &mut self.charge,
            &mut self.credit,
            &mut self.live_payload,
            bytes,
        )?;
        let evicted = self.insert_charged(item, keys, bytes);
        Ok((true, self.evicted_candidate(evicted)))
    }

    pub(crate) fn push_candidate(
        &mut self,
        mut candidate: TopKCandidate<T>,
    ) -> Result<(bool, Option<TopKCandidate<T>>), QueryError> {
        if !self.would_accept(&candidate.keys) {
            return Ok((false, None));
        }
        self.reserve_slot()?;
        let bytes = candidate.charge.charged();
        self.charge.absorb(&mut candidate.charge);
        self.live_payload = self.live_payload.saturating_add(bytes);
        let evicted = self.insert_charged(candidate.item, candidate.keys, bytes);
        Ok((true, self.evicted_candidate(evicted)))
    }
}

impl<T: AllocatedBytes, P: TiePolicy<T>> BoundedTopK<T, P> {
    pub(crate) fn resident_bytes(&self) -> usize {
        self.charge.charged()
    }

    pub(crate) fn memory_charge(&self) -> ScopedCharge {
        self.charge.sibling()
    }

    pub(crate) fn fields_for_spill(&self) -> Arc<[SortField]> {
        self.fields.clone()
    }

    pub(crate) fn drain_sorted(self) -> TopKCandidates<T, P> {
        TopKCandidates {
            entries: self.heap.into_sorted_vec().into_iter(),
            charge: self.charge,
        }
    }

    /// A producer admits the candidate's maximum copied footprint first;
    /// rejected keys still avoid materializing the payload.
    pub(crate) fn push_reserved_lazy_at<F: FnOnce() -> Result<T, QueryError>>(
        &mut self,
        keys: Vec<Value>,
        ordinal: u64,
        charge: ScopedCharge,
        make_item: F,
    ) -> Result<(), QueryError> {
        let mut owned = TopKCandidate {
            keys,
            item: (),
            ordinal,
            charge,
        };
        if self.k == 0 {
            return Ok(());
        }
        let item = if self.heap.len() < self.k {
            make_item()?
        } else {
            let worst = self.heap.peek().expect("full TopK");
            let order = compare_sort_keys(&self.fields, &owned.keys, &worst.keys);
            if order == Ordering::Greater
                || (order == Ordering::Equal && !P::COMPARE_PAYLOAD && ordinal >= worst.ordinal)
            {
                return Ok(());
            }
            let item = make_item()?;
            if order
                .then_with(|| P::compare(&item, &worst.item))
                .then_with(|| ordinal.cmp(&worst.ordinal))
                != Ordering::Less
            {
                return Ok(());
            }
            item
        };
        self.reserve_bounded_slot()?;
        let bytes = item
            .allocated_bytes()
            .saturating_add(owned.keys.allocated_bytes());
        if bytes > owned.charge.charged() {
            owned.charge.add_force(bytes - owned.charge.charged())?;
        } else {
            owned.charge.release_bytes(owned.charge.charged() - bytes);
        }
        self.charge.absorb(&mut owned.charge);
        self.live_payload = self.live_payload.saturating_add(bytes);
        if let Some(old) = self.insert_charged_at(item, owned.keys, bytes, ordinal) {
            let bytes = old.owned_bytes;
            drop(old);
            recycle_topk_payload(
                &mut self.charge,
                &mut self.credit,
                &mut self.live_payload,
                bytes,
            );
        }
        Ok(())
    }

    fn reserve_slot(&mut self) -> Result<(), QueryError> {
        reserve_topk_heap(
            &mut self.heap,
            self.k,
            &mut self.charge,
            &mut self.credit,
            false,
        )
    }

    fn reserve_bounded_slot(&mut self) -> Result<(), QueryError> {
        reserve_topk_heap(
            &mut self.heap,
            self.k,
            &mut self.charge,
            &mut self.credit,
            true,
        )
    }

    fn insert_charged(
        &mut self,
        item: T,
        keys: Vec<Value>,
        owned_bytes: usize,
    ) -> Option<HeapEntry<T, P>> {
        let ordinal = self.next_ordinal;
        self.next_ordinal = self.next_ordinal.saturating_add(1);
        self.insert_charged_at(item, keys, owned_bytes, ordinal)
    }

    fn insert_charged_at(
        &mut self,
        item: T,
        keys: Vec<Value>,
        owned_bytes: usize,
        ordinal: u64,
    ) -> Option<HeapEntry<T, P>> {
        let entry = HeapEntry {
            keys,
            ordinal,
            owned_bytes,
            item,
            fields: self.fields.clone(),
            policy: std::marker::PhantomData,
        };
        if self.heap.len() < self.k {
            self.heap.push(entry);
            return None;
        }
        let evicted = self.heap.pop();
        self.heap.push(entry);
        evicted
    }

    fn evicted_candidate(&mut self, evicted: Option<HeapEntry<T, P>>) -> Option<TopKCandidate<T>> {
        evicted.map(|entry| {
            self.live_payload = self.live_payload.saturating_sub(entry.owned_bytes);
            TopKCandidate {
                item: entry.item,
                keys: entry.keys,
                ordinal: entry.ordinal,
                charge: self.charge.split(entry.owned_bytes),
            }
        })
    }

    pub(crate) fn into_sorted_vec(self) -> Result<Vec<T>, QueryError> {
        self.finish_output::<false, false>()
            .map(|(rows, _charge)| rows)
    }

    pub(crate) fn into_sorted_vec_charged(self) -> Result<(Vec<T>, ScopedCharge), QueryError> {
        self.finish_output::<true, false>()
    }

    /// Streaming workers retain strict admission for their final output array.
    pub(crate) fn into_sorted_vec_charged_strict(
        self,
    ) -> Result<(Vec<T>, ScopedCharge), QueryError> {
        self.finish_output::<true, true>()
    }

    fn finish_output<const KEEP_CHARGE: bool, const STRICT: bool>(
        mut self,
    ) -> Result<(Vec<T>, ScopedCharge), QueryError> {
        // The output array coexists with the heap's array during conversion.
        let bytes = self.heap.len().saturating_mul(std::mem::size_of::<T>());
        if STRICT {
            self.charge.release_bytes(std::mem::take(&mut self.credit));
            self.charge.add(bytes)?;
        } else {
            charge_topk_payload(
                &mut self.charge,
                &mut self.credit,
                &mut self.live_payload,
                bytes,
            )?;
        }
        let mut out = mem_budget::try_vec_with_capacity(self.heap.len(), "TopK output")?;
        for entry in self.heap.into_sorted_vec() {
            out.push(entry.item);
        }
        if KEEP_CHARGE {
            let resident = out.allocated_bytes();
            self.charge
                .release_bytes(self.charge.charged().saturating_sub(resident));
        }
        Ok((out, self.charge))
    }
}

impl<T: AllocatedBytes> BoundedTopK<T> {
    pub(crate) fn push_lazy<F: FnOnce() -> T>(
        &mut self,
        keys: Vec<Value>,
        make_item: F,
    ) -> Result<(), QueryError> {
        let ordinal = self.next_ordinal;
        self.next_ordinal = self.next_ordinal.saturating_add(1);
        if !self.would_accept(&keys) {
            return Ok(());
        }
        self.admit_owned(make_item(), keys, ordinal)
    }

    pub(crate) fn with_collations(mut self, collations: Vec<Option<u16>>) -> Self {
        // The vector adapter sets schema metadata before accepting rows.
        assert!(self.heap.is_empty());
        debug_assert!(collations.is_empty() || collations.len() == self.fields.len());
        for (i, field) in Arc::make_mut(&mut self.fields).iter_mut().enumerate() {
            field.collation_id = collations.get(i).copied().flatten();
        }
        self
    }
}

impl<T: AllocatedBytes, P: TiePolicy<T>> BoundedTopK<T, P> {
    /// Shared fields are charged by the driver; workers start without speculative grants.
    pub(crate) fn new_unreserved(fields: Arc<[SortField]>, k: usize, charge: ScopedCharge) -> Self {
        Self {
            fields,
            heap: BinaryHeap::new(),
            k,
            next_ordinal: 0,
            charge,
            credit: 0,
            live_payload: 0,
        }
    }

    pub(crate) fn reserve_payload_width(&mut self, row_columns: usize) {
        let fixed_bytes = row_columns
            .saturating_add(self.fields.len())
            .saturating_mul(std::mem::size_of::<Value>());
        let slots = if self.k == 0 {
            0
        } else {
            self.k.min(64).saturating_add(1)
        };
        let available = self.charge.remaining().saturating_add(self.credit);
        let target = slots
            .saturating_mul(fixed_bytes)
            .min(64 * 1024)
            .min(available / 4);
        if target > self.credit {
            if self.charge.add(target - self.credit).is_ok() {
                self.credit = target;
            }
        } else {
            self.charge.release_bytes(self.credit - target);
            self.credit = target;
        }
    }

    pub(crate) fn push_lazy_at<F: FnOnce() -> T>(
        &mut self,
        keys: Vec<Value>,
        ordinal: u64,
        make_item: F,
    ) -> Result<(), QueryError> {
        if self.k == 0 {
            return Ok(());
        }
        let item = if self.heap.len() < self.k {
            make_item()
        } else {
            let worst = self.heap.peek().expect("full TopK");
            let order = compare_sort_keys(&self.fields, &keys, &worst.keys);
            if order == Ordering::Greater
                || (order == Ordering::Equal && !P::COMPARE_PAYLOAD && ordinal >= worst.ordinal)
            {
                return Ok(());
            }
            let item = make_item();
            if order
                .then_with(|| P::compare(&item, &worst.item))
                .then_with(|| ordinal.cmp(&worst.ordinal))
                != Ordering::Less
            {
                return Ok(());
            }
            item
        };
        self.admit_owned(item, keys, ordinal)
    }

    fn admit_owned(&mut self, item: T, keys: Vec<Value>, ordinal: u64) -> Result<(), QueryError> {
        self.reserve_slot()?;
        let bytes = item
            .allocated_bytes()
            .saturating_add(keys.allocated_bytes());
        charge_topk_payload(
            &mut self.charge,
            &mut self.credit,
            &mut self.live_payload,
            bytes,
        )?;
        if let Some(old) = self.insert_charged_at(item, keys, bytes, ordinal) {
            let bytes = old.owned_bytes;
            drop(old);
            recycle_topk_payload(
                &mut self.charge,
                &mut self.credit,
                &mut self.live_payload,
                bytes,
            );
        }
        Ok(())
    }

    /// Transfer worker ownership without cloning payloads or changing source ordinals.
    pub(crate) fn merge_from(&mut self, mut worker: Self) -> Result<(), QueryError> {
        debug_assert_eq!(self.fields, worker.fields);
        let backing_bytes = worker
            .heap
            .capacity()
            .saturating_mul(std::mem::size_of::<HeapEntry<T, P>>());
        worker.charge.release_bytes(worker.credit);
        self.charge.absorb(&mut worker.charge);
        for entry in worker.heap.into_sorted_vec() {
            if self.heap.len() < self.k {
                self.reserve_slot()?;
                self.live_payload = self.live_payload.saturating_add(entry.owned_bytes);
                self.heap.push(entry);
            } else if self
                .heap
                .peek()
                .is_some_and(|top| entry.cmp(top) == Ordering::Less)
            {
                let old = self.heap.pop().expect("full TopK");
                self.live_payload = self
                    .live_payload
                    .saturating_sub(old.owned_bytes)
                    .saturating_add(entry.owned_bytes);
                self.heap.push(entry);
                let bytes = old.owned_bytes;
                drop(old);
                self.charge.release_bytes(bytes);
            } else {
                let bytes = entry.owned_bytes;
                drop(entry);
                self.charge.release_bytes(bytes);
            }
        }
        self.charge.release_bytes(backing_bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use std::cell::Cell;

    fn fields() -> Arc<[SortField]> {
        Arc::from(vec![SortField::mysql_order(true)])
    }

    #[test]
    fn lazy_admission_preserves_distinct_tie_policies() {
        let made = Cell::new(0);
        let mut stable = BoundedTopK::new(&fields(), 1).unwrap();
        stable
            .push_lazy(vec![Value::Int(1)], || vec![Value::Int(9)])
            .unwrap();
        stable
            .push_lazy(vec![Value::Int(1)], || {
                made.set(made.get() + 1);
                vec![Value::Int(2)]
            })
            .unwrap();
        assert_eq!(made.get(), 0);
        assert_eq!(stable.into_sorted_vec().unwrap(), vec![vec![Value::Int(9)]]);

        let mut payload =
            BoundedTopK::<_, PayloadThenOrdinal>::new_unreserved(fields(), 1, ScopedCharge::new(0));
        payload
            .push_lazy_at(vec![Value::Int(1)], 0, || vec![Value::Int(9)])
            .unwrap();
        payload
            .push_lazy_at(vec![Value::Int(1)], 8, || vec![Value::Int(2)])
            .unwrap();
        payload
            .push_lazy_at(vec![Value::Int(2)], 1, || {
                panic!("worse keys must not materialise a row")
            })
            .unwrap();
        assert_eq!(
            payload.into_sorted_vec().unwrap(),
            vec![vec![Value::Int(2)]]
        );
    }

    #[test]
    fn merging_retains_source_ordinal_and_live_output_charge() {
        let budget = Arc::new(MemBudget::new(64 * 1024));
        with_query_budget(budget.clone(), || {
            let mut master = BoundedTopK::<_, PayloadThenOrdinal>::new_unreserved(
                fields(),
                1,
                ScopedCharge::new(0),
            );
            for ordinal in [90, 5, 40] {
                let mut worker = BoundedTopK::<_, PayloadThenOrdinal>::new_unreserved(
                    fields(),
                    1,
                    ScopedCharge::new(0),
                );
                worker
                    .push_lazy_at(vec![Value::Int(1)], ordinal, || {
                        vec![Value::String("x".repeat(1024))]
                    })
                    .unwrap();
                master.merge_from(worker).unwrap();
            }
            assert_eq!(master.heap.peek().unwrap().ordinal, 5);
            let (out, charge) = master.into_sorted_vec_charged().unwrap();
            assert_eq!(budget.used(), out.allocated_bytes());
            drop(out);
            drop(charge);
            assert_eq!(budget.used(), 0);
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn zero_k_never_materialises_and_merge_failure_releases_owners() {
        let mut empty = BoundedTopK::<Vec<Value>, PayloadThenOrdinal>::new_unreserved(
            fields(),
            0,
            ScopedCharge::new(0),
        );
        empty
            .push_lazy_at(vec![Value::Int(1)], 0, || panic!("zero K"))
            .unwrap();
        assert!(empty.into_sorted_vec().unwrap().is_empty());
        let budget = Arc::new(MemBudget::new(4096));
        with_query_budget(budget.clone(), || {
            let mut worker = BoundedTopK::<_, PayloadThenOrdinal>::new_unreserved(
                fields(),
                1,
                ScopedCharge::new(0),
            );
            worker
                .push_lazy_at(vec![Value::Int(1)], 0, || vec![Value::Int(1)])
                .unwrap();
            let mut blocker = ScopedCharge::new(0);
            blocker.add_non_spillable(budget.remaining()).unwrap();
            let mut master = BoundedTopK::<_, PayloadThenOrdinal>::new_unreserved(
                fields(),
                1,
                ScopedCharge::new(0),
            );
            // Remove speculative credit so the destination cannot fund its backing array.
            worker.charge.release_bytes(worker.credit);
            worker.credit = 0;
            blocker.add_non_spillable(budget.remaining()).unwrap();
            assert!(master.merge_from(worker).is_err());
            drop(master);
            drop(blocker);
            assert_eq!(budget.used(), 0);
        });
    }
    #[test]
    fn partitioned_topk_matches_full_sort_for_both_policies() {
        fn check<P: TiePolicy<Vec<Value>>>() {
            let fields: Arc<[SortField]> = Arc::from(vec![
                SortField {
                    ascending: false,
                    nulls_first: true,
                    collation_id: None,
                },
                SortField {
                    ascending: true,
                    nulls_first: false,
                    collation_id: Some(255),
                },
            ]);
            let input: Vec<_> = (0..257)
                .map(|i| {
                    let number = match i % 11 {
                        0 => Value::Null,
                        1 => Value::Double(f64::NAN),
                        2 => Value::Double(f64::INFINITY),
                        3 => Value::Double(-0.0),
                        _ => Value::Int((i % 7) as i64),
                    };
                    let text = ["éclair", "ECLAIR", "中文", "apple"][i % 4];
                    (
                        vec![number, Value::String(text.into())],
                        vec![Value::Int((256 - i) as i64)],
                    )
                })
                .collect();
            let mut oracle: Vec<_> = (0..input.len()).collect();
            oracle.sort_by(|&a, &b| {
                compare_sort_keys(&fields, &input[a].0, &input[b].0)
                    .then_with(|| P::compare(&input[a].1, &input[b].1))
                    .then_with(|| a.cmp(&b))
            });
            for k in [0, 1, 7, 64, 300] {
                for workers in [1, 3, 7] {
                    let mut master = BoundedTopK::<_, P>::new_unreserved(
                        fields.clone(),
                        k,
                        ScopedCharge::new(0),
                    );
                    // Merge in reverse worker order to expose lost source ordinals.
                    for worker in (0..workers).rev() {
                        let mut local = BoundedTopK::<_, P>::new_unreserved(
                            fields.clone(),
                            k,
                            ScopedCharge::new(0),
                        );
                        for i in (worker..input.len()).step_by(workers) {
                            local
                                .push_lazy_at(input[i].0.clone(), i as u64, || input[i].1.clone())
                                .unwrap();
                        }
                        master.merge_from(local).unwrap();
                    }
                    let actual: Vec<_> = master
                        .heap
                        .into_sorted_vec()
                        .into_iter()
                        .map(|entry| entry.ordinal as usize)
                        .collect();
                    assert_eq!(
                        actual,
                        oracle[..k.min(input.len())],
                        "k={k}, workers={workers}"
                    );
                }
            }
        }
        check::<ArrivalOrder>();
        check::<PayloadThenOrdinal>();
    }
}
