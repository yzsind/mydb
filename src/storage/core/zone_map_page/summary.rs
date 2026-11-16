//! Typed summary records. Bounds and membership are separate key spaces so
//! range predicates never fetch Bloom pages. Membership views borrow the
//! pinned page; there is no decoded bitmap allocation on the read path.

use super::*;
use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};

const RECORD_VERSION: u8 = 1;
const MEMBERSHIP_HEADER: usize = 8;

pub(in crate::storage::core) fn key(column: u32, kind: Kind, zone: u64) -> Key {
    Key {
        column,
        // Logical metadata keys precede column 0 bounds. Column ids are
        // zero-based and the full u32 domain must remain representable.
        kind: if kind == Kind::ZoneMeta {
            Kind::Meta as u8
        } else {
            kind as u8
        },
        zone,
    }
}

pub(in crate::storage::core) fn encode_bounds(
    summary: &ZoneColumnSummary,
) -> Result<Vec<u8>, QueryError> {
    let flags = u8::from(summary.has_null)
        | (u8::from(summary.has_non_null) << 1)
        | (u8::from(summary.truncated) << 2);
    let mut bytes = vec![RECORD_VERSION, zone_state_tag(summary.state), flags, 0];
    put_zone_scalar_v2(&mut bytes, summary.min.as_ref())
        .ok_or_else(|| corrupt("invalid minimum"))?;
    put_zone_scalar_v2(&mut bytes, summary.max.as_ref())
        .ok_or_else(|| corrupt("invalid maximum"))?;
    Ok(bytes)
}

pub(in crate::storage::core) fn decode_bounds(
    bytes: &[u8],
) -> Result<ZoneColumnSummary, QueryError> {
    if bytes.len() < 6
        || bytes.len() > PAGE_SIZE - HEADER - SLOT
        || bytes[0] != RECORD_VERSION
        || bytes[2] & !7 != 0
        || bytes[3] != 0
    {
        return Err(corrupt("invalid bounds header"));
    }
    let mut reader = ZoneBytesReader::new(&bytes[4..]);
    let summary = ZoneColumnSummary {
        state: zone_state_from_tag(bytes[1]).ok_or_else(|| corrupt("invalid bounds state"))?,
        durable_unknown: false,
        has_null: bytes[2] & 1 != 0,
        has_non_null: bytes[2] & 2 != 0,
        truncated: bytes[2] & 4 != 0,
        min: read_zone_scalar_v2(&mut reader).ok_or_else(|| corrupt("invalid minimum"))?,
        max: read_zone_scalar_v2(&mut reader).ok_or_else(|| corrupt("invalid maximum"))?,
        membership: None,
    };
    // Scalar readers validate fixed tags, canonical varints and Boolean
    // flags directly. Only Decimal has a normalized representation requiring
    // a round trip; integer/range probes avoid an encoding allocation.
    let decimal = matches!(summary.min, Some(ZoneScalar::Decimal(_)))
        || matches!(summary.max, Some(ZoneScalar::Decimal(_)));
    if !reader.done() || (decimal && encode_bounds(&summary)? != bytes) {
        return Err(corrupt("noncanonical or trailing bounds data"));
    }
    match (&summary.min, &summary.max) {
        (Some(min), Some(max))
            if matches!(
                min.compare(max),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ) => {}
        (None, None) if summary.state != ZoneMapState::Ready || !summary.has_non_null => {}
        _ => return Err(corrupt("inconsistent bounds")),
    }
    if summary.state == ZoneMapState::Empty
        && (summary.has_null || summary.has_non_null || summary.min.is_some())
    {
        return Err(corrupt("nonempty Empty summary"));
    }
    Ok(summary)
}

mod membership;
use membership::MembershipView;
pub(in crate::storage::core) use membership::{
    decode_membership, encode_membership, membership_owned_bytes,
    merge_membership_record_foreground,
};

pub(in crate::storage::core) fn encode_meta(zone: &ZoneSummary) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[0] = RECORD_VERSION;
    bytes[1] = zone.state.map_or(255, zone_state_tag);
    write_u64(&mut bytes, 8, zone.row_count_estimate);
    write_u64(&mut bytes, 16, zone.stale_mutations);
    write_u64(&mut bytes, 24, zone.rebuild_scn);
    bytes
}

pub(in crate::storage::core) fn decode_meta(bytes: &[u8]) -> Result<ZoneSummary, QueryError> {
    if bytes.len() != 32 || bytes[0] != RECORD_VERSION || bytes[2..8] != [0; 6] {
        return Err(corrupt("invalid zone metadata"));
    }
    Ok(ZoneSummary {
        state: if bytes[1] == 255 {
            None
        } else {
            Some(zone_state_from_tag(bytes[1]).ok_or_else(|| corrupt("invalid zone state"))?)
        },
        columns: BTreeMap::new(),
        row_count_estimate: read_u64(bytes, 8),
        stale_mutations: read_u64(bytes, 16),
        rebuild_scn: read_u64(bytes, 24),
    })
}

/// None is a DML summary: merges may only widen its column bounds. Rebuilds
/// that shrink bounds must publish an explicit state and a snapshot fence.
fn meta_allows_pruning(meta: &ZoneSummary, safe_read_scn: u64) -> bool {
    meta.rebuild_scn <= safe_read_scn
        && matches!(
            meta.state,
            None | Some(ZoneMapState::Ready | ZoneMapState::Empty)
        )
}

/// One bounded pending leaf. Records must arrive in (column, kind, zone)
/// order: metadata first, then each column's bounds and membership passes.
#[cfg(test)]
pub(in crate::storage::core) struct Writer<'a> {
    builder: directory::Builder<'a>,
    pending: Vec<(Key, Vec<u8>)>,
    bytes: usize,
    failed: bool,
    _scratch: ControlledMemoryCharge,
}

#[cfg(test)]
impl<'a> Writer<'a> {
    pub fn new(file: &'a PageFile, schema: u64, zone_pages: u32) -> Result<Self, QueryError> {
        let scratch = ControlledMemoryCharge::try_new(
            ControlledMemoryCategory::StorageScratch,
            PAGE_SIZE * 6,
        )?;
        Ok(Self {
            builder: directory::Builder::new(file, schema, zone_pages)?,
            pending: Vec::with_capacity(MAX_ENTRIES),
            bytes: HEADER,
            failed: false,
            _scratch: scratch,
        })
    }

    fn push(&mut self, key: Key, payload: Result<Vec<u8>, QueryError>) -> Result<(), QueryError> {
        if self.failed {
            return Err(corrupt("summary writer previously failed"));
        }
        self.failed = true;
        let bytes = payload?;
        if bytes.len() > PAGE_SIZE - HEADER - SLOT {
            return Err(corrupt("summary does not fit a page"));
        }
        if self.pending.last().is_some_and(|(last, _)| *last >= key) {
            return Err(corrupt("summary keys not strictly ordered"));
        }
        if self.bytes + SLOT + bytes.len() > PAGE_SIZE
            || self
                .pending
                .first()
                .is_some_and(|(first, _)| first.kind != key.kind)
        {
            self.flush()?;
        }
        self.bytes += SLOT + bytes.len();
        self.pending.push((key, bytes));
        self.failed = false;
        Ok(())
    }

    pub fn meta(&mut self, zone_id: u64, zone: &ZoneSummary) -> Result<(), QueryError> {
        self.push(
            key(0, Kind::ZoneMeta, zone_id),
            Ok(encode_meta(zone).to_vec()),
        )
    }

    pub fn bounds(
        &mut self,
        column: u32,
        zone: u64,
        summary: &ZoneColumnSummary,
    ) -> Result<(), QueryError> {
        self.push(key(column, Kind::Bounds, zone), encode_bounds(summary))
    }

    pub fn membership(
        &mut self,
        column: u32,
        zone: u64,
        membership: Option<&ZoneMembership>,
    ) -> Result<(), QueryError> {
        self.push(
            key(column, Kind::Membership, zone),
            encode_membership(membership),
        )
    }

    fn flush(&mut self) -> Result<(), QueryError> {
        let Some((first, _)) = self.pending.first() else {
            return Ok(());
        };
        let entries: Vec<_> = self
            .pending
            .iter()
            .map(|(key, bytes)| (*key, bytes.as_slice()))
            .collect();
        let kind = if first.kind == Kind::Meta as u8 {
            Kind::ZoneMeta
        } else {
            Kind::try_from(first.kind)?
        };
        self.builder.push_leaf(kind, &entries)?;
        self.pending.clear();
        self.bytes = HEADER;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), QueryError> {
        if self.failed {
            return Err(corrupt("cannot publish failed summary writer"));
        }
        self.flush()?;
        self.builder.finish()?;
        Ok(())
    }
}

pub(in crate::storage::core) struct Reader<'a> {
    file: &'a PageFile,
    root: directory::Root,
    safe_read_scn: u64,
    leaf_hints: std::cell::RefCell<[Option<directory::LeafHint>; 16]>,
    next_hint: std::cell::Cell<usize>,
}

impl<'a> Reader<'a> {
    pub fn open(
        file: &'a PageFile,
        schema: u64,
        zone_pages: u32,
        safe_read_scn: u64,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            file,
            root: directory::Root::open(file, schema, zone_pages)?,
            safe_read_scn,
            leaf_hints: std::cell::RefCell::new([None; 16]),
            next_hint: std::cell::Cell::new(0),
        })
    }

    fn get<T>(
        &self,
        key: Key,
        consume: impl FnOnce(Option<&[u8]>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        let mut hints = self.leaf_hints.borrow_mut();
        let slot = hints
            .iter()
            .position(|hint| hint.is_some_and(|hint| hint.contains(key)))
            .unwrap_or_else(|| {
                let slot = self.next_hint.get();
                self.next_hint.set((slot + 1) % hints.len());
                slot
            });
        self.root
            .get_with_hint(self.file, key, &mut hints[slot], consume)
    }

    fn validate_generation(&self) -> Result<(), QueryError> {
        if directory::Root::open(self.file, self.root.schema, self.root.zone_pages)? != self.root {
            return Err(corrupt("summary generation changed"));
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn invalidate(&self) -> Result<u64, QueryError> {
        self.root.invalidate(self.file)
    }

    /// A decode, I/O or admission failure only retains candidates. The owner
    /// handles diagnostics/repair; a missing optional bitmap is not corruption.
    #[cfg(test)]
    pub fn may_match(
        &self,
        zone: u64,
        predicate: &ZonePredicateProgram,
    ) -> Result<bool, QueryError> {
        self.may_match_with_cost(zone, predicate)
            .map(|(matched, _)| matched)
    }

    /// CPU costing uses only logical records actually inspected, rather than
    /// charging a full physical page again for every record in that page.
    #[cfg(test)]
    pub fn may_match_with_cost(
        &self,
        zone: u64,
        predicate: &ZonePredicateProgram,
    ) -> Result<(bool, u64), QueryError> {
        let mut result = (true, 0);
        self.visit_matches(std::iter::once(zone), predicate, |_, matched, bytes| {
            result = (matched, bytes);
            Ok(())
        })?;
        Ok(result)
    }

    /// The caller holds the Store read lock from open through traversal.
    /// Opening validates the initial generation; retain the final fence.
    pub fn open_and_visit_matches(
        file: &'a PageFile,
        schema: u64,
        zone_pages: u32,
        safe_read_scn: u64,
        zones: impl IntoIterator<Item = u64>,
        predicate: &ZonePredicateProgram,
        consume: impl FnMut(u64, bool, u64) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        Self::open(file, schema, zone_pages, safe_read_scn)?
            .visit_matches_inner(zones, predicate, consume)
    }

    /// Reused readers must also check for changes since they were opened.
    #[cfg(test)]
    pub fn visit_matches(
        &self,
        zones: impl IntoIterator<Item = u64>,
        predicate: &ZonePredicateProgram,
        consume: impl FnMut(u64, bool, u64) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        self.validate_generation()?;
        self.visit_matches_inner(zones, predicate, consume)
    }

    fn visit_matches_inner(
        &self,
        zones: impl IntoIterator<Item = u64>,
        predicate: &ZonePredicateProgram,
        mut consume: impl FnMut(u64, bool, u64) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let _scratch = ControlledMemoryCharge::try_new_optional(
            ControlledMemoryCategory::StorageScratch,
            PAGE_SIZE * 4,
        )
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_OUTOFMEMORY,
                "zone map read scratch budget exceeded",
            )
        })?;
        let mut zones = zones.into_iter().peekable();
        while let Some(first) = zones.next() {
            let mut len = 1usize;
            while len < 64
                && first
                    .checked_add(len as u64)
                    .is_some_and(|next| zones.peek() == Some(&next))
            {
                zones.next();
                len += 1;
            }
            if len >= 4 && first.checked_add(len as u64).is_some() {
                let mut bytes = [0u64; 64];
                let mask = self.probe_batch(first, len, predicate, &mut bytes)?;
                for (index, bytes) in bytes.into_iter().enumerate().take(len) {
                    consume(first + index as u64, mask & (1u64 << index) != 0, bytes)?;
                }
            } else {
                for index in 0..len {
                    let zone = first + index as u64;
                    let (matched, bytes) = self.probe_zone(zone, predicate)?;
                    consume(zone, matched, bytes)?;
                }
            }
        }
        self.validate_generation()
    }

    fn probe_batch(
        &self,
        first: u64,
        len: usize,
        predicate: &ZonePredicateProgram,
        bytes: &mut [u64; 64],
    ) -> Result<u64, QueryError> {
        if matches!(predicate, ZonePredicateProgram::Never) {
            return Ok(0);
        }
        let all = u64::MAX >> (64 - len);
        let mut trusted = 0u64;
        self.root.visit_range(
            self.file,
            key(0, Kind::ZoneMeta, first),
            key(0, Kind::ZoneMeta, first + len as u64),
            |key, value| {
                let index = (key.zone - first) as usize;
                bytes[index] += value.len() as u64;
                let meta = decode_meta(value)?;
                if meta_allows_pruning(&meta, self.safe_read_scn) {
                    trusted |= 1u64 << index;
                }
                Ok(true)
            },
        )?;
        Ok((all & !trusted) | self.probe_mask(predicate, first, len, trusted, bytes)?)
    }

    fn probe_mask(
        &self,
        predicate: &ZonePredicateProgram,
        first: u64,
        len: usize,
        active: u64,
        bytes: &mut [u64; 64],
    ) -> Result<u64, QueryError> {
        if active == 0 {
            return Ok(0);
        }
        use ZonePredicateProgram::*;
        match predicate {
            Never => Ok(0),
            And(left, right) => {
                let remaining = self.probe_mask(left, first, len, active, bytes)?;
                self.probe_mask(right, first, len, remaining, bytes)
            }
            Or(left, right) => {
                let matched = self.probe_mask(left, first, len, active, bytes)?;
                Ok(matched | self.probe_mask(right, first, len, active & !matched, bytes)?)
            }
            Compare { column_id, .. }
            | NullSafeNull { column_id }
            | IsNull { column_id, .. }
            | Between { column_id, .. }
            | InList { column_id, .. }
            | LikePrefix { column_id, .. } => {
                self.probe_leaf_mask(predicate, *column_id, first, len, active, bytes)
            }
        }
    }

    fn probe_leaf_mask(
        &self,
        predicate: &ZonePredicateProgram,
        column: u32,
        first: u64,
        len: usize,
        active: u64,
        bytes: &mut [u64; 64],
    ) -> Result<u64, QueryError> {
        // Only one 64-zone leaf window is decoded at a time. Bounds pins are
        // released before membership pages are visited; bitmap bytes remain
        // borrowed from their pinned page, never materialized per query.
        let mut lease = ZoneScratchLease::default();
        let mut bounds: [Option<ZoneColumnSummary>; 64] = std::array::from_fn(|_| None);
        let mut retained = active;
        let mut membership = 0u64;
        self.root.visit_range(
            self.file,
            key(column, Kind::Bounds, first),
            key(column, Kind::Bounds, first + len as u64),
            |key, value| {
                let index = (key.zone - first) as usize;
                let bit = 1u64 << index;
                if active & bit == 0 {
                    return Ok(true);
                }
                bytes[index] += value.len() as u64;
                lease.reserve(value.len().saturating_mul(4))?;
                let summary = decode_bounds(value)?;
                let mut needs_membership = false;
                if !predicate.column_may_match(&summary, |_| {
                    needs_membership = true;
                    None
                }) {
                    retained &= !bit;
                } else if needs_membership {
                    membership |= bit;
                    bounds[index] = Some(summary);
                }
                Ok(true)
            },
        )?;
        if membership != 0 {
            self.root.visit_range(
                self.file,
                key(column, Kind::Membership, first),
                key(column, Kind::Membership, first + len as u64),
                |key, value| {
                    let index = (key.zone - first) as usize;
                    let bit = 1u64 << index;
                    if membership & bit == 0 {
                        return Ok(true);
                    }
                    bytes[index] += value.len() as u64;
                    let view = MembershipView::open(value)?;
                    if !predicate.column_may_match(
                        bounds[index]
                            .as_ref()
                            .expect("membership requested for decoded bounds"),
                        |query| view.probe(zone_membership_hash(query)),
                    ) {
                        retained &= !bit;
                    }
                    Ok(true)
                },
            )?;
        }
        Ok(retained)
    }

    fn probe_zone(
        &self,
        zone: u64,
        predicate: &ZonePredicateProgram,
    ) -> Result<(bool, u64), QueryError> {
        if matches!(predicate, ZonePredicateProgram::Never) {
            return Ok((false, 0));
        }
        let inspected_bytes = std::cell::Cell::new(0u64);
        let meta = self.get(key(0, Kind::ZoneMeta, zone), |bytes| {
            if let Some(bytes) = bytes {
                inspected_bytes.set(inspected_bytes.get().saturating_add(bytes.len() as u64));
            }
            bytes.map(decode_meta).transpose()
        })?;
        if meta.is_none_or(|meta| {
            // A source-side rebuild does not know which older snapshots a
            // physical replica still serves. Historical views opened after
            // rebuilding need the same protection on the source itself.
            !meta_allows_pruning(&meta, self.safe_read_scn)
        }) {
            return Ok((true, inspected_bytes.get()));
        }
        let mut failure = None;
        let matched = predicate.may_match_by(&mut |leaf, column| {
            let bounds = self.get(key(column, Kind::Bounds, zone), |bytes| {
                if let Some(bytes) = bytes {
                    inspected_bytes.set(inspected_bytes.get().saturating_add(bytes.len() as u64));
                }
                bytes.map(decode_bounds).transpose()
            });
            match bounds {
                Ok(Some(summary)) => leaf.column_may_match(&summary, |query| {
                    match self.get(key(column, Kind::Membership, zone), |bytes| {
                        if let Some(bytes) = bytes {
                            inspected_bytes
                                .set(inspected_bytes.get().saturating_add(bytes.len() as u64));
                        }
                        bytes.map(MembershipView::open).transpose().map(|view| {
                            view.and_then(|view| view.probe(zone_membership_hash(query)))
                        })
                    }) {
                        Ok(result) => result,
                        Err(err) => {
                            failure = Some(err);
                            None
                        }
                    }
                }),
                Ok(None) => true,
                Err(err) => {
                    failure = Some(err);
                    true
                }
            }
        });
        match failure {
            Some(err) => Err(err),
            None => Ok((matched, inspected_bytes.get())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zone_map_meta_trust_requires_known_state_and_safe_snapshot() {
        for (state, trusted) in [
            (None, true),
            (Some(ZoneMapState::Ready), true),
            (Some(ZoneMapState::Empty), true),
            (Some(ZoneMapState::Unknown), false),
            (Some(ZoneMapState::Building), false),
        ] {
            let meta = ZoneSummary {
                state,
                rebuild_scn: 100,
                ..ZoneSummary::default()
            };
            let decoded = decode_meta(&encode_meta(&meta)).unwrap();
            assert!(!meta_allows_pruning(&decoded, 99));
            assert_eq!(meta_allows_pruning(&decoded, 100), trusted);
            assert_eq!(meta_allows_pruning(&decoded, 101), trusted);
        }
    }

    #[test]
    fn zone_map_page_bounds_reject_noncanonical_scalars_without_roundtrip() {
        let mut bounds = ZoneColumnSummary::default();
        bounds.merge_value(ZoneScalar::Signed(0));
        let mut overlong = encode_bounds(&bounds).unwrap();
        overlong.splice(5..6, [0x80, 0]);
        assert!(decode_bounds(&overlong).is_err());
        let mut bounds = ZoneColumnSummary::default();
        bounds.merge_value(ZoneScalar::Unsigned(u64::MAX));
        let mut overflowing = encode_bounds(&bounds).unwrap();
        overflowing[14] = 3;
        assert!(decode_bounds(&overflowing).is_err());
        let mut bounds = ZoneColumnSummary::default();
        bounds.merge_value(ZoneScalar::Binary {
            prefix: vec![42],
            truncated: false,
        });
        let mut invalid_flag = encode_bounds(&bounds).unwrap();
        invalid_flag[5] = 2;
        assert!(decode_bounds(&invalid_flag).is_err());
    }

    #[test]
    fn zone_map_page_typed_bounds_cover_all_scalar_domains() {
        let values = [
            ZoneScalar::Signed(i64::MIN),
            ZoneScalar::Unsigned(u64::MAX),
            ZoneScalar::Float32((-0.0f32).to_bits()),
            ZoneScalar::Float64(f64::MAX.to_bits()),
            ZoneScalar::Decimal("-12345678901234567890123456789.012345".into()),
            ZoneScalar::Temporal(i64::MIN),
            ZoneScalar::Binary {
                prefix: vec![0, 255, 128],
                truncated: true,
            },
            ZoneScalar::Text {
                weight: vec![0, 255, 128],
                truncated: true,
            },
        ];
        for value in values {
            let summary = ZoneColumnSummary {
                state: ZoneMapState::Ready,
                has_null: true,
                has_non_null: true,
                min: Some(value.clone()),
                max: Some(value.clone()),
                truncated: value.was_truncated(),
                ..Default::default()
            };
            let encoded = encode_bounds(&summary).unwrap();
            let decoded = decode_bounds(&encoded).unwrap();
            assert_eq!(decoded.min, summary.min);
            assert_eq!(decoded.max, summary.max);
            for length in 0..encoded.len() {
                assert!(decode_bounds(&encoded[..length]).is_err());
            }
            for op in [
                crate::parser::BinaryOp::Eq,
                crate::parser::BinaryOp::NotEq,
                crate::parser::BinaryOp::Lt,
                crate::parser::BinaryOp::GtEq,
            ] {
                assert_eq!(
                    comparison_may_match(&summary, op, &value),
                    comparison_may_match(&decoded, op, &value)
                );
            }
        }
    }

    #[test]
    fn zone_map_page_borrowed_membership_matches_owned_and_rejects_bad_lengths() {
        for values in [0, 1, 32, 33, 100, 500] {
            let mut owned = ZoneMembership::empty();
            for i in 0..values {
                owned.insert_hash(zone_membership_hash(&ZoneScalar::Signed(i)));
            }
            let bytes = encode_membership(Some(&owned)).unwrap();
            let view = MembershipView::open(&bytes).unwrap();
            for i in -100..1000 {
                let hash = zone_membership_hash(&ZoneScalar::Signed(i));
                assert_eq!(view.probe(hash), Some(owned.may_contain_hash(hash)));
            }
            for length in 0..bytes.len() {
                assert!(MembershipView::open(&bytes[..length]).is_err());
            }
        }
        for membership in [None, Some(ZoneMembership::Saturated)] {
            let bytes = encode_membership(membership.as_ref()).unwrap();
            assert_eq!(
                MembershipView::open(&bytes).unwrap().probe(123),
                membership.map(|_| true)
            );
        }
    }

    #[test]
    fn zone_map_page_typed_predicates_match_resident_and_skip_unneeded_membership() {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = Arc::new(parking_lot::Mutex::new(
            LogManager::open(dir.path()).unwrap(),
        ));
        let pool = BufferPool::new(
            8,
            Arc::clone(&log),
            DurabilityMode::Strict,
            Duration::from_secs(5),
        );
        let path = crate::storage::naming::table_object_path(dir.path(), "test", "typed", 17, "zm");
        let file =
            PageFile::new(path, 17, 1, Arc::clone(&pool), log, DurabilityMode::Strict).unwrap();
        let mut zones = BTreeMap::new();
        for id in 0..100 {
            let mut summary = ZoneColumnSummary::default();
            summary.enable_membership_with_layout(ZoneMembershipLayout::default());
            summary.merge_null();
            summary.merge_value(ZoneScalar::Signed(id as i64));
            summary.merge_value(ZoneScalar::Signed(id as i64 + 200));
            let mut zone = ZoneSummary {
                rebuild_scn: 100,
                ..ZoneSummary::default()
            };
            zone.columns.insert(0, summary);
            zones.insert(id, zone);
        }
        let mut writer = Writer::new(&file, 99, 64).unwrap();
        for (&id, zone) in &zones {
            writer.meta(id, zone).unwrap();
        }
        for (&id, zone) in &zones {
            writer.bounds(0, id, &zone.columns[&0]).unwrap();
        }
        for (&id, zone) in &zones {
            writer
                .membership(0, id, zone.columns[&0].membership.as_ref())
                .unwrap();
        }
        writer.bounds(2, 0, &zones[&0].columns[&0]).unwrap();
        // A valid physical page containing a malformed optional membership.
        writer
            .push(key(2, Kind::Membership, 0), Ok(vec![0; 8]))
            .unwrap();
        writer.finish().unwrap();
        file.flush().unwrap();
        pool.flush_and_evict_all().unwrap();
        let reader = Reader::open(&file, 99, 64, u64::MAX).unwrap();
        let impossible_now = ZonePredicateProgram::Compare {
            column_id: 0,
            op: crate::parser::BinaryOp::Eq,
            query: ZoneScalar::Signed(-1),
        };
        let older = Reader::open(&file, 99, 64, 99).unwrap();
        assert_eq!(
            older.may_match_with_cost(0, &impossible_now).unwrap(),
            (true, 32)
        );
        assert!(!Reader::open(&file, 99, 64, 100)
            .unwrap()
            .may_match(0, &impossible_now)
            .unwrap());
        assert!(!Reader::open(&file, 99, 64, 101)
            .unwrap()
            .may_match(0, &impossible_now)
            .unwrap());
        for query in [-1, 0, 50, 199, 200, 250, 301] {
            for op in [
                crate::parser::BinaryOp::Eq,
                crate::parser::BinaryOp::NotEq,
                crate::parser::BinaryOp::Lt,
                crate::parser::BinaryOp::GtEq,
            ] {
                let predicate = ZonePredicateProgram::Compare {
                    column_id: 0,
                    op,
                    query: ZoneScalar::Signed(query),
                };
                reader
                    .visit_matches(
                        zones.keys().copied().chain(std::iter::once(1000)),
                        &predicate,
                        |id, matched, _| {
                            assert_eq!(
                                matched,
                                zones.get(&id).is_none_or(|zone| predicate.may_match(zone))
                            );
                            Ok(())
                        },
                    )
                    .unwrap();
                // Reverse traversal exercises hint replacement and key limits.
                reader
                    .visit_matches(zones.keys().rev().copied(), &predicate, |id, matched, _| {
                        assert_eq!(matched, predicate.may_match(&zones[&id]));
                        Ok(())
                    })
                    .unwrap();
            }
        }
        // Bounds-only predicates must not fetch/parse the damaged bitmap.
        let bounds_only = ZonePredicateProgram::Between {
            column_id: 2,
            low: ZoneScalar::Signed(50),
            high: ZoneScalar::Signed(70),
        };
        assert!(reader.may_match(0, &bounds_only).unwrap());
        let equality = ZonePredicateProgram::Compare {
            column_id: 2,
            op: crate::parser::BinaryOp::Eq,
            query: ZoneScalar::Signed(50),
        };
        assert!(reader.may_match(0, &equality).is_err());
        let range = ZonePredicateProgram::Between {
            column_id: 0,
            low: ZoneScalar::Signed(50),
            high: ZoneScalar::Signed(70),
        };
        for (&id, zone) in &zones {
            assert_eq!(reader.may_match(id, &range).unwrap(), range.may_match(zone));
        }
        assert!(reader.may_match(1000, &range).unwrap());
        let hits = pool.buffer_stats().hits;
        reader
            .visit_matches(0..100, &range, |_, _, _| Ok(()))
            .unwrap();
        let batch_hits = pool.buffer_stats().hits - hits;
        let hits = pool.buffer_stats().hits;
        for zone in 0..100 {
            reader.probe_zone(zone, &range).unwrap();
        }
        let point_hits = pool.buffer_stats().hits - hits;
        assert!(
            batch_hits * 3 < point_hits,
            "bounded range reads must amortize page access: {batch_hits} versus {point_hits}"
        );
        let eq = |value| ZonePredicateProgram::Compare {
            column_id: 0,
            op: crate::parser::BinaryOp::Eq,
            query: ZoneScalar::Signed(value),
        };
        for program in [
            ZonePredicateProgram::Never,
            ZonePredicateProgram::And(Box::new(range.clone()), Box::new(eq(250))),
            ZonePredicateProgram::Or(Box::new(eq(50)), Box::new(eq(250))),
            ZonePredicateProgram::And(
                Box::new(ZonePredicateProgram::Or(
                    Box::new(eq(-1)),
                    Box::new(eq(200)),
                )),
                Box::new(range.clone()),
            ),
            ZonePredicateProgram::IsNull {
                column_id: 0,
                negated: false,
            },
            ZonePredicateProgram::IsNull {
                column_id: 0,
                negated: true,
            },
            ZonePredicateProgram::NullSafeNull { column_id: 0 },
            ZonePredicateProgram::InList {
                column_id: 0,
                values: vec![
                    ZoneScalar::Signed(-1),
                    ZoneScalar::Signed(50),
                    ZoneScalar::Signed(250),
                ],
            },
            ZonePredicateProgram::Between {
                column_id: u32::MAX,
                low: ZoneScalar::Signed(-1),
                high: ZoneScalar::Signed(1),
            },
        ] {
            // Cross two full windows and a short tail, including missing zones.
            reader
                .visit_matches(0..139, &program, |zone, actual, _| {
                    assert_eq!(
                        actual,
                        reader.probe_zone(zone, &program)?.0,
                        "zone={zone} program={program:?}"
                    );
                    Ok(())
                })
                .unwrap();
            older
                .visit_matches(0..139, &program, |zone, actual, _| {
                    assert_eq!(actual, older.probe_zone(zone, &program)?.0);
                    Ok(())
                })
                .unwrap();
        }
        let hits = pool.buffer_stats().hits;
        Reader::open_and_visit_matches(&file, 99, 64, u64::MAX, [], &range, |_, _, _| Ok(()))
            .unwrap();
        assert_eq!(
            pool.buffer_stats().hits - hits,
            2,
            "fresh traversal needs only opening and final root checks"
        );
        // Invalidation during a fresh traversal must still fail its final fence.
        assert!(
            Reader::open_and_visit_matches(&file, 99, 64, u64::MAX, [0], &range, |_, _, _| {
                reader.invalidate()?;
                Ok(())
            },)
            .is_err()
        );
        assert!(reader.may_match(0, &range).is_err());
        assert!(Reader::open(&file, 99, 64, u64::MAX).is_err());
        file.flush().unwrap();
        pool.flush_and_evict_all().unwrap();
        assert!(Reader::open(&file, 99, 64, u64::MAX).is_err());
    }
}
