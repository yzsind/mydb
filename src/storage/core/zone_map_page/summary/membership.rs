//! Borrowed membership records. Sparse Bloom filters use independently
//! addressable 512-bit Rice-coded blocks; a probe decodes one stack block.
use super::*;

const BLOCK_WORDS: usize = 8;
const BLOCK_BITS: usize = BLOCK_WORDS * 64;
const OFFSET_MASK: usize = (1 << 13) - 1;
const RAW_BLOCK: usize = 7;
const RICE_KIND: u8 = 4;

pub(in crate::storage::core) fn encode_membership(
    value: Option<&ZoneMembership>,
) -> Result<Vec<u8>, QueryError> {
    let mut bytes = vec![RECORD_VERSION, 0, 0, 0, 0, 0, 0, 0];
    match value {
        None => {}
        Some(ZoneMembership::Saturated) => bytes[1] = 3,
        Some(ZoneMembership::Exact {
            hashes,
            bloom_words,
            bloom_hashes,
        }) => {
            if hashes.len() > ZONE_MEMBERSHIP_EXACT_LIMIT
                || hashes.windows(2).any(|pair| pair[0] >= pair[1])
                || !ZoneMembership::valid_layout(usize::from(*bloom_words), *bloom_hashes)
            {
                return Err(corrupt("invalid exact membership"));
            }
            bytes[1] = 1;
            bytes[2] = *bloom_hashes;
            write_u16(&mut bytes, 4, hashes.len() as u16);
            write_u16(&mut bytes, 6, *bloom_words);
            for hash in hashes {
                bytes.extend_from_slice(&hash.to_le_bytes());
            }
        }
        Some(ZoneMembership::Bloom { words, hashes, .. }) => {
            if !ZoneMembership::valid_layout(words.len(), *hashes) {
                return Err(corrupt("invalid Bloom membership"));
            }
            let raw_len = MEMBERSHIP_HEADER + words.len() * 8;
            let blocks = words.len() / BLOCK_WORDS;
            let data_start = MEMBERSHIP_HEADER + blocks * 2;
            let mut packed = Vec::with_capacity(data_start + words.len() * 8);
            packed.resize(data_start, 0);
            packed[0] = RECORD_VERSION;
            packed[1] = RICE_KIND;
            packed[2] = *hashes;
            write_u16(&mut packed, 4, words.len() as u16);
            write_u16(&mut packed, 6, words.len() as u16);
            for (block_id, block) in words.chunks_exact(BLOCK_WORDS).enumerate() {
                let codec = encode_block(block, &mut packed);
                let end = packed.len() - data_start;
                if packed.len() >= raw_len || end > OFFSET_MASK {
                    break;
                }
                write_u16(
                    &mut packed,
                    MEMBERSHIP_HEADER + block_id * 2,
                    (end | (codec << 13)) as u16,
                );
            }
            // No compressed representation may be larger than the raw one.
            // Dense filters stay directly borrowable and pay no Rice decode.
            if packed.len() < raw_len {
                return Ok(packed);
            }
            bytes = Vec::with_capacity(raw_len);
            bytes.resize(MEMBERSHIP_HEADER, 0);
            bytes[0] = RECORD_VERSION;
            bytes[1] = 2;
            bytes[2] = *hashes;
            write_u16(&mut bytes, 4, words.len() as u16);
            write_u16(&mut bytes, 6, words.len() as u16);
            for word in words {
                bytes.extend_from_slice(&word.to_le_bytes());
            }
        }
    }
    Ok(bytes)
}

fn encode_block(block: &[u64], packed: &mut Vec<u8>) -> usize {
    if block.iter().all(|word| *word == 0) {
        0
    } else {
        let mut sizes = [0usize; RAW_BLOCK];
        for_each_gap(block, |gap| {
            for (k, size) in sizes.iter_mut().enumerate() {
                *size += (gap >> k) + 1 + k;
            }
        });
        let (k, bits) = sizes
            .iter()
            .enumerate()
            .min_by_key(|(_, bits)| **bits)
            .unwrap();
        if bits.div_ceil(8) >= BLOCK_WORDS * 8 {
            for word in block {
                packed.extend_from_slice(&word.to_le_bytes());
            }
            RAW_BLOCK
        } else {
            let start = packed.len();
            let mut writer = BitsWriter {
                bytes: packed,
                start,
                used: 0,
            };
            for_each_gap(block, |gap| writer.gap(gap, k));
            k
        }
    }
}

/// Union a small delta without expanding and recompressing unrelated blocks.
/// The existing record remains the complete baseline, including when the
/// incoming exact set uses a different provisional Bloom layout.
pub(in crate::storage::core) fn merge_membership_record(
    bytes: Option<&[u8]>,
    destination: &ZoneColumnSummary,
    source: &ZoneColumnSummary,
) -> Result<Option<Vec<u8>>, QueryError> {
    let view = bytes.map(MembershipView::open).transpose()?;
    let present = view.as_ref().is_some_and(|view| view.kind != 0);
    if destination.state == ZoneMapState::Unknown
        || source.state == ZoneMapState::Unknown
        || (source.has_non_null && source.membership.is_none())
    {
        return if present {
            encode_membership(None).map(Some)
        } else {
            Ok(None)
        };
    }
    let Some(incoming) = source.membership.as_ref() else {
        return Ok(None);
    };
    if !present {
        return if destination.has_non_null {
            Ok(None)
        } else {
            encode_membership(Some(incoming)).map(Some)
        };
    }
    let view = view.unwrap();
    if view.kind == 3 {
        return Ok(None);
    }
    let original = bytes.unwrap();
    if let ZoneMembership::Exact { hashes, .. } = incoming {
        if matches!(view.kind, 2 | RICE_KIND) {
            // Repeated values commonly leave the filter unchanged. Probe the
            // borrowed record first: avoid copying the whole filter, Rice
            // re-encoding, and a full bitmap population count in that case.
            // An invalid compressed block is not evidence of containment;
            // fall through to the checked update path so it reports corruption.
            if hashes.iter().all(|&hash| view.probe(hash) == Some(true)) {
                return Ok(None);
            }
            let mut blocks = [false; ZONE_MEMBERSHIP_MAX_BLOOM_WORDS / BLOCK_WORDS];
            for &hash in hashes {
                let bit = ZoneMembership::bloom_positions(hash, view.count, view.hashes)
                    .next()
                    .ok_or_else(|| corrupt("invalid Bloom probe count"))?;
                blocks[bit / BLOCK_BITS] = true;
            }
            let mut result = original.to_vec();
            let directory = view.count / BLOCK_WORDS * 2;
            if view.kind == RICE_KIND {
                result.truncate(MEMBERSHIP_HEADER + directory);
            }
            let mut changed = false;
            for (block, touched) in blocks
                .iter()
                .copied()
                .enumerate()
                .take(view.count / BLOCK_WORDS)
            {
                if !touched {
                    if view.kind == RICE_KIND {
                        let (codec, encoded) = view.encoded_block(block)?;
                        result.extend_from_slice(encoded);
                        let end = result.len() - MEMBERSHIP_HEADER - directory;
                        write_u16(
                            &mut result,
                            MEMBERSHIP_HEADER + block * 2,
                            (end | (codec << 13)) as u16,
                        );
                    }
                    continue;
                }
                let mut words = if view.kind == RICE_KIND {
                    view.block(block)?
                } else {
                    std::array::from_fn(|i| view.word(block * BLOCK_WORDS + i))
                };
                let before = words;
                for &hash in hashes {
                    for bit in ZoneMembership::bloom_positions(hash, view.count, view.hashes) {
                        if bit / BLOCK_BITS == block {
                            words[(bit / 64) % BLOCK_WORDS] |= 1 << (bit % 64);
                        }
                    }
                }
                changed |= words != before;
                if view.kind == RICE_KIND {
                    let codec = encode_block(&words, &mut result);
                    let end = result.len() - MEMBERSHIP_HEADER - directory;
                    if end > OFFSET_MASK || result.len() >= MEMBERSHIP_HEADER + view.count * 8 {
                        // A growing compressed filter may need raw encoding.
                        return merge_membership_owned(original, incoming);
                    }
                    write_u16(
                        &mut result,
                        MEMBERSHIP_HEADER + block * 2,
                        (end | (codec << 13)) as u16,
                    );
                } else {
                    for (index, word) in words.iter().enumerate() {
                        let at = MEMBERSHIP_HEADER + (block * BLOCK_WORDS + index) * 8;
                        result[at..at + 8].copy_from_slice(&word.to_le_bytes());
                    }
                }
            }
            // Untouched trailing blocks can also cross the raw size limit.
            if view.kind == RICE_KIND && result.len() >= MEMBERSHIP_HEADER + view.count * 8 {
                return merge_membership_owned(original, incoming);
            }
            if view.kind == 2 {
                let set_bits = result[MEMBERSHIP_HEADER..]
                    .chunks_exact(8)
                    .map(|word| u64::from_le_bytes(word.try_into().unwrap()).count_ones())
                    .sum();
                if ZoneMembership::set_bits_saturated(set_bits, view.count) {
                    return encode_membership(Some(&ZoneMembership::Saturated)).map(Some);
                }
            }
            return Ok(changed.then_some(result));
        }
    }
    merge_membership_owned(original, incoming)
}

/// Foreground (DML) form of `merge_membership_record`. First publications,
/// exact fingerprint sets and saturation are applied as usual, but growing a
/// Bloom filter (or promoting an exact set to one) drops the record instead.
/// Without it the zone merely cannot prove a value absent, which is always
/// conservative. A zone being filled with distinct values would otherwise
/// rewrite and log up to 8 KiB of filter for nearly every row under its merge
/// latch; dropping costs one small write per zone, after which DML on that
/// column is a no-op until background maintenance rebuilds the quiet zone.
pub(in crate::storage::core) fn merge_membership_record_foreground(
    bytes: Option<&[u8]>,
    destination: &ZoneColumnSummary,
    source: &ZoneColumnSummary,
) -> Result<Option<Vec<u8>>, QueryError> {
    let Some(merged) = merge_membership_record(bytes, destination, source)? else {
        return Ok(None);
    };
    let grows_existing = match bytes {
        Some(bytes) => MembershipView::open(bytes)?.kind != 0,
        None => false,
    };
    if grows_existing && matches!(MembershipView::open(&merged)?.kind, 2 | RICE_KIND) {
        return encode_membership(None).map(Some);
    }
    Ok(Some(merged))
}

fn merge_membership_owned(
    bytes: &[u8],
    incoming: &ZoneMembership,
) -> Result<Option<Vec<u8>>, QueryError> {
    let mut value =
        decode_membership(bytes)?.ok_or_else(|| corrupt("missing membership baseline"))?;
    // Owned decode normalizes dense bitmaps to Saturated. Persist that
    // normalization even if merge_from subsequently leaves it unchanged:
    // the old bitmap may not contain the incoming values.
    if matches!(value, ZoneMembership::Saturated) && bytes[1] != 3 {
        return encode_membership(Some(&value)).map(Some);
    }
    let before = value.union_change_token();
    value.merge_from(incoming);
    if value.union_change_token() == before {
        Ok(None)
    } else {
        encode_membership(Some(&value)).map(Some)
    }
}

fn for_each_gap(words: &[u64], mut visit: impl FnMut(usize)) {
    let mut next = 0;
    for (index, word) in words.iter().copied().enumerate() {
        let mut remaining = word;
        while remaining != 0 {
            let position = index * 64 + remaining.trailing_zeros() as usize;
            visit(position - next);
            next = position + 1;
            remaining &= remaining - 1;
        }
    }
    // A terminal position at 512 makes truncation distinguishable from zeros.
    visit(BLOCK_BITS - next);
}

struct BitsWriter<'a> {
    bytes: &'a mut Vec<u8>,
    start: usize,
    used: usize,
}
impl BitsWriter<'_> {
    fn gap(&mut self, gap: usize, k: usize) {
        let marker = self.used + (gap >> k);
        let end = marker + 1 + k;
        self.bytes.resize(self.start + end.div_ceil(8), 0);
        self.bytes[self.start + marker / 8] |= 1 << (marker % 8);
        if k != 0 {
            let at = marker + 1;
            let bits = ((gap & ((1 << k) - 1)) as u16) << (at % 8);
            self.bytes[self.start + at / 8] |= bits as u8;
            if at % 8 + k > 8 {
                self.bytes[self.start + at / 8 + 1] |= (bits >> 8) as u8;
            }
        }
        self.used = end;
    }
}

/// Heap capacity required by an owned decode, after bounded structural
/// validation. A compressed record can expand to a complete 8 KiB bitmap.
pub(in crate::storage::core) fn membership_owned_bytes(bytes: &[u8]) -> Result<usize, QueryError> {
    let view = MembershipView::open(bytes)?;
    Ok(if matches!(view.kind, 1 | 2 | RICE_KIND) {
        view.count * std::mem::size_of::<u64>()
    } else {
        0
    })
}

pub(in crate::storage::core) fn decode_membership(
    bytes: &[u8],
) -> Result<Option<ZoneMembership>, QueryError> {
    let view = MembershipView::open(bytes)?;
    Ok(match view.kind {
        0 => None,
        1 => Some(ZoneMembership::Exact {
            hashes: (0..view.count).map(|i| view.word(i)).collect(),
            bloom_words: read_u16(bytes, 6),
            bloom_hashes: view.hashes,
        }),
        2 | RICE_KIND => {
            let words = if view.kind == RICE_KIND {
                let mut words = Vec::with_capacity(view.count);
                for block in 0..view.count / BLOCK_WORDS {
                    words.extend_from_slice(&view.block(block)?);
                }
                words
            } else {
                (0..view.count).map(|i| view.word(i)).collect()
            };
            if ZoneMembership::is_saturated(&words) {
                Some(ZoneMembership::Saturated)
            } else {
                Some(ZoneMembership::Bloom {
                    set_bits: ZoneMembership::count_set_bits(&words),
                    words,
                    hashes: view.hashes,
                })
            }
        }
        _ => Some(ZoneMembership::Saturated),
    })
}

pub(super) struct MembershipView<'a> {
    bytes: &'a [u8],
    kind: u8,
    hashes: u8,
    count: usize,
}
impl<'a> MembershipView<'a> {
    pub(super) fn open(bytes: &'a [u8]) -> Result<Self, QueryError> {
        if bytes.len() < MEMBERSHIP_HEADER || bytes[0] != RECORD_VERSION || bytes[3] != 0 {
            return Err(corrupt("invalid membership header"));
        }
        let (kind, hashes, count, words) = (
            bytes[1],
            bytes[2],
            read_u16(bytes, 4) as usize,
            read_u16(bytes, 6) as usize,
        );
        if count > ZONE_MEMBERSHIP_MAX_BLOOM_WORDS {
            return Err(corrupt("invalid membership length"));
        }
        let view = Self {
            bytes: &bytes[MEMBERSHIP_HEADER..],
            kind,
            hashes,
            count,
        };
        if kind == RICE_KIND {
            if count != words
                || !ZoneMembership::valid_layout(words, hashes)
                || view.bytes.len() >= count * 8
                || view.bytes.len() < count / BLOCK_WORDS * 2
            {
                return Err(corrupt("invalid Rice membership layout"));
            }
            let directory = count / BLOCK_WORDS * 2;
            let data_len = view.bytes.len() - directory;
            let mut prior = 0;
            for block in 0..count / BLOCK_WORDS {
                let entry = read_u16(view.bytes, block * 2) as usize;
                let (end, codec) = (entry & OFFSET_MASK, entry >> 13);
                if end < prior
                    || end > data_len
                    || (end == prior && codec != 0)
                    || (end != prior && codec == RAW_BLOCK && end - prior != 64)
                    || (codec != RAW_BLOCK && end - prior >= 64)
                {
                    return Err(corrupt("invalid Rice block offset or length"));
                }
                prior = end;
            }
            if prior != data_len {
                return Err(corrupt("unreferenced Rice block bytes"));
            }
            return Ok(view);
        }
        if bytes.len() != MEMBERSHIP_HEADER + count * 8 {
            return Err(corrupt("invalid membership length"));
        }
        match kind {
            0 | 3 if count == 0 && hashes == 0 && words == 0 => {}
            1 if count <= ZONE_MEMBERSHIP_EXACT_LIMIT
                && ZoneMembership::valid_layout(words, hashes) =>
            {
                if (1..count).any(|i| view.word(i - 1) >= view.word(i)) {
                    return Err(corrupt("unordered exact membership"));
                }
            }
            2 if count == words && ZoneMembership::valid_layout(words, hashes) => {}
            _ => return Err(corrupt("invalid membership layout")),
        }
        Ok(view)
    }

    fn word(&self, index: usize) -> u64 {
        read_u64(self.bytes, index * 8)
    }

    fn encoded_block(&self, block: usize) -> Result<(usize, &[u8]), QueryError> {
        if block >= self.count / BLOCK_WORDS {
            return Err(corrupt("invalid Rice block index"));
        }
        let end = read_u16(self.bytes, block * 2) as usize;
        let codec = end >> 13;
        let end = end & OFFSET_MASK;
        let start = if block == 0 {
            0
        } else {
            read_u16(self.bytes, (block - 1) * 2) as usize & OFFSET_MASK
        };
        let directory = self.count / BLOCK_WORDS * 2;
        let encoded = self
            .bytes
            .get(directory + start..directory + end)
            .ok_or_else(|| corrupt("invalid Rice block range"))?;
        Ok((codec, encoded))
    }

    fn block(&self, block: usize) -> Result<[u64; BLOCK_WORDS], QueryError> {
        let (codec, encoded) = self.encoded_block(block)?;
        let mut words = [0u64; BLOCK_WORDS];
        if encoded.is_empty() {
            return Ok(words);
        }
        if codec == RAW_BLOCK {
            for (index, word) in words.iter_mut().enumerate() {
                *word = read_u64(encoded, index * 8);
            }
            return Ok(words);
        }
        let mut bit = 0usize;
        let mut position = 0usize;
        loop {
            let mut quotient = 0usize;
            while !read_bit(encoded, &mut bit)? {
                quotient += 1;
                if quotient > BLOCK_BITS >> codec {
                    return Err(corrupt("overlong Rice quotient"));
                }
            }
            let mut remainder = 0usize;
            for shift in 0..codec {
                remainder |= usize::from(read_bit(encoded, &mut bit)?) << shift;
            }
            position += (quotient << codec) | remainder;
            if position > BLOCK_BITS {
                return Err(corrupt("Rice position exceeds block"));
            }
            if position == BLOCK_BITS {
                if encoded.len() * 8 - bit >= 8
                    || (bit..encoded.len() * 8).any(|p| encoded[p / 8] & (1 << (p % 8)) != 0)
                {
                    return Err(corrupt("trailing Rice bits"));
                }
                return Ok(words);
            }
            words[position / 64] |= 1u64 << (position % 64);
            position += 1;
        }
    }

    pub(super) fn probe(&self, hash: u64) -> Option<bool> {
        match self.kind {
            0 => None,
            1 => {
                let (mut lo, mut hi) = (0, self.count);
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    match self.word(mid).cmp(&hash) {
                        std::cmp::Ordering::Equal => return Some(true),
                        std::cmp::Ordering::Less => lo = mid + 1,
                        std::cmp::Ordering::Greater => hi = mid,
                    }
                }
                Some(false)
            }
            2 => Some(
                ZoneMembership::bloom_positions(hash, self.count, self.hashes)
                    .all(|bit| self.word(bit >> 6) & (1u64 << (bit & 63)) != 0),
            ),
            RICE_KIND => {
                let mut positions = ZoneMembership::bloom_positions(hash, self.count, self.hashes);
                let first = positions.next()?;
                let block = match self.block(first / BLOCK_BITS) {
                    Ok(block) => block,
                    Err(_) => {
                        ZONE_MAP_FALLBACK_CORRUPT.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }
                };
                Some(
                    std::iter::once(first)
                        .chain(positions)
                        .all(|bit| block[(bit / 64) % BLOCK_WORDS] & (1u64 << (bit % 64)) != 0),
                )
            }
            _ => Some(true),
        }
    }
}

fn read_bit(bytes: &[u8], bit: &mut usize) -> Result<bool, QueryError> {
    let value = bytes
        .get(*bit / 8)
        .ok_or_else(|| corrupt("truncated Rice code"))?
        & (1 << (*bit % 8))
        != 0;
    *bit += 1;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bloom(words: Vec<u64>, hashes: u8) -> ZoneMembership {
        ZoneMembership::Bloom {
            set_bits: ZoneMembership::count_set_bits(&words),
            words,
            hashes,
        }
    }

    fn next(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    }

    #[test]
    fn zone_map_membership_incremental_union_matches_owned() {
        let mut seed = 0x517cc1b727220a95;
        for count in [128, 256, 512, 1024] {
            for hash_count in [1, 3, 5, 8] {
                for draws in [0, 24, 100, count * 8, count * 32] {
                    let mut membership = ZoneMembership::empty_with_layout(ZoneMembershipLayout {
                        words: count,
                        hashes: hash_count,
                    });
                    for _ in 0..draws {
                        membership.insert_hash(next(&mut seed));
                    }
                    let mut bytes = encode_membership(Some(&membership)).unwrap();
                    let destination = ZoneColumnSummary {
                        has_non_null: true,
                        ..Default::default()
                    };
                    for batch in [1, 10, 32, 80] {
                        // Deliberately use a different layout: exact hashes
                        // must be inserted into the baseline's layout.
                        let mut incoming =
                            ZoneMembership::empty_with_layout(ZoneMembershipLayout {
                                words: 128,
                                hashes: 5,
                            });
                        let mut added = Vec::new();
                        for _ in 0..batch {
                            let hash = next(&mut seed);
                            added.push(hash);
                            incoming.insert_hash(hash);
                        }
                        let source = ZoneColumnSummary {
                            has_non_null: true,
                            membership: Some(incoming.clone()),
                            ..Default::default()
                        };
                        membership.merge_from(&incoming);
                        if let Some(updated) =
                            merge_membership_record(Some(&bytes), &destination, &source).unwrap()
                        {
                            bytes = updated;
                        }
                        let decoded = decode_membership(&bytes).unwrap().unwrap();
                        for hash in added.into_iter().chain((0..256).map(|_| next(&mut seed))) {
                            assert_eq!(
                                decoded.may_contain_hash(hash),
                                membership.may_contain_hash(hash),
                                "words={count}, hashes={hash_count}, draws={draws}, batch={batch}"
                            );
                        }
                        assert!(merge_membership_record(Some(&bytes), &destination, &source)
                            .unwrap()
                            .is_none());
                    }
                }
            }
        }
    }

    #[test]
    fn zone_map_membership_incremental_preserves_missing_and_unknown_semantics() {
        let mut membership = ZoneMembership::empty();
        membership.insert_hash(123);
        let bytes = encode_membership(Some(&membership)).unwrap();
        let source = ZoneColumnSummary {
            has_non_null: true,
            membership: Some(membership),
            ..Default::default()
        };
        let destination = ZoneColumnSummary {
            has_non_null: true,
            ..Default::default()
        };
        assert!(merge_membership_record(None, &destination, &source)
            .unwrap()
            .is_none());
        assert!(
            merge_membership_record(None, &ZoneColumnSummary::default(), &source)
                .unwrap()
                .is_some()
        );
        let null_source = ZoneColumnSummary {
            has_null: true,
            ..Default::default()
        };
        assert!(
            merge_membership_record(Some(&bytes), &destination, &null_source)
                .unwrap()
                .is_none()
        );
        for source in [
            destination.clone(),
            ZoneColumnSummary {
                state: ZoneMapState::Unknown,
                ..Default::default()
            },
        ] {
            let cleared = merge_membership_record(Some(&bytes), &destination, &source)
                .unwrap()
                .unwrap();
            assert!(decode_membership(&cleared).unwrap().is_none());
        }
        let mut corrupt = bytes.clone();
        corrupt[0] = 255;
        assert!(merge_membership_record(Some(&corrupt), &destination, &source).is_err());
    }

    #[test]
    fn zone_map_page_rice_owned_and_borrowed_match_random_bitmaps() {
        let mut seed = 0x517cc1b727220a95;
        for count in [128usize, 256, 512, 1024] {
            for hashes in [1, 5, 8] {
                for draws in [0, count / 2, count * 6, count * 64] {
                    let mut words = vec![0; count];
                    for _ in 0..draws {
                        let bit = next(&mut seed) as usize % (count * 64);
                        words[bit / 64] |= 1 << (bit % 64);
                    }
                    if draws != 0 {
                        for hash in 0..32 {
                            for bit in ZoneMembership::bloom_positions(hash, count, hashes) {
                                words[bit / 64] |= 1 << (bit % 64);
                            }
                        }
                        for bit in [0, 63, 64, 127, 255, 511, 512, 1023, count * 64 - 1] {
                            words[bit / 64] |= 1 << (bit % 64);
                        }
                    }
                    let source = bloom(words.clone(), hashes);
                    let bytes = encode_membership(Some(&source)).unwrap();
                    assert!(bytes.len() <= MEMBERSHIP_HEADER + count * 8);
                    assert_eq!(membership_owned_bytes(&bytes).unwrap(), count * 8);
                    let restored = decode_membership(&bytes).unwrap().unwrap();
                    if let ZoneMembership::Bloom { words: actual, .. } = &restored {
                        assert_eq!(
                            *actual, words,
                            "count={count}, hashes={hashes}, draws={draws}"
                        );
                    } else {
                        assert!(matches!(restored, ZoneMembership::Saturated));
                        assert!(ZoneMembership::is_saturated(&words));
                    }
                    let view = MembershipView::open(&bytes).unwrap();
                    for hash in (0..64).chain((0..64).map(|_| next(&mut seed))) {
                        assert_eq!(view.probe(hash), Some(source.may_contain_hash(hash)));
                        if source.may_contain_hash(hash) {
                            assert!(restored.may_contain_hash(hash));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn zone_map_page_rice_zero_raw_blocks_and_exact_layout() {
        let zero = encode_membership(Some(&bloom(vec![0; 128], 5))).unwrap();
        assert_eq!(zero[1], RICE_KIND);
        assert_eq!(zero.len(), MEMBERSHIP_HEADER + 16 * 2);
        assert_eq!(MembershipView::open(&zero).unwrap().probe(0), Some(false));
        let dense = encode_membership(Some(&bloom(vec![u64::MAX; 128], 5))).unwrap();
        assert_eq!(dense[1], 2, "dense filters must retain raw fallback");
        let mut words = vec![0; 128];
        words[..8].fill(u64::MAX);
        let mixed = encode_membership(Some(&bloom(words, 5))).unwrap();
        assert_eq!(mixed[1], RICE_KIND);
        assert_eq!(
            read_u16(&mixed, MEMBERSHIP_HEADER) as usize >> 13,
            RAW_BLOCK
        );
        let view = MembershipView::open(&mixed).unwrap();
        assert_eq!(view.block(0).unwrap(), [u64::MAX; 8]);
        assert_eq!(view.block(1).unwrap(), [0; 8]);
        let exact = ZoneMembership::Exact {
            hashes: vec![0, 63, 64, u64::MAX],
            bloom_words: 128,
            bloom_hashes: 5,
        };
        let bytes = encode_membership(Some(&exact)).unwrap();
        assert_eq!(bytes[1], 1);
        assert_eq!(membership_owned_bytes(&bytes).unwrap(), 32);
        let view = MembershipView::open(&bytes).unwrap();
        assert_eq!(view.probe(u64::MAX), Some(true));
        assert_eq!(view.probe(62), Some(false));
        for member in [None, Some(&ZoneMembership::Saturated)] {
            assert_eq!(
                membership_owned_bytes(&encode_membership(member).unwrap()).unwrap(),
                0
            );
        }
    }

    fn one_block(codec: usize, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0; MEMBERSHIP_HEADER + 32];
        bytes[0] = RECORD_VERSION;
        bytes[1] = RICE_KIND;
        bytes[2] = 5;
        write_u16(&mut bytes, 4, 128);
        write_u16(&mut bytes, 6, 128);
        for block in 0..16 {
            write_u16(
                &mut bytes,
                MEMBERSHIP_HEADER + block * 2,
                (payload.len() | (if block == 0 { codec } else { 0 } << 13)) as u16,
            );
        }
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn zone_map_page_rice_rejects_offsets_truncation_and_overlong_codes() {
        let bytes = encode_membership(Some(&bloom(vec![1; 128], 5))).unwrap();
        assert_eq!(bytes[1], RICE_KIND);
        for cut in 0..bytes.len() {
            assert!(
                decode_membership(&bytes[..cut]).is_err(),
                "truncation at {cut}"
            );
        }
        let mut backward = bytes.clone();
        write_u16(&mut backward, MEMBERSHIP_HEADER + 2, 0);
        assert!(MembershipView::open(&backward).is_err());
        let mut outside = bytes.clone();
        write_u16(&mut outside, MEMBERSHIP_HEADER, OFFSET_MASK as u16);
        assert!(MembershipView::open(&outside).is_err());
        assert!(MembershipView::open(&one_block(RAW_BLOCK, &[0; 63])).is_err());
        let mut no_terminal = Vec::new();
        BitsWriter {
            bytes: &mut no_terminal,
            start: 0,
            used: 0,
        }
        .gap(0, 6);
        let mut beyond = Vec::new();
        BitsWriter {
            bytes: &mut beyond,
            start: 0,
            used: 0,
        }
        .gap(513, 6);
        let mut terminal = Vec::new();
        BitsWriter {
            bytes: &mut terminal,
            start: 0,
            used: 0,
        }
        .gap(512, 6);
        let mut trailing = terminal.clone();
        trailing.push(0);
        let mut padding = terminal.clone();
        *padding.last_mut().unwrap() |= 0x80;
        let hash = (0..10_000)
            .find(|hash| {
                ZoneMembership::bloom_positions(*hash, 128, 5)
                    .next()
                    .unwrap()
                    / BLOCK_BITS
                    == 0
            })
            .unwrap();
        for payload in [vec![0; 2], no_terminal, beyond, trailing, padding] {
            let malformed = one_block(6, &payload);
            let view = MembershipView::open(&malformed).unwrap();
            assert!(decode_membership(&malformed).is_err());
            assert_eq!(
                view.probe(hash),
                None,
                "bad target block must never exclude a row"
            );
            assert_eq!(
                view.block(1).unwrap(),
                [0; 8],
                "unrelated blocks remain independently readable"
            );
        }
        let valid = one_block(6, &terminal);
        assert!(decode_membership(&valid).is_ok());
    }
}
