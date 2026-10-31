//! Unicode 9.0 DUCET primary weights for utf8mb4_0900_ai_ci.
//! Generated from https://www.unicode.org/Public/UCA/9.0.0/allkeys.txt.
//! Copyright 2016 Unicode, Inc.; Unicode data license: https://www.unicode.org/license.txt.
//! Packed index (code point, weight offset, count), followed by little-endian u16 weights.
const DATA: &[u8] = include_bytes!("charset_uca9_primary.bin");
const COUNT: usize = 29809;
pub(super) const PRIMARY_EXPANSION_EXCEPTIONS: &[(char, usize)] = &[
    ('\u{fdfa}', 24),
    ('\u{fdfb}', 4),
    ('\u{337f}', 4),
    ('\u{321d}', 2),
];
const ASCII: &[&[u16]] = &[
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[0x201],
    &[0x202],
    &[0x203],
    &[0x204],
    &[0x205],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[],
    &[0x209],
    &[0x260],
    &[0x30c],
    &[0x398],
    &[0x1c12],
    &[0x399],
    &[0x396],
    &[0x305],
    &[0x317],
    &[0x318],
    &[0x38f],
    &[0x616],
    &[0x222],
    &[0x20d],
    &[0x277],
    &[0x394],
    &[0x1c3d],
    &[0x1c3e],
    &[0x1c3f],
    &[0x1c40],
    &[0x1c41],
    &[0x1c42],
    &[0x1c43],
    &[0x1c44],
    &[0x1c45],
    &[0x1c46],
    &[0x239],
    &[0x234],
    &[0x61a],
    &[0x61b],
    &[0x61c],
    &[0x266],
    &[0x38e],
    &[0x1c47],
    &[0x1c60],
    &[0x1c7a],
    &[0x1c8f],
    &[0x1caa],
    &[0x1ce5],
    &[0x1cf4],
    &[0x1d18],
    &[0x1d32],
    &[0x1d4c],
    &[0x1d65],
    &[0x1d77],
    &[0x1daa],
    &[0x1db9],
    &[0x1ddd],
    &[0x1e0c],
    &[0x1e21],
    &[0x1e33],
    &[0x1e71],
    &[0x1e95],
    &[0x1eb5],
    &[0x1ee3],
    &[0x1ef5],
    &[0x1eff],
    &[0x1f0b],
    &[0x1f21],
    &[0x319],
    &[0x395],
    &[0x31a],
    &[0x485],
    &[0x20b],
    &[0x482],
    &[0x1c47],
    &[0x1c60],
    &[0x1c7a],
    &[0x1c8f],
    &[0x1caa],
    &[0x1ce5],
    &[0x1cf4],
    &[0x1d18],
    &[0x1d32],
    &[0x1d4c],
    &[0x1d65],
    &[0x1d77],
    &[0x1daa],
    &[0x1db9],
    &[0x1ddd],
    &[0x1e0c],
    &[0x1e21],
    &[0x1e33],
    &[0x1e71],
    &[0x1e95],
    &[0x1eb5],
    &[0x1ee3],
    &[0x1ef5],
    &[0x1eff],
    &[0x1f0b],
    &[0x1f21],
    &[0x31b],
    &[0x61e],
    &[0x31c],
    &[0x620],
    &[],
];
const ASCII_PRIMARY: [u16; 128] = {
    let mut weights = [0; 128];
    let mut index = 0;
    while index < 128 {
        if !ASCII[index].is_empty() {
            weights[index] = ASCII[index][0];
        }
        index += 1;
    }
    weights
};

fn u32_at(offset: usize) -> u32 {
    u32::from_le_bytes(DATA[offset..offset + 4].try_into().unwrap())
}
fn emit(out: &mut Vec<u8>, weight: u16) {
    out.extend_from_slice(&weight.to_be_bytes());
}
fn single(cp: u32, emit: &mut impl FnMut(u16)) {
    let entry = if cp <= u16::MAX as u32 {
        static BMP: std::sync::OnceLock<Vec<u32>> = std::sync::OnceLock::new();
        let index = BMP.get_or_init(|| {
            let mut index = vec![0; 65536];
            for row in 0..COUNT {
                let position = row * 10;
                let cp = u32_at(position) as usize;
                if cp >= index.len() {
                    break;
                }
                let offset = u32_at(position + 4);
                let count =
                    u16::from_le_bytes(DATA[position + 8..position + 10].try_into().unwrap());
                index[cp] = ((count as u32) << 24) | (offset + 1);
            }
            index
        });
        index[cp as usize]
    } else {
        let mut lo = 0;
        let mut hi = COUNT;
        while lo < hi {
            let mid = (lo + hi) / 2;
            if u32_at(mid * 10) < cp {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo < COUNT && u32_at(lo * 10) == cp {
            let offset = u32_at(lo * 10 + 4);
            let count = u16::from_le_bytes(DATA[lo * 10 + 8..lo * 10 + 10].try_into().unwrap());
            ((count as u32) << 24) | (offset + 1)
        } else {
            0
        }
    };
    if entry != 0 {
        let offset = ((entry & 0x00ff_ffff) - 1) as usize;
        let count = (entry >> 24) as usize;
        for i in offset..offset + count {
            let p = COUNT * 10 + i * 2;
            emit(u16::from_le_bytes([DATA[p], DATA[p + 1]]));
        }
    } else {
        if (0x17000..=0x18aff).contains(&cp) {
            emit(0xfb00);
            emit(0x8000 | (cp - 0x17000) as u16);
            return;
        }
        let base = if (0x4e00..=0x9fd5).contains(&cp)
            || matches!(
                cp,
                0xfa0e
                    | 0xfa0f
                    | 0xfa11
                    | 0xfa13
                    | 0xfa14
                    | 0xfa1f
                    | 0xfa21
                    | 0xfa23
                    | 0xfa24
                    | 0xfa27
                    | 0xfa28
                    | 0xfa29
            ) {
            0xfb40
        } else if (0x3400..=0x4db5).contains(&cp)
            || (0x20000..=0x2a6d6).contains(&cp)
            || (0x2a700..=0x2b734).contains(&cp)
            || (0x2b740..=0x2b81d).contains(&cp)
            || (0x2b820..=0x2cea1).contains(&cp)
        {
            0xfb80
        } else {
            0xfbc0
        };
        emit(base + (cp >> 15) as u16);
        emit(0x8000 | (cp & 0x7fff) as u16);
    }
}
pub(super) fn scalar(ch: char, emit: &mut impl FnMut(u16)) {
    let cp = ch as u32;
    if ch.is_ascii() {
        for &weight in ASCII[cp as usize] {
            emit(weight);
        }
    } else if (0xac00..=0xd7a3).contains(&cp) {
        let index = cp - 0xac00;
        single(0x1100 + index / 588, emit);
        single(0x1161 + (index % 588) / 28, emit);
        if index % 28 != 0 {
            single(0x11a7 + index % 28, emit);
        }
    } else {
        single(cp, emit);
    }
}

pub(super) struct PrimaryWeights<'a> {
    chars: std::str::Chars<'a>,
    pending: smallvec::SmallVec<[u16; 18]>,
    position: usize,
}

pub(super) fn weights(value: &str) -> PrimaryWeights<'_> {
    PrimaryWeights {
        chars: value.chars(),
        pending: smallvec::SmallVec::new(),
        position: 0,
    }
}

impl Iterator for PrimaryWeights<'_> {
    type Item = u16;
    fn next(&mut self) -> Option<u16> {
        loop {
            if let Some(&weight) = self.pending.get(self.position) {
                self.position += 1;
                return Some(weight);
            }
            let ch = self.chars.next()?;
            if ch.is_ascii() {
                let weight = ASCII_PRIMARY[ch as usize];
                if weight != 0 {
                    return Some(weight);
                }
                continue;
            }
            self.pending.clear();
            self.position = 0;
            scalar(ch, &mut |weight| self.pending.push(weight));
        }
    }
}

pub(super) fn primary_into(value: &str, out: &mut Vec<u8>) {
    out.reserve(value.len().saturating_mul(2));
    for weight in weights(value) {
        emit(out, weight);
    }
}

pub(super) fn primary(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() * 2);
    primary_into(value, &mut out);
    out
}

/// A representable exclusive bound above all weights beginning with this
/// scalar's expansion. Using the next primary (rather than the next Unicode
/// code point) also handles accent/case equivalence and expansions safely.
pub(super) fn scalar_successor(ch: char) -> Option<char> {
    // Implicit CJK weights and ordinary ordered neighbors have an exact
    // representable bound. Require a larger weight before the old expansion
    // ends: a mere extension of that expansion cannot bound every suffix.
    if let Some(next) = (ch as u32).checked_add(1).and_then(char::from_u32) {
        let mut old_buf = [0; 4];
        let mut new_buf = [0; 4];
        let mut old = weights(ch.encode_utf8(&mut old_buf));
        let mut candidate = weights(next.encode_utf8(&mut new_buf));
        loop {
            match (old.next(), candidate.next()) {
                (Some(a), Some(b)) if b > a => return Some(next),
                (Some(a), Some(b)) if a == b => {}
                _ => break,
            }
        }
    }
    static SINGLE_PRIMARY: std::sync::OnceLock<Vec<(u16, char)>> = std::sync::OnceLock::new();
    let candidates = SINGLE_PRIMARY.get_or_init(|| {
        let mut entries = Vec::new();
        for index in 0..COUNT {
            let position = index * 10;
            let count = u16::from_le_bytes(DATA[position + 8..position + 10].try_into().unwrap());
            if count != 1 {
                continue;
            }
            let offset = COUNT * 10 + u32_at(position + 4) as usize * 2;
            let weight = u16::from_le_bytes([DATA[offset], DATA[offset + 1]]);
            if let Some(ch) = char::from_u32(u32_at(position)) {
                entries.push((weight, ch));
            }
        }
        entries.sort_unstable();
        entries.dedup_by_key(|entry| entry.0);
        entries
    });
    let mut first = None;
    scalar(ch, &mut |weight| {
        if first.is_none() {
            first = Some(weight);
        }
    });
    let first = first?;
    candidates
        .get(candidates.partition_point(|entry| entry.0 <= first))
        .map(|entry| entry.1)
}

pub(super) fn compare(left: &str, right: &str) -> std::cmp::Ordering {
    let mut common = super::common_collation_input_prefix(left.as_bytes(), right.as_bytes());
    while !left.is_char_boundary(common) {
        common -= 1;
    }
    let mut l = common;
    let mut r = common;
    while let (Some(&a), Some(&b)) = (left.as_bytes().get(l), right.as_bytes().get(r)) {
        if a | b >= 128 {
            break;
        }
        let a = ASCII_PRIMARY[a as usize];
        let b = ASCII_PRIMARY[b as usize];
        if a == 0 {
            l += 1;
            continue;
        }
        if b == 0 {
            r += 1;
            continue;
        }
        if a != b {
            return a.cmp(&b);
        }
        l += 1;
        r += 1;
    }
    if l == left.len() && r == right.len() {
        return std::cmp::Ordering::Equal;
    }
    compare_weight_tails(&left[l..], &right[r..])
}

#[inline(never)]
fn compare_weight_tails(left: &str, right: &str) -> std::cmp::Ordering {
    weights(left).cmp(weights(right))
}

const fn ascii_key(byte: usize) -> u8 {
    if ASCII[byte].is_empty() {
        return 0;
    }
    let weight = ASCII[byte][0];
    let mut rank = 0;
    let mut index = 0;
    while index < 128 {
        if !(index >= 65 && index <= 90) && !ASCII[index].is_empty() && ASCII[index][0] < weight {
            rank += 1;
        }
        index += 1;
    }
    2 * rank + 1
}
const ASCII_KEYS: [u8; 128] = {
    let mut keys = [0; 128];
    let mut index = 0;
    while index < 128 {
        keys[index] = ascii_key(index);
        index += 1;
    }
    keys
};

/// Prefix-free, order-preserving primary codes. ASCII primaries occupy one
/// byte, preserving ordinary index/hash-key width. Gaps use a separate prefix;
/// the common post-Latin range uses two bytes, and high weights use three.
fn key_table() -> &'static [[u8; 4]] {
    static TABLE: std::sync::OnceLock<Vec<[u8; 4]>> = std::sync::OnceLock::new();
    TABLE.get_or_init(|| {
        let mut ascii: Vec<u16> = ASCII
            .iter()
            .filter_map(|weights| weights.first().copied())
            .collect();
        ascii.sort_unstable();
        ascii.dedup();
        let max_ascii = *ascii.last().unwrap();
        (0..=u16::MAX)
            .map(|weight| {
                let rank = ascii.partition_point(|value| *value < weight);
                if ascii.get(rank) == Some(&weight) {
                    return [2 * rank as u8 + 1, 0, 0, 1];
                }
                if weight > max_ascii {
                    let delta = usize::from(weight - max_ascii - 1);
                    let head = 2 * ascii.len() + delta / 256;
                    if head < 255 {
                        return [head as u8, delta as u8, 0, 2];
                    }
                    let [hi, lo] = weight.to_be_bytes();
                    return [255, hi, lo, 3];
                }
                let [hi, lo] = weight.to_be_bytes();
                [2 * rank as u8, hi, lo, 3]
            })
            .collect()
    })
}

pub(super) fn key_unit(weight: u16) -> ([u8; 3], usize) {
    let code = key_table()[weight as usize];
    ([code[0], code[1], code[2]], code[3] as usize)
}

pub(super) fn key_bytes(value: &str) -> impl Iterator<Item = u8> + '_ {
    let table = key_table();
    weights(value).flat_map(move |weight| {
        let code = table[weight as usize];
        [code[0], code[1], code[2]]
            .into_iter()
            .take(code[3] as usize)
    })
}

pub(super) fn key_into(value: &str, out: &mut Vec<u8>) {
    if value.is_ascii() {
        let start = out.len();
        out.extend_from_slice(value.as_bytes());
        let mut ignored = false;
        // Transform a bounded slice in place: ordinary ASCII keys need no
        // per-byte Vec growth checks or length updates.
        for byte in &mut out[start..] {
            *byte = ASCII_KEYS[usize::from(*byte & 0x7f)];
            ignored |= *byte == 0;
        }
        if ignored {
            let mut write = start;
            for read in start..out.len() {
                if out[read] != 0 {
                    out[write] = out[read];
                    write += 1;
                }
            }
            out.truncate(write);
        }
        return;
    }
    out.extend(key_bytes(value));
}

pub(super) fn key_prefix_into(value: &str, max_bytes: usize, out: &mut Vec<u8>) {
    if value.is_ascii() {
        if value.len() <= max_bytes {
            key_into(value, out);
            return;
        }
        out.reserve(value.len().min(max_bytes));
        out.extend(
            value
                .bytes()
                .filter_map(|byte| {
                    let key = ASCII_KEYS[byte as usize];
                    (key != 0).then_some(key)
                })
                .take(max_bytes),
        );
    } else {
        out.extend(key_bytes(value).take(max_bytes));
    }
}

pub(super) fn ascii_key_size(value: &str) -> usize {
    value
        .bytes()
        .filter(|byte| ASCII_KEYS[*byte as usize] != 0)
        .count()
}

#[cfg(test)]
mod compact_key_tests {
    use super::*;
    #[test]
    fn primary_weight_expansion_bound_covers_the_complete_packed_table() {
        let bound = crate::charset::UCA9_PRIMARY_MAX_EXPANSION;
        for index in 0..COUNT {
            let position = index * 10;
            let ch = char::from_u32(u32_at(position)).unwrap();
            let weights = u16::from_le_bytes([DATA[position + 8], DATA[position + 9]]);
            assert!(usize::from(weights) * 2 <= ch.len_utf8() * bound, "{ch:?}");
            let mut utf8 = [0; 4];
            assert!(
                usize::from(weights) * 2
                    <= crate::charset::uca9_primary_byte_bound(
                        ch.encode_utf8(&mut utf8).as_bytes()
                    )
                    .unwrap(),
                "{ch:?}"
            );
        }
        for ch in (0..128).chain(0xac00..=0xd7a3).filter_map(char::from_u32) {
            let mut count = 0;
            scalar(ch, &mut |_| count += 2);
            assert!(count <= ch.len_utf8() * bound, "{ch:?}");
        }
        // Implicit weights emit at most two u16 units per scalar.
        assert!(bound >= 4);
    }
    #[test]
    fn audit1022_implicit_cjk_prefixes_keep_a_selective_upper_bound() {
        for ch in ['汉', '字', '漢', '語', '𠀀'] {
            assert_eq!(scalar_successor(ch), char::from_u32(ch as u32 + 1));
        }
    }

    #[test]
    fn audit1022_compact_primary_codes_are_ordered_and_prefix_free() {
        let table = key_table();
        for pair in table.windows(2) {
            let a = &pair[0][..pair[0][3] as usize];
            let b = &pair[1][..pair[1][3] as usize];
            assert!(a < b);
            assert!(!a.starts_with(b) && !b.starts_with(a));
        }
        for byte in 0..128 {
            if let Some(&weight) = ASCII[byte].first() {
                let code = table[weight as usize];
                assert_eq!(&code[..code[3] as usize], &[ASCII_KEYS[byte]]);
            } else {
                assert_eq!(ASCII_KEYS[byte], 0);
            }
        }
    }
}
