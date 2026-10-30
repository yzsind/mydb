//! Reusable collation search buffers. Scalar weights are cached only within
//! one collation-bound batch; original character boundaries remain explicit.
#[derive(Clone)]
struct Key {
    ch: char,
    len: usize,
    bytes: [u8; 128],
}
impl Default for Key {
    fn default() -> Self {
        Self {
            ch: '\0',
            len: usize::MAX,
            bytes: [0; 128],
        }
    }
}
pub(crate) struct SearchBuffer {
    collation: u16,
    needle: Vec<u8>,
    ascii: [Key; 128],
    unicode: [Key; 64],
    character: Vec<u8>,
    weights: Vec<u8>,
    boundaries: Vec<usize>,
}
impl SearchBuffer {
    pub(crate) fn new(collation: u16, needle: &str) -> Self {
        let mut key = Vec::new();
        crate::charset::collation_search_key_by_id_into(collation, needle, &mut key);
        Self {
            collation,
            needle: key,
            ascii: std::array::from_fn(|_| Key::default()),
            unicode: std::array::from_fn(|_| Key::default()),
            character: Vec::new(),
            weights: Vec::new(),
            boundaries: Vec::new(),
        }
    }
    pub(crate) fn find(&mut self, haystack: &str, needle: &str, start: i64) -> usize {
        if start <= 0 {
            return 0;
        }
        let count = haystack.chars().count();
        let first = start as usize - 1;
        if needle.is_empty() {
            return if first <= count { first + 1 } else { 0 };
        }
        if first >= count {
            return 0;
        }
        if self.collation == crate::charset::default_collation_info().id
            && haystack.is_ascii()
            && needle.is_ascii()
        {
            return super::find_position_default(haystack, needle, start);
        }
        self.weights.clear();
        self.boundaries.clear();
        self.boundaries.push(0);
        for ch in haystack.chars() {
            let entry = if ch.is_ascii() {
                &mut self.ascii[ch as usize]
            } else {
                &mut self.unicode[ch as usize % 64]
            };
            if entry.ch == ch && entry.len != usize::MAX {
                // Fixed small widths let the compiler copy weights directly.
                match entry.len {
                    0 => {}
                    1 => self.weights.push(entry.bytes[0]),
                    2 => self.weights.extend_from_slice(&entry.bytes[..2]),
                    4 => self.weights.extend_from_slice(&entry.bytes[..4]),
                    8 => self.weights.extend_from_slice(&entry.bytes[..8]),
                    _ => self.weights.extend_from_slice(&entry.bytes[..entry.len]),
                }
            } else {
                let mut utf8 = [0; 4];
                crate::charset::collation_search_key_by_id_into(
                    self.collation,
                    ch.encode_utf8(&mut utf8),
                    &mut self.character,
                );
                if self.character.len() <= entry.bytes.len() {
                    entry.ch = ch;
                    entry.len = self.character.len();
                    entry.bytes[..entry.len].copy_from_slice(&self.character);
                } else {
                    entry.len = usize::MAX;
                }
                self.weights.extend_from_slice(&self.character);
            }
            self.boundaries.push(self.weights.len());
            // A complete prefix match is globally earliest: every earlier
            // start has an earlier weight offset and cannot require a later
            // end than this match. Ignorable and expanded weights retain the
            // original boundary test. A miss continues the same linear build.
            if self.boundaries.len() == 17 {
                let found = super::match_collation_search_keys(
                    &self.weights,
                    &self.boundaries,
                    &self.needle,
                    first,
                );
                if found != 0 {
                    return found;
                }
            }
        }
        super::match_collation_search_keys(&self.weights, &self.boundaries, &self.needle, first)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prefix_search_agrees_with_full_scalar_weights_on_boundaries_and_long_misses() {
        let texts = [
            "a".repeat(15) + "ß" + &"a".repeat(100),
            "\u{301}".repeat(40) + "aßaeⅧviii",
            "x".repeat(14) + "ßss🙂ss",
            "a".repeat(16) + "🙂" + &"a".repeat(100),
            "a".repeat(10000) + "🙂",
            "\0".repeat(40) + "abc",
        ];
        for name in [
            "utf8mb4_0900_ai_ci",
            "utf8mb4_unicode_ci",
            "utf8mb4_general_ci",
            "utf8mb4_bin",
            "utf8mb4_german2_ci",
        ] {
            let cid = crate::charset::resolve_collation(name).unwrap().id;
            for needle in [
                "".into(),
                "a".into(),
                "ss".into(),
                "s".into(),
                "ae".into(),
                "viii".into(),
                "🙂".into(),
                "a".repeat(32),
                "a".repeat(500) + "b",
                "\u{301}".into(),
            ] {
                let mut plan = SearchBuffer::new(cid, &needle);
                for text in &texts {
                    for start in [0, 1, 2, 15, 16, 17, 31, 41, 10001, i64::MAX] {
                        assert_eq!(
                            plan.find(text, &needle, start),
                            super::super::find_position_collated(text, &needle, start, cid),
                            "{name}/{needle:?}/{start}"
                        );
                    }
                }
            }
        }
    }
}
