//! Streaming, character-at-a-time LIKE matching under a fixed collation.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::charset::{like_chars_equal_for_info, CollationInfo, PreparedLikeChar};

const CACHE_SLOTS: usize = 256;

#[derive(Debug, Clone)]
enum Token {
    Many,
    One,
    Literal {
        literal: PreparedLikeChar,
        ascii: [u64; 2],
    },
}

/// Cache complete character pairs, rather than folding the whole string or
/// assuming collation expansions are interchangeable with several characters.
/// All key bits and the result occupy one atomic word; collisions only evict.
#[derive(Debug)]
struct CharPairs {
    slots: Box<[AtomicU64]>,
}

impl CharPairs {
    fn new() -> Self {
        Self {
            slots: (0..CACHE_SLOTS).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    #[inline]
    fn matches(&self, info: &CollationInfo, value: char, literal: &PreparedLikeChar) -> bool {
        let pattern = literal.character();
        let key = ((u64::from(value as u32) + 1) << 22) | (u64::from(pattern as u32) << 1);
        let slot = &self.slots
            [((value as usize).wrapping_mul(0x9e37) ^ pattern as usize) & (CACHE_SLOTS - 1)];
        let cached = slot.load(Ordering::Relaxed);
        if cached & !1 == key {
            return cached & 1 != 0;
        }
        let equal = literal.matches(info, value);
        slot.store(key | u64::from(equal), Ordering::Relaxed);
        equal
    }
}

impl Clone for CharPairs {
    fn clone(&self) -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct UnicodeLikePlan {
    tokens: Box<[Token]>,
    pairs: CharPairs,
}

impl UnicodeLikePlan {
    pub(super) fn new(pattern: &str, escape: Option<char>, info: &CollationInfo) -> Self {
        let mut literals = HashMap::<char, [u64; 2]>::new();
        let mut tokens = Vec::new();
        let mut chars = pattern.chars();
        while let Some(ch) = chars.next() {
            let literal = if Some(ch) == escape {
                // A trailing escape is itself a literal, as in the scalar oracle.
                chars.next().unwrap_or(ch)
            } else if ch == '%' {
                if !matches!(tokens.last(), Some(Token::Many)) {
                    tokens.push(Token::Many);
                }
                continue;
            } else if ch == '_' {
                tokens.push(Token::One);
                continue;
            } else {
                ch
            };
            let ascii = *literals.entry(literal).or_insert_with(|| {
                let mut bits = [0; 2];
                for byte in 0..128u8 {
                    if like_chars_equal_for_info(info, byte as char, literal) {
                        bits[byte as usize / 64] |= 1u64 << (byte % 64);
                    }
                }
                bits
            });
            tokens.push(Token::Literal {
                literal: PreparedLikeChar::new(info, literal),
                ascii,
            });
        }
        Self {
            tokens: tokens.into_boxed_slice(),
            pairs: CharPairs::new(),
        }
    }

    pub(super) fn heap_capacity_bytes(&self) -> usize {
        self.tokens.len() * std::mem::size_of::<Token>()
            + self.pairs.slots.len() * std::mem::size_of::<AtomicU64>()
            + self
                .tokens
                .iter()
                .map(|token| match token {
                    Token::Literal { literal, .. } => literal.heap_capacity_bytes(),
                    _ => 0,
                })
                .sum::<usize>()
    }

    pub(super) fn matches(&self, value: &str, info: &CollationInfo) -> bool {
        let mut offset = 0;
        let mut token = 0;
        let mut star: Option<(usize, usize)> = None;
        while offset < value.len() {
            let current = self.tokens.get(token);
            if matches!(current, Some(Token::Many)) {
                token += 1;
                star = Some((token, offset));
                // A trailing percent accepts the entire remaining suffix.
                if token == self.tokens.len() {
                    return true;
                }
                continue;
            }
            if let Some(Token::Literal { ascii: [0, 0], .. }) = current {
                if star == Some((token, offset)) && value.as_bytes()[offset].is_ascii() {
                    // The precomputed collation table proves that no ASCII
                    // character can begin this literal. Skip only a run that
                    // the immediately preceding wildcard can consume freely.
                    let bytes = value.as_bytes();
                    while offset + 16 <= bytes.len() && bytes[offset..offset + 16].is_ascii() {
                        offset += 16;
                    }
                    while offset < bytes.len() && bytes[offset].is_ascii() {
                        offset += 1;
                    }
                    star = Some((token, offset));
                    if offset == bytes.len() {
                        break;
                    }
                }
            }
            let ch = value[offset..]
                .chars()
                .next()
                .expect("nonempty UTF-8 suffix");
            let matched = match current {
                Some(Token::One) => true,
                Some(Token::Literal { literal, ascii }) => {
                    ch == literal.character()
                        || if ch.is_ascii() {
                            ascii[ch as usize / 64] & (1u64 << (ch as u32 % 64)) != 0
                        } else {
                            self.pairs.matches(info, ch, literal)
                        }
                }
                _ => false,
            };
            if matched {
                offset += ch.len_utf8();
                token += 1;
                continue;
            }
            let Some((retry_token, retry_offset)) = star else {
                return false;
            };
            let next = retry_offset
                + if retry_offset == offset {
                    ch.len_utf8()
                } else {
                    value[retry_offset..].chars().next().unwrap().len_utf8()
                };
            star = Some((retry_token, next));
            offset = next;
            token = retry_token;
        }
        while matches!(self.tokens.get(token), Some(Token::Many)) {
            token += 1;
        }
        token == self.tokens.len()
    }
}
