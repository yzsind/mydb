//! Byte-preserving SQL regular expressions. Mixed text/binary inputs are errors.
use super::*;
use std::borrow::Cow;

pub(super) fn binary_operands(values: &[Value], count: usize) -> Result<bool, QueryError> {
    let binaries = values[..count]
        .iter()
        .filter(|v| matches!(v, Value::Bytes(_)))
        .count();
    if binaries != 0 && binaries != count {
        return Err(crate::error::catalog::ER_CHARACTER_SET_MISMATCH.error(
            "Character set 'binary' cannot be used with text operands in regular expressions",
        ));
    }
    Ok(binaries == count)
}

/// MySQL maps binary bytes through its latin1 (CP1252) character map. This is
/// lossless even for malformed UTF-8, while Unicode classes and explicit
/// case folding retain their normal meaning. Offsets map back to raw bytes.
struct Subject<'a> {
    text: Cow<'a, str>,
    offsets: Vec<usize>,
}
impl<'a> Subject<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        if bytes.is_ascii() {
            return Self {
                text: Cow::Borrowed(std::str::from_utf8(bytes).unwrap()),
                offsets: Vec::new(),
            };
        }
        let text = encoding_rs::WINDOWS_1252
            .decode_without_bom_handling(bytes)
            .0;
        let mut offsets = Vec::with_capacity(bytes.len() + 1);
        offsets.extend(text.char_indices().map(|(offset, _)| offset));
        offsets.push(text.len());
        Self { text, offsets }
    }
    fn decoded_offset(&self, byte: usize) -> usize {
        if self.offsets.is_empty() {
            byte
        } else {
            self.offsets[byte]
        }
    }
    fn byte_offset(&self, decoded: usize) -> usize {
        if self.offsets.is_empty() {
            decoded
        } else {
            self.offsets
                .binary_search(&decoded)
                .expect("regex matches scalar boundaries")
        }
    }
}
pub(super) fn evaluate(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    let Value::Bytes(subject) = &values[0] else {
        unreachable!()
    };
    let Value::Bytes(pattern) = &values[1] else {
        unreachable!()
    };
    if pattern.len() > 8192 {
        return Err(crate::error::catalog::ER_REGEXP_INTERNAL_ERROR
            .error("regular expression exceeded the implementation resource limit"));
    }
    let _scratch = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        (if subject.is_ascii() {
            0
        } else {
            subject
                .len()
                .saturating_add(1)
                .saturating_mul(3 + std::mem::size_of::<usize>())
        })
        .saturating_add(if pattern.is_ascii() {
            0
        } else {
            pattern.len().saturating_mul(3)
        }),
    )?;
    let input = Subject::new(subject);
    let pattern: Cow<str> = if pattern.is_ascii() {
        Cow::Borrowed(std::str::from_utf8(pattern).unwrap())
    } else {
        encoding_rs::WINDOWS_1252
            .decode_without_bom_handling(pattern)
            .0
    };
    let flags = values.get(match name {
        "REGEXP_LIKE" => 2,
        "REGEXP_INSTR" => 5,
        "REGEXP_SUBSTR" => 4,
        _ => 5,
    });
    let re = mysql_regex_for_operand(&pattern, flags, true)?;
    let _search = re.search_scope(&input.text)?;
    if name == "REGEXP_LIKE" {
        return Ok(Value::Int(re.is_match(&input.text) as i64));
    }
    let pos = values
        .get(if name == "REGEXP_REPLACE" { 3 } else { 2 })
        .map(int_value)
        .transpose()?
        .unwrap_or(1);
    let max = subject.len() + usize::from(name != "REGEXP_INSTR" || subject.is_empty());
    if pos < 1 || pos as u64 > max as u64 {
        return Err(crate::error::catalog::ER_REGEXP_INDEX_OUTOFBOUNDS_ERROR
            .error("Index out of bounds in regular expression search."));
    }
    let start = input.decoded_offset(pos as usize - 1);
    let occurrence = values
        .get(if name == "REGEXP_REPLACE" { 4 } else { 3 })
        .map(int_value)
        .transpose()?
        .unwrap_or(if name == "REGEXP_REPLACE" { 0 } else { 1 });
    if name == "REGEXP_INSTR" {
        let return_end = values.get(4).map(int_value).transpose()?.unwrap_or(0);
        if !matches!(return_end, 0 | 1) {
            return Err(crate::error::catalog::ER_WRONG_ARGUMENTS.error("Incorrect return_option"));
        }
        let found = re
            .find_iter(&input.text[start..])
            .nth(occurrence.max(1) as usize - 1);
        return Ok(Value::Int(
            found
                .map(|m| {
                    input.byte_offset(start + if return_end == 1 { m.end() } else { m.start() }) + 1
                })
                .unwrap_or(0) as i64,
        ));
    }
    if name == "REGEXP_SUBSTR" {
        return Ok(re
            .find_iter(&input.text)
            .filter(|m| m.start() >= start)
            .nth(occurrence.max(1) as usize - 1)
            .map(|m| {
                Value::Bytes(
                    subject[input.byte_offset(m.start())..input.byte_offset(m.end())].to_vec(),
                )
            })
            .unwrap_or(Value::Null));
    }
    if subject.is_empty() {
        return Ok(Value::Bytes(Vec::new()));
    }
    let Value::Bytes(replacement) = &values[2] else {
        unreachable!()
    };
    let mut out = Vec::new();
    let mut search = start;
    let mut copied = 0;
    let mut seen = 0;
    while let Some(caps) = re.captures_at(&input.text, search) {
        let m = caps.get(0).unwrap();
        seen += 1;
        if occurrence == 0 || seen == occurrence.max(1) {
            if !regexp_append(&mut out, 0, &subject[copied..input.byte_offset(m.start())])
                || !expand(&caps, &re, replacement, &mut out, &input, subject)?
            {
                return Ok(Value::Null);
            }
            copied = input.byte_offset(m.end());
        }
        if m.is_empty() {
            if m.end() == input.text.len() {
                break;
            }
            search = m.end() + input.text[m.end()..].chars().next().unwrap().len_utf8();
        } else {
            search = m.end();
        }
        if occurrence != 0 && seen == occurrence.max(1) {
            break;
        }
    }
    if !regexp_append(&mut out, 0, &subject[copied..]) {
        return Ok(Value::Null);
    }
    Ok(Value::Bytes(out))
}

fn expand(
    caps: &crate::engine::mysql_regex::Captures<'_>,
    re: &crate::engine::mysql_regex::Regex,
    replacement: &[u8],
    out: &mut Vec<u8>,
    input: &Subject<'_>,
    subject: &[u8],
) -> Result<bool, QueryError> {
    let mut pos = 0;
    while pos < replacement.len() {
        let b = replacement[pos];
        pos += 1;
        if b == b'\\' {
            if let Some(&next) = replacement.get(pos) {
                pos += 1;
                let byte = if matches!(next, b'u' | b'U') {
                    let digits = if next == b'u' { 4 } else { 8 };
                    let mut code = unicode_escape(replacement, &mut pos, digits)?;
                    if (0xd800..=0xdbff).contains(&code) {
                        if replacement.get(pos..pos + 2) != Some(b"\\u") {
                            return Err(bad_escape());
                        }
                        pos += 2;
                        let low = unicode_escape(replacement, &mut pos, 4)?;
                        if !(0xdc00..=0xdfff).contains(&low) {
                            return Err(bad_escape());
                        }
                        code = 0x10000 + ((code - 0xd800) << 10) + low - 0xdc00;
                    }
                    if char::from_u32(code).is_none() {
                        return Err(bad_escape());
                    }
                    encode_codepoint(code)
                } else {
                    next
                };
                if !regexp_append(out, 0, &[byte]) {
                    return Ok(false);
                }
            }
        } else if b == b'$' {
            if replacement.get(pos) == Some(&b'{') {
                pos += 1;
                let start = pos;
                while replacement.get(pos).is_some_and(|b| *b != b'}') {
                    pos += 1;
                }
                let name = std::str::from_utf8(&replacement[start..pos]).ok();
                if replacement.get(pos) != Some(&b'}')
                    || !name.is_some_and(|name| re.capture_names().any(|n| n == Some(name)))
                {
                    return Err(crate::error::catalog::ER_REGEXP_INVALID_CAPTURE_GROUP_NAME
                        .error("Invalid capture group name"));
                }
                pos += 1;
                if let Some(matched) = caps.name(name.unwrap()) {
                    if !regexp_append(
                        out,
                        0,
                        &subject
                            [input.byte_offset(matched.start())..input.byte_offset(matched.end())],
                    ) {
                        return Ok(false);
                    }
                }
                continue;
            }

            let first = replacement
                .get(pos)
                .filter(|b| b.is_ascii_digit())
                .ok_or_else(|| {
                    crate::error::catalog::ER_REGEXP_INVALID_CAPTURE_GROUP_NAME
                        .error("Invalid capture group")
                })?;
            let mut group = (first - b'0') as usize;
            pos += 1;
            if group >= re.captures_len() {
                return Err(crate::error::catalog::ER_REGEXP_INDEX_OUTOFBOUNDS_ERROR
                    .error("Invalid capture group"));
            }
            while let Some(digit) = replacement.get(pos).filter(|b| b.is_ascii_digit()) {
                let Some(next) = group
                    .checked_mul(10)
                    .and_then(|g| g.checked_add((digit - b'0') as usize))
                else {
                    break;
                };
                if next >= re.captures_len() {
                    break;
                }
                group = next;
                pos += 1;
            }
            if let Some(m) = caps.get(group) {
                if !regexp_append(
                    out,
                    0,
                    &subject[input.byte_offset(m.start())..input.byte_offset(m.end())],
                ) {
                    return Ok(false);
                }
            }
        } else if !regexp_append(out, 0, &[b]) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn bad_escape() -> QueryError {
    crate::error::catalog::ER_REGEXP_BAD_ESCAPE_SEQUENCE
        .error("Invalid Unicode escape in replacement")
}
fn unicode_escape(bytes: &[u8], pos: &mut usize, count: usize) -> Result<u32, QueryError> {
    let mut code = 0u32;
    for _ in 0..count {
        let digit = bytes
            .get(*pos)
            .and_then(|b| (*b as char).to_digit(16))
            .ok_or_else(bad_escape)?;
        *pos += 1;
        code = code
            .checked_mul(16)
            .and_then(|n| n.checked_add(digit))
            .ok_or_else(bad_escape)?;
    }
    Ok(code)
}

fn encode_codepoint(code: u32) -> u8 {
    let mut buf = [0u8; 4];
    let text = char::from_u32(code)
        .expect("validated Unicode escape")
        .encode_utf8(&mut buf);
    let (bytes, _, had_errors) = encoding_rs::WINDOWS_1252.encode(text);
    if had_errors {
        b'?'
    } else {
        bytes[0]
    }
}
