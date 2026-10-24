//! Conservative single-quoted literals for bulk INSERT. SQL mode is passed
//! explicitly because tuple decoding can run on scoped worker threads.

pub(super) fn end(bytes: &[u8], mut pos: usize, backslash: bool) -> Option<usize> {
    loop {
        pos += if backslash {
            memchr::memchr2(b'\'', b'\\', bytes.get(pos..)?)?
        } else {
            memchr::memchr(b'\'', bytes.get(pos..)?)?
        };
        if bytes[pos] == b'\\' {
            // Unknown/dialect-specific escapes stay on the canonical parser.
            if !matches!(
                *bytes.get(pos + 1)?,
                b'0' | b'b' | b'n' | b'r' | b't' | b'Z' | b'\\' | b'\'' | b'"' | b'%' | b'_'
            ) {
                return None;
            }
            pos += 2;
        } else if bytes.get(pos + 1) == Some(&b'\'') {
            pos += 2;
        } else {
            return Some(pos);
        }
    }
}

pub(super) fn decode(source: &str, backslash: bool) -> Option<String> {
    let body = source.as_bytes();
    let mut out = Vec::with_capacity(body.len());
    let mut pos = 0;
    loop {
        let next = if backslash {
            memchr::memchr2(b'\'', b'\\', &body[pos..])
        } else {
            memchr::memchr(b'\'', &body[pos..])
        };
        let Some(next) = next else {
            out.extend_from_slice(&body[pos..]);
            // Input is valid UTF-8. Chunk boundaries and every replacement are
            // ASCII quotes/backslashes, so decoding cannot split a code point
            // or introduce a non-UTF-8 byte sequence.
            return Some(unsafe { String::from_utf8_unchecked(out) });
        };
        let next = pos + next;
        out.extend_from_slice(&body[pos..next]);
        if body[next] == b'\'' {
            if body.get(next + 1) != Some(&b'\'') {
                return None;
            }
            out.push(b'\'');
        } else {
            let escaped = *body.get(next + 1)?;
            let decoded = match escaped {
                b'0' => 0,
                b'b' => 8,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'Z' => 26,
                b'\\' | b'\'' | b'"' => escaped,
                b'%' | b'_' => {
                    out.push(b'\\');
                    escaped
                }
                _ => return None,
            };
            out.push(decoded);
        }
        pos = next + 2;
    }
}
