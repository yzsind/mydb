//! Borrowed network/UUID semantics shared by scalar and column execution.
use super::*;
use std::net::Ipv6Addr;

#[derive(Debug, Clone, Copy)]
pub(crate) enum NetworkFunction {
    UuidToBin,
    BinToUuid,
    IsUuid,
    InetAton,
    InetNtoa,
    Inet6Aton,
    Inet6Ntoa,
    IsIpv4,
    IsIpv6,
    IsIpv4Compat,
    IsIpv4Mapped,
}
impl NetworkFunction {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "UUID_TO_BIN" => Self::UuidToBin,
            "BIN_TO_UUID" => Self::BinToUuid,
            "IS_UUID" => Self::IsUuid,
            "INET_ATON" => Self::InetAton,
            "INET_NTOA" => Self::InetNtoa,
            "INET6_ATON" => Self::Inet6Aton,
            "INET6_NTOA" => Self::Inet6Ntoa,
            "IS_IPV4" => Self::IsIpv4,
            "IS_IPV6" => Self::IsIpv6,
            "IS_IPV4_COMPAT" => Self::IsIpv4Compat,
            "IS_IPV4_MAPPED" => Self::IsIpv4Mapped,
            _ => return None,
        })
    }
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::UuidToBin => "UUID_TO_BIN",
            Self::BinToUuid => "BIN_TO_UUID",
            Self::IsUuid => "IS_UUID",
            Self::InetAton => "INET_ATON",
            Self::InetNtoa => "INET_NTOA",
            Self::Inet6Aton => "INET6_ATON",
            Self::Inet6Ntoa => "INET6_NTOA",
            Self::IsIpv4 => "IS_IPV4",
            Self::IsIpv6 => "IS_IPV6",
            Self::IsIpv4Compat => "IS_IPV4_COMPAT",
            Self::IsIpv4Mapped => "IS_IPV4_MAPPED",
        }
    }
    pub(crate) fn check_arity<T>(self, args: &[T]) -> Result<(), QueryError> {
        if matches!(self, Self::UuidToBin | Self::BinToUuid) {
            expect_arg_range(self.name(), args, 1, 2)
        } else {
            expect_arg_count(self.name(), args, 1)
        }
    }
    pub(crate) fn is_predicate(self) -> bool {
        matches!(
            self,
            Self::IsUuid | Self::IsIpv4 | Self::IsIpv6 | Self::IsIpv4Compat | Self::IsIpv4Mapped
        )
    }
    pub(crate) fn evaluate(self, value: &Value, swap: Option<&Value>) -> Result<Value, QueryError> {
        if matches!(value, Value::Null) || swap.is_some_and(|v| matches!(v, Value::Null)) {
            return Ok(Value::Null);
        }
        match self {
            Self::UuidToBin => {
                let mut bytes = parse_uuid_bytes(&str_value(value)).ok_or_else(uuid_error)?;
                if swap.map(int_value).transpose()?.unwrap_or(0) != 0 {
                    swap_uuid_time_parts(&mut bytes);
                }
                Ok(Value::Bytes(bytes.to_vec()))
            }
            Self::BinToUuid => {
                let raw = value_bytes_ref(value);
                let mut bytes: [u8; 16] = raw.as_ref().try_into().map_err(|_| uuid_error())?;
                if swap.map(int_value).transpose()?.unwrap_or(0) != 0 {
                    unswap_uuid_time_parts(&mut bytes);
                }
                Ok(Value::String(crate::util::uuid::format(&bytes)))
            }
            Self::IsUuid => Ok(Value::Int(
                parse_uuid_bytes(&str_value(value)).is_some() as i64
            )),
            Self::InetAton => {
                let text = str_value(value);
                match parse_mysql_legacy_ipv4(&text) {
                    Some(packed) => Ok(Value::UInt(packed as u64)),
                    None => invalid_string_function_value("string", &text, self.name()),
                }
            }
            Self::InetNtoa => {
                let number = match value {
                    Value::UInt(n) if *n <= u32::MAX as u64 => *n as u32,
                    Value::Int(n) if (0..=u32::MAX as i64).contains(n) => *n as u32,
                    _ => {
                        return invalid_string_function_value(
                            "integer",
                            &str_value(value),
                            self.name(),
                        )
                    }
                };
                Ok(Value::String(format_ipv4(number.to_be_bytes())))
            }
            Self::Inet6Aton => {
                let text = str_value(value);
                if let Some(octets) = parse_mysql_strict_ipv4(&text) {
                    return Ok(Value::Bytes(octets.to_vec()));
                }
                match parse_ipv6(&text) {
                    Some(addr) => Ok(Value::Bytes(addr.octets().to_vec())),
                    None => invalid_string_function_value("IP", &text, self.name()),
                }
            }
            Self::Inet6Ntoa => {
                let Value::Bytes(bytes) = value else {
                    return invalid_string_function_value("string", &str_value(value), self.name());
                };
                match bytes.len() {
                    4 => Ok(Value::String(format_ipv4(
                        bytes.as_slice().try_into().expect("length checked"),
                    ))),
                    16 => Ok(Value::String(format_mysql_ipv6(
                        bytes.as_slice().try_into().expect("length checked"),
                    ))),
                    _ => invalid_string_function_value("string", "<binary>", self.name()),
                }
            }
            Self::IsIpv4 => Ok(Value::Int(
                parse_mysql_strict_ipv4(&str_value(value)).is_some() as i64,
            )),
            Self::IsIpv6 => Ok(Value::Int(parse_ipv6(&str_value(value)).is_some() as i64)),
            Self::IsIpv4Compat | Self::IsIpv4Mapped => {
                let result = match value {
                    Value::Bytes(b) if b.len() == 16 => self.binary_predicate(b),
                    _ => false,
                };
                Ok(Value::Int(result as i64))
            }
        }
    }
    pub(crate) fn binary_predicate(self, b: &[u8]) -> bool {
        b.len() == 16
            && match self {
                Self::IsIpv4Compat => {
                    b[..12] == [0; 12] && b[12..] != [0, 0, 0, 0] && b[12..] != [0, 0, 0, 1]
                }
                Self::IsIpv4Mapped => b[..10] == [0; 10] && b[10..12] == [0xff, 0xff],
                _ => false,
            }
    }
}
fn uuid_error() -> QueryError {
    error(
        crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
        "incorrect uuid value",
    )
}

pub(crate) fn parse_ipv6(text: &str) -> Option<Ipv6Addr> {
    // Avoid allocating for the usual pure-hex form and already canonical dotted tails.
    text.parse()
        .ok()
        .or_else(|| normalize_mysql_ipv6_dotted_tail(text)?.parse().ok())
}

pub(crate) fn parse_uuid_bytes(input: &str) -> Option<[u8; 16]> {
    let raw = input.as_bytes();
    let raw = match raw {
        [b'{', inner @ .., b'}'] if inner.len() == 36 => inner,
        raw if raw.len() == 32 || raw.len() == 36 => raw,
        _ => return None,
    };
    let canonical = raw.len() == 36;
    let mut result = [0; 16];
    let mut cursor = 0;
    for (index, byte) in result.iter_mut().enumerate() {
        if canonical && matches!(index, 4 | 6 | 8 | 10) {
            if raw[cursor] != b'-' {
                return None;
            }
            cursor += 1;
        }
        *byte = (crate::util::hex::digit(raw[cursor])? << 4)
            | crate::util::hex::digit(raw[cursor + 1])?;
        cursor += 2;
    }
    Some(result)
}

pub(crate) fn swap_uuid_time_parts(bytes: &mut [u8; 16]) {
    let original = *bytes;
    bytes[0..2].copy_from_slice(&original[6..8]);
    bytes[2..4].copy_from_slice(&original[4..6]);
    bytes[4..8].copy_from_slice(&original[0..4]);
}

pub(crate) fn unswap_uuid_time_parts(bytes: &mut [u8; 16]) {
    let original = *bytes;
    bytes[0..4].copy_from_slice(&original[4..8]);
    bytes[4..6].copy_from_slice(&original[2..4]);
    bytes[6..8].copy_from_slice(&original[0..2]);
}

pub(crate) fn parse_mysql_legacy_ipv4(text: &str) -> Option<u32> {
    let mut values = [0u32; 4];
    let mut count = 0;
    for part in text.split('.') {
        if count == 4 || part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let value = part.parse::<u16>().ok()?;
        if value > 255 {
            return None;
        }
        values[count] = value as u32;
        count += 1;
    }
    Some(match count {
        1 => values[0],
        2 => (values[0] << 24) | values[1],
        3 => (values[0] << 24) | (values[1] << 16) | values[2],
        4 => (values[0] << 24) | (values[1] << 16) | (values[2] << 8) | values[3],
        _ => return None,
    })
}

pub(crate) fn parse_mysql_strict_ipv4(text: &str) -> Option<[u8; 4]> {
    let mut parts = text.split('.');
    let mut octets = [0; 4];
    for octet in &mut octets {
        let part = parts.next()?;
        if part.is_empty() || part.len() > 3 || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        *octet = part.parse::<u16>().ok()?.try_into().ok()?;
    }
    parts.next().is_none().then_some(octets)
}

fn normalize_mysql_ipv6_dotted_tail(text: &str) -> Option<String> {
    let dot = text.find('.')?;
    let tail_start = text[..dot].rfind(':').map_or(0, |index| index + 1);
    let octets = parse_mysql_strict_ipv4(&text[tail_start..])?;
    let dotted = format!("{}.{}.{}.{}", octets[0], octets[1], octets[2], octets[3]);
    Some(format!("{}{dotted}", &text[..tail_start]))
}

fn format_ipv4(octets: [u8; 4]) -> String {
    let mut out = Vec::with_capacity(15);
    append_ipv4(octets, &mut out);
    String::from_utf8(out).expect("IP output is ASCII")
}

pub(crate) fn append_ipv4(octets: [u8; 4], out: &mut Vec<u8>) {
    let mut buffer = itoa::Buffer::new();
    for (index, octet) in octets.into_iter().enumerate() {
        if index != 0 {
            out.push(b'.');
        }
        out.extend_from_slice(buffer.format(octet).as_bytes());
    }
}

pub(crate) fn format_mysql_ipv6(octets: [u8; 16]) -> String {
    let mut out = Vec::with_capacity(39);
    append_ipv6(octets, &mut out);
    String::from_utf8(out).expect("IP output is ASCII")
}

pub(crate) fn append_ipv6(octets: [u8; 16], out: &mut Vec<u8>) {
    let begin = out.len();
    let mut words = [0u16; 8];
    for (index, pair) in octets.chunks_exact(2).enumerate() {
        words[index] = u16::from_be_bytes([pair[0], pair[1]]);
    }

    // MySQL renders IPv4-compatible and IPv4-mapped addresses with a dotted
    // tail, including the legacy all-zero prefix form.
    let ipv4_tail = u32::from_be_bytes([octets[12], octets[13], octets[14], octets[15]]);
    if words[..6].iter().all(|word| *word == 0) && ipv4_tail > 1 {
        out.extend_from_slice(b"::");
        append_ipv4(octets[12..].try_into().expect("four octets"), out);
        return;
    }
    if words[..5].iter().all(|word| *word == 0) && words[5] == 0xffff {
        out.extend_from_slice(b"::ffff:");
        append_ipv4(octets[12..].try_into().expect("four octets"), out);
        return;
    }

    // Unlike RFC 5952, MySQL compresses even a single zero hextet. Choose the
    // longest run, preferring the first on ties.
    let mut best_start = 0usize;
    let mut best_len = 0usize;
    let mut cursor = 0usize;
    while cursor < words.len() {
        if words[cursor] != 0 {
            cursor += 1;
            continue;
        }
        let start = cursor;
        while cursor < words.len() && words[cursor] == 0 {
            cursor += 1;
        }
        let len = cursor - start;
        if len > best_len {
            best_start = start;
            best_len = len;
        }
    }

    let mut index = 0usize;
    while index < words.len() {
        if best_len != 0 && index == best_start {
            out.extend_from_slice(b"::");
            index += best_len;
            continue;
        }
        if out.len() != begin && out.last() != Some(&b':') {
            out.push(b':');
        }
        let word = words[index];
        let mut started = false;
        for shift in [12, 8, 4, 0] {
            let digit = ((word >> shift) & 15) as usize;
            if digit != 0 || started || shift == 0 {
                out.push(b"0123456789abcdef"[digit]);
                started = true;
            }
        }
        index += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reference(octets: [u8; 16]) -> String {
        let mut words = [0u16; 8];
        for (index, pair) in octets.chunks_exact(2).enumerate() {
            words[index] = u16::from_be_bytes([pair[0], pair[1]]);
        }

        // MySQL renders IPv4-compatible and IPv4-mapped addresses with a dotted
        // tail, including the legacy all-zero prefix form.
        let ipv4_tail = u32::from_be_bytes([octets[12], octets[13], octets[14], octets[15]]);
        if words[..6].iter().all(|word| *word == 0) && ipv4_tail > 1 {
            return format!(
                "::{}.{}.{}.{}",
                octets[12], octets[13], octets[14], octets[15]
            );
        }
        if words[..5].iter().all(|word| *word == 0) && words[5] == 0xffff {
            return format!(
                "::ffff:{}.{}.{}.{}",
                octets[12], octets[13], octets[14], octets[15]
            );
        }

        // Unlike RFC 5952, MySQL compresses even a single zero hextet. Choose the
        // longest run, preferring the first on ties.
        let mut best_start = 0usize;
        let mut best_len = 0usize;
        let mut cursor = 0usize;
        while cursor < words.len() {
            if words[cursor] != 0 {
                cursor += 1;
                continue;
            }
            let start = cursor;
            while cursor < words.len() && words[cursor] == 0 {
                cursor += 1;
            }
            let len = cursor - start;
            if len > best_len {
                best_start = start;
                best_len = len;
            }
        }

        let mut out = String::new();
        let mut index = 0usize;
        while index < words.len() {
            if best_len != 0 && index == best_start {
                out.push_str("::");
                index += best_len;
                continue;
            }
            if !out.is_empty() && !out.ends_with(':') {
                out.push(':');
            }
            use std::fmt::Write as _;
            let _ = write!(out, "{:x}", words[index]);
            index += 1;
        }
        if out.ends_with(':') && !out.ends_with("::") {
            out.push(':');
        }
        out
    }

    #[test]
    fn contiguous_ip_writers_preserve_all_hextets_zero_runs_and_append_boundaries() {
        let check = |words: [u16; 8]| {
            let mut octets = [0; 16];
            for (pair, word) in octets.chunks_exact_mut(2).zip(words) {
                pair.copy_from_slice(&word.to_be_bytes());
            }
            let expected = reference(octets);
            let mut output = b"prefix".to_vec();
            append_ipv6(octets, &mut output);
            assert_eq!(&output[6..], expected.as_bytes(), "{words:?}");
        };
        for position in 0..8 {
            for word in 0..=u16::MAX {
                let mut words = [0x1234, 0xabcd, 0xffff, 0x80, 0x100, 0x1000, 0x1, 0x2222];
                words[position] = word;
                check(words);
            }
        }
        for mask in 0..256 {
            let words = std::array::from_fn(|index| {
                if mask & (1 << index) != 0 {
                    0
                } else {
                    index as u16 + 1
                }
            });
            check(words);
        }
        for last in [0, 1, 2, u16::MAX] {
            check([0, 0, 0, 0, 0, 0, 0, last]);
            check([0, 0, 0, 0, 0, 0xffff, 0xc000, last]);
        }
        for position in 0..4 {
            for byte in 0..=u8::MAX {
                let mut octets = [1, 2, 3, 4];
                octets[position] = byte;
                let mut output = b"prefix".to_vec();
                append_ipv4(octets, &mut output);
                assert_eq!(
                    &output[6..],
                    std::net::Ipv4Addr::from(octets).to_string().as_bytes()
                );
            }
        }
    }
}
