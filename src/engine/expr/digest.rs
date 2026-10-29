//! Shared scalar and column digest kernels. Input octets are always borrowed.

use super::*;
use sha2::Digest;

#[derive(Clone, Copy)]
pub(crate) enum HashAlgorithm {
    Md5,
    Sha1,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
}

impl HashAlgorithm {
    pub(crate) fn from_sha2_length(length: i64) -> Option<Self> {
        Some(match length {
            0 | 256 => Self::Sha256,
            224 => Self::Sha224,
            384 => Self::Sha384,
            512 => Self::Sha512,
            _ => return None,
        })
    }
    pub(crate) fn hex_len(self) -> usize {
        match self {
            Self::Md5 => 32,
            Self::Sha1 => 40,
            Self::Sha224 => 56,
            Self::Sha256 => 64,
            Self::Sha384 => 96,
            Self::Sha512 => 128,
        }
    }
    pub(crate) fn append_hex(self, input: &[u8], output: &mut Vec<u8>) -> Result<(), QueryError> {
        fn append<D: Digest + Default>(
            input: &[u8],
            output: &mut Vec<u8>,
        ) -> Result<(), QueryError> {
            let hash = if input.len() <= 64 * 1024 {
                D::digest(input)
            } else {
                let mut state = D::new();
                for chunk in input.chunks(64 * 1024) {
                    crate::engine::cancel::check_cancelled()?;
                    state.update(chunk);
                }
                state.finalize()
            };
            const DIGITS: &[u8; 16] = b"0123456789abcdef";
            for byte in hash {
                output.push(DIGITS[(byte >> 4) as usize]);
                output.push(DIGITS[(byte & 15) as usize]);
            }
            Ok(())
        }
        match self {
            Self::Md5 => append::<md5::Md5>(input, output),
            Self::Sha1 => append::<sha1::Sha1>(input, output),
            Self::Sha224 => append::<sha2::Sha224>(input, output),
            Self::Sha256 => append::<sha2::Sha256>(input, output),
            Self::Sha384 => append::<sha2::Sha384>(input, output),
            Self::Sha512 => append::<sha2::Sha512>(input, output),
        }
    }
}

pub(crate) fn hash_value(algorithm: HashAlgorithm, value: &Value) -> Result<Value, QueryError> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let mut output = Vec::with_capacity(algorithm.hex_len());
    algorithm.append_hex(value_bytes_ref(value).as_ref(), &mut output)?;
    Ok(Value::String(
        String::from_utf8(output).expect("digest output is ASCII"),
    ))
}

pub(crate) fn sha2_value(value: &Value, length: &Value) -> Result<Value, QueryError> {
    let algorithm = if matches!(length, Value::Null) {
        None
    } else {
        HashAlgorithm::from_sha2_length(int_value(length)?)
    };
    let Some(algorithm) = algorithm else {
        warn_invalid_sha2_length();
        return Ok(Value::Null);
    };
    hash_value(algorithm, value)
}
