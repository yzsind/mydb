//! Reusable, governed zlib workspace; every input starts an independent stream.

use std::cell::RefCell;
use std::io::Write;

use flate2::write::ZlibEncoder;

use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::types::QueryError;

mod memo;

struct Workspace {
    encoder: ZlibEncoder<Vec<u8>>,
    decoder: flate2::Decompress,
    memo: Option<memo::Memo>,
    memo_attempted: bool,
    _charge: ControlledMemoryCharge,
}

thread_local! {
    // Includes the default backend's dictionary, Huffman state and stream
    // buffer. Output ownership is transferred to the caller, never retained.
    static WORKSPACE: RefCell<Option<Workspace>> = const { RefCell::new(None) };
}

fn io_error(error: std::io::Error) -> QueryError {
    crate::error::catalog::ER_GET_ERRNO.error(error.to_string())
}

fn write_input(encoder: &mut ZlibEncoder<Vec<u8>>, input: &[u8]) -> Result<(), QueryError> {
    for chunk in input.chunks(64 * 1024) {
        crate::engine::cancel::check_cancelled()?;
        encoder.write_all(chunk).map_err(io_error)?;
    }
    encoder.try_finish().map_err(io_error)
}

fn fresh(input: &[u8]) -> Result<Vec<u8>, QueryError> {
    let mut encoder = ZlibEncoder::new(
        (input.len() as u32).to_le_bytes().to_vec(),
        flate2::Compression::default(),
    );
    write_input(&mut encoder, input)?;
    encoder.finish().map_err(io_error)
}

pub(super) fn compress(input: &[u8]) -> Result<Vec<u8>, QueryError> {
    WORKSPACE.with(|cache| {
        let Ok(mut cache) = cache.try_borrow_mut() else {
            return fresh(input);
        };
        if let Err(error) = crate::engine::cancel::check_cancelled() {
            *cache = None;
            return Err(error);
        }
        if cache.is_none() {
            let Some(charge) = ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::DerivedCache,
                1024 * 1024,
            ) else {
                return fresh(input);
            };
            *cache = Some(Workspace {
                encoder: ZlibEncoder::new(Vec::new(), flate2::Compression::default()),
                decoder: flate2::Decompress::new(true),
                memo: None,
                memo_attempted: false,
                _charge: charge,
            });
        }
        let workspace = cache.as_mut().expect("initialized workspace");
        let mut admit = None;
        if memo::Memo::eligible(input) {
            if !workspace.memo_attempted {
                workspace.memo_attempted = true;
                workspace.memo = memo::Memo::try_new();
            }
            if let Some(memo) = &mut workspace.memo {
                let (cached, admission) = memo.lookup(input);
                if let Some(output) = cached {
                    return Ok(output);
                }
                admit = admission;
            }
        }
        let encoder = &mut workspace.encoder;
        let result = (|| {
            encoder
                .reset((input.len() as u32).to_le_bytes().to_vec())
                .map_err(io_error)?;
            write_input(encoder, input)?;
            Ok(std::mem::take(encoder.get_mut()))
        })();
        if let Ok(output) = &result {
            if let Some(hash) = admit {
                workspace
                    .memo
                    .as_mut()
                    .expect("admitted memo")
                    .insert(input, output, hash);
            }
        }
        if result.is_err() {
            // A failed or cancelled stream must never poison the next input.
            *cache = None;
        }
        result
    })
}

fn decode(
    decoder: &mut flate2::Decompress,
    input: &[u8],
    expected: usize,
) -> Result<Option<Vec<u8>>, QueryError> {
    decoder.reset(true);
    let mut out = Vec::with_capacity(expected.min(1024 * 1024));
    let mut buffer = [0; 16 * 1024];
    loop {
        crate::engine::cancel::check_cancelled()?;
        let start = decoder.total_in() as usize;
        let end = input.len().min(start.saturating_add(64 * 1024));
        let previous = decoder.total_out();
        // One excess byte proves the advertised size is wrong. Never inflate
        // an arbitrarily large corrupt stream into an unbounded output Vec.
        let capacity = buffer
            .len()
            .min(expected.saturating_sub(out.len()).saturating_add(1));
        let flush = if start == input.len() {
            flate2::FlushDecompress::Finish
        } else {
            flate2::FlushDecompress::None
        };
        let Ok(status) = decoder.decompress(&input[start..end], &mut buffer[..capacity], flush)
        else {
            return Ok(None);
        };
        let produced = (decoder.total_out() - previous) as usize;
        out.extend_from_slice(&buffer[..produced]);
        if out.len() > expected {
            return Ok(None);
        }
        if status == flate2::Status::StreamEnd {
            return Ok((out.len() == expected).then_some(out));
        }
        if produced == 0 && decoder.total_in() as usize == start {
            // Match the existing Read decoder's EOF contract, including its
            // behavior for an exhausted, incomplete stream of the right size.
            return Ok((start == input.len() && out.len() == expected).then_some(out));
        }
    }
}

pub(super) fn uncompress(input: &[u8], expected: usize) -> Result<Option<Vec<u8>>, QueryError> {
    WORKSPACE.with(|cache| {
        let Ok(mut cache) = cache.try_borrow_mut() else {
            return decode(&mut flate2::Decompress::new(true), input, expected);
        };
        match cache.as_mut() {
            Some(workspace) => decode(&mut workspace.decoder, input, expected),
            // Independent UNCOMPRESS calls still work when no compressor has
            // populated the optional cache, without making admission mandatory.
            None => decode(&mut flate2::Decompress::new(true), input, expected),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn reused_compression_streams_match_fresh_encoder_and_cross_decode() {
        let binary: Vec<_> = (0..200_003)
            .map(|i| ((i * 31 + i / 256) % 256) as u8)
            .collect();
        for input in [
            b"a".as_slice(),
            b"\0\xffbinary ",
            binary.as_slice(),
            "数据库🙂".as_bytes(),
            b"",
            b"a",
        ]
        .into_iter()
        .cycle()
        .take(18)
        {
            let actual = compress(input).unwrap();
            let mut reference = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            reference.write_all(input).unwrap();
            let mut expected = (input.len() as u32).to_le_bytes().to_vec();
            expected.extend(reference.finish().unwrap());
            assert_eq!(actual, expected);
            let mut decoded = Vec::new();
            flate2::read::ZlibDecoder::new(&actual[4..])
                .read_to_end(&mut decoded)
                .unwrap();
            assert_eq!(decoded, input);
        }
    }

    #[test]
    fn reentrant_compression_uses_an_independent_stream() {
        WORKSPACE.with(|cache| {
            let _guard = cache.borrow_mut();
            assert_eq!(compress(b"nested").unwrap(), fresh(b"nested").unwrap());
        });
    }

    #[test]
    fn reused_decoder_rejects_corruption_and_size_mismatch_then_recovers() {
        let bytes = compress(b"data\0\xff").unwrap();
        for input in [
            &bytes[4..],
            &bytes[4..bytes.len() - 1],
            b"bad".as_slice(),
            b"".as_slice(),
            &bytes[4..],
        ] {
            for expected in [0, 2, 6, 10] {
                let mut reference = Vec::new();
                let valid = flate2::read::ZlibDecoder::new(input)
                    .read_to_end(&mut reference)
                    .is_ok()
                    && reference.len() == expected;
                assert_eq!(
                    uncompress(input, expected).unwrap(),
                    valid.then_some(reference)
                );
            }
        }
        let mut trailing = bytes[4..].to_vec();
        trailing.extend_from_slice(b"trailing");
        assert_eq!(
            uncompress(&trailing, 6).unwrap(),
            Some(b"data\0\xff".to_vec())
        );
    }

    #[test]
    fn cancelled_compression_drops_workspace_and_next_statement_recovers() {
        let token = crate::engine::cancel::QueryCancelToken::noop();
        token.cancel_local();
        {
            let _scope = crate::engine::cancel::enter(token);
            assert!(compress(&vec![b'x'; 128 * 1024]).is_err());
            WORKSPACE.with(|cache| assert!(cache.borrow().is_none()));
        }
        let output = compress(b"recovered").unwrap();
        assert_eq!(
            uncompress(&output[4..], 9).unwrap(),
            Some(b"recovered".to_vec())
        );
    }

    #[test]
    fn cached_compression_still_observes_cancellation_and_recovers() {
        let expected = fresh(b"cached").unwrap();
        for _ in 0..3 {
            assert_eq!(compress(b"cached").unwrap(), expected);
        }
        let token = crate::engine::cancel::QueryCancelToken::noop();
        token.cancel_local();
        {
            let _scope = crate::engine::cancel::enter(token);
            assert!(compress(b"cached").is_err());
            WORKSPACE.with(|cache| assert!(cache.borrow().is_none()));
        }
        assert_eq!(compress(b"cached").unwrap(), expected);
    }

    #[test]
    fn refused_cache_admission_keeps_exact_compression_available() {
        WORKSPACE.with(|cache| *cache.borrow_mut() = None);
        let expected = fresh(b"no optional cache").unwrap();
        crate::engine::executor::mem_budget::with_test_effective_memory_limit(1, || {
            assert!(memo::Memo::try_new().is_none());
            assert_eq!(compress(b"no optional cache").unwrap(), expected);
            WORKSPACE.with(|cache| assert!(cache.borrow().is_none()));
        });
    }
}
