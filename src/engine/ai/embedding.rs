//! Typed embedding inputs and bounded image transport encoding.
use super::*;
use crate::types::Value;
use base64::Engine;
use serde_json::Value as Json;

pub(crate) const MAX_IMAGE_BYTES: usize = 5 * 1024 * 1024;
pub(crate) const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn image_protocol(protocol: &str) -> bool {
    matches!(
        protocol,
        "dashscope_multimodal_embeddings" | "vllm_embeddings"
    )
}

pub(crate) fn batch_limit(model: &Model, connection: &Connection) -> usize {
    model
        .limits
        .max_batch_items
        .unwrap_or(if image_protocol(&connection.protocol) {
            1
        } else {
            64
        })
        .min(64)
}

/// Include both sources of provider parameters before allocating a request.
/// JSONB strings and canonicalized JSON text can expand during serialization.
pub(crate) fn request_overhead_bound(
    model: &Model,
    options: Option<&Value>,
) -> Result<usize, QueryError> {
    struct Size(usize);
    impl std::io::Write for Size {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut defaults = Size(0);
    serde_json::to_writer(&mut defaults, &model.defaults)
        .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "cannot size model defaults"))?;
    let supplied = match options {
        Some(Value::Json(bytes)) => bytes.len().saturating_mul(6),
        Some(value) => {
            crate::types::value_str(value).map_or(0, |text| text.len().saturating_mul(6))
        }
        None => 0,
    };
    // Fixed adapter fields fit within 1 KiB; input-item wrappers are included
    // separately. Counting the complete defaults also covers merged parameters.
    Ok(1024usize
        .saturating_add(json_string_bytes(&model.remote_model))
        .saturating_add(defaults.0)
        .saturating_add(supplied))
}

pub(super) enum EmbeddingInput<'a> {
    Text(&'a str),
    ImageUrl(&'a str),
    ImageBytes(&'a [u8], &'static str),
}
impl<'a> EmbeddingInput<'a> {
    pub(super) fn image(value: &'a Value, model: &Model) -> Result<Self, QueryError> {
        if let Value::Bytes(bytes) = value {
            if bytes.is_empty()
                || bytes.len()
                    > model
                        .limits
                        .max_image_bytes
                        .unwrap_or(MAX_IMAGE_BYTES)
                        .min(MAX_IMAGE_BYTES)
            {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "image exceeds byte limit or is empty",
                ));
            }
            let mime = if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
                "image/png"
            } else if bytes.starts_with(b"\xff\xd8\xff") {
                "image/jpeg"
            } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
                "image/webp"
            } else if bytes.starts_with(b"BM") {
                "image/bmp"
            } else if bytes.starts_with(b"II\x2a\x00") || bytes.starts_with(b"MM\x00\x2a") {
                "image/tiff"
            } else {
                return Err(error(
                    "ER_AI_INVALID_ARGUMENT",
                    "unsupported image file signature",
                ));
            };
            return Ok(Self::ImageBytes(bytes, mime));
        }
        let url = crate::types::value_str(value).ok_or_else(|| {
            error(
                "ER_AI_INVALID_ARGUMENT",
                "image input requires an HTTP/HTTPS URL or binary data",
            )
        })?;
        Self::image_url(url)
    }

    pub(super) fn image_url(url: &'a str) -> Result<Self, QueryError> {
        if url.is_empty()
            || url.len() > 2048
            || url
                .bytes()
                .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
        {
            return Err(error("ER_AI_INVALID_ARGUMENT", "invalid image URL"));
        }
        let parsed = reqwest::Url::parse(url)
            .map_err(|_| error("ER_AI_INVALID_ARGUMENT", "invalid image URL"))?;
        if !matches!(parsed.scheme(), "http" | "https")
            || !parsed.has_host()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return Err(error(
                "ER_AI_INVALID_ARGUMENT",
                "image URL requires HTTP/HTTPS without credentials",
            ));
        }
        Ok(Self::ImageUrl(url))
    }

    /// Compute JSON string bytes without allocating an encoded buffer.
    pub(super) fn encoded_bound(&self) -> usize {
        match self {
            Self::Text(s) | Self::ImageUrl(s) => json_string_bytes(s).saturating_add(32),
            Self::ImageBytes(bytes, _) => {
                bytes.len().div_ceil(3).saturating_mul(4).saturating_add(64)
            }
        }
    }

    pub(super) fn json(&self) -> Json {
        Json::String(match self {
            Self::Text(s) | Self::ImageUrl(s) => (*s).to_owned(),
            Self::ImageBytes(bytes, mime) => {
                let mut data = String::with_capacity(self.encoded_bound());
                data.push_str("data:");
                data.push_str(mime);
                data.push_str(";base64,");
                base64::engine::general_purpose::STANDARD.encode_string(bytes, &mut data);
                data
            }
        })
    }
}

/// Exact JSON string length, including quotes, without constructing a DOM.
pub(crate) fn json_string_bytes(text: &str) -> usize {
    text.bytes().fold(2usize, |size, byte| {
        size + match byte {
            b'"' | b'\\' | b'\n' | b'\r' | b'\t' | 8 | 12 => 2,
            0..=31 => 6,
            _ => 1,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ai_image_signatures_url_validation_and_encoding_boundaries() {
        let mut model: Model = serde_json::from_value(serde_json::json!({"kind":"embedding","remote_model":"vision","model_revision":"v1","embedding":{"dimensions":3}})).unwrap();
        for (bytes, mime) in [
            (b"\x89PNG\r\n\x1a\n".as_slice(), "image/png"),
            (b"\xff\xd8\xff", "image/jpeg"),
            (b"RIFF1234WEBP", "image/webp"),
            (b"BMdata", "image/bmp"),
            (b"II\x2a\x00", "image/tiff"),
            (b"MM\x00\x2a", "image/tiff"),
        ] {
            let value = Value::Bytes(bytes.to_vec());
            model.limits.max_image_bytes = Some(bytes.len());
            let input = EmbeddingInput::image(&value, &model).unwrap();
            let encoded = input.json();
            assert!(encoded
                .as_str()
                .unwrap()
                .starts_with(&format!("data:{mime};base64,")));
            assert!(encoded.to_string().len() <= input.encoded_bound());
            model.limits.max_image_bytes = Some(bytes.len() - 1);
            assert!(EmbeddingInput::image(&value, &model).is_err());
        }
        for url in [
            "file:///tmp/a.png",
            "data:image/png;base64,YQ==",
            "https://user:pass@example.com/a.png",
            "ftp://example.com/a.png",
            "https://example.com/a\n.png",
            "a.png",
            "",
        ] {
            assert!(EmbeddingInput::image_url(url).is_err(), "{url}");
        }
        for url in [
            "https://example.com/a.png?token=abc",
            "http://127.0.0.1/p.png",
            "https://[::1]/p.png",
        ] {
            assert!(EmbeddingInput::image_url(url).is_ok());
        }
        assert!(
            EmbeddingInput::image_url(&format!("https://example.com/{}", "a".repeat(2048)))
                .is_err()
        );
        for text in ["plain", "\n\t\r\u{0001}\\\"", "图片"] {
            assert_eq!(
                json_string_bytes(text),
                serde_json::to_string(text).unwrap().len()
            );
        }
    }
}
