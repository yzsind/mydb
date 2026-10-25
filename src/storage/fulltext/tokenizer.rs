//! Full-text index tokenizer.
//!
//! The inverted index reuses the B-tree: the tokenizer splits a row of text into tokens, each encoded
//! into an index key (`KeyPart::String(token)`) that with the row's row_id forms an inverted item.
//! **The indexing side and the query side must use the same tokenizer and the same normalization
//! rules**, otherwise matches are missed (false negatives) — the most common and most insidious full-text bug; all tokens are lowercased here.
//!
//! Two tokenizers, matching MySQL 8.0:
//! - [`DefaultTokenizer`]: splits on Unicode non-alphanumeric boundaries, subject to min/max token size
//!   and stopword filtering. Suited to whitespace-delimited languages such as English. CJK text here
//!   falls into one overlong "token" (Chinese characters satisfy `is_alphanumeric()`), usually filtered
//!   out by max_token_size, so Chinese must use ngram.
//! - [`NgramTokenizer`]: `WITH PARSER ngram`, takes fixed-length n-grams (default 2) by sliding a window over `char`s,
//!   targeting CJK. Token length is always n, **not subject to min/max token size**.

use crate::util::env::process_bool as parse_env_bool;
use std::collections::HashSet;
use std::sync::OnceLock;

/// MySQL InnoDB's default English stopword list (`information_schema.INNODB_FT_DEFAULT_STOPWORD`).
const DEFAULT_STOPWORDS: &[&str] = &[
    "a", "about", "an", "are", "as", "at", "be", "by", "com", "de", "en", "for", "from", "how",
    "i", "in", "is", "it", "la", "of", "on", "or", "that", "the", "this", "to", "was", "what",
    "when", "where", "who", "will", "with", "und", "www",
];

/// The built-in English stopword list, exposed for
/// `information_schema.innodb_ft_default_stopword`.
pub(crate) fn default_stopwords() -> &'static [&'static str] {
    DEFAULT_STOPWORDS
}

fn stopword_set() -> &'static HashSet<&'static str> {
    static SET: OnceLock<HashSet<&'static str>> = OnceLock::new();
    SET.get_or_init(|| DEFAULT_STOPWORDS.iter().copied().collect())
}

/// Tokenizer configuration. Global defaults come from the `MYDB_FT_*` environment variables (process-level, cached on first access);
/// some fields (e.g. `ngram_size`) can be overridden per index by index metadata.
#[derive(Debug, Clone)]
pub struct TokenizerConfig {
    /// Minimum token length (in characters) kept by the default tokenizer. MySQL `innodb_ft_min_token_size`.
    pub min_token_size: usize,
    /// Maximum token length kept by the default tokenizer. MySQL `innodb_ft_max_token_size`.
    pub max_token_size: usize,
    /// Window size of the ngram tokenizer. MySQL `ngram_token_size`.
    pub ngram_size: usize,
    /// Whether default stopword filtering is enabled.
    pub use_stopwords: bool,
}

impl TokenizerConfig {
    /// Read a configuration from the `MYDB_FT_*` environment variables (bypassing the global cache, for easy test overrides).
    pub fn from_env() -> Self {
        TokenizerConfig {
            min_token_size: parse_env_usize("MYDB_FT_MIN_TOKEN_SIZE", 3).max(1),
            max_token_size: parse_env_usize("MYDB_FT_MAX_TOKEN_SIZE", 84).max(1),
            ngram_size: parse_env_usize("MYDB_FT_NGRAM_SIZE", 2).max(1),
            use_stopwords: parse_env_bool("MYDB_FT_STOPWORDS", true),
        }
    }
}

/// Process-level global default configuration (`MYDB_FT_*`, cached on first access).
pub fn global_config() -> &'static TokenizerConfig {
    static CFG: OnceLock<TokenizerConfig> = OnceLock::new();
    CFG.get_or_init(TokenizerConfig::from_env)
}

fn parse_env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

/// Full-text tokenizer. `tokenize` returns a **normalized (lowercased) and filtered** token sequence,
/// **preserving duplicates and order**: duplicates feed document-length / term-frequency statistics; the indexing side deduplicates before writing inverted items.
pub trait Tokenizer: Send + Sync {
    /// Split a piece of text into normalized tokens (duplicates included, order preserved).
    fn tokenize(&self, text: &str) -> Vec<String>;
    /// Visit normalized tokens without allocating a token vector. Implementations
    /// may borrow unchanged text; the default preserves the tokenization oracle.
    fn visit_tokens<'a>(&self, text: &'a str, visit: &mut dyn FnMut(std::borrow::Cow<'a, str>)) {
        for token in self.tokenize(text) {
            visit(std::borrow::Cow::Owned(token));
        }
    }
    /// MySQL `WITH PARSER` name: `""` for the default tokenizer (not displayed), `"ngram"` for ngram.
    /// SHOW CREATE TABLE / catalog use the persisted `FulltextMeta.parser` (no tokenizer rebuild
    /// needed), so this method currently serves only tokenizer self-description and tests.
    #[allow(dead_code)]
    fn parser_name(&self) -> &'static str;
}

/// Build a tokenizer from `parser_name` and an optional per-index `ngram_size`. `""` / unknown names → default tokenizer.
///
/// Only `ngram_size` supports per-index override; the rest use global defaults — **test use only**.
/// Production code must always go through [`build_tokenizer_from_meta`](build_tokenizer_from_meta): it
/// overrides ngram_size / min / max / stopwords from the index meta in one shot, preventing
/// build/query tokenization rules from diverging after an env change across restarts, which would silently cause false negatives.
#[cfg(test)]
pub fn build_tokenizer(parser_name: &str, ngram_size: Option<usize>) -> Box<dyn Tokenizer> {
    let cfg = global_config().clone();
    match parser_name {
        "ngram" => {
            let n = ngram_size.unwrap_or(cfg.ngram_size).max(1);
            Box::new(NgramTokenizer { n, cfg })
        }
        _ => Box::new(DefaultTokenizer { cfg }),
    }
}

/// Build a tokenizer from the index's persisted `FulltextMeta`. Any field that is `None` falls back
/// to the global `MYDB_FT_*` defaults (legacy-index compatibility); otherwise the value snapshotted
/// at DDL time in the meta is used. This is the entry the query and DML maintenance paths **must** go through — one shared set of rules keeps inverted items byte-identical.
pub fn build_tokenizer_from_meta(
    meta: &crate::storage::schema::FulltextMeta,
) -> Box<dyn Tokenizer> {
    let global = global_config();
    let cfg = TokenizerConfig {
        min_token_size: meta
            .min_token_size
            .map(|n| (n as usize).max(1))
            .unwrap_or(global.min_token_size),
        max_token_size: meta
            .max_token_size
            .map(|n| (n as usize).max(1))
            .unwrap_or(global.max_token_size),
        ngram_size: meta
            .ngram_size
            .map(|n| (n as usize).max(1))
            .unwrap_or(global.ngram_size),
        use_stopwords: meta.use_stopwords.unwrap_or(global.use_stopwords),
    };
    match meta.parser.as_str() {
        "ngram" => {
            let n = cfg.ngram_size;
            Box::new(NgramTokenizer { n, cfg })
        }
        _ => Box::new(DefaultTokenizer { cfg }),
    }
}

/// Fill unset tokenizer and posting-format settings at DDL time. Persisting
/// both keeps query tokenization and exact DML key reconstruction stable when
/// process defaults change. Explicit index settings and `parser` are preserved.
pub fn snapshot_tokenizer_config_into(meta: &mut crate::storage::schema::FulltextMeta) {
    let g = global_config();
    if meta.ngram_size.is_none() {
        meta.ngram_size = Some(g.ngram_size as u32);
    }
    if meta.min_token_size.is_none() {
        meta.min_token_size = Some(g.min_token_size as u32);
    }
    if meta.max_token_size.is_none() {
        meta.max_token_size = Some(g.max_token_size as u32);
    }
    if meta.use_stopwords.is_none() {
        meta.use_stopwords = Some(g.use_stopwords);
    }
    let payload = super::payload::config();
    if meta.positions.is_none() {
        meta.positions = Some(payload.positions);
    }
    if meta.position_gap.is_none() {
        meta.position_gap = Some(payload.position_gap);
    }
}

fn is_stopword(cfg: &TokenizerConfig, token: &str) -> bool {
    cfg.use_stopwords && stopword_set().contains(token)
}

/// Default (whitespace/punctuation) tokenizer.
pub struct DefaultTokenizer {
    cfg: TokenizerConfig,
}

impl DefaultTokenizer {
    fn flush(&self, cur: &mut String, out: &mut Vec<String>) {
        if cur.is_empty() {
            return;
        }
        let token = std::mem::take(cur);
        let len = token.chars().count();
        if len >= self.cfg.min_token_size
            && len <= self.cfg.max_token_size
            && !is_stopword(&self.cfg, &token)
        {
            out.push(token);
        }
    }
}

impl Tokenizer for DefaultTokenizer {
    fn visit_tokens<'a>(&self, text: &'a str, visit: &mut dyn FnMut(std::borrow::Cow<'a, str>)) {
        if !text.is_ascii() {
            for token in self.tokenize(text) {
                visit(std::borrow::Cow::Owned(token));
            }
            return;
        }
        for word in text.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_') {
            if word.len() < self.cfg.min_token_size || word.len() > self.cfg.max_token_size {
                continue;
            }
            let token = if word.bytes().any(|byte| byte.is_ascii_uppercase()) {
                std::borrow::Cow::Owned(word.to_ascii_lowercase())
            } else {
                std::borrow::Cow::Borrowed(word)
            };
            if !is_stopword(&self.cfg, &token) {
                visit(token);
            }
        }
    }

    fn tokenize(&self, text: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut cur = String::new();
        for ch in text.chars() {
            // `_` is treated as an in-word character (underscores fold into the token, as in MySQL).
            if ch.is_alphanumeric() || ch == '_' {
                cur.extend(ch.to_lowercase());
            } else {
                self.flush(&mut cur, &mut out);
            }
        }
        self.flush(&mut cur, &mut out);
        out
    }

    fn parser_name(&self) -> &'static str {
        ""
    }
}

/// ngram tokenizer (CJK).
pub struct NgramTokenizer {
    n: usize,
    cfg: TokenizerConfig,
}

impl NgramTokenizer {
    /// Emit all length-n sliding-window tokens for a run of alphanumeric characters (already lowercased).
    /// Emit nothing when the run is shorter than n (consistent with MySQL ngram behavior).
    fn emit_ngrams(&self, seg: &[char], out: &mut Vec<String>) {
        if seg.len() < self.n {
            return;
        }
        for window in seg.windows(self.n) {
            let token: String = window.iter().collect();
            if !is_stopword(&self.cfg, &token) {
                out.push(token);
            }
        }
    }
}

impl Tokenizer for NgramTokenizer {
    fn tokenize(&self, text: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut seg: Vec<char> = Vec::new();
        for ch in text.chars() {
            if ch.is_alphanumeric() {
                // to_lowercase may yield multiple chars; append them one by one (ASCII / CJK are usually 1:1).
                seg.extend(ch.to_lowercase());
            } else {
                self.emit_ngrams(&seg, &mut out);
                seg.clear();
            }
        }
        self.emit_ngrams(&seg, &mut out);
        out
    }

    fn parser_name(&self) -> &'static str {
        "ngram"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_tok() -> DefaultTokenizer {
        DefaultTokenizer {
            cfg: TokenizerConfig {
                min_token_size: 3,
                max_token_size: 84,
                ngram_size: 2,
                use_stopwords: true,
            },
        }
    }

    fn ngram_tok(n: usize) -> NgramTokenizer {
        NgramTokenizer {
            n,
            cfg: TokenizerConfig {
                min_token_size: 3,
                max_token_size: 84,
                ngram_size: n,
                use_stopwords: false,
            },
        }
    }

    #[test]
    fn default_lowercases_and_splits() {
        // Filter stopword the; retain other words lowercased.
        assert_eq!(
            default_tok().tokenize("The Quick Brown FOX!"),
            vec!["quick", "brown", "fox"]
        );
    }

    #[test]
    fn default_filters_short_and_stopwords() {
        // Only short words or stopwords yields no tokens.
        assert!(default_tok().tokenize("a an be of to").is_empty());
        // Two-character ox is below minimum length three and is filtered.
        assert_eq!(default_tok().tokenize("ox cat"), vec!["cat"]);
    }

    #[test]
    fn default_preserves_duplicates_and_order() {
        assert_eq!(
            default_tok().tokenize("cat dog cat"),
            vec!["cat", "dog", "cat"]
        );
    }

    #[test]
    fn ngram_cjk_sliding_window() {
        // Multibyte-safe slicing uses characters, not bytes.
        assert_eq!(ngram_tok(2).tokenize("数据库"), vec!["数据", "据库"]);
        assert_eq!(
            ngram_tok(2).tokenize("全文检索"),
            vec!["全文", "文检", "检索"]
        );
    }

    #[test]
    fn ngram_segments_on_non_alnum() {
        // Split at whitespace/punctuation, then generate ngrams independently inside each segment.
        assert_eq!(ngram_tok(2).tokenize("数据 检索"), vec!["数据", "检索"]);
    }

    #[test]
    fn ngram_short_segment_yields_nothing() {
        // Segments shorter than n emit nothing.
        assert!(ngram_tok(2).tokenize("我").is_empty());
        assert_eq!(ngram_tok(2).tokenize("我们好"), vec!["我们", "们好"]);
    }

    #[test]
    fn ngram_lowercases_ascii() {
        assert_eq!(ngram_tok(2).tokenize("AB"), vec!["ab"]);
    }
}
