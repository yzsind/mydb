//! BM25 relevance scoring (shared by natural-language mode and positive-token scoring in boolean mode).
//!
//! The per-(token, doc) `tf` / document length `|D|` / positions are all encoded by
//! [`super::payload::FtPayload`] into the second `Bytes` segment of the inverted key, so BM25 scoring
//! **reads straight from the inverted posting — zero row lookups, zero re-tokenization**. `df` comes
//! from the deduplicated count over the token's inverted range; `N` is estimated as the catalog's live
//! row count, floored to `max(df, candidates.len(), 1)` to keep IDF positive; `avgdl` approximates to
//! the mean of known doc_len in the **candidate set** (exact whole-store doc_len needs a full heap scan; the MySQL-style approximation is the chosen tradeoff).
//!
//! Formula (Lucene-style BM25; not a promise of MySQL score equality):
//! ```text
//! IDF(t)   = ln( (N - df + 0.5) / (df + 0.5) + 1 )
//! score(D) = Σ_t  IDF(t) · ( tf · (k1 + 1) )
//!                          / ( tf + k1 · (1 - b + b · |D| / avgdl) )
//! ```
//! `k1` / `b` come from `MYDB_FT_BM25_K1` (default 1.2) / `MYDB_FT_BM25_B` (default 0.75).

use std::sync::OnceLock;

/// Tunable BM25 parameters. `k1` controls term-frequency saturation speed; `b` controls document-length normalization strength.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bm25Params {
    pub k1: f64,
    pub b: f64,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Bm25Params { k1: 1.2, b: 0.75 }
    }
}

impl Bm25Params {
    /// Reads one set of parameters from the `MYDB_FT_BM25_*` env vars (bypasses the global cache, easy to override in tests).
    pub fn from_env() -> Self {
        let default = Bm25Params::default();
        Bm25Params {
            k1: parse_env_f64("MYDB_FT_BM25_K1", default.k1).max(0.0),
            b: parse_env_f64("MYDB_FT_BM25_B", default.b).clamp(0.0, 1.0),
        }
    }
}

/// Process-wide global parameters (cached on first access).
pub fn global_params() -> &'static Bm25Params {
    static PARAMS: OnceLock<Bm25Params> = OnceLock::new();
    PARAMS.get_or_init(Bm25Params::from_env)
}

fn parse_env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|v| v.is_finite())
        .unwrap_or(default)
}

/// Inverse document frequency. `n_docs` is the total document count N; `df` is the number of
/// documents containing the token. Callers must ensure `df <= n_docs` so IDF stays positive (a high-frequency term cannot push relevance negative).
#[inline]
pub fn idf(n_docs: f64, df: f64) -> f64 {
    ((n_docs - df + 0.5) / (df + 0.5) + 1.0).ln()
}

/// One query token's BM25 contribution to a document. `tf` is the token's frequency in the
/// document, `doc_len` its length `|D|`, and `avgdl` the mean document length (caller guarantees `> 0`).
#[inline]
pub fn term_score(idf: f64, tf: f64, doc_len: f64, avgdl: f64, params: &Bm25Params) -> f64 {
    if tf <= 0.0 {
        return 0.0;
    }
    let norm = params.k1 * (1.0 - params.b + params.b * doc_len / avgdl);
    idf * (tf * (params.k1 + 1.0)) / (tf + norm)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "expected {b}, got {a}");
    }

    #[test]
    fn idf_is_positive_when_df_le_n() {
        // Rare terms with low df have higher IDF than common terms with high df.
        let rare = idf(100.0, 1.0);
        let common = idf(100.0, 50.0);
        assert!(rare > common);
        // df==N still has positive IDF, preventing negative scores.
        assert!(idf(10.0, 10.0) > 0.0);
    }

    #[test]
    fn idf_hand_computed() {
        // N=2, df=1 → ln((2-1+0.5)/(1+0.5)+1) = ln(1.5/1.5+1) = ln(2)
        approx(idf(2.0, 1.0), std::f64::consts::LN_2);
    }

    #[test]
    fn term_score_hand_computed() {
        // With k1=1.2,b=0.75,idf=ln2,tf=1,and document length=avgdl, norm=k1 and denominator=tf+k1.
        let p = Bm25Params { k1: 1.2, b: 0.75 };
        let expected = std::f64::consts::LN_2 * (1.0 * 2.2) / (1.0 + 1.2);
        approx(
            term_score(std::f64::consts::LN_2, 1.0, 4.0, 4.0, &p),
            expected,
        );
    }

    #[test]
    fn term_score_zero_tf_is_zero() {
        let p = Bm25Params::default();
        approx(term_score(1.0, 0.0, 4.0, 4.0, &p), 0.0);
    }

    #[test]
    fn higher_tf_scores_higher() {
        let p = Bm25Params::default();
        let one = term_score(1.0, 1.0, 4.0, 4.0, &p);
        let three = term_score(1.0, 3.0, 4.0, 4.0, &p);
        assert!(three > one);
    }

    #[test]
    fn shorter_doc_scores_higher_for_same_tf() {
        // Length normalization makes shorter documents more relevant at equal tf.
        let p = Bm25Params::default();
        let short = term_score(1.0, 1.0, 2.0, 4.0, &p);
        let long = term_score(1.0, 1.0, 8.0, 4.0, &p);
        assert!(short > long);
    }
}
