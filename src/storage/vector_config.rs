//! Validated, persisted vector-index options. SQL comments never configure indexes.
use super::schema::{VectorAlgo, VectorIndexMeta};
use crate::{types::QueryError, vector::VectorDataType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct VectorIndexOptions {
    pub entries: Vec<VectorOption>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorOption {
    pub name: String,
    pub value: String,
    pub numeric: bool,
    pub offset: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorAlgorithmConfig {
    Hnsw {
        m: u32,
        ef_construction: u32,
        ef_search: Option<u32>,
    },
    Ivf {
        nlist: u32,
        nprobe: Option<u32>,
        kmeans_iters: u32,
        train_sample: u32,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum QuantizationConfig {
    #[default]
    None,
    Sq8,
    Sq4,
    Pq {
        m: Option<u32>,
        bits: u8,
        train_samples: u32,
        iterations: u32,
    },
    Bq,
}
impl QuantizationConfig {
    pub fn name(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Sq8 => "sq8",
            Self::Sq4 => "sq4",
            Self::Pq { .. } => "pq",
            Self::Bq => "bq",
        }
    }
    pub fn enabled(&self) -> bool {
        !matches!(self, Self::None)
    }
    pub fn code_len(&self, dim: u32) -> usize {
        let d = dim as usize;
        match self {
            Self::None => 0,
            Self::Sq8 => d,
            Self::Sq4 => d.div_ceil(2),
            Self::Bq => d.div_ceil(8) + 24,
            Self::Pq { m, bits, .. } => {
                (m.unwrap_or(dim.div_ceil(8)) as usize * *bits as usize).div_ceil(8)
            }
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorIndexConfig {
    pub metric: Option<String>,
    pub algorithm: VectorAlgorithmConfig,
    pub quantization: QuantizationConfig,
    pub rerank_mult: u32,
}
fn invalid(message: impl Into<String>) -> QueryError {
    crate::storage::error(crate::error::catalog::ER_NOT_SUPPORTED_YET, message)
}
impl VectorIndexConfig {
    pub fn from_options(
        index_type: &str,
        options: Option<&VectorIndexOptions>,
        comment: &str,
    ) -> Result<Option<Self>, QueryError> {
        let hnsw =
            index_type.eq_ignore_ascii_case("HNSW") || index_type.eq_ignore_ascii_case("VECTOR");
        let ivf = index_type.eq_ignore_ascii_case("IVF");
        if !hnsw && !ivf {
            return if options.is_some() {
                Err(invalid("WITH vector options require USING HNSW or IVF"))
            } else {
                Ok(None)
            };
        }
        Self::validate_comment(comment)?;
        let mut values = std::collections::BTreeMap::new();
        for option in options.into_iter().flat_map(|o| &o.entries) {
            let name = option.name.to_ascii_lowercase();
            if values.insert(name.clone(), option).is_some() {
                return Err(invalid(format!("duplicate vector WITH option '{name}'")));
            }
            let valid = match name.as_str() {
                "metric" | "quantization" | "rerank_mult" => true,
                "m" | "ef_construction" | "ef_search" => hnsw,
                "nlist" | "nprobe" | "kmeans_iters" | "train_sample" => ivf,
                "pq_m" | "pq_bits" | "pq_train_samples" | "pq_iterations" => true,
                _ => false,
            };
            if !valid {
                return Err(invalid(format!(
                    "invalid WITH option '{name}' for {index_type}"
                )));
            }
        }
        let number = |name: &str, default: u32, min: u32, max: u32| -> Result<u32, QueryError> {
            let Some(v) = values.get(name) else {
                return if (min..=max).contains(&default) {
                    Ok(default)
                } else {
                    Err(invalid(format!("invalid default for WITH {name}")))
                };
            };
            let n = v
                .numeric
                .then(|| v.value.parse::<u32>().ok())
                .flatten()
                .filter(|n| (min..=max).contains(n));
            n.ok_or_else(|| {
                invalid(format!(
                    "invalid vector WITH {name} value '{}': expected integer in {min}..={max}",
                    v.value
                ))
            })
        };
        let enum_value = |name: &str, default: &str| -> Result<String, QueryError> {
            match values.get(name) {
                Some(v) if v.numeric => Err(invalid(format!("WITH {name} requires a name"))),
                Some(v) => Ok(v.value.to_ascii_lowercase()),
                None => Ok(default.into()),
            }
        };
        let metric = if values.contains_key("metric") {
            let name = enum_value("metric", "l2")?;
            if !matches!(
                name.as_str(),
                "l2" | "cosine" | "dot" | "hamming" | "jaccard"
            ) {
                return Err(invalid(format!("invalid vector metric '{name}'")));
            }
            Some(name.to_ascii_uppercase())
        } else {
            None
        };
        let q = enum_value("quantization", "none")?;
        if q != "pq" && values.keys().any(|k| k.starts_with("pq_")) {
            return Err(invalid("pq_* options require quantization=pq"));
        }
        let quantization = match q.as_str() {
            "none" => QuantizationConfig::None,
            "sq8" => QuantizationConfig::Sq8,
            "sq4" => QuantizationConfig::Sq4,
            "bq" => QuantizationConfig::Bq,
            "pq" => {
                let bits = number("pq_bits", 8, 4, 8)?;
                if bits != 4 && bits != 8 {
                    return Err(invalid("pq_bits must be 4 or 8"));
                }
                QuantizationConfig::Pq {
                    m: values
                        .contains_key("pq_m")
                        .then(|| number("pq_m", 1, 1, 524256))
                        .transpose()?,
                    bits: bits as u8,
                    train_samples: number("pq_train_samples", 65536, 1 << bits, 1_048_576)?,
                    iterations: number("pq_iterations", 25, 1, 1000)?,
                }
            }
            _ => return Err(invalid(format!("unsupported quantization '{q}'"))),
        };
        let env_default = |key: &str, default: u32| -> Result<u32, QueryError> {
            match std::env::var(key) {
                Ok(s) => s
                    .parse()
                    .map_err(|_| invalid(format!("invalid {key} build default"))),
                Err(_) => Ok(default),
            }
        };
        let algorithm = if hnsw {
            let m = number("m", crate::engine::vector::DEFAULT_M as u32, 2, 1024)?;
            let ef_construction = number(
                "ef_construction",
                crate::engine::vector::DEFAULT_EF_CONSTRUCTION as u32,
                m,
                65535,
            )?;
            if ef_construction < m {
                return Err(invalid("ef_construction must be at least m"));
            }
            VectorAlgorithmConfig::Hnsw {
                m,
                ef_construction,
                ef_search: values
                    .contains_key("ef_search")
                    .then(|| number("ef_search", 64, 1, 1_000_000))
                    .transpose()?,
            }
        } else {
            let nlist = number("nlist", env_default("MYDB_IVF_NLIST", 0)?, 0, 1_048_576)?;
            let kmeans_iters = number(
                "kmeans_iters",
                env_default("MYDB_IVF_KMEANS_ITERS", 15)?,
                1,
                1000,
            )?;
            let train_sample = number(
                "train_sample",
                env_default("MYDB_IVF_TRAIN_SAMPLE", 256)?,
                1,
                65536,
            )?;
            if nlist > 1_048_576
                || !(1..=1000).contains(&kmeans_iters)
                || !(1..=65536).contains(&train_sample)
            {
                return Err(invalid("invalid IVF environment build defaults"));
            }
            VectorAlgorithmConfig::Ivf {
                nlist,
                kmeans_iters,
                train_sample,
                nprobe: values
                    .contains_key("nprobe")
                    .then(|| number("nprobe", 8, 1, 1_048_576))
                    .transpose()?,
            }
        };
        Ok(Some(Self {
            metric,
            algorithm,
            quantization,
            rerank_mult: number(
                "rerank_mult",
                env_default(
                    if hnsw {
                        "MYDB_HNSW_RERANK_MULT"
                    } else {
                        "MYDB_IVF_RERANK_MULT"
                    },
                    4,
                )?,
                1,
                1024,
            )?,
        }))
    }
    pub fn validate_comment(comment: &str) -> Result<(), QueryError> {
        let mut normalized = String::new();
        for c in comment.chars() {
            if c == '=' {
                while normalized.ends_with(char::is_whitespace) {
                    normalized.pop();
                }
            }
            normalized.push(c);
        }
        for token in normalized.split(|c: char| c.is_whitespace() || c == ',' || c == ';') {
            let Some((key, _)) = token.split_once('=') else {
                continue;
            };
            if matches!(
                key.to_ascii_uppercase().as_str(),
                "METRIC"
                    | "DISTANCE"
                    | "M"
                    | "EF_CONSTRUCTION"
                    | "EFCONSTRUCTION"
                    | "EF_CONSTRUCT"
                    | "EF_SEARCH"
                    | "EFSEARCH"
                    | "DEFAULT_EF_SEARCH"
                    | "NLIST"
                    | "LISTS"
                    | "NPROBE"
                    | "PROBES"
                    | "DEFAULT_NPROBE"
                    | "KMEANS_ITERS"
                    | "KMEANS_ITER"
                    | "ITERS"
                    | "TRAIN_SAMPLE"
                    | "TRAIN_SAMPLE_MULT"
                    | "SAMPLE_MULT"
                    | "QUANTIZATION"
            ) {
                return Err(invalid(format!("vector index COMMENT option '{key}' is no longer supported; use WITH ({key}=...)")));
            }
        }
        Ok(())
    }
    pub fn resolve(
        &mut self,
        datatype: VectorDataType,
        dimension: u32,
        algo: VectorAlgo,
    ) -> Result<(), QueryError> {
        if matches!(
            (&self.algorithm, algo),
            (VectorAlgorithmConfig::Hnsw { .. }, VectorAlgo::Ivf)
                | (VectorAlgorithmConfig::Ivf { .. }, VectorAlgo::Hnsw)
        ) {
            return Err(invalid("vector algorithm/configuration mismatch"));
        }
        if !(1..=1024).contains(&self.rerank_mult) {
            return Err(invalid("rerank_mult must be in 1..=1024"));
        }
        match self.algorithm {
            VectorAlgorithmConfig::Hnsw {
                m,
                ef_construction,
                ef_search,
            } => {
                if !(2..=1024).contains(&m)
                    || !(m..=65535).contains(&ef_construction)
                    || ef_search.is_some_and(|n| !(1..=1_000_000).contains(&n))
                {
                    return Err(invalid("invalid persisted HNSW configuration"));
                }
            }
            VectorAlgorithmConfig::Ivf {
                nlist,
                nprobe,
                kmeans_iters,
                train_sample,
            } => {
                if nlist > 1_048_576
                    || nprobe.is_some_and(|n| !(1..=1_048_576).contains(&n))
                    || !(1..=1000).contains(&kmeans_iters)
                    || !(1..=65536).contains(&train_sample)
                {
                    return Err(invalid("invalid persisted IVF configuration"));
                }
            }
        }
        if let QuantizationConfig::Pq {
            bits,
            train_samples,
            iterations,
            ..
        } = self.quantization
        {
            if !matches!(bits, 4 | 8)
                || !(1..=1000).contains(&iterations)
                || train_samples < (1u32 << bits)
                || train_samples > 1_048_576
            {
                return Err(invalid("invalid persisted PQ configuration"));
            }
        }
        self.metric.get_or_insert_with(|| {
            if datatype == VectorDataType::Binary {
                "HAMMING".into()
            } else {
                "L2".into()
            }
        });
        let metric = self.metric.as_deref().unwrap().to_ascii_uppercase();
        if !matches!(
            metric.as_str(),
            "L2" | "COSINE" | "DOT" | "HAMMING" | "JACCARD"
        ) {
            return Err(invalid("invalid persisted vector metric"));
        }
        if (datatype == VectorDataType::Binary) != matches!(metric.as_str(), "HAMMING" | "JACCARD")
        {
            return Err(invalid("vector datatype and metric are incompatible"));
        }
        self.metric = Some(metric);
        if datatype == VectorDataType::Binary && self.quantization.enabled() {
            return Err(invalid("BINARY vector indexes do not support quantization"));
        }
        if let QuantizationConfig::Pq { m, .. } = &mut self.quantization {
            let parts = *m.get_or_insert(dimension.div_ceil(8));
            if parts == 0 || parts > dimension {
                return Err(invalid("pq_m must be between 1 and the vector dimension"));
            }
        }
        Ok(())
    }
    pub fn meta(&self) -> VectorIndexMeta {
        let mut meta = VectorIndexMeta {
            metric: self.metric.clone().unwrap_or_else(|| "L2".into()),
            ..Default::default()
        };
        match self.algorithm {
            VectorAlgorithmConfig::Hnsw {
                m,
                ef_construction,
                ef_search,
            } => {
                meta.m = m;
                meta.ef_construction = ef_construction;
                meta.default_ef_search = ef_search;
            }
            VectorAlgorithmConfig::Ivf {
                nlist,
                nprobe,
                kmeans_iters,
                train_sample,
            } => {
                meta.nlist = Some(nlist);
                meta.default_nprobe = nprobe;
                meta.kmeans_iters = Some(kmeans_iters);
                meta.train_sample_mult = Some(train_sample);
            }
        }
        meta
    }
    pub fn sql(&self) -> String {
        let mut p = vec![format!(
            "metric={}",
            self.metric.as_deref().unwrap_or("L2").to_ascii_lowercase()
        )];
        match self.algorithm {
            VectorAlgorithmConfig::Hnsw {
                m,
                ef_construction,
                ef_search,
            } => {
                p.push(format!("m={m}"));
                p.push(format!("ef_construction={ef_construction}"));
                if let Some(n) = ef_search {
                    p.push(format!("ef_search={n}"));
                }
            }
            VectorAlgorithmConfig::Ivf {
                nlist,
                nprobe,
                kmeans_iters,
                train_sample,
            } => {
                p.push(format!("nlist={nlist}"));
                p.push(format!("kmeans_iters={kmeans_iters}"));
                p.push(format!("train_sample={train_sample}"));
                if let Some(n) = nprobe {
                    p.push(format!("nprobe={n}"));
                }
            }
        }
        p.push(format!("quantization={}", self.quantization.name()));
        p.push(format!("rerank_mult={}", self.rerank_mult));
        if let QuantizationConfig::Pq {
            m,
            bits,
            train_samples,
            iterations,
        } = self.quantization
        {
            if let Some(m) = m {
                p.push(format!("pq_m={m}"));
            }
            p.push(format!("pq_bits={bits}"));
            p.push(format!("pq_train_samples={train_samples}"));
            p.push(format!("pq_iterations={iterations}"));
        }
        format!(" WITH ({})", p.join(", "))
    }
}
