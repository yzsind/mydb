//! Native vector datatypes and self-describing value encoding.

use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

pub const MAX_VECTOR_PAYLOAD: usize = 65_532;

/// Borrowed access shared by typed values and legacy FLOAT32 kernel buffers.
pub trait VectorInput {
    fn datatype(&self) -> VectorDataType;
    fn dimension(&self) -> u32;
    fn payload(&self) -> &[u8];
    #[inline]
    fn element(&self, i: usize) -> f64 {
        let payload = self.payload();
        match self.datatype() {
            VectorDataType::Float32 => {
                f32::from_le_bytes(payload[i * 4..i * 4 + 4].try_into().unwrap()) as f64
            }
            VectorDataType::Float16 => {
                half::f16::from_le_bytes(payload[i * 2..i * 2 + 2].try_into().unwrap()).to_f64()
            }
            VectorDataType::BFloat16 => {
                half::bf16::from_le_bytes(payload[i * 2..i * 2 + 2].try_into().unwrap()).to_f64()
            }
            VectorDataType::Int8 => (payload[i] as i8) as f64,
            VectorDataType::Binary => ((payload[i / 8] >> (i % 8)) & 1) as f64,
        }
    }
}

#[derive(Clone, Copy)]
pub struct VectorRef<'a> {
    pub datatype: VectorDataType,
    pub dimension: u32,
    pub payload: &'a [u8],
}
impl VectorInput for VectorRef<'_> {
    fn datatype(&self) -> VectorDataType {
        self.datatype
    }
    fn dimension(&self) -> u32 {
        self.dimension
    }
    fn payload(&self) -> &[u8] {
        self.payload
    }
}

impl VectorInput for VectorValue {
    fn datatype(&self) -> VectorDataType {
        self.datatype
    }
    fn dimension(&self) -> u32 {
        self.dimension
    }
    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl VectorInput for [u8] {
    fn datatype(&self) -> VectorDataType {
        VectorDataType::Float32
    }
    fn dimension(&self) -> u32 {
        (self.len() / 4) as u32
    }
    fn payload(&self) -> &[u8] {
        self
    }
}

impl VectorInput for Vec<u8> {
    fn datatype(&self) -> VectorDataType {
        VectorDataType::Float32
    }
    fn dimension(&self) -> u32 {
        (self.len() / 4) as u32
    }
    fn payload(&self) -> &[u8] {
        self
    }
}

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[repr(u8)]
pub enum VectorDataType {
    #[default]
    #[serde(rename = "Float32")]
    Float32 = 0,
    #[serde(rename = "Float16")]
    Float16 = 1,
    #[serde(rename = "BFloat16")]
    BFloat16 = 2,
    #[serde(rename = "Int8")]
    Int8 = 3,
    #[serde(rename = "Binary")]
    Binary = 4,
}

impl VectorDataType {
    pub fn parse(name: &str) -> Option<Self> {
        let name = name.trim();
        match name.len() {
            4 if name.eq_ignore_ascii_case("INT8") => Some(Self::Int8),
            6 if name.eq_ignore_ascii_case("BINARY") => Some(Self::Binary),
            7 if name.eq_ignore_ascii_case("FLOAT32") => Some(Self::Float32),
            7 if name.eq_ignore_ascii_case("FLOAT16") => Some(Self::Float16),
            8 if name.eq_ignore_ascii_case("BFLOAT16") => Some(Self::BFloat16),
            _ => None,
        }
    }

    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Float32),
            1 => Some(Self::Float16),
            2 => Some(Self::BFloat16),
            3 => Some(Self::Int8),
            4 => Some(Self::Binary),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Float32 => "FLOAT32",
            Self::Float16 => "FLOAT16",
            Self::BFloat16 => "BFLOAT16",
            Self::Int8 => "INT8",
            Self::Binary => "BINARY",
        }
    }

    pub fn payload_len(self, dimension: u32) -> usize {
        let dimension = dimension as usize;
        match self {
            Self::Float32 => dimension * 4,
            Self::Float16 | Self::BFloat16 => dimension * 2,
            Self::Int8 => dimension,
            Self::Binary => dimension.div_ceil(8),
        }
    }

    pub fn max_dimension(self) -> u32 {
        match self {
            Self::Float32 => (MAX_VECTOR_PAYLOAD / 4) as u32,
            Self::Float16 | Self::BFloat16 => (MAX_VECTOR_PAYLOAD / 2) as u32,
            Self::Int8 => MAX_VECTOR_PAYLOAD as u32,
            Self::Binary => (MAX_VECTOR_PAYLOAD * 8) as u32,
        }
    }
}

impl std::fmt::Display for VectorDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VectorValue {
    pub datatype: VectorDataType,
    pub dimension: u32,
    pub payload: Vec<u8>,
}

impl VectorValue {
    pub fn new(datatype: VectorDataType, dimension: u32, payload: Vec<u8>) -> Self {
        Self {
            datatype,
            dimension,
            payload,
        }
    }

    #[inline]
    pub fn element(&self, i: usize) -> f64 {
        match self.datatype {
            VectorDataType::Float32 => {
                f32::from_le_bytes(self.payload[i * 4..i * 4 + 4].try_into().unwrap()) as f64
            }
            VectorDataType::Float16 => {
                half::f16::from_le_bytes(self.payload[i * 2..i * 2 + 2].try_into().unwrap())
                    .to_f64()
            }
            VectorDataType::BFloat16 => {
                half::bf16::from_le_bytes(self.payload[i * 2..i * 2 + 2].try_into().unwrap())
                    .to_f64()
            }
            VectorDataType::Int8 => (self.payload[i] as i8) as f64,
            VectorDataType::Binary => ((self.payload[i / 8] >> (i % 8)) & 1) as f64,
        }
    }

    pub fn replace_payload(&mut self, payload: Vec<u8>) {
        self.payload = payload;
    }

    /// Standalone values carry type information; fixed heap lanes use only payload.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12 + self.payload.len());
        out.extend_from_slice(&[
            crate::storage::format_versions::NATIVE_VECTOR_VALUE_VERSION,
            self.datatype as u8,
            0,
            0,
        ]);
        out.extend_from_slice(&self.dimension.to_le_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 12
            || bytes[0] != crate::storage::format_versions::NATIVE_VECTOR_VALUE_VERSION
            || bytes[2..4] != [0, 0]
        {
            return None;
        }
        let datatype = VectorDataType::from_tag(bytes[1])?;
        let dimension = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
        let length = u32::from_le_bytes(bytes[8..12].try_into().ok()?) as usize;
        if dimension == 0
            || dimension > datatype.max_dimension()
            || length != datatype.payload_len(dimension)
            || bytes.len() - 12 != length
        {
            return None;
        }
        Some(Self::new(datatype, dimension, bytes[12..].to_vec()))
    }
}

impl From<Vec<u8>> for VectorValue {
    fn from(payload: Vec<u8>) -> Self {
        Self::new(VectorDataType::Float32, (payload.len() / 4) as u32, payload)
    }
}

impl Deref for VectorValue {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl DerefMut for VectorValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.payload
    }
}

impl<const N: usize> VectorInput for [u8; N] {
    fn datatype(&self) -> VectorDataType {
        VectorDataType::Float32
    }
    fn dimension(&self) -> u32 {
        (N / 4) as u32
    }
    fn payload(&self) -> &[u8] {
        self
    }
}
impl FromIterator<u8> for VectorValue {
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        Vec::from_iter(iter).into()
    }
}

/// Semantic text parameters affecting a vector result's datatype or dimension.
pub(crate) fn text_parameter_shape(bytes: &[u8]) -> Option<(u8, u32)> {
    let bytes = std::str::from_utf8(bytes).ok()?.trim().as_bytes();
    if bytes.first() == Some(&b'[') && bytes.last() == Some(&b']') {
        return Some((2, (bytes.iter().filter(|&&b| b == b',').count() + 1) as u32));
    }
    let datatype = VectorDataType::parse(std::str::from_utf8(bytes).ok()?)?;
    Some((1, datatype as u32))
}
