//! MySQL 8.0-compatible partition table support.
//!
//! Each partition is a separate physical sub-table that uses the same
//! [`TableSchema`] (columns, indexes, constraints, defaults) as the
//! logical table. The partition has its own `storage_id` allocated from
//! the catalog's `next_table_id` counter, so:
//!
//! - heap file lives at `tables/{partition_storage_id}.heap`
//! - per-partition local indexes at `indexes/{partition_storage_id}_{name}.idx`
//!
//! The existing storage APIs (`heap_handle`, `index_handle`,
//! `with_heap_mut`, etc.) continue to take a single `u64` and remain
//! oblivious to partitioning. Partition-aware routing lives one level
//! above, in the DML / scan dispatch layer.
//!
//! `TableSchema.id` keeps acting as the catalog identifier; for an
//! unpartitioned table it doubles as the storage id. For a partitioned
//! table no heap file exists at `tables/{table.id}.heap` — the logical
//! table id only names the catalog entry.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};

use crate::parser::Expr;
use crate::types::{QueryError, Value};

/// How rows are mapped to partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionMethod {
    /// `PARTITION BY RANGE (expr)` — single integer-valued expression.
    Range,
    /// `PARTITION BY RANGE COLUMNS (c1, c2, ...)` — multi-column
    /// lexicographic compare against `VALUES LESS THAN (...)`.
    RangeColumns,
    /// `PARTITION BY LIST (expr)` — single integer-valued expression.
    List,
    /// `PARTITION BY LIST COLUMNS (c1, c2, ...)` — multi-column tuple
    /// `VALUES IN ((...), (...))`.
    ListColumns,
    /// `PARTITION BY HASH (expr) PARTITIONS n`.
    Hash,
    /// `PARTITION BY LINEAR HASH (expr) PARTITIONS n` — same partition
    /// function as `Hash` but uses MySQL's power-of-two linear hashing
    /// so `COALESCE PARTITION` does not need a full reshuffle.
    LinearHash,
    /// `PARTITION BY KEY (col_list) PARTITIONS n`.
    Key,
    /// `PARTITION BY LINEAR KEY (col_list) PARTITIONS n`.
    LinearKey,
}

impl PartitionMethod {
    pub fn is_range(self) -> bool {
        matches!(self, Self::Range | Self::RangeColumns)
    }

    pub fn is_list(self) -> bool {
        matches!(self, Self::List | Self::ListColumns)
    }

    pub fn is_hash_like(self) -> bool {
        matches!(
            self,
            Self::Hash | Self::LinearHash | Self::Key | Self::LinearKey
        )
    }

    pub fn uses_columns(self) -> bool {
        matches!(
            self,
            Self::RangeColumns | Self::ListColumns | Self::Key | Self::LinearKey
        )
    }

    /// MySQL SHOW CREATE TABLE keyword form.
    pub fn keyword(self) -> &'static str {
        match self {
            Self::Range => "RANGE",
            Self::RangeColumns => "RANGE COLUMNS",
            Self::List => "LIST",
            Self::ListColumns => "LIST COLUMNS",
            Self::Hash => "HASH",
            Self::LinearHash => "LINEAR HASH",
            Self::Key => "KEY",
            Self::LinearKey => "LINEAR KEY",
        }
    }
}

/// A single literal in a partition `VALUES LESS THAN (...)` /
/// `VALUES IN (...)` clause. `Value::Null` is rejected at CREATE time
/// for RANGE because MySQL forbids NULL as a range boundary, but
/// allowed for LIST (where it acts as a singleton partition
/// containing NULL).
///
/// `MaxValue` is the open-ended upper bound for `VALUES LESS THAN
/// MAXVALUE`; only legal in RANGE bounds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PartitionLiteral {
    Value(Value),
    MaxValue,
}

impl PartitionLiteral {
    pub fn is_max_value(&self) -> bool {
        matches!(self, Self::MaxValue)
    }

    pub fn as_value(&self) -> Option<&Value> {
        match self {
            Self::Value(v) => Some(v),
            Self::MaxValue => None,
        }
    }
}

/// Per-partition bound expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PartitionBounds {
    /// `VALUES LESS THAN (literal_1, literal_2, ...)`. Single-element for
    /// plain `RANGE`, multi-element for `RANGE COLUMNS`. Any element may
    /// be `MaxValue`.
    RangeLessThan(Vec<PartitionLiteral>),
    /// `VALUES IN ((tuple), (tuple), ...)`. Each tuple has one element
    /// for plain `LIST`, multi-element for `LIST COLUMNS`.
    ListIn(Vec<Vec<PartitionLiteral>>),
    /// Bucket position for HASH/KEY-family partitions. The partition's
    /// position in the `partitions` vector is the bucket index; this
    /// variant carries no extra state.
    Hash,
}

/// Legacy catalog shape for per-(sub)partition placement options. New DDL
/// rejects placement/sizing options it cannot honor; valid engine aliases are
/// checked but not persisted.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct PartitionPhysicalOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_directory: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_directory: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tablespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nodegroup: Option<u32>,
}

impl PartitionPhysicalOptions {
    pub fn is_empty(&self) -> bool {
        self.data_directory.is_none()
            && self.index_directory.is_none()
            && self.tablespace.is_none()
            && self.engine.is_none()
            && self.max_rows.is_none()
            && self.min_rows.is_none()
            && self.nodegroup.is_none()
    }
}

/// One persisted partition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Partition {
    pub name: String,
    /// Physical heap id for an ordinary leaf partition. Catalog v42 stores
    /// `0` for a logical parent that owns subpartitions; its physical ids live
    /// exclusively in `subpartitions`.
    pub storage_id: u64,
    pub bounds: PartitionBounds,
    #[serde(default)]
    pub comment: String,
    /// Legacy catalog field; new DDL validates and discards engine aliases
    /// and rejects every unsupported physical placement option.
    #[serde(default, skip_serializing_if = "PartitionPhysicalOptions::is_empty")]
    pub options: PartitionPhysicalOptions,
    /// Leaf subpartitions under this top-level partition. Empty for
    /// ordinary one-dimensional partitioning.
    #[serde(default)]
    pub subpartitions: Vec<Subpartition>,
}

/// One persisted leaf subpartition. MySQL subpartitioning is a
/// two-dimensional layout: RANGE/LIST chooses the parent partition,
/// then HASH/KEY chooses one leaf storage id below it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Subpartition {
    pub name: String,
    pub storage_id: u64,
    #[serde(default)]
    pub comment: String,
    /// Legacy catalog field; empty for catalog v42 DDL.
    #[serde(default, skip_serializing_if = "PartitionPhysicalOptions::is_empty")]
    pub options: PartitionPhysicalOptions,
}

/// One physical storage leaf derived from the logical partition descriptor.
/// This is the only partition-level object that may carry a storage id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionLeaf {
    /// Ordinal of the logical parent in `PartitionInfo::partitions`.
    pub parent_ordinal: usize,
    /// Present for two-level partitioning; `None` means the parent itself is
    /// the physical leaf of a one-level partitioned table.
    pub subpartition_name: Option<String>,
    pub storage_id: u64,
}

/// Explicit physical layout of a logical table. The value is derived from the
/// catalog descriptor when a schema snapshot is built, keeping logical table
/// identity separate from every heap/index/LOB storage id used by execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableStorageLayout {
    Single { storage_id: u64 },
    Partitioned { leaves: Vec<PartitionLeaf> },
}

impl TableStorageLayout {
    #[cfg(test)]
    pub fn storage_ids(&self) -> Vec<u64> {
        match self {
            Self::Single { storage_id } => vec![*storage_id],
            Self::Partitioned { leaves } => leaves.iter().map(|leaf| leaf.storage_id).collect(),
        }
    }
}

/// Compact physical-leaf bitmap shared by planner and every scan path. An
/// empty set is a first-class value and therefore never aliases `All`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSet {
    leaf_count: usize,
    words: Vec<u64>,
}

impl PartitionSet {
    pub fn empty(leaf_count: usize) -> Self {
        Self {
            leaf_count,
            words: vec![0; leaf_count.div_ceil(64)],
        }
    }

    pub fn from_ordinals(leaf_count: usize, ordinals: impl IntoIterator<Item = usize>) -> Self {
        let mut set = Self::empty(leaf_count);
        for ordinal in ordinals {
            if ordinal < leaf_count {
                set.words[ordinal / 64] |= 1u64 << (ordinal % 64);
            }
        }
        set
    }

    pub fn contains(&self, ordinal: usize) -> bool {
        ordinal < self.leaf_count && self.words[ordinal / 64] & (1u64 << (ordinal % 64)) != 0
    }

    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|word| *word == 0)
    }

    pub fn len(&self) -> usize {
        self.words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }

    pub fn ordinals(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.leaf_count).filter(|ordinal| self.contains(*ordinal))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionPruningPlan {
    All,
    Static(PartitionSet),
    /// Re-evaluate placeholders against execution parameters. This changes
    /// only the leaf bitmap, never cached join order or worker topology.
    Program,
}

/// Table-level `SUBPARTITION BY ...` descriptor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubpartitioningInfo {
    pub method: PartitionMethod,
    /// For HASH / LINEAR HASH subpartitioning.
    #[serde(default)]
    pub expr_sql: String,
    /// For KEY / LINEAR KEY subpartitioning.
    #[serde(default)]
    pub columns: Vec<String>,
    /// Optional `ALGORITHM=N` for KEY variants.
    #[serde(default)]
    pub key_algorithm: Option<u8>,
    /// Declared `SUBPARTITIONS n` count when present.
    #[serde(default)]
    pub count: Option<u32>,
}

/// Non-persistent lookup state derived from one catalog partition descriptor.
/// The reservation is owned by the same `Arc` as the lookup map, so accounting
/// is released only after the last schema snapshot drops it.
#[derive(Debug)]
pub(crate) struct PartitionRuntime {
    pub(crate) list_lookup: HashMap<Vec<u8>, usize>,
    pub(crate) _reservation: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

/// Allocation-accounted catalog lookup maps shared by routing, explicit
/// partition selection, DDL and maintenance paths. Kept separate from the
/// collation-aware LIST map because these maps can be built without a table
/// row schema.
#[derive(Debug)]
pub(crate) struct PartitionMetadataRuntime {
    name_to_storage_ids: HashMap<String, Vec<u64>>,
    storage_to_parent: HashMap<u64, usize>,
    _reservation: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

/// Per-table partitioning metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub method: PartitionMethod,
    /// For RANGE/LIST/HASH: canonical SQL text of the partition
    /// expression. Empty for `RangeColumns` / `ListColumns` / `Key`
    /// variants which list columns directly.
    #[serde(default)]
    pub expr_sql: String,
    /// For `RangeColumns` / `ListColumns` / `Key` variants: declared
    /// column names. Empty for the expression-based variants.
    #[serde(default)]
    pub columns: Vec<String>,
    /// For KEY variants — the optional `ALGORITHM=N` clause MySQL
    /// supports. `1` selects the MySQL 5.1 compatibility algorithm;
    /// `2` is the MySQL 5.5+ default algorithm.
    #[serde(default)]
    pub key_algorithm: Option<u8>,
    /// Ordered partitions; for HASH/KEY the position is the bucket
    /// index. For RANGE/RANGE COLUMNS the entries are stored in
    /// declared order — also their ascending bound order, since MySQL
    /// requires RANGE bounds to be monotonically increasing.
    pub partitions: Vec<Partition>,
    /// Optional `SUBPARTITION BY HASH/KEY` metadata. When present,
    /// `partitions[*].subpartitions` hold the leaf storage ids.
    #[serde(default)]
    pub subpartitioning: Option<SubpartitioningInfo>,
    /// Parsed expression (rebuilt on catalog load). Skipped on disk.
    #[serde(skip)]
    pub expr_ast: Option<Expr>,
    /// Compiled program over the table row (rebuilt on catalog load).
    #[serde(skip)]
    pub(crate) expr_program: Option<Arc<crate::engine::expr_program::CompiledTableExpr>>,
    /// Parsed subpartition expression (rebuilt on catalog load).
    #[serde(skip)]
    pub sub_expr_ast: Option<Expr>,
    /// Compiled subpartition program over the table row.
    #[serde(skip)]
    pub(crate) sub_expr_program: Option<Arc<crate::engine::expr_program::CompiledTableExpr>>,
    /// Optional derived cache. `None` inside the OnceLock means governor
    /// admission failed and callers must use the allocation-free fallback.
    #[serde(skip)]
    pub(crate) runtime: OnceLock<Option<Arc<PartitionRuntime>>>,
    #[serde(skip)]
    pub(crate) metadata_runtime: OnceLock<Option<Arc<PartitionMetadataRuntime>>>,
}

impl PartitionInfo {
    #[cfg(test)]
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn storage_ids(&self) -> Vec<u64> {
        let leaf_count: usize = self.partitions.iter().map(|p| p.subpartitions.len()).sum();
        if leaf_count == 0 {
            return self.partitions.iter().map(|p| p.storage_id).collect();
        }
        let mut ids = Vec::with_capacity(leaf_count);
        for partition in &self.partitions {
            ids.extend(partition.subpartitions.iter().map(|sp| sp.storage_id));
        }
        ids
    }

    pub fn leaves(&self) -> Vec<PartitionLeaf> {
        let leaf_count = self
            .partitions
            .iter()
            .map(|partition| partition.subpartitions.len().max(1))
            .sum();
        let mut leaves = Vec::with_capacity(leaf_count);
        for (parent_ordinal, partition) in self.partitions.iter().enumerate() {
            if partition.subpartitions.is_empty() {
                leaves.push(PartitionLeaf {
                    parent_ordinal,
                    subpartition_name: None,
                    storage_id: partition.storage_id,
                });
            } else {
                leaves.extend(
                    partition
                        .subpartitions
                        .iter()
                        .map(|subpartition| PartitionLeaf {
                            parent_ordinal,
                            subpartition_name: Some(subpartition.name.clone()),
                            storage_id: subpartition.storage_id,
                        }),
                );
            }
        }
        leaves
    }

    /// Resolve either a top-level partition name or a leaf subpartition
    /// name to the physical storage ids it denotes. MySQL's
    /// `PARTITION(p0)` targets all subpartitions under `p0`, while a
    /// subpartition name targets only that leaf.
    pub fn storage_ids_for_name(&self, name: &str) -> Option<Vec<u64>> {
        if let Some(runtime) = self.metadata_runtime() {
            return runtime
                .name_to_storage_ids
                .get(&name.to_ascii_lowercase())
                .cloned();
        }
        for partition in &self.partitions {
            if partition.name.eq_ignore_ascii_case(name) {
                if partition.subpartitions.is_empty() {
                    return Some(vec![partition.storage_id]);
                }
                return Some(
                    partition
                        .subpartitions
                        .iter()
                        .map(|sp| sp.storage_id)
                        .collect(),
                );
            }
            if let Some(subpartition) = partition
                .subpartitions
                .iter()
                .find(|sp| sp.name.eq_ignore_ascii_case(name))
            {
                return Some(vec![subpartition.storage_id]);
            }
        }
        None
    }

    /// Physical leaf storage ids belonging to a top-level partition
    /// index.
    pub fn storage_ids_for_partition_index(&self, index: usize) -> Vec<u64> {
        let Some(partition) = self.partitions.get(index) else {
            return Vec::new();
        };
        if partition.subpartitions.is_empty() {
            vec![partition.storage_id]
        } else {
            partition
                .subpartitions
                .iter()
                .map(|sp| sp.storage_id)
                .collect()
        }
    }

    pub fn find_by_storage_id(&self, storage_id: u64) -> Option<(usize, &Partition)> {
        if let Some(parent) = self
            .metadata_runtime()
            .and_then(|runtime| runtime.storage_to_parent.get(&storage_id).copied())
        {
            return self
                .partitions
                .get(parent)
                .map(|partition| (parent, partition));
        }
        self.partitions.iter().enumerate().find(|(_, p)| {
            (p.subpartitions.is_empty() && p.storage_id == storage_id)
                || p.subpartitions.iter().any(|sp| sp.storage_id == storage_id)
        })
    }

    /// Whether `storage_id` is one of this table's partitions.
    pub fn owns_storage_id(&self, storage_id: u64) -> bool {
        if let Some(runtime) = self.metadata_runtime() {
            return runtime.storage_to_parent.contains_key(&storage_id);
        }
        self.find_by_storage_id(storage_id).is_some()
    }

    fn metadata_runtime(&self) -> Option<&PartitionMetadataRuntime> {
        self.metadata_runtime
            .get_or_init(|| self.build_metadata_runtime())
            .as_deref()
    }

    fn build_metadata_runtime(&self) -> Option<Arc<PartitionMetadataRuntime>> {
        let leaf_count = self
            .partitions
            .iter()
            .map(|partition| partition.subpartitions.len().max(1))
            .sum::<usize>();
        let name_count = self
            .partitions
            .iter()
            .map(|partition| 1usize.saturating_add(partition.subpartitions.len()))
            .sum::<usize>();
        let name_bytes = self
            .partitions
            .iter()
            .map(|partition| {
                partition.name.capacity()
                    + partition
                        .subpartitions
                        .iter()
                        .map(|subpartition| subpartition.name.capacity())
                        .sum::<usize>()
            })
            .sum::<usize>();
        let estimated = name_count
            .saturating_mul(std::mem::size_of::<(String, Vec<u64>)>().saturating_mul(2))
            .saturating_add(
                leaf_count.saturating_mul(std::mem::size_of::<(u64, usize)>().saturating_mul(2)),
            )
            .saturating_add(name_bytes)
            .saturating_add(4096);
        let mut reservation =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::DerivedCache,
                estimated,
            )?;
        let mut names = HashMap::with_capacity(name_count);
        let mut storage_to_parent = HashMap::with_capacity(leaf_count);
        for (parent_ordinal, partition) in self.partitions.iter().enumerate() {
            let storage_ids = if partition.subpartitions.is_empty() {
                vec![partition.storage_id]
            } else {
                partition
                    .subpartitions
                    .iter()
                    .map(|subpartition| subpartition.storage_id)
                    .collect::<Vec<_>>()
            };
            for storage_id in &storage_ids {
                storage_to_parent.insert(*storage_id, parent_ordinal);
            }
            names.insert(partition.name.to_ascii_lowercase(), storage_ids);
            for subpartition in &partition.subpartitions {
                names.insert(
                    subpartition.name.to_ascii_lowercase(),
                    vec![subpartition.storage_id],
                );
            }
        }
        let retained = names
            .capacity()
            .saturating_mul(std::mem::size_of::<(String, Vec<u64>)>().saturating_mul(2))
            .saturating_add(
                names
                    .iter()
                    .map(|(name, ids)| name.capacity().saturating_add(ids.capacity() * 8))
                    .sum::<usize>(),
            )
            .saturating_add(
                storage_to_parent
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(u64, usize)>().saturating_mul(2)),
            );
        if retained > reservation.bytes()
            && reservation
                .try_grow(retained.saturating_sub(reservation.bytes()))
                .is_err()
        {
            return None;
        }
        Some(Arc::new(PartitionMetadataRuntime {
            name_to_storage_ids: names,
            storage_to_parent,
            _reservation: reservation,
        }))
    }
}

/// Outcome of evaluating the partition function on a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutedPartition {
    /// Index into `PartitionInfo::partitions`.
    pub index: usize,
    /// Convenience copy of that partition's `storage_id`.
    pub storage_id: u64,
}

/// Error raised when a row's partition value does not match any
/// declared RANGE/LIST bound. MySQL reports `1526 (HY000) Table has
/// no partition for value …`. We surface the same code and SQLSTATE.
pub fn no_partition_for_value(detail: impl AsRef<str>) -> QueryError {
    crate::storage::error(
        crate::error::catalog::ER_NO_PARTITION_FOR_GIVEN_VALUE,
        format!("Table has no partition for value {}", detail.as_ref()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_info_storage_ids_use_leaf_subpartitions_when_present() {
        let info = PartitionInfo {
            method: PartitionMethod::Range,
            expr_sql: "y".to_string(),
            columns: Vec::new(),
            key_algorithm: None,
            partitions: vec![
                Partition {
                    name: "p0".to_string(),
                    storage_id: 10,
                    bounds: PartitionBounds::RangeLessThan(vec![PartitionLiteral::Value(
                        Value::Int(10),
                    )]),
                    comment: String::new(),
                    options: PartitionPhysicalOptions::default(),
                    subpartitions: vec![
                        Subpartition {
                            name: "p0sp0".to_string(),
                            storage_id: 101,
                            comment: String::new(),
                            options: PartitionPhysicalOptions::default(),
                        },
                        Subpartition {
                            name: "p0sp1".to_string(),
                            storage_id: 102,
                            comment: String::new(),
                            options: PartitionPhysicalOptions::default(),
                        },
                    ],
                },
                Partition {
                    name: "p1".to_string(),
                    storage_id: 20,
                    bounds: PartitionBounds::RangeLessThan(vec![PartitionLiteral::MaxValue]),
                    comment: String::new(),
                    options: PartitionPhysicalOptions::default(),
                    subpartitions: vec![Subpartition {
                        name: "p1sp0".to_string(),
                        storage_id: 201,
                        comment: String::new(),
                        options: PartitionPhysicalOptions::default(),
                    }],
                },
            ],
            subpartitioning: Some(SubpartitioningInfo {
                method: PartitionMethod::Hash,
                expr_sql: "id".to_string(),
                columns: Vec::new(),
                key_algorithm: None,
                count: Some(2),
            }),
            expr_ast: None,
            expr_program: None,
            sub_expr_ast: None,
            sub_expr_program: None,
            runtime: OnceLock::new(),
            metadata_runtime: OnceLock::new(),
        };

        assert_eq!(info.storage_ids(), vec![101, 102, 201]);
        assert_eq!(
            info.leaves(),
            vec![
                PartitionLeaf {
                    parent_ordinal: 0,
                    subpartition_name: Some("p0sp0".to_string()),
                    storage_id: 101,
                },
                PartitionLeaf {
                    parent_ordinal: 0,
                    subpartition_name: Some("p0sp1".to_string()),
                    storage_id: 102,
                },
                PartitionLeaf {
                    parent_ordinal: 1,
                    subpartition_name: Some("p1sp0".to_string()),
                    storage_id: 201,
                },
            ]
        );
        assert!(info.owns_storage_id(102));
        assert!(info.find_by_storage_id(201).is_some());
        assert!(!info.owns_storage_id(20));
    }

    #[test]
    fn partition_info_deserializes_without_subpartition_fields() {
        let json = r#"{
            "method": "Hash",
            "expr_sql": "id",
            "columns": [],
            "key_algorithm": null,
            "partitions": [
                {
                    "name": "p0",
                    "storage_id": 42,
                    "bounds": "Hash",
                    "comment": ""
                }
            ]
        }"#;
        let info: PartitionInfo = serde_json::from_str(json).unwrap();
        assert!(info.subpartitioning.is_none());
        assert!(info.partitions[0].subpartitions.is_empty());
        assert_eq!(info.storage_ids(), vec![42]);
    }

    #[test]
    fn partition_options_default_when_absent_from_old_catalog() {
        // M3: a pre-existing catalog (no `options` field) must deserialize
        // with empty options — no CATALOG_VERSION bump required.
        let json = r#"{
            "method": "Range",
            "expr_sql": "y",
            "columns": [],
            "key_algorithm": null,
            "partitions": [
                {
                    "name": "p0",
                    "storage_id": 7,
                    "bounds": {"RangeLessThan": [{"Value": {"Int": 10}}]},
                    "comment": "",
                    "subpartitions": [
                        {"name": "p0sp0", "storage_id": 8, "comment": ""}
                    ]
                }
            ]
        }"#;
        let info: PartitionInfo = serde_json::from_str(json).unwrap();
        assert!(info.partitions[0].options.is_empty());
        assert!(info.partitions[0].subpartitions[0].options.is_empty());

        // And a round-trip of populated options preserves every field.
        let opts = PartitionPhysicalOptions {
            data_directory: Some("/data/p0".to_string()),
            tablespace: Some("ts1".to_string()),
            max_rows: Some(1000),
            nodegroup: Some(3),
            ..Default::default()
        };
        let encoded = serde_json::to_string(&opts).unwrap();
        let decoded: PartitionPhysicalOptions = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, opts);
        assert!(!decoded.is_empty());
    }
}

/// A7: strict integer coercion for HASH/LINEAR HASH bucket selection.
/// The partition expression is DDL-guaranteed integer (see
/// `expr_partition_field_verdict` and `partition_expr_is_int_valued`),
/// so any non-integer arrival is a bug in an earlier layer — reject it
/// with 1697 rather than silently `as i128`-casting Float/Double or
/// dropping DECIMAL scale to bucket 0. The Null → 0 mapping matches
/// MySQL 8.0 (NULL routes to the first partition for HASH).
fn value_to_hash_integer_strict(value: &Value) -> Result<i128, QueryError> {
    Ok(match value {
        Value::Null => 0,
        Value::Int(v) => *v as i128,
        Value::UInt(v) => *v as i128,
        Value::Year(y) => *y as i128,
        Value::Date(d) => *d as i128,
        Value::Time(t) => *t as i128,
        Value::DateTime(t) => *t as i128,
        Value::Timestamp(t) => *t as i128,
        _ => {
            return Err(crate::storage::error(
                crate::error::catalog::ER_VALUES_IS_NOT_INT_TYPE_ERROR,
                "VALUES value for partition must have type INT",
            ))
        }
    })
}

/// A2: normalize DECIMAL for hashing purposes so that logically equal
/// values (`1.00` == `1.0` == `1`, `Decimal("1.00")` == `DecimalCompact(100,2)`
/// == `DecimalCompact(1,0)`) hash to the same u64. Both variants are folded
/// to `(canonical_mag_i128, canonical_scale_u8)` with trailing zeros stripped;
/// unparseable strings fall back to a byte-trim hash so at least whitespace
/// noise doesn't collide with content.
fn decimal_hash_bytes(mag: i128, scale: u8) -> u64 {
    let (m, s) = strip_trailing_zeros_decimal(mag, scale);
    let mut buf = [0u8; 17];
    buf[..16].copy_from_slice(&m.to_le_bytes());
    buf[16] = s;
    fnv_hash_bytes(&buf)
}

fn decimal_string_hash_bytes(s: &str) -> u64 {
    if let Some((mag, scale)) = crate::util::decimal::parse_exact_decimal_str(s) {
        return decimal_hash_bytes(mag, scale);
    }
    fnv_hash_bytes(s.trim().as_bytes())
}

fn strip_trailing_zeros_decimal(mag: i128, scale: u8) -> (i128, u8) {
    let mut m = mag;
    let mut s = scale;
    while s > 0 && m != 0 && m % 10 == 0 {
        m /= 10;
        s -= 1;
    }
    (m, s)
}

/// HASH (and LINEAR HASH up to the round-up step) bucket selection.
/// MySQL: bucket = MOD(expr, num_partitions). For LINEAR HASH the
/// caller follows up with the power-of-two rounding step.
///
/// A7: strict integer expected — returns 1697 if the runtime value is
/// not an integer type (DDL guarantees it will be).
pub fn hash_value_to_bucket(value: &Value, num_partitions: u32) -> Result<u32, QueryError> {
    if num_partitions == 0 {
        return Ok(0);
    }
    let raw = value_to_hash_integer_strict(value)?;
    let abs = raw.unsigned_abs();
    Ok((abs % num_partitions as u128) as u32)
}

/// Linear-hash variant: round up the partition count to the next
/// power of two `V`, take `bucket = expr & (V - 1)`, then iteratively
/// halve until the bucket fits within the declared partition count.
/// This is what makes ADD/COALESCE PARTITION cheap under LINEAR HASH:
/// only a small subset of rows actually move.
pub fn linear_hash_to_bucket(value: &Value, num_partitions: u32) -> Result<u32, QueryError> {
    if num_partitions == 0 {
        return Ok(0);
    }
    let raw = value_to_hash_integer_strict(value)? as u64;
    let mut v: u64 = num_partitions as u64;
    // Round v up to next power of two.
    if v.is_power_of_two() {
    } else {
        v = v.next_power_of_two();
    }
    let mut bucket = (raw & (v - 1)) as u64;
    let mut step = v;
    while bucket >= num_partitions as u64 {
        step /= 2;
        if step == 0 {
            bucket = 0;
            break;
        }
        bucket -= step;
    }
    Ok(bucket as u32)
}

/// KEY hash over the declared column values. Even single-column KEY
/// goes through the composed FNV-1a kernel — this is A2's fix:
/// delegating single-column KEY to `hash_value_to_bucket` sent every
/// DECIMAL value to bucket 0 (because that path required integer
/// input). KEY partitioning supports every scalar column type MySQL
/// supports (except TEXT/BLOB/JSON/Geometry/Vector, which DDL rejects)
/// and its hash must be data-preserving for all of them.
pub fn key_hash_bucket(values: &[Value], num_partitions: u32) -> u32 {
    if num_partitions == 0 {
        return 0;
    }
    let mut hasher: u64 = 0xCBF29CE484222325; // FNV-1a 64 offset basis.
    for value in values {
        let h = fnv_hash_value(value);
        hasher ^= h;
        hasher = hasher.wrapping_mul(0x00000100000001B3);
    }
    (hasher % num_partitions as u64) as u32
}

pub fn linear_key_bucket(values: &[Value], num_partitions: u32) -> u32 {
    if num_partitions == 0 {
        return 0;
    }
    let mut h: u64 = 0xCBF29CE484222325;
    for v in values {
        h ^= fnv_hash_value(v);
        h = h.wrapping_mul(0x00000100000001B3);
    }
    let raw = h;
    let mut v: u64 = num_partitions as u64;
    if !v.is_power_of_two() {
        v = v.next_power_of_two();
    }
    let mut bucket = raw & (v - 1);
    let mut step = v;
    while bucket >= num_partitions as u64 {
        step /= 2;
        if step == 0 {
            bucket = 0;
            break;
        }
        bucket -= step;
    }
    bucket as u32
}

fn fnv_hash_value(value: &Value) -> u64 {
    match value {
        Value::Null => 0xCBF29CE484222325,
        Value::Int(v) => *v as u64,
        Value::UInt(v) => *v,
        Value::Float(v) => v.to_bits() as u64,
        Value::Double(v) => v.to_bits(),
        // A2: normalize DECIMAL so `1.00` == `1.0` == `1` hash the same
        // regardless of the surface representation (String vs compact).
        Value::Decimal(s) => decimal_string_hash_bytes(s),
        Value::DecimalCompact(mag, scale) => decimal_hash_bytes(*mag as i128, *scale),
        Value::Decimal128(mag, scale) => decimal_hash_bytes(*mag, *scale),
        Value::DecimalWide(value) => decimal_string_hash_bytes(
            &crate::util::decimal::format_big_decimal_parts(&value.magnitude, value.scale),
        ),
        Value::Year(y) => *y as u64,
        Value::Date(d) => *d as u64,
        Value::Time(t) => *t as u64,
        Value::DateTime(t) => *t as u64,
        Value::Timestamp(t) => *t as u64,
        Value::String(s) => fnv_hash_bytes(s.as_bytes()),
        Value::SmallString(b) => fnv_hash_bytes(b.as_slice()),
        Value::Bytes(b) => fnv_hash_bytes(b),
        Value::Json(b) => fnv_hash_bytes(b),
        Value::Geometry(b) => fnv_hash_bytes(b),
        Value::Vector(b) => fnv_hash_bytes(b),
    }
}

fn fnv_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xCBF29CE484222325;
    for b in bytes {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x00000100000001B3);
    }
    hash
}

/// Lexicographic compare of a row's tuple against a RANGE COLUMNS
/// boundary. Returns `Ordering::Less` if `row < bound`.
pub fn compare_row_to_range_bound(row: &[Value], bound: &[PartitionLiteral]) -> std::cmp::Ordering {
    compare_row_to_range_bound_with_collations(row, bound, &[])
}

/// A3: collation-aware variant used by the partition router when the
/// table's column context is available. `collations[i]` is the column
/// collation name for position `i` (empty slice / None means "no
/// collation info" — fall back to bytewise `compare_value`). Used for
/// RANGE COLUMNS on CHAR/VARCHAR columns so 'ABC' and 'abc' route to
/// the same partition under `utf8mb4_0900_ai_ci`.
pub fn compare_row_to_range_bound_with_collations(
    row: &[Value],
    bound: &[PartitionLiteral],
    collations: &[Option<&str>],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let len = row.len().min(bound.len());
    for i in 0..len {
        let r = &row[i];
        let b = &bound[i];
        match b {
            PartitionLiteral::MaxValue => return Ordering::Less,
            PartitionLiteral::Value(bv) => {
                let coll = collations.get(i).copied().flatten();
                let ord = compare_value_with_collation(r, bv, coll);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
        }
    }
    Ordering::Equal
}

/// A3: collation-aware value compare. When both sides are string-like
/// and a collation is provided, dispatch to `charset::compare_strings`
/// so ci/ai collations correctly equate `'ABC'` and `'abc'`. All other
/// value shapes fall through to `compare_value`'s pattern-match logic.
pub fn compare_value_with_collation(
    a: &Value,
    b: &Value,
    collation: Option<&str>,
) -> std::cmp::Ordering {
    let Some(c) = collation else {
        return compare_value(a, b);
    };
    fn as_str_view(v: &Value) -> Option<&str> {
        match v {
            Value::String(s) => Some(s.as_str()),
            Value::SmallString(b) => std::str::from_utf8(b.as_slice()).ok(),
            _ => None,
        }
    }
    match (as_str_view(a), as_str_view(b)) {
        (Some(x), Some(y)) => crate::charset::compare_strings(c, x, y),
        _ => compare_value(a, b),
    }
}

/// Compare two `Value` instances with MySQL's partition-comparison
/// semantics: NULL is less than every non-NULL.
///
/// A3/A4/A5 fixes: the previous `_ => Equal` fallback dropped several
/// legitimate cases to `Equal`, causing RANGE COLUMNS to route rows to
/// the MAXVALUE partition and LIST COLUMNS to reject valid rows. New
/// arms cover:
///   * `SmallString` vs `String` (row decoded to the inline optimization
///     while bounds are parsed as `String`) — bytewise identical means
///     equal, so promote and re-compare;
///   * `Bytes` vs `String` (BINARY column vs string literal bound);
///   * `Year` vs `Int`/`String` (RANGE COLUMNS(year_col) bounds).
///
/// Note: charset-aware collation comparison for CHAR/VARCHAR RANGE
/// COLUMNS/LIST COLUMNS is A3's separate fix — see
/// `compare_value_with_collation` used by the router with column context.
pub fn compare_value(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::UInt(x), Value::UInt(y)) => x.cmp(y),
        (Value::Int(x), Value::UInt(y)) => {
            if *x < 0 {
                Ordering::Less
            } else {
                (*x as u64).cmp(y)
            }
        }
        (Value::UInt(x), Value::Int(y)) => {
            if *y < 0 {
                Ordering::Greater
            } else {
                x.cmp(&(*y as u64))
            }
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bytes(x), Value::Bytes(y)) => x.cmp(y),
        // A4: SmallString ↔ String / Bytes cross-arms.
        (Value::SmallString(x), Value::SmallString(y)) => x.as_slice().cmp(y.as_slice()),
        (Value::SmallString(x), Value::String(y)) => x.as_slice().cmp(y.as_bytes()),
        (Value::String(x), Value::SmallString(y)) => x.as_bytes().cmp(y.as_slice()),
        (Value::SmallString(x), Value::Bytes(y)) => x.as_slice().cmp(y.as_slice()),
        (Value::Bytes(x), Value::SmallString(y)) => x.as_slice().cmp(y.as_slice()),
        (Value::String(x), Value::Bytes(y)) => x.as_bytes().cmp(y.as_slice()),
        (Value::Bytes(x), Value::String(y)) => x.as_slice().cmp(y.as_bytes()),
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Time(x), Value::Time(y)) => x.cmp(y),
        (Value::DateTime(x), Value::DateTime(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        (Value::Year(x), Value::Year(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Double(x), Value::Double(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Decimal(x), Value::Decimal(y)) => x.cmp(y),
        (Value::DecimalCompact(xm, xs), Value::DecimalCompact(ym, ys)) if xs == ys => xm.cmp(ym),
        // Numeric mixed widths: promote to f64 for comparison only.
        (Value::Int(x), Value::Double(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Double(y), Value::Int(x)) => y.partial_cmp(&(*x as f64)).unwrap_or(Ordering::Equal),
        (Value::Int(x), Value::Decimal(y)) | (Value::Decimal(y), Value::Int(x)) => {
            let ystr = y.parse::<i128>().unwrap_or(0);
            (*x as i128).cmp(&ystr)
        }
        // A5: Year vs Int literal. MySQL Year is stored as u8 offset from
        // 1900 (so 2020 → 120). Partition bounds like `LESS THAN (2020)`
        // arrive as `Value::Int(2020)`. Convert both sides to the full
        // year number and compare there.
        (Value::Year(y), Value::Int(x)) => {
            let year_full = 1900i64 + (*y as i64);
            year_full.cmp(x)
        }
        (Value::Int(x), Value::Year(y)) => {
            let year_full = 1900i64 + (*y as i64);
            x.cmp(&year_full)
        }
        // A5: Year vs 4-digit-year String literal ('2020').
        (Value::Year(y), Value::String(s)) => {
            let year_full = 1900i64 + (*y as i64);
            match s.trim().parse::<i64>() {
                Ok(x) => year_full.cmp(&x),
                Err(_) => Ordering::Equal,
            }
        }
        (Value::String(s), Value::Year(y)) => {
            let year_full = 1900i64 + (*y as i64);
            match s.trim().parse::<i64>() {
                Ok(x) => x.cmp(&year_full),
                Err(_) => Ordering::Equal,
            }
        }
        // A temporal row value vs an un-coerced partition bound literal
        // (RANGE COLUMNS(date/datetime/time) stores its bounds as string
        // literals like `'2020-01-01'`, while the row column decodes to a
        // `Value::Date`). Coerce the literal to the row's temporal kind
        // and compare.
        _ => compare_temporal_vs_literal(a, b).unwrap_or(Ordering::Equal),
    }
}

/// When exactly one side of a partition comparison is a temporal value and
/// the other is a raw literal (string / integer bound), coerce the literal to
/// the temporal kind and compare. Returns `None` when neither or both sides
/// are temporal (the caller then falls back to `Ordering::Equal`).
fn compare_temporal_vs_literal(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    let is_temporal = |v: &Value| {
        matches!(
            v,
            Value::Date(_)
                | Value::DateTime(_)
                | Value::Timestamp(_)
                | Value::Time(_)
                | Value::Year(_)
        )
    };
    let a_t = is_temporal(a);
    let b_t = is_temporal(b);
    if a_t == b_t {
        return None;
    }
    let (temporal, other, flip) = if a_t { (a, b, false) } else { (b, a, true) };
    let coerced = coerce_value_to_temporal_kind(temporal, other)?;
    let ord = compare_value(temporal, &coerced);
    Some(if flip { ord.reverse() } else { ord })
}

/// Parse `other` (a string / integer literal) into the same temporal
/// `Value` variant as `temporal`, reusing the storage temporal parsers so
/// zero sentinels and calendar validation match the DML path.
fn coerce_value_to_temporal_kind(temporal: &Value, other: &Value) -> Option<Value> {
    let s = match other {
        Value::String(s) | Value::Decimal(s) => s.clone(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Value::Int(_) | Value::UInt(_) => other.to_display_string(),
        _ => return None,
    };
    match temporal {
        Value::Date(_) => crate::storage::core::parse_date_to_days(&s)
            .ok()
            .map(Value::Date),
        Value::DateTime(_) => crate::storage::core::parse_datetime_to_micros(&s)
            .ok()
            .map(Value::DateTime),
        Value::Timestamp(_) => crate::storage::core::parse_datetime_to_micros(&s)
            .ok()
            .and_then(|wall| {
                crate::engine::expr::session_local_datetime_to_utc_timestamp_micros(wall).ok()
            })
            .map(Value::Timestamp),
        Value::Time(_) => crate::storage::core::parse_time_to_micros(&s)
            .ok()
            .map(Value::Time),
        // YEAR partition columns are rare (RANGE COLUMNS(year_col)); leave
        // them to the caller's Equal fallback rather than guess a pivot here.
        _ => None,
    }
}

/// Coerce a single literal into a `Value` whose enum variant matches
/// the storage shape used for that column. The parser produces
/// `PartitionLiteral::Value(Value::Int)` for unsuffixed integer
/// literals; if the partition column is a DATE/DATETIME/string we
/// re-coerce here so [`compare_value`] picks the right arm.
pub fn coerce_literal_for_column_type(
    lit: &PartitionLiteral,
    target: &crate::storage::DataTypeKind,
) -> PartitionLiteral {
    use crate::storage::DataTypeKind as K;
    let value = match lit {
        PartitionLiteral::MaxValue => return PartitionLiteral::MaxValue,
        PartitionLiteral::Value(v) => v,
    };
    let coerced = match (target, value) {
        (K::Date, Value::String(s)) => parse_date_value(s).unwrap_or_else(|| value.clone()),
        (K::DateTime { .. }, Value::String(s)) => {
            parse_datetime_value(s).unwrap_or_else(|| value.clone())
        }
        (K::Timestamp { .. }, Value::String(s)) => {
            parse_timestamp_value(s).unwrap_or_else(|| value.clone())
        }
        (K::Year, Value::Int(v)) if *v >= 1900 => Value::Year((*v - 1900) as u8),
        _ => value.clone(),
    };
    PartitionLiteral::Value(coerced)
}

fn parse_date_value(s: &str) -> Option<Value> {
    use chrono::{Datelike, NaiveDate};
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some(Value::Date(
        (date.num_days_from_ce() - epoch.num_days_from_ce()) as i32,
    ))
}

fn parse_datetime_value(s: &str) -> Option<Value> {
    use chrono::NaiveDateTime;
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
        })
        .ok()?;
    Some(Value::DateTime(dt.and_utc().timestamp_micros()))
}

fn parse_timestamp_value(s: &str) -> Option<Value> {
    let Value::DateTime(wall) = parse_datetime_value(s)? else {
        return None;
    };
    crate::engine::expr::session_local_datetime_to_utc_timestamp_micros(wall)
        .ok()
        .map(Value::Timestamp)
}
