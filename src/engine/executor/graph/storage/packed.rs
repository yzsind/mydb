//! Immutable property columns. A visible heap fragment proves membership, root
//! location and field values together. Changes retire the fragment through MVCC.
use super::*;
use crate::graph::ReadProjection;
use serde::{Deserialize, Serialize};

const DEFINITION: u64 = 7;
const FRAGMENT: u64 = 8;
const WIDTH: u64 = 1024;

#[derive(Clone, Serialize, Deserialize)]
struct Descriptor {
    generation: u64,
    spec: ReadProjection,
}

#[derive(Serialize, Deserialize)]
struct Fragment {
    offsets: Vec<u16>,
    // The low two bits encode the node/directed/undirected role.
    locations: Vec<u64>,
    columns: Vec<PackedColumn>,
}

#[derive(Serialize, Deserialize)]
enum PackedColumn {
    Integers(Vec<i64>),
    Scalars(Vec<Json>),
}

impl PackedColumn {
    fn get(&self, position: usize) -> Json {
        match self {
            Self::Integers(values) => Json::from(values[position]),
            Self::Scalars(values) => values[position].clone(),
        }
    }
}

type Definitions = Arc<[(u64, Descriptor)]>;
type BlockId = (u64, u64);
type RowProjection = (Arc<Fragment>, Arc<[String]>, usize);

#[derive(Default)]
pub(super) struct StatementProjection {
    definitions: Option<Definitions>,
    blocks: AhHashMap<BlockId, Vec<Arc<Fragment>>>,
    elements: AhHashMap<RowId, RowProjection>,
    charge: Option<super::super::super::mem_budget::ScopedCharge>,
}

impl StatementProjection {
    pub(super) fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
    pub(super) fn has_field(&self, row: RowId, field: &str) -> bool {
        self.elements
            .get(&row)
            .is_some_and(|(_, fields, _)| fields.iter().any(|f| f == field))
    }

    pub(super) fn property(&self, row: RowId, field: &str) -> Option<Json> {
        let (block, fields, position) = self.elements.get(&row)?;
        let field = fields.iter().position(|f| f == field)?;
        Some(block.columns[field].get(*position))
    }
}

#[derive(Default)]
pub(super) struct BlockCache {
    definitions: Option<(u64, Definitions)>,
    fragments: AhHashMap<BlockId, (Arc<Fragment>, ControlledMemoryCharge)>,
    bytes: usize,
}

fn encode<T: Serialize>(value: T) -> Result<Value, QueryError> {
    let data =
        serde_json::to_value(value).map_err(|_| invalid("Cannot encode graph projection"))?;
    Ok(json(data))
}

fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T, QueryError> {
    let Value::Json(bytes) = value else {
        return Err(invalid("Missing graph projection data"));
    };
    serde_json::from_value(crate::engine::json::to_serde(bytes)?)
        .map_err(|_| invalid("Invalid graph projection data"))
}

impl GraphStore<'_> {
    fn packed_definitions(&mut self) -> Result<Definitions, QueryError> {
        if let Some(defs) = &self.packed.definitions {
            return Ok(defs.clone());
        }
        let directory = self.read_cache().cloned();
        let scn = self.txn.snapshot_scn;
        let hit = directory.as_ref().and_then(|d| {
            d.packed
                .read()
                .definitions
                .as_ref()
                .filter(|(v, _)| *v == scn)
                .map(|(_, defs)| defs.clone())
        });
        let defs = if let Some(defs) = hit {
            defs
        } else {
            let mut defs = Vec::new();
            let key = IndexKey(smallvec![KeyPart::UInt(DEFINITION)]);
            for rid in self.prefix("g_out", key, self.txn)? {
                if let Some(row) = self.read_at(rid, self.txn)? {
                    let data: Descriptor = decode(&row.values[PAYLOAD])?;
                    if data.generation == self.table.active_generation {
                        defs.push((uint(&row.values[ID])?, data));
                    }
                }
            }
            let defs = Definitions::from(defs);
            if let Some(d) = directory {
                d.packed.write().definitions = Some((scn, defs.clone()));
            }
            defs
        };
        self.packed.definitions = Some(defs.clone());
        Ok(defs)
    }

    pub(in crate::engine::executor::graph) fn create_projection(
        &mut self,
        name: &str,
        spec: ReadProjection,
    ) -> Result<(), QueryError> {
        if spec.fields.is_empty()
            || spec.fields.len() > 32
            || spec
                .fields
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != spec.fields.len()
        {
            return Err(invalid("Graph projections require 1 to 32 distinct fields"));
        }
        self.db
            .acquire_table_exclusive(self.txn.txn_id, self.table.id)?;
        let view = self.current();
        self.txn = view;
        let owner = self
            .insert_row(vec![
                Value::Null,
                Value::UInt(DEFINITION),
                Value::Null,
                Value::Null,
                Value::UInt(0),
                Value::Bytes(token(name)?),
                Value::UInt(0),
                encode(Descriptor {
                    generation: self.table.active_generation,
                    spec: spec.clone(),
                })?,
            ])?
            .id;
        let role = if spec.node { NODE_LABEL } else { EDGE_LABEL };
        let bound = Some(IndexBound {
            key: IndexKey(smallvec![
                KeyPart::UInt(role),
                KeyPart::UInt(0),
                KeyPart::Bytes(token(&spec.label)?)
            ]),
            inclusive: true,
        });
        let mut members = ah_hash_map::<u64, u64>();
        let mut scratch = super::super::super::mem_budget::ScopedCharge::for_budget(
            super::super::super::mem_budget::current_budget(),
        );
        let mut cursor = self.db.range_index_snapshot_cursor(
            &self.table,
            self.index("g_key"),
            bound.clone(),
            bound,
            view,
        )?;
        loop {
            let (keys, page) = cursor.next_leaf_chunk(4096)?;
            if keys.is_empty() {
                break;
            }
            crate::engine::cancel::check_cancelled()?;
            let visible =
                self.index_visibility_mask("g_key", page, keys.iter().map(|(_, r)| Ok(*r)))?;
            for (slot, (key, _)) in keys.into_iter().enumerate() {
                if visible.as_ref().is_some_and(|mask| !mask[slot]) {
                    continue;
                }
                let Some(KeyPart::UInt(id)) = key.0.last() else {
                    return Err(invalid("Invalid graph label index"));
                };
                if !members.contains_key(&(id / 64)) {
                    scratch.add(48)?;
                }
                *members.entry(id / 64).or_default() |= 1 << (id % 64);
            }
        }
        drop(cursor);
        let total = self.db.heap_page_count_for_storage(self.table.id)?;
        for first in (0..total).step_by(512) {
            crate::engine::cancel::check_cancelled()?;
            let mut windows = ah_hash_map::<u64, Vec<(u16, u64, Vec<Json>)>>();
            let mut batch = super::super::super::mem_budget::ScopedCharge::for_budget(
                super::super::super::mem_budget::current_budget(),
            );
            self.db.scan_rows_columns_storage_streaming_range_with_ids(
                &self.table,
                self.table.id,
                view,
                &[ID, ROLE, PAYLOAD],
                first,
                (first + 512).min(total),
                |rid, values| {
                    let kind = uint(&values[1])?;
                    if kind > UNDIRECTED || (kind == NODE) != spec.node {
                        return Ok(());
                    }
                    let id = uint(&values[0])?;
                    if !members
                        .get(&(id / 64))
                        .is_some_and(|bits| bits & (1 << (id % 64)) != 0)
                    {
                        return Ok(());
                    }
                    let properties = spec
                        .fields
                        .iter()
                        .map(|f| read_property(&values[2], f))
                        .collect::<Result<Vec<_>, _>>()?;
                    batch.add(80 + properties.iter().map(projected_value_bytes).sum::<usize>())?;
                    let location =
                        (u64::from(rid.page_id) << 18) | (u64::from(rid.slot_id) << 2) | kind;
                    windows.entry(id / WIDTH).or_default().push((
                        (id % WIDTH) as u16,
                        location,
                        properties,
                    ));
                    Ok(())
                },
            )?;
            for (block, mut records) in windows {
                records.sort_unstable_by_key(|row| row.0);
                let tie = block * WIDTH + u64::from(records[0].0);
                let mut offsets = Vec::with_capacity(records.len());
                let mut locations = Vec::with_capacity(records.len());
                let mut values = vec![Vec::new(); spec.fields.len()];
                for (offset, location, properties) in records {
                    offsets.push(offset);
                    locations.push(location);
                    for (column, property) in values.iter_mut().zip(properties) {
                        column.push(property);
                    }
                }
                let columns = values
                    .into_iter()
                    .map(|col| {
                        if col.iter().all(|v| v.as_i64().is_some()) {
                            PackedColumn::Integers(
                                col.iter().map(|v| v.as_i64().unwrap()).collect(),
                            )
                        } else {
                            PackedColumn::Scalars(col)
                        }
                    })
                    .collect();
                self.insert_row(vec![
                    Value::Null,
                    Value::UInt(FRAGMENT),
                    Value::UInt(block),
                    Value::UInt(owner),
                    Value::UInt(owner),
                    Value::Bytes(block.to_be_bytes().to_vec()),
                    Value::UInt(tie),
                    encode(Fragment {
                        offsets,
                        locations,
                        columns,
                    })?,
                ])?;
            }
        }
        Ok(())
    }

    pub(in crate::engine::executor::graph) fn drop_projection(
        &mut self,
        name: &str,
    ) -> Result<(), QueryError> {
        self.db
            .acquire_table_exclusive(self.txn.txn_id, self.table.id)?;
        let view = self.current();
        let rows = self.rows_for_key(DEFINITION, 0, token(name)?, view)?;
        let entry = rows
            .first()
            .ok_or_else(|| invalid("Graph projection does not exist"))?;
        let owner = uint(&entry.values[ID])?;
        let key = IndexKey(smallvec![KeyPart::UInt(FRAGMENT), KeyPart::UInt(owner)]);
        for row in self.prefix("g_key", key, view)? {
            self.delete_row(row)?;
        }
        self.delete_row(entry.row_id)?;
        Ok(())
    }

    pub(super) fn invalidate_projection(&self, id: u64) -> Result<(), QueryError> {
        let key = IndexKey(smallvec![
            KeyPart::UInt(FRAGMENT),
            KeyPart::UInt(id / WIDTH)
        ]);
        for row in self.prefix("g_out", key, self.current())? {
            self.delete_row(row)?;
        }
        Ok(())
    }

    fn reserve_projection(&mut self, bytes: usize) -> bool {
        let charge = self.packed.charge.get_or_insert_with(|| {
            super::super::super::mem_budget::ScopedCharge::for_budget(
                super::super::super::mem_budget::current_budget(),
            )
        });
        let cap = (charge.charged().saturating_add(charge.remaining()) / 4).min(64 * MIB);
        charge.add_within(bytes, cap)
    }

    fn read_fragment(
        &mut self,
        owner: u64,
        tie: u64,
        rid: RowId,
        field_count: usize,
    ) -> Result<Option<Arc<Fragment>>, QueryError> {
        let directory = self.read_cache();
        if let Some(block) = directory.and_then(|d| {
            d.packed
                .read()
                .fragments
                .get(&(owner, tie))
                .map(|pair| pair.0.clone())
        }) {
            return Ok(Some(block));
        }
        let row = self
            .read_at(rid, self.txn)?
            .ok_or_else(|| invalid("Missing visible graph projection fragment"))?;
        let fragment: Fragment = decode(&row.values[PAYLOAD])?;
        let n = fragment.offsets.len();
        if n == 0
            || n > WIDTH as usize
            || fragment.locations.len() != n
            || fragment.columns.len() != field_count
            || !fragment.offsets.windows(2).all(|w| w[0] < w[1])
            || fragment.offsets[n - 1] >= WIDTH as u16
            || fragment.columns.iter().any(|col| match col {
                PackedColumn::Integers(v) => v.len() != n,
                PackedColumn::Scalars(v) => v.len() != n,
            })
        {
            return Err(invalid("Invalid graph projection column lengths"));
        }
        let bytes = 192
            + n * 16
            + fragment
                .columns
                .iter()
                .map(|c| match c {
                    PackedColumn::Integers(v) => v.capacity() * 8,
                    PackedColumn::Scalars(v) => v.iter().map(projected_value_bytes).sum::<usize>(),
                })
                .sum::<usize>();
        let fragment = Arc::new(fragment);
        if let Some(d) = directory {
            let mut cache = d.packed.write();
            if let Some((existing, _)) = cache.fragments.get(&(owner, tie)) {
                return Ok(Some(existing.clone()));
            }
            let limit = super::super::super::mem_budget::memory_target_bytes() / 12;
            if bytes <= limit.saturating_sub(cache.bytes) {
                if let Some(charge) = ControlledMemoryCharge::try_new_optional(
                    ControlledMemoryCategory::DerivedCache,
                    bytes,
                ) {
                    cache.bytes += bytes;
                    cache
                        .fragments
                        .insert((owner, tie), (fragment.clone(), charge));
                    return Ok(Some(fragment));
                }
            }
        }
        Ok(self.reserve_projection(bytes).then_some(fragment))
    }

    pub(in crate::engine::executor::graph) fn projected_members(
        &mut self,
        node: bool,
        label: &str,
        ids: &[u64],
        field: Option<&str>,
    ) -> Result<AhHashMap<u64, ElementRef>, QueryError> {
        let mut hit = ah_hash_map();
        if !self.share_read_snapshot || crate::storage::lock_waits_nonblocking() || ids.len() < 8 {
            return Ok(hit);
        }
        let defs = self.packed_definitions()?;
        let Some((owner, definition)) = defs
            .iter()
            .filter(|(_, d)| d.spec.node == node && d.spec.label == label)
            .max_by_key(|(_, d)| {
                (
                    field.is_some_and(|name| d.spec.fields.iter().any(|f| f == name)),
                    d.spec.fields.len(),
                )
            })
        else {
            return Ok(hit);
        };
        let fields: Arc<[String]> = definition.spec.fields.clone().into();
        let mut blocks: Vec<_> = ids
            .iter()
            .map(|id| id / WIDTH)
            .filter(|id| !self.packed.blocks.contains_key(&(*owner, *id)))
            .collect();
        blocks.sort_unstable();
        blocks.dedup();
        if !blocks.is_empty() {
            let keys: Vec<_> = blocks
                .iter()
                .map(|id| {
                    IndexKey(smallvec![
                        KeyPart::UInt(FRAGMENT),
                        KeyPart::UInt(*owner),
                        KeyPart::Bytes(id.to_be_bytes().to_vec())
                    ])
                })
                .collect();
            let Some((entries, bound)) = self.db.lookup_prefixes_prefetched(
                &self.table,
                self.index("g_key"),
                &keys,
                self.txn,
                blocks.len() * 32,
            )?
            else {
                return Ok(hit);
            };
            let visible = self.index_visibility_mask(
                "g_key",
                bound,
                entries.iter().map(|(_, rid)| Ok(*rid)),
            )?;
            if !self.reserve_projection(blocks.len() * 80 + entries.len() * 32) {
                return Ok(hit);
            }
            for block in blocks {
                self.packed.blocks.insert((*owner, block), Vec::new());
            }
            for (slot, (key, rid)) in entries.into_iter().enumerate() {
                if visible.as_ref().is_some_and(|mask| !mask[slot]) {
                    continue;
                }
                let Some(KeyPart::UInt(tie)) = key.0.last() else {
                    return Err(invalid("Invalid graph projection key"));
                };
                let Some(fragment) = self.read_fragment(*owner, *tie, rid, fields.len())? else {
                    continue;
                };
                self.packed
                    .blocks
                    .get_mut(&(*owner, tie / WIDTH))
                    .ok_or_else(|| invalid("Invalid graph projection offset"))?
                    .push(fragment);
            }
        }
        let mut pending = Vec::new();
        for &id in ids {
            if hit.contains_key(&id) {
                continue;
            }
            let Some(fragments) = self.packed.blocks.get(&(*owner, id / WIDTH)) else {
                continue;
            };
            for fragment in fragments {
                if let Ok(slot) = fragment.offsets.binary_search(&((id % WIDTH) as u16)) {
                    let location = fragment.locations[slot];
                    let entry = ElementRef {
                        id,
                        role: location & 3,
                        row: RowId {
                            page_id: (location >> 18) as u32,
                            slot_id: (location >> 2) as u16,
                        },
                    };
                    hit.insert(id, entry);
                    if !self.packed.elements.contains_key(&entry.row) {
                        pending.push((entry, fragment.clone(), slot));
                    }
                    break;
                }
            }
        }
        if !self.reserve_projection(pending.len() * 128) {
            return Ok(ah_hash_map());
        }
        for (entry, fragment, slot) in pending {
            self.packed
                .elements
                .insert(entry.row, (fragment, fields.clone(), slot));
            if node {
                self.nodes.insert(entry.id, Some(entry));
            }
        }
        if let Some(c) = &self.profile.counters {
            c.projection_hits
                .fetch_add(hit.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(hit)
    }
}
