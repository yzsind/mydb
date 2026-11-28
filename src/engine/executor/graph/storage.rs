mod packed;
use super::super::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use super::*;
use crate::graph::{Direction, IndexType, PropertyIndex};
use crate::storage::{IndexState, KeyPart, StoredRow};
use crate::util::fast_hash::{ah_hash_map, ah_hash_set, AhHashMap, AhHashSet};
use serde_json::{Map, Value as Json};
use smallvec::{smallvec, SmallVec};

const NODE: u64 = 0;
const DIRECTED: u64 = 1;
const UNDIRECTED: u64 = 2;
const NODE_LABEL: u64 = 3;
const EDGE_LABEL: u64 = 4;
const INDEX: u64 = 5;
const PROPERTY: u64 = 6;
const ID: usize = 0;
const ROLE: usize = 1;
const A: usize = 2;
const B: usize = 3;
const OWNER: usize = 4;
const TOKEN: usize = 5;
const TIE: usize = 6;
const PAYLOAD: usize = 7;
const EDGE_ROLE: usize = 10;
const LABELED_OUT: &str = "g_lout";
const LABELED_IN: &str = "g_lin";
const MAX_KEY_BYTES: usize = 512;
pub(super) const MAX_EDGES: usize = 1_000_000;
const MAX_INLINE_EDGES: usize = 128;
const MAX_ORDERED_ROOTS: usize = 1024;

pub(super) fn create(db: &Database, name: &str) -> Result<(), QueryError> {
    // Each graph has one catalog descriptor and one heap. Stable element IDs,
    // adjacency, label membership, and index definitions share its lifetime.
    let Statement::CreateTable(def) = parse_statement(
        "CREATE TABLE graph_layout (
            g_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            g_role BIGINT UNSIGNED NOT NULL,
            g_a BIGINT UNSIGNED,
            g_b BIGINT UNSIGNED,
            g_owner BIGINT UNSIGNED,
            g_token VARBINARY(512),
            g_tie BIGINT UNSIGNED,
            g_payload JSON,
            g_edge_src BIGINT UNSIGNED,
            g_edge_dst BIGINT UNSIGNED,
            g_edge_role BIGINT UNSIGNED,
            INDEX g_out (g_role, g_a, g_b, g_id),
            INDEX g_in (g_role, g_b, g_a, g_id) WHERE g_role IN (1,2,6),
            UNIQUE INDEX g_key (g_role, g_owner, g_token, g_tie) WHERE g_role >= 3,
            INDEX g_lout (g_token, g_edge_role, g_edge_src, g_edge_dst, g_a) WHERE g_role = 4,
            INDEX g_lin (g_token, g_edge_role, g_edge_dst, g_edge_src, g_a) WHERE g_role = 4
        )",
    )
    .map_err(super::super::parse_error_to_query_error)?
    else {
        unreachable!()
    };
    db.create_table(name, def.columns, def.primary_key, def.indexes)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(super) struct ElementRef {
    pub id: u64,
    pub role: u64,
    pub row: RowId,
}

impl ElementRef {
    pub fn node(self) -> bool {
        self.role == NODE
    }
}

#[derive(Clone, Copy)]
pub(super) struct Neighbor {
    pub edge: ElementRef,
    pub node: u64,
}

#[derive(Clone, Copy)]
pub(super) struct LabeledNeighbor {
    pub edge: u64,
    pub role: u64,
    pub node: u64,
}

#[derive(Clone)]
struct Definition {
    element: ElementRef,
    spec: PropertyIndex,
}

const LOCATOR_DIRECTORY_BYTES: usize = 32 * 1024 * 1024;
const LOCATOR_CACHE_GRAPHS: usize = 4;

// An optional map from immutable element IDs to heap root row IDs. Only
// callers that have already reached a node in their own snapshot may skip
// its primary lookup. The heap remains authoritative for property versions.
#[derive(Default)]
pub(super) struct NodeLocatorCache {
    entries: parking_lot::RwLock<Vec<LocatorEntry>>,
}

pub(super) fn node_locators() -> &'static NodeLocatorCache {
    static CACHE: std::sync::OnceLock<NodeLocatorCache> = std::sync::OnceLock::new();
    CACHE.get_or_init(NodeLocatorCache::default)
}

struct LocatorEntry {
    database: std::sync::Weak<Database>,
    table_id: u64,
    generation: u64,
    directory: Arc<GraphReadCache>,
}

struct GraphReadCache {
    packed: parking_lot::RwLock<packed::BlockCache>,
    locations: parking_lot::RwLock<NodeLocations>,
    projected: [parking_lot::RwLock<Option<ProjectedPropertySnapshot>>; READ_CACHE_SHARDS],
    adjacency: [parking_lot::RwLock<Option<AdjacencySnapshot>>; READ_CACHE_SHARDS],
    memberships: parking_lot::RwLock<Option<LabelMemberships>>,
    label_bounds: parking_lot::RwLock<Option<LabelBoundsSnapshot>>,
    index_definitions: parking_lot::RwLock<Option<IndexDefinitionsSnapshot>>,
    indexed_nodes: parking_lot::RwLock<Option<IndexedNodesSnapshot>>,
}

struct NodeLocations {
    blocks: AhHashMap<u64, LocatorBlock>,
    charge: ControlledMemoryCharge,
}

// Entries are populated by successful primary lookups. Four bitmap words
// address 256 logical IDs without allocating slots for their missing IDs.
#[derive(Default)]
struct LocatorBlock {
    occupied: [u64; 4],
    roots: Vec<RowId>,
}

impl LocatorBlock {
    fn position(&self, offset: u8) -> (usize, bool) {
        let word = usize::from(offset / 64);
        let bit = 1u64 << (offset % 64);
        let prior = self.occupied[..word]
            .iter()
            .map(|bits| bits.count_ones())
            .sum::<u32>();
        let rank = prior + (self.occupied[word] & (bit - 1)).count_ones();
        (rank as usize, self.occupied[word] & bit != 0)
    }
}

impl NodeLocations {
    fn find(&self, id: u64) -> Option<RowId> {
        let block = self.blocks.get(&(id >> 8))?;
        let (position, present) = block.position(id as u8);
        present.then(|| block.roots[position])
    }

    fn remember(&mut self, id: u64, row: RowId) {
        let key = id >> 8;
        // Includes the bitmap, vector header, key, and spare hash buckets.
        if !self.blocks.contains_key(&key) {
            let bytes = 128;
            if bytes > LOCATOR_DIRECTORY_BYTES.saturating_sub(self.charge.bytes())
                || !self.charge.try_grow_optional(bytes)
            {
                return;
            }
        }
        let block = self.blocks.entry(key).or_default();
        let offset = id as u8;
        let (position, present) = block.position(offset);
        if present {
            return;
        }
        if block.roots.len() == block.roots.capacity() {
            let extra = block.roots.capacity().clamp(4, 16);
            let bytes = extra * std::mem::size_of::<RowId>();
            if bytes > LOCATOR_DIRECTORY_BYTES.saturating_sub(self.charge.bytes())
                || !self.charge.try_grow_optional(bytes)
            {
                return;
            }
            if block.roots.try_reserve_exact(extra).is_err() {
                self.charge.shrink(bytes);
                return;
            }
        }
        block.roots.insert(position, row);
        block.occupied[usize::from(offset / 64)] |= 1u64 << (offset % 64);
    }
}

struct ProjectedPropertySnapshot {
    scn: u64,
    fields: AhHashMap<String, ProjectedField>,
    charge: ControlledMemoryCharge,
}

#[derive(Default)]
struct ProjectedField {
    integers: AhHashMap<(u32, u16), Box<IntegerSlots>>,
    scalars: AhHashMap<RowId, Json>,
}

#[derive(Default)]
struct IntegerSlots {
    present: u32,
    values: [i64; 32],
}

impl ProjectedField {
    fn get(&self, row: RowId) -> Option<Json> {
        let slot = usize::from(row.slot_id % 32);
        if let Some(block) = self.integers.get(&(row.page_id, row.slot_id / 32)) {
            if block.present & (1 << slot) != 0 {
                return Some(Json::from(block.values[slot]));
            }
        }
        self.scalars.get(&row).cloned()
    }

    fn remember(
        &mut self,
        row: RowId,
        value: &Json,
        charge: &mut ControlledMemoryCharge,
        limit: usize,
    ) {
        let mut reserve = |bytes: usize| {
            bytes <= limit.saturating_sub(charge.bytes()) && charge.try_grow_optional(bytes)
        };
        if let Some(number) = value.as_i64() {
            let key = (row.page_id, row.slot_id / 32);
            if !self.integers.contains_key(&key)
                && !reserve(64 + std::mem::size_of::<IntegerSlots>())
            {
                return;
            }
            let block = self.integers.entry(key).or_default();
            let slot = usize::from(row.slot_id % 32);
            block.values[slot] = number;
            block.present |= 1 << slot;
        } else {
            let bytes = projected_value_bytes(value);
            if bytes <= 4096
                && !value.is_object()
                && !value.is_array()
                && !self.scalars.contains_key(&row)
                && reserve(bytes)
            {
                self.scalars.insert(row, value.clone());
            }
        }
    }
}

struct AdjacencySnapshot {
    scn: u64,
    lists: AhHashMap<AdjacencyCacheKey, AdjacentList>,
    charge: ControlledMemoryCharge,
}

type AdjacencyCacheKey = (u64, Direction, bool, AdjacencyBounds);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(super) struct AdjacencyBounds {
    edge: Option<(u64, u64)>,
    endpoint: Option<(u64, u64)>,
}

impl AdjacencyBounds {
    fn contains(self, edge: u64, endpoint: u64) -> bool {
        self.edge
            .is_none_or(|(low, high)| (low..=high).contains(&edge))
            && self
                .endpoint
                .is_none_or(|(low, high)| (low..=high).contains(&endpoint))
    }
}

type LabelKey = (u64, Vec<u8>);
type LabelExtent = Option<(u64, u64)>;

// One bit records whether the ID was probed. Another records membership.
struct LabelMemberships {
    scn: u64,
    labels: AhHashMap<LabelKey, AhHashMap<u64, (u64, u64)>>,
    charge: ControlledMemoryCharge,
}

impl LabelMemberships {
    fn remember(&mut self, role: u64, label: &[u8], ids: &[u64], members: &AhHashSet<u64>) {
        let limit =
            (super::super::mem_budget::memory_target_bytes() / 128).clamp(1024 * 1024, 32 * MIB);
        let key = (role, label.to_vec());
        let bytes = 128 + label.len();
        if !self.labels.contains_key(&key)
            && (bytes > limit.saturating_sub(self.charge.bytes())
                || !self.charge.try_grow_optional(bytes))
        {
            return;
        }
        let blocks = self.labels.entry(key).or_default();
        for &id in ids {
            let block = id / 64;
            if !blocks.contains_key(&block)
                && (self.charge.bytes() + 64 > limit || !self.charge.try_grow_optional(64))
            {
                continue;
            }
            let masks = blocks.entry(block).or_default();
            let bit = 1 << (id % 64);
            masks.0 |= bit;
            if members.contains(&id) {
                masks.1 |= bit;
            }
        }
    }
}

struct LabelBoundsSnapshot {
    scn: u64,
    bounds: AhHashMap<LabelKey, LabelExtent>,
    charge: ControlledMemoryCharge,
}

struct IndexDefinitionsSnapshot {
    scn: u64,
    definitions: Arc<[Definition]>,
    _charge: ControlledMemoryCharge,
}

struct IndexedNodesSnapshot {
    scn: u64,
    by_index: AhHashMap<u64, AhHashMap<Vec<u8>, ElementRef>>,
    charge: ControlledMemoryCharge,
}

const READ_CACHE_SHARDS: usize = 16;
const MIB: usize = 1024 * 1024;
const SHARED_INDEXED_NODE_BYTES: usize = 2 * 1024 * 1024;

fn property_shard(row: RowId) -> usize {
    row.page_id as usize % READ_CACHE_SHARDS
}

fn property_cache_shard_limit() -> usize {
    let memory = super::super::mem_budget::memory_target_bytes();
    (memory / 80).clamp(32 * MIB, 96 * MIB) / READ_CACHE_SHARDS
}

fn adjacency_cache_limit() -> usize {
    let memory = super::super::mem_budget::memory_target_bytes();
    (memory / 128).clamp(32 * MIB, 64 * MIB) / READ_CACHE_SHARDS
}

fn projected_value_bytes(value: &Json) -> usize {
    // A graph property is a scalar; reserve the map/allocator overhead as
    // well as the string's owned bytes, when applicable.
    80 + value.as_str().map_or(0, str::len)
}

impl GraphReadCache {
    fn remember_indexed_node(&self, scn: u64, owner: u64, token: Vec<u8>, node: ElementRef) {
        let Some(mut slot) = self.indexed_nodes.try_write() else {
            return;
        };
        if slot.as_ref().is_some_and(|cache| cache.scn > scn) {
            return;
        }
        if slot.as_ref().is_none_or(|cache| cache.scn != scn) {
            let Some(charge) =
                ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::DerivedCache, 0)
            else {
                return;
            };
            *slot = Some(IndexedNodesSnapshot {
                scn,
                by_index: ah_hash_map(),
                charge,
            });
        }
        let cache = slot
            .as_mut()
            .expect("indexed node snapshot was initialized");
        let bytes = 200 + token.capacity();
        if bytes > SHARED_INDEXED_NODE_BYTES.saturating_sub(cache.charge.bytes())
            || cache
                .by_index
                .get(&owner)
                .is_some_and(|nodes| nodes.contains_key(&token))
            || !cache.charge.try_grow_optional(bytes)
        {
            return;
        }
        cache.by_index.entry(owner).or_default().insert(token, node);
    }

    fn remember_locations(&self, rows: impl IntoIterator<Item = (u64, RowId)>) {
        let Some(mut locations) = self.locations.try_write() else {
            return;
        };
        for (id, row) in rows {
            locations.remember(id, row);
        }
    }

    fn remember_properties(&self, scn: u64, field: &str, items: &[(RowId, Json)]) {
        let mut shards: [SmallVec<[&(RowId, Json); 16]>; READ_CACHE_SHARDS] =
            std::array::from_fn(|_| SmallVec::new());
        for item in items {
            shards[property_shard(item.0)].push(item);
        }
        let limit = property_cache_shard_limit();
        for (slot, shard) in self.projected.iter().zip(shards) {
            if !shard.is_empty() {
                Self::remember_property_shard(slot, scn, field, &shard, limit);
            }
        }
    }

    fn remember_property_shard(
        slot: &parking_lot::RwLock<Option<ProjectedPropertySnapshot>>,
        scn: u64,
        field: &str,
        items: &[&(RowId, Json)],
        limit: usize,
    ) {
        let Some(mut slot) = slot.try_write() else {
            return;
        };
        if slot.as_ref().is_some_and(|cache| cache.scn > scn) {
            return;
        }
        if slot.as_ref().is_none_or(|cache| cache.scn != scn) {
            let Some(charge) =
                ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::DerivedCache, 0)
            else {
                return;
            };
            *slot = Some(ProjectedPropertySnapshot {
                scn,
                fields: ah_hash_map(),
                charge,
            });
        }
        let cache = slot.as_mut().expect("initialized projected fields");
        if !cache.fields.contains_key(field) {
            let bytes = 192 + field.len();
            if bytes > limit.saturating_sub(cache.charge.bytes())
                || !cache.charge.try_grow_optional(bytes)
            {
                return;
            }
            cache
                .fields
                .insert(field.to_owned(), ProjectedField::default());
        }
        let column = cache.fields.get_mut(field).expect("initialized field");
        for &(row, value) in items {
            column.remember(*row, value, &mut cache.charge, limit);
        }
    }

    fn remember_adjacency(
        &self,
        scn: u64,
        direction: Direction,
        vertices_only: bool,
        bounds: AdjacencyBounds,
        lists: &AhHashMap<u64, AdjacentList>,
        missed: &[u64],
    ) {
        let limit = adjacency_cache_limit();
        if let [id] = missed {
            Self::remember_adjacency_shard(
                &self.adjacency[*id as usize % READ_CACHE_SHARDS],
                scn,
                direction,
                vertices_only,
                bounds,
                lists,
                missed,
                limit,
            );
            return;
        }
        let mut shards: [SmallVec<[u64; 16]>; READ_CACHE_SHARDS] =
            std::array::from_fn(|_| SmallVec::new());
        for &id in missed {
            shards[id as usize % READ_CACHE_SHARDS].push(id);
        }
        for (slot, ids) in self.adjacency.iter().zip(shards) {
            if !ids.is_empty() {
                Self::remember_adjacency_shard(
                    slot,
                    scn,
                    direction,
                    vertices_only,
                    bounds,
                    lists,
                    &ids,
                    limit,
                );
            }
        }
    }

    fn remember_adjacency_shard(
        slot: &parking_lot::RwLock<Option<AdjacencySnapshot>>,
        scn: u64,
        direction: Direction,
        vertices_only: bool,
        bounds: AdjacencyBounds,
        lists: &AhHashMap<u64, AdjacentList>,
        missed: &[u64],
        limit: usize,
    ) {
        if slot.try_read().is_some_and(|state| {
            state.as_ref().is_some_and(|cache| {
                cache.scn == scn && limit.saturating_sub(cache.charge.bytes()) < 96
            })
        }) {
            return;
        }
        let Some(mut slot) = slot.try_write() else {
            return;
        };
        if slot.as_ref().is_some_and(|cache| cache.scn > scn) {
            return;
        }
        if slot.as_ref().is_none_or(|cache| cache.scn != scn) {
            let Some(charge) =
                ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::DerivedCache, 0)
            else {
                return;
            };
            *slot = Some(AdjacencySnapshot {
                scn,
                lists: ah_hash_map(),
                charge,
            });
        }
        let cache = slot.as_mut().expect("adjacency snapshot was initialized");
        let mut remaining = limit.saturating_sub(cache.charge.bytes());
        let mut reserved = 0;
        let mut additions = SmallVec::<[(AdjacencyCacheKey, &AdjacentList); 16]>::new();
        for &id in missed {
            let list = &lists[&id];
            let bytes = 96 + std::mem::size_of::<AdjacencyBounds>() + list.payload.bytes();
            let key = (id, direction, vertices_only, bounds);
            if bytes > remaining || bytes > 16 * 1024 || cache.lists.contains_key(&key) {
                continue;
            }
            remaining -= bytes;
            reserved += bytes;
            additions.push((key, list));
        }
        if reserved != 0 && cache.charge.try_grow_optional(reserved) {
            cache
                .lists
                .extend(additions.into_iter().map(|(key, list)| (key, list.clone())));
        }
    }
}

impl NodeLocatorCache {
    pub(super) fn discard(&self, db: &Arc<Database>, table_id: u64) {
        self.entries.write().retain(|entry| {
            entry.database.as_ptr() != Arc::as_ptr(db) || entry.table_id != table_id
        });
    }

    fn directory(&self, store: &GraphStore<'_>) -> Option<Arc<GraphReadCache>> {
        if !store.share_read_snapshot {
            return None;
        }
        let same_graph = |entry: &&LocatorEntry| {
            entry.database.as_ptr() == Arc::as_ptr(store.db)
                && entry.table_id == store.table.id
                && entry.generation == store.table.active_generation
        };
        if let Some(entry) = self.entries.read().iter().find(same_graph) {
            return Some(entry.directory.clone());
        }
        let mut entries = self.entries.write();
        if let Some(entry) = entries.iter().find(same_graph) {
            return Some(entry.directory.clone());
        }
        let charge =
            ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::DerivedCache, 0)?;
        let directory = Arc::new(GraphReadCache {
            packed: Default::default(),
            locations: parking_lot::RwLock::new(NodeLocations {
                blocks: ah_hash_map(),
                charge,
            }),
            projected: Default::default(),
            adjacency: Default::default(),
            memberships: Default::default(),
            label_bounds: Default::default(),
            index_definitions: parking_lot::RwLock::default(),
            indexed_nodes: parking_lot::RwLock::default(),
        });
        entries.retain(|entry| entry.database.strong_count() != 0);
        if entries.len() == LOCATOR_CACHE_GRAPHS {
            entries.remove(0);
        }
        entries.push(LocatorEntry {
            database: Arc::downgrade(store.db),
            table_id: store.table.id,
            generation: store.table.active_generation,
            directory: directory.clone(),
        });
        Some(directory)
    }
}

pub(super) const ADJACENCY_BATCH_SIZE: usize = 512;
pub(super) const ADJACENCY_WINDOW_SIZE: usize = ADJACENCY_BATCH_SIZE * 4;

pub(super) fn adjacency_batch_size(txn: TxnView) -> usize {
    // Route one wider frontier together while storage has spare parallel
    // I/O capacity. Under load, retain the smaller per-command working set.
    if txn.txn_id == 0 && crate::engine::memory_reclaimer::active_statement_count() == 1 {
        ADJACENCY_BATCH_SIZE * 2
    } else {
        ADJACENCY_BATCH_SIZE
    }
}

pub(super) struct AdjacentBatch {
    lists: AhHashMap<u64, AdjacentList>,
    examined: usize,
}

impl AdjacentBatch {
    pub fn neighbors(&self, id: u64) -> &[Neighbor] {
        match self.lists.get(&id).map(|list| &list.payload) {
            Some(AdjacencyPayload::Edges(edges)) => edges,
            Some(AdjacencyPayload::Vertices(_)) => unreachable!("edge data was not requested"),
            None => &[],
        }
    }

    pub fn append_vertices(&self, id: u64, frontier: &mut Vec<u64>) {
        if let Some(list) = self.lists.get(&id) {
            match &list.payload {
                AdjacencyPayload::Edges(edges) => {
                    frontier.extend(edges.iter().map(|edge| edge.node))
                }
                AdjacencyPayload::Vertices(vertices) => frontier.extend_from_slice(vertices),
            }
        }
    }
}

#[derive(Clone)]
struct AdjacentList {
    payload: AdjacencyPayload,
    examined: usize,
}

#[derive(Clone)]
enum AdjacencyPayload {
    Edges(Arc<[Neighbor]>),
    Vertices(Arc<[u64]>),
}

impl AdjacencyPayload {
    fn bytes(&self) -> usize {
        match self {
            Self::Edges(edges) => std::mem::size_of_val(edges.as_ref()),
            Self::Vertices(vertices) => std::mem::size_of_val(vertices.as_ref()),
        }
    }
}

enum NeighborBuffer {
    Edges(Vec<Neighbor>),
    Vertices(Vec<u64>),
}

impl NeighborBuffer {
    fn new(vertices_only: bool) -> Self {
        if vertices_only {
            Self::Vertices(Vec::new())
        } else {
            Self::Edges(Vec::new())
        }
    }

    fn push(&mut self, neighbor: Neighbor) {
        match self {
            Self::Edges(edges) => edges.push(neighbor),
            Self::Vertices(nodes) => nodes.push(neighbor.node),
        }
    }

    fn into_payload(self) -> AdjacencyPayload {
        match self {
            Self::Edges(edges) => AdjacencyPayload::Edges(edges.into()),
            Self::Vertices(nodes) => AdjacencyPayload::Vertices(nodes.into()),
        }
    }
}

#[derive(Default, Debug)]
struct TraversalCounters {
    edge_candidates: std::sync::atomic::AtomicU64,
    label_probes: std::sync::atomic::AtomicU64,
    projection_hits: std::sync::atomic::AtomicU64,
    heap_page_visits: std::sync::atomic::AtomicU64,
    predicate_survivors: std::sync::atomic::AtomicU64,
}

struct TraversalProfile {
    connection: u64,
    counters: Option<Box<TraversalCounters>>,
}

impl TraversalProfile {
    fn new(connection: u64) -> Self {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        let enabled = *ENABLED.get_or_init(|| {
            std::env::var_os("MYDB_GRAPH_PROFILE").as_deref() == Some(std::ffi::OsStr::new("1"))
        });
        Self {
            connection,
            counters: enabled.then(Box::default),
        }
    }
}

impl Drop for TraversalProfile {
    fn drop(&mut self) {
        if let Some(value) = &self.counters {
            eprintln!("[graph-profile] connection={} {:?}", self.connection, value);
        }
    }
}

pub(super) struct GraphStore<'a> {
    db: &'a Arc<Database>,
    table: Arc<TableSchema>,
    name: String,
    pub txn: TxnView,
    share_read_snapshot: bool,
    read_cache: std::sync::OnceLock<Option<Arc<GraphReadCache>>>,
    nodes: AhHashMap<u64, Option<ElementRef>>,
    packed: packed::StatementProjection,
    profile: TraversalProfile,
    projection_payloads: AhHashMap<RowId, Value>,
    projected_property: Option<String>,
    projected_values: AhHashMap<RowId, Json>,
    projection_charge: super::super::mem_budget::ScopedCharge,
    definitions: Option<Vec<Definition>>,
    read_definitions: Option<Arc<[Definition]>>,
    edges_examined: usize,
    charge: super::super::mem_budget::ScopedCharge,
}

fn adjacency_probes(direction: Direction) -> &'static [(&'static str, u64)] {
    match direction {
        Direction::Out => &[("g_out", DIRECTED)],
        Direction::In => &[("g_in", DIRECTED)],
        Direction::Undirected => &[("g_out", UNDIRECTED), ("g_in", UNDIRECTED)],
        Direction::Either => &[
            ("g_out", DIRECTED),
            ("g_in", DIRECTED),
            ("g_out", UNDIRECTED),
            ("g_in", UNDIRECTED),
        ],
    }
}

fn decode_neighbor(
    key: &IndexKey,
    row: RowId,
    index: &str,
    role: u64,
    direction: Direction,
) -> Result<(u64, Option<Neighbor>), QueryError> {
    let (anchor, node, edge) =
        adjacency_key(key).ok_or_else(|| invalid("Invalid graph adjacency index key"))?;
    Ok((
        anchor,
        neighbor_for_key(anchor, node, edge, row, index, role, direction),
    ))
}

fn adjacency_key(key: &IndexKey) -> Option<(u64, u64, u64)> {
    let [_, KeyPart::UInt(anchor), KeyPart::UInt(node), KeyPart::UInt(edge)] = key.0.as_slice()
    else {
        return None;
    };
    Some((*anchor, *node, *edge))
}

fn neighbor_for_key(
    anchor: u64,
    node: u64,
    edge: u64,
    row: RowId,
    index: &str,
    role: u64,
    direction: Direction,
) -> Option<Neighbor> {
    let duplicate_loop = index == "g_in"
        && node == anchor
        && matches!(direction, Direction::Either | Direction::Undirected);
    (!duplicate_loop).then_some(Neighbor {
        edge: ElementRef {
            id: edge,
            role,
            row,
        },
        node,
    })
}

fn uint(v: &Value) -> Result<u64, QueryError> {
    match v {
        Value::UInt(v) => Ok(*v),
        Value::Int(v) if *v >= 0 => Ok(*v as u64),
        _ => Err(invalid("Invalid property graph numeric storage value")),
    }
}

fn bytes(v: &Value) -> Result<&[u8], QueryError> {
    match v {
        Value::Bytes(bytes) => Ok(bytes),
        _ => Err(invalid("Invalid property graph key storage value")),
    }
}

fn payload(v: &Value) -> Result<Map<String, Json>, QueryError> {
    match v {
        Value::Null => Ok(Map::new()),
        Value::Json(bytes) => match crate::engine::json::to_serde(bytes)? {
            Json::Object(map) => Ok(map),
            _ => Err(invalid("Invalid property graph storage payload")),
        },
        _ => Err(invalid("Invalid property graph storage payload type")),
    }
}

fn read_inline_property(bytes: Option<&[u8]>, name: &str) -> Result<Json, QueryError> {
    let Some(bytes) = bytes else {
        return Ok(Json::Null);
    };
    let object = crate::engine::json::JsonbRef::new(bytes)?;
    object.validate()?;
    match object.get(name) {
        Some(value) => value.to_serde(),
        None => Ok(Json::Null),
    }
}

fn read_property(value: &Value, name: &str) -> Result<Json, QueryError> {
    let Value::Json(bytes) = value else {
        return Ok(Json::Null);
    };
    let object = crate::engine::json::JsonbRef::new(bytes)?;
    match object.get(name) {
        Some(value) => value.to_serde(),
        None => Ok(Json::Null),
    }
}

fn json(value: Json) -> Value {
    Value::Json(crate::engine::json::from_serde(&value))
}

fn element(row: &StoredRow) -> Result<ElementRef, QueryError> {
    Ok(ElementRef {
        id: uint(&row.values[ID])?,
        role: uint(&row.values[ROLE])?,
        row: row.row_id,
    })
}

fn token(input: &str) -> Result<Vec<u8>, QueryError> {
    if input.len() > MAX_KEY_BYTES {
        return Err(invalid("Graph label or index key exceeds 512 bytes"));
    }
    Ok(input.as_bytes().to_vec())
}

fn property_token(kind: IndexType, value: &Json) -> Result<Vec<u8>, QueryError> {
    match kind {
        IndexType::String => token(
            value
                .as_str()
                .ok_or_else(|| invalid("Graph index expects a STRING property"))?,
        ),
        IndexType::Int64 => Ok(value
            .as_i64()
            .ok_or_else(|| invalid("Graph index expects an INT64 property"))?
            .to_be_bytes()
            .to_vec()),
    }
}

impl<'a> GraphStore<'a> {
    pub fn new(
        db: &'a Arc<Database>,
        table: Arc<TableSchema>,
        txn: TxnView,
    ) -> Result<Self, QueryError> {
        let name = table.qualified_name();
        let share_read_snapshot = !txn.historical
            && !txn.dirty_read
            && (txn.txn_id == 0 || (txn.read_only && db.transaction_is_read_only(txn.txn_id)));
        Ok(Self {
            db,
            table,
            name,
            txn,
            share_read_snapshot,
            read_cache: std::sync::OnceLock::new(),
            nodes: ah_hash_map(),
            packed: Default::default(),
            profile: TraversalProfile::new(txn.connection_id as u64),
            projection_payloads: ah_hash_map(),
            projected_property: None,
            projected_values: ah_hash_map(),
            projection_charge: super::super::mem_budget::ScopedCharge::for_budget(
                super::super::mem_budget::current_budget(),
            ),
            definitions: None,
            read_definitions: None,
            edges_examined: 0,
            charge: super::super::mem_budget::ScopedCharge::for_budget(
                super::super::mem_budget::current_budget(),
            ),
        })
    }

    pub fn projection_source(&self) -> (&'a Arc<Database>, Arc<TableSchema>) {
        (self.db, self.table.clone())
    }

    pub fn into_charge(self) -> super::super::mem_budget::ScopedCharge {
        self.charge
    }

    pub fn retain_worker_charge(&mut self, mut charge: super::super::mem_budget::ScopedCharge) {
        self.charge.absorb(&mut charge);
    }

    pub fn charge(&mut self, bytes: usize) -> Result<(), QueryError> {
        crate::engine::cancel::check_cancelled()?;
        self.charge.add(bytes)
    }

    fn index(&self, name: &str) -> &IndexSchema {
        self.table
            .indexes
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(name))
            .expect("graph indexes are fixed")
    }

    pub fn has_labeled_adjacency(&self) -> bool {
        !self.txn.historical
            && [LABELED_OUT, LABELED_IN].iter().all(|name| {
                self.table
                    .indexes
                    .iter()
                    .any(|index| index.name == *name && index.state == IndexState::Public)
            })
    }

    fn read_cache(&self) -> Option<&Arc<GraphReadCache>> {
        self.read_cache
            .get_or_init(|| node_locators().directory(self))
            .as_ref()
    }

    fn current(&self) -> TxnView {
        let mut view =
            self.db
                .session_statement_view(self.txn.txn_id, IsolationLevel::ReadCommitted, None);
        view.connection_id = self.txn.connection_id;
        view
    }

    fn prefix(&self, index: &str, key: IndexKey, txn: TxnView) -> Result<Vec<RowId>, QueryError> {
        self.db
            .prefix_index_visible(&self.table, self.index(index), key, txn)
    }

    fn rows_for_key(
        &self,
        role: u64,
        owner: u64,
        key: Vec<u8>,
        txn: TxnView,
    ) -> Result<Vec<StoredRow>, QueryError> {
        let ids = self.prefix(
            "g_key",
            IndexKey(smallvec![
                KeyPart::UInt(role),
                KeyPart::UInt(owner),
                KeyPart::Bytes(key.clone())
            ]),
            txn,
        )?;
        let mut rows = Vec::new();
        for id in ids {
            crate::engine::cancel::check_cancelled()?;
            if let Some(row) = self.read_at(id, txn)? {
                if uint(&row.values[ROLE])? == role
                    && uint(&row.values[OWNER])? == owner
                    && bytes(&row.values[TOKEN])? == key
                {
                    rows.push(row);
                }
            }
        }
        Ok(rows)
    }

    fn read_at(&self, row: RowId, txn: TxnView) -> Result<Option<StoredRow>, QueryError> {
        self.db.read_row_at(&self.table, self.table.id, row, txn)
    }

    pub fn lookup(&mut self, id: u64) -> Result<Option<ElementRef>, QueryError> {
        if let Some(node) = self.nodes.get(&id) {
            return Ok(*node);
        }
        let node = self.lookup_at(id, self.txn)?.filter(|e| e.node());
        // The cache is only an acceleration for one statement's fixed view.
        self.charge(64)?;
        self.nodes.insert(id, node);
        Ok(node)
    }

    pub fn lookup_reached_node(&mut self, id: u64) -> Result<Option<ElementRef>, QueryError> {
        if let Some(node) = self.nodes.get(&id) {
            return Ok(*node);
        }
        let node = self.visible_node_root(id)?;
        self.charge(64)?;
        self.nodes.insert(id, node);
        Ok(node)
    }

    fn visible_node_root(&self, id: u64) -> Result<Option<ElementRef>, QueryError> {
        if let Some(row) = self
            .read_cache()
            .and_then(|cache| cache.locations.try_read()?.find(id))
        {
            return Ok(Some(ElementRef {
                id,
                role: NODE,
                row,
            }));
        }
        Ok(self
            .db
            .lookup_unique_index_candidate_one(
                &self.table,
                self.index("PRIMARY"),
                IndexKey(smallvec![KeyPart::UInt(id)]),
                self.txn,
            )?
            .map(|row| ElementRef {
                id,
                role: NODE,
                row,
            }))
    }

    fn lookup_at(&self, id: u64, txn: TxnView) -> Result<Option<ElementRef>, QueryError> {
        self.db.read_unique_index_candidate(
            &self.table,
            self.index("PRIMARY"),
            IndexKey(smallvec![KeyPart::UInt(id)]),
            txn,
            |row, _| {
                let Some(values) = self.db.read_row_columns_at(
                    self.table.id,
                    &self.table,
                    row,
                    txn,
                    &[ID, ROLE],
                )?
                else {
                    return Ok(None);
                };
                let found = uint(&values[0])?;
                let role = uint(&values[1])?;
                Ok((found == id).then_some(ElementRef { id, role, row }))
            },
        )
    }

    // Every reached ID is already known to be a visible node in this view:
    // edge insert and endpoint delete serialize, and their changes publish
    // through the same transaction. The primary index only supplies row IDs.
    pub fn preload_reached_nodes(
        &mut self,
        ids: impl IntoIterator<Item = u64>,
    ) -> Result<(), QueryError> {
        const WINDOW: usize = 8192;
        let index = self.index("PRIMARY");
        let handle = self.db.cached_index_handle(self.table.id, &index.name)?;
        let mut ids = ids.into_iter();
        let directory = self.read_cache().cloned();
        let mut window = Vec::new();
        loop {
            window.extend(ids.by_ref().take(WINDOW));
            if window.is_empty() {
                break;
            }
            window.retain(|id| !self.nodes.contains_key(id));
            window.sort_unstable();
            window.dedup();
            if let Some(cache) = &directory {
                let locations = cache.locations.try_read();
                let before = window.len();
                window.retain(|&id| {
                    let row = locations.as_ref().and_then(|locations| locations.find(id));
                    let Some(row) = row else {
                        return true;
                    };
                    self.nodes.insert(
                        id,
                        Some(ElementRef {
                            id,
                            role: NODE,
                            row,
                        }),
                    );
                    false
                });
                self.charge((before - window.len()) * 64)?;
            }
            if window.is_empty() {
                continue;
            }
            let keys: Vec<_> = window
                .iter()
                .map(|&id| IndexKey(smallvec![KeyPart::UInt(id)]))
                .collect();
            let index = self.index("PRIMARY");
            if window.len() <= MAX_ORDERED_ROOTS {
                if let Some((roots, _)) = self.db.lookup_exact_rows_prefetched(
                    &self.table,
                    index,
                    &keys,
                    self.txn,
                    window.len(),
                )? {
                    self.charge(roots.len() * 64)?;
                    if let Some(directory) = &directory {
                        directory.remember_locations(
                            roots
                                .iter()
                                .map(|&(slot, row)| (window[slot as usize], row)),
                        );
                    }
                    for (slot, row) in roots {
                        let id = window[slot as usize];
                        self.nodes.insert(
                            id,
                            Some(ElementRef {
                                id,
                                role: NODE,
                                row,
                            }),
                        );
                    }
                    window.clear();
                    continue;
                }
            }
            let (entries, _entries_charge) =
                super::super::select::plan::lookup_inlj_entries_parallel(
                    self.db,
                    &handle,
                    &self.table,
                    index,
                    &keys,
                    self.txn,
                )?;
            self.charge(entries.len() * 64)?;
            if let Some(directory) = &directory {
                directory.remember_locations(
                    entries
                        .iter()
                        .map(|&(slot, row)| (window[slot as usize], row)),
                );
            }
            for (slot, row) in entries {
                let id = window[slot as usize];
                self.nodes.insert(
                    id,
                    Some(ElementRef {
                        id,
                        role: NODE,
                        row,
                    }),
                );
            }
            window.clear();
        }
        Ok(())
    }

    pub fn record_survivors(&self, count: usize) {
        if let Some(c) = &self.profile.counters {
            c.predicate_survivors
                .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn projection_has_all(&self, row: RowId, fields: &[&str]) -> bool {
        !self.packed.is_empty() && fields.iter().all(|name| self.packed.has_field(row, name))
    }

    pub fn preload_projection(
        &mut self,
        mut ids: Vec<RowId>,
        field: Option<&str>,
    ) -> Result<(), QueryError> {
        self.clear_projection();
        if let Some(name) = field {
            if !self.packed.is_empty() {
                ids.retain(|id| !self.packed.has_field(*id, name));
            }
        }
        let cache_limit = self.projection_charge.remaining().min(2 * 1024 * 1024);
        let min_batch = if field.is_some() { 2 } else { 8 };
        if ids.len() < min_batch || cache_limit == 0 {
            return Ok(());
        }
        ids.sort_unstable();
        ids.dedup();
        let heap = self.db.cached_heap_handle(self.table.id)?;
        if let Some(field) = field {
            if self.projected_property.as_deref() != Some(field) {
                self.projected_property = Some(field.to_owned());
            }
            return self.preload_projected_field(ids, field, &heap, cache_limit);
        }
        self.projected_property = None;
        // Read heap pages in physical order, then evaluate expressions in the
        // original result order. Duplicated paths share a single payload read.
        for window in ids.chunks(128) {
            if let Some(c) = &self.profile.counters {
                let pages = window.chunk_by(|a, b| a.page_id == b.page_id).count();
                c.heap_page_visits
                    .fetch_add(pages as u64, std::sync::atomic::Ordering::Relaxed);
            }

            crate::engine::cancel::check_cancelled()?;
            let rows = self.db.read_row_columns_many_with_handle(
                &heap,
                &self.table,
                window,
                self.txn,
                &[PAYLOAD],
            )?;
            for (row, mut values) in rows {
                let value = values.pop().unwrap_or(Value::Null);
                let bytes = super::super::mem_budget::estimate_value_bytes(&value) + 32;
                if !self.projection_charge.add_within(bytes, cache_limit) {
                    return Ok(());
                }
                self.projection_payloads.insert(row, value);
            }
        }
        Ok(())
    }

    pub fn clear_projection(&mut self) {
        self.projection_payloads.clear();
        self.projected_values.clear();
        self.projection_charge
            .release_bytes(self.projection_charge.charged());
    }

    fn preload_projected_field(
        &mut self,
        mut ids: Vec<RowId>,
        field: &str,
        heap: &crate::storage::HeapHandle,
        cache_limit: usize,
    ) -> Result<(), QueryError> {
        let directory = self.read_cache().cloned();
        let scn = self.txn.snapshot_scn;
        if let Some(directory) = &directory {
            for addresses in ids.chunks(1024) {
                let hits = {
                    let mut shards: [_; READ_CACHE_SHARDS] = std::array::from_fn(|_| None);
                    let mut probed = 0u16;
                    addresses
                        .iter()
                        .filter_map(|id| {
                            let shard = property_shard(*id);
                            let flag = 1u16 << shard;
                            if probed & flag == 0 {
                                probed |= flag;
                                shards[shard] = directory.projected[shard].try_read();
                            }
                            let cache = shards[shard].as_ref()?.as_ref()?;
                            if cache.scn != scn {
                                return None;
                            }
                            cache.fields.get(field)?.get(*id).map(|value| (*id, value))
                        })
                        .collect::<Vec<_>>()
                };
                self.insert_projected_values(hits, cache_limit);
            }
            if !self.projected_values.is_empty() {
                ids.retain(|id| !self.projected_values.contains_key(id));
            }
        }
        // Scalar predicates borrow the JSON bytes. Give each ordered page
        // read a wider set of positions without accumulating whole payloads.
        for window in ids.chunks(512) {
            if let Some(c) = &self.profile.counters {
                let pages = window.chunk_by(|a, b| a.page_id == b.page_id).count();
                c.heap_page_visits
                    .fetch_add(pages as u64, std::sync::atomic::Ordering::Relaxed);
            }

            crate::engine::cancel::check_cancelled()?;
            let mut inline = Vec::new();
            self.db.visit_inline_json_rows(
                heap,
                &self.table,
                window,
                self.txn,
                PAYLOAD,
                |row, bytes| {
                    inline.push((row, read_inline_property(bytes, field)?));
                    Ok(())
                },
            )?;
            let mut missing = Vec::new();
            if inline.len() < window.len() {
                let mut known = inline.iter().peekable();
                for &id in window {
                    if known.peek().is_some_and(|(row, _)| *row == id) {
                        known.next();
                    } else {
                        missing.push(id);
                    }
                }
            }
            if !missing.is_empty() {
                for (row, values) in self.db.read_row_columns_many_with_handle(
                    heap,
                    &self.table,
                    &missing,
                    self.txn,
                    &[PAYLOAD],
                )? {
                    inline.push((row, read_property(&values[0], field)?));
                }
            }
            if let Some(directory) = &directory {
                directory.remember_properties(scn, field, &inline);
            }
            if !self.insert_projected_values(inline, cache_limit) {
                break;
            }
        }
        Ok(())
    }

    fn insert_projected_values(&mut self, mut values: Vec<(RowId, Json)>, limit: usize) -> bool {
        values.retain(|(_, value)| !value.is_array() && !value.is_object());
        let bytes = values
            .iter()
            .map(|(_, value)| projected_value_bytes(value))
            .sum();
        if self.projection_charge.add_within(bytes, limit) {
            self.projected_values.extend(values);
            return true;
        }
        for (row, value) in values {
            if !self
                .projection_charge
                .add_within(projected_value_bytes(&value), limit)
            {
                return false;
            }
            self.projected_values.insert(row, value);
        }
        true
    }

    pub fn property(&self, element: ElementRef, name: &str) -> Result<Json, QueryError> {
        if let Some(value) = self.packed.property(element.row, name) {
            return Ok(value);
        }
        if self.projected_property.as_deref() == Some(name) {
            if let Some(value) = self.projected_values.get(&element.row) {
                return Ok(value.clone());
            }
        }
        if let Some(value) = self.projection_payloads.get(&element.row) {
            return read_property(value, name);
        }
        let Some(values) = self.db.read_row_columns_at(
            self.table.id,
            &self.table,
            element.row,
            self.txn,
            &[PAYLOAD],
        )?
        else {
            return Ok(Json::Null);
        };
        read_property(&values[0], name)
    }

    pub fn properties(&self, element: ElementRef) -> Result<Map<String, Json>, QueryError> {
        self.read_at(element.row, self.txn)?
            .map_or_else(|| Ok(Map::new()), |row| payload(&row.values[PAYLOAD]))
    }

    pub fn has_label(&self, entry: ElementRef, label: &str) -> Result<bool, QueryError> {
        let role = if entry.node() { NODE_LABEL } else { EDGE_LABEL };
        self.has_label_id(role, entry.id, &token(label)?)
    }

    fn has_label_id(&self, role: u64, id: u64, key: &[u8]) -> Result<bool, QueryError> {
        let full = IndexKey(smallvec![
            KeyPart::UInt(role),
            KeyPart::UInt(0),
            KeyPart::Bytes(key.to_vec()),
            KeyPart::UInt(id)
        ]);
        for row in self
            .db
            .lookup_index_visible(&self.table, self.index("g_key"), full, self.txn)?
        {
            if let Some(row) = self.read_at(row, self.txn)? {
                if uint(&row.values[ROLE])? == role
                    && uint(&row.values[TIE])? == id
                    && bytes(&row.values[TOKEN])? == key
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub fn label_members(
        &self,
        node: bool,
        label: &str,
        mut ids: Vec<u64>,
    ) -> Result<AhHashSet<u64>, QueryError> {
        if ids.is_empty() {
            return Ok(ah_hash_set());
        }
        let role = if node { NODE_LABEL } else { EDGE_LABEL };
        let label = token(label)?;
        ids.sort_unstable();
        ids.dedup();
        let mut members = ah_hash_set();
        let directory = self
            .read_cache()
            .filter(|_| !self.txn.historical && ids.len() >= 16);
        if let Some(slot) = directory.and_then(|cache| cache.memberships.try_read()) {
            if let Some(blocks) = slot
                .as_ref()
                .filter(|s| s.scn == self.txn.snapshot_scn)
                .and_then(|s| s.labels.get(&(role, label.clone())))
            {
                ids.retain(|id| {
                    let bit = 1 << (id % 64);
                    if let Some((known, yes)) = blocks.get(&(id / 64)) {
                        if known & bit != 0 {
                            if yes & bit != 0 {
                                members.insert(*id);
                            }
                            return false;
                        }
                    }
                    true
                });
            }
        }
        if ids.is_empty() {
            return Ok(members);
        }
        let filtered = if ids.len() >= 64 {
            match self.label_extent(role, &label)? {
                Some(None) => return Ok(members),
                Some(Some((low, high))) => {
                    let start = ids.partition_point(|id| *id < low);
                    let end = ids.partition_point(|id| *id <= high);
                    &ids[start..end]
                }
                None => &ids,
            }
        } else {
            &ids
        };
        if let Some(c) = &self.profile.counters {
            c.label_probes
                .fetch_add(filtered.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        for window in filtered.chunks(1024) {
            crate::engine::cancel::check_cancelled()?;
            if window.len() >= 16 && !self.txn.historical {
                let keys: Vec<_> = window
                    .iter()
                    .map(|&id| {
                        IndexKey(smallvec![
                            KeyPart::UInt(role),
                            KeyPart::UInt(0),
                            KeyPart::Bytes(label.clone()),
                            KeyPart::UInt(id),
                        ])
                    })
                    .collect();
                let entries = self.db.lookup_exact_rows_prefetched(
                    &self.table,
                    self.index("g_key"),
                    &keys,
                    self.txn,
                    window.len(),
                )?;
                if let Some((entries, bound)) = entries {
                    // A label membership is inserted or deleted as a whole.
                    // Its key never changes in place, so only visibility is needed.
                    let visible = self.index_visibility_mask(
                        "g_key",
                        bound,
                        entries.iter().map(|&(_, row)| Ok(row)),
                    )?;
                    for (slot, (key, _)) in entries.into_iter().enumerate() {
                        if visible.as_ref().is_none_or(|mask| mask[slot]) {
                            members.insert(window[key as usize]);
                        }
                    }
                    continue;
                }
            }
            for &id in window {
                if self.has_label_id(role, id, &label)? {
                    members.insert(id);
                }
            }
        }
        if let Some(mut slot) = directory.and_then(|cache| cache.memberships.try_write()) {
            if slot
                .as_ref()
                .is_none_or(|cache| cache.scn < self.txn.snapshot_scn)
            {
                if let Some(charge) = ControlledMemoryCharge::try_new_optional(
                    ControlledMemoryCategory::DerivedCache,
                    0,
                ) {
                    *slot = Some(LabelMemberships {
                        scn: self.txn.snapshot_scn,
                        labels: ah_hash_map(),
                        charge,
                    });
                }
            }
            if let Some(cache) = slot
                .as_mut()
                .filter(|cache| cache.scn == self.txn.snapshot_scn)
            {
                cache.remember(role, &label, filtered, &members);
            }
        }
        Ok(members)
    }

    pub fn adjacency_bounds(
        &self,
        edge_label: Option<&String>,
        endpoint_label: Option<&String>,
    ) -> Result<Option<AdjacencyBounds>, QueryError> {
        let extent = |role, label: Option<&String>| {
            if let Some(label) = label.filter(|label| label.len() <= MAX_KEY_BYTES) {
                self.label_extent(role, label.as_bytes())
            } else {
                Ok(None)
            }
        };
        let edge = extent(EDGE_LABEL, edge_label)?;
        let endpoint = extent(NODE_LABEL, endpoint_label)?;
        if edge == Some(None) || endpoint == Some(None) {
            return Ok(None);
        }
        Ok(Some(AdjacencyBounds {
            edge: edge.flatten(),
            endpoint: endpoint.flatten(),
        }))
    }

    // The first and last keys are conservative for visibility: a snapshot's
    // retired keys are merged by the cursor and newer physical keys can
    // only widen the bounds. The point probes still check label visibility.
    fn label_extent(&self, role: u64, label: &[u8]) -> Result<Option<LabelExtent>, QueryError> {
        if self.txn.historical || crate::storage::lock_waits_nonblocking() {
            return Ok(None);
        }
        let Some(cache) = self.read_cache() else {
            return Ok(None);
        };
        let key = (role, label.to_vec());
        let scn = self.txn.snapshot_scn;
        {
            let Some(slot) = cache.label_bounds.try_read() else {
                return Ok(None);
            };
            if let Some(cache) = slot.as_ref() {
                if cache.scn > scn {
                    return Ok(None);
                }
                if cache.scn == scn {
                    if let Some(extent) = cache.bounds.get(&key) {
                        return Ok(Some(*extent));
                    }
                    if cache.bounds.len() >= 128 {
                        return Ok(None);
                    }
                }
            }
        }
        let Some(mut slot) = cache.label_bounds.try_write() else {
            return Ok(None);
        };
        if slot.as_ref().is_some_and(|cache| cache.scn > scn) {
            return Ok(None);
        }
        if slot.as_ref().is_none_or(|cache| cache.scn != scn) {
            let Some(charge) =
                ControlledMemoryCharge::try_new_optional(ControlledMemoryCategory::DerivedCache, 0)
            else {
                return Ok(None);
            };
            *slot = Some(LabelBoundsSnapshot {
                scn,
                bounds: ah_hash_map(),
                charge,
            });
        }
        let state = slot
            .as_mut()
            .expect("label bounds snapshot was initialized");
        if let Some(extent) = state.bounds.get(&key) {
            return Ok(Some(*extent));
        }
        if state.bounds.len() >= 128 {
            return Ok(None);
        }

        let prefix = Some(IndexBound {
            key: IndexKey(smallvec![
                KeyPart::UInt(role),
                KeyPart::UInt(0),
                KeyPart::Bytes(label.to_vec()),
            ]),
            inclusive: true,
        });
        let index = self.index("g_key");
        let mut forward = self.db.range_index_snapshot_cursor(
            &self.table,
            index,
            prefix.clone(),
            prefix.clone(),
            self.txn,
        )?;
        let read_id = |entries: Vec<(IndexKey, RowId)>| {
            entries.first().and_then(|(key, _)| match key.0.last() {
                Some(KeyPart::UInt(id)) => Some(*id),
                _ => None,
            })
        };
        let (entries, _) = forward.next_leaf_chunk(1)?;
        let extent = if entries.is_empty() {
            None
        } else {
            let low = read_id(entries).ok_or_else(|| invalid("Invalid graph label index key"))?;
            let mut reverse = self.db.range_index_snapshot_reverse_cursor(
                &self.table,
                index,
                prefix.clone(),
                prefix,
                self.txn,
            )?;
            let Some(high) = read_id(reverse.next_leaf_chunk(1)?.0).filter(|high| *high >= low)
            else {
                return Ok(None);
            };
            Some((low, high))
        };
        if state.charge.try_grow_optional(96 + key.1.len()) {
            state.bounds.insert(key, extent);
        }
        Ok(Some(extent))
    }

    fn labels_for(
        &self,
        entry: ElementRef,
        txn: TxnView,
    ) -> Result<Vec<(RowId, String)>, QueryError> {
        let role = if entry.node() { NODE_LABEL } else { EDGE_LABEL };
        self.related(role, entry.id, txn)?
            .into_iter()
            .map(|row| {
                let label = std::str::from_utf8(bytes(&row.values[TOKEN])?)
                    .map_err(|_| invalid("Invalid persisted graph label"))?;
                Ok((row.row_id, label.to_owned()))
            })
            .collect()
    }

    fn related(&self, role: u64, id: u64, txn: TxnView) -> Result<Vec<StoredRow>, QueryError> {
        let ids = self.prefix(
            "g_out",
            IndexKey(smallvec![KeyPart::UInt(role), KeyPart::UInt(id)]),
            txn,
        )?;
        let mut rows = Vec::new();
        for row in ids {
            crate::engine::cancel::check_cancelled()?;
            if let Some(row) = self.read_at(row, txn)? {
                if uint(&row.values[ROLE])? == role && uint(&row.values[A])? == id {
                    rows.push(row);
                }
            }
        }
        Ok(rows)
    }

    pub fn materialize(&self, entry: ElementRef) -> Result<Json, QueryError> {
        let Some(row) = self.read_at(entry.row, self.txn)? else {
            return Ok(Json::Null);
        };
        let labels: Vec<_> = self
            .labels_for(entry, self.txn)?
            .into_iter()
            .map(|(_, label)| label)
            .collect();
        let properties = payload(&row.values[PAYLOAD])?;
        if entry.node() {
            Ok(
                serde_json::json!({ "id": entry.id.to_string(), "labels": labels, "properties": properties }),
            )
        } else {
            Ok(
                serde_json::json!({ "id": entry.id.to_string(), "labels": labels, "properties": properties,
                "source": uint(&row.values[A])?.to_string(), "destination": uint(&row.values[B])?.to_string(),
                "directed": entry.role == DIRECTED }),
            )
        }
    }

    pub fn node_candidates(
        &mut self,
        labels: &[String],
        equality: Option<(&str, &Json)>,
    ) -> Result<(Vec<ElementRef>, Option<usize>, bool), QueryError> {
        if let Some((property, value)) = equality.filter(|_| !labels.is_empty()) {
            if self.read_definitions.is_none() {
                self.read_definitions = Some(self.read_index_definitions()?);
            }
            if let Some(def) = self
                .read_definitions
                .as_deref()
                .unwrap_or_default()
                .iter()
                .find(|def| def.spec.property == property && labels.contains(&def.spec.label))
            {
                if value.is_null() {
                    return Ok((Vec::new(), None, false));
                }
                if let Ok(key) = property_token(def.spec.kind, value) {
                    let label = labels.iter().position(|label| label == &def.spec.label);
                    if def.spec.unique {
                        return Ok((self.unique_indexed_node(def.element.id, key)?, label, true));
                    }
                    if crate::storage::lock_waits_nonblocking() {
                        return Err(crate::storage::lock_would_block_error());
                    }
                    let entries = self.rows_for_key(PROPERTY, def.element.id, key, self.txn)?;
                    return Ok((self.resolve_node_rows(entries)?, label, true));
                }
                // A differently represented numeric predicate can still compare equal.
            }
        }
        if crate::storage::lock_waits_nonblocking() {
            return Err(crate::storage::lock_would_block_error());
        }
        if let Some(label) = labels.first() {
            let entries = self.rows_for_key(NODE_LABEL, 0, token(label)?, self.txn)?;
            return Ok((self.resolve_node_rows(entries)?, Some(0), false));
        }
        let ids = self.prefix("g_out", IndexKey(smallvec![KeyPart::UInt(NODE)]), self.txn)?;
        let mut nodes = Vec::new();
        for id in ids {
            self.charge(std::mem::size_of::<ElementRef>())?;
            if let Some(row) = self.read_at(id, self.txn)? {
                let entry = element(&row)?;
                if entry.node() {
                    nodes.push(entry);
                }
            }
        }
        Ok((nodes, None, false))
    }

    fn unique_indexed_node(
        &mut self,
        owner: u64,
        token: Vec<u8>,
    ) -> Result<Vec<ElementRef>, QueryError> {
        let scn = self.txn.snapshot_scn;
        let directory = self.read_cache().cloned();
        let cached = directory.as_ref().and_then(|directory| {
            let slot = directory.indexed_nodes.try_read()?;
            let cache = slot.as_ref().filter(|cache| cache.scn == scn)?;
            cache.by_index.get(&owner)?.get(token.as_slice()).copied()
        });
        if let Some(node) = cached {
            self.charge(std::mem::size_of::<ElementRef>())?;
            return Ok(vec![node]);
        }
        let key = IndexKey(smallvec![
            KeyPart::UInt(PROPERTY),
            KeyPart::UInt(owner),
            KeyPart::Bytes(token.clone()),
            KeyPart::UInt(0)
        ]);
        let Some(row) = self.db.lookup_unique_index_candidate_one(
            &self.table,
            self.index("g_key"),
            key,
            self.txn,
        )?
        else {
            return Ok(Vec::new());
        };
        let Some(values) =
            self.db
                .read_row_columns_at(self.table.id, &self.table, row, self.txn, &[A])?
        else {
            return Ok(Vec::new());
        };
        self.charge(std::mem::size_of::<ElementRef>())?;
        let node = self.visible_node_root(uint(&values[0])?)?;
        if let (Some(directory), Some(node)) = (directory, node) {
            directory.remember_indexed_node(scn, owner, token, node);
        }
        Ok(node.into_iter().collect())
    }

    fn resolve_node_rows(
        &mut self,
        entries: Vec<StoredRow>,
    ) -> Result<Vec<ElementRef>, QueryError> {
        let mut nodes = Vec::new();
        for row in entries {
            let id = uint(&row.values[A])?;
            // A visible membership guarantees a visible node. The primary
            // index only supplies its immutable heap root row ID.
            if let Some(node) = self.visible_node_root(id)? {
                nodes.push(node);
            }
        }
        self.charge(nodes.len() * std::mem::size_of::<ElementRef>())?;
        Ok(nodes)
    }

    /// No mask is needed when the page proves all entries visible.
    fn index_visibility_mask(
        &self,
        name: &str,
        bound: u64,
        rows: impl Iterator<Item = Result<RowId, QueryError>>,
    ) -> Result<Option<Vec<bool>>, QueryError> {
        if !self.db.has_pending_index_repairs_for(self.table.id, name)
            && self.db.index_page_all_visible(bound, self.txn)
        {
            return Ok(None);
        }
        let heap = self.db.cached_heap_handle(self.table.id)?;
        let ids = rows.collect::<Result<SmallVec<[RowId; 16]>, _>>()?;
        self.db
            .filter_visible_row_ids_mask_with_handle(&heap, &self.table, &ids, self.txn)
            .map(Some)
    }

    fn lookup_adjacent_batch(
        &self,
        anchors: &[u64],
        direction: Direction,
        vertices_only: bool,
        bounds: AdjacencyBounds,
    ) -> Result<Option<AdjacentBatch>, QueryError> {
        if anchors.is_empty() {
            return Ok(None);
        }
        let mut unique = anchors.to_vec();
        unique.sort_unstable();
        unique.dedup();

        let mut found = ah_hash_map::<u64, AdjacentList>();
        let cache = self.read_cache().cloned();
        if let Some(cache) = &cache {
            let mut shards: [_; READ_CACHE_SHARDS] = std::array::from_fn(|_| None);
            let mut probed = 0u16;
            unique.retain(|&id| {
                let shard = id as usize % READ_CACHE_SHARDS;
                let flag = 1u16 << shard;
                if probed & flag == 0 {
                    probed |= flag;
                    shards[shard] = cache.adjacency[shard].try_read();
                }
                let list = shards[shard]
                    .as_ref()
                    .and_then(|slot| slot.as_ref())
                    .filter(|state| state.scn == self.txn.snapshot_scn)
                    .and_then(|state| state.lists.get(&(id, direction, vertices_only, bounds)));
                let Some(list) = list else {
                    return true;
                };
                found.insert(id, list.clone());
                false
            });
        }

        let mut pending = ah_hash_map::<u64, (usize, NeighborBuffer)>();
        let inline = crate::storage::lock_waits_nonblocking();
        let edge_limit = if inline { MAX_INLINE_EDGES } else { MAX_EDGES };
        let remaining = edge_limit.saturating_sub(self.edges_examined);
        let mut batch_entries = 0;
        for &(name, role) in adjacency_probes(direction)
            .iter()
            .filter(|_| !unique.is_empty())
        {
            let index = self.index(name);
            let keys: Vec<_> = unique
                .iter()
                .map(|&id| IndexKey(smallvec![KeyPart::UInt(role), KeyPart::UInt(id)]))
                .collect();
            let Some((mut entries, bound)) = self.db.lookup_prefixes_prefetched_mapped(
                &self.table,
                index,
                &keys,
                self.txn,
                remaining.saturating_sub(batch_entries).min(65_536),
                |key, row| {
                    adjacency_key(&key).map(|(anchor, node, edge)| (anchor, node, edge, row))
                },
            )?
            else {
                return if inline {
                    Err(crate::storage::lock_would_block_error())
                } else {
                    Ok(None)
                };
            };
            batch_entries += entries.len();
            let invalid_key = || invalid("Invalid graph adjacency index key");
            if bounds != AdjacencyBounds::default() {
                // A label extent is only a negative proof. Drop unrelated
                // entries before visiting their heap pages for visibility.
                entries.retain(|entry| {
                    let Some((anchor, node, edge, _)) = *entry else {
                        return true;
                    };
                    if bounds.contains(edge, node) {
                        return true;
                    }
                    pending
                        .entry(anchor)
                        .or_insert_with(|| (0, NeighborBuffer::new(vertices_only)))
                        .0 += 1;
                    false
                });
            }
            let visible = self.index_visibility_mask(
                name,
                bound,
                entries
                    .iter()
                    .map(|entry| entry.map(|(_, _, _, row)| row).ok_or_else(invalid_key)),
            )?;
            for (slot, entry) in entries.into_iter().enumerate() {
                let (anchor, node, edge, row) = entry.ok_or_else(invalid_key)?;
                let (examined, neighbors) = pending
                    .entry(anchor)
                    .or_insert_with(|| (0, NeighborBuffer::new(vertices_only)));
                *examined += 1;
                if visible.as_ref().is_none_or(|mask| mask[slot]) {
                    if let Some(neighbor) =
                        neighbor_for_key(anchor, node, edge, row, name, role, direction)
                    {
                        neighbors.push(neighbor);
                    }
                }
            }
        }
        for &id in &unique {
            let (examined, neighbors) = pending
                .remove(&id)
                .unwrap_or_else(|| (0, NeighborBuffer::new(vertices_only)));
            found.insert(
                id,
                AdjacentList {
                    payload: neighbors.into_payload(),
                    examined,
                },
            );
        }
        let examined: usize = anchors.iter().map(|id| found[id].examined).sum();
        if examined > remaining {
            if inline {
                return Err(crate::storage::lock_would_block_error());
            }
            return Err(invalid("GQL adjacency examination limit exceeded"));
        }
        if let Some(cache) = cache.filter(|_| !unique.is_empty()) {
            cache.remember_adjacency(
                self.txn.snapshot_scn,
                direction,
                vertices_only,
                bounds,
                &found,
                &unique,
            );
        }
        Ok(Some(AdjacentBatch {
            lists: found,
            examined,
        }))
    }

    pub fn adjacent_window(
        &self,
        anchors: &[u64],
        direction: Direction,
        vertices_only: bool,
        batch_size: usize,
        bounds: AdjacencyBounds,
    ) -> Result<SmallVec<[Option<AdjacentBatch>; 4]>, QueryError> {
        let collect = |part: &[u64]| {
            part.chunks(batch_size)
                .map(|batch| self.lookup_adjacent_batch(batch, direction, vertices_only, bounds))
                .collect::<Result<SmallVec<[Option<AdjacentBatch>; 4]>, _>>()
        };
        if anchors.len() <= batch_size {
            return collect(anchors);
        }
        let Some(worker) = try_graph_read_worker(self.txn) else {
            return collect(anchors);
        };
        let split = (anchors.len() / 2).div_ceil(batch_size) * batch_size;
        let (left, right) = anchors.split_at(split);
        let context = super::super::select::execution_context::ExecutionContext::capture(self.txn);
        crate::util::thread_spawn::scope(|scope| {
            let task = crate::util::thread_spawn::Builder::new()
                .stack_size(2 * 1024 * 1024)
                .spawn_scoped(scope, move || {
                    let _lease = worker;
                    context.run(self.db, || collect(right))
                });
            let Ok(task) = task else {
                return collect(anchors);
            };
            let mut result = collect(left)?;
            result.extend(
                task.join()
                    .map_err(|_| invalid("Graph adjacency worker failed"))??,
            );
            Ok(result)
        })
    }

    /// The index entries belong to transactional edge-label memberships.
    /// An anonymous edge needs only the logical edge and neighbor IDs.
    pub fn labeled_adjacency(
        &mut self,
        anchors: &[u64],
        direction: Direction,
        label: &str,
        bounds: AdjacencyBounds,
    ) -> Result<AhHashMap<u64, Vec<LabeledNeighbor>>, QueryError> {
        let label = token(label)?;
        let mut unique = anchors.to_vec();
        unique.sort_unstable();
        unique.dedup();
        let inline = crate::storage::lock_waits_nonblocking();
        let limit = if inline { MAX_INLINE_EDGES } else { MAX_EDGES };
        let remaining = limit.saturating_sub(self.edges_examined);
        let too_many = || {
            if inline {
                crate::storage::lock_would_block_error()
            } else {
                invalid("GQL adjacency examination limit exceeded")
            }
        };
        let mut found = ah_hash_map::<u64, (usize, Vec<LabeledNeighbor>)>();
        let mut examined = 0;
        for &(adjacency, role) in adjacency_probes(direction) {
            let incoming = adjacency == "g_in";
            let name = if incoming { LABELED_IN } else { LABELED_OUT };
            let index = self.index(name);
            let prefix = |anchor| {
                IndexKey(smallvec![
                    KeyPart::Bytes(label.clone()),
                    KeyPart::UInt(role),
                    KeyPart::UInt(anchor),
                ])
            };
            let keys = unique.iter().map(|&id| prefix(id)).collect::<Vec<_>>();
            let decode = |key: IndexKey, row| match key.0.as_slice() {
                [_, _, KeyPart::UInt(anchor), KeyPart::UInt(node), KeyPart::UInt(edge)] => Some((
                    *anchor,
                    LabeledNeighbor {
                        edge: *edge,
                        role,
                        node: *node,
                    },
                    row,
                )),
                _ => None,
            };
            let consume = |entries: Vec<Option<(u64, LabeledNeighbor, RowId)>>,
                           page_bound,
                           found: &mut AhHashMap<u64, (usize, Vec<LabeledNeighbor>)>,
                           examined: &mut usize|
             -> Result<(), QueryError> {
                *examined += entries.len();
                if *examined > remaining {
                    return Err(too_many());
                }
                let invalid_key = || invalid("Invalid labeled graph adjacency index key");
                let visible = self.index_visibility_mask(
                    name,
                    page_bound,
                    entries
                        .iter()
                        .map(|entry| entry.map(|(_, _, row)| row).ok_or_else(invalid_key)),
                )?;
                for (slot, entry) in entries.into_iter().enumerate() {
                    let (anchor, edge, _) = entry.ok_or_else(invalid_key)?;
                    let (count, neighbors) = found.entry(anchor).or_default();
                    *count += 1;
                    let duplicate_loop = incoming
                        && edge.node == anchor
                        && matches!(direction, Direction::Either | Direction::Undirected);
                    if !duplicate_loop
                        && bounds.contains(edge.edge, edge.node)
                        && visible.as_ref().is_none_or(|mask| mask[slot])
                    {
                        neighbors.push(edge);
                    }
                }
                Ok(())
            };
            if let Some((entries, bound)) = self.db.lookup_prefixes_prefetched_mapped(
                &self.table,
                index,
                &keys,
                self.txn,
                (remaining - examined).min(65_536),
                decode,
            )? {
                consume(entries, bound, &mut found, &mut examined)?;
                continue;
            }
            if inline {
                return Err(crate::storage::lock_would_block_error());
            }
            for key in keys {
                let bound = Some(IndexBound {
                    key,
                    inclusive: true,
                });
                let mut cursor = self.db.range_index_snapshot_cursor(
                    &self.table,
                    index,
                    bound.clone(),
                    bound,
                    self.txn,
                )?;
                loop {
                    crate::engine::cancel::check_cancelled()?;
                    let (entries, page_bound) = cursor.next_leaf_chunk(256)?;
                    if entries.is_empty() {
                        break;
                    }
                    consume(
                        entries
                            .into_iter()
                            .map(|(key, row)| decode(key, row))
                            .collect(),
                        page_bound,
                        &mut found,
                        &mut examined,
                    )?;
                }
            }
        }
        let examined: usize = anchors
            .iter()
            .filter_map(|id| found.get(id))
            .map(|(count, _)| *count)
            .sum();
        if examined > remaining {
            return Err(too_many());
        }
        if let Some(c) = &self.profile.counters {
            c.edge_candidates
                .fetch_add(examined as u64, std::sync::atomic::Ordering::Relaxed);
        }
        self.edges_examined += examined;
        self.charge(examined * std::mem::size_of::<LabeledNeighbor>())?;
        Ok(found
            .into_iter()
            .map(|(id, (_, edges))| (id, edges))
            .collect())
    }

    // Visible label memberships prove the logical edge exists. Its immutable
    // primary key supplies the heap root for reads in the same snapshot.
    pub fn labeled_edge_roots(
        &self,
        edges: impl Iterator<Item = LabeledNeighbor>,
    ) -> Result<AhHashMap<u64, ElementRef>, QueryError> {
        let mut ids: Vec<_> = edges.map(|edge| (edge.edge, edge.role)).collect();
        ids.sort_unstable();
        ids.dedup();
        let keys: Vec<_> = ids
            .iter()
            .map(|(id, _)| IndexKey(smallvec![KeyPart::UInt(*id)]))
            .collect();
        let mut roots = ah_hash_map();
        if let Some((rows, _)) = self.db.lookup_exact_rows_prefetched(
            &self.table,
            self.index("PRIMARY"),
            &keys,
            self.txn,
            ids.len(),
        )? {
            for (slot, row) in rows {
                let (id, role) = ids[slot as usize];
                roots.insert(id, ElementRef { id, role, row });
            }
        } else {
            for (id, _) in &ids {
                if let Some(edge) = self.lookup_at(*id, self.txn)? {
                    roots.insert(*id, edge);
                }
            }
        }
        if roots.len() != ids.len() {
            return Err(invalid("Missing heap root for a visible labeled edge"));
        }
        Ok(roots)
    }

    pub fn account_adjacency_batch(&mut self, batch: &AdjacentBatch) -> Result<(), QueryError> {
        if batch.examined > MAX_EDGES - self.edges_examined {
            return Err(invalid("GQL adjacency examination limit exceeded"));
        }
        if let Some(c) = &self.profile.counters {
            c.edge_candidates
                .fetch_add(batch.examined as u64, std::sync::atomic::Ordering::Relaxed);
        }
        self.edges_examined += batch.examined;
        self.charge(batch.examined * std::mem::size_of::<Neighbor>())
    }

    pub fn adjacent(&mut self, id: u64, direction: Direction) -> Result<Vec<Neighbor>, QueryError> {
        self.adjacent_bounded(id, direction, AdjacencyBounds::default())
    }

    pub fn adjacent_bounded(
        &mut self,
        id: u64,
        direction: Direction,
        bounds: AdjacencyBounds,
    ) -> Result<Vec<Neighbor>, QueryError> {
        if crate::storage::lock_waits_nonblocking() {
            return Err(crate::storage::lock_would_block_error());
        }
        let mut neighbors = Vec::new();
        for &(name, role) in adjacency_probes(direction) {
            let idx = self.index(name);
            let bound = |endpoint: Option<u64>| {
                let mut key = smallvec![KeyPart::UInt(role), KeyPart::UInt(id)];
                if let Some(endpoint) = endpoint {
                    key.push(KeyPart::UInt(endpoint));
                }
                Some(IndexBound {
                    key: IndexKey(key),
                    inclusive: true,
                })
            };
            let mut cursor = self.db.range_index_snapshot_cursor(
                &self.table,
                idx,
                bound(bounds.endpoint.map(|(low, _)| low)),
                bound(bounds.endpoint.map(|(_, high)| high)),
                self.txn,
            )?;
            let mut batch_size = 16;
            loop {
                let (mut entries, page_bound) = cursor.next_leaf_chunk(batch_size)?;
                if entries.is_empty() {
                    break;
                }
                batch_size = 256;
                if let Some(c) = &self.profile.counters {
                    c.edge_candidates
                        .fetch_add(entries.len() as u64, std::sync::atomic::Ordering::Relaxed);
                }
                self.edges_examined += entries.len();
                if self.edges_examined > MAX_EDGES {
                    return Err(invalid("GQL adjacency examination limit exceeded"));
                }
                if bounds.edge.is_some() {
                    entries.retain(|(key, _)| {
                        adjacency_key(key).is_none_or(|(_, node, edge)| bounds.contains(edge, node))
                    });
                }
                self.charge(entries.len() * std::mem::size_of::<Neighbor>())?;
                let visible = self.index_visibility_mask(
                    name,
                    page_bound,
                    entries.iter().map(|(_, row)| Ok(*row)),
                )?;
                for (slot, (key, row)) in entries.into_iter().enumerate() {
                    if visible.as_ref().is_some_and(|mask| !mask[slot]) {
                        continue;
                    }
                    if let Some(neighbor) = decode_neighbor(&key, row, name, role, direction)?.1 {
                        neighbors.push(neighbor);
                    }
                }
            }
        }
        Ok(neighbors)
    }

    fn insert_row(&self, mut values: Vec<Value>) -> Result<ElementRef, QueryError> {
        let role = uint(&values[ROLE])?;
        if values.len() < self.table.columns.len() {
            values.resize(self.table.columns.len(), Value::Null);
        }
        let inserted = self
            .db
            .insert_rows_detailed(self.txn, &self.name, vec![values])?;
        let id = inserted.first_insert_id;
        let row = inserted.row_ids[0];
        Ok(ElementRef { id, role, row })
    }

    pub fn insert_node(
        &mut self,
        labels: &[String],
        props: Map<String, Json>,
    ) -> Result<ElementRef, QueryError> {
        let entry = self.insert_row(vec![
            Value::Null,
            Value::UInt(NODE),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            json(Json::Object(props.clone())),
        ])?;
        self.nodes.remove(&entry.id);
        self.insert_labels(entry, labels, None)?;
        self.refresh_properties(entry, labels, &props)?;
        Ok(entry)
    }

    pub fn insert_edge(
        &mut self,
        mut src: ElementRef,
        mut dst: ElementRef,
        direction: Direction,
        labels: &[String],
        props: Map<String, Json>,
    ) -> Result<ElementRef, QueryError> {
        if !src.node() || !dst.node() {
            return Err(invalid("An edge endpoint must be a node"));
        }
        if direction == Direction::In || (direction == Direction::Undirected && src.id > dst.id) {
            std::mem::swap(&mut src, &mut dst);
        }
        let (first, second) = if src.id < dst.id {
            (src, dst)
        } else {
            (dst, src)
        };
        self.lock_node(first, crate::storage::LockMode::Shared)?;
        if second.id != first.id {
            self.lock_node(second, crate::storage::LockMode::Shared)?;
        }
        let role = if direction == Direction::Undirected {
            UNDIRECTED
        } else {
            DIRECTED
        };
        let entry = self.insert_row(vec![
            Value::Null,
            Value::UInt(role),
            Value::UInt(src.id),
            Value::UInt(dst.id),
            Value::Null,
            Value::Null,
            Value::Null,
            json(Json::Object(props)),
        ])?;
        self.insert_labels(entry, labels, Some((src.id, dst.id)))?;
        Ok(entry)
    }

    fn lock_node(
        &self,
        entry: ElementRef,
        mode: crate::storage::LockMode,
    ) -> Result<(), QueryError> {
        self.db
            .acquire_row_lock_blocking(self.txn.txn_id, self.table.id, entry.row, mode)?;
        let still_current = self
            .read_at(entry.row, self.current())?
            .is_some_and(|row| uint(&row.values[ID]).is_ok_and(|id| id == entry.id));
        if !still_current {
            return Err(invalid(
                "Graph node was deleted by a concurrent transaction",
            ));
        }
        Ok(())
    }

    fn insert_labels(
        &self,
        entry: ElementRef,
        labels: &[String],
        endpoints: Option<(u64, u64)>,
    ) -> Result<(), QueryError> {
        let role = if entry.node() { NODE_LABEL } else { EDGE_LABEL };
        let labels: std::collections::BTreeSet<_> = labels.iter().collect();
        for label in labels {
            let mut row = vec![
                Value::Null,
                Value::UInt(role),
                Value::UInt(entry.id),
                Value::Null,
                Value::UInt(0),
                Value::Bytes(token(label)?),
                Value::UInt(entry.id),
                Value::Null,
            ];
            if let Some((src, dst)) = endpoints.filter(|_| self.table.columns.len() > EDGE_ROLE) {
                row.extend([Value::UInt(src), Value::UInt(dst), Value::UInt(entry.role)]);
            }
            self.insert_row(row)?;
        }
        Ok(())
    }

    pub fn change(
        &mut self,
        entry: ElementRef,
        key: &str,
        value: Option<Json>,
        label: bool,
    ) -> Result<(), QueryError> {
        self.db.acquire_row_lock_blocking(
            self.txn.txn_id,
            self.table.id,
            entry.row,
            crate::storage::LockMode::Exclusive,
        )?;
        let Some(current) = self.read_at(entry.row, self.current())? else {
            return Err(invalid("Graph element is no longer available"));
        };
        if element(&current)? != entry {
            return Err(invalid("Graph element identity changed"));
        }
        self.invalidate_projection(entry.id)?;
        let mut props = payload(&current.values[PAYLOAD])?;
        let mut labels = self.labels_for(entry, self.current())?;
        if label {
            let old = labels.iter().position(|(_, old_label)| old_label == key);
            if value.is_some() && old.is_none() {
                let endpoints = if entry.node() {
                    None
                } else {
                    Some((uint(&current.values[A])?, uint(&current.values[B])?))
                };
                self.insert_labels(entry, &[key.to_owned()], endpoints)?;
                labels.push((entry.row, key.into()));
            } else if let Some(old) = old.filter(|_| value.is_none()) {
                self.delete_row(labels.remove(old).0)?;
            }
        } else {
            match value {
                Some(value) => {
                    props.insert(key.into(), value);
                }
                None => {
                    props.remove(key);
                }
            }
            let encoded = json(Json::Object(props.clone()));
            self.db
                .update_row_by_id(self.txn, &self.table, entry.row, |values| {
                    let mut updated = values.to_vec();
                    updated[PAYLOAD] = encoded.clone();
                    Ok(updated)
                })?;
        }
        if entry.node() {
            let names = labels
                .into_iter()
                .map(|(_, label)| label)
                .collect::<Vec<_>>();
            self.refresh_properties(entry, &names, &props)?;
        }
        Ok(())
    }

    fn delete_row(&self, row: RowId) -> Result<bool, QueryError> {
        self.db
            .delete_row_by_id_at(self.txn, &self.table, self.table.id, row)
    }

    pub fn delete(&mut self, entry: ElementRef, detach: bool) -> Result<bool, QueryError> {
        self.db.acquire_row_lock_blocking(
            self.txn.txn_id,
            self.table.id,
            entry.row,
            crate::storage::LockMode::Exclusive,
        )?;
        let current = self.current();
        let Some(record) = self.read_at(entry.row, current)? else {
            return Ok(false);
        };
        if element(&record)? != entry {
            return Ok(false);
        }
        if entry.node() {
            let mut edges = HashSet::new();
            for (index, field) in [("g_out", A), ("g_in", B)] {
                for role in [DIRECTED, UNDIRECTED] {
                    let ids = self.prefix(
                        index,
                        IndexKey(smallvec![KeyPart::UInt(role), KeyPart::UInt(entry.id)]),
                        current,
                    )?;
                    if ids.len() > MAX_EDGES.saturating_sub(self.edges_examined) {
                        return Err(invalid("GQL adjacency examination limit exceeded"));
                    }
                    self.edges_examined += ids.len();
                    self.charge(ids.len() * std::mem::size_of::<ElementRef>())?;
                    for id in ids {
                        if let Some(row) = self.read_at(id, current)? {
                            if uint(&row.values[ROLE])? == role
                                && uint(&row.values[field])? == entry.id
                            {
                                edges.insert(element(&row)?);
                            }
                        }
                    }
                }
            }
            if !detach && !edges.is_empty() {
                return Err(invalid(
                    "Cannot delete a graph node with incident edges. Use DETACH DELETE.",
                ));
            }
            let mut edges: Vec<_> = edges.into_iter().collect();
            edges.sort_unstable();
            for edge in edges {
                self.delete_edge(edge)?;
            }
            for row in self.related(PROPERTY, entry.id, current)? {
                self.delete_row(row.row_id)?;
            }
            self.nodes.remove(&entry.id);
        }
        self.invalidate_projection(entry.id)?;
        self.delete_labels(entry)?;
        self.delete_row(entry.row)
    }

    fn delete_edge(&self, edge: ElementRef) -> Result<(), QueryError> {
        self.db.acquire_row_lock_blocking(
            self.txn.txn_id,
            self.table.id,
            edge.row,
            crate::storage::LockMode::Exclusive,
        )?;
        self.invalidate_projection(edge.id)?;
        self.delete_labels(edge)?;
        self.delete_row(edge.row)?;
        Ok(())
    }

    fn delete_labels(&self, entry: ElementRef) -> Result<(), QueryError> {
        for (row, _) in self.labels_for(entry, self.current())? {
            self.delete_row(row)?;
        }
        Ok(())
    }

    fn read_index_definitions(&self) -> Result<Arc<[Definition]>, QueryError> {
        let directory = self.read_cache().cloned();
        let scn = self.txn.snapshot_scn;
        if let Some(slot) = directory
            .as_ref()
            .and_then(|cache| cache.index_definitions.try_read())
        {
            if let Some(cache) = slot.as_ref().filter(|cache| cache.scn == scn) {
                return Ok(cache.definitions.clone());
            }
        }
        if crate::storage::lock_waits_nonblocking() {
            return Err(crate::storage::lock_would_block_error());
        }
        let definitions = Arc::<[Definition]>::from(self.definitions_at(self.txn)?);
        let bytes = std::mem::size_of_val(definitions.as_ref())
            + definitions
                .iter()
                .map(|def| def.spec.property.capacity() + def.spec.label.capacity())
                .sum::<usize>();
        if bytes <= 16 * 1024 {
            if let Some(mut slot) = directory
                .as_ref()
                .and_then(|cache| cache.index_definitions.try_write())
            {
                if slot.as_ref().is_none_or(|cache| cache.scn < scn) {
                    if let Some(charge) = ControlledMemoryCharge::try_new_optional(
                        ControlledMemoryCategory::DerivedCache,
                        bytes,
                    ) {
                        *slot = Some(IndexDefinitionsSnapshot {
                            scn,
                            definitions: definitions.clone(),
                            _charge: charge,
                        });
                    }
                }
            }
        }
        Ok(definitions)
    }

    fn definitions_at(&self, txn: TxnView) -> Result<Vec<Definition>, QueryError> {
        let ids = self.prefix(
            "g_key",
            IndexKey(smallvec![KeyPart::UInt(INDEX), KeyPart::UInt(0)]),
            txn,
        )?;
        let mut definitions = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(row) = self.read_at(id, txn)? {
                if uint(&row.values[ROLE])? != INDEX {
                    continue;
                }
                let props = payload(&row.values[PAYLOAD])?;
                let spec = serde_json::from_value(Json::Object(props))
                    .map_err(|_| invalid("Invalid graph property index descriptor"))?;
                definitions.push(Definition {
                    element: element(&row)?,
                    spec,
                });
            }
        }
        Ok(definitions)
    }

    fn refresh_properties(
        &mut self,
        entry: ElementRef,
        labels: &[String],
        props: &Map<String, Json>,
    ) -> Result<(), QueryError> {
        if self.definitions.is_none() {
            self.definitions = Some(self.definitions_at(self.current())?);
        }
        let definitions = self.definitions.as_deref().unwrap_or_default();
        if definitions.is_empty() {
            return Ok(());
        }
        let existing = self.related(PROPERTY, entry.id, self.current())?;
        for def in definitions {
            let want = props
                .get(&def.spec.property)
                .filter(|value| !value.is_null() && labels.contains(&def.spec.label))
                .map(|value| property_token(def.spec.kind, value))
                .transpose()?;
            let old = existing
                .iter()
                .find(|row| uint(&row.values[OWNER]).is_ok_and(|id| id == def.element.id));
            if let Some(old) = old {
                if want.as_deref() == Some(bytes(&old.values[TOKEN])?) {
                    continue;
                }
                self.delete_row(old.row_id)?;
            }
            if let Some(key) = want {
                self.insert_property(entry, def, key)?;
            }
        }
        Ok(())
    }

    fn insert_property(
        &self,
        node: ElementRef,
        def: &Definition,
        key: Vec<u8>,
    ) -> Result<(), QueryError> {
        let tie = if def.spec.unique { 0 } else { node.id };
        self.insert_row(vec![
            Value::Null,
            Value::UInt(PROPERTY),
            Value::UInt(node.id),
            Value::UInt(def.element.id),
            Value::UInt(def.element.id),
            Value::Bytes(key),
            Value::UInt(tie),
            Value::Null,
        ])?;
        Ok(())
    }

    pub fn create_index(&mut self, name: &str, spec: PropertyIndex) -> Result<(), QueryError> {
        let data =
            serde_json::to_value(&spec).map_err(|_| invalid("Invalid graph index definition"))?;
        let entry = self.insert_row(vec![
            Value::Null,
            Value::UInt(INDEX),
            Value::Null,
            Value::Null,
            Value::UInt(0),
            Value::Bytes(token(name)?),
            Value::UInt(0),
            json(data),
        ])?;
        let def = Definition {
            element: entry,
            spec,
        };
        for row in self.rows_for_key(NODE_LABEL, 0, token(&def.spec.label)?, self.txn)? {
            crate::engine::cancel::check_cancelled()?;
            if let Some(node) = self.lookup(uint(&row.values[A])?)? {
                if let Some(value) = self
                    .properties(node)?
                    .get(&def.spec.property)
                    .filter(|value| !value.is_null())
                {
                    self.insert_property(node, &def, property_token(def.spec.kind, value)?)?;
                }
            }
        }
        Ok(())
    }

    pub fn drop_index(&self, name: &str) -> Result<(), QueryError> {
        let mut descriptors = self.rows_for_key(INDEX, 0, token(name)?, self.txn)?;
        let record = descriptors
            .pop()
            .ok_or_else(|| invalid("Graph property index does not exist"))?;
        let id = uint(&record.values[ID])?;
        let rows = self.prefix(
            "g_in",
            IndexKey(smallvec![KeyPart::UInt(PROPERTY), KeyPart::UInt(id)]),
            self.txn,
        )?;
        for row in rows {
            crate::engine::cancel::check_cancelled()?;
            if let Some(record) = self.read_at(row, self.txn)? {
                if uint(&record.values[ROLE])? == PROPERTY && uint(&record.values[OWNER])? == id {
                    self.delete_row(row)?;
                }
            }
        }
        self.delete_row(record.row_id)?;
        Ok(())
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[test]
    fn property_graph_lazy_locators_cover_sparse_ids_and_bitmap_boundaries() {
        let mut map = NodeLocations {
            blocks: ah_hash_map(),
            charge: ControlledMemoryCharge::try_new_optional(
                ControlledMemoryCategory::DerivedCache,
                0,
            )
            .unwrap(),
        };
        let ids = [u64::MAX, 256, 64, 2, 0, 127, 63, 255, 128, 192, 191, 65536];
        for (i, id) in ids.into_iter().enumerate() {
            map.remember(
                id,
                RowId {
                    page_id: i as u32,
                    slot_id: u16::MAX,
                },
            );
        }
        for (i, id) in ids.into_iter().enumerate() {
            map.remember(
                id,
                RowId {
                    page_id: u32::MAX,
                    slot_id: 0,
                },
            );
            assert_eq!(
                map.find(id),
                Some(RowId {
                    page_id: i as u32,
                    slot_id: u16::MAX
                })
            );
        }
        for id in [1, 65, 126, 129, 257, u64::MAX - 1] {
            assert_eq!(map.find(id), None);
        }
    }

    #[test]
    fn property_graph_shared_read_cache_requires_a_read_only_transaction_slot() {
        let path = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let db = Arc::new(Database::open(path.path()).unwrap());
        let name = "test.__mydb_graph_visibility";
        create(&db, name).unwrap();
        let table = db.graph_descriptor(name).unwrap();
        let read = db.begin_read_only_txn().unwrap();
        let writer = db.begin_txn().unwrap();
        for (id, expected) in [(read.txn_id, true), (writer.txn_id, false)] {
            let view = db.session_statement_view(
                id,
                IsolationLevel::RepeatableRead,
                Some(read.snapshot_scn),
            );
            assert!(view.read_only);
            let store = GraphStore::new(&db, table.clone(), view).unwrap();
            assert_eq!(store.read_cache().is_some(), expected);
        }
        db.rollback_txn(writer.txn_id).unwrap();
        db.rollback_txn(read.txn_id).unwrap();
    }
}
