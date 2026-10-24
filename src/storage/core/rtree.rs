//! Derived in-memory R-tree spatial index over geometry MBRs.
//!
//! A genuine R-tree (Guttman quadratic split) mapping each row's geometry
//! minimum bounding rectangle to its [`RowId`]. The tree lives in an arena of
//! nodes ([`Vec<Node>`]). The durable/WAL source of truth is the spatial
//! index's MBR B-tree; this structure is only a governor-accounted cache and
//! is rebuilt from that B-tree when absent or stale.
//!
//! The query primitive is [`RTree::search`] — every row whose stored MBR
//! intersects a query window. This is a correct *superset* for all MySQL
//! spatial-index-driving predicates (MBRContains / MBRWithin / MBRIntersects /
//! ST_Contains / ST_Within / ST_Intersects / …): the executor re-checks the
//! exact predicate against the heap row, so the R-tree only has to avoid false
//! negatives, never false positives.

use crate::engine::geo::Mbr;

use super::RowId;

const MAX_ENTRIES: usize = 16;
const MIN_ENTRIES: usize = 6;

#[inline]
fn pack_row(r: RowId) -> u64 {
    (u64::from(r.page_id) << 32) | u64::from(r.slot_id)
}

#[inline]
fn unpack_row(v: u64) -> RowId {
    RowId {
        page_id: (v >> 32) as u32,
        slot_id: v as u16,
    }
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    mbr: Mbr,
    /// Child node index (internal node) or packed `RowId` (leaf node).
    payload: u64,
}

#[derive(Debug, Clone)]
struct Node {
    leaf: bool,
    entries: Vec<Entry>,
}

impl Node {
    fn new_leaf() -> Self {
        Node {
            leaf: true,
            entries: Vec::new(),
        }
    }

    fn cover(&self) -> Mbr {
        let mut it = self.entries.iter();
        let mut m = it
            .next()
            .map(|e| e.mbr)
            .unwrap_or(Mbr::new(0.0, 0.0, 0.0, 0.0));
        for e in it {
            m = m.union(&e.mbr);
        }
        m
    }
}

/// An in-memory R-tree derived from the durable MBR B-tree.
#[derive(Debug)]
pub(crate) struct RTree {
    nodes: Vec<Node>,
    root: u32,
    len: usize,
}

impl RTree {
    pub(crate) fn new() -> Self {
        RTree {
            nodes: vec![Node::new_leaf()],
            root: 0,
            len: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn memory_capacity_bytes(&self) -> usize {
        self.nodes
            .capacity()
            .saturating_mul(std::mem::size_of::<Node>())
            .saturating_add(
                self.nodes
                    .iter()
                    .map(|node| {
                        node.entries
                            .capacity()
                            .saturating_mul(std::mem::size_of::<Entry>())
                    })
                    .sum::<usize>(),
            )
    }

    /// Bulk-build from `(mbr, row)` pairs (used by backfill / rebuild).
    pub(crate) fn from_entries(items: impl IntoIterator<Item = (Mbr, RowId)>) -> Self {
        let mut t = RTree::new();
        for (mbr, row) in items {
            t.insert(mbr, row);
        }
        t
    }

    // ---- insert ----

    pub(crate) fn insert(&mut self, mbr: Mbr, row: RowId) {
        let path = self.choose_leaf_path(mbr);
        let leaf = *path.last().unwrap() as usize;
        self.nodes[leaf].entries.push(Entry {
            mbr,
            payload: pack_row(row),
        });
        self.len += 1;

        // Walk back up, splitting overfull nodes and inserting the new sibling
        // into the parent.
        let mut carry: Option<Entry> = None;
        for level in (0..path.len()).rev() {
            let node_idx = path[level];
            if let Some(e) = carry.take() {
                self.nodes[node_idx as usize].entries.push(e);
            }
            if self.nodes[node_idx as usize].entries.len() > MAX_ENTRIES {
                let (new_idx, new_cover) = self.split(node_idx);
                carry = Some(Entry {
                    mbr: new_cover,
                    payload: u64::from(new_idx),
                });
            } else {
                break;
            }
        }

        // Root split.
        if let Some(e) = carry {
            let old_root = self.root;
            let old_cover = self.nodes[old_root as usize].cover();
            let new_root = Node {
                leaf: false,
                entries: vec![
                    Entry {
                        mbr: old_cover,
                        payload: u64::from(old_root),
                    },
                    e,
                ],
            };
            self.root = self.nodes.len() as u32;
            self.nodes.push(new_root);
        }
    }

    /// Descend from the root to a leaf, enlarging each chosen child entry's
    /// MBR to cover `mbr` so ancestors always bound their descendants.
    fn choose_leaf_path(&mut self, mbr: Mbr) -> Vec<u32> {
        let mut path = vec![self.root];
        let mut current = self.root;
        loop {
            if self.nodes[current as usize].leaf {
                return path;
            }
            // Pick the child entry needing least enlargement (tie: smallest area).
            let mut best = 0usize;
            let mut best_enl = f64::INFINITY;
            let mut best_area = f64::INFINITY;
            for (i, e) in self.nodes[current as usize].entries.iter().enumerate() {
                let enl = e.mbr.enlargement(&mbr);
                let area = e.mbr.area();
                if enl < best_enl || (enl == best_enl && area < best_area) {
                    best = i;
                    best_enl = enl;
                    best_area = area;
                }
            }
            // Enlarge the chosen entry to include the new mbr.
            let e = &mut self.nodes[current as usize].entries[best];
            e.mbr = e.mbr.union(&mbr);
            current = e.payload as u32;
            path.push(current);
        }
    }

    /// Quadratic split of `node_idx`: keeps roughly half the entries, returns
    /// the index and cover of the freshly created sibling.
    fn split(&mut self, node_idx: u32) -> (u32, Mbr) {
        let leaf = self.nodes[node_idx as usize].leaf;
        let entries = std::mem::take(&mut self.nodes[node_idx as usize].entries);

        let (seed_a, seed_b) = quadratic_seeds(&entries);
        let mut group_a: Vec<Entry> = Vec::new();
        let mut group_b: Vec<Entry> = Vec::new();
        let mut cover_a = entries[seed_a].mbr;
        let mut cover_b = entries[seed_b].mbr;
        group_a.push(entries[seed_a]);
        group_b.push(entries[seed_b]);

        let mut remaining: Vec<Entry> = entries
            .into_iter()
            .enumerate()
            .filter(|(i, _)| *i != seed_a && *i != seed_b)
            .map(|(_, e)| e)
            .collect();

        while !remaining.is_empty() {
            // If one group needs all the rest to reach MIN_ENTRIES, give them.
            let need_a = MIN_ENTRIES.saturating_sub(group_a.len());
            let need_b = MIN_ENTRIES.saturating_sub(group_b.len());
            if remaining.len() <= need_a {
                for e in remaining.drain(..) {
                    cover_a = cover_a.union(&e.mbr);
                    group_a.push(e);
                }
                break;
            }
            if remaining.len() <= need_b {
                for e in remaining.drain(..) {
                    cover_b = cover_b.union(&e.mbr);
                    group_b.push(e);
                }
                break;
            }
            // Pick the entry with the strongest preference, assign it.
            let mut best = 0usize;
            let mut best_diff = -1.0;
            let mut best_to_a = true;
            for (i, e) in remaining.iter().enumerate() {
                let da = cover_a.union(&e.mbr).area() - cover_a.area();
                let db = cover_b.union(&e.mbr).area() - cover_b.area();
                let diff = (da - db).abs();
                if diff > best_diff {
                    best_diff = diff;
                    best = i;
                    best_to_a = da < db;
                }
            }
            let e = remaining.swap_remove(best);
            if best_to_a {
                cover_a = cover_a.union(&e.mbr);
                group_a.push(e);
            } else {
                cover_b = cover_b.union(&e.mbr);
                group_b.push(e);
            }
        }

        self.nodes[node_idx as usize].entries = group_a;
        let new_idx = self.nodes.len() as u32;
        self.nodes.push(Node {
            leaf,
            entries: group_b,
        });
        (new_idx, cover_b)
    }

    // ---- search ----

    /// All rows whose stored MBR intersects `query`.
    pub(crate) fn search(&self, query: &Mbr) -> Vec<RowId> {
        let mut out = Vec::new();
        if self.len == 0 {
            return out;
        }
        let mut stack = vec![self.root];
        while let Some(idx) = stack.pop() {
            let node = &self.nodes[idx as usize];
            if node.leaf {
                for e in &node.entries {
                    if e.mbr.intersects(query) {
                        out.push(unpack_row(e.payload));
                    }
                }
            } else {
                for e in &node.entries {
                    if e.mbr.intersects(query) {
                        stack.push(e.payload as u32);
                    }
                }
            }
        }
        out
    }

    // ---- delete ----

    /// Remove the entry for `row` whose stored MBR matches `mbr`. Underfull
    /// nodes are left as-is (no condense) — correctness is unaffected, and the
    /// tree is periodically rebuilt. Returns true if an entry was removed.
    pub(crate) fn delete(&mut self, mbr: Mbr, row: RowId) -> bool {
        let target = pack_row(row);
        let mut stack = vec![self.root];
        while let Some(idx) = stack.pop() {
            let node = &mut self.nodes[idx as usize];
            if node.leaf {
                if let Some(pos) = node
                    .entries
                    .iter()
                    .position(|e| e.payload == target && e.mbr.equals(&mbr))
                {
                    node.entries.swap_remove(pos);
                    self.len -= 1;
                    return true;
                }
            } else {
                for e in &node.entries {
                    if e.mbr.intersects(&mbr) {
                        stack.push(e.payload as u32);
                    }
                }
            }
        }
        false
    }
}

/// Pick the two entries whose combined bounding box wastes the most area —
/// the Guttman quadratic-split seeds.
fn quadratic_seeds(entries: &[Entry]) -> (usize, usize) {
    let mut a = 0;
    let mut b = 1;
    let mut worst = f64::NEG_INFINITY;
    for i in 0..entries.len() {
        for j in (i + 1)..entries.len() {
            let combined = entries[i].mbr.union(&entries[j].mbr).area();
            let waste = combined - entries[i].mbr.area() - entries[j].mbr.area();
            if waste > worst {
                worst = waste;
                a = i;
                b = j;
            }
        }
    }
    (a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pt(x: f64, y: f64) -> Mbr {
        Mbr::new(x, y, x, y)
    }

    fn rid(n: u32) -> RowId {
        RowId {
            page_id: n,
            slot_id: (n % 7) as u16,
        }
    }

    #[test]
    fn search_matches_brute_force() {
        // Insert a grid of point MBRs plus a few box MBRs.
        let mut items: Vec<(Mbr, RowId)> = Vec::new();
        let mut n = 0u32;
        for x in 0..20 {
            for y in 0..20 {
                items.push((pt(x as f64, y as f64), rid(n)));
                n += 1;
            }
        }
        for k in 0..30 {
            let x = (k % 18) as f64;
            let y = ((k * 7) % 18) as f64;
            items.push((Mbr::new(x, y, x + 2.0, y + 2.0), rid(n)));
            n += 1;
        }
        let tree = RTree::from_entries(items.iter().cloned());
        assert_eq!(tree.len(), items.len());

        // Many query windows; R-tree result must equal brute-force intersect.
        for q in 0..40 {
            let qx = (q % 19) as f64;
            let qy = ((q * 3) % 19) as f64;
            let query = Mbr::new(qx, qy, qx + 3.5, qy + 3.5);
            let mut got = tree.search(&query);
            got.sort();
            let mut expect: Vec<RowId> = items
                .iter()
                .filter(|(m, _)| m.intersects(&query))
                .map(|(_, r)| *r)
                .collect();
            expect.sort();
            assert_eq!(got, expect, "query window {query:?} mismatch");
        }
    }

    #[test]
    fn delete_removes_entries() {
        let items: Vec<(Mbr, RowId)> = (0..50).map(|i| (pt(i as f64, 0.0), rid(i))).collect();
        let mut tree = RTree::from_entries(items.iter().cloned());
        // Delete the even ones.
        for i in (0..50).step_by(2) {
            assert!(tree.delete(pt(i as f64, 0.0), rid(i)));
        }
        assert_eq!(tree.len(), 25);
        let all = tree.search(&Mbr::new(-1.0, -1.0, 100.0, 1.0));
        let mut got: Vec<u32> = all.iter().map(|r| r.page_id).collect();
        got.sort();
        assert_eq!(got, (1..50).step_by(2).collect::<Vec<_>>());
    }
}
