//! Structurally shared grant rows with dense, snapshot-local ordinals.
//!
//! A mutation copies a logarithmic path, never the complete grant catalog.
//! Removed rows are replaced by the last ordinal; both positions enter the
//! delta so readers' principal indexes and DD stable keys update together.
use super::{GrantEntry, RestrictionEntry, RoleEdge};
use imbl::{OrdMap, OrdSet};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ops::{Index, IndexMut};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct AuthRows<T: AuthRow> {
    rows: OrdMap<usize, Arc<T>>,
    changed: OrdSet<usize>,
    revision: u64,
    bytes: usize,
    identity: Arc<()>,
}

pub trait AuthRow: Clone + PartialEq {
    fn memory_bytes(&self) -> usize;
}
impl AuthRow for GrantEntry {
    fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + 128
            + self.grantor.user.capacity()
            + self.grantor.host.capacity()
            + self.grantee.user.capacity()
            + self.grantee.host.capacity()
            + self.privilege.sql_name().len()
            + super::scope_name(&self.scope).len()
    }
}
impl AuthRow for RoleEdge {
    fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + 128
            + self.from_role.user.capacity()
            + self.from_role.host.capacity()
            + self.to_account.user.capacity()
            + self.to_account.host.capacity()
    }
}
impl AuthRow for RestrictionEntry {
    fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + 128
            + self.grantee.user.capacity()
            + self.grantee.host.capacity()
            + self.database.capacity()
            + self.privilege.sql_name().len()
    }
}
pub type GrantStore = AuthRows<GrantEntry>;
pub fn row_bytes<T: AuthRow>(row: &T) -> usize {
    row.memory_bytes()
}
impl<T: AuthRow> Default for AuthRows<T> {
    fn default() -> Self {
        Self {
            rows: OrdMap::new(),
            changed: OrdSet::new(),
            revision: 0,
            bytes: 0,
            identity: Arc::new(()),
        }
    }
}

impl<T: AuthRow> AuthRows<T> {
    pub fn weak(&self) -> std::sync::Weak<()> {
        Arc::downgrade(&self.identity)
    }
    pub fn same_storage(&self, other: &std::sync::Weak<()>) -> bool {
        Arc::as_ptr(&self.identity) == other.as_ptr()
    }
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn capacity(&self) -> usize {
        self.len()
    }
    pub fn memory_bytes(&self) -> usize {
        self.bytes
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn begin_mutation(&mut self) {
        self.changed.clear();
    }
    pub fn changed_slots(&self) -> impl Iterator<Item = usize> + '_ {
        self.changed.iter().copied()
    }
    pub fn get(&self, index: usize) -> Option<&T> {
        self.rows.get(&index).map(Arc::as_ref)
    }
    pub fn shared(&self, index: usize) -> Option<Arc<T>> {
        self.rows.get(&index).cloned()
    }
    pub fn shared_rows(&self) -> impl Iterator<Item = (usize, Arc<T>)> + '_ {
        self.rows.iter().map(|(i, g)| (*i, g.clone()))
    }
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> + '_ {
        self.rows.values().map(Arc::as_ref)
    }
    fn touched(&mut self, index: usize) {
        let _ = Arc::make_mut(&mut self.identity);
        self.changed.insert(index);
        self.revision = self.revision.wrapping_add(1);
    }
    pub fn push(&mut self, grant: T) {
        let index = self.len();
        self.bytes = self.bytes.saturating_add(row_bytes(&grant));
        self.rows.insert(index, Arc::new(grant));
        self.touched(index);
    }
    pub fn replace(&mut self, index: usize, grant: T) {
        self.bytes = self
            .bytes
            .saturating_sub(row_bytes(&self[index]))
            .saturating_add(row_bytes(&grant));
        self.rows.insert(index, Arc::new(grant));
        self.touched(index);
    }
    pub fn swap_remove(&mut self, index: usize) {
        let last = self.len().checked_sub(1).expect("nonempty grant store");
        let removed = self.rows.remove(&index).expect("valid grant ordinal");
        self.bytes = self.bytes.saturating_sub(row_bytes(removed.as_ref()));
        if index != last {
            let tail = self.rows.remove(&last).expect("dense grant ordinals");
            self.rows.insert(index, tail);
        }
        self.touched(index);
        self.touched(last);
    }
    pub fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        let remove: Vec<_> = self
            .rows
            .iter()
            .filter_map(|(i, g)| (!keep(g)).then_some(*i))
            .collect();
        for i in remove.into_iter().rev() {
            self.swap_remove(i);
        }
    }
    pub fn for_each_mut(&mut self, mut f: impl FnMut(&mut T)) {
        for index in 0..self.len() {
            let old = &self[index];
            let mut new = old.clone();
            f(&mut new);
            if &new != old {
                self.replace(index, new);
            }
        }
    }
    pub fn clear(&mut self) {
        self.retain(|_| false);
    }
    pub fn reconcile_bytes(&mut self, before: &Self) {
        self.bytes = before.bytes;
        for i in &self.changed {
            if let Some(g) = before.get(*i) {
                self.bytes = self.bytes.saturating_sub(row_bytes(g));
            }
            if let Some(g) = self.get(*i) {
                self.bytes = self.bytes.saturating_add(row_bytes(g));
            }
        }
    }
}
impl<T: AuthRow> Index<usize> for AuthRows<T> {
    type Output = T;
    fn index(&self, i: usize) -> &T {
        self.get(i).expect("valid grant ordinal")
    }
}
impl<T: AuthRow> IndexMut<usize> for AuthRows<T> {
    fn index_mut(&mut self, i: usize) -> &mut T {
        self.touched(i);
        Arc::make_mut(self.rows.get_mut(&i).expect("valid grant ordinal"))
    }
}
impl<T: AuthRow> FromIterator<T> for AuthRows<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut store = Self::new();
        for row in iter {
            store.push(row);
        }
        store
    }
}
impl<T: AuthRow> From<Vec<T>> for AuthRows<T> {
    fn from(rows: Vec<T>) -> Self {
        rows.into_iter().collect()
    }
}
impl<'a, T: AuthRow> IntoIterator for &'a AuthRows<T> {
    type Item = &'a T;
    type IntoIter = Box<dyn Iterator<Item = &'a T> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}
impl<T: AuthRow> IntoIterator for AuthRows<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter().cloned().collect::<Vec<_>>().into_iter()
    }
}
impl<T: AuthRow + Serialize> Serialize for AuthRows<T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(self.iter())
    }
}
impl<'de, T: AuthRow + Deserialize<'de>> Deserialize<'de> for AuthRows<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Vec::<T>::deserialize(d).map(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::auth::{AccountKey, GrantOrigin, Privilege, PrivilegeScope};
    fn row(n: usize) -> GrantEntry {
        GrantEntry {
            updated_ms: 1234,
            grantor: AccountKey::new("root", "%"),
            grantee: AccountKey::new(format!("u{n}"), "%"),
            privilege: Privilege::Select,
            scope: PrivilegeScope::Table {
                database: "db".into(),
                table: format!("t{n}"),
            },
            grant_option: false,
            origin: GrantOrigin::Explicit,
        }
    }
    #[test]
    fn persistent_grants_delta_and_snapshot_match_vec_model() {
        let mut store: GrantStore = (0..2000).map(row).collect();
        let before = store.clone();
        let mut model: Vec<_> = before.iter().cloned().collect();
        store.begin_mutation();
        let mut rng = 0x5eed_u64;
        for step in 0..5000 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let i = (rng as usize) % model.len();
            match step % 3 {
                0 => {
                    store.swap_remove(i);
                    model.swap_remove(i);
                }
                1 => {
                    let g = row(2000 + step);
                    store.push(g.clone());
                    model.push(g);
                }
                _ => {
                    store[i].grant_option = true;
                    model[i].grant_option = true;
                }
            }
        }
        store.reconcile_bytes(&before);
        assert_eq!(store.iter().cloned().collect::<Vec<_>>(), model);
        assert_eq!(
            before.iter().cloned().collect::<Vec<_>>(),
            (0..2000).map(row).collect::<Vec<_>>()
        );
        assert_eq!(
            store.memory_bytes(),
            store.iter().map(row_bytes).sum::<usize>()
        );
        for i in 0..before.len().max(store.len()) {
            if before.get(i) != store.get(i) {
                assert!(store.changed.contains(&i));
            }
        }
        let decoded: GrantStore =
            serde_json::from_str(&serde_json::to_string(&store).unwrap()).unwrap();
        assert_eq!(decoded.iter().cloned().collect::<Vec<_>>(), model);
    }
}
