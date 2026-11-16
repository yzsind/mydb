use super::grants::AuthRow;
use super::{AccountKey, AuthRows};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
pub(super) type RowIndex<T> = imbl::HashMap<AccountKey, Arc<Vec<(usize, Arc<T>)>>>;
pub(super) fn refresh<T: AuthRow>(
    rows: &AuthRows<T>,
    before: Option<&AuthRows<T>>,
    old: Option<&RowIndex<T>>,
    key: impl Fn(&T) -> &AccountKey,
) -> RowIndex<T> {
    if let (Some(before), Some(old)) = (before, old) {
        let mut index = old.clone();
        let dirty: BTreeSet<_> = rows.changed_slots().collect();
        let mut affected: BTreeMap<AccountKey, Vec<(usize, Arc<T>)>> = BTreeMap::new();
        for i in &dirty {
            if let Some(row) = before.get(*i) {
                affected.entry(key(row).clone()).or_default();
            }
            if let Some(row) = rows.shared(*i) {
                affected
                    .entry(key(&row).clone())
                    .or_default()
                    .push((*i, row));
            }
        }
        for (principal, additions) in affected {
            let mut entries = old
                .get(&principal)
                .map(|v| v.as_ref().clone())
                .unwrap_or_default();
            entries.retain(|(i, _)| !dirty.contains(i));
            entries.extend(additions);
            if entries.is_empty() {
                index.remove(&principal);
            } else {
                index.insert(principal, Arc::new(entries));
            }
        }
        index
    } else {
        let mut index: RowIndex<T> = imbl::HashMap::new();
        for (i, row) in rows.shared_rows() {
            Arc::make_mut(index.entry(key(&row).clone()).or_default()).push((i, row));
        }
        index
    }
}
