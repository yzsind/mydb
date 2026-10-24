//! Lossless run-length storage for statement diagnostics. Bulk producers often
//! emit the same condition for every row; retaining those conditions must not
//! require one allocation-sized record per input row.
use super::SqlCondition;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Run {
    condition: SqlCondition,
    count: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SqlConditions {
    runs: Vec<Run>,
    len: usize,
}

impl SqlConditions {
    pub const fn new() -> Self {
        Self {
            runs: Vec::new(),
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn clear(&mut self) {
        self.runs.clear();
        self.len = 0;
    }

    pub fn push(&mut self, condition: SqlCondition) {
        self.push_run(condition, 1);
    }

    fn push_run(&mut self, condition: SqlCondition, count: usize) {
        if count == 0 {
            return;
        }
        self.len += count;
        if let Some(last) = self
            .runs
            .last_mut()
            .filter(|last| last.condition == condition)
        {
            last.count += count;
        } else {
            self.runs.push(Run { condition, count });
        }
    }

    pub fn append(&mut self, other: &mut Self) {
        if self.is_empty() {
            std::mem::swap(self, other);
            return;
        }
        for run in other.runs.drain(..) {
            self.push_run(run.condition, run.count);
        }
        other.len = 0;
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &SqlCondition> + Clone {
        self.runs
            .iter()
            .flat_map(|run| std::iter::repeat_n(&run.condition, run.count))
    }

    /// Count matching logical conditions without expanding repeated runs.
    pub fn count_matching(&self, matches: impl Fn(&SqlCondition) -> bool) -> usize {
        self.runs
            .iter()
            .filter(|run| matches(&run.condition))
            .map(|run| run.count)
            .sum()
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&SqlCondition) -> bool) {
        self.len = 0;
        self.runs.retain_mut(|run| {
            run.count = (0..run.count).filter(|_| keep(&run.condition)).count();
            self.len += run.count;
            run.count != 0
        });
        // Removing a different condition can make two equal runs adjacent.
        let mut written = 0;
        for read in 0..self.runs.len() {
            if written > 0 && self.runs[written - 1].condition == self.runs[read].condition {
                self.runs[written - 1].count += self.runs[read].count;
            } else {
                self.runs.swap(written, read);
                written += 1;
            }
        }
        self.runs.truncate(written);
    }

    pub fn get(&self, mut index: usize) -> Option<&SqlCondition> {
        for run in &self.runs {
            if index < run.count {
                return Some(&run.condition);
            }
            index -= run.count;
        }
        None
    }

    pub fn split_off(&mut self, at: usize) -> Self {
        assert!(at <= self.len);
        let mut position = 0;
        let mut suffix = Self::new();
        for run in &mut self.runs {
            let keep = at.saturating_sub(position).min(run.count);
            position += run.count;
            suffix.push_run(run.condition.clone(), run.count - keep);
            run.count = keep;
        }
        self.runs.retain(|run| run.count != 0);
        self.len = at;
        suffix
    }
}

impl std::ops::Index<usize> for SqlConditions {
    type Output = SqlCondition;
    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("diagnostic index out of bounds")
    }
}

fn expand(run: Run) -> std::iter::RepeatN<SqlCondition> {
    std::iter::repeat_n(run.condition, run.count)
}

pub struct IntoIter {
    inner: std::iter::FlatMap<
        std::vec::IntoIter<Run>,
        std::iter::RepeatN<SqlCondition>,
        fn(Run) -> std::iter::RepeatN<SqlCondition>,
    >,
}
impl Iterator for IntoIter {
    type Item = SqlCondition;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
impl IntoIterator for SqlConditions {
    type Item = SqlCondition;
    type IntoIter = IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self.runs.into_iter().flat_map(expand as fn(Run) -> _),
        }
    }
}
impl Extend<SqlCondition> for SqlConditions {
    fn extend<T: IntoIterator<Item = SqlCondition>>(&mut self, iter: T) {
        for condition in iter {
            self.push(condition);
        }
    }
}
impl FromIterator<SqlCondition> for SqlConditions {
    fn from_iter<T: IntoIterator<Item = SqlCondition>>(iter: T) -> Self {
        let mut result = Self::new();
        result.extend(iter);
        result
    }
}
impl From<Vec<SqlCondition>> for SqlConditions {
    fn from(values: Vec<SqlCondition>) -> Self {
        values.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{catalog, ConditionLevel};

    fn condition(message: &str) -> SqlCondition {
        SqlCondition::known(
            ConditionLevel::Warning,
            catalog::ER_WARN_TOO_MANY_RECORDS,
            message,
        )
    }

    #[test]
    fn bulk_duplicates_stay_compact_across_handoffs() {
        let mut warnings = SqlConditions::new();
        for _ in 0..4096 {
            warnings.push(condition("extra field"));
        }
        for _ in 0..14 {
            let mut batch = warnings.clone();
            warnings.append(&mut batch);
            assert!(batch.is_empty());
        }
        assert_eq!(warnings.len(), 67_108_864);
        assert_eq!(warnings.runs.len(), 1);
        assert!(warnings.runs.capacity() <= 4);
        let mut suffix = warnings.split_off(60_000_000);
        assert_eq!(suffix.len(), 7_108_864);
        assert_eq!(suffix[7_108_863].message(), "extra field");
        warnings.append(&mut suffix);
        assert_eq!(warnings.len(), 67_108_864);
        assert_eq!(warnings.runs.len(), 1);
    }

    #[test]
    fn compression_preserves_order_splits_and_stateful_filtering() {
        let source: Vec<_> = ["a", "a", "b", "b", "b", "c", "a", "a"]
            .into_iter()
            .map(condition)
            .collect();
        for at in 0..=source.len() {
            let mut compact: SqlConditions = source.clone().into();
            assert_eq!(compact.iter().cloned().collect::<Vec<_>>(), source);
            assert_eq!(
                compact.iter().rev().cloned().collect::<Vec<_>>(),
                source.iter().rev().cloned().collect::<Vec<_>>()
            );
            let mut suffix = compact.split_off(at);
            assert_eq!(compact.iter().cloned().collect::<Vec<_>>(), source[..at]);
            assert_eq!(suffix.iter().cloned().collect::<Vec<_>>(), source[at..]);
            compact.append(&mut suffix);
            let mut position = 0;
            compact.retain(|_| {
                position += 1;
                position % 2 == 0
            });
            assert_eq!(
                compact.into_iter().collect::<Vec<_>>(),
                source
                    .iter()
                    .skip(1)
                    .step_by(2)
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn condition_identity_includes_level_code_state_and_message() {
        let warning = condition("same");
        let note = SqlCondition::known(
            ConditionLevel::Note,
            catalog::ER_WARN_TOO_MANY_RECORDS,
            "same",
        );
        let other_code = SqlCondition::known(
            ConditionLevel::Warning,
            catalog::ER_WARN_TOO_FEW_RECORDS,
            "same",
        );
        let conditions = vec![
            warning.clone(),
            note,
            other_code,
            condition("different"),
            warning,
        ];
        let compact: SqlConditions = conditions.clone().into();
        assert_eq!(compact.runs.len(), conditions.len());
        assert_eq!(compact.into_iter().collect::<Vec<_>>(), conditions);
    }
}
