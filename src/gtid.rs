//! Shared GTID interval operations for the source and replica.
//! Input validation remains the responsibility of each protocol boundary.

use std::collections::BTreeMap;

/// A normalized set reused across a dump connection or a history scan.
#[derive(Debug, Default)]
pub(crate) struct GtidSet(BTreeMap<String, Vec<(u64, u64)>>);

impl GtidSet {
    pub(crate) fn parse(value: &str) -> Self {
        Self(parse_gtid_interval_map(value))
    }

    pub(crate) fn contains(&self, gtid: &str) -> bool {
        if self.0.is_empty() {
            return false;
        }
        let Some((sid, gno)) = gtid.split_once(':') else {
            return false;
        };
        let Ok(gno) = gno.parse::<u64>() else {
            return false;
        };
        let Some(intervals) = self.0.get(sid).or_else(|| {
            if sid.bytes().any(|byte| byte.is_ascii_uppercase()) {
                self.0.get(&sid.to_ascii_lowercase())
            } else {
                None
            }
        }) else {
            return false;
        };
        let idx = intervals.partition_point(|(_, hi)| *hi < gno);
        intervals.get(idx).is_some_and(|(lo, _)| *lo <= gno)
    }
}

pub(crate) fn append_gtid(set: &str, gtid: &str) -> String {
    let Some((uuid, gno)) = parse_single_gtid(gtid) else {
        return set.to_string();
    };
    let mut parsed = parse_gtid_interval_map(set);
    let intervals = parsed.entry(uuid).or_default();
    intervals.push((gno, gno));
    merge_intervals(intervals);
    format_gtid_interval_map(&parsed)
}

// Used by the receiver and the standalone lookup comparison benchmark.
#[cfg_attr(not(feature = "mysql-replica"), allow(dead_code))]
pub(crate) fn gtid_set_contains(set: &str, gtid: &str) -> bool {
    let Some((uuid, gno)) = parse_single_gtid(gtid) else {
        return false;
    };
    parse_gtid_interval_map(set)
        .get(&uuid)
        .is_some_and(|intervals| intervals.iter().any(|(lo, hi)| *lo <= gno && gno <= *hi))
}

pub(crate) fn parse_single_gtid(gtid: &str) -> Option<(String, u64)> {
    let mut parts = gtid.trim().split(':');
    let uuid = parts.next()?.to_ascii_lowercase();
    let gno = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((uuid, gno))
}

pub(crate) fn parse_gtid_interval_map(set: &str) -> BTreeMap<String, Vec<(u64, u64)>> {
    let mut out: BTreeMap<String, Vec<(u64, u64)>> = BTreeMap::new();
    for sid in set
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let mut parts = sid.split(':');
        let Some(uuid) = parts.next() else {
            continue;
        };
        let intervals = out.entry(uuid.to_ascii_lowercase()).or_default();
        for part in parts {
            if let Some((lo, hi)) = part.split_once('-') {
                if let (Ok(lo), Ok(hi)) = (lo.parse::<u64>(), hi.parse::<u64>()) {
                    intervals.push((lo, hi));
                }
            } else if let Ok(gno) = part.parse::<u64>() {
                intervals.push((gno, gno));
            }
        }
        merge_intervals(intervals);
    }
    out
}

pub(crate) fn merge_intervals(intervals: &mut Vec<(u64, u64)>) {
    intervals.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(intervals.len());
    for (lo, hi) in intervals.drain(..) {
        if let Some((_, last_hi)) = merged.last_mut() {
            if lo <= last_hi.saturating_add(1) {
                *last_hi = (*last_hi).max(hi);
                continue;
            }
        }
        merged.push((lo, hi));
    }
    *intervals = merged;
}

pub(crate) fn format_gtid_interval_map(map: &BTreeMap<String, Vec<(u64, u64)>>) -> String {
    map.iter()
        .filter(|(_, intervals)| !intervals.is_empty())
        .map(|(uuid, intervals)| {
            let intervals = intervals
                .iter()
                .map(|(lo, hi)| {
                    if lo == hi {
                        lo.to_string()
                    } else {
                        format!("{lo}-{hi}")
                    }
                })
                .collect::<Vec<_>>()
                .join(":");
            format!("{uuid}:{intervals}")
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_normalization_and_append_are_shared_by_source_and_receiver() {
        let set = "B:9:1-3:4-8,A:3:1-2";
        assert_eq!(
            format_gtid_interval_map(&parse_gtid_interval_map(set)),
            "a:1-3,b:1-9"
        );
        assert_eq!(append_gtid(set, "A:4"), "a:1-4,b:1-9");
        assert!(gtid_set_contains(set, "B:5"));
        assert!(!gtid_set_contains(set, "B:10"));
        let max = u64::MAX;
        let mut intervals = vec![(max, max), (max - 1, max - 1)];
        merge_intervals(&mut intervals);
        assert_eq!(intervals, [(max - 1, max)]);
    }
}
