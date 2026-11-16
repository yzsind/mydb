//! Shared saturating statement counters for event and digest summaries.
//! Timers use performance_schema picoseconds; sample selection and histogram
//! ownership remain with the collectors.

#[derive(Clone, Copy, Debug)]
pub(super) struct StatementSample {
    pub(super) timer_wait: u64,
    pub(super) lock_time: u64,
    pub(super) errors: u64,
    pub(super) warnings: u64,
    pub(super) rows_affected: u64,
    pub(super) rows_sent: u64,
    pub(super) rows_examined: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StatementMetrics {
    pub(super) count_star: u64,
    pub(super) sum_timer_wait: u64,
    pub(super) min_timer_wait: u64,
    pub(super) max_timer_wait: u64,
    pub(super) sum_lock_time: u64,
    pub(super) sum_errors: u64,
    pub(super) sum_warnings: u64,
    pub(super) sum_rows_affected: u64,
    pub(super) sum_rows_sent: u64,
    pub(super) sum_rows_examined: u64,
}

impl Default for StatementMetrics {
    fn default() -> Self {
        Self {
            count_star: 0,
            sum_timer_wait: 0,
            min_timer_wait: u64::MAX,
            max_timer_wait: 0,
            sum_lock_time: 0,
            sum_errors: 0,
            sum_warnings: 0,
            sum_rows_affected: 0,
            sum_rows_sent: 0,
            sum_rows_examined: 0,
        }
    }
}

impl StatementMetrics {
    #[inline]
    pub(super) fn record(&mut self, sample: StatementSample) {
        self.count_star = self.count_star.saturating_add(1);
        self.sum_timer_wait = self.sum_timer_wait.saturating_add(sample.timer_wait);
        self.min_timer_wait = self.min_timer_wait.min(sample.timer_wait);
        self.max_timer_wait = self.max_timer_wait.max(sample.timer_wait);
        self.sum_lock_time = self.sum_lock_time.saturating_add(sample.lock_time);
        self.sum_errors = self.sum_errors.saturating_add(sample.errors);
        self.sum_warnings = self.sum_warnings.saturating_add(sample.warnings);
        self.sum_rows_affected = self.sum_rows_affected.saturating_add(sample.rows_affected);
        self.sum_rows_sent = self.sum_rows_sent.saturating_add(sample.rows_sent);
        self.sum_rows_examined = self.sum_rows_examined.saturating_add(sample.rows_examined);
    }

    #[inline]
    pub(super) fn merge(&mut self, other: &Self) {
        self.count_star = self.count_star.saturating_add(other.count_star);
        self.sum_timer_wait = self.sum_timer_wait.saturating_add(other.sum_timer_wait);
        self.min_timer_wait = self.min_timer_wait.min(other.min_timer_wait);
        self.max_timer_wait = self.max_timer_wait.max(other.max_timer_wait);
        self.sum_lock_time = self.sum_lock_time.saturating_add(other.sum_lock_time);
        self.sum_errors = self.sum_errors.saturating_add(other.sum_errors);
        self.sum_warnings = self.sum_warnings.saturating_add(other.sum_warnings);
        self.sum_rows_affected = self
            .sum_rows_affected
            .saturating_add(other.sum_rows_affected);
        self.sum_rows_sent = self.sum_rows_sent.saturating_add(other.sum_rows_sent);
        self.sum_rows_examined = self
            .sum_rows_examined
            .saturating_add(other.sum_rows_examined);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merged_partitions_match_recording_and_empty_is_identity() {
        let samples = [
            StatementSample {
                timer_wait: 30,
                lock_time: 3,
                errors: 1,
                warnings: 2,
                rows_affected: 4,
                rows_sent: 5,
                rows_examined: 6,
            },
            StatementSample {
                timer_wait: 10,
                lock_time: 11,
                errors: 0,
                warnings: 7,
                rows_affected: 8,
                rows_sent: 9,
                rows_examined: 10,
            },
        ];
        let mut serial = StatementMetrics::default();
        let mut merged = StatementMetrics::default();
        for sample in samples {
            serial.record(sample);
            let mut partial = StatementMetrics::default();
            partial.record(sample);
            merged.merge(&partial);
            merged.merge(&StatementMetrics::default());
        }
        assert_eq!(serial, merged);
        assert_eq!(
            serial,
            StatementMetrics {
                count_star: 2,
                sum_timer_wait: 40,
                min_timer_wait: 10,
                max_timer_wait: 30,
                sum_lock_time: 14,
                sum_errors: 1,
                sum_warnings: 9,
                sum_rows_affected: 12,
                sum_rows_sent: 14,
                sum_rows_examined: 16,
            }
        );
        let mut zero = StatementMetrics::default();
        zero.record(StatementSample {
            timer_wait: 0,
            ..samples[0]
        });
        merged.merge(&zero);
        assert_eq!(merged.min_timer_wait, 0);
        assert_eq!(merged.count_star, 3);
    }

    #[test]
    fn record_and_merge_saturate_every_additive_counter() {
        let mut totals = StatementMetrics {
            count_star: u64::MAX - 1,
            sum_timer_wait: u64::MAX - 1,
            min_timer_wait: 1,
            max_timer_wait: 1,
            sum_lock_time: u64::MAX - 1,
            sum_errors: u64::MAX - 1,
            sum_warnings: u64::MAX - 1,
            sum_rows_affected: u64::MAX - 1,
            sum_rows_sent: u64::MAX - 1,
            sum_rows_examined: u64::MAX - 1,
        };
        let sample = StatementSample {
            timer_wait: u64::MAX,
            lock_time: u64::MAX,
            errors: u64::MAX,
            warnings: u64::MAX,
            rows_affected: u64::MAX,
            rows_sent: u64::MAX,
            rows_examined: u64::MAX,
        };
        totals.record(sample);
        assert_eq!(
            [
                totals.count_star,
                totals.sum_timer_wait,
                totals.max_timer_wait,
                totals.sum_lock_time,
                totals.sum_errors,
                totals.sum_warnings,
                totals.sum_rows_affected,
                totals.sum_rows_sent,
                totals.sum_rows_examined,
            ],
            [u64::MAX; 9]
        );
        assert_eq!(totals.min_timer_wait, 1);
        let saturated = totals.clone();
        totals.record(sample);
        totals.merge(&saturated);
        assert_eq!(totals, saturated);
    }
}
