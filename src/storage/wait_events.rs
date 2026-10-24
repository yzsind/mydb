use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WaitEvent {
    RowLockContention,
    TableLockContention,
    LogFileSync,
    BufferPoolWait,
    BufferPoolReadIo,
    UndoSpaceWait,
    LatchContention,
}

impl WaitEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            WaitEvent::RowLockContention => "row_lock_contention",
            WaitEvent::TableLockContention => "table_lock_contention",
            WaitEvent::LogFileSync => "log_file_sync",
            WaitEvent::BufferPoolWait => "buffer_pool_wait",
            WaitEvent::BufferPoolReadIo => "buffer_pool_read_io",
            WaitEvent::UndoSpaceWait => "undo_space_wait",
            WaitEvent::LatchContention => "latch_contention",
        }
    }

    pub fn all() -> [WaitEvent; 7] {
        [
            WaitEvent::RowLockContention,
            WaitEvent::TableLockContention,
            WaitEvent::LogFileSync,
            WaitEvent::BufferPoolWait,
            WaitEvent::BufferPoolReadIo,
            WaitEvent::UndoSpaceWait,
            WaitEvent::LatchContention,
        ]
    }
}

#[derive(Debug, Clone)]
pub struct WaitEventSample {
    pub txn_id: u64,
    pub event: WaitEvent,
    pub started_unix_us: u64,
    pub duration_us: u64,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EventAggregate {
    pub count: u64,
    pub total_us: u64,
    pub max_us: u64,
}

#[derive(Debug)]
pub struct WaitEventHistory {
    samples: VecDeque<WaitEventSample>,
    max_samples: usize,
    aggregates: HashMap<WaitEvent, EventAggregate>,
}

impl Default for WaitEventHistory {
    fn default() -> Self {
        Self {
            samples: VecDeque::with_capacity(1024),
            max_samples: 8192,
            aggregates: HashMap::new(),
        }
    }
}

impl WaitEventHistory {
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(max_samples.min(4096)),
            max_samples: max_samples.max(64),
            aggregates: HashMap::new(),
        }
    }

    pub fn record(&mut self, sample: WaitEventSample) {
        let entry = self.aggregates.entry(sample.event).or_default();
        entry.count = entry.count.saturating_add(1);
        entry.total_us = entry.total_us.saturating_add(sample.duration_us);
        if sample.duration_us > entry.max_us {
            entry.max_us = sample.duration_us;
        }
        if self.samples.len() >= self.max_samples {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    pub fn aggregates(&self) -> Vec<(WaitEvent, EventAggregate)> {
        let mut out: Vec<(WaitEvent, EventAggregate)> = self
            .aggregates
            .iter()
            .map(|(event, agg)| (*event, *agg))
            .collect();
        out.sort_by_key(|(event, _)| event.as_str());
        out
    }

    #[cfg(test)]
    pub fn aggregate(&self, event: WaitEvent) -> EventAggregate {
        self.aggregates.get(&event).copied().unwrap_or_default()
    }

    #[cfg(test)]
    pub fn recent(&self, max: usize) -> Vec<WaitEventSample> {
        self.samples
            .iter()
            .rev()
            .take(max)
            .cloned()
            .collect::<Vec<_>>()
    }

    pub fn clear(&mut self) {
        self.samples.clear();
        self.aggregates.clear();
    }
}
