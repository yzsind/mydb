//! Unified read-side memory observability for `SHOW MYDB MEMORY`.
//!
//! The snapshot deliberately separates exact engine charges, estimates and
//! OS measurements. Rows tagged `IN_TRACKED_TOTAL=NO` are rollups or
//! breakdowns and must not be summed again.

use crate::engine::api::{QueryResponse, ResultSet};
use crate::engine::executor::{mem_budget, work_area};
use crate::engine::session_registry::{ProcessInfo, SessionRegistry};
use crate::storage::{Database, PAGE_SIZE};
use crate::types::{Column, ColumnType, Value};

#[derive(Clone, Copy, Debug)]
struct OsMemory {
    host_used: Option<u64>,
    host_total: Option<u64>,
    cgroup_used: Option<u64>,
    cgroup_limit: Option<u64>,
    process_rss: Option<u64>,
    process_private_footprint: Option<u64>,
    process_peak_rss: Option<u64>,
}

#[derive(Debug)]
struct MemoryRow {
    category: &'static str,
    component: String,
    current: Option<i128>,
    peak: Option<u64>,
    limit: Option<u64>,
    hits: Option<u64>,
    misses: Option<u64>,
    entries: Option<u64>,
    evictions: Option<u64>,
    accounting: &'static str,
    in_total: bool,
    scope: &'static str,
    counter_scope: Option<&'static str>,
    parent: Option<&'static str>,
    rejections: Option<u64>,
}

impl MemoryRow {
    fn values(self) -> Vec<Value> {
        let used_pct = match (self.current, self.limit) {
            (Some(current), Some(limit)) if limit > 0 => percentage(current as u64, limit),
            _ => Value::Null,
        };
        let hit_pct = match (self.hits, self.misses) {
            (Some(hits), Some(misses)) if hits.saturating_add(misses) > 0 => {
                percentage(hits, hits.saturating_add(misses))
            }
            _ => Value::Null,
        };
        vec![
            Value::String(self.category.to_string()),
            Value::String(self.component),
            self.current.map_or(Value::Null, |bytes| {
                if bytes < 0 {
                    Value::Int(bytes.max(i64::MIN as i128) as i64)
                } else {
                    Value::UInt(bytes as u64)
                }
            }),
            opt_u64(self.peak),
            opt_u64(self.limit),
            used_pct,
            opt_u64(self.hits),
            opt_u64(self.misses),
            hit_pct,
            opt_u64(self.entries),
            opt_u64(self.evictions),
            Value::String(self.accounting.to_string()),
            Value::String(if self.in_total { "YES" } else { "NO" }.to_string()),
            Value::String(self.scope.into()),
            self.counter_scope
                .map_or(Value::Null, |s| Value::String(s.into())),
            self.parent.map_or(Value::Null, |s| Value::String(s.into())),
            opt_u64(self.rejections),
        ]
    }
}

fn opt_u64(value: Option<u64>) -> Value {
    value.map(Value::UInt).unwrap_or(Value::Null)
}

fn percentage(numerator: u64, denominator: u64) -> Value {
    let pct = numerator as f64 * 100.0 / denominator as f64;
    Value::Double((pct * 10_000.0).round() / 10_000.0)
}

fn col(name: &str, column_type: ColumnType) -> Column {
    Column {
        source: Default::default(),
        name: name.to_string(),
        column_type,
    }
}

fn percent_col(name: &str) -> Column {
    let mut column = col(name, ColumnType::Double);
    column.source.decimals = Some(4);
    column
}

pub(crate) fn summary_columns() -> Vec<Column> {
    vec![
        col("CATEGORY", ColumnType::VarString),
        col("COMPONENT", ColumnType::VarString),
        col("CURRENT_BYTES", ColumnType::LongLong),
        col("PEAK_BYTES", ColumnType::LongLong),
        col("LIMIT_BYTES", ColumnType::LongLong),
        percent_col("USED_PCT"),
        col("HITS", ColumnType::LongLong),
        col("MISSES", ColumnType::LongLong),
        percent_col("HIT_PCT"),
        col("ENTRIES", ColumnType::LongLong),
        col("EVICTIONS", ColumnType::LongLong),
        col("ACCOUNTING", ColumnType::VarString),
        col("IN_TRACKED_TOTAL", ColumnType::VarString),
        col("SCOPE", ColumnType::VarString),
        col("COUNTER_SCOPE", ColumnType::VarString),
        col("PARENT_COMPONENT", ColumnType::VarString),
        col("REJECTIONS", ColumnType::LongLong),
    ]
}

pub(crate) fn session_columns() -> Vec<Column> {
    vec![
        col("CONNECTION_ID", ColumnType::LongLong),
        col("USER", ColumnType::VarString),
        col("HOST", ColumnType::VarString),
        col("DB", ColumnType::VarString),
        col("COMMAND", ColumnType::VarString),
        col("QUERY_ID", ColumnType::LongLong),
        col("EXEC_CURRENT_BYTES", ColumnType::LongLong),
        col("QUERY_PEAK_BYTES", ColumnType::LongLong),
        col("SESSION_PEAK_BYTES", ColumnType::LongLong),
        col("QUERY_LIMIT_BYTES", ColumnType::LongLong),
        col("SPILL_BYTES", ColumnType::LongLong),
        col("RETAINED_CACHE_BYTES", ColumnType::LongLong),
        col("TOTAL_CURRENT_BYTES", ColumnType::LongLong),
        col("STATE", ColumnType::VarString),
    ]
}

pub(crate) fn summary(db: &Database, registry: &SessionRegistry) -> QueryResponse {
    let status = db.storage_status();
    let os = os_memory();
    let (controlled_current, controlled_peak, controlled, _) =
        mem_budget::controlled_memory_snapshot();
    let categories = mem_budget::pfs_memory_category_snapshots();
    let rejections = mem_budget::controlled_memory_rejections_by_category();
    let work = work_area::snapshot();
    let sessions = registry.snapshot();
    let retained_sessions = sessions.iter().fold(0u64, |sum, session| {
        sum.saturating_add(session.retained_cache_bytes)
    });
    let prepared_sessions = sessions.iter().fold(0u64, |sum, session| {
        sum.saturating_add(session.prepared_cache_bytes)
    });
    let cte_sessions = sessions.iter().fold(0u64, |sum, session| {
        sum.saturating_add(session.cte_cache_bytes)
    });
    let page_bytes = PAGE_SIZE as u64;
    let raw_capacity = status.buffer_pool_pages.saturating_mul(page_bytes);
    let raw_children = [
        ("data_raw", status.buffer_pool_pages_heap),
        ("index_raw", status.buffer_pool_pages_index),
        ("lob_raw", status.buffer_pool_pages_lob),
        ("vector_raw", status.buffer_pool_pages_vector),
        ("zone_map_raw", status.buffer_pool_pages_zone_map),
    ];
    let mut rows = Vec::new();
    let mut tracked = 0u64;

    rows.push(row(
        "system",
        "host_memory",
        os.host_used,
        None,
        os.host_total,
        None,
        None,
        None,
        None,
        "OS",
        false,
    ));
    rows.push(row(
        "system",
        "cgroup_memory",
        os.cgroup_used,
        None,
        os.cgroup_limit,
        None,
        None,
        None,
        None,
        "OS",
        false,
    ));
    rows.push(row(
        "system",
        if cfg!(windows) {
            "process_working_set"
        } else {
            "process_rss"
        },
        os.process_rss,
        os.process_peak_rss,
        None,
        None,
        None,
        None,
        None,
        "OS",
        false,
    ));
    rows.push(row(
        "system",
        if cfg!(windows) {
            "process_private_bytes"
        } else {
            "process_private_footprint"
        },
        os.process_private_footprint,
        None,
        Some(mem_budget::managed_memory_limit_bytes() as u64),
        None,
        None,
        None,
        None,
        "OS",
        false,
    ));
    rows.push(row(
        "system",
        "controlled_memory",
        Some(controlled_current as u64),
        Some(controlled_peak as u64),
        Some(mem_budget::operational_memory_limit_bytes() as u64),
        None,
        None,
        None,
        None,
        "ROLLUP",
        false,
    ));

    let raw = controlled[0] as u64;
    tracked = tracked.saturating_add(raw);
    rows.push(row(
        "buffer_pool",
        "raw_total",
        Some(raw),
        None,
        Some(raw_capacity),
        Some(status.buffer_hits),
        Some(status.buffer_misses),
        Some(status.buffer_pool_used_pages),
        Some(status.buffer_pool_evictions),
        "EXACT",
        true,
    ));
    rows.push(row(
        "buffer_pool",
        "raw_active",
        Some(status.buffer_pool_used_pages.saturating_mul(page_bytes)),
        None,
        Some(raw_capacity),
        None,
        None,
        Some(status.buffer_pool_used_pages),
        None,
        "EXACT_BREAKDOWN",
        false,
    ));
    rows.push(row(
        "buffer_pool",
        "raw_recycled",
        Some(
            status
                .buffer_pool_recycled_raw_pages
                .saturating_mul(page_bytes),
        ),
        None,
        Some(raw_capacity),
        Some(status.buffer_pool_raw_page_reuse_hits),
        Some(status.buffer_pool_raw_page_reuse_misses),
        Some(status.buffer_pool_recycled_raw_pages),
        None,
        "EXACT_BREAKDOWN",
        false,
    ));
    for (name, pages) in raw_children {
        rows.push(row(
            "buffer_pool",
            name,
            Some(pages.saturating_mul(page_bytes)),
            None,
            None,
            None,
            None,
            Some(pages),
            None,
            "SUBSET",
            false,
        ));
    }

    let storage_scratch = controlled[1] as u64;
    tracked = tracked.saturating_add(storage_scratch);
    rows.push(row(
        "storage",
        "scratch",
        Some(storage_scratch),
        None,
        Some(mem_budget::operational_memory_limit_bytes() as u64),
        None,
        None,
        None,
        None,
        "EXACT",
        true,
    ));

    rows.push(row(
        "storage",
        "undo_resident",
        Some(status.undo_resident_bytes),
        None,
        nonzero(status.undo_mem_cache_limit_bytes),
        Some(status.undo_cache_hits),
        Some(status.undo_cache_misses),
        None,
        Some(status.undo_evicted_records),
        "ESTIMATE_SUBSET",
        false,
    ));

    let executor = controlled[2] as u64;
    tracked = tracked.saturating_add(executor);
    rows.push(row(
        "executor",
        "query_execution",
        Some(executor),
        Some(mem_budget::process_query_mem_high_water() as u64),
        Some(mem_budget::process_mem_limit_bytes() as u64),
        None,
        None,
        None,
        None,
        "EXACT",
        true,
    ));
    rows.push(row(
        "executor",
        "work_area_reserved",
        Some(work.allocated_bytes as u64),
        Some(work.high_water_bytes as u64),
        Some(work.target_bytes as u64),
        None,
        None,
        Some(work.active_handles),
        None,
        "RESERVATION",
        false,
    ));
    rows.push(row(
        "executor",
        "work_area_used",
        Some(work.used_bytes as u64),
        Some(work.high_water_used_bytes as u64),
        Some(work.target_bytes as u64),
        None,
        None,
        None,
        None,
        "SUBSET",
        false,
    ));

    let derived = controlled[3] as u64;
    tracked = tracked.saturating_add(derived);
    rows.push(row(
        "cache",
        "controlled_derived",
        Some(derived),
        None,
        None,
        None,
        None,
        None,
        None,
        "EXACT",
        true,
    ));

    for (component, index) in [("dml_batch", 4usize), ("ddl", 5usize), ("stats", 6usize)] {
        let bytes = controlled[index] as u64;
        tracked = tracked.saturating_add(bytes);
        rows.push(row(
            "partition",
            component,
            Some(bytes),
            None,
            Some(mem_budget::operational_memory_limit_bytes() as u64),
            None,
            None,
            None,
            None,
            "EXACT",
            true,
        ));
    }

    let ddl_copy = controlled[7] as u64;
    tracked = tracked.saturating_add(ddl_copy);
    rows.push(row(
        "ddl",
        "alter_copy_batch",
        Some(ddl_copy),
        None,
        Some(mem_budget::operational_memory_limit_bytes() as u64),
        None,
        None,
        None,
        None,
        "EXACT",
        true,
    ));

    // Every remaining governor category is a disjoint charge, not a subset.
    for ((category, name), bytes) in [
        ("system", "system_metadata"),
        ("storage", "temporal_history"),
        ("protocol", "protocol_transient"),
        ("replication", "binlog_transient"),
        ("parser", "parser_transient"),
        ("replication", "native_replication_transient"),
        ("instrumentation", "performance_schema"),
        ("ddl", "ddl_online_log"),
    ]
    .into_iter()
    .zip(&controlled[8..])
    {
        let bytes = *bytes as u64;
        tracked = tracked.saturating_add(bytes);
        rows.push(row(
            category,
            name,
            Some(bytes),
            None,
            None,
            None,
            None,
            None,
            None,
            "EXACT",
            true,
        ));
    }

    for cache in crate::engine::cache_registry::collect() {
        // Only disjoint, process-wide estimates can extend the governor total.
        // Governed objects and shared L1/L2 payloads are diagnostic breakdowns.
        let in_total = cache.memory_accounting
            == crate::engine::cache_registry::CacheMemoryAccounting::IndependentEstimate
            && cache.scope == "process"
            && cache.bytes.is_some();
        if in_total {
            tracked = tracked.saturating_add(cache.bytes.unwrap_or(0));
        }
        let mut memory = row(
            "cache",
            cache.name,
            cache.bytes,
            None,
            None,
            Some(cache.hits),
            Some(cache.misses),
            Some(cache.size_entries),
            cache.memory_evictions,
            cache.memory_accounting.label(),
            in_total,
        );
        memory.scope = cache.scope;
        memory.parent = cache.memory_accounting.parent();
        rows.push(memory);
    }

    rows.push(row(
        "session_cache",
        "prepared_total",
        Some(prepared_sessions),
        None,
        None,
        None,
        None,
        None,
        None,
        "ESTIMATE",
        false,
    ));
    rows.push(row(
        "session_cache",
        "cte_result_total",
        Some(cte_sessions),
        None,
        None,
        None,
        None,
        None,
        None,
        "ESTIMATE",
        false,
    ));
    rows.push(row(
        "session_cache",
        "retained_total",
        Some(retained_sessions),
        None,
        None,
        None,
        None,
        None,
        None,
        "ROLLUP",
        false,
    ));
    rows.push(row(
        "system",
        "tracked_engine",
        Some(tracked),
        None,
        None,
        None,
        None,
        None,
        None,
        "DERIVED",
        false,
    ));
    let mut difference = row(
        "system",
        if cfg!(windows) {
            "working_set_minus_tracked"
        } else {
            "rss_minus_tracked"
        },
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "DERIVED",
        false,
    );
    difference.current = os
        .process_rss
        .map(|rss| i128::from(rss) - i128::from(tracked));
    rows.push(difference);

    // Read existing counters only: observability adds no allocation-path work.
    let governed = [
        "raw_total",
        "scratch",
        "query_execution",
        "controlled_derived",
        "dml_batch",
        "ddl",
        "stats",
        "alter_copy_batch",
        "system_metadata",
        "temporal_history",
        "protocol_transient",
        "binlog_transient",
        "parser_transient",
        "native_replication_transient",
        "performance_schema",
        "ddl_online_log",
    ];
    for memory in &mut rows {
        if let Some(slot) = governed.iter().position(|name| *name == memory.component) {
            memory.peak = Some(categories[slot].peak_bytes);
            memory.rejections = Some(rejections[slot]);
            memory.accounting = "CHARGED";
            memory.parent = Some("controlled_memory");
        }
        match memory.component.as_str() {
            "host_memory" => memory.scope = "host",
            "cgroup_memory" => memory.scope = "cgroup",
            "raw_active" | "raw_recycled" => memory.parent = Some("raw_total"),
            "data_raw" | "index_raw" | "lob_raw" | "vector_raw" | "zone_map_raw" => {
                memory.parent = Some("raw_active")
            }
            "undo_resident" => memory.parent = Some("system_metadata"),
            "work_area_used" => memory.parent = Some("query_execution"),
            "prepared_total" | "cte_result_total" => memory.parent = Some("retained_total"),
            "controlled_memory" => {
                memory.rejections = Some(rejections.iter().copied().fold(0u64, u64::saturating_add))
            }
            _ => {}
        }
    }

    QueryResponse::ResultSet(ResultSet {
        columns: summary_columns(),
        rows: rows.into_iter().map(MemoryRow::values).collect(),
    })
}

#[allow(clippy::too_many_arguments)]
fn row(
    category: &'static str,
    component: impl Into<String>,
    current: Option<u64>,
    peak: Option<u64>,
    limit: Option<u64>,
    hits: Option<u64>,
    misses: Option<u64>,
    entries: Option<u64>,
    evictions: Option<u64>,
    accounting: &'static str,
    in_total: bool,
) -> MemoryRow {
    MemoryRow {
        category,
        component: component.into(),
        current: current.map(i128::from),
        peak,
        limit,
        hits,
        misses,
        entries,
        evictions,
        accounting,
        in_total,
        scope: "process",
        counter_scope: (hits.is_some() || misses.is_some() || evictions.is_some())
            .then_some("process"),
        parent: None,
        rejections: None,
    }
}

fn nonzero(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

pub(crate) fn sessions(registry: &SessionRegistry, connection_id: Option<u32>) -> QueryResponse {
    let mut sessions = registry.snapshot();
    sessions.retain(|session| connection_id.is_none_or(|id| session.id == id));
    sessions.sort_by_key(|session| session.id);
    QueryResponse::ResultSet(ResultSet {
        columns: session_columns(),
        rows: sessions.into_iter().map(session_values).collect(),
    })
}

fn session_values(session: ProcessInfo) -> Vec<Value> {
    let total = session
        .exec_current_bytes
        .saturating_add(session.retained_cache_bytes);
    vec![
        Value::UInt(session.id as u64),
        Value::String(session.user),
        Value::String(session.host),
        session.database.map(Value::String).unwrap_or(Value::Null),
        Value::String(session.command),
        opt_u64(session.query_id),
        Value::UInt(session.exec_current_bytes),
        opt_u64(session.query_peak_bytes),
        Value::UInt(session.session_peak_bytes),
        opt_u64(session.query_limit_bytes),
        opt_u64(session.spill_bytes),
        Value::UInt(session.retained_cache_bytes),
        Value::UInt(total),
        Value::String(session.state.unwrap_or_else(|| {
            if session.query_id.is_some() {
                "RUNNING"
            } else {
                "IDLE"
            }
            .to_string()
        })),
    ]
}

#[cfg(any(not(windows), test))]
fn peak_resident_bytes() -> Option<u64> {
    crate::platform::memory::peak_resident_bytes()
}

#[cfg(target_os = "linux")]
fn os_memory() -> OsMemory {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok();
    let kib = |name: &str| -> Option<u64> {
        meminfo
            .as_deref()?
            .lines()
            .find_map(|line| {
                let (key, rest) = line.split_once(':')?;
                (key == name)
                    .then(|| rest.split_whitespace().next()?.parse::<u64>().ok())
                    .flatten()
            })
            .map(|value| value.saturating_mul(1024))
    };
    let host_total = kib("MemTotal");
    let host_used = match (host_total, kib("MemAvailable")) {
        (Some(total), Some(available)) => Some(total.saturating_sub(available)),
        _ => None,
    };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let process_rss = (page_size > 0)
        .then(|| std::fs::read_to_string("/proc/self/statm").ok())
        .flatten()
        .and_then(|line| line.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages.saturating_mul(page_size as u64));
    let (cgroup_used, cgroup_limit) = crate::util::system_memory::linux_cgroup_memory();
    OsMemory {
        host_used,
        host_total,
        cgroup_used,
        cgroup_limit,
        process_rss,
        process_private_footprint: crate::engine::memory_reclaimer::current_private_footprint_bytes(
        ),
        process_peak_rss: peak_resident_bytes(),
    }
}

// Approximation, not the memory_pressure percentage. Do not add speculative
// pages to free_count: XNU already includes them there. Compressor pages are
// physical storage, not the uncompressed logical page count.
#[cfg(any(target_os = "macos", test))]
fn macos_used_bytes(total: u64, page: u64, active: u64, wired: u64, compressed: u64) -> u64 {
    active
        .saturating_add(wired)
        .saturating_add(compressed)
        .saturating_mul(page)
        .min(total)
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
fn os_memory() -> OsMemory {
    unsafe {
        let mut total = 0u64;
        let mut total_len = std::mem::size_of::<u64>();
        let host_total = (libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&mut total as *mut u64).cast(),
            &mut total_len,
            std::ptr::null_mut(),
            0,
        ) == 0)
            .then_some(total);

        let mut task: libc::mach_task_basic_info = std::mem::zeroed();
        let mut task_count = libc::MACH_TASK_BASIC_INFO_COUNT;
        let process_rss = (libc::task_info(
            libc::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            (&mut task as *mut libc::mach_task_basic_info).cast(),
            &mut task_count,
        ) == libc::KERN_SUCCESS)
            .then_some(task.resident_size as u64);

        let mut vm: libc::vm_statistics64 = std::mem::zeroed();
        let mut vm_count = libc::HOST_VM_INFO64_COUNT;
        let host_used = (libc::host_statistics64(
            libc::mach_host_self(),
            libc::HOST_VM_INFO64,
            (&mut vm as *mut libc::vm_statistics64).cast(),
            &mut vm_count,
        ) == libc::KERN_SUCCESS)
            .then(|| {
                let page = libc::sysconf(libc::_SC_PAGESIZE).max(0) as u64;
                (page > 0)
                    .then(|| {
                        host_total.map(|total| {
                            macos_used_bytes(
                                total,
                                page,
                                vm.active_count as u64,
                                vm.wire_count as u64,
                                vm.compressor_page_count as u64,
                            )
                        })
                    })
                    .flatten()
            })
            .flatten();
        OsMemory {
            host_used,
            host_total,
            cgroup_used: None,
            cgroup_limit: None,
            process_rss,
            process_private_footprint:
                crate::engine::memory_reclaimer::current_private_footprint_bytes(),
            process_peak_rss: peak_resident_bytes(),
        }
    }
}

#[cfg(windows)]
fn os_memory() -> OsMemory {
    let host = crate::platform::memory::host_memory();
    let process = crate::platform::memory::process_memory();
    OsMemory {
        host_used: host.map(|(total, available)| total.saturating_sub(available)),
        host_total: host.map(|(total, _)| total),
        cgroup_used: None,
        cgroup_limit: None,
        process_rss: process.as_ref().map(|p| p.working_set),
        process_private_footprint: process.as_ref().map(|p| p.private_bytes),
        process_peak_rss: process.map(|p| p.peak_working_set),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn os_memory() -> OsMemory {
    OsMemory {
        host_used: None,
        host_total: None,
        cgroup_used: None,
        cgroup_limit: None,
        process_rss: None,
        process_private_footprint: None,
        process_peak_rss: peak_resident_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn signed_rss_difference_preserves_negative_values_and_nulls() {
        let mut difference = row(
            "system",
            "rss_minus_tracked",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "DERIVED",
            false,
        );
        assert_eq!(difference.values()[2], Value::Null);
        difference = row(
            "system",
            "rss_minus_tracked",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "DERIVED",
            false,
        );
        difference.current = Some(-4096);
        let values = difference.values();
        assert_eq!(values[2], Value::Int(-4096));
        assert_eq!(values[5], Value::Null);
        let mut packet = Vec::new();
        crate::mysql::protocol::text_row_packet_into_with_columns(
            &[values[2].clone()],
            &mut packet,
            None,
            Some(&[summary_columns()[2].clone()]),
        );
        assert_eq!(packet, b"\x05-4096");
    }

    #[test]
    fn macos_usage_counts_physical_compressor_and_clamps() {
        assert_eq!(macos_used_bytes(1000, 10, 20, 10, 5), 350);
        assert_eq!(macos_used_bytes(100, 10, 20, 10, 5), 100);
        assert_eq!(macos_used_bytes(100, u64::MAX, u64::MAX, 1, 1), 100);
    }

    #[test]
    fn session_visibility_and_wait_state_follow_registry() {
        let registry = SessionRegistry::new();
        let alice = registry.register(11, "alice".into(), "a".into(), "db".into());
        let _same_user = registry.register(12, "alice".into(), "b".into(), "db".into());
        let _bob = registry.register(13, "bob".into(), "c".into(), "db".into());
        let _stage = alice
            .enter_stage(crate::engine::session_registry::ProcessState::WaitingTableMetadataLock);
        let QueryResponse::ResultSet(result) = sessions(&registry, Some(11)) else {
            unreachable!()
        };
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::UInt(11));
        assert_eq!(
            result.rows[0][13],
            Value::String(
                registry
                    .snapshot()
                    .into_iter()
                    .find(|p| p.id == 11)
                    .unwrap()
                    .state
                    .unwrap()
            )
        );
        let QueryResponse::ResultSet(result) = sessions(&registry, None) else {
            unreachable!()
        };
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn hit_pct_is_null_without_samples() {
        let values = row(
            "cache",
            "cold",
            Some(0),
            None,
            None,
            Some(0),
            Some(0),
            None,
            None,
            "ESTIMATE",
            true,
        )
        .values();
        assert_eq!(values[8], Value::Null);
    }

    #[test]
    fn percentages_have_four_decimal_places() {
        let values = row(
            "cache",
            "sample",
            Some(1),
            None,
            Some(3),
            Some(1),
            Some(2),
            None,
            None,
            "EXACT",
            true,
        )
        .values();
        assert_eq!(values[5], Value::Double(33.3333));
        assert_eq!(values[8], Value::Double(33.3333));

        let columns = summary_columns();
        assert_eq!(columns[5].source.decimals, Some(4));
        assert_eq!(columns[8].source.decimals, Some(4));
        let mut packet = Vec::new();
        crate::mysql::protocol::text_row_packet_into_with_columns(
            &[values[5].clone(), values[8].clone()],
            &mut packet,
            None,
            Some(&[columns[5].clone(), columns[8].clone()]),
        );
        assert_eq!(packet, b"\x0733.3333\x0733.3333");
    }

    #[test]
    fn os_sampler_is_best_effort_but_peak_is_live() {
        assert!(peak_resident_bytes().unwrap_or(0) > 0);
    }

    #[test]
    fn session_snapshot_tracks_live_budget_and_retained_cache() {
        let registry = SessionRegistry::new();
        let active = registry.register(11, "alice".into(), "host-a".into(), "db1".into());
        let idle = registry.register(12, "bob".into(), "host-b".into(), "db2".into());
        active.set_protocol_prepared_memory_bytes(321);
        idle.set_cte_memory_bytes(99);

        let budget = mem_budget::MemBudget::for_session(Arc::clone(&active));
        budget.charge(4096).unwrap();
        budget.add_spill_bytes(2048);
        let QueryResponse::ResultSet(result) = sessions(&registry, None) else {
            unreachable!();
        };
        assert_eq!(result.rows.len(), 2);
        let active_row = result
            .rows
            .iter()
            .find(|row| row[0] == Value::UInt(11))
            .unwrap();
        assert_eq!(active_row[6], Value::UInt(4096));
        assert_eq!(active_row[10], Value::UInt(2048));
        assert_eq!(active_row[11], Value::UInt(321));
        assert_eq!(active_row[12], Value::UInt(4417));
        assert_eq!(active_row[13], Value::String("RUNNING".into()));

        budget.release(4096);
        drop(budget);
        let snapshot = registry.snapshot();
        let active = snapshot.iter().find(|session| session.id == 11).unwrap();
        assert_eq!(active.exec_current_bytes, 0);
        assert_eq!(active.session_peak_bytes, 4096);
        assert!(active.query_id.is_none());
    }
}
