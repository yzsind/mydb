//! Minimal IANA tzdata (TZif) reader for named time zones.
//!
//! `CONVERT_TZ('…','Asia/Shanghai','UTC')` and `SET time_zone =
//! 'Europe/Berlin'` need real UTC-offset history, not just `±HH:MM`
//! literals. mydb deliberately avoids a tzdata crate dependency: every
//! deployment target (macOS / Linux) ships compiled TZif files under a
//! well-known zoneinfo directory, so this module parses those files
//! directly (v1 32-bit and v2/v3 64-bit blocks) and caches the result
//! process-wide. Hosts without a zoneinfo database simply report
//! "unknown time zone", same as a MySQL server whose `mysql.time_zone*`
//! tables were never loaded.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, RwLock};

/// One local-time type (a TZif `ttinfo` record): the UTC offset, whether
/// it is daylight-saving time, and the abbreviation string (`CST`, `+08`,
/// …). Indexed by `TimeZoneData::trans_types`; also surfaced verbatim in
/// `mysql.time_zone_transition_type`.
#[derive(Clone)]
pub(crate) struct LocalTimeType {
    pub(crate) utoff: i32,
    pub(crate) is_dst: bool,
    pub(crate) abbreviation: String,
}

/// One parsed zone: transition instants (UTC seconds) plus the UTC
/// offset in effect from each transition onward. `first_offset` covers
/// instants before the first transition.
pub(crate) struct TimeZoneData {
    transitions: Vec<i64>,
    /// Offset (seconds east of UTC) active from `transitions[i]`.
    offsets: Vec<i32>,
    first_offset: i32,
    /// Local-time type index applied at `transitions[i]` (parallel to
    /// `transitions`), indexing into `types`. Feeds
    /// `mysql.time_zone_transition.Transition_type_id`.
    trans_types: Vec<u8>,
    /// The zone's local-time type table (`ttinfo` records). Feeds
    /// `mysql.time_zone_transition_type`.
    types: Vec<LocalTimeType>,
    /// TZif v2/v3 POSIX footer used after the final explicit transition.
    /// Modern zoneinfo commonly stops enumerating transitions in 2037 and
    /// relies on this rule for all later years.
    future_rule: Option<PosixTz>,
}

#[derive(Clone)]
struct PosixTz {
    std_offset: i32,
    dst_offset: Option<i32>,
    start: Option<PosixTransition>,
    end: Option<PosixTransition>,
}

#[derive(Clone, Copy)]
struct PosixTransition {
    day: PosixDay,
    seconds: i32,
    basis: TimeBasis,
}

#[derive(Clone, Copy)]
enum PosixDay {
    JulianNoLeap(u16),
    Julian(u16),
    MonthWeekDay { month: u8, week: u8, weekday: u8 },
}

#[derive(Clone, Copy)]
enum TimeBasis {
    Wall,
    Standard,
    Utc,
}

impl TimeZoneData {
    /// Transitions as `(utc_seconds, type_id)`, in file order (ascending
    /// time). Mirrors `mysql.time_zone_transition` for one zone.
    pub(crate) fn transitions(&self) -> impl Iterator<Item = (i64, u8)> + '_ {
        self.transitions
            .iter()
            .copied()
            .zip(self.trans_types.iter().copied())
    }

    /// The local-time type table (`ttinfo` records), indexed by type id.
    pub(crate) fn local_time_types(&self) -> &[LocalTimeType] {
        &self.types
    }
}

impl TimeZoneData {
    /// UTC offset in effect at the given UTC epoch second.
    pub(crate) fn offset_at_utc(&self, epoch: i64) -> i32 {
        if self.transitions.last().is_some_and(|last| epoch > *last) {
            if let Some(rule) = &self.future_rule {
                return rule.offset_at_utc(epoch);
            }
        }
        match self.transitions.partition_point(|&t| t <= epoch) {
            0 => self.first_offset,
            n => self.offsets[n - 1],
        }
    }

    /// Convert a local wall-clock epoch (seconds since 1970 *as if the
    /// local time were UTC*) into the actual UTC epoch. Ambiguous local
    /// times (DST fall-back) resolve to the earlier instant; skipped
    /// local times (spring-forward gap) shift forward, matching MySQL.
    pub(crate) fn utc_from_local(&self, local: i64) -> i64 {
        // Away from a transition, local and UTC differ by at most a civil
        // day's offset and the active offset is unambiguous. Resolve that
        // overwhelmingly common case with one partition search. The guarded
        // two-day window retains the exhaustive fold/gap logic below around
        // every historical transition; future POSIX-rule instants also stay on
        // the exact fallback because their transitions are computed on demand.
        const TRANSITION_GUARD_SECS: i64 = 2 * 86_400;
        let explicit_range = self
            .transitions
            .last()
            .is_none_or(|last| local <= *last || self.future_rule.is_none());
        if explicit_range {
            let slot = self.transitions.partition_point(|&t| t <= local);
            let previous_is_far = slot == 0
                || local.saturating_sub(self.transitions[slot - 1]) > TRANSITION_GUARD_SECS;
            let next_is_far = slot == self.transitions.len()
                || self.transitions[slot].saturating_sub(local) > TRANSITION_GUARD_SECS;
            if previous_is_far && next_is_far {
                let offset = if slot == 0 {
                    self.first_offset
                } else {
                    self.offsets[slot - 1]
                };
                return local - i64::from(offset);
            }
        }
        // Candidate offsets are those plausibly in effect around this
        // local instant; testing each for self-consistency is exact and
        // the candidate set is at most a handful of values.
        let mut candidates = [0i32; 3];
        let mut candidate_count = 0usize;
        for probe in [local - 86_400, local, local + 86_400] {
            let off = self.offset_at_utc(probe);
            if !candidates[..candidate_count].contains(&off) {
                candidates[candidate_count] = off;
                candidate_count += 1;
            }
        }
        let mut best: Option<i64> = None;
        for &off in &candidates[..candidate_count] {
            let utc = local - off as i64;
            if self.offset_at_utc(utc) == off {
                best = Some(match best {
                    Some(prev) => prev.min(utc),
                    None => utc,
                });
            }
        }
        if let Some(utc) = best {
            return utc;
        }
        let mut low = local - i64::from(*candidates[..candidate_count].iter().max().unwrap());
        let mut high = local - i64::from(*candidates[..candidate_count].iter().min().unwrap());
        let before = self.offset_at_utc(low);
        while low < high {
            let mid = low + (high - low) / 2;
            if self.offset_at_utc(mid) == before {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low
    }
}

/// Resolve a named IANA zone (`Asia/Shanghai`, `UTC`, `posix/CET`, …).
/// Returns `None` when the name is invalid or the host has no zoneinfo
/// entry for it. Bound both successful and negative lookups: zone names can
/// come from a different SQL row on every call.
pub(crate) fn find_named_zone(name: &str) -> Option<Arc<TimeZoneData>> {
    const MAX_POSITIVE_ZONES: usize = 1024;
    const MAX_NEGATIVE_ZONES: usize = 1024;
    let trimmed = name.trim();
    if !is_safe_zone_name(trimmed) {
        return None;
    }
    #[derive(Default)]
    struct ZoneCache {
        positive: HashMap<String, Arc<TimeZoneData>>,
        negative: HashSet<String>,
    }
    static CACHE: OnceLock<RwLock<ZoneCache>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| RwLock::new(ZoneCache::default()));
    {
        let cache = cache.read().unwrap();
        if let Some(hit) = cache.positive.get(trimmed) {
            return Some(Arc::clone(hit));
        }
        if cache.negative.contains(trimmed) {
            return None;
        }
    }
    let loaded = load_zone(trimmed).map(Arc::new);
    let mut cache = cache.write().unwrap();
    if let Some(hit) = cache.positive.get(trimmed) {
        return Some(Arc::clone(hit));
    }
    if cache.negative.contains(trimmed) {
        return None;
    }
    if let Some(zone) = loaded.as_ref() {
        if cache.positive.len() >= MAX_POSITIVE_ZONES {
            if let Some(victim) = cache.positive.keys().next().cloned() {
                cache.positive.remove(&victim);
            }
        }
        cache.positive.insert(trimmed.to_string(), Arc::clone(zone));
    } else {
        if cache.negative.len() >= MAX_NEGATIVE_ZONES {
            if let Some(victim) = cache.negative.iter().next().cloned() {
                cache.negative.remove(&victim);
            }
        }
        cache.negative.insert(trimmed.to_string());
    }
    loaded
}

/// Parse the host's active `/etc/localtime` once so `time_zone=SYSTEM` can
/// use the same lock-free transition lookup as a named IANA zone. Reading the
/// symlink target bytes preserves historical/DST behavior; hosts without a
/// readable TZif file retain the chrono/OS fallback in the caller.
pub(crate) fn find_system_zone() -> Option<Arc<TimeZoneData>> {
    static SYSTEM: OnceLock<Option<Arc<TimeZoneData>>> = OnceLock::new();
    SYSTEM
        .get_or_init(|| {
            std::fs::read("/etc/localtime")
                .ok()
                .and_then(|bytes| parse_tzif(&bytes))
                .map(Arc::new)
        })
        .clone()
}

/// Zone names come from SQL text — restrict to the IANA charset and
/// reject path traversal before touching the filesystem.
fn is_safe_zone_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && !name.starts_with('/')
        && !name.contains("..")
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '_' | '-' | '+'))
}

/// Well-known compiled-zoneinfo (TZif) locations across macOS / Linux.
const SEARCH_DIRS: &[&str] = &[
    "/usr/share/zoneinfo",
    "/var/db/timezone/zoneinfo", // macOS
    "/usr/lib/zoneinfo",
    "/etc/zoneinfo",
];

fn load_zone(name: &str) -> Option<TimeZoneData> {
    for dir in SEARCH_DIRS {
        let path = std::path::Path::new(dir).join(name);
        if let Ok(bytes) = std::fs::read(&path) {
            if let Some(zone) = parse_tzif(&bytes) {
                return Some(zone);
            }
        }
    }
    #[cfg(windows)]
    if let Some((_, bytes)) = jiff_tzdb::get(name) {
        return parse_tzif(bytes);
    }
    None
}

/// Parse a compiled TZif file (RFC 8536). Version 2+ files carry a
/// second header + data block with 64-bit transition times; prefer it.
fn parse_tzif(bytes: &[u8]) -> Option<TimeZoneData> {
    let (header, body_start) = parse_tzif_header(bytes, 0)?;
    if header.version >= b'2' {
        // Skip the v1 (32-bit) data block, then parse the v2 (64-bit)
        // header + block.
        let v1_len = header.data_len(4);
        let (h2, b2) = parse_tzif_header(bytes, body_start + v1_len)?;
        let mut zone = parse_tzif_block(bytes, b2, &h2, 8)?;
        let footer_start = b2.checked_add(h2.data_len(8))?;
        zone.future_rule = parse_posix_footer(bytes.get(footer_start..).unwrap_or_default());
        Some(zone)
    } else {
        parse_tzif_block(bytes, body_start, &header, 4)
    }
}

struct TzifHeader {
    version: u8,
    isutcnt: usize,
    isstdcnt: usize,
    leapcnt: usize,
    timecnt: usize,
    typecnt: usize,
    charcnt: usize,
}

impl TzifHeader {
    fn data_len(&self, time_size: usize) -> usize {
        self.timecnt * time_size
            + self.timecnt
            + self.typecnt * 6
            + self.charcnt
            + self.leapcnt * (time_size + 4)
            + self.isstdcnt
            + self.isutcnt
    }
}

fn parse_tzif_header(bytes: &[u8], at: usize) -> Option<(TzifHeader, usize)> {
    let head = bytes.get(at..at + 44)?;
    if &head[0..4] != b"TZif" {
        return None;
    }
    let be32 = |o: usize| -> usize {
        u32::from_be_bytes([head[o], head[o + 1], head[o + 2], head[o + 3]]) as usize
    };
    let header = TzifHeader {
        version: head[4],
        isutcnt: be32(20),
        isstdcnt: be32(24),
        leapcnt: be32(28),
        timecnt: be32(32),
        typecnt: be32(36),
        charcnt: be32(40),
    };
    // Sanity caps: a real tzdata file has at most a few thousand
    // transitions; reject anything absurd before allocating.
    if header.timecnt > 100_000 || header.typecnt > 1_000 || header.typecnt == 0 {
        return None;
    }
    Some((header, at + 44))
}

fn parse_tzif_block(
    bytes: &[u8],
    at: usize,
    header: &TzifHeader,
    time_size: usize,
) -> Option<TimeZoneData> {
    let data = bytes.get(at..at + header.data_len(time_size))?;
    let mut pos = 0usize;
    let mut transitions = Vec::with_capacity(header.timecnt);
    for _ in 0..header.timecnt {
        let t = if time_size == 8 {
            i64::from_be_bytes(data[pos..pos + 8].try_into().ok()?)
        } else {
            i32::from_be_bytes(data[pos..pos + 4].try_into().ok()?) as i64
        };
        transitions.push(t);
        pos += time_size;
    }
    let type_indices = &data[pos..pos + header.timecnt];
    let trans_types: Vec<u8> = type_indices.to_vec();
    pos += header.timecnt;
    // ttinfo records are followed by the abbreviation string table
    // (`charcnt` NUL-separated bytes); each record's 6th byte indexes into
    // it. Locate the abbreviation buffer so we can decode designations.
    let ttinfo_start = pos;
    let abbrev_buf = data.get(
        ttinfo_start + header.typecnt * 6..ttinfo_start + header.typecnt * 6 + header.charcnt,
    )?;
    let mut types = Vec::with_capacity(header.typecnt);
    let mut first_std: Option<i32> = None;
    for i in 0..header.typecnt {
        let o = ttinfo_start + i * 6;
        let utoff = i32::from_be_bytes(data[o..o + 4].try_into().ok()?);
        let isdst = data[o + 4] != 0;
        let desig_idx = data[o + 5] as usize;
        if first_std.is_none() && !isdst {
            first_std = Some(utoff);
        }
        types.push(LocalTimeType {
            utoff,
            is_dst: isdst,
            abbreviation: abbreviation_at(abbrev_buf, desig_idx),
        });
    }
    let offsets: Vec<i32> = type_indices
        .iter()
        .map(|&idx| types.get(idx as usize).map(|t| t.utoff).unwrap_or(0))
        .collect();
    // RFC 8536: the offset before the first transition is the first
    // standard-time type (fall back to type 0).
    let first_offset = first_std.unwrap_or_else(|| types.first().map(|t| t.utoff).unwrap_or(0));
    Some(TimeZoneData {
        transitions,
        offsets,
        first_offset,
        trans_types,
        types,
        future_rule: None,
    })
}

impl PosixTz {
    fn offset_at_utc(&self, epoch: i64) -> i32 {
        let Some(dst_offset) = self.dst_offset else {
            return self.std_offset;
        };
        let (Some(start), Some(end)) = (self.start, self.end) else {
            return self.std_offset;
        };
        let Some(year) =
            chrono::DateTime::from_timestamp(epoch, 0).map(|dt| chrono::Datelike::year(&dt))
        else {
            return self.std_offset;
        };
        let Some(start_utc) = self.transition_utc(year, start, self.std_offset) else {
            return self.std_offset;
        };
        let Some(end_utc) = self.transition_utc(year, end, dst_offset) else {
            return self.std_offset;
        };
        let in_dst = if start_utc < end_utc {
            epoch >= start_utc && epoch < end_utc
        } else {
            // Southern-hemisphere rules cross the year boundary.
            epoch >= start_utc || epoch < end_utc
        };
        if in_dst {
            dst_offset
        } else {
            self.std_offset
        }
    }

    fn transition_utc(
        &self,
        year: i32,
        transition: PosixTransition,
        wall_offset: i32,
    ) -> Option<i64> {
        let date = transition.day.date_in_year(year)?;
        let local = date
            .and_hms_opt(0, 0, 0)?
            .and_utc()
            .timestamp()
            .checked_add(transition.seconds as i64)?;
        let offset = match transition.basis {
            TimeBasis::Utc => 0,
            TimeBasis::Standard => self.std_offset,
            TimeBasis::Wall => wall_offset,
        };
        local.checked_sub(offset as i64)
    }
}

impl PosixDay {
    fn date_in_year(self, year: i32) -> Option<chrono::NaiveDate> {
        use chrono::{Datelike, NaiveDate};
        match self {
            // J1 is January 1 and J60 is March 1 in leap years: February 29
            // is deliberately not counted by the POSIX J form.
            PosixDay::JulianNoLeap(day) => {
                let leap = NaiveDate::from_ymd_opt(year, 2, 29).is_some();
                let ordinal = day as u32 + u32::from(leap && day >= 60);
                NaiveDate::from_yo_opt(year, ordinal)
            }
            // Numeric day form is zero-based and includes February 29.
            PosixDay::Julian(day) => NaiveDate::from_yo_opt(year, day as u32 + 1),
            PosixDay::MonthWeekDay {
                month,
                week,
                weekday,
            } => {
                let first = NaiveDate::from_ymd_opt(year, month as u32, 1)?;
                let first_weekday = first.weekday().num_days_from_sunday() as i32;
                let delta = (weekday as i32 - first_weekday).rem_euclid(7);
                let mut day = 1 + delta as u32 + 7 * (week as u32 - 1);
                if NaiveDate::from_ymd_opt(year, month as u32, day).is_none() && week == 5 {
                    day -= 7;
                }
                NaiveDate::from_ymd_opt(year, month as u32, day)
            }
        }
    }
}

/// Parse the newline-delimited POSIX TZ string after a TZif v2/v3 data block.
/// Examples: `CET-1CEST,M3.5.0,M10.5.0/3` and
/// `<-03>3<-02>,M3.2.0,M11.1.0`.
fn parse_posix_footer(bytes: &[u8]) -> Option<PosixTz> {
    let text = std::str::from_utf8(bytes)
        .ok()?
        .trim_matches(['\n', '\r', '\0']);
    if text.is_empty() {
        return None;
    }
    let mut p = PosixParser::new(text);
    p.parse_name()?;
    let std_offset = -p.parse_hms(true)?;
    if p.done() {
        return Some(PosixTz {
            std_offset,
            dst_offset: None,
            start: None,
            end: None,
        });
    }
    p.parse_name()?;
    let dst_offset = if p.peek() == Some(b',') {
        std_offset.checked_add(3600)?
    } else {
        -p.parse_hms(true)?
    };
    p.expect(b',')?;
    let start = p.parse_transition()?;
    p.expect(b',')?;
    let end = p.parse_transition()?;
    if !p.done() {
        return None;
    }
    Some(PosixTz {
        std_offset,
        dst_offset: Some(dst_offset),
        start: Some(start),
        end: Some(end),
    })
}

struct PosixParser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> PosixParser<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            bytes: text.as_bytes(),
            pos: 0,
        }
    }

    fn done(&self) -> bool {
        self.pos == self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn expect(&mut self, byte: u8) -> Option<()> {
        if self.peek()? != byte {
            return None;
        }
        self.pos += 1;
        Some(())
    }

    fn parse_name(&mut self) -> Option<()> {
        if self.peek() == Some(b'<') {
            self.pos += 1;
            let start = self.pos;
            while !matches!(self.peek(), None | Some(b'>')) {
                self.pos += 1;
            }
            if self.pos == start {
                return None;
            }
            return self.expect(b'>');
        }
        let start = self.pos;
        while self.peek().is_some_and(|b| b.is_ascii_alphabetic()) {
            self.pos += 1;
        }
        (self.pos - start >= 3).then_some(())
    }

    /// Parse `[+-]hh[:mm[:ss]]`, returning the signed textual value.
    fn parse_hms(&mut self, signed: bool) -> Option<i32> {
        let sign: i32 = if signed {
            match self.peek() {
                Some(b'-') => {
                    self.pos += 1;
                    -1
                }
                Some(b'+') => {
                    self.pos += 1;
                    1
                }
                _ => 1,
            }
        } else {
            1
        };
        let hours = self.parse_number()?;
        let mut minutes = 0;
        let mut seconds = 0;
        if self.peek() == Some(b':') {
            self.pos += 1;
            minutes = self.parse_number()?;
            if minutes > 59 {
                return None;
            }
            if self.peek() == Some(b':') {
                self.pos += 1;
                seconds = self.parse_number()?;
                if seconds > 59 {
                    return None;
                }
            }
        }
        let total = hours
            .checked_mul(3600)?
            .checked_add(minutes.checked_mul(60)?)?
            .checked_add(seconds)?;
        sign.checked_mul(total)
    }

    fn parse_number(&mut self) -> Option<i32> {
        let start = self.pos;
        let mut value = 0i32;
        while let Some(b) = self.peek().filter(|b| b.is_ascii_digit()) {
            value = value.checked_mul(10)?.checked_add((b - b'0') as i32)?;
            self.pos += 1;
        }
        (self.pos > start).then_some(value)
    }

    fn parse_transition(&mut self) -> Option<PosixTransition> {
        let day = match self.peek()? {
            b'M' => {
                self.pos += 1;
                let month = u8::try_from(self.parse_number()?).ok()?;
                self.expect(b'.')?;
                let week = u8::try_from(self.parse_number()?).ok()?;
                self.expect(b'.')?;
                let weekday = u8::try_from(self.parse_number()?).ok()?;
                if !(1..=12).contains(&month) || !(1..=5).contains(&week) || weekday > 6 {
                    return None;
                }
                PosixDay::MonthWeekDay {
                    month,
                    week,
                    weekday,
                }
            }
            b'J' => {
                self.pos += 1;
                let day = u16::try_from(self.parse_number()?).ok()?;
                if !(1..=365).contains(&day) {
                    return None;
                }
                PosixDay::JulianNoLeap(day)
            }
            b if b.is_ascii_digit() => {
                let day = u16::try_from(self.parse_number()?).ok()?;
                if day > 365 {
                    return None;
                }
                PosixDay::Julian(day)
            }
            _ => return None,
        };
        let mut seconds = 2 * 3600;
        let mut basis = TimeBasis::Wall;
        if self.peek() == Some(b'/') {
            self.pos += 1;
            seconds = self.parse_hms(true)?;
            basis = match self.peek() {
                Some(b'u' | b'g' | b'z') => {
                    self.pos += 1;
                    TimeBasis::Utc
                }
                Some(b's') => {
                    self.pos += 1;
                    TimeBasis::Standard
                }
                Some(b'w') => {
                    self.pos += 1;
                    TimeBasis::Wall
                }
                _ => TimeBasis::Wall,
            };
        }
        Some(PosixTransition {
            day,
            seconds,
            basis,
        })
    }
}

/// Decode the NUL-terminated abbreviation starting at `idx` in the TZif
/// designation buffer, clamped to MySQL's `Abbreviation char(8)` width.
fn abbreviation_at(buf: &[u8], idx: usize) -> String {
    let start = idx.min(buf.len());
    let end = buf[start..]
        .iter()
        .position(|&b| b == 0)
        .map(|p| start + p)
        .unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[start..end])
        .chars()
        .take(8)
        .collect()
}

// ---------------------------------------------------------------------------
// Host zone catalog (backs the `mysql.time_zone*` tables)
// ---------------------------------------------------------------------------

/// One named zone with a stable id, mirroring a `mysql.time_zone` row.
pub(crate) struct CatalogZone {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) data: Arc<TimeZoneData>,
}

/// Host-derived catalog of named zones. Built once (lazily) by walking the
/// host zoneinfo tree, parsing each TZif file, and assigning 1-based ids in
/// name order — the same shape `mysql_tzinfo_to_sql` loads into a real
/// server's `mysql.time_zone*` tables. A host with no zoneinfo database
/// yields an empty catalog (honest empty, same as an un-loaded MySQL).
pub(crate) struct ZoneCatalog {
    pub(crate) zones: Vec<CatalogZone>,
}

/// The process-wide zone catalog, enumerated on first access and cached for
/// the process lifetime (tzdata never changes mid-run).
pub(crate) fn zone_catalog() -> &'static ZoneCatalog {
    static CATALOG: OnceLock<ZoneCatalog> = OnceLock::new();
    CATALOG.get_or_init(build_zone_catalog)
}

fn build_zone_catalog() -> ZoneCatalog {
    // Bound the walk so a pathological zoneinfo tree can never balloon memory
    // or stall startup; a real database has ~600 zones.
    const MAX_ZONES: usize = 5000;
    const MAX_DEPTH: usize = 8;

    let base = SEARCH_DIRS
        .iter()
        .map(std::path::Path::new)
        .find(|p| p.is_dir());
    let Some(base) = base else {
        #[cfg(windows)]
        {
            let zones = jiff_tzdb::available()
                .filter_map(|name| {
                    jiff_tzdb::get(name)
                        .and_then(|(_, bytes)| parse_tzif(bytes))
                        .map(|data| (name.to_string(), Arc::new(data)))
                })
                .enumerate()
                .map(|(index, (name, data))| CatalogZone {
                    id: (index + 1) as u32,
                    name,
                    data,
                })
                .collect();
            return ZoneCatalog { zones };
        }
        #[cfg(not(windows))]
        return ZoneCatalog { zones: Vec::new() };
    };

    let mut found: Vec<(String, Arc<TimeZoneData>)> = Vec::new();
    // (directory, name-prefix, depth)
    let mut stack: Vec<(std::path::PathBuf, String, usize)> =
        vec![(base.to_path_buf(), String::new(), 0)];
    while let Some((dir, prefix, depth)) = stack.pop() {
        if found.len() >= MAX_ZONES {
            break;
        }
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let Some(component) = file_name.to_str() else {
                continue;
            };
            // Skip the leap-second (`right/`) and POSIX-TZ (`posix/`) variant
            // trees: MySQL's canonical load excludes them, and they duplicate
            // every zone under an alternate name.
            if prefix.is_empty() && matches!(component, "right" | "posix") {
                continue;
            }
            let name = if prefix.is_empty() {
                component.to_string()
            } else {
                format!("{prefix}/{component}")
            };
            // Only recurse into *real* directories (symlink_metadata does not
            // follow links) so zone aliases that are directory symlinks cannot
            // create a walk cycle.
            let Ok(meta) = entry.path().symlink_metadata() else {
                continue;
            };
            if meta.is_dir() {
                if depth < MAX_DEPTH {
                    stack.push((entry.path(), name, depth + 1));
                }
                continue;
            }
            if !is_safe_zone_name(&name) {
                continue;
            }
            // Files (and file-target symlinks like `US/Eastern`) are zone
            // candidates only if they carry the TZif magic.
            let Ok(bytes) = std::fs::read(entry.path()) else {
                continue;
            };
            if bytes.len() < 4 || &bytes[0..4] != b"TZif" {
                continue;
            }
            if let Some(zone) = parse_tzif(&bytes) {
                found.push((name, Arc::new(zone)));
            }
        }
    }

    // Stable id assignment: sort by name, number 1..=n (MySQL uses 1-based
    // ids; the exact numbering differs per install but must be stable and
    // consistent across all five time_zone* tables — sorting guarantees that).
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found.dedup_by(|a, b| a.0 == b.0);
    let zones = found
        .into_iter()
        .enumerate()
        .map(|(i, (name, data))| CatalogZone {
            id: (i + 1) as u32,
            name,
            data,
        })
        .collect();
    ZoneCatalog { zones }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsafe_names() {
        assert!(find_named_zone("../etc/passwd").is_none());
        assert!(find_named_zone("/etc/localtime").is_none());
        assert!(find_named_zone("").is_none());
    }

    #[test]
    fn loads_common_zones_when_zoneinfo_present() {
        // Host zoneinfo may be absent in minimal containers; only assert
        // behaviour when the database exists.
        if let Some(shanghai) = find_named_zone("Asia/Shanghai") {
            // CST has been a constant +08:00 since 1991.
            let t = 1_700_000_000; // 2023-11-14
            assert_eq!(shanghai.offset_at_utc(t), 8 * 3600);
            // Round-trip local → UTC.
            let local = t + 8 * 3600;
            assert_eq!(shanghai.utc_from_local(local), t);
        }
        if let Some(berlin) = find_named_zone("Europe/Berlin") {
            // 2023-07-01: CEST (+02:00); 2023-01-15: CET (+01:00).
            assert_eq!(berlin.offset_at_utc(1_688_169_600), 2 * 3600);
            assert_eq!(berlin.offset_at_utc(1_673_740_800), 3600);
            // TZif v2/v3 stops enumerating explicit transitions around 2037;
            // the POSIX footer must keep DST correct beyond 2038.
            assert_eq!(berlin.offset_at_utc(2_224_713_600), 2 * 3600); // 2040-07-01
            assert_eq!(berlin.offset_at_utc(2_210_457_600), 3600); // 2040-01-18
        }
    }

    #[test]
    fn dst_gap_and_overlap_resolution() {
        let Some(berlin) = find_named_zone("Europe/Berlin") else {
            return;
        };
        // Spring-forward 2023-03-26 02:30 local does not exist; the
        // conversion shifts past the gap instead of failing.
        // local epoch for 2023-03-26 02:30 "as UTC":
        let gap_local = 1_679_797_800;
        let utc = berlin.utc_from_local(gap_local);
        // MySQL clamps the skipped wall time to the transition boundary.
        let back = utc + berlin.offset_at_utc(utc) as i64;
        assert_eq!(back, gap_local + 1800);
        // Fall-back overlap resolves to the earlier UTC instant.
        // 2023-10-29 02:30 local occurs twice (CEST and CET).
        let dup_local = 1_698_546_600;
        let chosen = berlin.utc_from_local(dup_local);
        assert_eq!(chosen + berlin.offset_at_utc(chosen) as i64, dup_local);
        assert_eq!(berlin.offset_at_utc(chosen), 2 * 3600, "earlier = CEST");
    }
}
