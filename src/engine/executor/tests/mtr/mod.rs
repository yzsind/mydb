//! MTR-lite: a process-internal runner for MySQL-`mysqltest`-style `.test`
//! files, diffed against mydb-owned `.result` golden files.
//!
//! Design & migration plan: `doc_old/design/mysql-mtr-port-plan.md`. The runner
//! discovers every `tests/mydb-test/t/**/*.test`, executes each against a fresh
//! in-memory engine (`open_test_engine`) through `execute_with_session`,
//! renders the outcome to mysql `--batch` text (`render`), and either records
//! or diffs the golden `tests/mydb-test/r/.../*.result`.
//!
//! Golden files are mydb's own recorded output, **not** MySQL's — see §2 of the
//! design doc. Regenerate after adding/adapting a `.test`:
//!
//! ```bash
//! MYDB_MTR_RECORD=1 cargo nextest run --run-ignored only mtr_suite
//! ```
//!
//! ## Supported mysqltest subset (M1)
//! * `;`-delimited SQL (multi-line) + `delimiter` changes;
//! * `#` comments; `--echo`; `--error <errno|Ssqlstate|ER_name>` (multi);
//! * `--disable/enable_query_log`, `--disable/enable_result_log`,
//!   `--disable/enable_warnings` (default on → warnings auto-expand);
//! * `--sorted_result`, `--partially_sorted_result <start_column>`,
//!   `--replace_column`, `--replace_result`, `--replace_regex`;
//! * `--character_set` source-decoding markers (the vendored corpus is already
//!   UTF-8 decoded; binary payloads remain represented as SQL hex literals);
//! * `let $v = ...` (literal / `` `SQL` `` / `query_get_value(...)`), `--inc`,
//!   `--dec`, `eval`, `if (cond) { ... }`, `while (cond) { ... }`;
//! * `--source <file>` includes (resolved under `tests/mydb-test/`);
//! * multiple connections: `connect (name,host,user,pass[,db...])`,
//!   `connection <name>`, `disconnect <name>` (synchronous), and
//!   `send [stmt]` / `reap` (the sent statement runs on a background thread so
//!   it can genuinely block on a lock held by another connection).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;

use serde::{Deserialize, Serialize};

use super::*;

use crate::types::QueryError;

mod interp;
mod render;

use interp::Expected;
use render::RenderMods;

/// Outcome of running one `.test` file.
enum FileOutcome {
    Passed,
    Recorded,
    Skipped(String),
    Failed(String),
}

#[test]
#[ignore = "requires the local MySQL-derived corpus, which is excluded from the repository"]
fn mtr_suite() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let test_root = Path::new(manifest).join("tests/mydb-test");
    let t_root = test_root.join("t");
    let r_root = test_root.join("r");
    let record = std::env::var_os("MYDB_MTR_RECORD").is_some();

    let mut test_files = Vec::new();
    collect_test_files(&t_root, &mut test_files);
    test_files.sort();
    assert!(
        !test_files.is_empty(),
        "no .test files found under {}",
        t_root.display()
    );

    let mut passed = 0usize;
    let mut recorded = 0usize;
    let mut skipped: Vec<String> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    for test_path in &test_files {
        let rel = test_path.strip_prefix(&t_root).unwrap();
        let golden = r_root.join(rel).with_extension("result");
        let name = rel.display().to_string();

        match run_one(test_path, &golden, &test_root, record) {
            FileOutcome::Passed => passed += 1,
            FileOutcome::Recorded => recorded += 1,
            FileOutcome::Skipped(reason) => skipped.push(format!("{name}: {reason}")),
            FileOutcome::Failed(detail) => failures.push(format!("=== {name} ===\n{detail}")),
        }
    }

    eprintln!(
        "mtr-lite: {passed} passed, {recorded} recorded, {} skipped, {} failed",
        skipped.len(),
        failures.len()
    );
    for s in &skipped {
        eprintln!("  SKIP {s}");
    }

    // The explicitly enabled local curated suite must finish every file.
    // Accepting a harness-level SKIP would hide an unsupported directive or
    // missing symbol. Only the triage corpus may skip while capabilities are
    // still being implemented. Both corpora remain excluded from Git.
    if !skipped.is_empty() || !failures.is_empty() {
        let skipped_detail = skipped
            .iter()
            .map(|s| format!("=== SKIPPED ===\n{s}"))
            .collect::<Vec<_>>();
        let mut all_failures = skipped_detail;
        all_failures.extend(failures);
        panic!(
            "mtr-lite: {} curated file(s) did not run to completion:\n\n{}",
            all_failures.len(),
            all_failures.join("\n\n")
        );
    }
}

/// Triage scanner for un-adapted upstream MySQL tests (M3). Env-gated by
/// `MYDB_MTR_TRIAGE` so it does nothing on a normal run. It walks
/// `tests/mydb-test/triage/t/**/*.test`, runs each through the harness, and writes a
/// categorised `tests/mydb-test/triage/REPORT.md` — WITHOUT failing the build, so a
/// pile of not-yet-adapted files never breaks CI. Categories:
///
/// * `SKIP`      — unsupported include / directive / `ER_` symbol (harness gap
///                 or MySQL-only dependency);
/// * `VIOLATION` — ran but hit an unexpected error / `--error` mismatch (the
///                 highest-signal mydb compatibility gaps);
/// * `MATCH`     — ran clean and byte-matches MySQL's upstream `.result`;
/// * `DIFF`      — ran clean but differs from MySQL (intentional-difference vs
///                 bug triage needed).
///
/// Fetch a corpus first with `scripts/mtr_import.sh`, then:
/// `MYDB_MTR_TRIAGE=1 cargo nextest run --no-capture mtr_triage`.
/// Set `MYDB_MTR_TRIAGE_FILTER=<path-substring>` to isolate one file while
/// diagnosing a panic or a noisy violation cascade.
#[test]
fn mtr_triage() {
    if std::env::var_os("MYDB_MTR_TRIAGE").is_none() {
        return;
    }
    let manifest = env!("CARGO_MANIFEST_DIR");
    let test_root = Path::new(manifest).join("tests/mydb-test");
    let triage_root = test_root.join("triage");
    let t_root = triage_root.join("t");
    let upstream_root = triage_root.join("upstream");
    let allowlist_path = std::env::var_os("MYDB_MTR_TRIAGE_ALLOWLIST")
        .map(PathBuf::from)
        .unwrap_or_else(|| triage_root.join("diff_allowlist.json"));
    let allowlist = load_triage_diff_allowlist(&allowlist_path);
    if !t_root.exists() {
        eprintln!(
            "mtr-triage: no corpus at {} — run scripts/mtr_import.sh",
            t_root.display()
        );
        return;
    }

    let mut files = Vec::new();
    collect_test_files(&t_root, &mut files);
    files.sort();
    let active_filter = std::env::var("MYDB_MTR_TRIAGE_FILTER").ok();
    if let Some(filter) = active_filter.as_deref() {
        files.retain(|path| {
            path.strip_prefix(&t_root)
                .unwrap_or(path)
                .to_string_lossy()
                .contains(filter)
        });
        assert!(
            !files.is_empty(),
            "MYDB_MTR_TRIAGE_FILTER={filter:?} matched no test files"
        );
        eprintln!(
            "mtr-triage: filter {filter:?} selected {} file(s)",
            files.len()
        );
    }

    let (mut n_skip, mut n_viol, mut n_match, mut n_diff, mut n_err) = (0, 0, 0, 0, 0);
    let mut rows: Vec<String> = Vec::new();
    let mut detail: Vec<String> = Vec::new();
    let mut events: Vec<TriageEvent> = Vec::new();

    for f in &files {
        let rel = f.strip_prefix(&t_root).unwrap().display().to_string();
        eprintln!("mtr-triage: running {rel}");
        match execute_test_isolated(f, &triage_root) {
            Err(e) => {
                n_err += 1;
                rows.push(format!("| `{rel}` | ERROR | {e} |"));
                events.push(TriageEvent {
                    file: rel,
                    statement_index: 0,
                    statement: None,
                    stage: "harness".to_string(),
                    category: "ERROR".to_string(),
                    expected: None,
                    errno: parse_reported_errno(&e),
                    actual: e,
                    root_cause: "harness_error".to_string(),
                    primary: true,
                    diff_kind: None,
                    fingerprint: None,
                    semantic_diff: false,
                    ignored_kinds: Vec::new(),
                    allow_reason: None,
                    normalized_diff: None,
                });
            }
            Ok(o) if o.skip.is_some() => {
                n_skip += 1;
                let reason = o.skip.unwrap();
                rows.push(format!("| `{rel}` | SKIP | {reason} |"));
                events.push(TriageEvent {
                    file: rel,
                    statement_index: 0,
                    statement: None,
                    stage: "harness".to_string(),
                    category: "SKIP".to_string(),
                    expected: None,
                    errno: parse_reported_errno(&reason),
                    actual: reason,
                    root_cause: "missing_capability".to_string(),
                    primary: true,
                    diff_kind: None,
                    fingerprint: None,
                    semantic_diff: false,
                    ignored_kinds: Vec::new(),
                    allow_reason: None,
                    normalized_diff: None,
                });
            }
            Ok(o) if !o.violations.is_empty() => {
                n_viol += 1;
                let n = o.violations.len();
                rows.push(format!(
                    "| `{rel}` | VIOLATION | {n} incompatibilit(y/ies) |"
                ));
                detail.push(format!(
                    "### `{rel}` — {n} violation(s)\n\n```\n{}\n```",
                    o.violations.join("\n")
                ));
                let mut saw_setup_failure = false;
                events.extend(o.violations.into_iter().enumerate().map(
                    |(statement_index, actual)| {
                        let cascade = saw_setup_failure && is_likely_cascade(&actual);
                        if !cascade && is_setup_failure(&actual) {
                            saw_setup_failure = true;
                        }
                        triage_event_from_violation(
                            rel.clone(),
                            statement_index + 1,
                            actual,
                            !cascade,
                        )
                    },
                ));
            }
            Ok(o) => {
                let upstream = upstream_root
                    .join(f.strip_prefix(&t_root).unwrap())
                    .with_extension("result");
                match std::fs::read(&upstream) {
                    Ok(exp_bytes) if exp_bytes == o.out.as_bytes() => {
                        let exp = lossless_report_text(&exp_bytes);
                        n_match += 1;
                        rows.push(format!("| `{rel}` | MATCH | byte-identical to MySQL |"));
                        events.push(TriageEvent {
                            file: rel,
                            statement_index: 0,
                            statement: None,
                            stage: "comparison".to_string(),
                            category: "MATCH".to_string(),
                            expected: Some(exp),
                            actual: o.out,
                            errno: None,
                            root_cause: "none".to_string(),
                            primary: true,
                            diff_kind: None,
                            fingerprint: None,
                            semantic_diff: false,
                            ignored_kinds: Vec::new(),
                            allow_reason: None,
                            normalized_diff: None,
                        });
                    }
                    Ok(exp_bytes) => {
                        let exp = lossless_report_text(&exp_bytes);
                        n_diff += 1;
                        let d = diff(&exp, &o.out);
                        let nlines = d.lines().count();
                        let expected_normalized =
                            normalize_triage_output(&exp, &o.echoed_statements, &o.display_echoes);
                        let actual_normalized = normalize_triage_output(
                            &o.out,
                            &o.echoed_statements,
                            &o.display_echoes,
                        );
                        write_triage_normalization_debug(
                            &rel,
                            &exp,
                            &o.out,
                            &expected_normalized.text,
                            &actual_normalized.text,
                            &o.echoed_statements,
                            &expected_normalized.matched_indices,
                            &actual_normalized.matched_indices,
                        );
                        let normalized_text_diff =
                            expected_normalized.text != actual_normalized.text;
                        let expected_echo_matches = expected_normalized.matched_echoes;
                        let actual_echo_matches = actual_normalized.matched_echoes;
                        let mut ignored_kinds = expected_normalized.ignored_kinds.clone();
                        ignored_kinds.extend(actual_normalized.ignored_kinds.iter().copied());
                        let mut ignored_kinds: Vec<String> =
                            ignored_kinds.into_iter().map(str::to_string).collect();
                        if expected_echo_matches != actual_echo_matches {
                            ignored_kinds.push("statement_echo_alignment".to_string());
                        }
                        let segment_differences = if expected_echo_matches == actual_echo_matches {
                            triage_segment_differences(&expected_normalized, &actual_normalized)
                        } else {
                            Err(format!(
                                "normalization_unresolved: expected {expected_echo_matches} statement echoes, actual {actual_echo_matches}"
                            ))
                        };
                        let mut file_events = Vec::new();
                        match segment_differences {
                            Err(reason) => {
                                let fingerprint = triage_diff_fingerprint(
                                    &rel,
                                    0,
                                    &expected_normalized.text,
                                    &actual_normalized.text,
                                    &ignored_kinds,
                                );
                                file_events.push(TriageEvent {
                                    file: rel.clone(),
                                    statement_index: 0,
                                    statement: None,
                                    stage: "normalization".to_string(),
                                    category: "DIFF".to_string(),
                                    expected: Some(expected_normalized.text.clone()),
                                    actual: actual_normalized.text.clone(),
                                    errno: None,
                                    root_cause: reason,
                                    primary: true,
                                    diff_kind: Some("normalization_unresolved".to_string()),
                                    fingerprint: Some(fingerprint),
                                    semantic_diff: true,
                                    ignored_kinds: ignored_kinds.clone(),
                                    allow_reason: None,
                                    normalized_diff: Some(truncate_lines(
                                        &diff(&expected_normalized.text, &actual_normalized.text),
                                        100,
                                    )),
                                });
                            }
                            Ok(segment_differences) if normalized_text_diff => {
                                assert!(
                                    !segment_differences.is_empty(),
                                    "normalized output differs without a statement-scoped difference for {rel}"
                                );
                                for (statement_index, expected_text, actual_text) in
                                    segment_differences
                                {
                                    let fingerprint = triage_diff_fingerprint(
                                        &rel,
                                        statement_index,
                                        expected_text,
                                        actual_text,
                                        &ignored_kinds,
                                    );
                                    let allow_entry = allowlist.entries.iter().find(|entry| {
                                        entry.file == rel
                                            && entry.statement_index == statement_index
                                            && entry.fingerprint == fingerprint
                                    });
                                    let allowed_non_semantic = allow_entry.is_some_and(|entry| {
                                        matches!(
                                            entry.scope.as_str(),
                                            "non_semantic_output"
                                                | "equivalent_row_order"
                                                | "equivalent_group_concat_order"
                                        )
                                    });
                                    let semantic_diff = !allowed_non_semantic;
                                    let diff_kind = if semantic_diff {
                                        "semantic"
                                    } else {
                                        "non_semantic"
                                    };
                                    file_events.push(TriageEvent {
                                        file: rel.clone(),
                                        statement_index,
                                        statement: o
                                            .echoed_statements
                                            .get(statement_index.saturating_sub(1))
                                            .cloned(),
                                        stage: "comparison".to_string(),
                                        category: "DIFF".to_string(),
                                        expected: Some(expected_text.to_string()),
                                        actual: actual_text.to_string(),
                                        errno: None,
                                        root_cause: format!("output_{diff_kind}; statement_scoped"),
                                        primary: true,
                                        diff_kind: Some(diff_kind.to_string()),
                                        fingerprint: Some(fingerprint),
                                        semantic_diff,
                                        ignored_kinds: ignored_kinds.clone(),
                                        allow_reason: (!semantic_diff)
                                            .then(|| allow_entry.unwrap().reason.clone()),
                                        normalized_diff: Some(truncate_lines(
                                            &diff(expected_text, actual_text),
                                            100,
                                        )),
                                    });
                                }
                            }
                            Ok(_) => {
                                let fingerprint = triage_diff_fingerprint(
                                    &rel,
                                    0,
                                    &expected_normalized.text,
                                    &actual_normalized.text,
                                    &ignored_kinds,
                                );
                                let allow_entry = allowlist.entries.iter().find(|entry| {
                                    entry.file == rel
                                        && entry.statement_index == 0
                                        && entry.fingerprint == fingerprint
                                });
                                file_events.push(TriageEvent {
                                    file: rel.clone(),
                                    statement_index: 0,
                                    statement: None,
                                    stage: "comparison".to_string(),
                                    category: "DIFF".to_string(),
                                    expected: Some(expected_normalized.text.clone()),
                                    actual: actual_normalized.text.clone(),
                                    errno: None,
                                    root_cause: "output_non_semantic; normalization_policy"
                                        .to_string(),
                                    primary: true,
                                    diff_kind: Some("non_semantic".to_string()),
                                    fingerprint: Some(fingerprint),
                                    semantic_diff: false,
                                    ignored_kinds: ignored_kinds.clone(),
                                    allow_reason: allow_entry.map(|entry| entry.reason.clone()),
                                    normalized_diff: None,
                                });
                            }
                        }
                        let semantic_diff = file_events.iter().any(|event| event.semantic_diff);
                        let diff_kind = if semantic_diff {
                            "semantic"
                        } else {
                            "non_semantic"
                        };
                        rows.push(format!(
                            "| `{rel}` | DIFF | {nlines} diff line(s), {} statement event(s), {diff_kind} |",
                            file_events.len()
                        ));
                        detail.push(format!(
                            "### `{rel}` — differs from MySQL ({diff_kind})\n\n```\n{}\n```",
                            truncate_lines(&d, 30)
                        ));
                        events.extend(file_events);
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        n_diff += 1;
                        rows.push(format!(
                            "| `{rel}` | RAN | clean (no upstream .result to diff) |"
                        ));
                        events.push(TriageEvent {
                            file: rel,
                            statement_index: 0,
                            statement: None,
                            stage: "comparison".to_string(),
                            category: "DIFF".to_string(),
                            expected: None,
                            actual: o.out,
                            errno: None,
                            root_cause: "missing_oracle".to_string(),
                            primary: true,
                            diff_kind: Some("missing_oracle".to_string()),
                            fingerprint: None,
                            semantic_diff: true,
                            ignored_kinds: Vec::new(),
                            allow_reason: None,
                            normalized_diff: None,
                        });
                    }
                    Err(err) => panic!("read upstream result {}: {err}", upstream.display()),
                }
            }
        }
    }

    apply_triage_residual_policies(&mut events, &allowlist);

    let total = files.len();
    let semantic_diff_count = events
        .iter()
        .filter(|event| event.category == "DIFF" && event.semantic_diff)
        .count();
    let allowed_non_semantic_diff_count = events
        .iter()
        .filter(|event| {
            event.category == "DIFF" && !event.semantic_diff && event.allow_reason.is_some()
        })
        .count();
    let unlisted_non_semantic_diff_count = events
        .iter()
        .filter(|event| {
            event.category == "DIFF" && !event.semantic_diff && event.allow_reason.is_none()
        })
        .count();
    let selected_files: std::collections::BTreeSet<String> = files
        .iter()
        .map(|path| {
            path.strip_prefix(&t_root)
                .unwrap_or(path)
                .display()
                .to_string()
        })
        .collect();
    let stale_allowlist_entries: Vec<String> = allowlist
        .entries
        .iter()
        .filter(|entry| selected_files.contains(&entry.file))
        .filter(|entry| {
            !events.iter().any(|event| {
                event.file == entry.file
                    && event.statement_index == entry.statement_index
                    && event.fingerprint.as_deref() == Some(entry.fingerprint.as_str())
                    && event.allow_reason.is_some()
            })
        })
        .map(|entry| {
            format!(
                "{} statement {} {}",
                entry.file, entry.statement_index, entry.fingerprint
            )
        })
        .collect();
    let event_blocker_count = events
        .iter()
        .filter(|event| strict_event_is_blocking(event))
        .count();
    let strict_blocker_count = event_blocker_count + stale_allowlist_entries.len();
    let baseline = TriageCounts {
        files: 68,
        matched: 0,
        diff: 53,
        violation: 14,
        skip: 1,
        error: 0,
        strict_blockers: 44,
        semantic_diff: 0,
        allowed_non_semantic_diff: 0,
        unlisted_non_semantic_diff: 0,
    };
    let current = TriageCounts {
        files: total,
        matched: n_match,
        diff: n_diff,
        violation: n_viol,
        skip: n_skip,
        error: n_err,
        strict_blockers: strict_blocker_count,
        semantic_diff: semantic_diff_count,
        allowed_non_semantic_diff: allowed_non_semantic_diff_count,
        unlisted_non_semantic_diff: unlisted_non_semantic_diff_count,
    };
    let improvement = TriageImprovement {
        match_delta: current.matched as i64 - baseline.matched as i64,
        diff_delta: current.diff as i64 - baseline.diff as i64,
        violation_delta: current.violation as i64 - baseline.violation as i64,
        skip_delta: current.skip as i64 - baseline.skip as i64,
        error_delta: current.error as i64 - baseline.error as i64,
        blocker_delta: current.strict_blockers as i64 - baseline.strict_blockers as i64,
    };
    let mut report = String::new();
    report.push_str("# MTR-lite triage report\n\n");
    report.push_str("Generated by `mtr_triage` (env `MYDB_MTR_TRIAGE`). Corpus is un-adapted\n");
    report.push_str("upstream MySQL tests under `tests/mydb-test/triage/t/` (git-ignored).\n\n");
    report.push_str(&format!(
        "**{total} files**: {n_match} MATCH · {n_diff} DIFF · {n_viol} VIOLATION · {n_skip} SKIP · {n_err} ERROR\n\n",
    ));
    report.push_str(&format!(
        "Semantic DIFF: {semantic_diff_count} · allowlisted non-semantic DIFF: {allowed_non_semantic_diff_count} · unlisted non-semantic DIFF: {unlisted_non_semantic_diff_count} · strict blockers: {strict_blocker_count}.\n\n",
    ));
    if active_filter.is_none() {
        report.push_str(&format!(
            "Baseline: 0 MATCH · 53 DIFF · 14 VIOLATION · 1 SKIP · 0 ERROR · 44 blockers. Blocker delta: {:+}.\n\n",
            improvement.blocker_delta
        ));
    }
    if !stale_allowlist_entries.is_empty() {
        report.push_str("Stale allowlist entries (strict blockers):\n\n");
        for entry in &stale_allowlist_entries {
            report.push_str(&format!("- `{entry}`\n"));
        }
        report.push('\n');
    }
    report.push_str("| file | outcome | note |\n|------|---------|------|\n");
    for r in &rows {
        report.push_str(r);
        report.push('\n');
    }
    if !detail.is_empty() {
        report.push_str("\n## Details (violations & diffs)\n\n");
        report.push_str(&detail.join("\n\n"));
        report.push('\n');
    }

    if let Some(expected_files) = std::env::var("MYDB_MTR_TRIAGE_EXPECT_FILES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        assert_eq!(
            total, expected_files,
            "mtr-triage corpus drift: expected {expected_files} selected file(s), found {total}"
        );
    }
    let report_root = std::env::var_os("MYDB_MTR_TRIAGE_REPORT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| triage_root.clone());
    std::fs::create_dir_all(&report_root).expect("create triage report directory");
    let report_path = report_root.join("REPORT.md");
    std::fs::write(&report_path, &report).expect("write triage report");
    let events_path = report_root.join("REPORT.json");
    let structured = TriageStructuredReport {
        schema_version: 2,
        filter: active_filter.as_deref(),
        baseline,
        current,
        improvement,
        stale_allowlist_entries: stale_allowlist_entries.clone(),
        events: &events,
    };
    let events_json = serde_json::to_string_pretty(&structured).expect("serialize triage report");
    std::fs::write(&events_path, format!("{events_json}\n")).expect("write triage event report");
    eprintln!(
        "mtr-triage: {total} files → {n_match} MATCH, {n_diff} DIFF, {n_viol} VIOLATION, {n_skip} SKIP, {n_err} ERROR"
    );
    eprintln!("mtr-triage: report written to {}", report_path.display());
    eprintln!(
        "mtr-triage: structured events written to {}",
        events_path.display()
    );
    if std::env::var_os("MYDB_MTR_TRIAGE_STRICT").is_some() {
        let blockers: Vec<&TriageEvent> = events
            .iter()
            .filter(|event| strict_event_is_blocking(event))
            .collect();
        assert!(
            blockers.is_empty() && stale_allowlist_entries.is_empty(),
            "mtr-triage strict gate: {} semantic/unallowlisted/stale blocker(s); see {}",
            blockers.len() + stale_allowlist_entries.len(),
            events_path.display()
        );
    }
    if let Some(max_blockers) = std::env::var("MYDB_MTR_TRIAGE_MAX_BLOCKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        assert!(
            strict_blocker_count <= max_blockers,
            "mtr-triage non-regression gate: {strict_blocker_count} blocker(s) exceeds budget {max_blockers}; see {}",
            events_path.display()
        );
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TriageDiffAllowlistEntry {
    file: String,
    #[serde(default)]
    statement_index: usize,
    fingerprint: String,
    reason: String,
    scope: String,
    #[serde(default)]
    risk: String,
    #[serde(default)]
    capability: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct TriageDiffAllowlist {
    version: u32,
    #[serde(default)]
    entries: Vec<TriageDiffAllowlistEntry>,
}

fn load_triage_diff_allowlist(path: &Path) -> TriageDiffAllowlist {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return TriageDiffAllowlist {
                version: 2,
                entries: Vec::new(),
            };
        }
        Err(err) => panic!("read triage diff allowlist {}: {err}", path.display()),
    };
    let allowlist: TriageDiffAllowlist = serde_json::from_slice(&bytes)
        .unwrap_or_else(|err| panic!("parse triage diff allowlist {}: {err}", path.display()));
    assert!(
        matches!(allowlist.version, 1 | 2),
        "unsupported triage diff allowlist version {} in {}",
        allowlist.version,
        path.display()
    );
    let mut seen = std::collections::BTreeSet::new();
    for entry in &allowlist.entries {
        assert!(
            matches!(
                entry.scope.as_str(),
                "non_semantic_output"
                    | "equivalent_row_order"
                    | "equivalent_group_concat_order"
                    | "capability_guard"
                    | "environment_guard"
            ),
            "invalid allowlist scope for {}",
            entry.file
        );
        assert!(
            !entry.reason.trim().is_empty(),
            "allowlist reason must not be empty for {}",
            entry.file
        );
        if allowlist.version >= 2 {
            assert!(
                !entry.risk.trim().is_empty() && !entry.capability.trim().is_empty(),
                "version-2 allowlist entries require risk and capability for {} statement {}",
                entry.file,
                entry.statement_index
            );
        }
        assert!(
            seen.insert((
                entry.file.clone(),
                entry.statement_index,
                entry.fingerprint.clone()
            )),
            "duplicate triage allowlist entry for {} statement {} {}",
            entry.file,
            entry.statement_index,
            entry.fingerprint
        );
    }
    allowlist
}

#[derive(Debug, Serialize)]
struct TriageEvent {
    file: String,
    statement_index: usize,
    statement: Option<String>,
    stage: String,
    category: String,
    expected: Option<String>,
    actual: String,
    errno: Option<u16>,
    root_cause: String,
    primary: bool,
    diff_kind: Option<String>,
    fingerprint: Option<String>,
    semantic_diff: bool,
    ignored_kinds: Vec<String>,
    allow_reason: Option<String>,
    normalized_diff: Option<String>,
}

#[derive(Debug, Serialize)]
struct TriageCounts {
    files: usize,
    matched: usize,
    diff: usize,
    violation: usize,
    skip: usize,
    error: usize,
    strict_blockers: usize,
    semantic_diff: usize,
    allowed_non_semantic_diff: usize,
    unlisted_non_semantic_diff: usize,
}

#[derive(Debug, Serialize)]
struct TriageImprovement {
    match_delta: i64,
    diff_delta: i64,
    violation_delta: i64,
    skip_delta: i64,
    error_delta: i64,
    blocker_delta: i64,
}

#[derive(Debug, Serialize)]
struct TriageStructuredReport<'a> {
    schema_version: u32,
    filter: Option<&'a str>,
    baseline: TriageCounts,
    current: TriageCounts,
    improvement: TriageImprovement,
    stale_allowlist_entries: Vec<String>,
    events: &'a [TriageEvent],
}

fn lossless_report_text(bytes: &[u8]) -> String {
    // Result files can interleave normal UTF-8 SQL/labels with raw binary
    // cells. Decoding the *whole file* as Latin-1 after one invalid byte
    // corrupts every later multibyte statement echo (and creates false
    // semantic differences). Preserve valid UTF-8 runs and project only each
    // invalid octet into a private-use code point U+E000..U+E0FF. The mapping
    // stays reversible and cannot collide with decoded text.
    let mut remaining = bytes;
    let mut out = String::with_capacity(bytes.len());
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(text) => {
                out.push_str(text);
                break;
            }
            Err(error) => {
                let valid = error.valid_up_to();
                out.push_str(
                    std::str::from_utf8(&remaining[..valid])
                        .expect("valid_up_to prefix must be valid UTF-8"),
                );
                let invalid_len = error
                    .error_len()
                    .unwrap_or_else(|| remaining.len().saturating_sub(valid));
                for byte in &remaining[valid..valid + invalid_len] {
                    out.push(char::from_u32(0xE000 + u32::from(*byte)).unwrap());
                }
                remaining = &remaining[valid + invalid_len..];
            }
        }
    }
    out
}

#[test]
fn lossless_report_text_preserves_utf8_around_binary_octets() {
    let decoded = lossless_report_text(b"before \xff after \xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e");
    assert_eq!(
        decoded,
        format!("before {} after 日本語", char::from_u32(0xE0FF).unwrap())
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TriageStatementKind {
    Normal,
    Explain,
    ShowCreate,
}

fn triage_statement_kind(statement: &str) -> TriageStatementKind {
    let lower = statement.trim_start().to_ascii_lowercase();
    if lower.starts_with("explain ") || lower.starts_with("explain\n") {
        TriageStatementKind::Explain
    } else if lower.starts_with("show create ") {
        TriageStatementKind::ShowCreate
    } else {
        TriageStatementKind::Normal
    }
}

fn triage_statement_has_result(statement: &str) -> bool {
    let lower = statement.trim_start().to_ascii_lowercase();
    [
        "select", "with", "execute", "call", "show", "describe", "desc", "explain", "table",
        "values",
    ]
    .iter()
    .any(|prefix| {
        lower == *prefix
            || lower
                .strip_prefix(prefix)
                .is_some_and(|tail| tail.starts_with(char::is_whitespace))
    })
}

#[derive(Debug)]
struct NormalizedTriageOutput {
    text: String,
    ignored_kinds: std::collections::BTreeSet<&'static str>,
    matched_echoes: usize,
    matched_indices: Vec<(usize, usize, usize)>,
    segments: Vec<NormalizedTriageSegment>,
}

#[derive(Debug)]
struct NormalizedTriageSegment {
    statement_index: usize,
    text: String,
}

#[derive(Debug, Clone, Copy)]
enum UnorderedResultMode {
    SortRows,
    MaskLimitedRows,
}

fn unordered_result_mode(statement: &str) -> Option<UnorderedResultMode> {
    if !triage_statement_has_result(statement) {
        return None;
    }
    let top_level_tokens = triage_top_level_tokens(statement);
    if top_level_tokens
        .windows(2)
        .any(|tokens| tokens == ["order", "by"])
    {
        return None;
    }
    if top_level_tokens.iter().any(|token| token == "limit") {
        Some(UnorderedResultMode::MaskLimitedRows)
    } else {
        Some(UnorderedResultMode::SortRows)
    }
}

/// SQL clauses inside aggregate/window/subquery parentheses do not order the
/// statement's result rows. In particular `GROUP_CONCAT(x ORDER BY x)` was
/// previously mistaken for a top-level ORDER BY, turning harmless group-row
/// permutations into hundreds of semantic blockers.
fn triage_top_level_tokens(statement: &str) -> Vec<String> {
    let bytes = statement.as_bytes();
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;

    let flush = |token: &mut String, tokens: &mut Vec<String>| {
        if !token.is_empty() {
            tokens.push(std::mem::take(token));
        }
    };

    for &byte in bytes {
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if byte == b'\\' && delimiter != b'`' {
                escaped = true;
            } else if byte == delimiter {
                quote = None;
            }
            continue;
        }
        match byte {
            b'\'' | b'"' | b'`' => {
                flush(&mut token, &mut tokens);
                quote = Some(byte);
            }
            b'(' => {
                flush(&mut token, &mut tokens);
                depth = depth.saturating_add(1);
            }
            b')' => {
                flush(&mut token, &mut tokens);
                depth = depth.saturating_sub(1);
            }
            byte if depth == 0 && (byte.is_ascii_alphanumeric() || byte == b'_') => {
                token.push((byte as char).to_ascii_lowercase());
            }
            _ => flush(&mut token, &mut tokens),
        }
    }
    flush(&mut token, &mut tokens);
    tokens
}

fn canonicalize_unordered_result_rows(
    out: &mut [String],
    pending: &mut Option<(usize, UnorderedResultMode)>,
) {
    let Some((start, mode)) = pending.take() else {
        return;
    };
    let rows = &mut out[start..];
    match mode {
        UnorderedResultMode::SortRows => rows.sort(),
        UnorderedResultMode::MaskLimitedRows => {
            for row in rows {
                *row = "@UNORDERED_LIMIT_ROW".to_string();
            }
        }
    }
}

fn finish_triage_segment(
    out: &[String],
    current: &mut Option<(usize, usize)>,
    segments: &mut Vec<NormalizedTriageSegment>,
) {
    let Some((statement_index, start)) = current.take() else {
        return;
    };
    if statement_index != 0 || start < out.len() {
        segments.push(NormalizedTriageSegment {
            statement_index,
            text: out[start..].join("\n"),
        });
    }
}

fn canonical_triage_echo_line(line: &str) -> String {
    canonical_triage_echo_charset(line)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace(" ;", ";")
}

fn canonical_triage_echo_block<'a>(lines: impl IntoIterator<Item = &'a str>) -> String {
    let mut block = lines
        .into_iter()
        .map(canonical_triage_echo_charset)
        .flat_map(|line| {
            line.split_whitespace()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join(" ");
    block = block.replace(" ;", ";");
    // mysqltest preserves a custom delimiter written on its own source line;
    // the in-process runner reconstructs the query echo with that delimiter
    // attached to the final SQL line. They are the same echo surface.
    for delimiter in ["//", "$$", ";;"] {
        let spaced = format!(" {delimiter}");
        if block.ends_with(&spaced) {
            block.truncate(block.len() - spaced.len());
            block.push_str(delimiter);
        }
    }
    block
}

fn canonical_triage_echo_charset(line: &str) -> String {
    // Upstream .result files may contain source bytes that are invalid UTF-8.
    // `lossless_report_text` preserves those octets in the private-use range,
    // while the local .test reader converts the same source through its
    // declared mysqltest charset. Statement echoes are presentation only, so
    // compare their non-ASCII characters by position rather than encoding.
    line.chars()
        .map(|ch| if ch.is_ascii() { ch } else { '\u{FFFD}' })
        .collect()
}

fn write_triage_normalization_debug(
    file: &str,
    expected_raw: &str,
    actual_raw: &str,
    expected: &str,
    actual: &str,
    echoed_statements: &[String],
    expected_matches: &[(usize, usize, usize)],
    actual_matches: &[(usize, usize, usize)],
) {
    let Some(debug_dir) = std::env::var_os("MYDB_MTR_TRIAGE_DEBUG_DIR") else {
        return;
    };
    let debug_dir = std::path::PathBuf::from(debug_dir);
    if let Err(err) = std::fs::create_dir_all(&debug_dir) {
        eprintln!(
            "warning: cannot create MYDB_MTR_TRIAGE_DEBUG_DIR {}: {err}",
            debug_dir.display()
        );
        return;
    }
    let stem = file
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    for (suffix, contents) in [
        ("expected-raw", expected_raw),
        ("actual-raw", actual_raw),
        ("expected", expected),
        ("actual", actual),
    ] {
        let path = debug_dir.join(format!("{stem}.{suffix}.txt"));
        if let Err(err) = std::fs::write(&path, contents) {
            eprintln!("warning: cannot write {}: {err}", path.display());
        }
    }
    let echoes = echoed_statements
        .iter()
        .enumerate()
        .map(|(index, statement)| format!("### {index}\n{statement}"))
        .collect::<Vec<_>>()
        .join("\n");
    let path = debug_dir.join(format!("{stem}.echoes.txt"));
    if let Err(err) = std::fs::write(&path, echoes) {
        eprintln!("warning: cannot write {}: {err}", path.display());
    }
    for (suffix, matches) in [
        ("expected-matches", expected_matches),
        ("actual-matches", actual_matches),
    ] {
        let contents = matches
            .iter()
            .map(|(echo, line, consumed)| format!("echo={echo} line={line} consumed={consumed}"))
            .collect::<Vec<_>>()
            .join("\n");
        let path = debug_dir.join(format!("{stem}.{suffix}.txt"));
        if let Err(err) = std::fs::write(&path, contents) {
            eprintln!("warning: cannot write {}: {err}", path.display());
        }
    }
}

fn triage_parenthesis_balance(line: &str) -> i32 {
    let mut balance = 0i32;
    let mut quote = None;
    let mut escaped = false;
    for ch in line.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' && quote.is_some() {
            escaped = true;
            continue;
        }
        if let Some(active) = quote {
            if ch == active {
                quote = None;
            }
            continue;
        }
        if matches!(ch, '\'' | '"' | '`') {
            quote = Some(ch);
        } else if ch == '(' {
            balance += 1;
        } else if ch == ')' {
            balance -= 1;
        }
    }
    balance
}

/// Return the top-level SELECT-list text, stopping at the query block's FROM.
/// Nested FROM tokens (for example inside a scalar subquery) are deliberately
/// ignored. This is only a presentation oracle for mysqltest column labels;
/// SQL parsing and execution never depend on it.
fn triage_select_projection(statement: &str) -> Option<&str> {
    let trimmed = statement.trim_start();
    if trimmed.len() < 6 || !trimmed[..6].eq_ignore_ascii_case("select") {
        return None;
    }
    let bytes = trimmed.as_bytes();
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    let mut token_start = None;
    let projection_start = 6usize;

    for index in projection_start..=bytes.len() {
        let byte = bytes.get(index).copied().unwrap_or(b' ');
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if byte == b'\\' && delimiter != b'`' {
                escaped = true;
            } else if byte == delimiter {
                quote = None;
            }
            continue;
        }
        match byte {
            b'\'' | b'"' | b'`' => {
                token_start = None;
                quote = Some(byte);
            }
            b'(' => {
                token_start = None;
                depth = depth.saturating_add(1);
            }
            b')' => {
                token_start = None;
                depth = depth.saturating_sub(1);
            }
            byte if depth == 0 && (byte.is_ascii_alphanumeric() || byte == b'_') => {
                token_start.get_or_insert(index);
            }
            _ => {
                if let Some(start) = token_start.take() {
                    if trimmed[start..index].eq_ignore_ascii_case("from") {
                        return Some(trimmed[projection_start..start].trim());
                    }
                }
            }
        }
    }
    Some(
        trimmed[projection_start..]
            .trim()
            .trim_end_matches([';', '/', '$'])
            .trim(),
    )
}

/// Normalize only output surfaces explicitly permitted by the triage policy.
/// Result values, result types/metadata, warning codes, SQLSTATEs and statement
/// cardinality remain byte-sensitive.
fn normalize_triage_output(
    text: &str,
    echoed_statements: &[String],
    display_echoes: &[String],
) -> NormalizedTriageOutput {
    let lines: Vec<&str> = text.lines().collect();
    let echoes: Vec<Vec<String>> = echoed_statements
        .iter()
        .map(|statement| statement.lines().map(canonical_triage_echo_line).collect())
        .collect();
    let mut echoes_by_first_line: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, echo) in echoes.iter().enumerate() {
        if let Some(first) = echo.first() {
            echoes_by_first_line
                .entry(first.clone())
                .or_default()
                .push(index);
        }
    }
    let mut out = Vec::with_capacity(lines.len());
    let mut ignored_kinds = std::collections::BTreeSet::new();
    let mut echo_index = 0usize;
    let mut display_echo_index = 0usize;
    let mut matched_echoes = 0usize;
    let mut matched_indices = Vec::new();
    let mut line_index = 0usize;
    let mut context = TriageStatementKind::Normal;
    let mut emitted_ignored_result_marker = false;
    let mut expect_result_header = false;
    let mut expected_projection_header = String::new();
    let mut unordered_mode = None;
    let mut unordered_rows = None;
    let mut segments = Vec::new();
    let mut current_segment = Some((0usize, 0usize));

    while line_index < lines.len() {
        let current_line = canonical_triage_echo_line(lines[line_index]);
        let mut echo_candidates = echoes_by_first_line
            .get(&current_line)
            .cloned()
            .unwrap_or_default();
        if echo_candidates.is_empty() {
            // A formatter may wrap before the end of the first source line,
            // so exact first-line indexing is only a fast path. Restrict the
            // fallback to the next few statements with the same leading SQL
            // keyword; this keeps result rows out and preserves linear-ish
            // behaviour on large result files.
            let current_keyword = current_line.split_whitespace().next();
            echo_candidates.extend(
                (echo_index..echoes.len().min(echo_index.saturating_add(32))).filter(|candidate| {
                    current_keyword.is_some()
                        && echoes[*candidate]
                            .first()
                            .and_then(|line| line.split_whitespace().next())
                            == current_keyword
                }),
            );
        }
        let mut matched = echo_candidates
            .into_iter()
            .skip_while(|candidate| *candidate < echo_index)
            .filter_map(|candidate| {
                let echo = &echoes[candidate];
                let direct = line_index + echo.len() <= lines.len()
                    && echo.iter().enumerate().all(|(offset, expected_line)| {
                        canonical_triage_echo_line(lines[line_index + offset]) == *expected_line
                    });
                if direct {
                    return Some((candidate, echo.len()));
                }
                let expected_block =
                    canonical_triage_echo_block(echoed_statements[candidate].lines());
                // The source formatter and mysqltest can wrap the same SQL at
                // different physical lines (especially deeply nested IF/CASE
                // expressions and stored-program bodies). Match the complete
                // canonical token block in a bounded window instead of only
                // accepting exactly N or N+1 lines. Equality of the complete
                // block prevents a query prefix from consuming its result.
                let max_consumed = lines
                    .len()
                    .saturating_sub(line_index)
                    .min(echo.len().saturating_add(32));
                (1..=max_consumed).find_map(|consumed| {
                    (canonical_triage_echo_block(
                        lines[line_index..line_index + consumed].iter().copied(),
                    ) == expected_block)
                        .then_some((candidate, consumed))
                })
            })
            // Several statements can share their first physical line (for
            // example a family of multiline STATEMENT_DIGEST calls). Never
            // consume a one-line prefix when a longer source echo matches.
            .max_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)));
        if matched.is_none() {
            let current_keyword = current_line.split_whitespace().next();
            // Fuzzy reformatted-echo recovery is deliberately restricted to
            // the *next* source statement. Searching later statements can
            // mistake an EXPLAIN result header (also literally "EXPLAIN")
            // for a future EXPLAIN query and consume the plan plus intervening
            // statements up to the next semicolon. Exact canonical matches
            // above may still skip source statements hidden by
            // --disable_query_log; fuzzy matches may not.
            let candidate = (echo_index < echoes.len()
                && current_keyword.is_some()
                && echoes[echo_index]
                    .first()
                    .and_then(|line| line.split_whitespace().next())
                    == current_keyword)
                .then_some(echo_index);
            if let Some(candidate) = candidate {
                let max_consumed = lines
                    .len()
                    .saturating_sub(line_index)
                    .min(echoes[candidate].len().saturating_add(32));
                // Last-resort presentation match for upstream mysqltest
                // echoes that normalize tokens as well as line wrapping.
                // It is constrained by execution order, leading SQL keyword
                // and a complete statement terminator.
                matched = (1..=max_consumed).find_map(|consumed| {
                    let last = lines[line_index + consumed - 1].trim_end();
                    (last.ends_with(';') || last.ends_with("//") || last.ends_with("$$"))
                        .then_some((candidate, consumed))
                });
            }
        }
        // A result column label can itself begin with a SQL keyword and can
        // even be textually identical to the next statement.  This happens
        // for scalar/quantified subqueries whose projected label starts with
        // `SELECT`.  mysqltest statement echoes retain their terminator while
        // result labels do not, so do not let an unterminated label consume
        // the next source statement merely because canonical echo matching
        // made the two strings equal.
        let matched_is_exact = matched.is_some_and(|(candidate, consumed)| {
            canonical_triage_echo_block(lines[line_index..line_index + consumed].iter().copied())
                == canonical_triage_echo_block(echoed_statements[candidate].lines())
        });
        if expect_result_header
            && matched.is_some()
            && !matched_is_exact
            && !lines[line_index].trim_end().ends_with([';', '/', '$'])
        {
            matched = None;
        }
        if let Some((matched_index, consumed_lines)) = matched {
            canonicalize_unordered_result_rows(&mut out, &mut unordered_rows);
            finish_triage_segment(&out, &mut current_segment, &mut segments);
            let statement = &echoed_statements[matched_index];
            // Statement echo is a presentation surface. It still drives the
            // result-context state below, but is deliberately absent from the
            // normalized semantic stream.
            ignored_kinds.insert("statement_echo");
            context = triage_statement_kind(statement);
            emitted_ignored_result_marker = false;
            expect_result_header = triage_statement_has_result(statement);
            unordered_mode = unordered_result_mode(statement);
            expected_projection_header = triage_select_projection(statement)
                .map(|projection| canonical_triage_echo_block(projection.lines()))
                .unwrap_or_default();
            line_index += consumed_lines;
            echo_index = matched_index + 1;
            matched_echoes += 1;
            matched_indices.push((matched_index, line_index - consumed_lines, consumed_lines));
            current_segment = Some((matched_index + 1, out.len()));
            continue;
        }

        // `--echo` is test narration, not a server result. Record the exact
        // expanded strings while executing the source and remove only those
        // strings, in order, from both the upstream and mydb streams. This is
        // deliberately data-driven: an ordinary result row that merely looks
        // like a comment is never discarded unless the source emitted the
        // same `--echo` at this point.
        if display_echoes
            .get(display_echo_index)
            .is_some_and(|display| canonical_triage_echo_line(display) == current_line)
        {
            ignored_kinds.insert("directive_echo");
            display_echo_index += 1;
            line_index += 1;
            continue;
        }

        match context {
            TriageStatementKind::Explain | TriageStatementKind::ShowCreate => {
                if !emitted_ignored_result_marker {
                    let kind = if context == TriageStatementKind::Explain {
                        "explain"
                    } else {
                        "show_create"
                    };
                    out.push(format!("@IGNORED {kind}"));
                    ignored_kinds.insert(kind);
                    emitted_ignored_result_marker = true;
                }
            }
            TriageStatementKind::Normal => {
                let line = lines[line_index];
                if line == "Warnings:" {
                    canonicalize_unordered_result_rows(&mut out, &mut unordered_rows);
                    // The block label is mysqltest presentation. Warning
                    // level/code rows remain in the semantic stream.
                    ignored_kinds.insert("warning_format");
                    line_index += 1;
                    continue;
                }
                if expect_result_header && !expected_projection_header.is_empty() {
                    let mut header_lines = 0usize;
                    let mut previous_candidate = String::new();
                    for consumed in 1..=lines.len().saturating_sub(line_index).min(64) {
                        let candidate = canonical_triage_echo_block(
                            lines[line_index..line_index + consumed].iter().copied(),
                        );
                        // An empty result value is a real row. Appending a
                        // blank physical line adds no header text, so it must
                        // never extend a projection-prefix header match.
                        if candidate == previous_candidate {
                            break;
                        }
                        if expected_projection_header.starts_with(&candidate) {
                            header_lines = consumed;
                            previous_candidate = candidate;
                        } else {
                            break;
                        }
                    }
                    if header_lines > 0 {
                        out.push("@RESULT_HEADER".to_string());
                        unordered_rows = unordered_mode.map(|mode| (out.len(), mode));
                        ignored_kinds.insert("column_label");
                        expect_result_header = false;
                        line_index += header_lines;
                        continue;
                    }
                }
                // mysqltest prints a scalar-subquery expression as a
                // multi-line column label. The runner may preserve different
                // indentation from the upstream result, but the balanced
                // parenthesised block is still one header, not a result row.
                // Consume that complete block while we are *specifically*
                // waiting for a header; ordinary string values retain their
                // whitespace and parentheses unchanged.
                if expect_result_header {
                    let mut balance = triage_parenthesis_balance(lines[line_index]);
                    if balance > 0 {
                        let mut consumed = 1usize;
                        while balance > 0 && line_index + consumed < lines.len() {
                            let next_line =
                                canonical_triage_echo_line(lines[line_index + consumed]);
                            let starts_next_statement = echoes_by_first_line
                                .get(&next_line)
                                .is_some_and(|candidates| {
                                    candidates.iter().any(|candidate| *candidate >= echo_index)
                                });
                            // mysqltest replacement directives can truncate a
                            // very long expression label without preserving
                            // balanced parentheses. Never let header recovery
                            // consume the next statement echo in that case.
                            if starts_next_statement {
                                break;
                            }
                            balance += triage_parenthesis_balance(lines[line_index + consumed]);
                            consumed += 1;
                        }
                        out.push("@RESULT_HEADER".to_string());
                        unordered_rows = unordered_mode.map(|mode| (out.len(), mode));
                        ignored_kinds.insert("column_label");
                        expect_result_header = false;
                        line_index += consumed;
                        continue;
                    }
                }
                if let Some((level, rest)) = line.split_once('\t') {
                    if matches!(level, "Warning" | "Note") {
                        canonicalize_unordered_result_rows(&mut out, &mut unordered_rows);
                        if let Some((code, _message)) = rest.split_once('\t') {
                            out.push(format!("{level}\t{code}"));
                            ignored_kinds.insert("warning_text");
                            line_index += 1;
                            continue;
                        }
                    }
                }
                if line.starts_with("ERROR ") {
                    canonicalize_unordered_result_rows(&mut out, &mut unordered_rows);
                    out.push(
                        line.split_once(':')
                            .map(|(surface, _)| surface.to_string())
                            .unwrap_or_else(|| line.to_string()),
                    );
                    ignored_kinds.insert("error_text");
                    // An error is the terminal response for this statement;
                    // there is no result header to consume.  Keeping the flag
                    // set makes the first line of a following multiline query
                    // look like the failed statement's column label.
                    expect_result_header = false;
                    unordered_mode = None;
                } else if expect_result_header {
                    out.push("@RESULT_HEADER".to_string());
                    unordered_rows = unordered_mode.map(|mode| (out.len(), mode));
                    ignored_kinds.insert("column_label");
                    expect_result_header = false;
                } else {
                    out.push(line.to_string());
                }
            }
        }
        line_index += 1;
    }
    canonicalize_unordered_result_rows(&mut out, &mut unordered_rows);
    finish_triage_segment(&out, &mut current_segment, &mut segments);

    NormalizedTriageOutput {
        text: out.join("\n"),
        ignored_kinds,
        matched_echoes,
        matched_indices,
        segments,
    }
}

fn triage_diff_fingerprint(
    file: &str,
    statement_index: usize,
    expected_normalized: &str,
    actual_normalized: &str,
    ignored_kinds: &[String],
) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(file.as_bytes());
    hasher.update([0]);
    // Index zero is the legacy whole-file/preamble scope. Keeping its digest
    // stable lets version-1 allowlists fail closed while they are migrated to
    // statement scope; every real statement is bound to its execution index.
    if statement_index != 0 {
        hasher.update(statement_index.to_le_bytes());
        hasher.update([0]);
    }
    hasher.update(expected_normalized.as_bytes());
    hasher.update([0]);
    hasher.update(actual_normalized.as_bytes());
    hasher.update([0]);
    for kind in ignored_kinds {
        hasher.update(kind.as_bytes());
        hasher.update([0]);
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn triage_policy_fingerprint(event: &TriageEvent) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    for value in [
        event.file.as_str(),
        event.stage.as_str(),
        event.category.as_str(),
        event.root_cause.as_str(),
        event.actual.as_str(),
    ] {
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    hasher.update(event.statement_index.to_le_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn apply_triage_residual_policies(events: &mut [TriageEvent], allowlist: &TriageDiffAllowlist) {
    for event in events {
        let expected_scope = match event.category.as_str() {
            "SKIP" => "capability_guard",
            "VIOLATION" if event.primary => "environment_guard",
            _ => continue,
        };
        let fingerprint = triage_policy_fingerprint(event);
        let policy = allowlist.entries.iter().find(|entry| {
            entry.file == event.file
                && entry.statement_index == event.statement_index
                && entry.fingerprint == fingerprint
                && entry.scope == expected_scope
        });
        event.fingerprint = Some(fingerprint);
        event.allow_reason = policy.map(|entry| entry.reason.clone());
    }
}

fn triage_segment_differences<'a>(
    expected: &'a NormalizedTriageOutput,
    actual: &'a NormalizedTriageOutput,
) -> Result<Vec<(usize, &'a str, &'a str)>, String> {
    let expected_by_statement: std::collections::BTreeMap<usize, &str> = expected
        .segments
        .iter()
        .map(|segment| (segment.statement_index, segment.text.as_str()))
        .collect();
    let actual_by_statement: std::collections::BTreeMap<usize, &str> = actual
        .segments
        .iter()
        .map(|segment| (segment.statement_index, segment.text.as_str()))
        .collect();
    let expected_indices: Vec<usize> = expected_by_statement.keys().copied().collect();
    let actual_indices: Vec<usize> = actual_by_statement.keys().copied().collect();
    if expected_indices != actual_indices {
        return Err(format!(
            "normalization_unresolved: expected statement segments {expected_indices:?}, actual {actual_indices:?}"
        ));
    }
    Ok(expected_by_statement
        .into_iter()
        .filter_map(|(statement_index, expected_text)| {
            let actual_text = actual_by_statement[&statement_index];
            (expected_text != actual_text).then_some((statement_index, expected_text, actual_text))
        })
        .collect())
}

fn is_setup_failure(actual: &str) -> bool {
    let lower = actual.to_ascii_lowercase();
    lower.contains("create table")
        || lower.contains("create view")
        || lower.contains("create function")
        || lower.contains("create procedure")
        || lower.contains("rename table")
}

fn is_likely_cascade(actual: &str) -> bool {
    let lower = actual.to_ascii_lowercase();
    lower.contains("doesn't exist")
        || lower.contains("does not exist")
        || lower.contains("unknown table")
        || lower.contains("unknown column")
}

fn strict_event_is_blocking(event: &TriageEvent) -> bool {
    match event.category.as_str() {
        "ERROR" => true,
        "SKIP" => event.allow_reason.is_none(),
        "DIFF" => event.semantic_diff || event.allow_reason.is_none(),
        "VIOLATION" if event.primary => event.allow_reason.is_none(),
        _ => false,
    }
}

fn parse_reported_errno(text: &str) -> Option<u16> {
    for marker in [" got ", "ERROR ", "errno="] {
        if let Some(tail) = text.rsplit_once(marker).map(|(_, tail)| tail) {
            let digits: String = tail
                .trim_start()
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            if let Ok(errno) = digits.parse() {
                return Some(errno);
            }
        }
    }
    None
}

fn triage_event_from_violation(
    file: String,
    statement_index: usize,
    actual: String,
    primary: bool,
) -> TriageEvent {
    let expected = actual
        .split_once(" expected ")
        .and_then(|(_, tail)| tail.split_once(" but "))
        .map(|(expected, _)| expected.to_string());
    let stage = if actual.contains("connection `")
        || actual.contains("outstanding send")
        || actual.contains("reap on")
    {
        "protocol"
    } else if actual.contains("directive") || actual.contains("panicked") {
        "harness"
    } else {
        "execution"
    };
    let errno = parse_reported_errno(&actual);
    TriageEvent {
        file,
        statement_index,
        statement: None,
        stage: stage.to_string(),
        category: "VIOLATION".to_string(),
        expected,
        root_cause: classify_violation_root_cause(&actual).to_string(),
        actual,
        errno,
        primary,
        diff_kind: None,
        fingerprint: None,
        semantic_diff: false,
        ignored_kinds: Vec::new(),
        allow_reason: None,
        normalized_diff: None,
    }
}

fn classify_violation_root_cause(actual: &str) -> &'static str {
    let lower = actual.to_ascii_lowercase();
    if lower.contains("rename table mysql.time_zone")
        || lower.contains("rename table time_zone_backup")
    {
        "environment_time_zone_catalog"
    } else if lower.contains("kdf arguments") {
        "aes_kdf"
    } else if lower.contains("at time zone") || lower.contains("timestamp with time zone") {
        "at_time_zone"
    } else if lower.contains("geometry")
        || lower.contains("st_")
        || lower.contains("point(")
        || lower.contains("polygon(")
    {
        "structured_type_coercion"
    } else if lower.contains("character set")
        || lower.contains("collation")
        || lower.contains("convert string")
    {
        "charset_collation"
    } else if lower.contains("join") || lower.contains("table reference") {
        "join_shape"
    } else if lower.contains("subquery")
        || lower.contains("with recursive")
        || lower.contains(" cte ")
        || lower.contains("cte1")
    {
        "subquery_shape"
    } else if lower.contains("group") || lower.contains("having") || lower.contains("aggregate") {
        "grouping"
    } else if lower.contains("date") || lower.contains("time") || lower.contains("timestamp") {
        "temporal"
    } else if lower.contains("decimal")
        || lower.contains("out of range")
        || lower.contains("truncate")
    {
        "numeric"
    } else if lower.contains("function") || lower.contains("trigger") {
        "stored_routine"
    } else {
        "execution_semantics"
    }
}
/// Protocol conformance (M4): run curated SQL scripts through the *real* wire
/// — a spawned `mydb` server + the `mysql` CLI — and diff the wire output
/// against the in-process engine oracle. This is the only path that exercises
/// the handshake, auth, packet framing, and result-set encoding that the
/// in-process runner bypasses.
///
/// Env-gated by `MYDB_MTR_PROTO` (needs a built `target/{debug,release}/mydb`
/// and a `mysql` client), so it never runs in a normal `cargo nextest` pass.
/// Skips gracefully when the binary or client is unavailable.
///
/// `MYDB_MTR_PROTO=1 cargo nextest run --no-capture mtr_protocol`
#[test]
fn mtr_protocol() {
    if std::env::var_os("MYDB_MTR_PROTO").is_none() {
        return;
    }
    let manifest = env!("CARGO_MANIFEST_DIR");
    let test_root = Path::new(manifest).join("tests/mydb-test");
    let proto_dir = test_root.join("proto");
    if !proto_dir.exists() {
        eprintln!("mtr-protocol: no corpus at {}", proto_dir.display());
        return;
    }

    let Some(server_bin) = find_mydb_binary(manifest) else {
        eprintln!("mtr-protocol: no built mydb binary under target/{{debug,release}} — run `cargo build` first; skipping");
        return;
    };
    let Some(mysql_cli) = find_mysql_cli() else {
        eprintln!("mtr-protocol: no `mysql` client on PATH; skipping");
        return;
    };

    let mut scripts = Vec::new();
    collect_ext(&proto_dir, "sql", &mut scripts);
    scripts.sort();
    if scripts.is_empty() {
        eprintln!(
            "mtr-protocol: no *.sql scripts under {}",
            proto_dir.display()
        );
        return;
    }

    let port = free_port();
    let data_dir = tempfile::tempdir().expect("server data dir");
    let mut server = ServerProcess::start(&server_bin, port, data_dir.path());
    if !server.wait_ready(&mysql_cli, port) {
        panic!(
            "mtr-protocol: server on port {port} never became ready\n{}",
            server.tail_log()
        );
    }

    let mut failures: Vec<String> = Vec::new();
    for script in &scripts {
        let rel = script
            .strip_prefix(&proto_dir)
            .unwrap()
            .display()
            .to_string();
        let sql = std::fs::read_to_string(script).expect("read proto script");

        let wire = run_mysql_batch(&mysql_cli, port, &sql);
        let engine = run_engine_results_only(&sql, &test_root);

        if wire.trim_end() == engine.trim_end() {
            eprintln!("mtr-protocol: OK {rel}");
        } else {
            failures.push(format!(
                "=== {rel} ===\n{}",
                diff_labelled(&engine, &wire, "engine", "wire")
            ));
        }
    }

    server.stop();
    if !failures.is_empty() {
        panic!(
            "mtr-protocol: {} script(s) diverged between wire and engine:\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
    eprintln!(
        "mtr-protocol: {} script(s) matched wire == engine",
        scripts.len()
    );
}

/// Feature-gated binary-protocol acceptance. Unlike `mtr_protocol` this uses
/// the Rust client driver's `prep`/`exec` API, guaranteeing COM_STMT_* rather
/// than COM_QUERY.
#[cfg(feature = "mysql-client")]
#[test]
fn mtr_binary_prepared_protocol() {
    if std::env::var_os("MYDB_MTR_PROTO").is_none() {
        return;
    }
    use ::mysql::prelude::Queryable;

    let manifest = env!("CARGO_MANIFEST_DIR");
    let Some(server_bin) = find_mydb_binary(manifest) else {
        panic!("mtr-binary-protocol: build the mydb binary before running this gate");
    };
    let port = free_port();
    let data_dir = tempfile::tempdir().expect("server data dir");
    let mut server = ServerProcess::start(&server_bin, port, data_dir.path());
    let opts: ::mysql::Opts = ::mysql::OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("root"))
        .db_name(Some("test"))
        .into();
    let mut connection = None;
    for _ in 0..150 {
        match ::mysql::Conn::new(opts.clone()) {
            Ok(conn) => {
                connection = Some(conn);
                break;
            }
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(100)),
        }
    }
    let mut conn = connection.unwrap_or_else(|| {
        panic!(
            "mtr-binary-protocol: server never became ready\n{}",
            server.tail_log()
        )
    });

    conn.query_drop(
        "CREATE TABLE binary_ps (
            id INT PRIMARY KEY,
            si BIGINT,
            ui BIGINT UNSIGNED,
            d DECIMAL(20,4),
            payload LONGBLOB,
            j JSON
        )",
    )
    .unwrap();
    let insert = conn
        .prep("INSERT INTO binary_ps VALUES (?, ?, ?, ?, ?, ?)")
        .unwrap();
    let long_payload = vec![0x5au8; 128 * 1024];
    conn.exec_drop(
        &insert,
        (
            1i32,
            -9i64,
            u64::MAX,
            "1234567890123456.2500",
            long_payload.clone(),
            "{\"ok\":true}",
        ),
    )
    .unwrap();

    let select = conn
        .prep(
            "SELECT si, ui, CAST(d AS CHAR), payload, JSON_EXTRACT(j, '$.ok') \
               FROM binary_ps WHERE id = ?",
        )
        .unwrap();
    let row: Option<::mysql::Row> = conn.exec_first(&select, (1i32,)).unwrap();
    let row = row.expect("prepared row");
    assert_eq!(row.len(), 5);
    let payload: Vec<u8> = row.get(3).expect("blob column");
    assert_eq!(payload, long_payload);

    // Re-execute one handle with a NULL and a new concrete type.
    let echo = conn.prep("SELECT ? IS NULL, CAST(? AS SIGNED)").unwrap();
    let null_row: Option<(u8, i64)> = conn.exec_first(&echo, (Option::<i64>::None, "42")).unwrap();
    assert_eq!(null_row, Some((1, 42)));
    let typed_row: Option<(u8, i64)> = conn.exec_first(&echo, (Some(7i64), -3i64)).unwrap();
    assert_eq!(typed_row, Some((0, -3)));

    // Schema changes invalidate cached metadata/plans; execution must
    // transparently rebuild against the new schema.
    let by_id = conn.prep("SELECT si FROM binary_ps WHERE id = ?").unwrap();
    conn.query_drop("ALTER TABLE binary_ps ADD COLUMN extra INT DEFAULT 1")
        .unwrap();
    let value: Option<i64> = conn.exec_first(&by_id, (1i32,)).unwrap();
    assert_eq!(value, Some(-9));

    drop(by_id);
    drop(echo);
    drop(select);
    drop(insert);
    drop(conn);
    server.stop();
}

/// Locate a built `mydb` binary under the workspace target dir.
fn find_mydb_binary(manifest: &str) -> Option<PathBuf> {
    for profile in ["debug", "release"] {
        let p = Path::new(manifest)
            .join("target")
            .join(profile)
            .join("mydb");
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

/// Locate a `mysql` client — PATH, then the common Homebrew keg path.
fn find_mysql_cli() -> Option<PathBuf> {
    if let Ok(out) = std::process::Command::new("sh")
        .arg("-c")
        .arg("command -v mysql")
        .output()
    {
        if out.status.success() {
            let p = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !p.is_empty() {
                return Some(PathBuf::from(p));
            }
        }
    }
    let brew = Path::new("/opt/homebrew/opt/mysql-client/bin/mysql");
    if brew.is_file() {
        return Some(brew.to_path_buf());
    }
    None
}

/// Bind an ephemeral port, read it, and release it (small TOCTOU window is
/// acceptable for a local, on-demand test).
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().unwrap().port()
}

/// Collect files with the given extension under `dir`.
fn collect_ext(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_ext(&path, ext, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some(ext) {
            out.push(path);
        }
    }
}

/// A spawned `mydb` server, killed on `stop`/drop.
struct ServerProcess {
    child: std::process::Child,
    log_path: PathBuf,
}

impl ServerProcess {
    fn start(bin: &Path, port: u16, data_dir: &Path) -> Self {
        let log_path = data_dir.join("server.log");
        let log = std::fs::File::create(&log_path).expect("server log");
        let err = log.try_clone().expect("clone log handle");
        let child = std::process::Command::new(bin)
            .args([
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--data-dir",
                data_dir.to_str().unwrap(),
            ])
            .stdout(std::process::Stdio::from(log))
            .stderr(std::process::Stdio::from(err))
            .spawn()
            .expect("spawn mydb server");
        ServerProcess { child, log_path }
    }

    /// Poll until the server answers `SELECT 1`, up to ~15s.
    fn wait_ready(&mut self, mysql_cli: &Path, port: u16) -> bool {
        for _ in 0..150 {
            let ok = std::process::Command::new(mysql_cli)
                .args(mysql_args(port))
                .arg("-e")
                .arg("SELECT 1")
                .arg("test")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if ok {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        false
    }

    fn tail_log(&self) -> String {
        std::fs::read_to_string(&self.log_path)
            .map(|s| {
                s.lines()
                    .rev()
                    .take(10)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default()
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Common `mysql` CLI connection args (non-TLS, root/root, default db `test`).
fn mysql_args(port: u16) -> Vec<String> {
    vec![
        "-h127.0.0.1".to_string(),
        format!("-P{port}"),
        "-uroot".to_string(),
        "-proot".to_string(),
        "--ssl-mode=DISABLED".to_string(),
    ]
}

/// Pipe a whole SQL script through `mysql --batch` (one session) and return
/// stdout — tab-separated result sets, the same shape our renderer emits.
fn run_mysql_batch(mysql_cli: &Path, port: u16, sql: &str) -> String {
    use std::io::Write;
    let mut child = std::process::Command::new(mysql_cli)
        .args(mysql_args(port))
        .arg("--batch")
        .arg("test")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn mysql client");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(sql.as_bytes())
        .expect("write sql to mysql stdin");
    let out = child.wait_with_output().expect("mysql output");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn diff_labelled(a: &str, b: &str, la: &str, lb: &str) -> String {
    let ae: Vec<&str> = a.lines().collect();
    let be: Vec<&str> = b.lines().collect();
    let max = ae.len().max(be.len());
    let mut lines = vec![format!("{la}(-) vs {lb}(+):")];
    for i in 0..max {
        let x = ae.get(i).copied();
        let y = be.get(i).copied();
        if x != y {
            lines.push(format!("  -[{i}] {}", x.unwrap_or("<none>")));
            lines.push(format!("  +[{i}] {}", y.unwrap_or("<none>")));
        }
    }
    lines.join("\n")
}

fn truncate_lines(s: &str, max: usize) -> String {
    let mut out: Vec<&str> = s.lines().take(max).collect();
    if s.lines().count() > max {
        out.push("... (truncated)");
    }
    out.join("\n")
}

/// Recursively collect `*.test` files under `dir` into `out`.
fn collect_test_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_test_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("test") {
            out.push(path);
        }
    }
}

/// Result of interpreting one `.test` file's source against a fresh engine.
struct ExecOutcome {
    skip: Option<String>,
    violations: Vec<String>,
    out: String,
    /// Statements actually echoed by mysqltest query logging, in execution
    /// order. The triage comparator uses this to distinguish SQL echo
    /// whitespace from result rows and to isolate EXPLAIN/SHOW CREATE output.
    echoed_statements: Vec<String>,
    /// Expanded text emitted by mysqltest `--echo` directives. It is kept
    /// separate from SQL echoes so the semantic comparator can ignore test
    /// narration without guessing from line prefixes such as `#`.
    display_echoes: Vec<String>,
}

/// Run `execute_test` on a dedicated 256 MiB-stack worker thread with panic
/// catching, so a single pathological upstream file (deep expression recursion
/// in the engine, or a panic) is reported as an error for that file instead of
/// aborting the whole triage batch. Only used by the triage scanner.
fn execute_test_isolated(test_path: &Path, test_root: &Path) -> Result<ExecOutcome, String> {
    let path = test_path.to_path_buf();
    let root = test_root.to_path_buf();
    let handle = std::thread::Builder::new()
        .stack_size(256 * 1024 * 1024)
        .spawn(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute_test(&path, &root)))
        })
        .map_err(|e| format!("spawn worker: {e}"))?;
    match handle.join() {
        Ok(Ok(res)) => res,
        Ok(Err(payload)) => Err(format!(
            "panicked during execution: {}",
            panic_payload_message(payload.as_ref())
        )),
        Err(payload) => Err(format!(
            "worker thread crashed: {}",
            panic_payload_message(payload.as_ref())
        )),
    }
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    "<non-string panic payload>".to_string()
}

#[test]
fn mtr_panic_payload_message_preserves_diagnostics() {
    let owned: Box<dyn std::any::Any + Send> = Box::new("owned detail".to_string());
    let borrowed: Box<dyn std::any::Any + Send> = Box::new("static detail");
    assert_eq!(panic_payload_message(owned.as_ref()), "owned detail");
    assert_eq!(panic_payload_message(borrowed.as_ref()), "static detail");
}

/// Read a `.test`/`.inc` file using the charset selected by preceding
/// `SET NAMES` / `--character_set` directives. Upstream MTR sources may
/// switch encodings inside one file; whole-file UTF-8 lossy decoding destroys
/// the very bytes exercised by ENUM/SET/charset tests.
fn read_test_source(path: &Path) -> Result<String, std::io::Error> {
    let bytes = std::fs::read(path)?;
    let mut charset = "utf8mb4".to_string();
    let mut source = String::with_capacity(bytes.len());
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let decoded = match charset.as_str() {
            // MTR's latin1 source files use all byte values, including the
            // C1 range that WHATWG/windows-1252 marks undefined. A direct
            // one-byte-to-code-point mapping preserves both value and length.
            "latin1" => line.iter().map(|byte| char::from(*byte)).collect(),
            "utf8" | "utf8mb3" | "utf8mb4" => String::from_utf8_lossy(line).into_owned(),
            other => crate::charset::decode_bytes(other, line)
                .unwrap_or_else(|_| String::from_utf8_lossy(line).into_owned()),
        };
        source.push_str(&decoded);

        let directive = decoded.trim().to_ascii_lowercase();
        let selected = directive
            .strip_prefix("--character_set ")
            .or_else(|| directive.strip_prefix("set names "))
            .map(|value| {
                value
                    .trim()
                    .trim_end_matches(';')
                    .split_ascii_whitespace()
                    .next()
                    .unwrap_or("")
            });
        if let Some(selected) = selected {
            charset = if selected.eq_ignore_ascii_case("default") {
                "utf8mb4".to_string()
            } else {
                selected.to_string()
            };
        }
    }
    Ok(source)
}

#[test]
fn mtr_source_decoding_tracks_inline_charset_switches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mixed.test");
    std::fs::write(
        &path,
        b"SELECT 'utf8';\nSET NAMES latin1;\nSELECT '\xe4';\n--character_set utf8mb4\nSELECT '\xc3\xb6';\n",
    )
    .unwrap();
    assert_eq!(
        read_test_source(&path).unwrap(),
        "SELECT 'utf8';\nSET NAMES latin1;\nSELECT 'ä';\n--character_set utf8mb4\nSELECT 'ö';\n"
    );
}

#[test]
fn mtr_std_data_paths_are_rewritten_and_confined() {
    let root = tempfile::tempdir().unwrap();
    let std_data = root.path().join("std_data");
    std::fs::create_dir(&std_data).unwrap();
    std::fs::write(std_data.join("fixture.dat"), b"1\n").unwrap();

    let source = "LOAD DATA INFILE '../../std_data/fixture.dat' INTO TABLE t CHARACTER SET utf8mb3";
    let rewritten = resolve_std_data_fixture_paths(source, root.path()).unwrap();
    assert!(rewritten.contains(std_data.join("fixture.dat").to_string_lossy().as_ref()));
    assert!(!rewritten.contains("../../std_data/"));

    let missing = resolve_std_data_fixture_paths(
        "LOAD DATA INFILE '../../std_data/missing.dat' INTO TABLE t",
        root.path(),
    )
    .unwrap();
    assert!(missing.contains(std_data.join("missing.dat").to_string_lossy().as_ref()));

    let load_file = resolve_std_data_fixture_paths(
        "SELECT LOAD_FILE('../../std_data/fixture.dat')",
        root.path(),
    )
    .unwrap();
    assert!(load_file.contains(std_data.join("fixture.dat").to_string_lossy().as_ref()));

    let escaped = resolve_std_data_fixture_paths(
        "LOAD DATA INFILE '../../std_data/../secret' INTO TABLE t",
        root.path(),
    )
    .unwrap_err();
    assert_eq!(escaped.code(), 1085);
}

/// Run a raw SQL script through the strict-durability in-process engine with
/// SQL echo and warning auto-expansion off, so the output is *results only* —
/// the same shape `mysql --batch` prints. File-bundle DDL such as EXCHANGE
/// PARTITION depends on dirty-page flush + rename ordering, so the
/// durability-memory fixture is not a valid oracle for this protocol gate.
fn run_engine_results_only(source: &str, test_root: &Path) -> String {
    let tmp = match tempfile::tempdir() {
        Ok(t) => t,
        Err(e) => return format!("<engine tempdir error: {e}>"),
    };
    let engine = Arc::new(open_strict_test_engine(tmp.path()));
    let mut runner = Runner::new(engine, test_root.to_path_buf(), tmp.path());
    runner.query_log = false;
    runner.warnings_enabled = false;
    let lines: Vec<String> = source.lines().map(|l| l.to_string()).collect();
    runner.exec_range(&lines, 0, lines.len());
    runner.drain_pending_sends();
    runner.out
}

/// Read and interpret a `.test` file against a fresh in-memory engine.
fn execute_test(test_path: &Path, test_root: &Path) -> Result<ExecOutcome, String> {
    let source = read_test_source(test_path).map_err(|e| format!("cannot read test file: {e}"))?;
    let max_digest_length = read_mtr_max_digest_length(test_path)?;
    let tmp = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let engine = Arc::new(open_test_engine(tmp.path()));
    // Match mysql-test-run's freshly initialized MySQL 8.0 grant catalog.
    // Several upstream main tests legitimately query mysql.user; a production
    // mydb data directory starts with root only, but the upstream oracle has
    // these three locked internal accounts in addition to root. Seed them for
    // every isolated MTR file rather than special-casing individual queries.
    for account in ["mysql.infoschema", "mysql.session", "mysql.sys"] {
        engine
            .execute(&format!(
                "CREATE USER '{account}'@'localhost' IDENTIFIED BY 'mtr-internal-locked'"
            ))
            .map_err(|error| format!("seed MTR system account {account}: {error}"))?;
    }
    // Sysvar GLOBAL backends are process-wide by design. A triage scan runs
    // many otherwise-isolated server instances in one process, so restore the
    // globals mutated by upstream main tests before creating the file's first
    // session; otherwise file order changes semantics (notably a preceding
    // max_allowed_packet test can turn a later REPEAT() into NULL).
    crate::engine::sysvar::builtin::connection::reset_max_allowed_packet_for_mtr();
    crate::engine::sysvar::builtin::query::reset_query_globals_for_mtr();
    let mut runner = Runner::new(engine, test_root.to_path_buf(), tmp.path());
    let lines: Vec<String> = source.lines().map(|l| l.to_string()).collect();
    crate::engine::expr::with_statement_digest_token_limit(max_digest_length, || {
        runner.exec_range(&lines, 0, lines.len());
        // Join any statements still in flight so their threads (which borrow
        // the engine) finish before `tmp` is dropped.
        runner.drain_pending_sends();
    });
    Ok(ExecOutcome {
        skip: runner.skip,
        violations: runner.violations,
        out: runner.out,
        echoed_statements: runner.echoed_statements,
        display_echoes: runner.display_echoes,
    })
}

/// Read startup options accompanying an upstream MTR test. mysql-test-run
/// applies `<name>-master.opt` (or `<name>.opt`) when it starts the server;
/// importing the `.test` without this metadata changes server semantics.
///
/// Only options implemented by the in-process server may appear here. An
/// unknown option fails loudly instead of being silently ignored.
fn read_mtr_max_digest_length(test_path: &Path) -> Result<Option<usize>, String> {
    let stem = test_path
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid MTR test path {}", test_path.display()))?;
    let candidates = [
        test_path.with_file_name(format!("{stem}-master.opt")),
        test_path.with_extension("opt"),
    ];
    let Some(path) = candidates.iter().find(|path| path.is_file()) else {
        return Ok(None);
    };
    let options = std::fs::read_to_string(path)
        .map_err(|error| format!("read {}: {error}", path.display()))?;
    let mut max_digest_length = None;
    for option in options.split_whitespace() {
        if option.starts_with('#') {
            break;
        }
        let Some(value) = option.strip_prefix("--max-digest-length=") else {
            return Err(format!(
                "unsupported MTR server option `{option}` in {}",
                path.display()
            ));
        };
        let parsed = value.parse::<usize>().map_err(|_| {
            format!(
                "invalid --max-digest-length value `{value}` in {}",
                path.display()
            )
        })?;
        if parsed == 0 {
            return Err(format!(
                "--max-digest-length must be positive in {}",
                path.display()
            ));
        }
        max_digest_length = Some(parsed);
    }
    Ok(max_digest_length)
}

fn run_one(test_path: &Path, golden: &Path, test_root: &Path, record: bool) -> FileOutcome {
    let outcome = match execute_test(test_path, test_root) {
        Ok(o) => o,
        Err(e) => return FileOutcome::Failed(e),
    };

    if let Some(reason) = outcome.skip {
        return FileOutcome::Skipped(reason);
    }
    if !outcome.violations.is_empty() {
        // Expectation violations are hard failures independent of the golden
        // diff — do not record a broken baseline from them.
        return FileOutcome::Failed(outcome.violations.join("\n"));
    }

    let out = outcome.out;

    if record {
        if let Some(parent) = golden.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                return FileOutcome::Failed(format!("mkdir {}: {e}", parent.display()));
            }
        }
        if let Err(e) = std::fs::write(golden, &out) {
            return FileOutcome::Failed(format!("write golden {}: {e}", golden.display()));
        }
        return FileOutcome::Recorded;
    }

    let expected = match std::fs::read_to_string(golden) {
        Ok(s) => s,
        Err(_) => {
            return FileOutcome::Failed(format!(
                "missing golden {} — run with MYDB_MTR_RECORD=1 to create it",
                golden.display()
            ));
        }
    };

    if expected == out {
        FileOutcome::Passed
    } else {
        FileOutcome::Failed(diff(&expected, &out))
    }
}

/// A statement dispatched with `send`, running on a background thread until
/// `reap` joins it. Carries the output modifiers captured at `send` time so
/// `reap` renders identically to a synchronous statement.
struct SendHandle {
    join: JoinHandle<(SessionContext, Result<QueryResponse, QueryError>)>,
    exec: String,
    expect: Vec<Expected>,
    mods: RenderMods,
    replace_regex: Vec<(regex::Regex, String)>,
    replace_result: Vec<(String, String)>,
    result_log: bool,
    warnings_enabled: bool,
}

/// Stateful interpreter for one `.test` file.
struct Runner {
    engine: Arc<StorageQueryEngine>,
    /// Named connections; `"default"` always exists.
    sessions: HashMap<String, SessionContext>,
    /// Name of the active connection.
    current: String,
    /// Outstanding `send` per connection, awaiting `reap`.
    pending_sends: HashMap<String, SendHandle>,
    test_root: PathBuf,
    /// Per-test writable root exposed as `$MYSQLTEST_VARDIR`. Filesystem
    /// directives are confined to this directory.
    vardir: PathBuf,
    vars: HashMap<String, String>,
    out: String,
    query_log: bool,
    result_log: bool,
    warnings_enabled: bool,
    /// `--disable_abort_on_error`: when false, an unexpected statement error is
    /// rendered but does not count as a violation (the test tolerates errors).
    abort_on_error: bool,
    /// Whether subsequent statements should be exercised through prepared
    /// statement semantics. The in-process backend records the state; the
    /// feature-gated wire backend performs the actual binary protocol switch.
    ps_protocol: bool,
    /// Persistent mysqltest output mode toggled by `--vertical_results` /
    /// `--horizontal_results`.
    vertical_results: bool,
    metadata_enabled: bool,
    /// A normal mysqltest `exit` stops the current file without turning it
    /// into a skip or a failed SQL statement.
    terminated: bool,
    /// `--disable_testcase` … `--enable_testcase`: a block upstream disables
    /// (a known-broken testcase). mysqltest does not execute the enclosed
    /// statements and the upstream `.result` has no output for them, so the
    /// harness must skip them too (verified against func_str.result).
    testcase_disabled: bool,
    delimiter: String,
    violations: Vec<String>,
    /// When set, the whole file is reported as skipped (unsupported include,
    /// unresolved `ER_` symbol, or unsupported directive).
    skip: Option<String>,
    // Modifiers pending for the next SQL statement.
    p_expect: Vec<Expected>,
    p_mods: RenderMods,
    p_replace_regex: Vec<(regex::Regex, String)>,
    p_replace_result: Vec<(String, String)>,
    echoed_statements: Vec<String>,
    display_echoes: Vec<String>,
}

impl Runner {
    fn new(engine: Arc<StorageQueryEngine>, test_root: PathBuf, vardir: &Path) -> Self {
        let mut sessions = HashMap::new();
        sessions.insert("default".to_string(), new_mtr_session(&engine));
        let vardir = vardir.to_path_buf();
        let tmp_dir_result = std::fs::create_dir_all(vardir.join("tmp"));
        let mut vars = HashMap::new();
        // Common mysqltest built-ins used by upstream main/ and JSON suites.
        // MYSQLTEST_VARDIR is per-test and writable; DEFAULT_ENGINE reflects
        // the engine name exposed by mydb's default_storage_engine sysvar.
        vars.insert("default_engine".to_string(), "mydb".to_string());
        vars.insert("ps_protocol".to_string(), "0".to_string());
        vars.insert(
            "explicit_defaults_for_timestamp".to_string(),
            "1".to_string(),
        );
        vars.insert(
            "mysqltest_vardir".to_string(),
            crate::util::file::sql_path(&vardir),
        );
        Runner {
            engine,
            sessions,
            current: "default".to_string(),
            pending_sends: HashMap::new(),
            test_root,
            vardir,
            vars,
            out: String::new(),
            query_log: true,
            result_log: true,
            warnings_enabled: true,
            abort_on_error: true,
            ps_protocol: false,
            vertical_results: false,
            metadata_enabled: false,
            terminated: false,
            testcase_disabled: false,
            delimiter: ";".to_string(),
            violations: Vec::new(),
            skip: tmp_dir_result
                .err()
                .map(|e| format!("cannot initialize $MYSQLTEST_VARDIR/tmp: {e}")),
            p_expect: Vec::new(),
            p_mods: RenderMods::default(),
            p_replace_regex: Vec::new(),
            p_replace_result: Vec::new(),
            echoed_statements: Vec::new(),
            display_echoes: Vec::new(),
        }
    }

    /// Execute a statement on the active connection, returning the owned
    /// outcome (the session borrow is released before the caller touches other
    /// `self` fields).
    fn exec_on_current(&mut self, sql: &str) -> Result<QueryResponse, QueryError> {
        let engine = Arc::clone(&self.engine);
        let sess = self
            .sessions
            .get_mut(&self.current)
            .expect("active connection has a session");
        engine.execute_with_session(sess, sql)
    }

    /// Execute an ordinary mysqltest SQL statement under the currently
    /// selected protocol mode. The in-process backend exercises the same
    /// named prepared-statement AST/bind path; the feature-gated protocol
    /// suite separately verifies COM_STMT_* packet behaviour.
    fn exec_sql_statement(&mut self, sql: &str) -> Result<QueryResponse, QueryError> {
        // mysql-test exposes the same read-only fixture directory through
        // both ../../std_data and $MYSQLTEST_VARDIR/std_data. Normalize the
        // latter before the common confined resolver, without copying large
        // fixtures into every per-test vardir.
        let vardir_prefix = format!("{}/", self.vardir.join("std_data").to_string_lossy());
        let normalized = sql.replace(&vardir_prefix, "../../std_data/");
        let execution_sql = resolve_std_data_fixture_paths(&normalized, &self.test_root)?;
        if !self.ps_protocol || !is_ps_eligible(&execution_sql) {
            return self.exec_on_current(&execution_sql);
        }
        let engine = Arc::clone(&self.engine);
        let sess = self
            .sessions
            .get_mut(&self.current)
            .expect("active connection has a session");
        let prepare = format!(
            "PREPARE __mydb_mtr_ps FROM '{}'",
            escape_mysql_string(&execution_sql)
        );
        engine.execute_with_session(sess, &prepare)?;
        engine.execute_with_session(sess, "EXECUTE __mydb_mtr_ps")
    }

    /// Run lines in `[start, end)`; stops early once `skip` is set.
    fn exec_range(&mut self, lines: &[String], start: usize, end: usize) {
        let mut i = start;
        while i < end && self.skip.is_none() && !self.terminated {
            i = self.exec_one(lines, i, end);
        }
    }

    /// Process the logical unit beginning at line `i`; return the next index.
    fn exec_one(&mut self, lines: &[String], i: usize, end: usize) -> usize {
        let raw = &lines[i];
        let trimmed = raw.trim();

        // mysqltest accepts whitespace between `--` and a command name (for
        // example `-- error 1264`).  SQL also uses `-- ` for comments, so only
        // route a spaced line through directive dispatch when its command is
        // one of the mysqltest commands understood by this runner.  Doing the
        // generic SQL-comment check first silently discarded the expectation
        // and turned a matching engine error into a false VIOLATION.
        if trimmed.starts_with("-- ") {
            if let Some((cmd, arg)) = classify_directive(trimmed) {
                if is_known_directive_command(&cmd) {
                    return self.exec_directive(lines, i, end, cmd, arg);
                }
            }
            return i + 1;
        }

        // Comments: `#...`, and SQL-style `-- ` (dash-dash-space) or a bare
        // `--`. A `--word` (no space) is a mysqltest directive, handled below.
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed == "--" {
            return i + 1;
        }

        // Inside a `--disable_testcase` block: skip everything (consuming any
        // multi-line SQL) until the matching `--enable_testcase`.
        if self.testcase_disabled {
            if let Some((cmd, _)) = classify_directive(trimmed) {
                if cmd == "enable_testcase" {
                    self.testcase_disabled = false;
                }
                return i + 1;
            }
            let (_statements, next) = self.read_statements(lines, i, end);
            return next;
        }

        if let Some((cmd, arg)) = classify_directive(trimmed) {
            return self.exec_directive(lines, i, end, cmd, arg);
        }

        // A stray closing brace (defensive — block matching should consume it).
        if trimmed == "}" {
            return i + 1;
        }

        // Otherwise: a SQL statement, possibly spanning lines.
        let (statements, next) = self.read_statements(lines, i, end);
        for stmt in statements {
            self.run_sql(&stmt, false);
        }
        next
    }

    /// Directive dispatch. `cmd` is the lowercase command word, `arg` its
    /// trimmed remainder. Returns the next line index.
    fn exec_directive(
        &mut self,
        lines: &[String],
        i: usize,
        end: usize,
        cmd: String,
        arg: String,
    ) -> usize {
        match cmd.as_str() {
            "echo" => {
                let text = interp::substitute_vars(&arg, &self.vars);
                self.out.push_str(&text);
                self.out.push('\n');
                self.display_echoes.push(text);
                i + 1
            }
            "disable_query_log" => {
                self.query_log = false;
                i + 1
            }
            "enable_query_log" => {
                self.query_log = true;
                i + 1
            }
            "disable_result_log" => {
                self.result_log = false;
                i + 1
            }
            "enable_result_log" => {
                self.result_log = true;
                i + 1
            }
            "disable_warnings" => {
                self.warnings_enabled = false;
                i + 1
            }
            "enable_warnings" => {
                self.warnings_enabled = true;
                i + 1
            }
            "sorted_result" => {
                self.p_mods.sorted = true;
                i + 1
            }
            "partially_sorted_result" => {
                let expanded = interp::substitute_vars(&arg, &self.vars);
                match expanded.trim().parse::<usize>() {
                    Ok(start_column) => {
                        self.p_mods.partially_sorted_from = Some(start_column);
                    }
                    Err(_) => {
                        self.skip =
                            Some(format!("invalid --partially_sorted_result: `{expanded}`"));
                    }
                }
                i + 1
            }
            "replace_column" => {
                let expanded = interp::substitute_vars(&arg, &self.vars);
                match interp::parse_replace_column(&expanded) {
                    Ok(pairs) => self.p_mods.replace_column = pairs,
                    Err(e) => self.skip = Some(e),
                }
                i + 1
            }
            "replace_result" => {
                let expanded = interp::substitute_vars(&arg, &self.vars);
                match interp::parse_replace_result(&expanded) {
                    Ok(pairs) => self.p_replace_result = pairs,
                    Err(e) => self.skip = Some(e),
                }
                i + 1
            }
            "replace_regex" => {
                let expanded = interp::substitute_vars(&arg, &self.vars);
                match interp::parse_replace_regex(&expanded) {
                    Ok(pairs) => self.p_replace_regex = pairs,
                    Err(e) => self.skip = Some(e),
                }
                i + 1
            }
            "error" => {
                for spec in arg.split(',') {
                    // Tolerate a trailing comma / empty token (`--error ER_X,`).
                    if spec.trim().is_empty() {
                        continue;
                    }
                    match interp::parse_error_spec(spec.trim()) {
                        Ok(e) => self.p_expect.push(e),
                        Err(e) => {
                            self.skip = Some(e);
                            return i + 1;
                        }
                    }
                }
                i + 1
            }
            "let" => {
                // Dashed `--let` is line-scoped. Bare `let` is terminated by
                // the active mysqltest delimiter and may span lines (for
                // example when it stores a routine body).
                if should_collect_multiline_let(&lines[i], &arg, &self.delimiter) {
                    let mut joined = arg.clone();
                    let mut j = i + 1;
                    while j < end {
                        joined.push('\n');
                        joined.push_str(lines[j].as_str());
                        let (statements, has_tail) =
                            split_delimited_statements(&joined, &self.delimiter);
                        if !statements.is_empty() && !has_tail {
                            if statements.len() != 1 {
                                self.skip = Some(format!(
                                    "let contains {} delimiter-terminated units",
                                    statements.len()
                                ));
                                return j + 1;
                            }
                            self.exec_let(statements[0].trim());
                            return j + 1;
                        }
                        j += 1;
                    }
                    // No terminator found — retain mysqltest's permissive
                    // single-line fallback so the resulting SQL reports the
                    // useful error instead of silently swallowing the file.
                }
                self.exec_let(&arg);
                i + 1
            }
            "inc" => {
                self.exec_inc_dec(&arg, 1);
                i + 1
            }
            "dec" => {
                self.exec_inc_dec(&arg, -1);
                i + 1
            }
            "eval" => {
                // A dashed mysqltest command is line-scoped: `--eval $query`
                // is complete at the newline even though it has no SQL
                // delimiter. Bare `eval` retains mysqltest's delimiter-aware
                // multi-line form.
                let dashed = lines[i].trim_start().starts_with("--");
                let (statements, next) = if dashed {
                    (directive_line_statements(&arg, &self.delimiter), i + 1)
                } else {
                    self.read_statements_from(lines, i, end, &arg)
                };
                for stmt in statements {
                    let substituted = interp::substitute_vars(&stmt, &self.vars);
                    self.run_sql(&substituted, false);
                }
                next
            }
            "query_vertical" | "query_horizontal" => {
                let (statements, next) = self.read_statements_from(lines, i, end, arg.as_str());
                for stmt in statements {
                    self.p_mods.vertical = cmd == "query_vertical";
                    self.run_sql(&stmt, false);
                }
                next
            }
            "delimiter" => {
                // mysqltest statement modifiers (notably `--error`) apply to
                // the next SQL statement, not to control commands.  A
                // delimiter transition is a command boundary; carrying the
                // pending expectation across it makes the following routine
                // definition consume an expectation that belonged to the
                // preceding command.
                self.reset_pending();
                self.set_delimiter(&arg);
                i + 1
            }
            "source" => self.exec_source(&arg, i),
            "remove_file" => {
                self.exec_remove_file(&arg);
                i + 1
            }
            "connect" => {
                self.exec_connect(&arg);
                i + 1
            }
            "connection" => {
                self.exec_connection(&arg);
                i + 1
            }
            "disconnect" => {
                self.exec_disconnect(&arg);
                i + 1
            }
            "reap" => {
                self.exec_reap();
                i + 1
            }
            "send" => {
                // As with `--eval`, dashed `--send SQL` is a complete
                // mysqltest command at the newline. It must not consume a
                // following `connection`/`reap` directive while looking for
                // an optional semicolon.
                let dashed = lines[i].trim_start().starts_with("--");
                let (statements, next) = if dashed {
                    (directive_line_statements(&arg, &self.delimiter), i + 1)
                } else {
                    self.read_statements_from(lines, i, end, &arg)
                };
                if statements.len() != 1 {
                    self.violations.push(format!(
                        "send requires exactly one statement, got {}",
                        statements.len()
                    ));
                } else {
                    self.exec_send(&statements[0]);
                }
                next
            }
            "disable_ps_protocol" => {
                self.ps_protocol = false;
                self.vars.insert("ps_protocol".to_string(), "0".to_string());
                i + 1
            }
            "enable_ps_protocol" => {
                self.ps_protocol = true;
                self.vars.insert("ps_protocol".to_string(), "1".to_string());
                i + 1
            }
            "vertical_results" => {
                self.vertical_results = true;
                i + 1
            }
            "horizontal_results" => {
                self.vertical_results = false;
                i + 1
            }
            "enable_metadata" => {
                self.metadata_enabled = true;
                i + 1
            }
            "disable_metadata" => {
                self.metadata_enabled = false;
                i + 1
            }
            "exit" => {
                self.terminated = true;
                i + 1
            }
            "if" => self.exec_block(lines, i, end, false),
            "while" => self.exec_block(lines, i, end, true),
            "disable_testcase" => {
                self.testcase_disabled = true;
                i + 1
            }
            "enable_testcase" => {
                self.testcase_disabled = false;
                i + 1
            }
            "disable_abort_on_error" => {
                self.abort_on_error = false;
                i + 1
            }
            "enable_abort_on_error" => {
                self.abort_on_error = true;
                i + 1
            }
            // Cosmetic / protocol-only directives with no effect on our
            // in-process golden model — accepted as no-ops so the file still
            // runs (and yields DIFF/VIOLATION signal) instead of being skipped.
            other if IGNORED_DIRECTIVES.contains(&other) => i + 1,
            other => {
                self.skip = Some(format!(
                    "unsupported mysqltest directive `--{other}` (M1 subset)"
                ));
                i + 1
            }
        }
    }

    fn exec_let(&mut self, arg: &str) {
        // `$name = value` (the leading `$` and spaces around `=` are flexible).
        let Some((lhs, rhs)) = arg.split_once('=') else {
            self.skip = Some(format!("malformed let: `{arg}`"));
            return;
        };
        let name = lhs
            .trim()
            .trim_start_matches('$')
            .trim()
            .to_ascii_lowercase();
        let rhs = rhs.trim();
        let value = self.resolve_rhs(rhs);
        self.vars.insert(name, value);
    }

    fn exec_remove_file(&mut self, arg: &str) {
        let expanded = interp::substitute_vars(arg, &self.vars);
        let value = expanded.trim();
        let value = if value.len() >= 2
            && ((value.starts_with('\'') && value.ends_with('\''))
                || (value.starts_with('"') && value.ends_with('"')))
        {
            &value[1..value.len() - 1]
        } else {
            value
        };
        if value.is_empty() {
            self.violations
                .push("remove_file requires a file path".to_string());
            return;
        }

        let target = match resolve_remove_file_target(&self.vardir, Path::new(value)) {
            Ok(target) => target,
            Err(e) => {
                self.violations.push(e);
                return;
            }
        };
        if let Err(e) = std::fs::remove_file(&target) {
            self.violations
                .push(format!("remove_file {} failed: {e}", target.display()));
        }
    }

    /// Resolve a `let`/condition right-hand side: `` `SQL` `` scalar,
    /// `query_get_value(SQL, Col, Row)`, or a literal (with var substitution).
    fn resolve_rhs(&mut self, rhs: &str) -> String {
        if rhs.len() >= 2 && rhs.starts_with('`') && rhs.ends_with('`') {
            let sql = &rhs[1..rhs.len() - 1];
            let sql = interp::substitute_vars(sql, &self.vars);
            return self.scalar_query(&sql);
        }
        if let Some(inner) = rhs
            .strip_prefix("query_get_value(")
            .and_then(|s| s.strip_suffix(')'))
        {
            return self.query_get_value(inner);
        }
        interp::substitute_vars(rhs, &self.vars)
    }

    fn exec_inc_dec(&mut self, arg: &str, delta: i64) {
        let name = arg
            .trim()
            .trim_start_matches('$')
            .trim()
            .to_ascii_lowercase();
        let cur = self
            .vars
            .get(&name)
            .and_then(|v| v.trim().parse::<i64>().ok())
            .unwrap_or(0);
        self.vars.insert(name, (cur + delta).to_string());
    }

    fn set_delimiter(&mut self, arg: &str) {
        // `delimiter <new>` — the line may itself end with the *current*
        // delimiter; strip it, then take the first token as the new delimiter.
        let mut s = arg.trim();
        if let Some(stripped) = s.strip_suffix(&self.delimiter) {
            s = stripped.trim();
        }
        let new = s.split_whitespace().next().unwrap_or(";");
        if !new.is_empty() {
            self.delimiter = new.to_string();
        }
    }

    /// `connect (name, host, user, pass[, db[, port[, sock[, opts]]]])`.
    /// In-process there is no real network/auth, so host/user/pass/port/sock
    /// are ignored (the session runs as the default account); only the name
    /// and optional default database are honoured. `connect` also switches the
    /// active connection to the new one.
    fn exec_connect(&mut self, arg: &str) {
        let inner = arg.trim().trim_start_matches('(').trim_end_matches(')');
        let fields: Vec<String> = inner
            .split(',')
            .map(|s| interp::substitute_vars(s.trim(), &self.vars))
            .collect();
        let name = fields.first().cloned().unwrap_or_default();
        if name.is_empty() {
            self.skip = Some("connect without a connection name".to_string());
            return;
        }
        let mut sess = new_mtr_session(&self.engine);
        if let Some(db) = fields.get(4).filter(|d| !d.is_empty()) {
            let engine = Arc::clone(&self.engine);
            let _ = engine.execute_with_session(&mut sess, &format!("USE {db}"));
        }
        self.sessions.insert(name.clone(), sess);
        self.current = name;
    }

    fn exec_connection(&mut self, arg: &str) {
        let name = interp::substitute_vars(arg.trim(), &self.vars);
        // A connection with an outstanding `send` has its session temporarily
        // moved into a thread but is still a valid switch target (you switch to
        // it to `reap`).
        if !self.sessions.contains_key(&name) && !self.pending_sends.contains_key(&name) {
            self.violations
                .push(format!("connection `{name}` does not exist"));
            return;
        }
        self.current = name;
    }

    fn exec_disconnect(&mut self, arg: &str) {
        let name = interp::substitute_vars(arg.trim(), &self.vars);
        if let Some(handle) = self.pending_sends.remove(&name) {
            let _ = handle.join.join();
        }
        self.sessions.remove(&name);
        if self.current == name {
            self.current = "default".to_string();
        }
    }

    /// `send <stmt>`: run `stmt` on the active connection on a background
    /// thread so it can block on a lock held by another connection; the SQL is
    /// echoed now, the result rendered at `reap`.
    fn exec_send(&mut self, stmt: &str) {
        let exec = stmt.trim().to_string();
        if self.pending_sends.contains_key(&self.current) {
            self.violations.push(format!(
                "connection `{}` already has an outstanding send",
                self.current
            ));
            return;
        }
        if self.query_log {
            // mysqltest replacement directives transform the server payload,
            // not the query-log echo. Applying them here rewrites SQL source
            // text (for example `$DEFAULT_ENGINE` masking turns the literal
            // in `SELECT 'mydb'` into `SELECT 'ENGINE'`).
            let echo = format!("{exec}{}", self.delimiter);
            self.out.push_str(&echo);
            self.out.push('\n');
            self.echoed_statements.push(echo);
        }
        let Some(mut sess) = self.sessions.remove(&self.current) else {
            self.violations.push(format!(
                "connection `{}` has no session available to send on",
                self.current
            ));
            return;
        };
        let result_charset = sess.character_set_results.clone();
        let engine = Arc::clone(&self.engine);
        let sql = exec.clone();
        let ps_protocol = self.ps_protocol && is_ps_eligible(&sql);
        let join = std::thread::spawn(move || {
            let res = if ps_protocol {
                let prepare = format!("PREPARE __mydb_mtr_ps FROM '{}'", escape_mysql_string(&sql));
                engine
                    .execute_with_session(&mut sess, &prepare)
                    .and_then(|_| engine.execute_with_session(&mut sess, "EXECUTE __mydb_mtr_ps"))
            } else {
                engine.execute_with_session(&mut sess, &sql)
            };
            (sess, res)
        });
        self.p_mods.vertical |= self.vertical_results;
        self.p_mods.metadata |= self.metadata_enabled;
        self.p_mods.result_charset = result_charset;
        let handle = SendHandle {
            join,
            exec,
            expect: std::mem::take(&mut self.p_expect),
            mods: std::mem::take(&mut self.p_mods),
            replace_regex: std::mem::take(&mut self.p_replace_regex),
            replace_result: std::mem::take(&mut self.p_replace_result),
            result_log: self.result_log,
            warnings_enabled: self.warnings_enabled,
        };
        self.pending_sends.insert(self.current.clone(), handle);
    }

    /// `reap`: join the active connection's outstanding `send`, restore its
    /// session, and render the result using the modifiers captured at send.
    fn exec_reap(&mut self) {
        let Some(handle) = self.pending_sends.remove(&self.current) else {
            self.violations.push(format!(
                "reap on `{}` with no outstanding send",
                self.current
            ));
            return;
        };
        let (sess, result) = match handle.join.join() {
            Ok(v) => v,
            Err(_) => {
                self.violations.push("sent statement panicked".to_string());
                return;
            }
        };
        self.sessions.insert(self.current.clone(), sess);

        let mut stmt_out = String::new();
        match result {
            Ok(resp) => {
                if !handle.expect.is_empty() && !success_matches(&handle.expect) {
                    self.violations.push(format!(
                        "sent statement expected to fail but succeeded: `{}`",
                        handle.exec
                    ));
                }
                if handle.result_log {
                    render::render_response(&resp, &handle.mods, &mut stmt_out);
                    if handle.warnings_enabled {
                        self.warnings_block(&handle.exec, &mut stmt_out, handle.mods.vertical);
                    }
                    if handle.mods.sorted {
                        sort_mysqltest_statement_output(&mut stmt_out);
                    }
                }
            }
            Err(err) => {
                if handle.expect.is_empty() {
                    self.violations.push(format!(
                        "sent statement failed unexpectedly: `{}` -> ERROR {} ({}): {}",
                        handle.exec,
                        err.code(),
                        err.sql_state(),
                        err.message()
                    ));
                } else if !error_matches(&handle.expect, err.code(), &err.sql_state()) {
                    self.violations.push(format!(
                        "sent statement `{}` expected error {:?} but got {} ({}): {}",
                        handle.exec,
                        handle.expect,
                        err.code(),
                        err.sql_state(),
                        err.message()
                    ));
                }
                if handle.result_log {
                    render::render_error(&err, &mut stmt_out);
                }
            }
        }
        for (from, to) in &handle.replace_result {
            stmt_out = stmt_out.replace(from, to);
        }
        for (re, repl) in &handle.replace_regex {
            stmt_out = re.replace_all(&stmt_out, repl.as_str()).into_owned();
        }
        self.out.push_str(&stmt_out);
    }

    /// Join every still-outstanding `send` (called at end of file) so their
    /// engine-borrowing threads finish before teardown.
    fn drain_pending_sends(&mut self) {
        let names: Vec<String> = self.pending_sends.keys().cloned().collect();
        for name in names {
            if let Some(handle) = self.pending_sends.remove(&name) {
                let _ = handle.join.join();
            }
        }
    }

    fn exec_source(&mut self, arg: &str, i: usize) -> usize {
        let rel = interp::substitute_vars(arg.trim(), &self.vars);
        let basename = Path::new(&rel).file_name().unwrap_or_default();

        // Capability guards mydb genuinely cannot satisfy (debug-only server
        // instrumentation and cursor-protocol requirement): honestly
        // skip the whole file rather than no-op past the guard and mis-run the
        // debug-only SQL that follows. Distinct from satisfiable guards
        // (have_innodb_16k, …) which get a no-op stub in tests/mydb-test/inc/.
        const SKIP_GUARDS: &[&str] = &[
            "have_debug.inc",
            "have_debug_sync.inc",
            // Honest capability/mode guards mydb's harness does not run.
            "big_test.inc",
        ];
        if let Some(name) = basename.to_str() {
            if SKIP_GUARDS.contains(&name) {
                self.skip = Some(format!(
                    "requires capability mydb lacks (guard include `{name}`)"
                ));
                return i + 1;
            }
        }
        // Shared committed stub dir: no-op / semantic stand-ins for common
        // upstream guard includes (no_valgrind, no_ps_protocol, …) so triaged
        // files run instead of skipping. See tests/mydb-test/inc/.
        let stub_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/mydb-test/inc");
        let candidates = [
            self.test_root.join(&rel),
            self.test_root.join("include").join(basename),
            self.test_root.join("inc").join(basename),
            stub_dir.join(basename),
        ];
        let found = candidates.iter().find(|p| p.exists());
        let Some(path) = found else {
            // A MySQL include mydb does not vendor (e.g. have_debug.inc) — skip
            // the whole file rather than mis-run it.
            self.skip = Some(format!("missing include `{rel}` (M1 does not vendor it)"));
            return i + 1;
        };
        match read_test_source(path) {
            Ok(text) => {
                let inc_lines: Vec<String> = text.lines().map(|l| l.to_string()).collect();
                self.exec_range(&inc_lines, 0, inc_lines.len());
            }
            Err(e) => self.skip = Some(format!("cannot read include `{rel}`: {e}")),
        }
        i + 1
    }

    /// Execute an `if`/`while` block starting at line `i`. Returns the index
    /// after the block's closing `}`.
    fn exec_block(&mut self, lines: &[String], i: usize, end: usize, is_while: bool) -> usize {
        // Extract the condition between the first `(` and the matching `)`. The
        // condition may span lines (e.g. sys_vars' load_sysvars.inc has an
        // `if (`SELECT … \n IN (…)`)` spread over two lines), so scan forward
        // until the parens balance and note the line where it closes.
        let (cond_src, cond_end) = match extract_condition_spanning(lines, i, end) {
            Some(pair) => pair,
            None => {
                self.skip = Some(format!(
                    "malformed if/while condition: `{}`",
                    lines[i].trim()
                ));
                return i + 1;
            }
        };
        // Locate the block body `{ ... }`, starting from the condition's closing
        // line (the brace may sit on the header line, the next line, or after a
        // multi-line condition).
        let Some((body_start, body_end, after)) = find_block(lines, cond_end, end) else {
            self.skip = Some(format!("unbalanced block braces near line {}", i + 1));
            return i + 1;
        };

        if is_while {
            let mut guard = 0u32;
            while self.skip.is_none() && self.eval_condition(&cond_src) {
                self.exec_range(lines, body_start, body_end);
                guard += 1;
                if guard > 100_000 {
                    self.skip = Some("while loop exceeded 100000 iterations".to_string());
                    break;
                }
            }
        } else if self.eval_condition(&cond_src) {
            self.exec_range(lines, body_start, body_end);
        }
        after
    }

    fn eval_condition(&mut self, cond: &str) -> bool {
        // Resolve backticks then variables, then evaluate truthiness.
        let resolved = if cond.contains('`') {
            self.resolve_backticks(cond)
        } else {
            cond.to_string()
        };
        let substituted = interp::substitute_vars(&resolved, &self.vars);
        interp::eval_truthy(&substituted)
    }

    fn resolve_backticks(&mut self, s: &str) -> String {
        let mut out = String::new();
        let mut rest = s;
        while let Some(open) = rest.find('`') {
            out.push_str(&rest[..open]);
            let after = &rest[open + 1..];
            if let Some(close) = after.find('`') {
                let sql = interp::substitute_vars(&after[..close], &self.vars);
                out.push_str(&self.scalar_query(&sql));
                rest = &after[close + 1..];
            } else {
                out.push_str(after);
                return out;
            }
        }
        out.push_str(rest);
        out
    }

    /// Run a query and return the first column of the first row as text.
    fn scalar_query(&mut self, sql: &str) -> String {
        match self.exec_on_current(sql) {
            Ok(QueryResponse::ResultSet(rs)) => rs
                .rows
                .first()
                .and_then(|r| r.first())
                .map(|v| v.to_display_string())
                .unwrap_or_default(),
            _ => String::new(),
        }
    }

    /// `query_get_value(SQL, ColumnName, RowNumber)` — split from the right so
    /// the SQL may contain commas.
    fn query_get_value(&mut self, inner: &str) -> String {
        let parts: Vec<&str> = inner.rsplitn(3, ',').collect();
        if parts.len() != 3 {
            return String::new();
        }
        let row: usize = parts[0].trim().parse().unwrap_or(0);
        let col_name = parts[1].trim();
        let sql = interp::substitute_vars(parts[2].trim(), &self.vars);
        match self.exec_on_current(&sql) {
            Ok(QueryResponse::ResultSet(rs)) => {
                let Some(col_idx) = rs.columns.iter().position(|c| c.name == col_name) else {
                    return String::new();
                };
                rs.rows
                    .get(row.saturating_sub(1))
                    .and_then(|r| r.get(col_idx))
                    .map(|v| v.to_display_string())
                    .unwrap_or_default()
            }
            _ => String::new(),
        }
    }

    /// Accumulate all statements on the logical SQL unit beginning at line
    /// `i`. Delimiters inside quotes/comments are ignored, and a delimiter
    /// before an inline comment terminates the statement immediately.
    fn read_statements(&self, lines: &[String], i: usize, end: usize) -> (Vec<String>, usize) {
        self.read_statements_from(lines, i, end, lines[i].trim())
    }

    /// Same as `read_statements` but the first line's content is `first`
    /// (used by `eval` after stripping the `eval ` prefix).
    fn read_statements_from(
        &self,
        lines: &[String],
        i: usize,
        end: usize,
        first: &str,
    ) -> (Vec<String>, usize) {
        let mut buf = String::new();
        let mut j = i;
        let mut piece = first.to_string();
        loop {
            if !buf.is_empty() {
                buf.push('\n');
            }
            buf.push_str(&piece);
            let (statements, has_unterminated_sql) =
                split_delimited_statements(&buf, &self.delimiter);
            if !statements.is_empty() && !has_unterminated_sql {
                return (statements, j + 1);
            }
            j += 1;
            if j >= end {
                if statements.is_empty() && has_unterminated_sql {
                    return (vec![buf], j);
                }
                return (statements, j);
            }
            piece = lines[j].clone();
        }
    }

    /// Execute one SQL statement (already delimiter-stripped) and render.
    fn run_sql(&mut self, stmt: &str, already_substituted: bool) {
        let _ = already_substituted;
        let exec = stmt.trim();
        if exec.is_empty() {
            self.reset_pending();
            return;
        }

        if self.query_log {
            let echo = format!("{exec}{}", self.delimiter);
            self.out.push_str(&echo);
            self.out.push('\n');
            self.echoed_statements.push(echo);
        }

        // MTR framework helper calls (`mtr.add_suppression`, `mtr.force_restart`,
        // …) target a test-harness schema that mydb has no reason to implement.
        // Upstream just echoes them and produces no result output, so treat them
        // as a no-op here rather than a spurious "PROCEDURE mtr.* does not exist"
        // violation. The statement is still echoed above when query_log is on.
        if is_mtr_helper_call(exec) {
            self.reset_pending();
            return;
        }

        self.p_mods.vertical |= self.vertical_results;
        self.p_mods.metadata |= self.metadata_enabled;
        let mut stmt_out = String::new();
        match self.exec_sql_statement(exec) {
            Ok(resp) => {
                self.p_mods.result_charset = self
                    .sessions
                    .get(&self.current)
                    .and_then(|session| session.character_set_results.clone());
                if !self.p_expect.is_empty() && !success_matches(&self.p_expect) {
                    self.violations.push(format!(
                        "statement expected to fail but succeeded: `{exec}`"
                    ));
                }
                if self.result_log {
                    render::render_response(&resp, &self.p_mods, &mut stmt_out);
                    self.expand_warnings(exec, &mut stmt_out);
                    if self.p_mods.sorted {
                        sort_mysqltest_statement_output(&mut stmt_out);
                    }
                }
            }
            Err(err) => {
                if self.p_expect.is_empty() {
                    if self.abort_on_error {
                        self.violations.push(format!(
                            "statement failed unexpectedly: `{exec}` -> ERROR {} ({}): {}",
                            err.code(),
                            err.sql_state(),
                            err.message()
                        ));
                    }
                } else if !error_matches(&self.p_expect, err.code(), &err.sql_state()) {
                    self.violations.push(format!(
                        "statement `{exec}` expected error {:?} but got {} ({}): {}",
                        self.p_expect,
                        err.code(),
                        err.sql_state(),
                        err.message()
                    ));
                }
                if self.result_log {
                    render::render_error(&err, &mut stmt_out);
                }
            }
        }
        stmt_out = self.apply_output_transforms(stmt_out);
        self.out.push_str(&stmt_out);
        self.reset_pending();
    }

    /// Auto-expand warnings when the `--enable_warnings` state is on.
    fn expand_warnings(&mut self, exec: &str, out: &mut String) {
        if self.warnings_enabled {
            self.warnings_block(exec, out, self.p_mods.vertical);
        }
    }

    /// Emit the `Warnings:` block the way MTR does: a header followed by
    /// `Level<tab>Code<tab>Message` per warning. Skipped for the diagnostics
    /// statements themselves to avoid recursion. Caller gates on the
    /// `--enable_warnings` state (captured at `send` time for `reap`).
    fn warnings_block(&mut self, exec: &str, out: &mut String, vertical: bool) {
        let upper = exec.trim_start().to_ascii_uppercase();
        if upper.starts_with("SHOW WARNINGS")
            || upper.starts_with("SHOW ERRORS")
            || upper.starts_with("SHOW COUNT")
        {
            return;
        }
        let rows = match self.exec_on_current("SHOW WARNINGS") {
            Ok(QueryResponse::ResultSet(rs)) => rs.rows,
            _ => return,
        };
        if rows.is_empty() {
            return;
        }
        out.push_str("Warnings:\n");
        for row in &rows {
            let level = row
                .first()
                .map(|v| v.to_display_string())
                .unwrap_or_default();
            let code = row
                .get(1)
                .map(|v| v.to_display_string())
                .unwrap_or_default();
            let msg = row
                .get(2)
                .map(|v| v.to_display_string())
                .unwrap_or_default();
            if vertical {
                out.push_str(&format!("Level\t{level}\nCode\t{code}\nMessage\t{msg}\n"));
            } else {
                out.push_str(&format!("{level}\t{code}\t{msg}\n"));
            }
        }
    }

    fn apply_output_transforms(&self, mut text: String) -> String {
        for (from, to) in &self.p_replace_result {
            text = text.replace(from, to);
        }
        for (re, repl) in &self.p_replace_regex {
            text = re.replace_all(&text, repl.as_str()).into_owned();
        }
        text
    }

    fn reset_pending(&mut self) {
        self.p_expect.clear();
        self.p_mods = RenderMods::default();
        self.p_replace_regex.clear();
        self.p_replace_result.clear();
    }
}

/// mysqltest's one-shot `--sorted_result` modifier sorts the complete query
/// payload following the result header, including an automatically appended
/// warning block.  Sorting only `ResultSet::rows` leaves warning lines in a
/// different position from upstream and creates false semantic diffs.
fn sort_mysqltest_statement_output(out: &mut String) {
    if out.starts_with("Catalog\tDatabase\t") {
        // Metadata mode has a multi-row structural preamble; none of the
        // imported sorted-result cases combine these modifiers.
        return;
    }
    let mut lines = out.lines();
    let Some(header) = lines.next() else {
        return;
    };
    let mut tail = lines.collect::<Vec<_>>();
    if tail.len() < 2 {
        return;
    }
    tail.sort_unstable();
    let mut sorted = String::with_capacity(out.len());
    sorted.push_str(header);
    sorted.push('\n');
    for line in tail {
        sorted.push_str(line);
        sorted.push('\n');
    }
    *out = sorted;
}

fn new_mtr_session(engine: &StorageQueryEngine) -> SessionContext {
    // SessionContext::default() uses connection id 1, which lets named locks
    // leak between otherwise isolated MTR files through the process-global
    // user-lock registry. Give every harness session a stable unique identity
    // so RELEASE_ALL_LOCKS and CONNECTION_ID observe per-session state.
    static NEXT_MTR_CONNECTION_ID: std::sync::atomic::AtomicU32 =
        std::sync::atomic::AtomicU32::new(10_000);
    let mut session = SessionContext::default();
    session.connection_id =
        NEXT_MTR_CONNECTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // Upstream mysql-test starts with MySQL 8's complete default sql_mode;
    // its turn_off/restore include files use `SET sql_mode = DEFAULT` and
    // therefore require both the GLOBAL source and the new SESSION value.
    // Keep this harness-only so ordinary product sessions retain the configured
    // default.
    const MYSQL80_DEFAULT_SQL_MODE: &str = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,\
NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
    let _ = engine.execute_with_session(
        &mut session,
        &format!("SET GLOBAL sql_mode='{MYSQL80_DEFAULT_SQL_MODE}'"),
    );
    let _ = engine.execute_with_session(
        &mut session,
        &format!("SET SESSION sql_mode='{MYSQL80_DEFAULT_SQL_MODE}'"),
    );
    // The upstream main-suite oracle is recorded with +03:00 as its global
    // default.  Pin both scopes so `SET time_zone=@@global.time_zone` is
    // deterministic and independent of the developer machine's SYSTEM zone.
    let _ = engine.execute_with_session(&mut session, "SET GLOBAL time_zone='+03:00'");
    let _ = engine.execute_with_session(&mut session, "SET SESSION time_zone='+03:00'");
    session
}

/// Resolve mysql-test `../../std_data/<fixture>` string literals for the SQL
/// sent to the engine. This covers both `LOAD DATA` and `LOAD_FILE()` while
/// `run_sql` continues to echo the original source, keeping golden output
/// independent of the local checkout path.
///
/// Components outside the fixture directory are rejected lexically, and an
/// existing target (or parent) is canonicalized to prevent symlink escapes.
fn resolve_std_data_fixture_paths(sql: &str, test_root: &Path) -> Result<String, QueryError> {
    const PREFIX: &str = "../../std_data/";
    let fixture_root = test_root.join("std_data");
    let canonical_root =
        std::fs::canonicalize(&fixture_root).unwrap_or_else(|_| fixture_root.clone());
    let mut rewritten = sql.to_string();
    let mut search_from = 0usize;
    while let Some(found) = rewritten[search_from..].find(PREFIX) {
        let path_start = search_from + found;
        if path_start == 0 || rewritten.as_bytes()[path_start - 1] != b'\'' {
            search_from = path_start + PREFIX.len();
            continue;
        }
        let suffix_start = path_start + PREFIX.len();
        let Some(relative_end) = rewritten[suffix_start..].find('\'') else {
            break;
        };
        let path_end = suffix_start + relative_end;
        let relative = Path::new(&rewritten[suffix_start..path_end]);
        if relative.as_os_str().is_empty()
            || relative
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err(crate::error::catalog::ER_TEXTFILE_NOT_READABLE
                .error("MTR std_data fixture path escapes the fixture directory"));
        }

        let target = fixture_root.join(relative);
        let checked_target = std::fs::canonicalize(&target).unwrap_or_else(|_| {
            target
                .parent()
                .and_then(|parent| std::fs::canonicalize(parent).ok())
                .and_then(|parent| target.file_name().map(|name| parent.join(name)))
                .unwrap_or_else(|| target.clone())
        });
        if !checked_target.starts_with(&canonical_root) {
            return Err(crate::error::catalog::ER_TEXTFILE_NOT_READABLE
                .error("MTR std_data fixture path escapes the fixture directory"));
        }

        let replacement = target.to_string_lossy();
        if replacement.contains('\'') {
            return Err(crate::error::catalog::ER_TEXTFILE_NOT_READABLE
                .error("MTR std_data fixture path cannot be represented as a SQL string"));
        }
        rewritten.replace_range(path_start..path_end, replacement.as_ref());
        search_from = path_start + replacement.len();
    }
    Ok(rewritten)
}

/// Cosmetic / protocol-only mysqltest directives that do not affect the
/// in-process golden and are accepted as no-ops (so a file runs rather than
/// skipping). Some (e.g. `replace_numeric_round`, `sorted_result` variants)
/// could in principle change output; accepting them yields a DIFF for triage,
/// which is more useful than a SKIP.
const IGNORED_DIRECTIVES: &[&str] = &[
    "disable_info",
    "enable_info",
    "disable_session_track_info",
    "enable_session_track_info",
    "disable_connect_log",
    "enable_connect_log",
    "skip_if_hypergraph",
    "replace_numeric_round",
    // `character_set` changes how mysqltest decodes subsequent source bytes.
    // The vendored corpus is already decoded as UTF-8 (and uses hex literals
    // for non-UTF-8 bytes), so it has no additional in-process effect.
    "character_set",
];

fn should_collect_multiline_let(line: &str, arg: &str, delimiter: &str) -> bool {
    !line.trim_start().starts_with("--")
        && !line_has_complete_delimited_unit(line.trim(), delimiter)
        && !arg.trim_end().ends_with('`')
}

fn resolve_remove_file_target(vardir: &Path, requested: &Path) -> Result<PathBuf, String> {
    let target = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        vardir.join(requested)
    };
    let vardir = vardir
        .canonicalize()
        .map_err(|e| format!("remove_file cannot resolve test temporary directory: {e}"))?;
    let parent = target
        .parent()
        .ok_or_else(|| format!("remove_file has no parent directory: {}", target.display()))?;
    let parent = parent.canonicalize().map_err(|e| {
        format!(
            "remove_file cannot resolve parent {}: {e}",
            parent.display()
        )
    })?;
    if !parent.starts_with(&vardir) {
        return Err(format!(
            "remove_file path escapes $MYSQLTEST_VARDIR: {}",
            target.display()
        ));
    }
    Ok(parent.join(
        target
            .file_name()
            .expect("remove_file target with a parent has a file name"),
    ))
}

/// True for MTR framework helper calls (`CALL mtr.<helper>(...)`) that target
/// the test harness's own `mtr` schema, not the SUT — e.g. `mtr.add_suppression`.
fn is_mtr_helper_call(stmt: &str) -> bool {
    let s = stmt.trim_start().to_ascii_lowercase();
    let rest = match s.strip_prefix("call ") {
        Some(r) => r.trim_start(),
        None => return false,
    };
    rest.starts_with("mtr.")
}

fn error_matches(expect: &[Expected], code: u16, sql_state: &str) -> bool {
    expect.iter().any(|e| match e {
        Expected::Errno(n) => *n == code,
        Expected::SqlState(s) => s == sql_state,
    })
}

fn success_matches(expect: &[Expected]) -> bool {
    expect
        .iter()
        .any(|e| matches!(e, Expected::Errno(code) if *code == 0))
}

fn is_ps_eligible(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let head = trimmed
        .split(|c: char| c.is_ascii_whitespace())
        .next()
        .unwrap_or_default();
    let upper = trimmed.to_ascii_uppercase();
    !head.eq_ignore_ascii_case("PREPARE")
        && !head.eq_ignore_ascii_case("EXECUTE")
        && !head.eq_ignore_ascii_case("DEALLOCATE")
        // PREPARE itself replaces the session condition area. Wrapping a
        // diagnostics reader would therefore erase the very error/warnings
        // that SHOW is meant to inspect before EXECUTE can read them.
        && !upper.starts_with("SHOW WARNINGS")
        && !upper.starts_with("SHOW ERRORS")
        && !upper.starts_with("SHOW COUNT")
}

fn escape_mysql_string(sql: &str) -> String {
    sql.replace('\'', "''")
}

/// Parse the SQL payload of a dashed, line-scoped mysqltest command. A
/// trailing delimiter is optional; delimiters inside strings/comments retain
/// their lexical meaning. Dashed commands never consume a subsequent line.
fn directive_line_statements(input: &str, delimiter: &str) -> Vec<String> {
    let (statements, has_tail) = split_delimited_statements(input, delimiter);
    if has_tail {
        // The common (and upstream-defined) dashed form carries exactly one
        // SQL statement without a terminator. Keeping it intact also avoids
        // guessing how a deliberately embedded delimiter should be handled.
        vec![input.trim().to_string()]
    } else {
        statements
    }
}

fn line_has_complete_delimited_unit(input: &str, delimiter: &str) -> bool {
    let (statements, has_tail) = split_delimited_statements(input, delimiter);
    !statements.is_empty() && !has_tail
}

/// Split SQL at delimiters that occur outside quoted strings, identifiers, and
/// comments. The boolean reports whether non-comment SQL remains after the
/// last complete statement.
fn split_delimited_statements(input: &str, delimiter: &str) -> (Vec<String>, bool) {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum LexState {
        Normal,
        Single,
        Double,
        Backtick,
        LineComment,
        BlockComment,
    }

    if delimiter.is_empty() {
        return (Vec::new(), contains_sql_outside_comments(input));
    }

    let bytes = input.as_bytes();
    let delim = delimiter.as_bytes();
    let mut state = LexState::Normal;
    let mut escaped = false;
    let mut start = 0usize;
    let mut i = 0usize;
    let mut statements = Vec::new();

    while i < bytes.len() {
        match state {
            LexState::Normal => {
                if bytes[i..].starts_with(delim) {
                    let statement = input[start..i].trim();
                    if !statement.is_empty() && contains_sql_outside_comments(statement) {
                        statements.push(statement.to_string());
                    }
                    i += delim.len();
                    start = i;
                    continue;
                }
                match bytes[i] {
                    b'\'' => state = LexState::Single,
                    b'"' => state = LexState::Double,
                    b'`' => state = LexState::Backtick,
                    b'#' => state = LexState::LineComment,
                    b'-' if i + 1 < bytes.len()
                        && bytes[i + 1] == b'-'
                        && (i + 2 == bytes.len() || bytes[i + 2].is_ascii_whitespace()) =>
                    {
                        state = LexState::LineComment;
                        i += 1;
                    }
                    b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                        state = LexState::BlockComment;
                        i += 1;
                    }
                    _ => {}
                }
            }
            LexState::Single | LexState::Double | LexState::Backtick => {
                if escaped {
                    escaped = false;
                } else if bytes[i] == b'\\' {
                    escaped = true;
                } else {
                    let quote = match state {
                        LexState::Single => b'\'',
                        LexState::Double => b'"',
                        LexState::Backtick => b'`',
                        _ => unreachable!(),
                    };
                    if bytes[i] == quote {
                        if i + 1 < bytes.len() && bytes[i + 1] == quote {
                            i += 1;
                        } else {
                            state = LexState::Normal;
                        }
                    }
                }
            }
            LexState::LineComment => {
                if bytes[i] == b'\n' {
                    state = LexState::Normal;
                }
            }
            LexState::BlockComment => {
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    state = LexState::Normal;
                    i += 1;
                }
            }
        }
        i += 1;
    }

    (statements, contains_sql_outside_comments(&input[start..]))
}

fn contains_sql_outside_comments(input: &str) -> bool {
    let bytes = input.as_bytes();
    let mut i = 0usize;
    let mut in_block = false;
    let mut in_line = false;
    while i < bytes.len() {
        if in_line {
            if bytes[i] == b'\n' {
                in_line = false;
            }
            i += 1;
            continue;
        }
        if in_block {
            if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                in_block = false;
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i] == b'#' {
            in_line = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'-'
            && i + 1 < bytes.len()
            && bytes[i + 1] == b'-'
            && (i + 2 == bytes.len() || bytes[i + 2].is_ascii_whitespace())
        {
            in_line = true;
            i += 2;
            continue;
        }
        if bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            in_block = true;
            i += 2;
            continue;
        }
        return true;
    }
    false
}

#[cfg(test)]
mod statement_reader_tests {
    use std::path::Path;

    use super::{
        classify_directive, directive_line_statements, escape_mysql_string,
        is_known_directive_command, is_ps_eligible, line_has_complete_delimited_unit,
        resolve_remove_file_target, should_collect_multiline_let, split_delimited_statements,
        success_matches, Expected,
    };

    #[test]
    fn error_zero_accepts_success() {
        assert!(success_matches(&[
            Expected::Errno(0),
            Expected::Errno(1231)
        ]));
        assert!(!success_matches(&[Expected::Errno(1231)]));
    }

    #[test]
    fn delimiter_before_inline_comment_terminates_statement() {
        let (statements, tail) =
            split_delimited_statements("INSERT INTO t VALUES (1); # explanation", ";");
        assert_eq!(statements, ["INSERT INTO t VALUES (1)"]);
        assert!(!tail);
    }

    #[test]
    fn ignores_delimiters_in_quotes_and_comments() {
        let sql = "SELECT ';', \"x;y\", `a;b` /* ; */;\n-- ;\nSELECT 2;";
        let (statements, tail) = split_delimited_statements(sql, ";");
        assert_eq!(
            statements,
            ["SELECT ';', \"x;y\", `a;b` /* ; */", "-- ;\nSELECT 2"]
        );
        assert!(!tail);
    }

    #[test]
    fn supports_custom_delimiter_and_multiple_statements() {
        let (statements, tail) = split_delimited_statements(
            "CREATE PROCEDURE p() BEGIN SELECT 1; END$$ SELECT 2$$",
            "$$",
        );
        assert_eq!(
            statements,
            ["CREATE PROCEDURE p() BEGIN SELECT 1; END", "SELECT 2"]
        );
        assert!(!tail);
    }

    #[test]
    fn delimiter_command_does_not_inherit_pending_error() {
        let root = tempfile::tempdir().expect("test root");
        let data = tempfile::tempdir().expect("engine data");
        let engine = std::sync::Arc::new(super::open_test_engine(data.path()));
        let mut runner = super::Runner::new(engine, root.path().to_path_buf(), data.path());
        let lines = vec![
            "--error ER_NO_REFERENCED_ROW_2".to_string(),
            "delimiter //;".to_string(),
            "CREATE FUNCTION f() RETURNS INTEGER RETURN 1//".to_string(),
            "delimiter ;//".to_string(),
            "DROP FUNCTION f;".to_string(),
        ];

        runner.exec_range(&lines, 0, lines.len());

        assert!(
            runner.violations.is_empty(),
            "delimiter leaked --error to routine definition: {:?}",
            runner.violations
        );
        assert!(runner.p_expect.is_empty());
    }

    #[test]
    fn reports_unterminated_sql_tail() {
        let (statements, tail) = split_delimited_statements("SELECT 1; SELECT", ";");
        assert_eq!(statements, ["SELECT 1"]);
        assert!(tail);
    }

    #[test]
    fn dashed_eval_and_send_payloads_are_line_scoped_without_delimiter() {
        assert_eq!(
            directive_line_statements("EXPLAIN $query", ";"),
            ["EXPLAIN $query"]
        );
        assert_eq!(
            directive_line_statements("SELECT ';' AS semi;", ";"),
            ["SELECT ';' AS semi"]
        );
    }

    #[test]
    fn spaced_error_is_a_mysqltest_directive_not_a_sql_comment() {
        let (cmd, arg) = classify_directive("-- error 1264").expect("directive");
        assert_eq!(cmd, "error");
        assert_eq!(arg, "1264");
        assert!(is_known_directive_command(&cmd));
        assert!(!is_known_directive_command("this_is_a_sql_comment"));
    }

    #[test]
    fn multiline_let_uses_active_delimiter_not_function_body_semicolon() {
        let directive = "let $f =\nCREATE FUNCTION f() RETURNS INT\nBEGIN\nRETURN 1;\nEND//";
        assert!(line_has_complete_delimited_unit(directive, "//"));
        assert!(!line_has_complete_delimited_unit(
            "let $f =\nCREATE FUNCTION f() RETURNS INT\nBEGIN\nRETURN 1;",
            "//"
        ));

        let value = "$f =\nCREATE FUNCTION f() RETURNS INT\nBEGIN\nRETURN 1;\nEND//";
        let (statements, tail) = split_delimited_statements(value, "//");
        assert!(!tail);
        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("RETURN 1;"));
        assert!(statements[0].ends_with("END"));
    }

    #[test]
    fn dashed_let_is_line_scoped_but_bare_let_can_span_lines() {
        assert!(!should_collect_multiline_let(
            "--let $file=$MYSQLTEST_VARDIR/tmp/outfile1",
            "$file=$MYSQLTEST_VARDIR/tmp/outfile1",
            ";"
        ));
        assert!(should_collect_multiline_let(
            "let $routine=",
            "$routine=",
            "//"
        ));
    }

    #[test]
    fn remove_file_is_recognized_and_confined_to_vardir() {
        for line in [
            "--remove_file $MYSQLTEST_VARDIR/tmp/outfile1",
            "remove_file $MYSQLTEST_VARDIR/tmp/outfile1;",
        ] {
            let (cmd, _) = classify_directive(line).expect("remove_file directive");
            assert_eq!(cmd, "remove_file");
            assert!(is_known_directive_command(&cmd));
        }

        let vardir = tempfile::tempdir().expect("temporary vardir");
        std::fs::create_dir(vardir.path().join("tmp")).expect("temporary tmp directory");
        assert_eq!(
            resolve_remove_file_target(vardir.path(), Path::new("tmp/outfile1"))
                .expect("path inside vardir"),
            vardir
                .path()
                .canonicalize()
                .expect("canonical vardir")
                .join("tmp/outfile1")
        );
        assert!(resolve_remove_file_target(vardir.path(), Path::new("../outside")).is_err());
    }

    #[test]
    fn prepared_protocol_wrapper_escapes_source_and_skips_control_statements() {
        assert_eq!(escape_mysql_string("SELECT 'x'"), "SELECT ''x''");
        assert!(is_ps_eligible(" SELECT 1"));
        assert!(!is_ps_eligible("PREPARE s FROM @sql"));
        assert!(!is_ps_eligible("execute s"));
        assert!(!is_ps_eligible(" SHOW WARNINGS"));
        assert!(!is_ps_eligible("show errors limit 1"));
        assert!(!is_ps_eligible("SHOW COUNT(*) WARNINGS"));
    }
}

/// Classify a directive line into `(command, argument)`. Returns `None` for a
/// SQL line. A line starting with `--` is always a directive; a bare leading
/// keyword is a directive only for the control words we recognise.
fn classify_directive(trimmed: &str) -> Option<(String, String)> {
    let body = trimmed.strip_prefix("--").map(str::trim);
    let (is_dashed, body) = match body {
        Some(b) => (true, b),
        None => (false, trimmed),
    };
    // Read the leading command word ([a-z_]+).
    let cmd_end = body
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(body.len());
    let cmd = body[..cmd_end].to_ascii_lowercase();
    let mut arg = body[cmd_end..].trim().to_string();

    const BARE_KEYWORDS: &[&str] = &[
        "let",
        "if",
        "while",
        "echo",
        "inc",
        "dec",
        "delimiter",
        "source",
        "eval",
        "query_vertical",
        "query_horizontal",
        "exit",
        "connect",
        "connection",
        "disconnect",
        "send",
        "reap",
        "remove_file",
    ];
    if is_dashed || BARE_KEYWORDS.contains(&cmd.as_str()) {
        // Bare (non-`--`) mysqltest commands are `;`-terminated; `eval`/`send`
        // carry a SQL statement and keep their own delimiter handling, and
        // `echo` prints its text verbatim.
        if !is_dashed && cmd != "eval" && cmd != "echo" && cmd != "send" {
            if let Some(stripped) = arg.strip_suffix(';') {
                arg = stripped.trim_end().to_string();
            }
        }
        Some((cmd, arg))
    } else {
        None
    }
}

fn is_known_directive_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "echo"
            | "disable_query_log"
            | "enable_query_log"
            | "disable_result_log"
            | "enable_result_log"
            | "disable_warnings"
            | "enable_warnings"
            | "enable_metadata"
            | "disable_metadata"
            | "sorted_result"
            | "partially_sorted_result"
            | "replace_column"
            | "replace_result"
            | "replace_regex"
            | "error"
            | "let"
            | "inc"
            | "dec"
            | "eval"
            | "query_vertical"
            | "query_horizontal"
            | "delimiter"
            | "source"
            | "remove_file"
            | "connect"
            | "connection"
            | "disconnect"
            | "reap"
            | "send"
            | "disable_ps_protocol"
            | "enable_ps_protocol"
            | "vertical_results"
            | "horizontal_results"
            | "exit"
            | "if"
            | "while"
            | "disable_testcase"
            | "enable_testcase"
            | "disable_abort_on_error"
            | "enable_abort_on_error"
    ) || IGNORED_DIRECTIVES.contains(&cmd)
}

/// Extract the condition between the first `(` and its matching `)` on the
/// `if`/`while` line.
/// Extract an `if`/`while` condition that may span multiple lines, returning the
/// inner text (with newlines collapsed to spaces) and the index of the line on
/// which the parentheses balanced. Backticked SQL can legitimately contain
/// `(`/`)`, but those are matched by the same depth counter, and MTR conditions
/// don't nest an unbalanced paren inside backticks, so plain depth counting is
/// sufficient here.
fn extract_condition_spanning(lines: &[String], i: usize, end: usize) -> Option<(String, usize)> {
    let open_rel = lines[i].find('(')?;
    let mut depth = 0i32;
    let mut parts: Vec<&str> = Vec::new();
    for k in i..end {
        let line = &lines[k];
        let start = if k == i { open_rel } else { 0 };
        let seg_from = if k == i { open_rel + 1 } else { 0 };
        for (idx, &b) in line.as_bytes().iter().enumerate().skip(start) {
            match b {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        parts.push(&line[seg_from..idx]);
                        return Some((parts.join(" "), k));
                    }
                }
                _ => {}
            }
        }
        parts.push(&line[seg_from..]);
    }
    None
}

/// Find the `{ ... }` block for an `if`/`while` starting at `open_idx`. The
/// opening `{` must sit at the end of an `if`/`while` line; the closing `}` is
/// a line whose trimmed content is exactly `}`. Nested `if`/`while` blocks
/// nest by depth. Returns `(body_start, body_end_exclusive, after_index)`.
fn find_block(lines: &[String], open_idx: usize, end: usize) -> Option<(usize, usize, usize)> {
    // Find the line carrying the opening brace (open_idx line or later).
    let mut brace_line = None;
    for k in open_idx..end {
        if lines[k].trim_end().ends_with('{') {
            brace_line = Some(k);
            break;
        }
        // A non-empty, non-comment line before `{` that is not the if/while
        // header means the block style is unsupported.
        if k > open_idx {
            let t = lines[k].trim();
            if !t.is_empty() && !t.starts_with('#') {
                break;
            }
        }
    }
    let brace_line = brace_line?;
    let body_start = brace_line + 1;
    // Depth counting: any line ending in `{` opens a nested block (a `while`/
    // `if` header with a same-line brace, or a lone `{`); a line that is
    // exactly `}` closes one. Statements do not end in `{`.
    let mut depth = 1i32;
    let mut k = body_start;
    while k < end {
        let t = lines[k].trim();
        if t == "}" {
            depth -= 1;
            if depth == 0 {
                return Some((body_start, k, k + 1));
            }
        } else if lines[k].trim_end().ends_with('{') {
            depth += 1;
        }
        k += 1;
    }
    None
}

/// Bounded-lookahead line diff for failure reporting. Unlike a positional
/// comparison, one inserted warning does not make every following row appear
/// different. The semantic equality decision is made separately; this helper
/// only produces an actionable, aligned diagnostic.
fn diff(expected: &str, actual: &str) -> String {
    let exp: Vec<&str> = expected.lines().collect();
    let act: Vec<&str> = actual.lines().collect();
    let mut lines = vec!["golden(-) vs actual(+):".to_string()];
    let mut shown = 0usize;
    let (mut i, mut j) = (0usize, 0usize);
    const LOOKAHEAD: usize = 8;
    while i < exp.len() || j < act.len() {
        if exp.get(i) == act.get(j) {
            i += 1;
            j += 1;
            continue;
        }
        let exp_limit = (i + LOOKAHEAD).min(exp.len());
        let act_limit = (j + LOOKAHEAD).min(act.len());
        let mut anchor: Option<(usize, usize)> = None;
        for ei in i..exp_limit {
            for aj in j..act_limit {
                if ei == i && aj == j {
                    continue;
                }
                if exp[ei] == act[aj]
                    && anchor.is_none_or(|(best_e, best_a)| {
                        (ei - i) + (aj - j) < (best_e - i) + (best_a - j)
                    })
                {
                    anchor = Some((ei, aj));
                }
            }
        }
        let (next_i, next_j) = anchor.unwrap_or_else(|| {
            (
                (i + usize::from(i < exp.len())).min(exp.len()),
                (j + usize::from(j < act.len())).min(act.len()),
            )
        });
        for (idx, value) in exp[i..next_i].iter().enumerate() {
            lines.push(format!("  -[{}] {}", i + idx, value));
            shown += 1;
        }
        for (idx, value) in act[j..next_j].iter().enumerate() {
            lines.push(format!("  +[{}] {}", j + idx, value));
            shown += 1;
        }
        i = next_i;
        j = next_j;
        if shown >= 40 {
            lines.push("  ... (diff truncated)".to_string());
            break;
        }
    }
    lines.join("\n")
}

#[test]
fn triage_normalization_preserves_semantics_and_ignores_only_display_surfaces() {
    let echoes = vec!["SELECT value\n  FROM t;".to_string()];
    let left = "SELECT value\nFROM t ;\nvalue\n1\nWarnings:\nWarning\t1292\tleft text\n";
    let right = "SELECT value\n  FROM t;\nrenamed label\n1\nWarnings:\nWarning\t1292\tright text\n";
    let left = normalize_triage_output(left, &echoes, &[]);
    let right = normalize_triage_output(right, &echoes, &[]);
    assert_eq!(left.text, right.text);
    assert!(left.ignored_kinds.contains("statement_echo"));
    assert!(right.ignored_kinds.contains("column_label"));
    assert!(right.ignored_kinds.contains("warning_text"));

    let changed_value = normalize_triage_output("SELECT value\nFROM t;\nvalue\n2\n", &echoes, &[]);
    assert_ne!(left.text, changed_value.text);
    let changed_warning = normalize_triage_output(
        "SELECT value\nFROM t;\nvalue\n1\nWarnings:\nWarning\t1366\tx\n",
        &echoes,
        &[],
    );
    assert_ne!(left.text, changed_warning.text);
}

#[test]
fn triage_unordered_rows_ignore_only_top_level_order_by() {
    assert!(matches!(
        unordered_result_mode("SELECT grp, GROUP_CONCAT(c ORDER BY c) FROM t GROUP BY grp;"),
        Some(UnorderedResultMode::SortRows)
    ));
    assert!(unordered_result_mode(
        "SELECT grp, GROUP_CONCAT(c ORDER BY c) FROM t GROUP BY grp ORDER BY grp;"
    )
    .is_none());
    assert!(matches!(
        unordered_result_mode("SELECT * FROM (SELECT * FROM t ORDER BY a) AS d;"),
        Some(UnorderedResultMode::SortRows)
    ));
}

#[test]
fn triage_normalization_treats_indented_multiline_scalar_label_as_one_header() {
    let echoes =
        vec!["select concat((select 'a'),\n  (select 'b')) from (select 1) t;".to_string()];
    let left = "select concat((select 'a'),\n(select 'b')) from (select 1) t;\n\
                concat((select 'a'),\n(select 'b'))\nab\n";
    let right = "select concat((select 'a'),\n  (select 'b')) from (select 1) t;\n\
                 concat((select 'a'),\n  (select 'b'))\nab\n";
    let left = normalize_triage_output(left, &echoes, &[]);
    let right = normalize_triage_output(right, &echoes, &[]);
    assert_eq!(left.text, right.text);
    assert!(left.ignored_kinds.contains("column_label"));
}

#[test]
fn triage_normalization_does_not_mistake_select_column_label_for_next_echo() {
    let statement =
        "SELECT ( SELECT ( b <> 1 ) FROM t2) <> ALL(SELECT 1 FROM t1) FROM t1;".to_string();
    let echoes = vec![statement.clone(), statement];
    let upstream = "SELECT ( SELECT ( b <> 1 ) FROM t2) <> ALL(SELECT 1 FROM t1) FROM t1;\n\
                    ( SELECT ( b <> 1 ) FROM t2) <> ALL(SELECT 1 FROM t1)\n\
                    1\n\
                    SELECT ( SELECT ( b <> 1 ) FROM t2) <> ALL(SELECT 1 FROM t1) FROM t1;\n\
                    ( SELECT ( b <> 1 ) FROM t2) <> ALL(SELECT 1 FROM t1)\n\
                    1\n";
    let local = "SELECT ( SELECT ( b <> 1 ) FROM t2) <> ALL(SELECT 1 FROM t1) FROM t1;\n\
                 SELECT ( b <> 1 )\n\
                 1\n\
                 SELECT ( SELECT ( b <> 1 ) FROM t2) <> ALL(SELECT 1 FROM t1) FROM t1;\n\
                 SELECT ( b <> 1 )\n\
                 1\n";
    let upstream = normalize_triage_output(upstream, &echoes, &[]);
    let local = normalize_triage_output(local, &echoes, &[]);
    assert_eq!(upstream.text, local.text);
    assert_eq!(upstream.matched_echoes, 2);
    assert_eq!(local.matched_echoes, 2);
    assert!(local.ignored_kinds.contains("column_label"));
}

#[test]
fn triage_normalization_does_not_fuzzy_match_header_as_multiline_next_echo() {
    let echoes = vec![
        "SELECT (SELECT COUNT(DISTINCT 12)) FROM t GROUP BY a;".to_string(),
        "SELECT AVG(2), COUNT(*),\n  GROUP_CONCAT(2);".to_string(),
    ];
    let upstream = "SELECT (SELECT COUNT(DISTINCT 12)) FROM t GROUP BY a;\n\
                    (SELECT COUNT(DISTINCT 12))\n\
                    1\n\
                    1\n\
                    SELECT AVG(2), COUNT(*),\n\
                    GROUP_CONCAT(2);\n\
                    AVG(2)\tCOUNT(*)\tGROUP_CONCAT(2)\n\
                    2.0000\t1\t2\n";
    let local = "SELECT (SELECT COUNT(DISTINCT 12)) FROM t GROUP BY a;\n\
                 SELECT COUNT(DISTINCT 12)\n\
                 1\n\
                 1\n\
                 SELECT AVG(2), COUNT(*),\n\
                   GROUP_CONCAT(2);\n\
                 AVG(2)\tCOUNT(*)\tGROUP_CONCAT(2)\n\
                 2.0000\t1\t2\n";
    let upstream = normalize_triage_output(upstream, &echoes, &[]);
    let local = normalize_triage_output(local, &echoes, &[]);
    assert_eq!(upstream.text, local.text);
    assert_eq!(local.matched_echoes, 2);
    assert_eq!(local.segments[0].text, "@RESULT_HEADER\n1\n1");
}

#[test]
fn triage_normalization_preserves_row_after_truncated_scalar_subquery_header() {
    let query = "SELECT (\n\
                   SELECT MIN(inner_t.value)\n\
                   FROM inner_t\n\
                   WHERE inner_t.id = outer_t.id\n\
                 )\n\
                 FROM outer_t;";
    let echoes = vec![
        query.to_string(),
        "DROP TABLE inner_t, outer_t;".to_string(),
    ];
    let upstream = "SELECT (\n\
                    SELECT MIN(inner_t.value)\n\
                    FROM inner_t\n\
                    WHERE inner_t.id = outer_t.id\n\
                    )\n\
                    FROM outer_t;\n\
                    (\n\
                    SELECT MIN(inner_t.value)\n\
                    FROM inner_t\n\
                    WHERE inner_t.i\n\
                    NULL\n\
                    DROP TABLE inner_t, outer_t;\n";
    let local = "SELECT (\n\
                 SELECT MIN(inner_t.value)\n\
                 FROM inner_t\n\
                 WHERE inner_t.id = outer_t.id\n\
                 )\n\
                 FROM outer_t;\n\
                 SELECT MIN(inner_t.value)\n\
                 NULL\n\
                 DROP TABLE inner_t, outer_t;\n";
    let upstream = normalize_triage_output(upstream, &echoes, &[]);
    let local = normalize_triage_output(local, &echoes, &[]);
    assert_eq!(upstream.text, local.text);
    assert_eq!(upstream.matched_echoes, 2);
    assert_eq!(upstream.segments[0].text, "@RESULT_HEADER\nNULL");
}

#[test]
fn triage_normalization_preserves_empty_string_result_row() {
    let echoes = vec![
        "SELECT TRIM(TRAILING 'foo' FROM 'foo');".to_string(),
        "SELECT 1;".to_string(),
    ];
    let output = "SELECT TRIM(TRAILING 'foo' FROM 'foo');\n\
                  TRIM(TRAILING 'foo' FROM 'foo')\n\
                  \n\
                  SELECT 1;\n\
                  1\n\
                  1\n";
    let normalized = normalize_triage_output(output, &echoes, &[]);
    assert_eq!(normalized.matched_echoes, 2);
    assert_eq!(normalized.segments[0].text, "@RESULT_HEADER\n");
    assert_eq!(normalized.segments[1].text, "@RESULT_HEADER\n1");
}

#[test]
fn triage_normalization_recognizes_multiline_echo_after_suppressed_result() {
    let echoes = vec![
        "SHOW FUNCTION STATUS;".to_string(),
        "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE\n\
           FROM INFORMATION_SCHEMA.ROUTINES\n\
           WHERE ROUTINE_TYPE = 'FUNCTION'\n\
           ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME;"
            .to_string(),
        "SET optimizer_switch = DEFAULT;".to_string(),
    ];
    // SHOW's result is intentionally absent (as with mysqltest's
    // --disable_result_log). The following unterminated physical line is a
    // multiline statement echo, not SHOW's column header.
    let upstream = "SHOW FUNCTION STATUS;\n\
                    SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE\n\
                    FROM INFORMATION_SCHEMA.ROUTINES\n\
                    WHERE ROUTINE_TYPE = 'FUNCTION'\n\
                    ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME;\n\
                    SET optimizer_switch = DEFAULT;\n";
    let local = "SHOW FUNCTION STATUS;\n\
                 SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE\n\
                   FROM INFORMATION_SCHEMA.ROUTINES\n\
                   WHERE ROUTINE_TYPE = 'FUNCTION'\n\
                   ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME;\n\
                 SET optimizer_switch = DEFAULT;\n";
    let upstream = normalize_triage_output(upstream, &echoes, &[]);
    let local = normalize_triage_output(local, &echoes, &[]);
    assert_eq!(upstream.text, local.text);
    assert_eq!(upstream.matched_echoes, 3);
    assert_eq!(local.matched_echoes, 3);
}

#[test]
fn triage_normalization_error_ends_result_header_expectation() {
    let echoes = vec![
        "SELECT bad FROM t;".to_string(),
        "SELECT value FROM t WHERE\n  value = 1;".to_string(),
    ];
    let output = "SELECT bad FROM t;\n\
                  ERROR HY000: failed\n\
                  SELECT value FROM t WHERE\n\
                    value = 1;\n\
                  value\n\
                  1\n";
    let normalized = normalize_triage_output(output, &echoes, &[]);
    assert_eq!(normalized.text, "ERROR HY000\n@RESULT_HEADER\n1");
    assert_eq!(normalized.matched_echoes, 2);
}

#[test]
fn triage_normalization_truncated_header_stops_at_next_statement() {
    let echoes = vec![
        "SELECT a,\n  IF(a, IF(a, 2, 0), 0)\nFROM t;".to_string(),
        "DROP TABLE t;".to_string(),
    ];
    let upstream = "SELECT a,\nIF(a, IF(a, 2, 0), 0)\nFROM t;\n\
                    a\tIF(a, IF(a,\nIF((R\n\
                    DROP TABLE t;\n";
    let local = "SELECT a,\n  IF(a, IF(a, 2, 0), 0)\nFROM t;\n\
                 a\tIF(a, IF(a, 2, 0), 0)\n\
                 DROP TABLE t;\n";
    let upstream = normalize_triage_output(upstream, &echoes, &[]);
    let local = normalize_triage_output(local, &echoes, &[]);
    assert_eq!(upstream.text, local.text);
    assert_eq!(upstream.matched_echoes, 2);
    assert_eq!(local.matched_echoes, 2);
}

#[test]
fn triage_normalization_matches_equivalent_echoes_with_different_line_wrapping() {
    let echoes = vec!["SELECT IF(a,\n  IF(b, 'x', 'y'),\n  IF(c, 'z', 'w'))\nFROM t;".to_string()];
    let upstream = "SELECT IF(a, IF(b, 'x', 'y'),\nIF(c, 'z', 'w')) FROM t\n;\nvalue\nx\n";
    let local = "SELECT IF(a,\nIF(b, 'x', 'y'), IF(c,\n'z', 'w')) FROM t;\nvalue\nx\n";
    let upstream = normalize_triage_output(upstream, &echoes, &[]);
    let local = normalize_triage_output(local, &echoes, &[]);
    assert_eq!(upstream.text, local.text);
    assert_eq!(upstream.matched_echoes, 1);
    assert_eq!(local.matched_echoes, 1);
}

#[test]
fn triage_normalization_uses_statement_order_for_mysqltest_reformatted_echoes() {
    let echoes = vec!["SELECT a AS x\nFROM t;".to_string()];
    let upstream = "SELECT `a` `x`\nFROM `t`;\nx\n1\n";
    let local = "SELECT a AS x\nFROM t;\nx\n1\n";
    let upstream = normalize_triage_output(upstream, &echoes, &[]);
    let local = normalize_triage_output(local, &echoes, &[]);
    assert_eq!(upstream.text, local.text);
    assert_eq!(upstream.matched_echoes, 1);
}

#[test]
fn triage_normalization_matches_statement_echoes_across_source_encodings() {
    let echoes = vec!["INSERT INTO t VALUES ('ä'),('ö');".to_string()];
    let upstream = "INSERT INTO t VALUES ('\u{e0e4}'),('\u{e0f6}');\n";
    let local = "INSERT INTO t VALUES ('ä'),('ö');\n";

    let upstream = normalize_triage_output(upstream, &echoes, &[]);
    let local = normalize_triage_output(local, &echoes, &[]);
    assert_eq!(upstream.text, local.text);
    assert_eq!(upstream.matched_echoes, 1);
    assert_eq!(local.matched_echoes, 1);
}

#[test]
fn triage_fingerprint_is_stable_for_allowed_text_but_changes_for_values() {
    let ignored = vec!["error_text".to_string()];
    let first = triage_diff_fingerprint("main/x.test", 7, "ERROR HY000", "ERROR HY000", &ignored);
    let text_only =
        triage_diff_fingerprint("main/x.test", 7, "ERROR HY000", "ERROR HY000", &ignored);
    let value_change =
        triage_diff_fingerprint("main/x.test", 7, "1", "2", &["column_label".to_string()]);
    let other_statement =
        triage_diff_fingerprint("main/x.test", 8, "ERROR HY000", "ERROR HY000", &ignored);
    assert_eq!(first, text_only);
    assert_ne!(first, value_change);
    assert_ne!(first, other_statement);
}

#[test]
fn triage_diff_aligns_inserted_lines() {
    let report = diff("a\nb\nc\nd", "a\ninserted\nb\nc\nd");
    assert!(report.contains("+[1] inserted"), "{report}");
    assert!(!report.contains("-[2] c"), "{report}");
}
