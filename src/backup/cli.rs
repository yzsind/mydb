//! Offline backup front end. All publication and recovery lives in typed APIs.
use super::repository::{Lease, Publication, Repository};
use super::restore::Target;
use super::*;
pub(crate) fn run(args: Vec<String>) -> Result<(), QueryError> {
    let command = args.first().map(String::as_str).unwrap_or("help");
    if matches!(command, "help" | "--help" | "-h") {
        if args.len() > 1 {
            return Err(backup_err("help accepts no options"));
        }
        println!("mydb-backup init --repository PATH [--wal-encoding NONE|ADAPTIVE_LZ4] [--wal-key-id ID --key-file PATH]\n  WAL encryption does not encrypt the directly queryable native baseline.\nmydb-backup checkpoint --repository PATH [--gc]\nmydb-backup recoverability --repository PATH [--verify] [--key-file PATH]\nmydb-backup keys --repository PATH [--key-file PATH]\nmydb-backup inspect-operation --path PATH --kind KIND [--apply]\nmydb-backup gc --repository PATH [--apply] [--key-file PATH]\nmydb-backup list --repository PATH\nmydb-backup diagnostics --repository PATH\nmydb-backup verify --repository PATH [--base ID] [--recover-check --work-dir PATH [--stream ID]] [--key-file PATH] [--record]\nmydb-backup restore --repository PATH --data-dir PATH [--base ID] [--stream ID] [--target latest | --target-lsn N | --target-time RFC3339] [--key-file PATH] [--writable] [--dry-run] [--resume]\nmydb-backup promote --data-dir PATH\nmydb-backup confirm (--data-dir PATH | --repository PATH [--key-file PATH])\nmydb-backup prune --repository PATH [--stream ID --retain-pitr 7d] [--apply] [--key-file PATH]\nmydb-backup export --repository PATH --output PATH [--retain-pitr 7d] [--base ID] [--stream ID] [--target latest | --target-lsn N | --target-time RFC3339] [--key-file PATH]\nmydb-backup import --repository PATH --input PATH [--key-file PATH] [--resume]");
        return Ok(());
    }
    let allowed: &[&str] = match command {
        "init" => &[
            "--repository",
            "--wal-key-id",
            "--key-file",
            "--wal-encoding",
        ],
        "list" => &["--repository"],
        "checkpoint" => &["--repository", "--gc"],
        "recoverability" => &["--repository", "--key-file", "--verify"],
        "keys" => &["--repository", "--key-file"],
        "diagnostics" => &["--repository"],
        "inspect-operation" => &["--path", "--kind", "--apply"],
        "gc" => &["--repository", "--key-file", "--apply"],
        "verify" => &[
            "--repository",
            "--stream",
            "--base",
            "--key-file",
            "--recover-check",
            "--work-dir",
            "--record",
        ],
        "restore" => &[
            "--repository",
            "--base",
            "--stream",
            "--target",
            "--target-lsn",
            "--target-time",
            "--data-dir",
            "--key-file",
            "--writable",
            "--dry-run",
            "--resume",
        ],
        "promote" => &["--data-dir"],
        "confirm" => &["--data-dir", "--repository", "--key-file"],
        "prune" => &[
            "--repository",
            "--stream",
            "--retain-pitr",
            "--apply",
            "--key-file",
        ],
        "export" => &[
            "--repository",
            "--base",
            "--stream",
            "--target",
            "--target-lsn",
            "--target-time",
            "--output",
            "--key-file",
            "--retain-pitr",
        ],
        "import" => &["--repository", "--input", "--key-file", "--resume"],
        _ => return Err(backup_err("unknown/retired backup command")),
    };
    let mut options = BTreeMap::new();
    let mut i = 1;
    while i < args.len() {
        let name = args[i].as_str();
        if !allowed.contains(&name) || options.contains_key(name) {
            return Err(backup_err(format!("unknown or duplicate option {name}")));
        }
        i += 1;
        let value = if matches!(
            name,
            "--apply"
                | "--writable"
                | "--recover-check"
                | "--dry-run"
                | "--verify"
                | "--resume"
                | "--record"
                | "--gc"
        ) {
            String::new()
        } else {
            let value = args
                .get(i)
                .filter(|v| !v.is_empty() && !v.starts_with("--"))
                .ok_or_else(|| backup_err(format!("{name} requires a value")))?
                .clone();
            i += 1;
            value
        };
        options.insert(name, value);
    }
    let required = |name: &str| {
        options
            .get(name)
            .map(|v| v.as_str())
            .ok_or_else(|| backup_err(format!("missing {name}")))
    };
    let path = |name: &str| -> Result<PathBuf, QueryError> {
        let path = PathBuf::from(required(name)?);
        if !path.is_absolute() {
            return Err(backup_err("offline paths must be absolute"));
        }
        Ok(path)
    };
    let key_file = options.get("--key-file").map(PathBuf::from);
    let target_count = ["--target", "--target-lsn", "--target-time"]
        .iter()
        .filter(|n| options.contains_key(**n))
        .count();
    if target_count > 1 {
        return Err(backup_err("restore targets are mutually exclusive"));
    }
    let target = if let Some(value) = options.get("--target") {
        if value != "latest" {
            return Err(backup_err("--target accepts only latest"));
        }
        Target::Latest
    } else if let Some(value) = options.get("--target-lsn") {
        Target::Lsn(
            value
                .parse()
                .ok()
                .filter(|v| *v > 0 && *v < u64::MAX)
                .ok_or_else(|| backup_err("invalid target LSN"))?,
        )
    } else if let Some(value) = options.get("--target-time") {
        let time = chrono::DateTime::parse_from_rfc3339(value)
            .map_err(|_| backup_err("target time requires RFC3339 with an explicit timezone"))?
            .timestamp_micros();
        Target::Time(u64::try_from(time).map_err(|_| backup_err("target time is before epoch"))?)
    } else if options.contains_key("--base") {
        Target::Base
    } else {
        Target::Latest
    };
    let base = options.get("--base").map(String::as_str);
    let stream = options.get("--stream").map(String::as_str);
    if command == "restore"
        && options.contains_key("--resume")
        && (base.is_none() || matches!(target, Target::Latest))
    {
        return Err(backup_err(
            "restore --resume requires --base and a fixed base/LSN/time target",
        ));
    }
    match command {
        "recoverability" => println!(
            "{}",
            serde_json::to_string_pretty(&super::diagnostics::recoverability(
                &path("--repository")?,
                key_file.as_deref(),
                options.contains_key("--verify")
            )?)
            .map_err(|e| backup_err(e.to_string()))?
        ),
        "keys" => println!(
            "{}",
            serde_json::to_string_pretty(&super::diagnostics::keys(
                &path("--repository")?,
                key_file.as_deref()
            )?)
            .map_err(|e| backup_err(e.to_string()))?
        ),
        "inspect-operation" => {
            let destination = path("--path")?;
            let kind = required("--kind")?;
            if ![
                "repository-init",
                "control-init",
                "restore-work",
                "import-work",
                "export-work",
                "range-work",
                "generation-pins",
                "wal-pins",
                "catalog-checkpoint",
            ]
            .contains(&kind)
            {
                return Err(backup_err("unknown operation kind"));
            }
            let report = DirectoryOperation::inspect(&destination, kind)?;
            if options.contains_key("--apply") {
                if report.get("workspace").and_then(|v| v.as_str()) == Some("ACTIVE") {
                    return Err(backup_err("operation is active"));
                }
                DirectoryOperation::cleanup(&destination, kind)?;
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&DirectoryOperation::inspect(&destination, kind)?)
                    .map_err(|e| backup_err(e.to_string()))?
            );
        }
        "gc" => super::management::prune(
            &path("--repository")?,
            None,
            None,
            options.contains_key("--apply"),
            key_file.as_deref(),
        )?,
        "checkpoint" => {
            let repo = Repository::open(&path("--repository")?, None)?;
            let report = repo.checkpoint()?;
            if options.contains_key("--gc") {
                repo.gc_history()?;
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(|e| backup_err(e.to_string()))?
            );
        }
        "init" => {
            let repo = Repository::init_with_encoding(
                &path("--repository")?,
                options.get("--wal-key-id").map(String::as_str),
                key_file.as_deref(),
                options
                    .get("--wal-encoding")
                    .map_or("ADAPTIVE_LZ4", String::as_str),
            )?;
            println!("{} {}", repo.header.format, repo.header.id);
        }
        "list" => {
            let repo = Repository::open(&path("--repository")?, None)?;
            let _lease = Lease::acquire(&repo.root, false)?;
            let mut deleted = BTreeSet::new();
            repo.visit_live(&repo.position()?, |_, event| {
                match event {
                    Publication::Prune { bases, .. } => deleted.extend(bases),
                    Publication::Base { base } if !deleted.contains(&base.id) => println!(
                        "{}",
                        serde_json::to_string(&base).map_err(|e| backup_err(e.to_string()))?
                    ),
                    _ => {}
                }
                Ok(())
            })?;
        }
        "restore" => {
            if options.contains_key("--dry-run") {
                let report = super::plan::preflight(
                    &path("--repository")?,
                    base,
                    stream,
                    target,
                    &path("--data-dir")?,
                    key_file.as_deref(),
                )?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).map_err(|e| backup_err(e.to_string()))?
                );
                return Ok(());
            }
            let report = super::restore::restore_with_resume(
                &path("--repository")?,
                base,
                stream,
                target,
                &path("--data-dir")?,
                key_file.as_deref(),
                options.contains_key("--writable"),
                options.contains_key("--resume"),
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(|e| backup_err(e.to_string()))?
            );
        }
        "promote" => {
            native::promote(&path("--data-dir")?)?;
            println!("promoted independent restored image");
        }
        "diagnostics" => println!(
            "{}",
            serde_json::to_string_pretty(&super::diagnostics::latest(&path("--repository")?)?)
                .map_err(|e| backup_err(e.to_string()))?
        ),
        "confirm" => {
            match (
                options.contains_key("--data-dir"),
                options.contains_key("--repository"),
            ) {
                (true, false) => {
                    if key_file.is_some() {
                        return Err(backup_err("image confirm accepts no key"));
                    }
                    native::confirm_install(&path("--data-dir")?)?;
                }
                (false, true) => Repository::open(&path("--repository")?, key_file.as_deref())?
                    .confirm_install(key_file.as_deref())?,
                _ => {
                    return Err(backup_err(
                        "confirm requires exactly one of --data-dir or --repository",
                    ))
                }
            }
            println!("confirmed complete installation durability");
        }
        "verify" => {
            if stream.is_some() && !options.contains_key("--recover-check") {
                return Err(backup_err("verify --stream requires --recover-check"));
            }
            let mut repo = Repository::open(&path("--repository")?, key_file.as_deref())?;
            let _lease = Lease::acquire(&repo.root, false)?;
            let position = repo.position()?;
            let outcome = (|| -> Result<(), QueryError> {
                let mut count = 0;
                let mut dead_bases = BTreeSet::new();
                let mut dead_ranges = BTreeSet::new();
                if repo.needs_wal_key(&position)? {
                    repo.require_key(key_file.as_deref())?;
                }
                repo.visit_live(&position, |_, event| {
                    match event {
                        Publication::Prune { bases, ranges } => {
                            dead_bases.extend(bases);
                            dead_ranges.extend(ranges);
                        }
                        Publication::Base { base: entry }
                            if !dead_bases.contains(&entry.id)
                                && base.is_none_or(|id| entry.id == id) =>
                        {
                            repo.base(&entry.id)?;
                            count += 1;
                        }
                        Publication::Range { range } if !dead_ranges.contains(&range.id) => {
                            super::restore::verify_range(&repo, &range)?
                        }
                        _ => {}
                    }
                    Ok(())
                })?;
                repo.verify_coverage(&position)?;
                repo.verify_history()?;
                if base.is_some() && count == 0 {
                    return Err(backup_err("selected base is not published"));
                }
                if options.contains_key("--recover-check") {
                    let work = path("--work-dir")?;
                    reject_overlap(&repo.root, &work)?;
                    create_directory_durable(&work)?;
                    let scratch = ScratchDirectory::new(&work, "recover-check")?;
                    let report = super::restore::restore(
                        &repo.root,
                        base,
                        stream,
                        Target::Latest,
                        &scratch.0.join("image"),
                        key_file.as_deref(),
                        false,
                    )?;
                    let db = Database::open(scratch.0.join("image"))?;
                    if !db.is_snapshot_read_only() {
                        return Err(backup_err(
                            "recover-check did not produce an immutable image",
                        ));
                    }
                    drop(db);
                    println!(
                        "recovery checked through source LSN {}",
                        report.restored_source_lsn
                    );
                } else if options.contains_key("--work-dir") {
                    return Err(backup_err("--work-dir requires --recover-check"));
                }
                println!("verified {count} published bases");
                Ok(())
            })();
            if options.contains_key("--record") {
                repo.record_diagnostic(&position,if options.contains_key("--recover-check"){"RECOVERY_DRILL"}else{"VERIFY"},outcome.as_ref().err().map(error_detail),serde_json::json!({"base_id":base,"stream_id":stream,"validation":"BASE_SHA256_AND_WAL_AUTH_CRC"}))?;
            }
            outcome?;
        }
        "prune" => super::management::prune(
            &path("--repository")?,
            stream,
            options.get("--retain-pitr").map(String::as_str),
            options.contains_key("--apply"),
            key_file.as_deref(),
        )?,
        "export" => {
            if let Some(window) = options.get("--retain-pitr") {
                if base.is_some() || !matches!(target, Target::Latest) {
                    return Err(backup_err(
                        "export --retain-pitr cannot be combined with --base or a fixed target",
                    ));
                }
                super::management::export_window(
                    &path("--repository")?,
                    stream,
                    window,
                    &path("--output")?,
                    key_file.as_deref(),
                )?;
            } else {
                super::management::export(
                    &path("--repository")?,
                    base,
                    stream,
                    target,
                    &path("--output")?,
                    key_file.as_deref(),
                )?
            }
        }
        "import" => {
            if options.contains_key("--resume") {
                super::management::import_with_resume(
                    &path("--repository")?,
                    &path("--input")?,
                    key_file.as_deref(),
                    true,
                )?;
            } else {
                super::management::import(
                    &path("--repository")?,
                    &path("--input")?,
                    key_file.as_deref(),
                )?;
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
