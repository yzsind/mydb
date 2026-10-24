use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const VERSION: &str = "8.0.46";
const EXPECTED_HEADER_SHA256: &str =
    "204d68ebc078f1589c7d7b82ed74bf20ce53103c6f7c5b3ff83c9fafa121c698";
const EXPECTED_MESSAGES_SHA256: &str =
    "f717bafc66926a2998981c28d857a0a190c9e849238d388608b753d2994f3d83";

fn main() {
    if let Err(error) = run() {
        eprintln!("error_catalog_gen: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let mut check = false;
    let mut header = None;
    let mut messages = None;
    let mut output = PathBuf::from("src/error/catalog/mysql80_46_generated.rs");
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--check" => check = true,
            "--header" => {
                header = Some(PathBuf::from(
                    args.next().ok_or("--header requires a path")?,
                ))
            }
            "--messages" => {
                messages = Some(PathBuf::from(
                    args.next().ok_or("--messages requires a path")?,
                ))
            }
            "--output" => output = PathBuf::from(args.next().ok_or("--output requires a path")?),
            _ => return Err(format!("unknown argument `{arg}`")),
        }
    }
    let prefix = PathBuf::from("/opt/homebrew/opt/mysql@8.0");
    let header = header.unwrap_or_else(|| prefix.join("include/mysql/mysqld_error.h"));
    let messages = messages.unwrap_or_else(|| prefix.join("share/mysql/messages_to_clients.txt"));

    let header_bytes =
        fs::read(&header).map_err(|error| format!("read {}: {error}", header.display()))?;
    let message_bytes =
        fs::read(&messages).map_err(|error| format!("read {}: {error}", messages.display()))?;
    let header_sha = sha256(&header_bytes);
    let messages_sha = sha256(&message_bytes);
    if header_sha != EXPECTED_HEADER_SHA256 || messages_sha != EXPECTED_MESSAGES_SHA256 {
        return Err(format!(
            "inputs are not the pinned MySQL {VERSION} files: header={header_sha}, messages={messages_sha}"
        ));
    }

    let generated = format_generated(&generate(
        std::str::from_utf8(&header_bytes).map_err(|error| error.to_string())?,
        std::str::from_utf8(&message_bytes).map_err(|error| error.to_string())?,
        &header_sha,
        &messages_sha,
    )?)?;
    if check {
        let current = fs::read_to_string(&output)
            .map_err(|error| format!("read {}: {error}", output.display()))?;
        if current != generated {
            return Err(format!(
                "{} is stale; rerun error_catalog_gen",
                output.display()
            ));
        }
    } else {
        write_if_changed(&output, &generated)?;
    }
    Ok(())
}

fn generate(
    header: &str,
    messages: &str,
    header_sha: &str,
    messages_sha: &str,
) -> Result<String, String> {
    let mut codes = HashMap::<String, u16>::new();
    for line in header.lines() {
        let mut fields = line.split_whitespace();
        if fields.next() != Some("#define") {
            continue;
        }
        let Some(symbol) = fields.next() else {
            continue;
        };
        if !(symbol.starts_with("ER_") || symbol.starts_with("WARN_")) {
            continue;
        }
        let Some(raw_code) = fields.next() else {
            continue;
        };
        let Ok(code) = raw_code.parse::<u16>() else {
            continue;
        };
        codes.insert(symbol.to_string(), code);
    }

    let mut states = HashMap::<String, String>::new();
    for line in messages.lines() {
        if line.starts_with(char::is_whitespace) || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split_whitespace();
        let Some(symbol) = fields.next() else {
            continue;
        };
        if !codes.contains_key(symbol) {
            continue;
        }
        let state = fields
            .next()
            .filter(|value| {
                value.len() == 5
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || byte.is_ascii_uppercase())
            })
            .unwrap_or("HY000");
        states.insert(symbol.to_string(), state.to_string());
    }

    let mut by_code = BTreeMap::<u16, (String, String)>::new();
    for (symbol, code) in codes {
        let Some(state) = states.remove(&symbol) else {
            continue;
        };
        if let Some((previous, _)) = by_code.insert(code, (symbol.clone(), state)) {
            return Err(format!(
                "duplicate active errno {code}: {previous} and {symbol}"
            ));
        }
    }
    if by_code.len() != 1642 {
        return Err(format!(
            "expected 1642 active server-to-client conditions, found {}",
            by_code.len()
        ));
    }

    let mut out = String::with_capacity(by_code.len() * 180);
    writeln!(
        out,
        "// @generated by `cargo run --bin error_catalog_gen`; do not edit."
    )
    .unwrap();
    writeln!(out, "use crate::error::{{ErrorDomain, ErrorSpec}};\n").unwrap();
    writeln!(out, "pub const MYSQL80_46_VERSION: &str = \"{VERSION}\";").unwrap();
    writeln!(
        out,
        "pub const MYSQL80_46_MYSQLD_ERROR_SHA256: &str = \"{header_sha}\";"
    )
    .unwrap();
    writeln!(
        out,
        "pub const MYSQL80_46_MESSAGES_TO_CLIENTS_SHA256: &str = \"{messages_sha}\";\n"
    )
    .unwrap();
    for (code, (symbol, state)) in &by_code {
        writeln!(
            out,
            "pub const {symbol}: ErrorSpec = ErrorSpec::new(ErrorDomain::MysqlServer, {code}, *b\"{state}\", \"{symbol}\");"
        )
        .unwrap();
    }
    writeln!(
        out,
        "\npub static MYSQL80_46_ERROR_SPECS: &[ErrorSpec] = &["
    )
    .unwrap();
    for (symbol, _) in by_code.values() {
        writeln!(out, "    {symbol},").unwrap();
    }
    writeln!(out, "];").unwrap();
    Ok(out)
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn write_if_changed(path: &Path, contents: &str) -> Result<(), String> {
    if fs::read_to_string(path).ok().as_deref() == Some(contents) {
        return Ok(());
    }
    fs::write(path, contents).map_err(|error| format!("write {}: {error}", path.display()))
}

fn format_generated(contents: &str) -> Result<String, String> {
    let path = std::env::temp_dir().join(format!(
        "mydb-error-catalog-{}-{}.rs",
        std::process::id(),
        VERSION.replace('.', "_")
    ));
    fs::write(&path, contents).map_err(|error| format!("write {}: {error}", path.display()))?;
    let result = Command::new("rustfmt")
        .args(["--edition", "2021"])
        .arg(&path)
        .output()
        .map_err(|error| format!("run rustfmt: {error}"))?;
    if !result.status.success() {
        let _ = fs::remove_file(&path);
        return Err(format!(
            "rustfmt failed: {}",
            String::from_utf8_lossy(&result.stderr)
        ));
    }
    let formatted =
        fs::read_to_string(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let _ = fs::remove_file(&path);
    Ok(formatted)
}
