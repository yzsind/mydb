//! Pure parsing/eval helpers for the MTR-lite interpreter.
//!
//! Everything here is engine-independent string manipulation so it stays
//! trivially testable; the stateful interpreter loop (with live SQL execution,
//! includes, and control flow) lives in `super` (`mod.rs`), which owns the
//! engine handle. See `doc_old/design/mysql-mtr-port-plan.md`.

use std::collections::HashMap;

use regex::Regex;

/// A single `--error` expectation.
#[derive(Debug, Clone)]
pub(super) enum Expected {
    /// Numeric MySQL error number, matched against `QueryError::code`.
    Errno(u16),
    /// SQLSTATE (5 chars), matched against `QueryError::sql_state`.
    SqlState(String),
}

/// Parse one `--error` spec token: decimal errno, `S<sqlstate>`, or `ER_name`
/// (resolved against mydb's error catalog).
pub(super) fn parse_error_spec(spec: &str) -> Result<Expected, String> {
    if spec.is_empty() {
        return Err("empty --error spec".to_string());
    }
    if spec.starts_with("ER_") || spec.starts_with("WARN_") {
        return crate::mysql::errors::lookup_by_symbol(spec)
            .map(|found| Expected::Errno(found.code().get()))
            .ok_or_else(|| {
                format!(
                    "--error {spec}: symbol is not part of the MySQL 8.0.46 server-to-client catalog"
                )
            });
    }
    // `S<sqlstate>` form, e.g. S42S01.
    if let Some(state) = spec.strip_prefix('S') {
        if state.len() == 5 && state.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Ok(Expected::SqlState(state.to_string()));
        }
    }
    if let Ok(code) = spec.parse::<u16>() {
        return Ok(Expected::Errno(code));
    }
    Err(format!(
        "unrecognised --error spec `{spec}` (want errno, S<sqlstate>, or ER_name)"
    ))
}

/// Substitute `$name` / `${name}` references from `vars`. Unknown variables
/// expand to the empty string (lenient — the corpus controls its own vars).
pub(super) fn substitute_vars(input: &str, vars: &HashMap<String, String>) -> String {
    if !input.contains('$') {
        return input.to_string();
    }
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() {
            let (name, next) = read_var_name(bytes, i + 1);
            if let Some(name) = name {
                // mysqltest variable identifiers are ASCII
                // case-insensitive (`$s_value` and `$S_value` name the same
                // slot). Runner stores normalized keys; normalize references
                // here as well, including `${...}`.
                out.push_str(
                    vars.get(&name.to_ascii_lowercase())
                        .map(String::as_str)
                        .unwrap_or(""),
                );
                i = next;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Read a variable name starting at `start` (either `{name}` or bare `name`).
/// Returns `(Some(name), index_after)` or `(None, _)` when it is not a name.
fn read_var_name(bytes: &[u8], start: usize) -> (Option<String>, usize) {
    if start < bytes.len() && bytes[start] == b'{' {
        let mut j = start + 1;
        while j < bytes.len() && bytes[j] != b'}' {
            j += 1;
        }
        if j < bytes.len() {
            let name = String::from_utf8_lossy(&bytes[start + 1..j]).into_owned();
            return (Some(name), j + 1);
        }
        return (None, start);
    }
    let mut j = start;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        j += 1;
    }
    if j > start {
        (
            Some(String::from_utf8_lossy(&bytes[start..j]).into_owned()),
            j,
        )
    } else {
        (None, start)
    }
}

/// Evaluate the truthiness of an `if`/`while` condition string (already with
/// variables and backtick-SQL substituted). Supports bare values (truthy when
/// non-empty and not `0`/`""`) and a single binary comparison
/// (`< > <= >= == != <> =`), numeric when both sides parse as integers,
/// otherwise string comparison.
pub(super) fn eval_truthy(expr: &str) -> bool {
    let mut e = expr.trim();
    let mut negate = false;
    while let Some(rest) = e.strip_prefix('!') {
        // `!=` belongs to the comparison parser below, not unary negation.
        if rest.starts_with('=') {
            break;
        }
        negate = !negate;
        e = rest.trim_start();
    }
    let value = eval_truthy_inner(e);
    if negate {
        !value
    } else {
        value
    }
}

fn eval_truthy_inner(e: &str) -> bool {
    if let Some((lhs, op, rhs)) = split_comparison(e) {
        let (l, r) = (lhs.trim(), rhs.trim());
        if let (Ok(li), Ok(ri)) = (l.parse::<i64>(), r.parse::<i64>()) {
            return apply_cmp_i64(li, ri, op);
        }
        let l = unquote(l);
        let r = unquote(r);
        return apply_cmp_str(&l, &r, op);
    }
    let bare = unquote(e);
    !bare.is_empty() && bare != "0"
}

fn split_comparison(e: &str) -> Option<(&str, &str, &str)> {
    for op in ["<=", ">=", "==", "!=", "<>"] {
        if let Some(pos) = e.find(op) {
            return Some((&e[..pos], op, &e[pos + op.len()..]));
        }
    }
    for op in ["<", ">", "="] {
        if let Some(pos) = e.find(op) {
            return Some((&e[..pos], op, &e[pos + op.len()..]));
        }
    }
    None
}

fn apply_cmp_i64(l: i64, r: i64, op: &str) -> bool {
    match op {
        "<" => l < r,
        ">" => l > r,
        "<=" => l <= r,
        ">=" => l >= r,
        "==" | "=" => l == r,
        "!=" | "<>" => l != r,
        _ => false,
    }
}

fn apply_cmp_str(l: &str, r: &str, op: &str) -> bool {
    match op {
        "<" => l < r,
        ">" => l > r,
        "<=" => l <= r,
        ">=" => l >= r,
        "==" | "=" => l == r,
        "!=" | "<>" => l != r,
        _ => false,
    }
}

fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.len() >= 2 && (t.starts_with('\'') && t.ends_with('\'')) {
        return t[1..t.len() - 1].to_string();
    }
    if t.len() >= 2 && (t.starts_with('"') && t.ends_with('"')) {
        return t[1..t.len() - 1].to_string();
    }
    t.to_string()
}

/// Parse a `--replace_regex /pat/repl/[i] /pat2/repl2/ ...` argument into
/// compiled `(Regex, replacement)` pairs. The delimiter is the first char
/// (conventionally `/`); a trailing `i` flag after a pair enables
/// case-insensitive matching for that pair.
pub(super) fn parse_replace_regex(arg: &str) -> Result<Vec<(Regex, String)>, String> {
    let mut pairs = Vec::new();
    let s = arg.trim();
    let mut chars = s.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        let delim = c;
        chars.next();
        let pat = take_until(&mut chars, delim)?;
        let repl = take_until(&mut chars, delim)?;
        // Optional flags immediately following (e.g. `i`).
        let mut case_insensitive = false;
        while let Some(&f) = chars.peek() {
            if f.is_whitespace() {
                break;
            }
            if f == 'i' {
                case_insensitive = true;
            }
            chars.next();
        }
        let pattern = if case_insensitive {
            format!("(?i){pat}")
        } else {
            pat
        };
        let re =
            Regex::new(&pattern).map_err(|e| format!("bad --replace_regex `{pattern}`: {e}"))?;
        pairs.push((re, mtr_repl_to_regex_repl(&repl)));
    }
    Ok(pairs)
}

/// mysqltest replacement uses `\1` for capture groups; the `regex` crate uses
/// `$1`. Translate the common backreference forms.
fn mtr_repl_to_regex_repl(repl: &str) -> String {
    let mut out = String::with_capacity(repl.len());
    let mut chars = repl.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(&d) = chars.peek() {
                if d.is_ascii_digit() {
                    out.push('$');
                    out.push(d);
                    chars.next();
                    continue;
                }
            }
        }
        // Escape a literal `$` so the regex crate does not treat it as a group.
        if c == '$' {
            out.push_str("$$");
            continue;
        }
        out.push(c);
    }
    out
}

fn take_until(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    delim: char,
) -> Result<String, String> {
    let mut out = String::new();
    for c in chars.by_ref() {
        if c == delim {
            return Ok(out);
        }
        out.push(c);
    }
    Err(format!(
        "unterminated --replace_regex segment (missing `{delim}`)"
    ))
}

/// Parse `--replace_result from1 to1 from2 to2 ...` into literal pairs.
pub(super) fn parse_replace_result(arg: &str) -> Result<Vec<(String, String)>, String> {
    let toks = tokenize_ws(arg);
    if toks.len() % 2 != 0 {
        return Err("--replace_result needs an even number of tokens".to_string());
    }
    Ok(toks
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect())
}

/// Parse `--replace_column n1 val1 n2 val2 ...` into (1-based column, value).
pub(super) fn parse_replace_column(arg: &str) -> Result<Vec<(usize, String)>, String> {
    let toks = tokenize_ws(arg);
    if toks.len() % 2 != 0 {
        return Err("--replace_column needs an even number of tokens".to_string());
    }
    let mut out = Vec::new();
    for c in toks.chunks(2) {
        let col: usize = c[0]
            .parse()
            .map_err(|_| format!("--replace_column: bad column index `{}`", c[0]))?;
        out.push((col, c[1].clone()));
    }
    Ok(out)
}

/// Whitespace tokeniser honouring single/double quotes (quotes are stripped).
fn tokenize_ws(s: &str) -> Vec<String> {
    let mut toks = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut in_tok = false;
    for c in s.chars() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                } else {
                    cur.push(c);
                }
            }
            None => {
                if c == '\'' || c == '"' {
                    quote = Some(c);
                    in_tok = true;
                } else if c.is_whitespace() {
                    if in_tok {
                        toks.push(std::mem::take(&mut cur));
                        in_tok = false;
                    }
                } else {
                    cur.push(c);
                    in_tok = true;
                }
            }
        }
    }
    if in_tok {
        toks.push(cur);
    }
    toks
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{eval_truthy, parse_error_spec, substitute_vars, Expected};

    #[test]
    fn mysqltest_truthiness_supports_unary_not() {
        assert!(!eval_truthy("!1"));
        assert!(eval_truthy("!0"));
        assert!(eval_truthy("!!1"));
        assert!(eval_truthy("! 2 = 3"));
        assert!(eval_truthy("2 != 3"));
    }

    #[test]
    fn mysqltest_variables_are_ascii_case_insensitive() {
        let vars = HashMap::from([("s_value".to_string(), "'abc'".to_string())]);
        assert_eq!(
            substitute_vars("$S_value/${S_VALUE}/$s_value", &vars),
            "'abc'/'abc'/'abc'"
        );
    }

    #[test]
    fn resolves_mysql84_symbols_used_by_imported_corpus() {
        let cases = [
            ("ER_NONUNIQ_TABLE", 1066),
            ("ER_TOO_BIG_FIELDLENGTH", 1074),
            ("ER_WRONG_COLUMN_NAME", 1166),
            ("ER_SPECIFIC_ACCESS_DENIED_ERROR", 1227),
            ("ER_NON_UPDATABLE_TABLE", 1288),
            ("ER_NO_DEFAULT_FOR_FIELD", 1364),
            ("ER_TOO_BIG_SCALE", 1425),
            ("ER_TOO_BIG_DISPLAYWIDTH", 1439),
            ("ER_INVALID_ARGUMENT_FOR_LOGARITHM", 3020),
            ("ER_INVALID_JSON_PATH_WILDCARD", 3149),
            ("ER_TF_MUST_HAVE_ALIAS", 3667),
            ("ER_JT_MAX_NESTED_PATH", 3670),
            ("ER_TOO_LONG_SET_ENUM_VALUE", 3505),
            ("ER_WARN_DATA_TRUNCATED_FUNCTIONAL_INDEX", 3751),
            ("ER_WARN_DATA_OUT_OF_RANGE_FUNCTIONAL_INDEX", 3752),
            ("ER_FUNCTIONAL_INDEX_DATA_IS_TOO_LONG", 3907),
            ("ER_WRONG_MVI_VALUE", 3908),
            ("ER_SRS_NOT_FOUND", 3548),
            ("ER_INVALID_OPTION_KEY", 3558),
            ("ER_INVALID_OPTION_VALUE", 3559),
            ("ER_INVALID_OPTION_KEY_VALUE_PAIR", 3560),
            ("ER_INVALID_OPTION_START_CHARACTER", 3561),
            ("ER_INVALID_OPTION_END_CHARACTER", 3562),
            ("ER_INVALID_OPTION_CHARACTERS", 3563),
            ("ER_DUPLICATE_OPTION_KEY", 3564),
            ("ER_LONGITUDE_OUT_OF_RANGE", 3616),
            ("ER_LATITUDE_OUT_OF_RANGE", 3617),
            ("ER_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS", 3618),
            ("ER_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE", 3731),
            ("ER_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE", 3732),
            ("ER_UNIT_NOT_FOUND", 3902),
            ("ER_INVALID_CAST_TO_GEOMETRY", 4032),
            ("ER_INVALID_CAST_POLYGON_RING_DIRECTION", 4033),
        ];

        for (symbol, errno) in cases {
            assert!(
                matches!(parse_error_spec(symbol), Ok(Expected::Errno(code)) if code == errno),
                "{symbol} should resolve to MySQL errno {errno}"
            );
        }
    }
}
