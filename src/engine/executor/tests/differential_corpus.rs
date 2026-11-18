//! Differential execution corpus for analytic SQL.
//!
//! Every query is executed under a matrix of executor configurations and the
//! results are compared with the deterministic oracle (`VecMode::Off`,
//! serial). The corpus combines the TPC-H and TPC-DS query texts shipped in
//! `scripts/` with hand-written shape queries that stress NULL semantics,
//! outer/semi/anti joins, grouping sets, windows and aggregate edge cases.
//!
//! The data set is generated in SQL from `generate_series`, so it is fully
//! deterministic. Column values are derived from the column type plus a small
//! vocabulary scanned from the predicates of the query corpus itself, which
//! keeps most filters selective instead of trivially empty. The benchmark
//! texts are only measurement inputs here: nothing in the engine is aware of
//! them.

use super::*;
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;

// ---------------------------------------------------------------------------
// Script helpers
// ---------------------------------------------------------------------------

fn repo_path(relative: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn read_repo_file(relative: &str) -> String {
    std::fs::read_to_string(repo_path(relative))
        .unwrap_or_else(|err| panic!("read {relative}: {err}"))
}

/// Split a SQL script into statements, honouring quotes and `--` comments.
fn split_statements(script: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = script.chars().peekable();
    let mut quote: Option<char> = None;
    while let Some(ch) = chars.next() {
        match quote {
            Some(q) => {
                current.push(ch);
                if ch == q {
                    if chars.peek() == Some(&q) {
                        current.push(chars.next().unwrap());
                    } else {
                        quote = None;
                    }
                }
            }
            None => match ch {
                '\'' | '"' | '`' => {
                    quote = Some(ch);
                    current.push(ch);
                }
                '-' if chars.peek() == Some(&'-') => {
                    for next in chars.by_ref() {
                        if next == '\n' {
                            break;
                        }
                    }
                    current.push('\n');
                }
                ';' => {
                    let statement = current.trim();
                    if !statement.is_empty() {
                        statements.push(statement.to_string());
                    }
                    current.clear();
                }
                _ => current.push(ch),
            },
        }
    }
    let statement = current.trim();
    if !statement.is_empty() {
        statements.push(statement.to_string());
    }
    statements
}

fn is_database_switch(statement: &str) -> bool {
    let lower = statement.trim_start().to_ascii_lowercase();
    lower.starts_with("use ") || lower.starts_with("create database")
}

fn sql_quote(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "''"))
}

/// Small deterministic hash of a name; used to decorrelate column generators.
fn seed_of(name: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in name.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash % 999_983
}

// ---------------------------------------------------------------------------
// Schema model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum ColType {
    Int,
    Decimal { precision: u32, scale: u32 },
    Char(u32),
    Date,
    Time,
}

#[derive(Debug, Clone)]
struct Column {
    name: String,
    ty: ColType,
    nullable: bool,
}

#[derive(Debug, Clone)]
struct Table {
    name: String,
    columns: Vec<Column>,
    primary_key: Vec<String>,
}

fn split_top_level_commas(body: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();
    for ch in body.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                items.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    items
}

fn parse_col_type(text: &str) -> ColType {
    let lower = text.to_ascii_lowercase();
    let args = |s: &str| -> Vec<u32> {
        s.split_once('(')
            .and_then(|(_, rest)| rest.split_once(')'))
            .map(|(inner, _)| {
                inner
                    .split(',')
                    .filter_map(|part| part.trim().parse().ok())
                    .collect()
            })
            .unwrap_or_default()
    };
    if lower.starts_with("decimal") || lower.starts_with("numeric") {
        let a = args(&lower);
        ColType::Decimal {
            precision: a.first().copied().unwrap_or(10),
            scale: a.get(1).copied().unwrap_or(0),
        }
    } else if lower.starts_with("char") || lower.starts_with("varchar") {
        ColType::Char(args(&lower).first().copied().unwrap_or(16))
    } else if lower.starts_with("date") {
        ColType::Date
    } else if lower.starts_with("time") {
        ColType::Time
    } else {
        ColType::Int
    }
}

fn parse_schema(script: &str) -> Vec<Table> {
    let mut tables = Vec::new();
    for statement in split_statements(script) {
        let lower = statement.to_ascii_lowercase();
        let Some(rest) = lower.strip_prefix("create table") else {
            continue;
        };
        let name = rest
            .trim_start()
            .split(|c: char| c.is_whitespace() || c == '(')
            .next()
            .unwrap()
            .to_string();
        let open = lower.find('(').unwrap();
        let close = lower.rfind(')').unwrap();
        let mut columns = Vec::new();
        let mut primary_key = Vec::new();
        for item in split_top_level_commas(&lower[open + 1..close]) {
            if let Some(keys) = item.strip_prefix("primary key") {
                primary_key = keys
                    .trim()
                    .trim_start_matches('(')
                    .trim_end_matches(')')
                    .split(',')
                    .map(|key| key.trim().to_string())
                    .collect();
                continue;
            }
            let mut parts = item.split_whitespace();
            let column = parts.next().unwrap().to_string();
            let type_text = parts.next().unwrap_or("integer");
            let tail = parts.collect::<Vec<_>>().join(" ");
            if tail.contains("primary key") {
                primary_key = vec![column.clone()];
            }
            columns.push(Column {
                name: column,
                ty: parse_col_type(type_text),
                nullable: !tail.contains("not null") && !tail.contains("primary key"),
            });
        }
        tables.push(Table {
            name,
            columns,
            primary_key,
        });
    }
    tables
}

// ---------------------------------------------------------------------------
// Literal vocabulary scanned from query predicates
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Str(String),
    Num(f64),
    Sym(String),
}

fn tokenize(sql: &str) -> Vec<Token> {
    let chars: Vec<char> = sql.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        if ch.is_whitespace() {
            i += 1;
        } else if ch == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if ch == '\'' {
            let mut text = String::new();
            i += 1;
            while i < chars.len() {
                if chars[i] == '\'' {
                    if chars.get(i + 1) == Some(&'\'') {
                        text.push('\'');
                        i += 2;
                        continue;
                    }
                    break;
                }
                text.push(chars[i]);
                i += 1;
            }
            i += 1;
            tokens.push(Token::Str(text));
        } else if ch.is_ascii_digit()
            || (ch == '.' && chars.get(i + 1).is_some_and(char::is_ascii_digit))
        {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            let text: String = chars[start..i].iter().collect();
            tokens.push(Token::Num(text.parse().unwrap_or(0.0)));
        } else if ch.is_alphabetic() || ch == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let text: String = chars[start..i].iter().collect();
            tokens.push(Token::Ident(text.to_ascii_lowercase()));
        } else {
            let two: String = chars[i..(i + 2).min(chars.len())].iter().collect();
            if matches!(two.as_str(), "<=" | ">=" | "<>" | "!=") {
                tokens.push(Token::Sym(two));
                i += 2;
            } else {
                tokens.push(Token::Sym(ch.to_string()));
                i += 1;
            }
        }
    }
    tokens
}

#[derive(Debug, Default, Clone)]
struct ColumnVocab {
    strings: BTreeSet<String>,
    numbers: Vec<f64>,
    dates: BTreeSet<String>,
}

/// Collect the literals each known column is compared against. The column is
/// the nearest preceding known column name within a short token window, which
/// also covers `SUBSTR(col, 1, 5) IN (...)` and `col + 0 BETWEEN a AND b`.
fn scan_vocabulary(queries: &[String], known: &BTreeSet<String>) -> HashMap<String, ColumnVocab> {
    let mut vocab: HashMap<String, ColumnVocab> = HashMap::new();
    for sql in queries {
        let tokens = tokenize(sql);
        let column_before = |at: usize| -> Option<String> {
            (at.saturating_sub(8)..at)
                .rev()
                .find_map(|j| match &tokens[j] {
                    Token::Ident(name) if known.contains(name) => Some(name.clone()),
                    _ => None,
                })
        };
        for (at, token) in tokens.iter().enumerate() {
            let Token::Ident(word) = token else {
                if let Token::Sym(sym) = token {
                    if matches!(sym.as_str(), "=" | "<>" | "!=" | "<" | ">" | "<=" | ">=") {
                        let Some(column) = column_before(at) else {
                            continue;
                        };
                        let entry = vocab.entry(column).or_default();
                        match (tokens.get(at + 1), tokens.get(at + 2)) {
                            (Some(Token::Str(text)), _) => {
                                entry.strings.insert(text.clone());
                            }
                            (Some(Token::Num(n)), _) => entry.numbers.push(*n),
                            (Some(Token::Sym(minus)), Some(Token::Num(n))) if minus == "-" => {
                                entry.numbers.push(-*n)
                            }
                            (Some(Token::Ident(kw)), Some(Token::Str(text))) if kw == "date" => {
                                entry.dates.insert(text.clone());
                            }
                            _ => {}
                        }
                    }
                }
                continue;
            };
            match word.as_str() {
                "in" if tokens.get(at + 1) == Some(&Token::Sym("(".into())) => {
                    let Some(column) = column_before(at) else {
                        continue;
                    };
                    let entry = vocab.entry(column).or_default();
                    let mut j = at + 2;
                    let mut negative = false;
                    while let Some(token) = tokens.get(j) {
                        match token {
                            Token::Str(text) => {
                                entry.strings.insert(text.clone());
                            }
                            Token::Num(n) => {
                                entry.numbers.push(if negative { -*n } else { *n });
                                negative = false;
                            }
                            Token::Sym(s) if s == "-" => negative = true,
                            Token::Sym(s) if s == "," => {}
                            _ => break,
                        }
                        j += 1;
                    }
                }
                "like" => {
                    let Some(column) = column_before(at) else {
                        continue;
                    };
                    if let Some(Token::Str(pattern)) = tokens.get(at + 1) {
                        // Materialize a value that matches the pattern.
                        let value = pattern.replace('%', "x").replace('_', "y");
                        vocab.entry(column).or_default().strings.insert(value);
                    }
                }
                "between" => {
                    let Some(column) = column_before(at) else {
                        continue;
                    };
                    let entry = vocab.entry(column).or_default();
                    for offset in [1, 3, 2, 4] {
                        match tokens.get(at + offset) {
                            Some(Token::Num(n)) => entry.numbers.push(*n),
                            Some(Token::Str(text)) if text.len() == 10 => {
                                entry.dates.insert(text.clone());
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
    }
    vocab
}

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

/// Key domain of a referenced table: `base + step * k` for `k` in `0..count`.
#[derive(Debug, Clone, Copy)]
struct KeyDomain {
    base: i64,
    step: i64,
    count: i64,
}

struct Generator<'a> {
    vocab: &'a HashMap<String, ColumnVocab>,
    /// Column-name overrides (full SQL expression of `n`).
    overrides: HashMap<String, String>,
    /// Foreign-key suffix → referenced key domain.
    references: Vec<(String, KeyDomain)>,
    base_date: &'a str,
    date_span_days: i64,
}

impl Generator<'_> {
    /// A pseudo-random, well-mixed integer in `0..modulus` for row `n`.
    fn hash(seed: u64, modulus: i64, row: &str) -> String {
        format!("((({row} * 7919 + {seed}) * 104729) % {modulus})")
    }

    fn reference(&self, column: &str) -> Option<KeyDomain> {
        self.references
            .iter()
            .filter(|(suffix, _)| column.ends_with(suffix.as_str()))
            .max_by_key(|(suffix, _)| suffix.len())
            .map(|(_, domain)| *domain)
    }

    fn column_expr(&self, table: &Table, column: &Column, rows: i64, row: &str) -> String {
        if let Some(expr) = self.overrides.get(&column.name) {
            return expr.replace("{n}", row);
        }
        let is_pk = table.primary_key.len() == 1 && table.primary_key[0] == column.name;
        if is_pk {
            return row.to_string();
        }
        // Shared hash seed for a reference suffix lets sibling fact tables
        // (and returns rows mapped onto their sales rows) agree on keys.
        if let Some(domain) = self.reference(&column.name) {
            let suffix_seed = self
                .references
                .iter()
                .filter(|(suffix, _)| column.name.ends_with(suffix.as_str()))
                .max_by_key(|(suffix, _)| suffix.len())
                .map(|(suffix, _)| seed_of(suffix))
                .unwrap();
            let key = format!(
                "({} + {} * {})",
                domain.base,
                domain.step,
                Self::hash(suffix_seed, domain.count, row)
            );
            return if column.nullable {
                format!(
                    "CASE WHEN {} = 0 THEN NULL ELSE {key} END",
                    Self::hash(seed_of(&column.name), 50, row)
                )
            } else {
                key
            };
        }
        let seed = seed_of(&column.name);
        let vocab = self.vocab.get(&column.name).cloned().unwrap_or_default();
        let value = match &column.ty {
            ColType::Int => {
                if vocab.numbers.is_empty() {
                    format!("({} + 1)", Self::hash(seed, rows.clamp(7, 1000), row))
                } else {
                    let lo = vocab
                        .numbers
                        .iter()
                        .cloned()
                        .fold(f64::MAX, f64::min)
                        .floor() as i64;
                    let hi = vocab
                        .numbers
                        .iter()
                        .cloned()
                        .fold(f64::MIN, f64::max)
                        .ceil() as i64;
                    let pad = ((hi - lo) / 4).max(2);
                    format!(
                        "({} + {})",
                        lo - pad,
                        Self::hash(seed, hi - lo + 2 * pad + 1, row)
                    )
                }
            }
            ColType::Decimal { precision, scale } => {
                let limit = 10i64.pow(precision.saturating_sub(*scale).min(9)) - 1;
                let unit = 10i64.pow((*scale).min(6));
                let (lo, hi) = if vocab.numbers.is_empty() {
                    (-(limit.min(20)), limit.min(500))
                } else {
                    let lo = vocab.numbers.iter().cloned().fold(f64::MAX, f64::min);
                    let hi = vocab.numbers.iter().cloned().fold(f64::MIN, f64::max);
                    let pad = ((hi - lo) / 2.0).max(1.0 / unit as f64 * 5.0);
                    (
                        ((lo - pad).floor() as i64).max(-limit),
                        ((hi + pad).ceil() as i64).min(limit),
                    )
                };
                let span = ((hi - lo).max(1)) * unit;
                format!(
                    "CAST(({lo} * {unit} + {}) / {unit} AS DECIMAL({precision},{scale}))",
                    Self::hash(seed, span, row)
                )
            }
            ColType::Char(len) => {
                let len = *len as usize;
                let literals: Vec<&String> = vocab
                    .strings
                    .iter()
                    .filter(|text| text.chars().count() <= len)
                    .collect();
                let synthetic_len = len.min(14);
                let synthetic = format!(
                    "LEFT(CONCAT('{}', LPAD({}, 8, '0')), {synthetic_len})",
                    &column.name.chars().take(4).collect::<String>(),
                    Self::hash(seed, rows.clamp(3, 997), row)
                );
                if literals.is_empty() {
                    synthetic
                } else {
                    // Literals are ~3/4 of the rows; the rest are synthetic
                    // so every predicate also has non-matching rows.
                    let mut elt = format!(
                        "ELT(1 + {}, ",
                        Self::hash(seed, (literals.len() + literals.len() / 3 + 1) as i64, row)
                    );
                    for literal in &literals {
                        let _ = write!(elt, "{}, ", sql_quote(literal));
                    }
                    for _ in 0..(literals.len() / 3 + 1) {
                        let _ = write!(elt, "{synthetic}, ");
                    }
                    elt.truncate(elt.len() - 2);
                    elt.push(')');
                    elt
                }
            }
            ColType::Date => {
                let (base, span) = match (vocab.dates.first(), vocab.dates.last()) {
                    (Some(first), Some(_)) => (
                        format!("DATE_SUB({}, INTERVAL 400 DAY)", sql_quote(first)),
                        self.date_span_days.max(1200),
                    ),
                    _ => (sql_quote(self.base_date), self.date_span_days),
                };
                format!(
                    "DATE_ADD({base}, INTERVAL {} DAY)",
                    Self::hash(seed, span, row)
                )
            }
            ColType::Time => format!("SEC_TO_TIME({})", Self::hash(seed, 86_400, row)),
        };
        if column.nullable {
            format!(
                "CASE WHEN {} = 0 THEN NULL ELSE {value} END",
                Self::hash(seed ^ 0x55, 41, row)
            )
        } else {
            value
        }
    }

    fn insert_sql(&self, table: &Table, rows: i64, row: &str) -> String {
        let exprs = table
            .columns
            .iter()
            .map(|column| self.column_expr(table, column, rows, row))
            .collect::<Vec<_>>()
            .join(",\n  ");
        format!(
            "INSERT INTO {} SELECT\n  {exprs}\nFROM generate_series(1, {rows}) AS gs(n)",
            table.name
        )
    }
}

fn exec_script(engine: &StorageQueryEngine, script: &str) {
    for statement in split_statements(script) {
        if is_database_switch(&statement) {
            continue;
        }
        engine
            .execute(&statement)
            .unwrap_or_else(|err| panic!("setup statement failed: {err}\n{statement}"));
    }
}

fn load_queries(dir: &str) -> Vec<(String, String)> {
    let mut files: Vec<_> = std::fs::read_dir(repo_path(dir))
        .unwrap()
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "sql"))
        .collect();
    files.sort();
    let mut queries = Vec::new();
    for path in files {
        let stem = path.file_stem().unwrap().to_string_lossy().to_string();
        let statements = split_statements(&std::fs::read_to_string(&path).unwrap());
        let multi = statements.len() > 1;
        for (index, statement) in statements.into_iter().enumerate() {
            let name = if multi {
                format!("{stem}{}", (b'a' + index as u8) as char)
            } else {
                stem.clone()
            };
            queries.push((name, statement));
        }
    }
    queries
}

const TPCH_NATIONS: [(&str, i64); 25] = [
    ("ALGERIA", 0),
    ("ARGENTINA", 1),
    ("BRAZIL", 1),
    ("CANADA", 1),
    ("EGYPT", 4),
    ("ETHIOPIA", 0),
    ("FRANCE", 3),
    ("GERMANY", 3),
    ("INDIA", 2),
    ("INDONESIA", 2),
    ("IRAN", 4),
    ("IRAQ", 4),
    ("JAPAN", 2),
    ("JORDAN", 4),
    ("KENYA", 0),
    ("MOROCCO", 0),
    ("MOZAMBIQUE", 0),
    ("PERU", 1),
    ("CHINA", 2),
    ("ROMANIA", 3),
    ("SAUDI ARABIA", 4),
    ("VIETNAM", 2),
    ("RUSSIA", 3),
    ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1),
];

fn build_tpch(engine: &StorageQueryEngine, queries: &[(String, String)]) {
    let schema = read_repo_file("scripts/tpch/schema.sql");
    exec_script(engine, &schema);
    let tables = parse_schema(&schema);
    let known: BTreeSet<String> = tables
        .iter()
        .flat_map(|t| t.columns.iter().map(|c| c.name.clone()))
        .collect();
    let texts: Vec<String> = queries.iter().map(|(_, sql)| sql.clone()).collect();
    let vocab = scan_vocabulary(&texts, &known);

    let (suppliers, parts, customers, orders) = (60i64, 240i64, 300i64, 2400i64);
    let lineitems = orders * 4;
    let mut overrides = HashMap::new();
    // partsupp: four suppliers per part, mirroring dbgen's spread.
    overrides.insert("ps_partkey".into(), "(({n} - 1) DIV 4 + 1)".to_string());
    let supp_of =
        |part: &str, j: &str| format!("((({part}) + ({j}) * {}) % {suppliers} + 1)", suppliers / 4);
    overrides.insert(
        "ps_suppkey".into(),
        supp_of("({n} - 1) DIV 4", "({n} - 1) % 4"),
    );
    // lineitem: ~4 lines per order and (partkey, suppkey) always in partsupp.
    overrides.insert("l_orderkey".into(), "(({n} - 1) DIV 4 + 1)".to_string());
    overrides.insert("l_linenumber".into(), "(({n} - 1) % 4 + 1)".to_string());
    overrides.insert(
        "l_partkey".into(),
        format!("((({{n}} * 7919 + 17) * 104729) % {parts} + 1)"),
    );
    overrides.insert(
        "l_suppkey".into(),
        supp_of(
            &format!("((({{n}} * 7919 + 17) * 104729) % {parts})"),
            "({n} * 13) % 4",
        ),
    );
    // Every third customer places no orders (TPC-H Q13/Q22 shapes).
    overrides.insert(
        "o_custkey".into(),
        format!(
            "(((({{n}} * 7919 + 5) * 104729) % {}) * 3 + 1)",
            customers / 3
        ),
    );
    overrides.insert("c_nationkey".into(), "(({n} * 7) % 25)".to_string());
    overrides.insert("s_nationkey".into(), "(({n} * 11) % 25)".to_string());
    overrides.insert(
        "c_phone".into(),
        "CONCAT(10 + ({n} % 25), '-', LPAD({n}, 3, '0'), '-555-0100')".to_string(),
    );
    let generator = Generator {
        vocab: &vocab,
        overrides,
        references: Vec::new(),
        base_date: "1992-01-01",
        date_span_days: 2400,
    };

    // Region and nation carry the real names the predicates refer to.
    engine
        .execute(
            "INSERT INTO region VALUES (0,'AFRICA','r0'),(1,'AMERICA','r1'),(2,'ASIA','r2'),\
             (3,'EUROPE','r3'),(4,'MIDDLE EAST','r4')",
        )
        .unwrap();
    let nations = TPCH_NATIONS
        .iter()
        .enumerate()
        .map(|(key, (name, region))| format!("({key},'{name}',{region},'n{key}')"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO nation VALUES {nations}"))
        .unwrap();
    for (name, rows) in [
        ("supplier", suppliers),
        ("part", parts),
        ("partsupp", parts * 4),
        ("customer", customers),
        ("orders", orders),
        ("lineitem", lineitems),
    ] {
        let table = tables.iter().find(|t| t.name == name).unwrap();
        let sql = generator.insert_sql(table, rows, "n");
        engine
            .execute(&sql)
            .unwrap_or_else(|err| panic!("generate {name}: {err}\n{sql}"));
    }
    exec_script(engine, &read_repo_file("scripts/tpch/indexes.sql"));
    exec_script(engine, &read_repo_file("scripts/tpch/analyze.sql"));
}

/// Julian day number of 1998-01-01, the first `date_dim` row.
const TPCDS_FIRST_DATE_SK: i64 = 2_450_815;
const TPCDS_DATE_ROWS: i64 = 2_191; // 1998-01-01 ..= 2003-12-31

fn build_tpcds(engine: &StorageQueryEngine, queries: &[(String, String)]) {
    let schema = read_repo_file("scripts/tpcds/schema.sql");
    exec_script(engine, &schema);
    let tables = parse_schema(&schema);
    let known: BTreeSet<String> = tables
        .iter()
        .flat_map(|t| t.columns.iter().map(|c| c.name.clone()))
        .collect();
    let texts: Vec<String> = queries.iter().map(|(_, sql)| sql.clone()).collect();
    let vocab = scan_vocabulary(&texts, &known);

    let dimension_rows: BTreeMap<&str, i64> = [
        ("item", 240),
        ("customer", 400),
        ("customer_address", 300),
        ("customer_demographics", 320),
        ("household_demographics", 180),
        ("income_band", 20),
        ("store", 12),
        ("warehouse", 5),
        ("promotion", 30),
        ("reason", 35),
        ("ship_mode", 20),
        ("web_site", 6),
        ("web_page", 40),
        ("call_center", 6),
        ("catalog_page", 80),
        ("dbgen_version", 1),
    ]
    .into_iter()
    .collect();
    let domain = |rows: i64| KeyDomain {
        base: 1,
        step: 1,
        count: rows,
    };
    let references = vec![
        (
            "date_sk".to_string(),
            KeyDomain {
                base: TPCDS_FIRST_DATE_SK,
                step: 1,
                count: TPCDS_DATE_ROWS,
            },
        ),
        (
            "time_sk".to_string(),
            KeyDomain {
                base: 0,
                step: 60,
                count: 1440,
            },
        ),
        ("item_sk".into(), domain(dimension_rows["item"])),
        ("customer_sk".into(), domain(dimension_rows["customer"])),
        (
            "cdemo_sk".into(),
            domain(dimension_rows["customer_demographics"]),
        ),
        (
            "hdemo_sk".into(),
            domain(dimension_rows["household_demographics"]),
        ),
        ("addr_sk".into(), domain(dimension_rows["customer_address"])),
        ("store_sk".into(), domain(dimension_rows["store"])),
        ("promo_sk".into(), domain(dimension_rows["promotion"])),
        ("reason_sk".into(), domain(dimension_rows["reason"])),
        ("ship_mode_sk".into(), domain(dimension_rows["ship_mode"])),
        ("warehouse_sk".into(), domain(dimension_rows["warehouse"])),
        ("web_site_sk".into(), domain(dimension_rows["web_site"])),
        ("web_page_sk".into(), domain(dimension_rows["web_page"])),
        (
            "call_center_sk".into(),
            domain(dimension_rows["call_center"]),
        ),
        (
            "catalog_page_sk".into(),
            domain(dimension_rows["catalog_page"]),
        ),
        (
            "income_band_sk".into(),
            domain(dimension_rows["income_band"]),
        ),
        // Several sales lines share one ticket / order.
        ("ticket_number".into(), domain(1_500)),
        ("order_number".into(), domain(1_000)),
    ];

    let mut overrides: HashMap<String, String> = HashMap::new();
    // A real calendar: most TPC-DS predicates filter date_dim attributes.
    let day = format!("DATE_ADD('1998-01-01', INTERVAL ({{n}} - 1) DAY)");
    for (column, expr) in [
        ("d_date_sk", format!("({TPCDS_FIRST_DATE_SK} - 1 + {{n}})")),
        (
            "d_date_id",
            "CONCAT('AAAAAAAA', LPAD({n}, 8, '0'))".to_string(),
        ),
        ("d_date", day.clone()),
        (
            "d_month_seq",
            format!("((YEAR({day}) - 1900) * 12 + MONTH({day}) - 1)"),
        ),
        (
            "d_week_seq",
            format!("(DATEDIFF({day}, '1900-01-01') DIV 7 + 1)"),
        ),
        (
            "d_quarter_seq",
            format!("((YEAR({day}) - 1900) * 4 + QUARTER({day}))"),
        ),
        ("d_year", format!("YEAR({day})")),
        ("d_dow", format!("(DAYOFWEEK({day}) - 1)")),
        ("d_moy", format!("MONTH({day})")),
        ("d_dom", format!("DAY({day})")),
        ("d_qoy", format!("QUARTER({day})")),
        ("d_fy_year", format!("YEAR({day})")),
        (
            "d_fy_quarter_seq",
            format!("((YEAR({day}) - 1900) * 4 + QUARTER({day}))"),
        ),
        (
            "d_fy_week_seq",
            format!("(DATEDIFF({day}, '1900-01-01') DIV 7 + 1)"),
        ),
        ("d_day_name", format!("DAYNAME({day})")),
        (
            "d_quarter_name",
            format!("CONCAT(YEAR({day}), 'Q', QUARTER({day}))"),
        ),
        ("d_holiday", "IF({n} % 31 = 0, 'Y', 'N')".to_string()),
        (
            "d_weekend",
            format!("IF(DAYOFWEEK({day}) IN (1, 7), 'Y', 'N')"),
        ),
        (
            "d_following_holiday",
            "IF({n} % 31 = 1, 'Y', 'N')".to_string(),
        ),
        (
            "d_first_dom",
            format!("({TPCDS_FIRST_DATE_SK} + {{n}} - DAY({day}))"),
        ),
        (
            "d_last_dom",
            format!("({TPCDS_FIRST_DATE_SK} + {{n}} - DAY({day}) + 27)"),
        ),
        (
            "d_same_day_ly",
            format!("({TPCDS_FIRST_DATE_SK} - 1 + {{n}} - 365)"),
        ),
        (
            "d_same_day_lq",
            format!("({TPCDS_FIRST_DATE_SK} - 1 + {{n}} - 91)"),
        ),
        ("d_current_day", "'N'".to_string()),
        ("d_current_week", "'N'".to_string()),
        ("d_current_month", "IF({n} > 2160, 'Y', 'N')".to_string()),
        ("d_current_quarter", "'N'".to_string()),
        ("d_current_year", "IF({n} > 1826, 'Y', 'N')".to_string()),
        // time_dim at minute granularity.
        ("t_time_sk", "(({n} - 1) * 60)".to_string()),
        ("t_time", "(({n} - 1) * 60)".to_string()),
        ("t_hour", "(({n} - 1) DIV 60)".to_string()),
        ("t_minute", "(({n} - 1) % 60)".to_string()),
        ("t_second", "0".to_string()),
        (
            "t_am_pm",
            "IF(({n} - 1) DIV 60 < 12, 'AM', 'PM')".to_string(),
        ),
    ] {
        overrides.insert(column.to_string(), expr);
    }
    // Business ids are shared by two surrogate rows (slowly changing
    // dimensions), which exercises GROUP BY id vs. join by surrogate key.
    for (column, divisor) in [
        ("i_item_id", 2),
        ("c_customer_id", 1),
        ("s_store_id", 2),
        ("w_warehouse_id", 1),
        ("web_site_id", 2),
        ("cc_call_center_id", 2),
        ("cp_catalog_page_id", 1),
        ("wp_web_page_id", 1),
        ("p_promo_id", 1),
        ("ca_address_id", 1),
        ("r_reason_id", 1),
        ("sm_ship_mode_id", 1),
        ("t_time_id", 1),
    ] {
        overrides.insert(
            column.to_string(),
            format!("CONCAT('AAAAAAAA', LPAD(({{n}} + {divisor} - 1) DIV {divisor}, 8, '0'))"),
        );
    }
    let generator = Generator {
        vocab: &vocab,
        overrides,
        references,
        base_date: "1998-01-01",
        date_span_days: TPCDS_DATE_ROWS,
    };

    let fact_rows: [(&str, i64, Option<(&str, i64)>); 7] = [
        ("store_sales", 6_000, None),
        ("catalog_sales", 4_000, None),
        ("web_sales", 3_000, None),
        // Returns rows are generated from every 5th sales row so ticket /
        // order number and item agree with an existing sale.
        ("store_returns", 1_200, Some(("store_sales", 5))),
        ("catalog_returns", 800, Some(("catalog_sales", 5))),
        ("web_returns", 600, Some(("web_sales", 5))),
        ("inventory", 3_000, None),
    ];
    for table in &tables {
        let (rows, row_expr) = if table.name == "date_dim" {
            (TPCDS_DATE_ROWS, "n".to_string())
        } else if table.name == "time_dim" {
            (1_440, "n".to_string())
        } else if let Some(rows) = dimension_rows.get(table.name.as_str()) {
            (*rows, "n".to_string())
        } else if let Some((_, rows, parent)) =
            fact_rows.iter().find(|(name, _, _)| *name == table.name)
        {
            match parent {
                Some((_, stride)) => (*rows, format!("(n * {stride})")),
                None => (*rows, "n".to_string()),
            }
        } else {
            panic!("no row count for TPC-DS table {}", table.name);
        };
        let sql = generator.insert_sql(table, rows, &row_expr);
        engine
            .execute(&sql)
            .unwrap_or_else(|err| panic!("generate {}: {err}\n{sql}", table.name));
    }
    exec_script(engine, &read_repo_file("scripts/tpcds/indexes.sql"));
    exec_script(engine, &read_repo_file("scripts/tpcds/analyze.sql"));
}

// ---------------------------------------------------------------------------
// Execution matrix and comparison
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Config {
    mode: VecMode,
    parallel: u32,
}

const ORACLE: Config = Config {
    mode: VecMode::Off,
    parallel: 1,
};

const MATRIX: [Config; 5] = [
    Config {
        mode: VecMode::On,
        parallel: 1,
    },
    Config {
        mode: VecMode::Auto,
        parallel: 1,
    },
    Config {
        mode: VecMode::Off,
        parallel: 4,
    },
    Config {
        mode: VecMode::On,
        parallel: 4,
    },
    Config {
        mode: VecMode::Auto,
        parallel: 4,
    },
];

/// Canonical cell used for comparison: floats are compared with a relative
/// tolerance, everything else by its display text.
#[derive(Debug, Clone)]
enum Cell {
    Null,
    Float(f64),
    Text(String),
}

impl Cell {
    fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => Cell::Null,
            Value::Float(v) => Cell::Float(f64::from(*v)),
            Value::Double(v) => Cell::Float(*v),
            other => Cell::Text(other.to_display_string()),
        }
    }

    /// Sort key that is stable across tiny float differences.
    fn sort_key(&self) -> String {
        match self {
            Cell::Null => "\u{0}".to_string(),
            Cell::Float(v) => format!("\u{1}{:.6e}", v),
            Cell::Text(text) => format!("\u{2}{text}"),
        }
    }

    fn matches(&self, other: &Cell) -> bool {
        match (self, other) {
            (Cell::Null, Cell::Null) => true,
            (Cell::Float(a), Cell::Float(b)) => {
                a == b || (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1e-300)
            }
            (Cell::Text(a), Cell::Text(b)) => a == b,
            _ => false,
        }
    }
}

type Rows = Vec<Vec<Cell>>;

fn rows_match(left: &[Vec<Cell>], right: &[Vec<Cell>]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(a, b)| a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.matches(y)))
}

fn sorted(rows: &[Vec<Cell>]) -> Rows {
    let mut rows = rows.to_vec();
    rows.sort_by_cached_key(|row| row.iter().map(Cell::sort_key).collect::<Vec<_>>());
    rows
}

#[derive(Debug)]
enum Outcome {
    Rows(Rows),
    Error(u16, String),
}

fn with_parallel_hint(sql: &str, degree: u32) -> String {
    if degree <= 1 {
        return sql.to_string();
    }
    // Hint the first SELECT block; enough to route the main pipeline through
    // the parallel operators the planner supports for it.
    let lower = sql.to_ascii_lowercase();
    let mut search = 0;
    while let Some(found) = lower[search..].find("select") {
        let at = search + found;
        let before_ok = at == 0 || !lower.as_bytes()[at - 1].is_ascii_alphanumeric();
        let after_ok = lower
            .as_bytes()
            .get(at + 6)
            .is_none_or(|b| !b.is_ascii_alphanumeric() && *b != b'_');
        if before_ok && after_ok {
            return format!(
                "{} /*+ PARALLEL({degree}) */{}",
                &sql[..at + 6],
                &sql[at + 6..]
            );
        }
        search = at + 6;
    }
    sql.to_string()
}

fn run_query(
    engine: &StorageQueryEngine,
    session_setup: &[&str],
    sql: &str,
    config: Config,
) -> Outcome {
    let mut session = SessionContext::default();
    for statement in session_setup {
        engine
            .execute_with_session(&mut session, statement)
            .unwrap();
    }
    let sql = with_parallel_hint(sql, config.parallel);
    with_vec_mode(config.mode, || {
        match engine.execute_with_session(&mut session, &sql) {
            Ok(QueryResponse::ResultSet(result)) => Outcome::Rows(
                result
                    .rows
                    .iter()
                    .map(|row| row.iter().map(Cell::from_value).collect())
                    .collect(),
            ),
            Ok(other) => panic!("expected a result set, got {other:?}"),
            Err(err) => Outcome::Error(err.code(), err.message().to_string()),
        }
    })
}

/// Strip a trailing `LIMIT n` so ties at the limit boundary can be checked
/// against the complete ordered result.
fn without_trailing_limit(sql: &str) -> Option<(String, usize)> {
    let tokens = tokenize(sql);
    let len = tokens.len();
    if len < 2 {
        return None;
    }
    let (Token::Ident(kw), Token::Num(n)) = (&tokens[len - 2], &tokens[len - 1]) else {
        return None;
    };
    if kw != "limit" {
        return None;
    }
    let lower = sql.to_ascii_lowercase();
    let at = lower.rfind("limit")?;
    Some((sql[..at].to_string(), *n as usize))
}

/// `true` when every row of `part` occurs (as a multiset) in `whole`.
fn is_sub_multiset(part: &[Vec<Cell>], whole: &[Vec<Cell>]) -> bool {
    let mut available: Vec<bool> = vec![true; whole.len()];
    part.iter().all(|row| {
        let found = whole.iter().enumerate().find(|(i, candidate)| {
            available[*i]
                && candidate.len() == row.len()
                && candidate.iter().zip(row).all(|(a, b)| a.matches(b))
        });
        match found {
            Some((i, _)) => {
                available[i] = false;
                true
            }
            None => false,
        }
    })
}

#[derive(Default)]
struct Report {
    checked: usize,
    failures: Vec<String>,
    oracle_errors: Vec<String>,
    empty: Vec<String>,
}

fn compare_query(
    engine: &StorageQueryEngine,
    session_setup: &[&str],
    name: &str,
    sql: &str,
    report: &mut Report,
) {
    let oracle = run_query(engine, session_setup, sql, ORACLE);
    let limit = without_trailing_limit(sql);
    let oracle_sorted = match &oracle {
        Outcome::Rows(rows) => Some(sorted(rows)),
        Outcome::Error(..) => None,
    };
    // The unlimited oracle is only needed to resolve a different LIMIT tie.
    // Keep it lazy and shared across configurations: ordinary exact matches
    // must not execute and materialize an additional unbounded query.
    let oracle_full = std::cell::OnceCell::new();
    if let Outcome::Error(code, message) = &oracle {
        report
            .oracle_errors
            .push(format!("{name}: oracle error {code}: {message}"));
    }
    if matches!(&oracle, Outcome::Rows(rows) if rows.is_empty()) {
        report.empty.push(name.to_string());
    }
    for config in MATRIX {
        report.checked += 1;
        let candidate = run_query(engine, session_setup, sql, config);
        let label = format!("{name} [{:?}, parallel {}]", config.mode, config.parallel);
        match (&oracle, &candidate) {
            (Outcome::Error(a, _), Outcome::Error(b, _)) if a == b => {}
            (Outcome::Rows(expected), Outcome::Rows(actual)) => {
                // Results are compared as multisets: an ORDER BY whose keys do
                // not determine a total order may legitimately permute ties.
                if rows_match(oracle_sorted.as_ref().unwrap(), &sorted(actual)) {
                    continue;
                }
                // ORDER BY ... LIMIT may keep any rows that tie at the
                // boundary: accept a same-sized subset of the full result.
                let tie_ok = limit.is_some()
                    && expected.len() == actual.len()
                    && oracle_full
                        .get_or_init(|| {
                            let (unlimited, _) = limit.as_ref().unwrap();
                            match run_query(engine, session_setup, unlimited, ORACLE) {
                                Outcome::Rows(rows) => Some(rows),
                                Outcome::Error(..) => None,
                            }
                        })
                        .as_ref()
                        .is_some_and(|full| is_sub_multiset(actual, full));
                if !tie_ok {
                    report.failures.push(format!(
                        "{label}: result mismatch ({} oracle rows vs {} rows)\n  oracle: {:?}\n  actual: {:?}",
                        expected.len(),
                        actual.len(),
                        expected.iter().take(3).collect::<Vec<_>>(),
                        actual.iter().take(3).collect::<Vec<_>>()
                    ));
                }
            }
            (expected, actual) => report.failures.push(format!(
                "{label}: outcome mismatch\n  oracle: {}\n  actual: {}",
                describe(expected),
                describe(actual)
            )),
        }
    }
}

fn describe(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Rows(rows) => format!("{} rows", rows.len()),
        Outcome::Error(code, message) => format!("error {code}: {message}"),
    }
}

/// Queries whose oracle currently raises an error. Each entry must say why;
/// the list is expected to shrink, never to grow silently.
const KNOWN_ORACLE_ERRORS: &[(&str, &str)] = &[
    (
        "shape15",
        "non-finite VAR_POP raises ER_DATA_OUT_OF_RANGE in every mode",
    ),
    (
        "shape16",
        "non-finite STDDEV_POP raises ER_DATA_OUT_OF_RANGE in every mode",
    ),
];

// Captured from the original fixtures. Smaller inputs must not turn a query
// that exercised real output rows into a vacuous equality of empty results.
// Existing empty queries may gain coverage, so this is an upper bound.
const KNOWN_EMPTY_ORACLES: &[(&str, &str)] = &[
    ("tpch", "q02"),
    ("tpch", "q20"),
    ("tpch", "q21"),
    ("tpcds", "q03"),
    ("tpcds", "q06"),
    ("tpcds", "q08"),
    ("tpcds", "q14b"),
    ("tpcds", "q21"),
    ("tpcds", "q23b"),
    ("tpcds", "q24a"),
    ("tpcds", "q24b"),
    ("tpcds", "q30"),
    ("tpcds", "q33"),
    ("tpcds", "q34"),
    ("tpcds", "q37"),
    ("tpcds", "q39a"),
    ("tpcds", "q39b"),
    ("tpcds", "q40"),
    ("tpcds", "q41"),
    ("tpcds", "q43"),
    ("tpcds", "q44"),
    ("tpcds", "q54"),
    ("tpcds", "q56"),
    ("tpcds", "q57"),
    ("tpcds", "q58"),
    ("tpcds", "q60"),
    ("tpcds", "q64"),
    ("tpcds", "q65"),
    ("tpcds", "q72"),
    ("tpcds", "q74"),
    ("tpcds", "q81"),
    ("tpcds", "q82"),
    ("tpcds", "q91"),
];

fn finish(suite: &str, report: Report) {
    let unexpected_errors: Vec<&String> = report
        .oracle_errors
        .iter()
        .filter(|line| {
            !KNOWN_ORACLE_ERRORS
                .iter()
                .any(|(name, _)| line.starts_with(&format!("{name}:")))
        })
        .collect();
    eprintln!(
        "{suite}: {} comparisons, {} failures, {} oracle errors, {} empty oracle results: {:?}",
        report.checked,
        report.failures.len(),
        report.oracle_errors.len(),
        report.empty.len(),
        report.empty
    );
    let mut problems = report.failures.clone();
    problems.extend(unexpected_errors.into_iter().cloned());
    let family = suite.split_whitespace().next().unwrap();
    problems.extend(
        report
            .empty
            .iter()
            .filter(|name| !KNOWN_EMPTY_ORACLES.contains(&(family, name.as_str())))
            .map(|name| format!("{name}: previously nonempty oracle became empty")),
    );
    assert!(
        problems.is_empty(),
        "{suite} differential corpus found {} problem(s):\n{}",
        problems.len(),
        problems.join("\n")
    );
}

/// Statements execute on 8 MiB worker stacks in production
/// (`util::blocking_pool`); unoptimized builds use several times larger
/// frames, so the corpus thread scales the stack accordingly.
fn on_query_stack(body: impl FnOnce() + Send + 'static) {
    let stack = if cfg!(debug_assertions) { 64 } else { 8 } << 20;
    let handle = std::thread::Builder::new()
        .stack_size(stack)
        .spawn(body)
        .unwrap();
    if let Err(panic) = handle.join() {
        std::panic::resume_unwind(panic);
    }
}

const CORPUS_POOL_PAGES: u64 = 8_192;

fn open_corpus_fixture(
    dir: &std::path::Path,
    suite: &str,
    queries: &[(String, String)],
    build: fn(&StorageQueryEngine, &[(String, String)]),
    shards: usize,
) -> StorageQueryEngine {
    if shards > 1 {
        if let (Some(run), Some(root)) = (
            std::env::var_os("NEXTEST_RUN_ID"),
            std::env::var_os("MYDB_TEST_TMP_DIR"),
        ) {
            // Every shard builds the same data and statistics. Build one
            // durable image for this run and give each process a private
            // copy, with its own catalog, buffer pool and runtime state.
            let root = std::path::PathBuf::from(root)
                .join("differential-fixtures")
                .join(run);
            std::fs::create_dir_all(&root).unwrap();
            let fixture = root.join(suite);
            let lock = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(root.join(format!("{suite}.lock")))
                .unwrap();
            lock.lock().unwrap();
            if !fixture.is_dir() {
                let staging = tempfile::tempdir_in(&root).unwrap();
                let data = staging.path().join("data");
                std::fs::create_dir(&data).unwrap();
                let mut config = memory_test_storage_config();
                config.durability = crate::storage::DurabilityMode::Strict;
                config.buffer_pool_pages = CORPUS_POOL_PAGES;
                let prepare = crate::platform::fs::FixtureSyncBatch::new(&data).unwrap();
                let engine =
                    StorageQueryEngine::open_with_storage_config_without_background_workers(
                        &data, config,
                    )
                    .unwrap();
                build(&engine, queries);
                engine.shutdown().unwrap();
                drop(engine);
                prepare.finish().unwrap();
                std::fs::rename(&data, &fixture).unwrap();
            }
            drop(lock);
            crate::util::file::copy_fixture_tree(&fixture, dir).unwrap();
            return open_test_engine_with_buffer_pool_pages(dir, CORPUS_POOL_PAGES);
        }
    }
    let engine = open_test_engine_with_buffer_pool_pages(dir, CORPUS_POOL_PAGES);
    build(&engine, queries);
    engine
}

fn run_suite(
    suite: &str,
    queries: Vec<(String, String)>,
    build: fn(&StorageQueryEngine, &[(String, String)]),
    session_setup: &[&str],
    shard: usize,
    shards: usize,
) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_corpus_fixture(dir.path(), suite, &queries, build, shards);
    let filter = std::env::var("MYDB_DIFF_QUERIES").ok();
    let mut report = Report::default();
    for (index, (name, sql)) in queries.iter().enumerate() {
        if index % shards != shard {
            continue;
        }
        if let Some(filter) = &filter {
            if !filter.split(',').any(|wanted| wanted.trim() == name) {
                continue;
            }
        }
        compare_query(&engine, session_setup, name, sql, &mut report);
    }
    finish(&format!("{suite} shard {shard}/{shards}"), report);
}

const TPCDS_SESSION: &[&str] = &[
    "SET SESSION sql_mode = 'PIPES_AS_CONCAT,NO_ENGINE_SUBSTITUTION'",
    "SET SESSION div_precision_increment = 20",
];

fn tpch_queries() -> Vec<(String, String)> {
    load_queries("scripts/tpch/queries")
}

fn tpcds_queries() -> Vec<(String, String)> {
    load_queries("scripts/tpcds/queries")
}

macro_rules! corpus_shards {
    ($suite:literal, $queries:ident, $build:ident, $session:expr, $shards:literal, [$($test:ident = $shard:literal),*]) => {
        $(
            #[test]
            fn $test() {
                on_query_stack(|| run_suite($suite, $queries(), $build, $session, $shard, $shards));
            }
        )*
    };
}

corpus_shards!(
    "tpch",
    tpch_queries,
    build_tpch,
    &[],
    2,
    [tpch_0 = 0, tpch_1 = 1]
);
corpus_shards!(
    "tpcds",
    tpcds_queries,
    build_tpcds,
    TPCDS_SESSION,
    6,
    [
        tpcds_0 = 0,
        tpcds_1 = 1,
        tpcds_2 = 2,
        tpcds_3 = 3,
        tpcds_4 = 4,
        tpcds_5 = 5
    ]
);

// ---------------------------------------------------------------------------
// Hand-written shape queries over a small table pair with NULLs and duplicates
// ---------------------------------------------------------------------------

fn build_shapes(engine: &StorageQueryEngine, _queries: &[(String, String)]) {
    for statement in [
        "CREATE TABLE sa (id INT PRIMARY KEY, k INT, g VARCHAR(8), d DECIMAL(12,3), \
         f DOUBLE, s VARCHAR(40), dt DATE)",
        "CREATE TABLE sb (id INT PRIMARY KEY, k INT, g VARCHAR(8), d DECIMAL(10,1), \
         f DOUBLE, s VARCHAR(40))",
        "INSERT INTO sa SELECT n, CASE WHEN n % 11 = 0 THEN NULL ELSE n % 37 END, \
         ELT(1 + n % 5, 'a', 'b', 'c', 'd', NULL), \
         CAST((n * 7919 % 20001 - 10000) / 1000 AS DECIMAL(12,3)), \
         CASE WHEN n % 13 = 0 THEN NULL ELSE (n * 104729 % 1000) / 7.0 END, \
         CONCAT(REPEAT('x', n % 7), 'k', n % 23), \
         DATE_ADD('2020-01-01', INTERVAL n % 400 DAY) \
         FROM generate_series(1, 1025) gs(n)",
        "INSERT INTO sb SELECT n, CASE WHEN n % 7 = 0 THEN NULL ELSE n % 53 END, \
         ELT(1 + n % 4, 'a', 'c', 'e', NULL), \
         CAST((n * 31 % 2001 - 1000) / 10 AS DECIMAL(10,1)), n / 3.0, \
         CONCAT('k', n % 29) FROM generate_series(1, 513) gs(n)",
        "ALTER TABLE sb ADD INDEX idx_sb_k (k)",
        "ANALYZE TABLE sa",
        "ANALYZE TABLE sb",
    ] {
        engine
            .execute(statement)
            .unwrap_or_else(|err| panic!("{err}\n{statement}"));
    }
}

fn shape_queries() -> Vec<(String, String)> {
    [
        "SELECT a.id, b.id FROM sa a FULL JOIN sb b ON a.k = b.k AND a.g = b.g WHERE COALESCE(a.id, b.id) % 97 = 1",
        "SELECT a.k, COUNT(b.id), SUM(b.d) FROM sa a LEFT JOIN sb b ON a.k = b.k GROUP BY a.k",
        "SELECT b.k, COUNT(*) FROM sa a RIGHT JOIN sb b ON a.k = b.k AND a.d > b.d GROUP BY b.k",
        "SELECT COUNT(*) FROM sa WHERE k NOT IN (SELECT k FROM sb)",
        "SELECT COUNT(*) FROM sa WHERE k NOT IN (SELECT k FROM sb WHERE k IS NOT NULL)",
        "SELECT COUNT(*) FROM sa a WHERE NOT EXISTS (SELECT 1 FROM sb b WHERE b.k = a.k AND b.g = a.g)",
        "SELECT COUNT(*) FROM sa a WHERE EXISTS (SELECT 1 FROM sb b WHERE b.k = a.k AND b.d < a.d)",
        "SELECT g, k % 3, SUM(d), COUNT(DISTINCT s), GROUPING(g), GROUPING(k % 3) FROM sa GROUP BY g, k % 3 WITH ROLLUP",
        "SELECT g, k % 4 AS m, AVG(d), MIN(s), MAX(dt) FROM sa GROUP BY GROUPING SETS ((g), (k % 4), ())",
        "SELECT id, g, SUM(d) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING), \
         RANK() OVER (PARTITION BY g ORDER BY k), LAG(s, 2) OVER (ORDER BY id) FROM sa WHERE id % 5 = 0",
        "SELECT g, k, CUME_DIST() OVER (PARTITION BY g ORDER BY k), \
         AVG(f) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sa",
        "SELECT a.id, (SELECT MAX(b.d) FROM sb b WHERE b.k = a.k) FROM sa a WHERE a.id % 9 = 0",
        "SELECT a.g, (SELECT COUNT(*) FROM sb b WHERE b.g = a.g AND b.d > a.d) FROM sa a WHERE a.id < 200",
        "SELECT g, GROUP_CONCAT(DISTINCT k ORDER BY k SEPARATOR ','), SUM(d), COUNT(*) FROM sa GROUP BY g",
        "SELECT k % 5, VAR_POP(f), STDDEV_SAMP(d), VARIANCE(k), BIT_XOR(k) FROM sa GROUP BY k % 5",
        "SELECT VAR_POP(v) FROM (SELECT 1e300 * id AS v FROM sa) t",
        "SELECT STDDEV_POP(v) FROM (SELECT CASE WHEN id % 2 = 0 THEN 1e308 ELSE -1e308 END AS v FROM sa) t",
        "SELECT a.id, x.c FROM sa a, LATERAL (SELECT COUNT(*) AS c FROM sb b WHERE b.k = a.k) x WHERE a.id < 60",
        "WITH c AS (SELECT k, SUM(d) AS t FROM sa GROUP BY k) SELECT c1.k, c1.t, c2.t FROM c c1 JOIN c c2 ON c1.k = c2.k + 1",
        "SELECT k FROM sa INTERSECT SELECT k FROM sb",
        "SELECT k, g FROM sa EXCEPT SELECT k, g FROM sb",
        "SELECT g, s FROM sa UNION SELECT g, s FROM sb",
        "SELECT DISTINCT g, k % 6 FROM sa ORDER BY 1, 2",
        "SELECT id, d FROM sa ORDER BY d DESC, id LIMIT 25",
        "SELECT SUM(d * f), SUM(d) / COUNT(*), AVG(d + 0.0005) FROM sa",
        "SELECT a.k, SUM(a.d) FROM sa a JOIN sb b ON a.k <=> b.k GROUP BY a.k",
        "SELECT g, k FROM sa WHERE (k > 30 OR g = 'b') AND s LIKE 'xx%' ORDER BY id",
        "SELECT CASE WHEN d > 0 THEN 'p' WHEN d < 0 THEN 'n' END AS sign, COUNT(*), SUM(f) FROM sa GROUP BY 1",
        "SELECT YEAR(dt), MONTH(dt), COUNT(*) FROM sa WHERE dt BETWEEN '2020-03-01' AND '2020-09-30' GROUP BY 1, 2",
        // Aggregate rows sorted by an aggregate: serial, parallel sort and
        // parallel TopK must refine ORDER BY peers identically.
        "SELECT k, SUM(d) AS v FROM sa GROUP BY k ORDER BY v DESC",
        "SELECT g, k % 4, COUNT(*) AS c FROM sa GROUP BY g, k % 4 ORDER BY c DESC LIMIT 7",
        // Join residuals with uncorrelated IN subqueries (text sets compare
        // under the column collation, including case-insensitive matches)
        // and a scalar subquery that must stay lazily evaluated.
        "SELECT COUNT(*), SUM(a.d) FROM sa a JOIN sb b ON a.k = b.k AND (a.g = 'a' OR b.s IN (SELECT UPPER(s) FROM sb WHERE id < 40))",
        "SELECT a.g, COUNT(*) FROM sa a JOIN sb b ON a.k = b.k AND (a.s IN (SELECT s FROM sa WHERE id % 17 = 0) OR b.d > 50) GROUP BY a.g",
        "SELECT COUNT(*) FROM sa a JOIN sb b ON a.k = b.k AND a.id NOT IN (SELECT id FROM sb WHERE k IS NOT NULL)",
        "SELECT COUNT(*) FROM sa a JOIN sb b ON a.k = b.k AND a.d > (SELECT AVG(d) FROM sb)",
    ]
    .iter()
    .enumerate()
    .map(|(index, sql)| (format!("shape{index:02}"), sql.to_string()))
    .collect()
}

#[test]
fn shapes() {
    on_query_stack(|| run_suite("shapes", shape_queries(), build_shapes, &[], 0, 1));
}

// ---------------------------------------------------------------------------
// Harness self-tests
// ---------------------------------------------------------------------------

#[test]
fn harness_parses_schemas_and_queries() {
    let tpch = parse_schema(&read_repo_file("scripts/tpch/schema.sql"));
    assert_eq!(tpch.len(), 8);
    assert!(tpch
        .iter()
        .find(|t| t.name == "lineitem")
        .unwrap()
        .columns
        .iter()
        .any(|c| c.name == "l_shipdate" && matches!(c.ty, ColType::Date)));
    let tpcds = parse_schema(&read_repo_file("scripts/tpcds/schema.sql"));
    assert!(tpcds.len() >= 24);
    let date_dim = tpcds.iter().find(|t| t.name == "date_dim").unwrap();
    assert_eq!(date_dim.primary_key, vec!["d_date_sk".to_string()]);
    assert!(tpch_queries().len() >= 22);
    assert!(tpcds_queries().len() >= 99);
    assert_eq!(
        without_trailing_limit("select a from t order by a limit 100"),
        Some(("select a from t order by a ".to_string(), 100))
    );
    assert_eq!(
        with_parallel_hint("with x as (select 1) select * from x", 4),
        "with x as (select /*+ PARALLEL(4) */ 1) select * from x"
    );
    let known: BTreeSet<String> = ["r_name", "ca_zip", "d_year"]
        .into_iter()
        .map(String::from)
        .collect();
    let vocab = scan_vocabulary(
        &[
            "where r_name = 'EUROPE' and substr(ca_zip,1,5) in ('85669','86197') and d_year between 1999 and 2001"
                .to_string(),
        ],
        &known,
    );
    assert!(vocab["r_name"].strings.contains("EUROPE"));
    assert!(vocab["ca_zip"].strings.contains("86197"));
    assert_eq!(vocab["d_year"].numbers, vec![1999.0, 2001.0]);
}

/// The OR-EXISTS rewrite turns its alternatives into a UNION derived table.
/// EXPLAIN ANALYZE must materialize that set-operation body before running
/// the owner (it previously failed to resolve the synthetic table name).
#[test]
fn explain_analyze_materializes_set_operation_derived_tables() {
    on_query_stack(|| {
        std::fs::create_dir_all("/tmp/mydb").unwrap();
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine_with_buffer_pool_pages(dir.path(), 8_192);
        build_shapes(&engine, &[]);
        let query = "SELECT COUNT(*) FROM sa a WHERE EXISTS (SELECT * FROM sb b WHERE b.k = a.k AND b.d > 0) \
                     OR EXISTS (SELECT * FROM sb c WHERE c.k = a.k AND c.d < 0)";
        let QueryResponse::ResultSet(explained) = engine
            .execute(&format!("EXPLAIN ANALYZE {query}"))
            .unwrap_or_else(|err| panic!("EXPLAIN ANALYZE failed: {err}"))
        else {
            panic!("EXPLAIN ANALYZE must return a result set");
        };
        let text = format!("{:?}", explained.rows);
        // The owner runs through dispatch (a lossy shape), so the display
        // tree carries no per-node actuals; the statement must still run.
        assert!(text.contains("Query I/O"), "{text}");
        assert!(text.contains("__sj_exists_or_"), "{text}");
        assert!(engine.execute(query).is_ok());
    });
}
