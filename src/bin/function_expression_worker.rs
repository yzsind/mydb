//! JSON-lines protocol worker for the external function/expression benchmark.
//! Timed runs only drain results; correctness capture is a separate operation.

use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder, Value};
use serde::Deserialize;
use serde_json::{json, Value as Json};

#[derive(Deserialize)]
struct Request {
    sql: String,
    capture: Option<String>,
    #[serde(default)]
    prepared: bool,
    #[serde(default)]
    params: Vec<i64>,
}

fn cell(value: &Value) -> Json {
    match value {
        Value::NULL => Json::Null,
        Value::Bytes(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => Json::String(text.to_owned()),
            Err(_) => json!({"binary": bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()}),
        },
        Value::Int(v) => Json::String(v.to_string()),
        Value::UInt(v) => Json::String(v.to_string()),
        Value::Float(v) => Json::String(v.to_string()),
        Value::Double(v) => Json::String(v.to_string()),
        Value::Date(..) | Value::Time(..) => Json::String(value.as_sql(false)),
    }
}

fn run(conn: &mut Conn, request: Request) -> Result<Json, Box<dyn std::error::Error>> {
    let mut output = request.capture.map(File::create).transpose()?.map(BufWriter::new);
    let mut rows = 0_u64;
    let mut bytes = 0_u64;
    let mut sample = Vec::new();
    let mut columns = Vec::new();
    let start = Instant::now();
    // Both branches use the same drain/capture logic. Prepared statements are
    // reserved for the explicit parameter-expression cases.
    macro_rules! consume {
        ($result:expr) => {{
            let result = $result;
            for row in result {
                let row = row?;
                if columns.is_empty() {
                    columns = row.columns_ref().iter().map(|c| json!({
                        "name": c.name_str(), "type": format!("{:?}", c.column_type()),
                        "flags": c.flags().bits(), "charset": c.character_set(),
                        "decimals": c.decimals(), "length": c.column_length(),
                    })).collect();
                }
                rows += 1;
                for value in row.as_ref(0).into_iter() {
                    if let Value::Bytes(value) = value { bytes += value.len() as u64; }
                }
                if output.is_some() || sample.len() < 4 {
                    let values: Vec<Json> = row.unwrap().iter().map(cell).collect();
                    if sample.len() < 4 { sample.push(values.clone()); }
                    if let Some(writer) = output.as_mut() {
                        let key = values.first().and_then(Json::as_str).unwrap_or("0");
                        write!(writer, "{key}\t")?;
                        serde_json::to_writer(&mut *writer, &values)?;
                        writeln!(writer)?;
                    }
                }
            }
        }};
    }
    if request.prepared {
        consume!(conn.exec_iter(&request.sql, request.params)?);
    } else {
        consume!(conn.query_iter(&request.sql)?);
    }
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    if let Some(writer) = output.as_mut() { writer.flush()?; }
    Ok(json!({"status":"ok", "elapsed_ms":elapsed_ms, "rows":rows,
        "first_column_bytes":bytes, "sample":sample, "columns":columns}))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = std::env::args().nth(1).ok_or("port required")?.parse()?;
    let database = std::env::args().nth(2);
    let opts = OptsBuilder::new().ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port).user(Some("root"))
        .pass(Some(std::env::var("FEB_PASSWORD").unwrap_or_default()))
        .db_name(database).prefer_socket(false)
        .tcp_connect_timeout(Some(Duration::from_secs(5)))
        .read_timeout(Some(Duration::from_secs(600)))
        .write_timeout(Some(Duration::from_secs(30)));
    let mut conn = Conn::new(opts)?;
    let stdout = io::stdout();
    let mut writer = stdout.lock();
    writeln!(writer, "{}", json!({"event":"connected", "connection_id":conn.connection_id()}))?;
    writer.flush()?;
    for line in io::stdin().lock().lines() {
        let request: Request = serde_json::from_str(&line?)?;
        writeln!(writer, "{}", json!({"event":"started"}))?;
        writer.flush()?;
        let started = Instant::now();
        let response = match run(&mut conn, request) {
            Ok(value) => value,
            Err(error) => {
                let identity = error.downcast_ref::<mysql::Error>().and_then(|e| match e {
                    mysql::Error::MySqlError(e) => Some((e.code, e.state.clone())), _ => None,
                });
                json!({"status":"error", "elapsed_ms":started.elapsed().as_secs_f64()*1000.0,
                    "error":error.to_string(), "identity":identity})
            }
        };
        writeln!(writer, "{response}")?;
        writer.flush()?;
    }
    Ok(())
}
