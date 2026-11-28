use super::{
    element_id, number, publish_snapshot, scan_shards, token, write_io, Database, Result,
    TableSchema, TxnView, Value, MAX_ELEMENT_ID,
};
use serde_json::json;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

struct SnapshotLabels {
    dictionary: Vec<String>,
    codes: HashMap<String, u8>,
    first_node: Vec<u8>,
    second_node: Vec<u8>,
    edges: Vec<u8>,
}

impl SnapshotLabels {
    fn new() -> Self {
        Self {
            dictionary: vec![String::new()],
            codes: HashMap::new(),
            first_node: Vec::new(),
            second_node: Vec::new(),
            edges: Vec::new(),
        }
    }

    fn record(&mut self, node: bool, id: u32, code: u8) -> io::Result<()> {
        let first = if node {
            &mut self.first_node
        } else {
            &mut self.edges
        };
        let slot = label_slot(first, id as usize);
        if *slot == 0 {
            *slot = code;
            return Ok(());
        }
        if !node || *slot == code {
            return Err(io::Error::other("unexpected graph label multiplicity"));
        }
        let slot = label_slot(&mut self.second_node, id as usize);
        if *slot != 0 {
            return Err(io::Error::other("unexpected third node label"));
        }
        *slot = code;
        Ok(())
    }

    fn write(&mut self, out: &Path, max_id: u64) -> io::Result<()> {
        if max_id > MAX_ELEMENT_ID {
            return Err(io::Error::other("element ID exceeds the validation budget"));
        }
        for (name, labels) in [
            ("node-labels-first.bin", &mut self.first_node),
            ("node-labels-second.bin", &mut self.second_node),
            ("edge-labels.bin", &mut self.edges),
        ] {
            if labels.len() > max_id as usize + 1 {
                return Err(io::Error::other("label points past the graph extent"));
            }
            labels.resize(max_id as usize + 1, 0);
            fs::write(out.join(name), labels)?;
        }
        Ok(())
    }
}

fn intern(
    dictionary: &mut Vec<String>,
    codes: &mut HashMap<String, u8>,
    label: &str,
) -> io::Result<u8> {
    if let Some(&code) = codes.get(label) {
        return Ok(code);
    }
    let code = u8::try_from(dictionary.len())
        .map_err(|_| io::Error::other("too many distinct graph labels"))?;
    codes.insert(label.to_owned(), code);
    dictionary.push(label.to_owned());
    Ok(code)
}

fn label_slot(slots: &mut Vec<u8>, id: usize) -> &mut u8 {
    if id >= slots.len() {
        slots.resize(id + 1, 0);
    }
    &mut slots[id]
}

fn shard_output(out: &Path, name: &str) -> io::Result<BufWriter<File>> {
    Ok(BufWriter::with_capacity(
        1024 * 1024,
        File::create(out.join(name))?,
    ))
}

struct JsonBatches {
    output: BufWriter<File>,
    count: usize,
}

impl JsonBatches {
    fn new(out: &Path, name: &str) -> io::Result<Self> {
        Ok(Self {
            output: shard_output(out, name)?,
            count: 0,
        })
    }

    fn record(
        &mut self,
        write_value: impl FnOnce(&mut BufWriter<File>) -> io::Result<()>,
    ) -> io::Result<()> {
        const BATCH: usize = 1024;
        self.output
            .write_all(if self.count == 0 { b"[" } else { b"," })?;
        write_value(&mut self.output)?;
        self.count += 1;
        if self.count == BATCH {
            self.output.write_all(b"]\n")?;
            self.count = 0;
        }
        Ok(())
    }

    fn finish(mut self) -> io::Result<()> {
        if self.count != 0 {
            self.output.write_all(b"]\n")?;
        }
        self.output.flush()
    }
}

struct ExportShard {
    counts: [u64; 7],
    max_id: u32,
    max_node_id: u32,
    dictionary: Vec<String>,
    labels: Vec<(u32, u8, bool)>,
}

fn export_shard(
    db: &Database,
    table: &TableSchema,
    view: TxnView,
    out: &Path,
    shard: usize,
    page_lo: u64,
    page_hi: u64,
) -> io::Result<ExportShard> {
    let mut nodes = JsonBatches::new(out, &format!("nodes-{shard}.jsonl"))?;
    let mut edges = shard_output(out, &format!("edges-{shard}.bin"))?;
    let mut properties = JsonBatches::new(out, &format!("edge-props-{shard}.jsonl"))?;
    let mut counts = [0; 7];
    let mut max_id = 0;
    let mut max_node_id = 0;
    let mut dictionary = vec![String::new()];
    let mut codes = HashMap::new();
    let mut labels = Vec::new();
    let mut visit = |v: &[Value]| -> io::Result<()> {
        let id = element_id(&v[0])?;
        let role = number(&v[1]) as usize;
        max_id = max_id.max(id);
        // Projection metadata shares the ID sequence with graph elements.
        if matches!(role, 7 | 8) {
            return Ok(());
        }
        *counts.get_mut(role).ok_or_else(|| io::Error::other("Unknown graph storage role"))? += 1;
        match role {
            0 => {
                max_node_id = max_node_id.max(id);
                nodes.record(|output| write!(output, "[{id},{}]", v[5].to_display_string()))
            }
            1 | 2 => {
                let source = element_id(&v[2])?;
                let target = element_id(&v[3])?;
                let json = if matches!(v[5], Value::Null) {
                    String::new()
                } else {
                    v[5].to_display_string()
                };
                let has_properties = !json.is_empty() && json != "{}";
                // Each flag consumes one entry from the separate property stream.
                let mut record = [0u8; 16];
                record[..4].copy_from_slice(&id.to_le_bytes());
                record[4..8].copy_from_slice(&source.to_le_bytes());
                record[8..12].copy_from_slice(&target.to_le_bytes());
                let flags = (role as u32) << 1 | u32::from(has_properties);
                record[12..].copy_from_slice(&flags.to_le_bytes());
                edges.write_all(&record)?;
                if has_properties {
                    properties.record(|output| output.write_all(json.as_bytes()))?;
                }
                Ok(())
            }
            3 | 4 => {
                let owner = element_id(&v[2])?;
                let code = intern(&mut dictionary, &mut codes, token(&v[4]))?;
                labels.push((owner, code, role == 3));
                Ok(())
            }
            5 | 6 => Ok(()),
            _ => unreachable!(),
        }
    };
    db.scan_rows_columns_storage_streaming_range(
        table,
        table.id,
        view,
        &[0, 1, 2, 3, 5, 7],
        page_lo,
        page_hi,
        |row| visit(row).map_err(write_io),
    )
    .map_err(|e| io::Error::other(e.to_string()))?;
    nodes.finish()?;
    edges.flush()?;
    properties.finish()?;
    Ok(ExportShard {
        counts,
        max_id,
        max_node_id,
        dictionary,
        labels,
    })
}

pub(super) fn export(db: &Arc<Database>, table: &TableSchema, out: &Path) -> Result<u64> {
    let start = Instant::now();
    let (scn, shards) = scan_shards(db, table, |view, shard, lo, hi| {
        export_shard(db, table, view, out, shard, lo, hi)
    })?;
    let scanned = start.elapsed();
    let mut labels = SnapshotLabels::new();
    let mut counts = [0; 7];
    let mut max_id = 0;
    let mut max_node_id = 0;
    let shard_count = shards.len();
    for shard in shards {
        max_id = max_id.max(shard.max_id);
        max_node_id = max_node_id.max(shard.max_node_id);
        for (total, count) in counts.iter_mut().zip(shard.counts) {
            *total += count;
        }
        let mut remap = vec![0];
        for label in &shard.dictionary[1..] {
            remap.push(intern(&mut labels.dictionary, &mut labels.codes, label)?);
        }
        for (owner, code, node) in shard.labels {
            labels.record(node, owner, remap[code as usize])?;
        }
    }
    labels.write(out, u64::from(max_id))?;
    let meta = json!({ "snapshot_scn": scn, "max_element_id":max_id,
        "max_node_id": max_node_id, "role_rows":counts, "format": "sharded-v1",
        "label_dictionary":labels.dictionary, "shards": shard_count,
        "scan_seconds": scanned.as_secs_f64(), "label_seconds": (start.elapsed() - scanned).as_secs_f64() });
    publish_snapshot(out, &meta)?;
    Ok(scn)
}
