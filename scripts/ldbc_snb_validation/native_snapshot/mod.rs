mod python;

use crate::{
    capture_updates, element_id, number, open_graph, publish_snapshot, scan_shards, token,
    write_io, Database, Json, Result, TableSchema, TxnView, Value, MAX_ELEMENT_ID,
};
use python::{Object, Py};
use std::collections::HashMap;
use std::ffi::{c_char, CStr};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::ptr::null_mut;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use serde_json::json;

const ABSENT: u32 = u32::MAX;
const KIND_NAMES: &[&str] = &[
    "",
    "Person",
    "Place",
    "Organisation",
    "Tag",
    "TagClass",
    "Forum",
    "Post",
    "Comment",
];
const PERSON: u8 = 1;
const PLACE: u8 = 2;
const ORGANISATION: u8 = 3;
const TAG: u8 = 4;
const TAG_CLASS: u8 = 5;
const FORUM: u8 = 6;
const POST: u8 = 7;
const COMMENT: u8 = 8;
const MESSAGE: u8 = 9;

#[repr(u8)]
#[derive(Clone, Copy)]
enum Relation {
    Creator = 1,
    Located,
    Reply,
    Knows,
    PartOf,
    Study,
    Work,
    Interest,
    Moderator,
    Member,
    Container,
    Tag,
    Type,
    Subclass,
    Likes,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn node_label(name: &str) -> io::Result<u8> {
    Ok(match name {
        "Person" => PERSON,
        "Place" => PLACE,
        "Organisation" => ORGANISATION,
        "Tag" => TAG,
        "TagClass" => TAG_CLASS,
        "Forum" => FORUM,
        "Post" => POST,
        "Comment" => COMMENT,
        "Message" => MESSAGE,
        "City" => 10,
        "Country" => 11,
        "Continent" => 12,
        "Company" => 13,
        "University" => 14,
        _ => return Err(invalid("Unknown node label in snapshot")),
    })
}

fn relation(name: &str) -> io::Result<Relation> {
    Ok(match name {
        "HAS_CREATOR" => Relation::Creator,
        "IS_LOCATED_IN" => Relation::Located,
        "REPLY_OF" => Relation::Reply,
        "KNOWS" => Relation::Knows,
        "IS_PART_OF" => Relation::PartOf,
        "STUDY_AT" => Relation::Study,
        "WORK_AT" => Relation::Work,
        "HAS_INTEREST" => Relation::Interest,
        "HAS_MODERATOR" => Relation::Moderator,
        "HAS_MEMBER" => Relation::Member,
        "CONTAINER_OF" => Relation::Container,
        "HAS_TAG" => Relation::Tag,
        "HAS_TYPE" => Relation::Type,
        "IS_SUBCLASS_OF" => Relation::Subclass,
        "LIKES" => Relation::Likes,
        _ => return Err(invalid("Unknown edge label in snapshot")),
    })
}

struct Edge {
    id: u32,
    from: u32,
    to: u32,
    property: u8,
    value: i64,
}

const EDGE_KEYS: [&str; 4] = ["creationDate", "classYear", "workFrom", "joinDate"];

#[derive(Default)]
struct Shard {
    counts: [u64; 7],
    extent: u32,
    nodes: Vec<(u32, Vec<u8>)>,
    edges: Vec<Edge>,
    node_labels: Vec<(u32, u8)>,
    edge_labels: Vec<(u32, Relation)>,
}

fn scan(db: &Database, table: &TableSchema, view: TxnView, lo: u64, hi: u64) -> io::Result<Shard> {
    let mut shard = Shard::default();
    let mut visit = |row: &[Value]| -> io::Result<()> {
        let id = element_id(&row[0])?;
        let role = number(&row[1]) as usize;
        shard.extent = shard.extent.max(id);
        // Projection metadata shares the ID sequence with graph elements.
        if matches!(role, 7 | 8) {
            return Ok(());
        }
        *shard.counts.get_mut(role).ok_or_else(|| invalid("Unknown graph storage role"))? += 1;
        match role {
            0 => {
                let Value::Json(props) = &row[5] else {
                    return Err(invalid("Node properties must be a JSON object"));
                };
                shard.nodes.push((id, props.clone()));
            }
            1 => {
                let mut property = 0;
                let mut value = 0;
                if let Some(props) = row[5].as_json().map_err(query_error)? {
                    let count = props
                        .object_len()
                        .ok_or_else(|| invalid("Edge properties must be a JSON object"))?;
                    if count != 0 {
                        for (index, name) in EDGE_KEYS.iter().enumerate() {
                            if let Some(field) = props.get(name) {
                                property = (index + 1) as u8;
                                value = field
                                    .as_i64()
                                    .ok_or_else(|| invalid("Expected an integer edge property"))?;
                                break;
                            }
                        }
                        if count != 1 || property == 0 {
                            return Err(invalid("Unexpected properties on an Interactive edge"));
                        }
                    }
                } else if !matches!(row[5], Value::Null) {
                    return Err(invalid("Edge properties must be JSON"));
                }
                shard.edges.push(Edge {
                    id,
                    from: element_id(&row[2])?,
                    to: element_id(&row[3])?,
                    property,
                    value,
                });
            }
            2 => return Err(invalid("Interactive edges must be directed")),
            3 => shard
                .node_labels
                .push((element_id(&row[2])?, node_label(token(&row[4]))?)),
            4 => shard
                .edge_labels
                .push((element_id(&row[2])?, relation(token(&row[4]))?)),
            5 | 6 => (),
            _ => unreachable!(),
        }
        Ok(())
    };
    db.scan_rows_columns_storage_streaming_range(
        table,
        table.id,
        view,
        &[0, 1, 2, 3, 5, 7],
        lo,
        hi,
        |row| visit(row).map_err(write_io),
    )
    .map_err(query_error)?;
    Ok(shard)
}

fn query_error(error: crate::QueryError) -> io::Error {
    io::Error::other(error.to_string())
}

struct Message {
    when: i64,
    body: Option<String>,
    author: u32,
    country: u32,
    forum: u32,
    parent: u32,
}

struct Node {
    kind: u8,
    id: i64,
    message: Option<Message>,
    props: Option<Json>,
}

impl Node {
    fn decode(bytes: Vec<u8>, kind: u8, extra: u8) -> io::Result<Self> {
        let value = Value::Json(bytes);
        let props = value.as_json().map_err(query_error)?.expect("JSON value");
        let id = props
            .get("id")
            .and_then(|v| v.as_i64())
            .filter(|&id| id >= 0)
            .ok_or_else(|| invalid("Node requires a nonnegative integer business ID"))?;
        let expected = match kind {
            POST | COMMENT => MESSAGE,
            PLACE | ORGANISATION => match props.get("type").and_then(|v| v.as_str()) {
                Some("city") => 10,
                Some("country") => 11,
                Some("continent") => 12,
                Some("company") => 13,
                Some("university") => 14,
                _ => return Err(invalid("Unknown node subtype")),
            },
            _ => 0,
        };
        if expected != extra {
            return Err(invalid("Incorrect secondary node label"));
        }
        if matches!(kind, POST | COMMENT) {
            let when = props
                .get("creationDate")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| invalid("Message requires a creation date"))?;
            let text = |name| -> io::Result<Option<&str>> {
                match props.get(name) {
                    None => Ok(None),
                    Some(value) if value.is_null() => Ok(None),
                    Some(value) => value
                        .as_str()
                        .map(Some)
                        .ok_or_else(|| invalid("Message body must be a string")),
                }
            };
            let body = match text("content")? {
                Some(body) if !body.is_empty() => Some(body),
                _ => text("imageFile")?,
            };
            Ok(Self {
                kind,
                id,
                message: Some(Message {
                    when,
                    body: body.map(str::to_owned),
                    author: ABSENT,
                    country: ABSENT,
                    forum: ABSENT,
                    parent: ABSENT,
                }),
                props: None,
            })
        } else {
            Ok(Self {
                kind,
                id,
                message: None,
                props: Some(props.to_serde().map_err(query_error)?),
            })
        }
    }
}

struct Keys {
    slots: Vec<u32>,
    nodes: Vec<(u8, i64)>,
}

fn slot(slots: &[u32], id: u32) -> io::Result<u32> {
    slots
        .get(id as usize)
        .copied()
        .filter(|slot| *slot != ABSENT)
        .ok_or_else(|| invalid("Snapshot edge references a missing node"))
}

struct Snapshot {
    keys: Keys,
    nodes: Vec<Node>,
    edges: Vec<Vec<Edge>>,
    relations: Vec<Option<Relation>>,
    extent: u32,
    counts: [u64; 7],
}

type DecodedShard = (Vec<(u32, Node)>, Vec<Edge>);

fn assemble(mut shards: Vec<Shard>) -> Result<Snapshot> {
    let extent = shards.iter().map(|s| s.extent).max().unwrap_or(0);
    if u64::from(extent) > MAX_ELEMENT_ID {
        return Err("Snapshot exceeds the validation identity budget".into());
    }
    let mut counts = [0; 7];
    let mut primary = vec![0; extent as usize + 1];
    let mut secondary = vec![0; extent as usize + 1];
    let mut relations = vec![None; extent as usize + 1];
    for shard in &mut shards {
        for (total, count) in counts.iter_mut().zip(shard.counts) {
            *total += count;
        }
        for (id, label) in std::mem::take(&mut shard.node_labels) {
            let map = if label < MESSAGE {
                &mut primary
            } else {
                &mut secondary
            };
            let slot = map
                .get_mut(id as usize)
                .ok_or_else(|| invalid("Node label owner exceeds the snapshot extent"))?;
            if std::mem::replace(slot, label) != 0 {
                return Err("Duplicate node label category".into());
            }
        }
        for (id, label) in std::mem::take(&mut shard.edge_labels) {
            let slot = relations
                .get_mut(id as usize)
                .ok_or_else(|| invalid("Edge label owner exceeds the snapshot extent"))?;
            if slot.replace(label).is_some() {
                return Err("Multiple labels on a snapshot edge".into());
            }
        }
    }
    if counts[1] != counts[4] {
        return Err("Snapshot edge and edge label counts differ".into());
    }

    // Each scan shard owns its properties, so compact node decoding can run
    // independently once the primary and secondary label maps are known.
    let workers = thread::available_parallelism()
        .map_or(1, usize::from)
        .min(8);
    let mut groups: Vec<Vec<_>> = (0..workers).map(|_| Vec::new()).collect();
    for (i, shard) in shards.into_iter().enumerate() {
        groups[i % workers].push((i, shard));
    }
    let mut decoded = thread::scope(|scope| -> io::Result<Vec<(usize, DecodedShard)>> {
        let handles: Vec<_> = groups
            .into_iter()
            .map(|group| {
                let primary = &primary;
                let secondary = &secondary;
                scope.spawn(move || {
                    group
                        .into_iter()
                        .map(|(index, shard)| {
                            let nodes = shard
                                .nodes
                                .into_iter()
                                .map(|(id, props)| {
                                    let kind = primary[id as usize];
                                    if kind == 0 {
                                        return Err(invalid("Node lacks a primary label"));
                                    }
                                    Ok((id, Node::decode(props, kind, secondary[id as usize])?))
                                })
                                .collect::<io::Result<Vec<_>>>()?;
                            Ok((index, (nodes, shard.edges)))
                        })
                        .collect::<io::Result<Vec<_>>>()
                })
            })
            .collect();
        let mut output = Vec::new();
        for handle in handles {
            output.extend(
                handle
                    .join()
                    .map_err(|_| io::Error::other("node decode worker panicked"))??,
            );
        }
        Ok(output)
    })?;
    decoded.sort_unstable_by_key(|(index, _)| *index);

    let mut slots = Vec::new();
    let mut nodes = Vec::with_capacity(counts[0] as usize);
    let mut node_keys = Vec::with_capacity(counts[0] as usize);
    let mut edges = Vec::with_capacity(decoded.len());
    let mut labels_seen = 0;
    for (_, (records, links)) in decoded {
        for (id, node) in records {
            let id = id as usize;
            if slots.len() <= id {
                slots.resize(id + 1, ABSENT);
            }
            if std::mem::replace(&mut slots[id], nodes.len() as u32) != ABSENT {
                return Err("Duplicate physical node identity".into());
            }
            labels_seen += 1 + u64::from(secondary[id] != 0);
            node_keys.push((node.kind, node.id));
            nodes.push(node);
        }
        edges.push(links);
    }
    if labels_seen != counts[3] {
        return Err("Snapshot has node labels without an owner".into());
    }
    Ok(Snapshot {
        keys: Keys {
            slots,
            nodes: node_keys,
        },
        nodes,
        edges,
        relations,
        extent,
        counts,
    })
}

fn py_entry(operation: impl FnOnce() -> Result<Object>) -> *mut Py {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(value)) => value.into_ptr(),
        Ok(Err(error)) => {
            python::report(error.to_string());
            null_mut()
        }
        Err(_) => {
            python::report("Native graph validation panicked".to_owned());
            null_mut()
        }
    }
}

type Capture = JoinHandle<std::result::Result<(), String>>;

unsafe fn path(pointer: *const c_char) -> PathBuf {
    Path::new(std::ffi::OsStr::from_bytes(
        CStr::from_ptr(pointer).to_bytes(),
    ))
    .to_owned()
}

#[no_mangle]
unsafe extern "C" fn snb_snapshot(
    data: *const c_char,
    out: *const c_char,
    writes: *const c_char,
    model: *mut Py,
    message_type: *mut Py,
    none: *mut Py,
    capture: *mut *mut Capture,
    keys: *mut *mut Keys,
) -> *mut Py {
    py_entry(|| {
        let (data, out, writes) = (path(data), path(out), path(writes));
        let start = Instant::now();
        let (scn, scanned, snapshot) = python::allow_threads(|| -> Result<_> {
            std::fs::create_dir(&out)?;
            let (engine, schema) = open_graph(&data)?;
            let db = engine.database_for_replication();
            let (scn, shards) = scan_shards(db, &schema, |view, _, lo, hi| {
                scan(db, &schema, view, lo, hi)
            })?;
            let scanned = start.elapsed();
            let target = out.clone();
            *capture = Box::into_raw(Box::new(
                thread::Builder::new()
                    .name("snb-commits".into())
                    .spawn(move || {
                        capture_updates(engine, schema, &target, &writes, scn, start)
                            .map_err(|e| e.to_string())
                    })?,
            ));
            Ok((scn, scanned, assemble(shards)?))
        })?;
        let assembled = start.elapsed();
        let stats = materialize(
            snapshot,
            &Object::borrow(model)?,
            &Object::borrow(message_type)?,
            &Object::borrow(none)?,
        )?;
        publish_snapshot(
            &out,
            &json!({
                "format":"native-v1", "snapshot_scn":scn, "max_element_id":stats.extent,
                "role_rows":stats.counts, "open_scan_seconds":scanned.as_secs_f64(),
                "assemble_seconds":(assembled-scanned).as_secs_f64(),
                "materialize_seconds":(start.elapsed()-assembled).as_secs_f64(),
            }),
        )?;
        let result = Object::tuple([
            &Object::int(scn as i64)?,
            &Object::int(i64::from(stats.extent))?,
        ])?;
        *keys = Box::into_raw(Box::new(stats.keys));
        Ok(result)
    })
}

#[no_mangle]
unsafe extern "C" fn snb_wait(capture: *mut Capture, none: *mut Py) -> *mut Py {
    py_entry(|| {
        python::allow_threads(|| Box::from_raw(capture).join())
            .map_err(|_| "Commit capture panicked")??;
        Ok(Object::borrow(none)?)
    })
}

#[no_mangle]
unsafe extern "C" fn snb_rebuild(
    data: *const c_char,
    snapshot_scn: u64,
    extent: u32,
    model: *mut Py,
    message_type: *mut Py,
    none: *mut Py,
    keys: *mut *mut Keys,
) -> *mut Py {
    py_entry(|| {
        let snapshot = python::allow_threads(|| -> Result<_> {
            let (engine, schema) = open_graph(&path(data))?;
            let db = engine.database_for_replication();
            let (scn, shards) = scan_shards(db, &schema, |view, _, lo, hi| {
                if view.snapshot_scn != snapshot_scn {
                    return Err(invalid(
                        "Source clone SCN differs from the verified snapshot",
                    ));
                }
                scan(db, &schema, view, lo, hi)
            })?;
            let snapshot = assemble(shards)?;
            engine.shutdown()?;
            if scn != snapshot_scn || snapshot.extent != extent {
                return Err("Source clone does not match the verified snapshot extent".into());
            }
            Ok(snapshot)
        })?;
        let stats = materialize(
            snapshot,
            &Object::borrow(model)?,
            &Object::borrow(message_type)?,
            &Object::borrow(none)?,
        )?;
        let counts = Object::list()?;
        for count in stats.counts {
            counts.append(&Object::int(count as i64)?)?;
        }
        *keys = Box::into_raw(Box::new(stats.keys));
        Ok(counts)
    })
}

#[no_mangle]
unsafe extern "C" fn snb_key(keys: *const Keys, id: u32) -> *mut Py {
    py_entry(|| {
        let keys = &*keys;
        let (kind, value) = keys.nodes[slot(&keys.slots, id)? as usize];
        Ok(Object::tuple([
            &Object::str(KIND_NAMES[kind as usize])?,
            &Object::int(value)?,
        ])?)
    })
}

#[no_mangle]
unsafe extern "C" fn snb_drop_keys(keys: *mut Keys) {
    drop(Box::from_raw(keys));
}

macro_rules! index_fields {
    ($name:ident { $($field:ident),+ $(,)? }) => {
        struct $name {
            $($field: Object),+
        }
        impl $name {
            fn from_model(model: &Object) -> io::Result<Self> {
                Ok(Self {
                    $($field: model.attr(CStr::from_bytes_with_nul(concat!(stringify!($field), "\0").as_bytes()).expect("attribute name"))?),+
                })
            }
        }
    }
}

index_fields!(ModelIndexes {
    nodes,
    messages,
    friend_dates,
    person_city,
    org_place,
    place_parent,
    study,
    work,
    interest,
    forum_moderator,
    by_tag,
    tags,
    tag_members,
    tag_subclasses,
    posts,
    comments,
    by_country,
    replies,
    received,
    reply_weights,
    liked,
    birthday,
    first_names,
});
index_fields!(RankingIndexes {
    adjacent,
    by_author,
    messages
});
index_fields!(ForumIndexes {
    memberships,
    ordered_memberships,
    posts
});

struct PreparedNative {
    keys: Keys,
    extent: u32,
    counts: [u64; 7],
}

fn increment(index: &Object, key: &Object, amount: i64) -> io::Result<()> {
    let before = index.get(key)?.map_or(Ok(0), |value| value.as_int())?;
    index.put(key, &Object::int(before + amount)?)
}

fn set(index: &Object, key: &Object, value: &Object) -> io::Result<()> {
    index.group(key, Object::set)?.add(value)
}

fn append(index: &Object, key: &Object, value: &Object) -> io::Result<()> {
    index.group(key, Object::list)?.append(value)
}

fn required_field<'a>(props: &'a Json, field: &str) -> io::Result<&'a str> {
    props[field]
        .as_str()
        .ok_or_else(|| invalid("Missing string node property"))
}

fn message_mut(node: &mut Node) -> io::Result<&mut Message> {
    node.message
        .as_mut()
        .ok_or_else(|| invalid("Expected a message endpoint"))
}

fn materialize(
    snapshot: Snapshot,
    model: &Object,
    message_type: &Object,
    none: &Object,
) -> Result<PreparedNative> {
    let Snapshot {
        keys,
        mut nodes,
        edges,
        relations,
        extent,
        counts,
    } = snapshot;
    let ix = ModelIndexes::from_model(model)?;
    let ranking = RankingIndexes::from_model(&model.attr(c"ranking")?)?;
    let forums = ForumIndexes::from_model(&model.attr(c"forums")?)?;
    let ids = nodes
        .iter()
        .map(|node| Object::int(node.id))
        .collect::<io::Result<Vec<_>>>()?;
    let tags_by_name = Object::dict()?;
    let classes_by_name = Object::dict()?;
    let countries_by_name = Object::dict()?;

    let kinds: Vec<_> = KIND_NAMES
        .iter()
        .map(|name| Object::str(name))
        .collect::<io::Result<_>>()?;
    let node_maps = kinds
        .iter()
        .map(|kind| ix.nodes.group(kind, Object::dict))
        .collect::<io::Result<Vec<_>>>()?;
    let mut likes = Vec::new();
    for edge in edges.into_iter().flatten() {
        let from = slot(&keys.slots, edge.from)?;
        let to = slot(&keys.slots, edge.to)?;
        let (a, b) = (&ids[from as usize], &ids[to as usize]);
        let rel =
            relations[edge.id as usize].ok_or_else(|| invalid("Snapshot edge lacks a label"))?;
        let expected = match rel {
            Relation::Knows | Relation::Likes => 1,
            Relation::Study => 2,
            Relation::Work => 3,
            Relation::Member => 4,
            _ => 0,
        };
        if edge.property != expected {
            return Err("Snapshot edge properties do not match the label".into());
        }
        match rel {
            Relation::Creator => message_mut(&mut nodes[from as usize])?.author = to,
            Relation::Located => match nodes[from as usize].kind {
                PERSON => ix.person_city.put(a, b)?,
                ORGANISATION => ix.org_place.put(a, b)?,
                _ => message_mut(&mut nodes[from as usize])?.country = to,
            },
            Relation::Reply => message_mut(&mut nodes[from as usize])?.parent = to,
            Relation::Container => message_mut(&mut nodes[to as usize])?.forum = from,
            Relation::Knows => {
                let when = Object::int(edge.value)?;
                ix.friend_dates.group(a, Object::dict)?.put(b, &when)?;
                ix.friend_dates.group(b, Object::dict)?.put(a, &when)?;
                set(&ranking.adjacent, a, b)?;
                set(&ranking.adjacent, b, a)?;
            }
            Relation::PartOf => ix.place_parent.put(a, b)?,
            Relation::Study | Relation::Work => {
                let index = if matches!(rel, Relation::Study) {
                    &ix.study
                } else {
                    &ix.work
                };
                append(index, a, &Object::tuple([b, &Object::int(edge.value)?])?)?;
            }
            Relation::Interest => set(&ix.interest, a, b)?,
            Relation::Moderator => ix.forum_moderator.put(a, b)?,
            Relation::Member => {
                let group = forums.memberships.group(b, Object::dict)?;
                let previous = group.get(a)?.map(|value| value.as_int()).transpose()?;
                if previous.is_none_or(|p| p < edge.value) {
                    group.put(a, &Object::int(edge.value)?)?;
                }
            }
            Relation::Tag => match nodes[from as usize].kind {
                POST => {
                    let tags = ix.tags.group(a, Object::set)?;
                    if !tags.contains(b)? {
                        tags.add(b)?;
                        append(&ix.by_tag, b, a)?;
                    }
                }
                FORUM | COMMENT => (),
                _ => return Err("Unexpected tag source in snapshot".into()),
            },
            Relation::Type => set(&ix.tag_members, b, a)?,
            Relation::Subclass => set(&ix.tag_subclasses, b, a)?,
            Relation::Likes => likes.push((from, to, edge.value)),
        }
    }
    let edges_done = Instant::now();

    for (index, node) in nodes.iter().enumerate() {
        let id = &ids[index];
        let Some(props) = node.props.as_ref() else {
            continue;
        };
        node_maps[node.kind as usize].put(id, &Object::json(props, none)?)?;
        match node.kind {
            PERSON => {
                let timestamp = props["birthday"]
                    .as_i64()
                    .ok_or_else(|| invalid("Missing birthday"))?;
                let days =
                    i32::try_from(timestamp.div_euclid(86_400_000)).map_err(io::Error::other)?;
                let date = Value::Date(days).to_display_string();
                let mut fields = date.rsplit('-');
                let part = |text: Option<&str>| -> io::Result<Object> {
                    let value: i64 = text
                        .ok_or_else(|| invalid("Invalid birthday"))?
                        .parse()
                        .map_err(io::Error::other)?;
                    Object::int(value)
                };
                let day = part(fields.next())?;
                let month = part(fields.next())?;
                ix.birthday.put(id, &Object::tuple([&month, &day])?)?;
                set(
                    &ix.first_names,
                    &Object::str(required_field(props, "firstName")?)?,
                    id,
                )?;
            }
            TAG => tags_by_name.put(&Object::str(required_field(props, "name")?)?, id)?,
            TAG_CLASS => classes_by_name.put(&Object::str(required_field(props, "name")?)?, id)?,
            PLACE if props["type"] == "country" => {
                countries_by_name.put(&Object::str(required_field(props, "name")?)?, id)?;
            }
            _ => (),
        }
    }

    let is_post = Object::bool(true)?;
    let is_comment = Object::bool(false)?;
    let mut reply_weights = HashMap::new();
    let endpoint = |index: u32| ids.get(index as usize).unwrap_or(none);
    for (index, node) in nodes.iter().enumerate() {
        let Some(message) = node.message.as_ref() else {
            continue;
        };
        let author = message.author as usize;
        let country = message.country as usize;
        if !nodes.get(author).is_some_and(|n| n.kind == PERSON)
            || !nodes.get(country).is_some_and(|n| n.kind == PLACE)
        {
            return Err(invalid("Message lacks an author or country").into());
        }
        let id = &ids[index];
        let author_id = &ids[author];
        let country_id = &ids[country];
        let when = Object::int(message.when)?;
        let body = message.body.as_deref().map(Object::str).transpose()?;
        let body = body.as_ref().unwrap_or(none);
        let post = node.kind == POST;
        let obj = message_type.call([
            &when,
            body,
            if post { &is_post } else { &is_comment },
            author_id,
            country_id,
            endpoint(message.forum),
            endpoint(message.parent),
        ])?;
        ix.messages.put(id, &obj)?;
        ranking.messages.put(id, body)?;
        let rank = Object::tuple([&when, &Object::int(-node.id)?])?;
        append(&ranking.by_author, author_id, &rank)?;
        append(
            &ix.by_country,
            &Object::tuple([author_id, country_id])?,
            &when,
        )?;
        if post {
            increment(
                &forums.posts,
                &Object::tuple([author_id, endpoint(message.forum)])?,
                1,
            )?;
            append(&ix.posts, author_id, &rank)?;
        } else {
            let parent = nodes
                .get(message.parent as usize)
                .filter(|n| n.message.is_some())
                .ok_or_else(|| invalid("Comment has no message parent"))?;
            let parent_author = parent.message.as_ref().expect("message parent").author;
            let parent_author_id = ids
                .get(parent_author as usize)
                .ok_or_else(|| invalid("Comment parent lacks an author"))?;
            append(&ix.comments, author_id, id)?;
            append(&ix.replies, endpoint(message.parent), id)?;
            append(&ix.received, parent_author_id, &rank)?;
            let (a, b) = (message.author, parent_author);
            let pair = (a.min(b), a.max(b));
            *reply_weights.entry(pair).or_insert(0) += if parent.kind == POST { 2 } else { 1 };
        }
    }

    for ((a, b), weight) in reply_weights {
        let (a, b) = if nodes[a as usize].id <= nodes[b as usize].id {
            (a, b)
        } else {
            (b, a)
        };
        ix.reply_weights.put(
            &Object::tuple([endpoint(a), endpoint(b)])?,
            &Object::int(weight)?,
        )?;
    }
    for (person, message, when) in likes {
        let liked = &nodes[message as usize];
        let owner = liked
            .message
            .as_ref()
            .and_then(|m| ids.get(m.author as usize))
            .ok_or_else(|| invalid("Liked message lacks an author"))?;
        let liker = endpoint(person);
        let group = ix.liked.group(owner, Object::dict)?;
        let rank = (when, -liked.id);
        let previous = group
            .get(liker)?
            .map(|v| Ok::<_, io::Error>((v.int_item(0)?, v.int_item(1)?)))
            .transpose()?;
        if previous.is_none_or(|previous| previous < rank) {
            group.put(
                liker,
                &Object::tuple([&Object::int(rank.0)?, &Object::int(rank.1)?])?,
            )?;
        }
    }
    eprintln!(
        "native message and likes indexes: {:.3}s",
        edges_done.elapsed().as_secs_f64()
    );

    for index in [&ix.posts, &ix.received, &ix.by_country, &ranking.by_author] {
        index.each(|_, group| group.sort())?;
    }
    forums.memberships.each(|author, forums_for_author| {
        let ordered = Object::list()?;
        forums_for_author.each(|forum, when| ordered.append(&Object::tuple([when, forum])?))?;
        ordered.sort()?;
        forums.ordered_memberships.put(author, &ordered)
    })?;
    model.set_attr(c"tags_by_name", &tags_by_name)?;
    model.set_attr(c"classes_by_name", &classes_by_name)?;
    model.set_attr(c"country_by_name", &countries_by_name)?;
    model.set_attr(c"loading", &is_comment)?;
    Ok(PreparedNative {
        keys,
        extent,
        counts,
    })
}
