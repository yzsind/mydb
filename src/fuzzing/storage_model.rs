//! Bounded transaction sequences checked against an independent committed map.
use super::{
    sql_execution::{query, Fixture},
    FuzzBytes,
};
use crate::engine::SessionContext;
use crate::storage::DurabilityMode;
use crate::types::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
struct Row {
    k: Option<i64>,
    bytes: Vec<u8>,
}

type Model = BTreeMap<i64, Row>;

fn payload(seed: u64, size: usize) -> Vec<u8> {
    let mut state = seed | 1;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn check(fixture: &Fixture, session: &mut SessionContext, model: &Model, trace: &[String]) {
    let engine = fixture.engine.as_ref().unwrap();
    let expected: Vec<_> = model
        .iter()
        .map(|(&id, r)| {
            vec![
                Value::Int(id),
                r.k.map(Value::Int).unwrap_or(Value::Null),
                Value::Bytes(r.bytes.clone()),
            ]
        })
        .collect();
    for (hint, predicate) in [
        ("IGNORE INDEX(by_k)", "1"),
        ("FORCE INDEX(by_k)", "k IS NULL OR k>=-128"),
    ] {
        let sql = format!("SELECT id,k,body FROM model_rows {hint} WHERE {predicate} ORDER BY id");
        let actual = query(engine, session, &sql);
        assert_eq!(actual.1, expected, "model mismatch after {trace:?}; {sql}");
    }
}

pub fn storage_model(bytes: &[u8]) {
    let mut input = FuzzBytes::new(bytes);
    let buffered = if input.next() & 1 == 0 { "0" } else { "1" };
    let _env = crate::util::env::scoped_vars(&[("MYDB_WAL_TXN_BUFFER", Some(buffered))]);
    let mut fixture = Fixture::with_durability(DurabilityMode::Strict);
    fixture.engine.as_ref().unwrap().execute("CREATE TABLE model_rows(id INT PRIMARY KEY,k INT,u BIGINT NOT NULL,body LONGBLOB,UNIQUE KEY by_u(u),KEY by_k(k,id))").unwrap();
    fixture
        .engine
        .as_ref()
        .unwrap()
        .database_for_replication()
        .fuzz_lob_chain("test.model_rows", bytes);
    let mut session = SessionContext::default();
    let mut committed = Model::new();
    let mut trace = Vec::new();
    // These mandatory transitions cannot disappear as mutations shorten input.
    for round in 0..4 {
        let mut pending = committed.clone();
        fixture
            .engine
            .as_ref()
            .unwrap()
            .execute_with_session(&mut session, "BEGIN")
            .unwrap();
        let mut saved = None;
        for step in 0..12 {
            let tag = input.next();
            let id = if step == 0 {
                round * 16
            } else {
                (input.next() % 64) as i64
            };
            let opcode = if step == 0 { 0 } else { tag % 7 };
            let engine = fixture.engine.as_ref().unwrap();
            let sql = match opcode {
                0 | 1 => {
                    let row = Row {
                        k: if tag & 8 == 0 {
                            None
                        } else {
                            Some((tag as i8 % 9) as i64)
                        },
                        bytes: payload(
                            input.u64(),
                            if step == 0 { 35_000 } else { tag as usize * 3 },
                        ),
                    };
                    let hex = row
                        .bytes
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>();
                    let sql = format!(
                        "INSERT INTO model_rows VALUES({id},{},{id},X'{hex}')",
                        row.k.map(|k| k.to_string()).unwrap_or("NULL".into())
                    );
                    let result = engine.execute_with_session(&mut session, &sql);
                    if pending.contains_key(&id) {
                        assert_eq!(result.unwrap_err().code(), 1062);
                    } else {
                        result.unwrap();
                        pending.insert(id, row);
                    }
                    format!("insert {id}")
                }
                2 => {
                    let k = (input.next() as i8 % 9) as i64;
                    engine
                        .execute_with_session(
                            &mut session,
                            &format!("UPDATE model_rows SET k={k} WHERE id={id}"),
                        )
                        .unwrap();
                    if let Some(row) = pending.get_mut(&id) {
                        row.k = Some(k);
                    }
                    format!("update {id} {k}")
                }
                3 => {
                    engine
                        .execute_with_session(
                            &mut session,
                            &format!("DELETE FROM model_rows WHERE id={id}"),
                        )
                        .unwrap();
                    pending.remove(&id);
                    format!("delete {id}")
                }
                4 => {
                    engine
                        .execute_with_session(&mut session, "SAVEPOINT model_sp")
                        .unwrap();
                    saved = Some(pending.clone());
                    "savepoint".into()
                }
                5 if saved.is_some() => {
                    engine
                        .execute_with_session(&mut session, "ROLLBACK TO SAVEPOINT model_sp")
                        .unwrap();
                    pending = saved.as_ref().unwrap().clone();
                    "rollback-to".into()
                }
                _ => "read".into(),
            };
            trace.push(sql);
            check(&fixture, &mut session, &pending, &trace);
        }
        // A mandatory savepoint path remains covered even for an empty input.
        fixture
            .engine
            .as_ref()
            .unwrap()
            .execute_with_session(&mut session, "SAVEPOINT required_sp")
            .unwrap();
        fixture
            .engine
            .as_ref()
            .unwrap()
            .execute_with_session(&mut session, "UPDATE model_rows SET k=127")
            .unwrap();
        fixture
            .engine
            .as_ref()
            .unwrap()
            .execute_with_session(&mut session, "ROLLBACK TO SAVEPOINT required_sp")
            .unwrap();
        check(&fixture, &mut session, &pending, &trace);
        let commit = round % 2 == 0;
        fixture
            .engine
            .as_ref()
            .unwrap()
            .execute_with_session(&mut session, if commit { "COMMIT" } else { "ROLLBACK" })
            .unwrap();
        if commit {
            committed = pending;
        }
        trace.push(if commit { "commit" } else { "rollback" }.into());
        check(&fixture, &mut session, &committed, &trace);
        fixture
            .engine
            .as_ref()
            .unwrap()
            .execute("ALTER SYSTEM CHECKPOINT")
            .unwrap();
        fixture.reopen();
        session = SessionContext::default();
        trace.push("checkpoint/reopen".into());
        check(&fixture, &mut session, &committed, &trace);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn stateful_storage_commits_rollbacks_and_reopen() {
        for seed in [0u8, 1, 42, 255] {
            super::storage_model(
                &(0..512)
                    .map(|i| seed.wrapping_add((i * 37) as u8))
                    .collect::<Vec<_>>(),
            );
        }
    }
}
