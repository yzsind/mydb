//! Stock MySQL-driver checks for COM_STMT_PREPARE/EXECUTE and RRF rebinding.
use mysql::{prelude::Queryable, Conn, OptsBuilder, Value};
use std::{
    env,
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut conn = Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some(
                env::var("MYDB_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            ))
            .tcp_port(
                env::var("MYDB_PORT")
                    .unwrap_or_else(|_| "8738".into())
                    .parse()?,
            )
            .user(Some(
                env::var("MYDB_USER").unwrap_or_else(|_| "root".into()),
            ))
            .pass(Some(
                env::var("MYDB_PASSWORD").unwrap_or_else(|_| "root".into()),
            ))
            .prefer_socket(false)
            .read_timeout(Some(Duration::from_secs(30))),
    )?;
    let database = format!(
        "hybrid_wire_{}_{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros()
    );
    conn.query_drop(format!("CREATE DATABASE {database}"))?;
    let result = (|| -> Result<(), Box<dyn Error>> {
        conn.query_drop(format!("USE {database}"))?;
        conn.query_drop("CREATE TABLE docs(id INT PRIMARY KEY,body TEXT,v VECTOR(2) NOT NULL,FULLTEXT KEY ft(body))")?;
        conn.query_drop("INSERT INTO docs VALUES(1,'database systems','[0,0]'),(2,'database rollback','[1,0]'),(3,'other topic','[2,0]')")?;
        conn.query_drop("CREATE INDEX hv ON docs(v) USING HNSW")?;
        let stmt=conn.prep("SELECT id,rrf_score,best_rank,match_count,rrf_details FROM ((SELECT id FROM docs WHERE MATCH(body) AGAINST(?) ORDER BY MATCH(body) AGAINST(?) DESC,id LIMIT ?) AS lexical RRF (SELECT /*+ HNSW_EF(64) */ id FROM docs ORDER BY L2_DISTANCE(v,TO_VECTOR(?)),id LIMIT ?) AS semantic WITH(RANK_CONSTANT=?,WEIGHTS=(?,?),SCORE_DETAILS=TRUE)) h ORDER BY rrf_score DESC,best_rank,id LIMIT ?")?;
        assert_eq!(stmt.num_params(), 9);
        assert_eq!(stmt.columns().len(), 5);
        for (vector, expected) in [("[0,0]", 1), ("[2,0]", 3)] {
            let rows: Vec<mysql::Row> = conn.exec(
                &stmt,
                vec![
                    Value::from("database"),
                    Value::from("database"),
                    Value::from(3u64),
                    Value::from(vector),
                    Value::from(3u64),
                    Value::from(0u64),
                    Value::from(0.0f64),
                    Value::from(1.0f64),
                    Value::from(1u64),
                ],
            )?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get::<i64, _>(0), Some(expected));
            assert_eq!(rows[0].get::<f64, _>(1), Some(1.0));
            assert_eq!(rows[0].get::<u64, _>(2), Some(1));
            assert_eq!(rows[0].get::<u64, _>(3), Some(1));
            assert!(rows[0]
                .get::<String, _>(4)
                .unwrap()
                .contains("contribution"));
        }
        let invalid = conn.exec::<mysql::Row, _, _>(
            &stmt,
            vec![
                Value::from("database"),
                Value::from("database"),
                Value::from(3u64),
                Value::from("[0,0]"),
                Value::from(3u64),
                Value::from(0u64),
                Value::from(-1.0f64),
                Value::from(1.0f64),
                Value::from(1u64),
            ],
        );
        assert!(invalid.is_err());
        assert_eq!(conn.query_first::<u64, _>("SELECT 1")?, Some(1));
        conn.close(stmt)?;
        let reordered = conn.prep("SELECT id,rrf_score FROM ((SELECT CAST(? AS SIGNED) AS id ORDER BY id LIMIT ?) RRF (SELECT CAST(? AS SIGNED) AS id ORDER BY id LIMIT ?) WITH(WEIGHTS=(?,?),RANK_CONSTANT=?)) h ORDER BY rrf_score DESC,best_rank,id LIMIT ?")?;
        assert_eq!(reordered.num_params(), 8);
        let rows: Vec<(i64, f64)> = conn.exec(
            &reordered,
            (11i64, 1u64, 22i64, 1u64, 0.0f64, 2.0f64, 0u64, 1u64),
        )?;
        assert_eq!(rows, vec![(22, 2.0)]);
        let rows: Vec<(i64, f64)> = conn.exec(
            &reordered,
            (33i64, 1u64, 44i64, 1u64, 3.0f64, 0.0f64, 0u64, 1u64),
        )?;
        assert_eq!(rows, vec![(33, 3.0)]);
        conn.close(reordered)?;
        println!(
            "PASS binary prepared RRF metadata, vector/weight/window rebinding and error recovery"
        );
        Ok(())
    })();
    let cleanup = conn.query_drop(format!("DROP DATABASE IF EXISTS {database}"));
    result?;
    cleanup?;
    Ok(())
}
