//! Native-vector SQL boundary, atomicity and prepared-execution regressions.
use super::*;

#[test]
fn vector_datatypes_conditionals_preserve_native_domains_through_scalar_escape() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for datatype in ["FLOAT32", "FLOAT16", "BFLOAT16", "INT8", "BINARY"] {
        engine
            .execute(&format!(
                "CREATE TABLE native_{datatype}(id INT PRIMARY KEY,v VECTOR(3,{datatype}))"
            ))
            .unwrap();
        engine
            .execute(&format!(
                "INSERT INTO native_{datatype} VALUES(1,'[1,0,1]'),(2,NULL)"
            ))
            .unwrap();
        let original = query_rows(
            &engine,
            &format!("SELECT v FROM native_{datatype} WHERE id=1"),
        )[0][0]
            .clone();
        let Value::Vector(expected) = &original else {
            panic!("vector required")
        };
        for source in [
            format!("native_{datatype}"),
            format!("(SELECT id,v FROM native_{datatype}) d"),
        ] {
            let sql=format!("SELECT IF(id=1,v,NULL),CASE WHEN id=1 THEN v ELSE NULL END,COALESCE(NULL,v),IFNULL(v,NULL),NULLIF(v,TO_VECTOR('[0,0,0]','{datatype}')) FROM {source} ORDER BY id");
            let QueryResponse::ResultSet(result) =
                engine.execute(&sql).unwrap().materialize().unwrap()
            else {
                panic!("rows required")
            };
            assert_eq!(
                result.rows,
                vec![vec![original.clone(); 5], vec![Value::Null; 5]],
                "{datatype}: {source}"
            );
            for column in &result.columns {
                assert_eq!(
                    column.source.vector_type,
                    Some((expected.datatype, Some(3))),
                    "{datatype}: {}",
                    column.name
                );
            }
        }
    }
}

#[test]
fn vector_datatypes_functions_read_text_and_options_from_columns() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE vector_args(id INT PRIMARY KEY,txt VARCHAR(20),dt VARCHAR(10),metric VARCHAR(10),pos VARCHAR(2),n VARCHAR(2))").unwrap();
    engine.execute("INSERT INTO vector_args VALUES(1,'[3,4]','FLOAT16','L2','2','1'),(2,'[0,1]','INT8','DOT','1','2')").unwrap();
    assert_eq!(
        query_rows(&engine, "SELECT VECTOR_DATATYPE(TO_VECTOR(txt,dt)),VECTOR_TO_STRING(SUBVECTOR(txt,pos,n)),DISTANCE(txt,'[0,0]',metric) FROM vector_args ORDER BY id"),
        vec![
            vec![Value::String("FLOAT16".into()), Value::String("[4]".into()), Value::Double(5.0)],
            vec![Value::String("INT8".into()), Value::String("[0,1]".into()), Value::Double(0.0)],
        ]
    );
}

#[test]
fn vector_datatypes_binary_subvectors_cover_unaligned_bit_ranges() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let bits = [1, 0, 1, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 0, 0, 1, 1];
    let text = format!(
        "[{}]",
        bits.iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    engine
        .execute("CREATE TABLE packed(id INT PRIMARY KEY,v VECTOR(17,BINARY))")
        .unwrap();
    engine
        .execute(&format!("INSERT INTO packed VALUES(1,'{text}')"))
        .unwrap();
    for start in 0..bits.len() {
        for count in 1..=bits.len() - start {
            let expected_text = format!(
                "[{}]",
                bits[start..start + count]
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let sql = format!("SELECT SUBVECTOR(v,{},{}),HAMMING_DISTANCE(SUBVECTOR(v,{},{}),TO_VECTOR('{expected_text}','BINARY')) FROM packed", start+1,count,start+1,count);
            let row = &query_rows(&engine, &sql)[0];
            let Value::Vector(value) = &row[0] else {
                panic!("native vector required")
            };
            assert_eq!(value.datatype, crate::vector::VectorDataType::Binary);
            assert_eq!(value.dimension, count as u32);
            let expected = (0..count.div_ceil(8))
                .map(|byte| {
                    (0..8)
                        .filter(|bit| byte * 8 + bit < count)
                        .fold(0u8, |out, bit| {
                            out | ((bits[start + byte * 8 + bit] as u8) << bit)
                        })
                })
                .collect::<Vec<_>>();
            assert_eq!(value.payload, expected, "start={start}, count={count}");
            assert_eq!(row[1], Value::Double(0.0));
        }
    }
    for (start, count) in [(0, 1), (1, 0), (17, 2), (i64::MAX, 2), (1, i64::MAX)] {
        let sql = format!("SELECT SUBVECTOR(v,{start},{count}) FROM packed");
        assert!(
            engine
                .execute(&sql)
                .and_then(QueryResponse::materialize)
                .is_err(),
            "{sql}"
        );
    }
}

#[test]
fn vector_datatypes_invalid_dml_is_atomic_and_keeps_indexes_consistent() {
    let _env = HnswEnv::set(&[("MYDB_VECTOR_STORAGE", "memory")]);
    for (datatype, invalid) in [
        ("FLOAT32", "[3.5e38,1]"),
        ("FLOAT16", "[65520,1]"),
        ("BFLOAT16", "[3.4e38,1]"),
        ("INT8", "[128,1]"),
        ("BINARY", "[2,1]"),
    ] {
        for algorithm in ["HNSW", "IVF"] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = open_test_engine(dir.path());
            engine
                .execute(&format!(
                    "CREATE TABLE atomic_v(id INT PRIMARY KEY,v VECTOR(2,{datatype}) NOT NULL)"
                ))
                .unwrap();
            engine
                .execute("INSERT INTO atomic_v VALUES(1,'[0,1]'),(2,'[1,0]')")
                .unwrap();
            let metric = if datatype == "BINARY" {
                "HAMMING"
            } else {
                "L2"
            };
            let options = if algorithm == "IVF" { ",nlist=1" } else { "" };
            engine.execute(&format!("CREATE INDEX vx ON atomic_v(v) USING {algorithm} WITH(metric={metric}{options})")).unwrap();
            let before = query_rows(
                &engine,
                "SELECT id,VECTOR_TO_BINARY(v) FROM atomic_v ORDER BY id",
            );
            for dml in [
                format!("INSERT INTO atomic_v VALUES(3,'[1,1]'),(4,'{invalid}')"),
                format!("UPDATE atomic_v SET v=CASE WHEN id=1 THEN '[1,1]' ELSE '{invalid}' END"),
                "INSERT INTO atomic_v VALUES(3,'[1]')".into(),
                "UPDATE atomic_v SET v=NULL WHERE id=1".into(),
            ] {
                let error = engine
                    .execute(&dml)
                    .expect_err("invalid vector DML must fail");
                assert!(!error.message().is_empty());
                assert_eq!(
                    query_rows(
                        &engine,
                        "SELECT id,VECTOR_TO_BINARY(v) FROM atomic_v ORDER BY id"
                    ),
                    before,
                    "{datatype}/{algorithm}: {dml}"
                );
                let sql = format!("SELECT id FROM atomic_v ORDER BY DISTANCE(v,TO_VECTOR('[0,1]','{datatype}'),'{metric}'),id LIMIT 2");
                assert_eq!(
                    query_rows(&engine, &sql),
                    vec![vec![Value::Int(1)], vec![Value::Int(2)]],
                    "{datatype}/{algorithm}: {dml}"
                );
            }
            let mut session = SessionContext::default();
            for sql in [
                "BEGIN",
                "UPDATE atomic_v SET v='[1,1]' WHERE id=1",
                "DELETE FROM atomic_v WHERE id=2",
                "ROLLBACK",
            ] {
                engine.execute_with_session(&mut session, sql).unwrap();
            }
            assert_eq!(
                query_rows(
                    &engine,
                    "SELECT id,VECTOR_TO_BINARY(v) FROM atomic_v ORDER BY id"
                ),
                before
            );
        }
    }
}

#[test]
fn vector_datatypes_prepared_binary_errors_do_not_poison_rebinding() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let sql = "SELECT VECTOR_FROM_BINARY(?,?,?) AS v";
    let statement = engine.prepare_cached(&mut session, sql, 3).unwrap();
    for (datatype, dimension, payload, valid) in [
        ("FLOAT16", 1, vec![0x00, 0x3c], true),
        ("FLOAT16", 1, vec![0x00, 0x7c], false),
        ("BFLOAT16", 1, vec![0x80, 0x3f], true),
        ("BINARY", 7, vec![0x80], false),
        ("BINARY", 9, vec![0x05, 0x01], true),
        ("BINARY", 10, vec![0x05, 0x01], true),
        ("BINARY", 9, vec![0x05, 0x01], true),
        ("INT8", 2, vec![0x80], false),
        ("INT8", 2, vec![0x80, 0x7f], true),
        ("FLOAT32", 1, vec![0, 0, 0xc0, 0x7f], false),
        ("FLOAT32", 1, 1.0f32.to_le_bytes().to_vec(), true),
    ] {
        let params = vec![
            Value::Bytes(payload.clone()),
            Value::String(datatype.into()),
            Value::Int(dimension),
        ];
        let response = with_params(params, || {
            engine.execute_prepared_statement(&mut session, sql, (*statement).clone())
        })
        .expect("prepared SELECT")
        .and_then(QueryResponse::materialize);
        if !valid {
            assert!(response.is_err(), "{datatype}/{dimension}/{payload:?}");
            continue;
        }
        let QueryResponse::ResultSet(result) = response.unwrap() else {
            panic!("rows required")
        };
        let Value::Vector(value) = &result.rows[0][0] else {
            panic!("vector required")
        };
        assert_eq!(value.payload, payload);
        assert_eq!(value.dimension, dimension as u32);
        assert_eq!(value.datatype.name(), datatype);
        assert_eq!(
            result.columns[0].source.vector_type,
            Some((value.datatype, Some(value.dimension)))
        );
    }
}

#[test]
fn vector_datatypes_prepared_subvector_dimension_rebinds_with_same_parameter_type() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE dimension_v(id INT PRIMARY KEY,v VECTOR(9,BINARY) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO dimension_v VALUES(1,'[1,0,1,0,1,0,1,0,1]')")
        .unwrap();
    let mut session = SessionContext::default();
    for expression in [
        "SUBVECTOR(v,1,?)",
        "IF(id=1,SUBVECTOR(v,1,?),NULL)",
        "CASE WHEN id=1 THEN SUBVECTOR(v,1,?) ELSE NULL END",
    ] {
        let sql = format!("SELECT {expression} AS v FROM dimension_v");
        let sql = sql.as_str();
        let statement = engine.prepare_cached(&mut session, sql, 1).unwrap();
        let mut memo = crate::engine::api::PreparedSelectMemoState::default();
        for count in [1, 2, 7, 8, 9, 1] {
            let params = vec![Value::Int(count)];
            let response = match engine.execute_prepared_select(
                &mut session,
                &mut memo,
                &statement,
                sql,
                &params,
            ) {
                Some(response) => response.unwrap(),
                None => with_params(params, || {
                    engine.execute_prepared_statement(&mut session, sql, (*statement).clone())
                })
                .unwrap()
                .unwrap(),
            }
            .materialize()
            .unwrap();
            let QueryResponse::ResultSet(result) = response else {
                panic!("rows required")
            };
            let Value::Vector(value) = &result.rows[0][0] else {
                panic!("vector required")
            };
            assert_eq!(value.dimension, count as u32, "{sql}");
            assert_eq!(
                result.columns[0].source.vector_type,
                Some((value.datatype, Some(count as u32)))
            );
        }
    }
}
