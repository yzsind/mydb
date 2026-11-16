use super::*;

fn fixture(sql: &[&str]) -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for statement in sql {
        engine
            .execute(statement)
            .unwrap_or_else(|e| panic!("{statement}: {e:?}"));
    }
    (dir, engine)
}

#[test]
fn optimizer1108_b01_minmax_join_collation() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE p (pk INT PRIMARY KEY, s2 VARCHAR(16) COLLATE utf8mb4_bin, k INT)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE b (pk INT PRIMARY KEY, k INT, t2 VARCHAR(16) COLLATE utf8mb4_bin)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO p VALUES (1,'a',1),(2,'E',1),(3,'B',2),(4,'y',2)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO b VALUES (1,1,'a'),(2,2,'E'),(3,3,'Z')"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT MAX(p.s2), MIN(p.s2) FROM p"###,
        &[&[r###"y"###, r###"B"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT /*+ HASH_JOIN(p b) */ MAX(p.s2), MIN(p.s2) FROM p JOIN b ON p.k = b.k"###,
        &[&[r###"y"###, r###"B"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT /*+ HASH_JOIN(p b) */ MAX(b.t2), MIN(b.t2) FROM p JOIN b ON p.k = b.k"###,
        &[&[r###"a"###, r###"E"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT /*+ INDEX_NLJ(b) */ MAX(p.s2), MIN(p.s2) FROM p JOIN b ON p.k = b.pk"###,
        &[&[r###"y"###, r###"B"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT /*+ HASH_JOIN(p b) */ b.k, MAX(p.s2) FROM p JOIN b ON p.k = b.k GROUP BY b.k"###,
        &[&[r###"1"###, r###"a"###], &[r###"2"###, r###"y"###]],
        false,
    );
}

#[test]
fn optimizer1108_b03_spill_agg_minmax_collation() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE a (id INT PRIMARY KEY, k INT, s VARCHAR(16) COLLATE utf8mb4_bin)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE b (id INT PRIMARY KEY, k INT, g INT)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO a VALUES (1,1,'z'),(2,1,'é'),(3,1,'E'),(4,2,'b'),(5,2,'B')"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO b VALUES (1,1,10),(2,1,20),(3,2,10)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT a.k, MAX(a.s), MIN(a.s) FROM a GROUP BY a.k"###,
        &[
            &[r###"1"###, r###"é"###, r###"E"###],
            &[r###"2"###, r###"b"###, r###"B"###],
        ],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT b.g, MAX(a.s), MIN(a.s) FROM a JOIN b ON a.k = b.k GROUP BY b.g"###,
        &[
            &[r###"20"###, r###"é"###, r###"E"###],
            &[r###"10"###, r###"é"###, r###"B"###],
        ],
        false,
    );
}

#[test]
fn optimizer1108_b04_vec_agg_computed_minmax_collation() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, r###"CREATE TABLE t (pk INT PRIMARY KEY, i2 INT, s2 VARCHAR(16) COLLATE utf8mb4_bin)"###).unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO t VALUES (21,5,'a '),(27,5,'B')"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT ABS(i2) AS g1, MIN(COALESCE(s2, 'n')) AS x1 FROM t GROUP BY g1"###,
        &[&[r###"5"###, r###"B"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT ABS(i2) AS g1, SUM(-2) AS x0, MIN(COALESCE(s2, 'n')) AS x1 FROM t GROUP BY g1"###,
        &[&[r###"5"###, r###"-4"###, r###"B"###]],
        false,
    );
}

#[test]
fn optimizer1108_b05_inlj_index_collation() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE t1 (pk INT PRIMARY KEY, s1 VARCHAR(16), KEY k_s1 (s1))"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE t0 (pk INT PRIMARY KEY, s2 VARCHAR(16) COLLATE utf8mb4_bin)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO t1 VALUES (31,'01'),(32,'z'),(34,'a')"###,
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"INSERT INTO t0 VALUES (57,'A')"###)
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT a0.pk, a1.pk FROM t1 a0 JOIN t0 a1 ON a0.s1 = a1.s2"###,
        &[],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM t1 a0 JOIN t0 a1 ON a0.s1 = a1.s2"###,
        &[&[r###"0"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT /*+ INDEX_NLJ(a0) */ a1.pk FROM t0 a1 JOIN t1 a0 ON a0.s1 = a1.s2"###,
        &[],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT a1.pk FROM t0 a1 WHERE EXISTS (SELECT 1 FROM t1 a0 WHERE a0.s1 = a1.s2)"###,
        &[],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT a1.pk FROM t0 a1 WHERE a1.s2 IN (SELECT s1 FROM t1)"###,
        &[],
        false,
    );
}

#[test]
fn optimizer1108_b06_subquery_comparison_collation() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE a (pk INT PRIMARY KEY, s1 VARCHAR(16))"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE b (pk INT PRIMARY KEY, s2 VARCHAR(16) COLLATE utf8mb4_bin)"###,
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"INSERT INTO a VALUES (1,'a'),(2,'C')"###)
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"INSERT INTO b VALUES (1,'B'),(2,'b')"###)
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM a WHERE s1 > (SELECT MAX(s2) FROM b WHERE b.pk = 1)"###,
        &[&[r###"1"###], &[r###"2"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk, s1 > (SELECT s2 FROM b WHERE b.pk = 1) AS gt FROM a ORDER BY pk"###,
        &[&[r###"1"###, r###"1"###], &[r###"2"###, r###"1"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM a WHERE s1 > ANY (SELECT s2 FROM b)"###,
        &[&[r###"1"###], &[r###"2"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM a WHERE s1 > ALL (SELECT s2 FROM b)"###,
        &[],
        false,
    );
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE t2 (pk INT PRIMARY KEY, s2 VARCHAR(16) COLLATE utf8mb4_bin)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO t2 VALUES (1,'a'),(2,'A'),(3,'b'),(4,'B')"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM t2 WHERE s2 = SOME (SELECT s2 FROM t2 WHERE pk = 1)"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM t2 WHERE s2 IN (SELECT s2 FROM t2 WHERE pk = 1)"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM t2 WHERE s2 > ALL (SELECT s2 FROM t2 WHERE pk IN (1,2))"###,
        &[&[r###"3"###]],
        false,
    );
}

#[test]
fn optimizer1108_b07_setop_result_collation() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE t (pk INT PRIMARY KEY, s VARCHAR(10) COLLATE utf8mb4_bin)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO t VALUES (1,'a'),(2,'a '),(3,'A'),(4,'b')"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM (SELECT CONCAT(s, '') AS u FROM t UNION SELECT 'B') x"###,
        &[&[r###"4"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM (SELECT UPPER(s) AS u FROM t UNION SELECT 'B') x"###,
        &[&[r###"2"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"(SELECT UPPER(s) AS u FROM t WHERE pk = 1) INTERSECT (SELECT 'a')"###,
        &[],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT c0, COUNT(*) FROM ((SELECT UPPER(s) AS c0 FROM t) UNION ALL (SELECT 'x')) u GROUP BY c0 ORDER BY c0"###,
        &[
            &[r###"A"###, r###"3"###],
            &[r###"B"###, r###"1"###],
            &[r###"x"###, r###"1"###],
        ],
        true,
    );
}

#[test]
fn optimizer1108_b08_coalesce_mixed_type_order() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE t9 (id INT PRIMARY KEY, si BIGINT, s VARCHAR(20))"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session, r###"INSERT INTO t9 VALUES (1,5,'0abc'),(2,-3,'zzz'),(3,100,'9z'),(4,NULL,'abc'),(5,7,'007')"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM t9 ORDER BY COALESCE(si, s), id"###,
        &[
            &[r###"2"###],
            &[r###"3"###],
            &[r###"1"###],
            &[r###"5"###],
            &[r###"4"###],
        ],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM t9 ORDER BY IFNULL(si, s) DESC, id"###,
        &[
            &[r###"4"###],
            &[r###"5"###],
            &[r###"1"###],
            &[r###"3"###],
            &[r###"2"###],
        ],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT MAX(COALESCE(si, s)), MIN(COALESCE(si, s)) FROM t9"###,
        &[&[r###"abc"###, r###"-3"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT GROUP_CONCAT(COALESCE(si, s) ORDER BY COALESCE(si, s)) FROM t9"###,
        &[&[r###"-3,100,5,7,abc"###]],
        false,
    );
}

#[test]
fn optimizer1108_b09_time_vs_date_compare() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE tt (id INT PRIMARY KEY, tm TIME, dt DATE, dts DATETIME)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO tt VALUES (1,'12:00:00','2019-01-01','2019-01-01 00:00:00')"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT TIME'12:00:00' > DATE'2019-01-01', TIME'12:00:00' < TIMESTAMP'2030-01-01 00:00:00'"###,
        &[&[r###"1"###, r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM tt WHERE tm > dt"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM tt WHERE tm > dts"###,
        &[&[r###"1"###]],
        false,
    );
}

#[test]
fn optimizer1108_b10_cast_time_datetime() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT YEAR(CAST(TIME'10:00:00' AS DATETIME)) = YEAR(CURDATE()) AS anchored_today"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT DATE(CAST(TIME'10:00:00' AS DATETIME)) = CURDATE() AS same_day"###,
        &[&[r###"1"###]],
        false,
    );
}

#[test]
fn optimizer1108_b11_decimal_spurious_overflow() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"CREATE TABLE dd (a DECIMAL(65,30))"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO dd VALUES (12345678901234567890.123456789012345678901234567890)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT a * a FROM dd"###,
        &[&[r###"152415787532388367504953515625666819450.083828733757049236500533455763"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT SUM(a * a) FROM dd"###,
        &[&[r###"152415787532388367504953515625666819450.083828733757049236500533455763"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT CAST(1.5 AS DECIMAL(40,30)) * CAST(2.5 AS DECIMAL(40,30)) AS p"###,
        &[&[r###"3.750000000000000000000000000000"###]],
        false,
    );
}

#[test]
fn optimizer1108_b12_unsigned_minus_decimal() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE t (pk INT PRIMARY KEY, u INT UNSIGNED, d DECIMAL(10,2))"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO t VALUES (1,0,0.50),(2,5,2.50)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT CAST(0 AS UNSIGNED) - 0.5"###,
        &[&[r###"-0.5"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk, u - d FROM t ORDER BY pk"###,
        &[&[r###"1"###, r###"-0.50"###], &[r###"2"###, r###"2.50"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT AVG(u - d) FROM t"###,
        &[&[r###"1.000000"###]],
        false,
    );
}

#[test]
fn optimizer1108_b13_bit_agg_decimal_rounding() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"CREATE TABLE m1 (d DECIMAL(5,2))"###)
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"INSERT INTO m1 VALUES (2.5)"###)
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT BIT_XOR(d), BIT_AND(d), BIT_OR(d) FROM m1"###,
        &[&[r###"3"###, r###"3"###, r###"3"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT BIT_OR(2.5), BIT_OR(-1.5), BIT_OR(0.5)"###,
        &[&[r###"3"###, r###"18446744073709551614"###, r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT 2.5 | 0, -1.5 | 0, 0.5 | 0"###,
        &[&[r###"3"###, r###"18446744073709551614"###, r###"1"###]],
        false,
    );
}

#[test]
fn optimizer1108_b14_sum_avg_time() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE m3 (id INT PRIMARY KEY, tm TIME, tm3 TIME(3))"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session, r###"INSERT INTO m3 VALUES (1,'01:00:00','01:00:00.250'),(2,'01:30:00','01:30:00.500'),(3,'25:00:00','25:00:00.000'),(4,'11:59:59','11:59:59.999')"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT AVG(tm), SUM(tm) FROM m3"###,
        &[&[r###"97239.7500"###, r###"388959"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT AVG(tm3), SUM(tm3) FROM m3"###,
        &[&[r###"97240.1872500"###, r###"388960.749"###]],
        false,
    );
}

#[test]
fn optimizer1108_b15_outer_join_topk_nulls() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, r###"CREATE TABLE a (pk INT PRIMARY KEY, u INT NOT NULL, k INT, UNIQUE KEY uu (u), KEY kk (k))"###).unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE b (pk INT PRIMARY KEY, k INT)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO a VALUES (1,11,1),(2,12,2),(3,13,3)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO b VALUES (1,1),(2,5),(3,6),(4,1)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT a.pk FROM a RIGHT JOIN b ON a.k = b.k ORDER BY a.pk"###,
        &[
            &[r###"NULL"###],
            &[r###"NULL"###],
            &[r###"1"###],
            &[r###"1"###],
        ],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT a.pk FROM a RIGHT JOIN b ON a.k = b.k ORDER BY a.pk LIMIT 2"###,
        &[&[r###"NULL"###], &[r###"NULL"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT a.u FROM a RIGHT JOIN b ON a.k = b.k ORDER BY a.u LIMIT 2"###,
        &[&[r###"NULL"###], &[r###"NULL"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT a.pk FROM a RIGHT JOIN b ON a.k = b.k ORDER BY a.pk LIMIT 10"###,
        &[
            &[r###"NULL"###],
            &[r###"NULL"###],
            &[r###"1"###],
            &[r###"1"###],
        ],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT b.pk FROM a LEFT JOIN b ON a.k = b.pk ORDER BY b.pk LIMIT 2"###,
        &[&[r###"1"###], &[r###"2"###]],
        true,
    );
}

#[test]
fn optimizer1108_b16_using_qualified_columns() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"CREATE TABLE um (id INT, v INT)"###)
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"CREATE TABLE un (id INT, w INT)"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO um VALUES (1, 10), (2, 20)"###,
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"INSERT INTO un VALUES (1, 100)"###)
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT um.id, un.id FROM um LEFT JOIN un USING (id) ORDER BY um.id"###,
        &[&[r###"1"###, r###"1"###], &[r###"2"###, r###"NULL"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT un.id FROM um LEFT JOIN un USING (id) ORDER BY un.id"###,
        &[&[r###"NULL"###], &[r###"1"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT um.id, un.id FROM um NATURAL LEFT JOIN un ORDER BY um.id"###,
        &[&[r###"1"###, r###"1"###], &[r###"2"###, r###"NULL"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM um LEFT JOIN un USING (id) ORDER BY id"###,
        &[&[r###"1"###], &[r###"2"###]],
        true,
    );
}

#[test]
fn optimizer1108_b19_parallel_agg_null_split() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE a (pk INT PRIMARY KEY, k INT, s VARCHAR(8))"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE b (pk INT PRIMARY KEY, k INT, dt DATE)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO a VALUES (1, NULL, 'x'), (2, 1, 'X')"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO b VALUES (1, 1, NULL), (2, 1, NULL), (3, 1, '2020-01-01')"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT b.dt, LOWER(a.s) AS g, COUNT(*) FROM a LEFT JOIN b ON a.k = b.k GROUP BY b.dt, g ORDER BY b.dt, g"###,
        &[
            &[r###"NULL"###, r###"x"###, r###"3"###],
            &[r###"2020-01-01"###, r###"x"###, r###"1"###],
        ],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT /*+ PARALLEL(4) */ b.dt, LOWER(a.s) AS g, COUNT(*) FROM a LEFT JOIN b ON a.k = b.k GROUP BY b.dt, g ORDER BY b.dt, g"###,
        &[
            &[r###"NULL"###, r###"x"###, r###"3"###],
            &[r###"2020-01-01"###, r###"x"###, r###"1"###],
        ],
        true,
    );
    let QueryResponse::ResultSet(rs) = engine.execute("EXPLAIN ANALYZE SELECT /*+ PARALLEL(4) */ b.dt, LOWER(a.s) g, COUNT(*) FROM a LEFT JOIN b ON a.k=b.k GROUP BY b.dt,g").and_then(QueryResponse::materialize).unwrap() else { panic!("expected rows") };
    assert!(!rs.rows[0][0].to_display_string().contains("spill_bytes="));
}

#[test]
fn optimizer1108_b20_group_by_int_literal_alias() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE t (pk INT PRIMARY KEY, i INT)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO t VALUES (1,1),(2,2),(3,3)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT 2 AS g, COUNT(*) FROM t GROUP BY g"###,
        &[&[r###"2"###, r###"3"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT 2 AS g, SUM(i) FROM t GROUP BY g HAVING g > 0"###,
        &[&[r###"2"###, r###"6"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT 'x' AS g, COUNT(*) FROM t GROUP BY g"###,
        &[&[r###"x"###, r###"3"###]],
        false,
    );
}

#[test]
fn optimizer1108_b21_rollup_duplicate_keys() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE t (pk INT PRIMARY KEY, s VARCHAR(5), v INT)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO t VALUES (1,'a',1),(2,'a',2),(3,'b',3)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT s AS g0, s AS g1, SUM(v) FROM t GROUP BY g0, g1 WITH ROLLUP"###,
        &[
            &[r###"a"###, r###"a"###, r###"3"###],
            &[r###"a"###, r###"NULL"###, r###"3"###],
            &[r###"b"###, r###"b"###, r###"3"###],
            &[r###"b"###, r###"NULL"###, r###"3"###],
            &[r###"NULL"###, r###"NULL"###, r###"6"###],
        ],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT s, s, SUM(v) FROM t GROUP BY s, s WITH ROLLUP"###,
        &[
            &[r###"a"###, r###"a"###, r###"3"###],
            &[r###"a"###, r###"a"###, r###"3"###],
            &[r###"b"###, r###"b"###, r###"3"###],
            &[r###"b"###, r###"b"###, r###"3"###],
            &[r###"NULL"###, r###"NULL"###, r###"6"###],
        ],
        false,
    );
}

#[test]
fn optimizer1108_b23_grouping_outside_rollup() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"CREATE TABLE m4 (g INT, v INT)"###)
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"INSERT INTO m4 VALUES (1,1),(2,2)"###)
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                r###"SELECT g, COUNT(*), GROUPING(g) FROM m4 GROUP BY g"###
            )
            .and_then(QueryResponse::materialize)
            .unwrap_err()
            .code(),
        1111,
        r###"SELECT g, COUNT(*), GROUPING(g) FROM m4 GROUP BY g"###
    );
}

#[test]
fn optimizer1108_b24_group_concat_separator_null() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"CREATE TABLE m4 (g INT, v INT)"###)
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"INSERT INTO m4 VALUES (1,1),(2,2)"###)
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                r###"SELECT GROUP_CONCAT(v SEPARATOR NULL) FROM m4"###
            )
            .and_then(QueryResponse::materialize)
            .unwrap_err()
            .code(),
        1064,
        r###"SELECT GROUP_CONCAT(v SEPARATOR NULL) FROM m4"###
    );
}

#[test]
fn optimizer1108_b26_spurious_3065_after_decorrelation() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE t0 (pk INT PRIMARY KEY, i1 INT, s1 VARCHAR(16), dt DATE)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE t1 (pk INT PRIMARY KEY, b1 BIGINT, s1 VARCHAR(16))"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO t0 VALUES (1,1,'a','2020-01-01'),(2,2,'b','2020-01-02')"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO t1 VALUES (1,1,'a'),(2,2,'c')"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT DISTINCT a0.dt AS c0, DATE_ADD(a1.dt, INTERVAL -9 DAY) AS c3 FROM t0 a0 CROSS JOIN t0 a1 WHERE a0.s1 = (SELECT MAX(sq.s1) FROM t1 sq WHERE sq.b1 = a0.i1) ORDER BY c0, c3"###,
        &[
            &[r###"2020-01-01"###, r###"2019-12-23"###],
            &[r###"2020-01-01"###, r###"2019-12-24"###],
        ],
        true,
    );
}

#[test]
fn optimizer1108_b28_lead_lag_default_type() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE t2 (pk INT PRIMARY KEY, d1 DECIMAL(10,2))"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO t2 VALUES (1,0.50),(2,1.00),(3,-0.01)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk, LEAD(d1, 2, 0) OVER (ORDER BY pk), LAG(d1, 1, 7) OVER (ORDER BY pk) FROM t2"###,
        &[
            &[r###"1"###, r###"-0.01"###, r###"7.00"###],
            &[r###"2"###, r###"0.00"###, r###"0.50"###],
            &[r###"3"###, r###"0.00"###, r###"1.00"###],
        ],
        false,
    );
}

#[test]
fn optimizer1108_b29_const_expr_limit_error() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE a (pk INT PRIMARY KEY, k INT)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO a VALUES (1,1),(2,2),(3,3),(4,4),(5,5)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM a WHERE (IF((0.5 IS NOT NULL), 10, 2) NOT BETWEEN 1 AND -2) ORDER BY pk LIMIT 2, 4"###,
        &[&[r###"3"###], &[r###"4"###], &[r###"5"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM a WHERE (10 NOT BETWEEN 1 AND -2) ORDER BY pk LIMIT 2, 4"###,
        &[&[r###"3"###], &[r###"4"###], &[r###"5"###]],
        true,
    );
}

#[test]
fn optimizer1108_b38_qualified_collate_parse() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE t1 (pk INT PRIMARY KEY, s1 VARCHAR(16))"###,
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"INSERT INTO t1 VALUES (1,'a'),(2,'A')"###)
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM t1 WHERE s1 COLLATE utf8mb4_bin = 'a'"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM t1 a0 WHERE a0.s1 COLLATE utf8mb4_bin = 'a'"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT a0.s1 COLLATE utf8mb4_bin FROM t1 a0 ORDER BY 1"###,
        &[&[r###"A"###], &[r###"a"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM t1 x JOIN t1 y ON x.s1 = y.s1 COLLATE utf8mb4_bin"###,
        &[&[r###"2"###]],
        false,
    );
}

#[test]
fn optimizer1108_b40_cte_union_pushdown_window() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"CREATE TABLE bga (k INT, v INT)"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO bga VALUES (1,10),(2,20),(3,30),(5,50),(8,80)"###,
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"CREATE TABLE bgb (k INT, w INT)"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO bgb VALUES (2,7),(3,9),(9,1)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"WITH u AS (SELECT k, SUM(k) OVER (ORDER BY k) AS s FROM bga UNION ALL SELECT k, 0 FROM bgb) SELECT * FROM u WHERE k = 2 ORDER BY s"###,
        &[&[r###"2"###, r###"0"###], &[r###"2"###, r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"WITH u AS (SELECT k, LAG(k) OVER (ORDER BY k) AS prev FROM bga UNION ALL SELECT k, 0 FROM bgb) SELECT * FROM u WHERE k = 2 ORDER BY prev"###,
        &[&[r###"2"###, r###"0"###], &[r###"2"###, r###"1"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT * FROM (SELECT k, SUM(k) OVER (ORDER BY k) AS s FROM bga UNION ALL SELECT k, 0 FROM bgb) d WHERE k = 2 ORDER BY s"###,
        &[&[r###"2"###, r###"0"###], &[r###"2"###, r###"3"###]],
        true,
    );
}

#[test]
fn optimizer1108_b41_cte_prune_having_alias() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"CREATE TABLE bkc (k INT, v INT)"###)
        .unwrap();
    engine.execute_with_session(&mut session, r###"INSERT INTO bkc VALUES (1,10),(1,11),(1,12),(2,20),(2,21),(3,30),(3,31),(3,32),(3,33)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"WITH c AS (SELECT k, COUNT(*) AS cnt FROM bkc GROUP BY k HAVING cnt > 1) SELECT a.k FROM c a JOIN c b ON a.k = b.k ORDER BY a.k"###,
        &[&[r###"1"###], &[r###"2"###], &[r###"3"###]],
        true,
    );
}

#[test]
fn optimizer1108_b42_cte_union_signed_unsigned_group() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE bg1 (g INT UNSIGNED, x INT)"###,
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"INSERT INTO bg1 VALUES (1,5)"###)
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"CREATE TABLE bg2 (g INT, x INT)"###)
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"INSERT INTO bg2 VALUES (1,6)"###)
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"WITH src AS (SELECT g, x FROM bg1 UNION ALL SELECT g, x FROM bg2), agg AS (SELECT g, SUM(x) AS sx FROM src GROUP BY g) SELECT COUNT(*) AS n FROM agg a JOIN agg b ON a.g = b.g"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"WITH src AS (SELECT g, x FROM bg1 UNION ALL SELECT g, x FROM bg2), agg AS (SELECT g, SUM(x) AS sx FROM src GROUP BY g) SELECT a.g AS ga, a.sx AS sa, b.sx AS sb FROM agg a JOIN agg b ON a.g = b.g ORDER BY ga"###,
        &[&[r###"1"###, r###"11"###, r###"11"###]],
        true,
    );
}

#[test]
fn optimizer1108_b43_cte_union_agg_key_types() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"CREATE TABLE bv1 (g INT, x INT)"###)
        .unwrap();
    engine
        .execute_with_session(&mut session, r###"INSERT INTO bv1 VALUES (1,10),(2,20)"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE bv2 (g VARCHAR(10), x INT)"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO bv2 VALUES ('2',5),('3',7)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"WITH src AS (SELECT g, x FROM bv1 UNION ALL SELECT g, x FROM bv2), agg AS (SELECT g, SUM(x) AS sx FROM src GROUP BY g) SELECT a.g AS ga, b.sx AS sb FROM agg a JOIN agg b ON a.g = b.g ORDER BY ga"###,
        &[
            &[r###"1"###, r###"10"###],
            &[r###"2"###, r###"25"###],
            &[r###"3"###, r###"7"###],
        ],
        true,
    );
}

#[test]
fn optimizer1108_b44_union_numeric_string_render() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT 1.5 UNION SELECT 2 UNION SELECT '3'"###,
        &[&[r###"1.5"###], &[r###"2"###], &[r###"3"###]],
        false,
    );
}

#[test]
fn optimizer1108_b47_prefix_index_param_lookup() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, r###"CREATE TABLE bug_p (id INT PRIMARY KEY, a INT, p VARCHAR(100), KEY kp2 (p(3), a))"###).unwrap();
    engine.execute_with_session(&mut session, r###"INSERT INTO bug_p VALUES (1, 1, 'abcdef'), (2, 2, 'abcxyz'), (3, 3, 'ab'), (4, 4, 'xyzabc'), (5, 5, 'abc'), (6, NULL, NULL)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id, p, a FROM bug_p WHERE p = 'abcdef' ORDER BY id"###,
        &[&[r###"1"###, r###"abcdef"###, r###"1"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id, p, a FROM bug_p WHERE p = 'abcxyz' ORDER BY id"###,
        &[&[r###"2"###, r###"abcxyz"###, r###"2"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM bug_p WHERE p = 'abcdef'"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM bug_p WHERE p = 'abcdef'"###,
        &[&[r###"1"###]],
        false,
    );
}

#[test]
fn optimizer1108_b48_window_topn_grouping_alias() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"CREATE TABLE vj1 (g INT, x INT)"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO vj1 VALUES (1,10),(1,20),(2,5),(3,7),(3,8)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT * FROM (SELECT g, sx, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY sx DESC, g) rn FROM (SELECT g, SUM(x) sx, GROUPING(g) grp FROM vj1 GROUP BY g WITH ROLLUP) t) q WHERE rn <= 2 ORDER BY q.g"###,
        &[
            &[r###"NULL"###, r###"50"###, r###"1"###],
            &[r###"1"###, r###"30"###, r###"1"###],
            &[r###"3"###, r###"15"###, r###"2"###],
        ],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT * FROM (SELECT g, sx, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY sx DESC, g) rn FROM (SELECT g, SUM(x) sx, GROUPING(g) grp FROM vj1 GROUP BY g WITH ROLLUP) t) q ORDER BY q.g"###,
        &[
            &[r###"NULL"###, r###"50"###, r###"1"###],
            &[r###"1"###, r###"30"###, r###"1"###],
            &[r###"2"###, r###"5"###, r###"3"###],
            &[r###"3"###, r###"15"###, r###"2"###],
        ],
        true,
    );
}

#[test]
fn optimizer1108_b51_partition_pruning_plan_cache() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"SET SESSION cte_max_recursion_depth = 1000000"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session, r###"CREATE TABLE bug_in (id INT, k INT, PRIMARY KEY (id, k)) PARTITION BY LIST (k) (PARTITION p1 VALUES IN (1,2,3), PARTITION p2 VALUES IN (4,5,6), PARTITION p3 VALUES IN (7,8,9))"###).unwrap();
    engine.execute_with_session(&mut session, r###"INSERT INTO bug_in SELECT n, 1 + (n % 9) FROM (WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3000) SELECT n FROM r) x"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM bug_in WHERE k IN (1, 9)"###,
        &[&[r###"666"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM bug_in WHERE k IN (4, 5)"###,
        &[&[r###"667"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM bug_in WHERE k IN (1, 9)"###,
        &[&[r###"666"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM bug_in WHERE k = 1 OR k = 5"###,
        &[&[r###"666"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM bug_in WHERE k = 2 OR k = 8"###,
        &[&[r###"667"###]],
        false,
    );
}

#[test]
fn optimizer1108_b54_minmax_geometry() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE ty (id INT PRIMARY KEY, geo GEOMETRY)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session, r###"INSERT INTO ty VALUES (1, ST_GeomFromText('POINT(1 1)')), (2, ST_GeomFromText('POINT(2 2)'))"###).unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, r###"SELECT ST_AsText(MIN(geo)) FROM ty"###)
            .and_then(QueryResponse::materialize)
            .unwrap_err()
            .code(),
        1210,
        r###"SELECT ST_AsText(MIN(geo)) FROM ty"###
    );
    assert_eq!(engine.execute_with_session(&mut session, r###"SELECT ST_AsText(MIN(geo)) FROM (SELECT geo FROM ty ORDER BY ST_AsText(geo) LIMIT 5) x"###).and_then(QueryResponse::materialize).unwrap_err().code(), 1210, r###"SELECT ST_AsText(MIN(geo)) FROM (SELECT geo FROM ty ORDER BY ST_AsText(geo) LIMIT 5) x"###);
}

fn check_optimizer_rows(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
    expected: &[&[&str]],
    ordered: bool,
) {
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(session, sql)
        .and_then(QueryResponse::materialize)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    else {
        panic!("expected rows")
    };
    let mut actual: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|row| row.iter().map(Value::to_display_string).collect())
        .collect();
    let mut expected: Vec<Vec<String>> = expected
        .iter()
        .map(|row| row.iter().map(|v| v.to_string()).collect())
        .collect();
    if !ordered {
        actual.sort();
        expected.sort();
    }
    assert_eq!(actual, expected, "{sql}");
}

#[test]
fn optimizer1108_round2_c01_corr_dt_vs_outer_int() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"DROP TABLE IF EXISTS b"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE b (pk INT PRIMARY KEY, dt DATETIME, i INT)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO b VALUES (1,'2020-01-01 00:00:00',3),(2,'2021-06-15 12:00:00',NULL),(3,NULL,7)"###).unwrap();
    engine
        .execute_with_session(&mut session, r###"DROP TABLE IF EXISTS o"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE o (pk INT PRIMARY KEY, oi INT, odt DATETIME)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO o VALUES (1,3,'2020-01-01 00:00:00'),(2,1000000,'2021-01-01 00:00:00'),(3,NULL,NULL)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT o.pk,
  (SELECT COUNT(*) FROM b WHERE b.dt > o.oi) c_dt_gt_int,
  (SELECT COUNT(*) FROM b WHERE b.i < o.odt) c_int_lt_odt,
  (SELECT MAX(b.pk) FROM b WHERE b.dt > o.oi) m
FROM o ORDER BY o.pk"###,
        &[
            &[r###"1"###, r###"2"###, r###"2"###, r###"2"###],
            &[r###"2"###, r###"2"###, r###"2"###, r###"2"###],
            &[r###"3"###, r###"0"###, r###"0"###, r###"NULL"###],
        ],
        true,
    );
}

#[test]
fn optimizer1108_round2_c02_uncorr_colcol() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"DROP TABLE IF EXISTS b"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE b (pk INT PRIMARY KEY, i INT, dt DATETIME)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO b VALUES (1,3,'2020-01-01 00:00:00'),(2,NULL,'2021-06-15 12:00:00'),(3,7,NULL)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT (SELECT COUNT(*) FROM b WHERE i < dt) q"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM b WHERE i < dt"###,
        &[&[r###"1"###]],
        false,
    );
}

#[test]
fn optimizer1108_round2_c03_in_notin_any_all_exists() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"DROP TABLE IF EXISTS b"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE b (pk INT PRIMARY KEY, i INT, dt DATETIME)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO b VALUES (1,3,'2020-01-01 00:00:00'),(2,4,'2021-06-15 12:00:00'),(3,7,NULL)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM b WHERE pk IN (SELECT pk FROM b WHERE dt > 1) ORDER BY pk"###,
        &[&[r###"1"###], &[r###"2"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM b WHERE pk NOT IN (SELECT pk FROM b WHERE dt > 1) ORDER BY pk"###,
        &[&[r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM b WHERE i > ANY (SELECT pk FROM b WHERE dt > 1) ORDER BY pk"###,
        &[&[r###"1"###], &[r###"2"###], &[r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM b WHERE i < ALL (SELECT pk FROM b WHERE dt > 1) ORDER BY pk"###,
        &[],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM b WHERE EXISTS (SELECT 1 FROM b x WHERE x.dt > 1 AND x.pk = b.pk) ORDER BY pk"###,
        &[&[r###"1"###], &[r###"2"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM b WHERE pk = (SELECT MAX(pk) FROM b WHERE dt > 1) ORDER BY pk"###,
        &[&[r###"2"###]],
        true,
    );
}

#[test]
fn optimizer1108_round2_c04_limit_offset_subq() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"DROP TABLE IF EXISTS b"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE b (pk INT PRIMARY KEY, dt DATETIME)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO b VALUES (1,'2020-01-01 00:00:00'),(2,'2021-06-15 12:00:00'),(3,NULL)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM b WHERE pk = (SELECT pk FROM b x WHERE x.dt > 1 ORDER BY x.pk LIMIT 1)"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM b WHERE pk = (SELECT pk FROM b x WHERE x.dt > 1 ORDER BY x.pk LIMIT 1 OFFSET 1)"###,
        &[&[r###"2"###]],
        false,
    );
}

#[test]
fn optimizer1108_round2_c05_having_in_subq() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"DROP TABLE IF EXISTS b"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE b (pk INT PRIMARY KEY, dt DATETIME, d DATE)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO b VALUES (1,'2020-01-01 00:00:00','2020-01-01'),(2,'2021-06-15 12:00:00','2021-06-15'),(3,NULL,NULL)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM b WHERE pk IN (SELECT pk FROM b x GROUP BY pk HAVING MAX(x.dt) > 1) ORDER BY pk"###,
        &[&[r###"1"###], &[r###"2"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM b WHERE pk IN (SELECT pk FROM b x GROUP BY pk HAVING MAX(x.d) > 1) ORDER BY pk"###,
        &[&[r###"1"###], &[r###"2"###]],
        true,
    );
}

#[test]
fn optimizer1108_round2_c06_generated_col() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"DROP TABLE IF EXISTS b"###)
        .unwrap();
    engine.execute_with_session(&mut session,r###"CREATE TABLE b (pk INT PRIMARY KEY, d DATE, g_d DATE AS (d + INTERVAL 1 DAY) STORED)"###).unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO b VALUES (1,'2020-01-01',DEFAULT),(2,'2021-06-15',DEFAULT),(3,NULL,DEFAULT)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT (SELECT COUNT(*) FROM b WHERE g_d > 1) q"###,
        &[&[r###"2"###]],
        false,
    );
}

#[test]
fn optimizer1108_round2_c07_union_all() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"DROP TABLE IF EXISTS b"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE b (pk INT PRIMARY KEY, dt DATETIME, tm TIME)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO b VALUES (1,'2020-01-01 00:00:00','10:00:00'),(2,'2021-06-15 12:00:00','08:30:00'),(3,NULL,NULL)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT (SELECT COUNT(*) FROM b WHERE dt > 1) UNION ALL SELECT (SELECT COUNT(*) FROM b WHERE tm > 1)"###,
        &[&[r###"2"###], &[r###"2"###]],
        false,
    );
}

#[test]
fn optimizer1108_round2_c08_partitioned() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"DROP TABLE IF EXISTS pt"###)
        .unwrap();
    engine.execute_with_session(&mut session,r###"CREATE TABLE pt (pk INT PRIMARY KEY, dt DATETIME) PARTITION BY HASH (pk) PARTITIONS 4"###).unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO pt VALUES (1,'2020-01-01 00:00:00'),(2,'2021-06-15 12:00:00'),(3,NULL)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT (SELECT COUNT(*) FROM pt WHERE dt > 1) q"###,
        &[&[r###"2"###]],
        false,
    );
}

#[test]
fn optimizer1108_round2_c09_row_tuple() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"DROP TABLE IF EXISTS b"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE b (pk INT PRIMARY KEY, i INT, dt DATETIME)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO b VALUES (1,3,'2020-01-01 00:00:00'),(2,4,'2021-06-15 12:00:00'),(3,7,NULL)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT pk FROM b WHERE (pk, i) = (SELECT pk, i FROM b x WHERE x.dt > 1 LIMIT 1)"###,
        &[&[r###"1"###]],
        false,
    );
}

#[test]
fn optimizer1108_round2_c10_decimal_literal_1525() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, r###"DROP TABLE IF EXISTS big"###)
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE big (pk INT PRIMARY KEY, dt DATETIME)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO big VALUES (1,'2020-01-01 00:00:00'),(2,'2021-06-15 12:00:00'),(3,NULL)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM big WHERE dt > 1.0"###,
        &[&[r###"2"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT (SELECT COUNT(*) FROM big WHERE dt > 1.5) q"###,
        &[&[r###"2"###]],
        false,
    );
}

#[test]
fn optimizer1108_round2_n11_char_index_pad() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session,r###"CREATE TABLE cq (id INT PRIMARY KEY, ch CHAR(6), v VARCHAR(6), KEY k_ch (ch), KEY k_v (v))"###).unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO cq VALUES (1,'a','a'),(2,'A','A'),(3,'ab','ab'),(4,'',''),(5,'b','b'),(6,NULL,NULL),(7,'a ','a ')"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM cq WHERE ch < 'a ' ORDER BY id"###,
        &[&[r###"1"###], &[r###"2"###], &[r###"4"###], &[r###"7"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM cq IGNORE INDEX (k_ch) WHERE ch < 'a ' ORDER BY id"###,
        &[&[r###"1"###], &[r###"2"###], &[r###"4"###], &[r###"7"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM cq WHERE ch <= 'a' ORDER BY id"###,
        &[&[r###"1"###], &[r###"2"###], &[r###"4"###], &[r###"7"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM cq WHERE ch >= 'a ' ORDER BY id"###,
        &[&[r###"3"###], &[r###"5"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM cq IGNORE INDEX (k_ch) WHERE ch >= 'a ' ORDER BY id"###,
        &[&[r###"3"###], &[r###"5"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM cq WHERE ch > 'a' ORDER BY id"###,
        &[&[r###"3"###], &[r###"5"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM cq WHERE ch BETWEEN 'a ' AND 'b' ORDER BY id"###,
        &[&[r###"3"###], &[r###"5"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM cq IGNORE INDEX (k_ch) WHERE ch BETWEEN 'a ' AND 'b' ORDER BY id"###,
        &[&[r###"3"###], &[r###"5"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM cq WHERE v < 'a ' ORDER BY id"###,
        &[&[r###"1"###], &[r###"2"###], &[r###"4"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM cq IGNORE INDEX (k_v) WHERE v < 'a ' ORDER BY id"###,
        &[&[r###"1"###], &[r###"2"###], &[r###"4"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM cq WHERE ch = 'a ' ORDER BY id"###,
        &[],
        true,
    );
}

#[test]
fn optimizer1108_round2_n15_year_idx() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE yi (id INT PRIMARY KEY, yr YEAR, KEY k_yr (yr))"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO yi VALUES (1,2000),(2,0),(3,2001),(4,NULL),(5,2000),(6,1999)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM yi WHERE yr = '0.1'"###,
        &[&[r###"2"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM yi WHERE yr = '0.1' ORDER BY id"###,
        &[&[r###"1"###], &[r###"5"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM yi IGNORE INDEX (k_yr) WHERE yr = '0.1'"###,
        &[&[r###"2"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM yi WHERE yr = '1.5'"###,
        &[&[r###"0"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM yi WHERE yr = '0.9'"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM yi WHERE yr = 0.1"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM yi WHERE yr = '99'"###,
        &[&[r###"1"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM yi WHERE yr = '2000.4'"###,
        &[&[r###"2"###]],
        false,
    );
}

#[test]
fn optimizer1108_round2_n23_str_vs_date_col() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE sd (id INT PRIMARY KEY, s VARCHAR(20), dt DATETIME, d DATE)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO sd VALUES (1,'2020-01-01','2020-01-01 00:00:00','2020-01-01'),(2,'b','2020-01-01 00:00:00','2020-01-01'),(3,'2021-05-05 10:00:00','2021-05-05 10:00:00','2021-05-05'),(4,NULL,NULL,NULL),(5,'','2020-01-01 00:00:00','2020-01-01')"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sd WHERE s = dt ORDER BY id"###,
        &[&[r###"1"###], &[r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sd WHERE s <> dt ORDER BY id"###,
        &[&[r###"2"###], &[r###"5"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sd WHERE s = d ORDER BY id"###,
        &[&[r###"1"###], &[r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id, s = d, s < dt FROM sd ORDER BY id"###,
        &[
            &[r###"1"###, r###"1"###, r###"0"###],
            &[r###"2"###, r###"0"###, r###"1"###],
            &[r###"3"###, r###"1"###, r###"0"###],
            &[r###"4"###, r###"NULL"###, r###"NULL"###],
            &[r###"5"###, r###"0"###, r###"1"###],
        ],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sd WHERE dt > s ORDER BY id"###,
        &[&[r###"2"###], &[r###"5"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT COUNT(*) FROM sd WHERE s <= dt"###,
        &[&[r###"4"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sd WHERE s = '2020-01-01' ORDER BY id"###,
        &[&[r###"1"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sd WHERE DATE '2020-01-01' = s ORDER BY id"###,
        &[&[r###"1"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sd WHERE d IN (s, '2021-05-05') ORDER BY id"###,
        &[&[r###"1"###], &[r###"3"###]],
        true,
    );
}

#[test]
fn optimizer1108_round2_n47_year_dec_idx() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE y47 (id INT PRIMARY KEY, yr YEAR, KEY k_yr (yr))"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO y47 VALUES (1,1999),(2,2000),(3,2003)"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y47 WHERE yr = 2003.0 ORDER BY id"###,
        &[&[r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y47 WHERE yr <= 2003.5 ORDER BY id"###,
        &[&[r###"1"###], &[r###"2"###], &[r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y47 WHERE yr < 2002.9 ORDER BY id"###,
        &[&[r###"1"###], &[r###"2"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y47 WHERE yr IN (2003.0, 1999) ORDER BY id"###,
        &[&[r###"1"###], &[r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y47 IGNORE INDEX (k_yr) WHERE yr = 2003.0 ORDER BY id"###,
        &[&[r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y47 WHERE yr = 2003 ORDER BY id"###,
        &[&[r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y47 WHERE yr BETWEEN 1999.5 AND 2003.5 ORDER BY id"###,
        &[&[r###"2"###], &[r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y47 WHERE yr > 2002.1 ORDER BY id"###,
        &[&[r###"3"###]],
        true,
    );
}

#[test]
fn optimizer1108_round2_n48_year_dt_cmp() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE y48 (id INT PRIMARY KEY, yr YEAR, d DATE, dt DATETIME)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO y48 VALUES (1,2000,'2000-05-05','2000-05-05 00:00:00'),(2,2003,'2003-01-01','2003-01-01 00:00:00'),(3,2020,'2019-12-31','2019-12-31 23:59:59')"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y48 WHERE yr < d ORDER BY id"###,
        &[&[r###"1"###], &[r###"2"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y48 WHERE yr > d ORDER BY id"###,
        &[&[r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y48 WHERE yr <= d ORDER BY id"###,
        &[&[r###"1"###], &[r###"2"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y48 WHERE yr >= d ORDER BY id"###,
        &[&[r###"3"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id, yr < dt, yr > dt FROM y48 ORDER BY id"###,
        &[
            &[r###"1"###, r###"1"###, r###"0"###],
            &[r###"2"###, r###"1"###, r###"0"###],
            &[r###"3"###, r###"0"###, r###"1"###],
        ],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM y48 WHERE yr = d ORDER BY id"###,
        &[],
        true,
    );
    assert_eq!(
        engine
            .execute_with_session(
                &mut session,
                r###"SELECT id FROM y48 IGNORE INDEX (k_yr) WHERE yr < d ORDER BY id"###
            )
            .and_then(QueryResponse::materialize)
            .unwrap_err()
            .code(),
        1176
    );
}

#[test]
fn optimizer1108_round2_n49_enum_derived() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE e49 (id INT PRIMARY KEY, e ENUM('aa','bb','cc'))"###,
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            r###"INSERT INTO e49 VALUES (1,'aa'),(2,'bb'),(3,'cc')"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM (SELECT id, e FROM e49) t WHERE e = 2"###,
        &[&[r###"2"###]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id, e + 0 FROM (SELECT id, e FROM e49) t WHERE e = 2 ORDER BY id"###,
        &[&[r###"2"###, r###"2"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM e49 WHERE e = 2 ORDER BY id"###,
        &[&[r###"2"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id, e + 0 FROM e49 WHERE e = 2 ORDER BY id"###,
        &[&[r###"2"###, r###"2"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM (SELECT id, e FROM e49) t WHERE e = 'bb' ORDER BY id"###,
        &[&[r###"2"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"WITH c AS (SELECT id, e FROM e49) SELECT id FROM c WHERE e = 2 ORDER BY id"###,
        &[&[r###"2"###]],
        true,
    );
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE VIEW v49 AS SELECT id, e FROM e49"###,
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM v49 WHERE e = 2 ORDER BY id"###,
        &[&[r###"2"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM (SELECT * FROM (SELECT id, e FROM e49) a) b WHERE e = 2 ORDER BY id"###,
        &[&[r###"2"###]],
        true,
    );
}

#[test]
fn optimizer1108_round2_n51_set_binary_cmp() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE sb (id INT PRIMARY KEY, sb SET('a','b','c') COLLATE utf8mb4_bin)"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO sb VALUES (1,'a'),(2,'b'),(3,'a,b'),(4,'b,c'),(5,'a,b,c'),(6,''),(7,NULL)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sb WHERE sb < 'a,b' ORDER BY id"###,
        &[&[r###"1"###], &[r###"6"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sb WHERE sb > 'b' ORDER BY id"###,
        &[&[r###"4"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sb WHERE sb = 'b,a' ORDER BY id"###,
        &[],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sb WHERE sb BETWEEN 'a' AND 'b' ORDER BY id"###,
        &[&[r###"1"###], &[r###"2"###], &[r###"3"###], &[r###"5"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sb WHERE sb = 'q' ORDER BY id"###,
        &[],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sb WHERE sb = 'a,a' ORDER BY id"###,
        &[],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sb WHERE sb IN ('a,a', 'c') ORDER BY id"###,
        &[],
        true,
    );
}

#[test]
fn optimizer1108_round2_n52_ci_set_cmp() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            r###"CREATE TABLE sc (id INT PRIMARY KEY, st SET('a','b','c'))"###,
        )
        .unwrap();
    engine.execute_with_session(&mut session,r###"INSERT INTO sc VALUES (1,'a'),(2,'b'),(3,'a,b'),(4,'b,c'),(5,'a,b,c'),(6,''),(7,NULL)"###).unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sc WHERE st < 'a,b' ORDER BY id"###,
        &[&[r###"1"###], &[r###"6"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sc WHERE st > 'b' ORDER BY id"###,
        &[&[r###"4"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sc WHERE st = 'b,a' ORDER BY id"###,
        &[],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sc WHERE st BETWEEN 'a' AND 'b' ORDER BY id"###,
        &[&[r###"1"###], &[r###"2"###], &[r###"3"###], &[r###"5"###]],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sc WHERE st = 'q' ORDER BY id"###,
        &[],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sc WHERE st = 'a,a' ORDER BY id"###,
        &[],
        true,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        r###"SELECT id FROM sc WHERE st IN ('a,a', 'c') ORDER BY id"###,
        &[],
        true,
    );
}
#[test]
fn optimizer1108_bit_string_functions_use_declared_octets() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE octets(id INT PRIMARY KEY,b BIT(16))",
        "INSERT INTO octets VALUES(1,65),(2,255),(3,0),(4,NULL)",
    ]);
    let mut session = SessionContext::default();
    check_optimizer_rows(&engine,&mut session,"SELECT LENGTH(b),HEX(UPPER(b)),HEX(SUBSTRING(b,1,1)),HEX(LPAD(b,3,'x')),HEX(REPEAT(b,2)) FROM octets ORDER BY id",&[
        &["2","0041","00","780041","00410041"],
        &["2","00FF","00","7800FF","00FF00FF"],
        &["2","0000","00","780000","00000000"],
        &["NULL","NULL","NULL","NULL","NULL"],
    ],true);
}

#[test]
fn optimizer1108_locking_subqueries_release_and_retain_transaction_locks() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE lock_outer(id INT PRIMARY KEY)",
        "INSERT INTO lock_outer VALUES(1),(2)",
        "CREATE TABLE lock_inner(id INT PRIMARY KEY)",
        "INSERT INTO lock_inner VALUES(1),(2)",
    ]);
    let mut first = SessionContext::default();
    let mut second = SessionContext::default();
    second.connection_id = 2;
    engine
        .execute_with_session(&mut second, "SET innodb_lock_wait_timeout=1")
        .unwrap();
    for query in [
        "SELECT id FROM lock_outer WHERE EXISTS(SELECT 1 FROM lock_inner FOR UPDATE)",
        "SELECT (SELECT id FROM lock_inner WHERE id=1 FOR UPDATE) FROM lock_outer",
    ] {
        engine
            .execute_with_session(&mut first, query)
            .and_then(QueryResponse::materialize)
            .unwrap();
        engine
            .execute_with_session(&mut second, "SELECT * FROM lock_inner FOR UPDATE")
            .and_then(QueryResponse::materialize)
            .unwrap();
    }
    engine.execute_with_session(&mut first, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut first,
            "SELECT id FROM lock_outer WHERE EXISTS(SELECT 1 FROM lock_inner FOR UPDATE)",
        )
        .and_then(QueryResponse::materialize)
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut second, "SELECT * FROM lock_inner FOR UPDATE")
            .and_then(QueryResponse::materialize)
            .unwrap_err()
            .code(),
        1205
    );
    engine.execute_with_session(&mut first, "ROLLBACK").unwrap();
    engine
        .execute_with_session(&mut second, "SELECT * FROM lock_inner FOR UPDATE")
        .and_then(QueryResponse::materialize)
        .unwrap();
}

#[test]
fn optimizer1108_cached_limit_validates_each_binding() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE limits(id INT PRIMARY KEY,v INT)",
        "INSERT INTO limits VALUES(1,10),(2,20),(3,30)",
    ]);
    let mut session = SessionContext::default();
    let sql = "SELECT id FROM limits ORDER BY id LIMIT ? OFFSET ?";
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = crate::engine::api::PreparedSelectMemoState::default();
    for (params, expected) in [
        (
            vec![Value::String("2".into()), Value::String("1".into())],
            vec![vec![Value::Int(2)], vec![Value::Int(3)]],
        ),
        (vec![Value::String("0".into()), Value::Int(0)], vec![]),
        (
            vec![Value::Int(1), Value::Int(0)],
            vec![vec![Value::Int(1)]],
        ),
    ] {
        let response = engine
            .execute_prepared_select(&mut session, &mut state, &parsed, sql, &params)
            .unwrap()
            .and_then(QueryResponse::materialize)
            .unwrap();
        let QueryResponse::ResultSet(result) = response else {
            panic!()
        };
        assert_eq!(result.rows, expected);
    }
    for (value, code) in [(Value::Int(-1), 1690), (Value::Double(2.0), 1210)] {
        assert_eq!(
            engine
                .execute_prepared_select(
                    &mut session,
                    &mut state,
                    &parsed,
                    sql,
                    &[value, Value::Int(0)]
                )
                .unwrap()
                .and_then(QueryResponse::materialize)
                .unwrap_err()
                .code(),
            code
        );
    }
}

#[test]
fn optimizer1108_ordered_prepared_dml_limit_parameters() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE limits(id INT PRIMARY KEY,v INT)",
        "INSERT INTO limits VALUES(1,10),(2,20),(3,30)",
    ]);
    let mut session = SessionContext::default();
    for query in [
        "PREPARE upd FROM 'UPDATE limits SET v=v+1 ORDER BY id LIMIT ?'",
        "PREPARE del FROM 'DELETE FROM limits ORDER BY id DESC LIMIT ?'",
        "SET @n='2'",
        "EXECUTE upd USING @n",
    ] {
        engine.execute_with_session(&mut session, query).unwrap();
    }
    check_optimizer_rows(
        &engine,
        &mut session,
        "SELECT id,v FROM limits ORDER BY id",
        &[&["1", "11"], &["2", "21"], &["3", "30"]],
        true,
    );
    for statement in ["upd", "del"] {
        for (binding, code) in [("-1", 1690), ("1.5e0", 1210)] {
            engine
                .execute_with_session(&mut session, &format!("SET @n={binding}"))
                .unwrap();
            let error = engine
                .execute_with_session(&mut session, &format!("EXECUTE {statement} USING @n"))
                .and_then(QueryResponse::materialize)
                .unwrap_err();
            assert_eq!(error.code(), code, "{statement}; {binding}");
        }
    }
    for query in ["SET @n='1'", "EXECUTE del USING @n"] {
        engine.execute_with_session(&mut session, query).unwrap();
    }
    check_optimizer_rows(
        &engine,
        &mut session,
        "SELECT id,v FROM limits ORDER BY id",
        &[&["1", "11"], &["2", "21"]],
        true,
    );
}

#[test]
fn optimizer1108_nlj_inner_comparison_domains() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE u (id INT)",
        "INSERT INTO u VALUES (1),(2),(3)",
        "CREATE TABLE t (a VARCHAR(8), b VARCHAR(8) COLLATE utf8mb4_bin)",
        "INSERT INTO t VALUES ('a','x'),('A','X'),('é','x '),('E','X')",
    ]);
    let mut session = SessionContext::default();
    for query in [
        "SELECT COUNT(*) FROM (SELECT a, COUNT(b) FROM u CROSS JOIN t GROUP BY a) d",
        "SELECT COUNT(*) FROM (SELECT b, COUNT(a) FROM u CROSS JOIN t GROUP BY b) d",
    ] {
        check_optimizer_rows(&engine, &mut session, query, &[&["2"]], false);
    }
}
#[test]
fn optimizer1108_low_memory_domains() {
    with_test_env(
        &[
            ("MYDB_EXECUTOR_MEMORY_TARGET", Some("16")),
            ("MYDB_QUERY_MEM_LIMIT_MB", Some("8")),
            ("MYDB_HASH_JOIN_MEM_LIMIT_MB", Some("1")),
        ],
        || {
            optimizer1108_b03_spill_agg_minmax_collation();
            optimizer1108_b19_parallel_agg_null_split();
        },
    );
}
#[test]
fn optimizer1108_range_join_access_and_semantics() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE ra (id INT PRIMARY KEY, k INT, KEY(k))",
        "CREATE TABLE rb (id INT PRIMARY KEY, k INT, KEY(k))",
        "INSERT INTO ra VALUES (1,0),(2,1),(3,2),(4,NULL)",
        "INSERT INTO rb VALUES (1,0),(2,1),(3,3),(4,NULL)",
    ]);
    let mut session = SessionContext::default();
    for hint in ["", "/*+ INDEX_NLJ(rb) */"] {
        let q = format!("SELECT {hint} ra.id, rb.id FROM ra JOIN rb ON ra.k BETWEEN rb.k AND rb.k+1 ORDER BY ra.id, rb.id");
        check_optimizer_rows(
            &engine,
            &mut session,
            &q,
            &[&["1", "1"], &["2", "1"], &["2", "2"], &["3", "2"]],
            true,
        );
    }
    check_optimizer_rows(&engine, &mut session, "SELECT ra.id, rb.id FROM ra LEFT JOIN rb ON rb.k BETWEEN ra.k AND ra.k+1 ORDER BY ra.id, rb.id",
        &[&["1","1"],&["1","2"],&["2","2"],&["3","3"],&["4","NULL"]], true);
}
#[test]
fn optimizer1108_setop_domain_precedes_nested_coercion() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    for q in [
        "SELECT 1.5 UNION SELECT 2 UNION SELECT '3'",
        "(SELECT 1.5 UNION SELECT 2 LIMIT 10) UNION SELECT '3'",
        "SELECT 1.5 UNION (SELECT 2 UNION SELECT '3' LIMIT 10)",
    ] {
        check_optimizer_rows(&engine, &mut session, q, &[&["1.5"], &["2"], &["3"]], false);
    }
    check_optimizer_rows(
        &engine,
        &mut session,
        "SELECT CAST(2 AS DECIMAL(5,2)) UNION SELECT 'x'",
        &[&["2.00"], &["x"]],
        false,
    );
}
#[test]
fn optimizer1108_temporal_enum_numeric_domain() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE b (id INT, dt DATETIME)",
        "CREATE TABLE o (id INT, e ENUM('a','b','c'))",
        "INSERT INTO b VALUES (1,'2020-01-01'),(2,'2021-06-15 12:00:00'),(3,NULL)",
        "INSERT INTO o VALUES (1,'c'),(2,NULL)",
    ]);
    let mut session = SessionContext::default();
    check_optimizer_rows(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM b JOIN o ON b.id=o.id WHERE b.dt>o.e",
        &[&["1"]],
        false,
    );
    check_optimizer_rows(
        &engine,
        &mut session,
        "SELECT id,(SELECT COUNT(*) FROM b WHERE b.dt>o.e) FROM o ORDER BY id",
        &[&["1", "2"], &["2", "0"]],
        true,
    );
}
#[test]
fn optimizer1108_year_literal_and_cached_domains() {
    for mode in ["off", "on"] {
        with_test_env(&[("MYDB_PLAN_CACHE", Some(mode))], || {
            let (_dir, engine) = fixture(&[
                "CREATE TABLE y (id INT PRIMARY KEY, yr YEAR, KEY(yr))",
                "INSERT INTO y VALUES (1,0),(2,2000),(3,1999),(4,2069),(5,1970)",
            ]);
            let mut session = SessionContext::default();
            for (literal, expected) in [
                ("'00'", "2"),
                ("'0.1'", "2"),
                ("1999.1", "3"),
                ("1999.9", "2"),
            ] {
                check_optimizer_rows(
                    &engine,
                    &mut session,
                    &format!("SELECT id FROM y WHERE yr={literal} ORDER BY id"),
                    &[&[expected]],
                    true,
                );
            }
        });
    }
}

#[test]
fn optimizer1108_session_modes_are_effective_or_rejected() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET optimizer_switch='index_merge=off'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET optimizer_switch='default'")
        .unwrap();
    assert!(session.sysvars.optimizer_switch.contains("index_merge=on"));
    assert_eq!(
        engine
            .execute_with_session(&mut session, "SET sql_mode='PAD_CHAR_TO_FULL_LENGTH'")
            .unwrap_err()
            .code(),
        1235
    );
}
#[test]
fn optimizer1108_recursive_width_is_enforced() {
    let (_dir, engine) = fixture(&[]);
    let error=engine.execute("WITH RECURSIVE r(s) AS (SELECT CAST('x' AS CHAR(1)) UNION ALL SELECT CONCAT(s,'x') FROM r WHERE LENGTH(s)<3) SELECT * FROM r")
        .and_then(QueryResponse::materialize).unwrap_err();
    assert_eq!(error.code(), 1406);
}

#[test]
fn optimizer1108_key_partition_collation_hash_uses_all_buckets() {
    let (_dir, engine) =
        fixture(&["CREATE TABLE kp (s VARCHAR(20)) PARTITION BY KEY(s) PARTITIONS 4"]);
    let values = (0..128)
        .map(|i| format!("('key{i}')"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO kp VALUES {values}"))
        .unwrap();
    let mut session = SessionContext::default();
    let mut total = 0;
    for partition in 0..4 {
        let QueryResponse::ResultSet(rs) = engine
            .execute_with_session(
                &mut session,
                &format!("SELECT COUNT(*) FROM kp PARTITION(p{partition})"),
            )
            .and_then(QueryResponse::materialize)
            .unwrap()
        else {
            panic!("expected rows")
        };
        let count = rs.rows[0][0].to_display_string().parse::<usize>().unwrap();
        assert!(count > 10, "underused partition {partition}: {count}");
        total += count;
    }
    assert_eq!(total, 128);
    engine
        .execute("INSERT INTO kp VALUES ('a'),('A'),('á')")
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM kp WHERE s='a'",
        &[&["3"]],
        false,
    );
}
#[test]
fn optimizer1108_explain_cte_and_derived_estimates() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE ex (id INT PRIMARY KEY,k INT,KEY(k))",
        "INSERT INTO ex VALUES (1,1),(2,1),(3,2),(4,3)",
        "ANALYZE TABLE ex",
    ]);
    for query in [
        "EXPLAIN WITH c AS (SELECT * FROM ex WHERE k=1) SELECT COUNT(*) FROM c",
        "EXPLAIN SELECT * FROM (SELECT id,k FROM ex WHERE k<3) t JOIN ex x ON t.id=x.id",
    ] {
        let QueryResponse::ResultSet(rs) = engine
            .execute(query)
            .and_then(QueryResponse::materialize)
            .unwrap()
        else {
            panic!("expected rows")
        };
        for row in rs.rows {
            assert!(
                row[9].to_display_string().parse::<usize>().unwrap() > 0,
                "{query}: {row:?}"
            );
        }
    }
}
#[test]
fn optimizer1108_cross_join_cardinality_is_product() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE ca (id INT)",
        "CREATE TABLE cb (id INT)",
        "INSERT INTO ca VALUES (1),(2),(3)",
        "INSERT INTO cb VALUES (1),(2),(3),(4)",
        "ANALYZE TABLE ca,cb",
    ]);
    let QueryResponse::ResultSet(rs) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT * FROM ca CROSS JOIN cb")
        .and_then(QueryResponse::materialize)
        .unwrap()
    else {
        panic!("expected rows")
    };
    let tree = rs.rows[0][0].to_display_string();
    assert!(
        tree.lines()
            .any(|line| line.contains("join") && line.contains("rows=12")),
        "{tree}"
    );
}
#[test]
fn optimizer1108_forced_parallel_cost_invariants() {
    with_test_env(
        &[
            ("MYDB_PARALLEL_THRESHOLD_ROWS", Some("1")),
            ("MYDB_PARALLEL_MIN_PAGES", Some("0")),
            ("MYDB_NLJ_OUTER_PARALLEL_THRESHOLD", Some("1")),
        ],
        || {
            let (_dir, engine) = fixture(&[
                "CREATE TABLE d (k INT,v INT)",
                "INSERT INTO d VALUES (1,1),(1,2),(2,1)",
                "CREATE TABLE pd (id INT PRIMARY KEY)",
                "INSERT INTO pd VALUES (1),(2),(3)",
            ]);
            let mut session = SessionContext::default();
            check_optimizer_rows(&engine,&mut session,"WITH c AS (SELECT k FROM d) SELECT 1 FROM pd WHERE EXISTS (SELECT 1 FROM c) LIMIT 1",&[&["1"]],false);
            check_optimizer_rows(&engine,&mut session,"WITH c AS (SELECT k FROM d) SELECT COUNT(*) FROM pd WHERE EXISTS (SELECT 1 FROM c) AND EXISTS (SELECT 1 FROM c c2)",&[&["3"]],false);
        },
    );
}

#[test]
fn optimizer1108_routine_variable_slots_retain_temporal_comparison_semantics() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE rt (id INT PRIMARY KEY,dt DATETIME,KEY kd(dt))",
        "INSERT INTO rt VALUES (1,'2020-01-01'),(2,'2021-01-01'),(3,NULL)",
    ]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE PROCEDURE rt_count(IN x INT) BEGIN
         SELECT COUNT(*) FROM rt FORCE INDEX(kd) WHERE dt>x;
         SELECT COUNT(*) FROM rt IGNORE INDEX(kd) WHERE dt>x;
         SELECT COUNT(*) FROM (SELECT dt FROM rt LIMIT 18446744073709551615) d WHERE dt>x;
         END",
        )
        .unwrap();
    for input in ["1", "-1", "'1'", "1"] {
        let response = engine
            .execute_with_session(&mut session, &format!("CALL rt_count({input})"))
            .and_then(QueryResponse::materialize)
            .unwrap();
        let QueryResponse::MultiResult(results) = response else {
            panic!("expected CALL results")
        };
        let rows = results
            .into_iter()
            .filter_map(|result| match result {
                QueryResponse::ResultSet(result) => Some(result.rows),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![vec![Value::Int(2)]]; 3], "{input}");
    }
    for query in [
        "CREATE PROCEDURE rt_dynamic(IN x INT) BEGIN
         SET @rt_value=x;
         PREPARE rt_stmt FROM 'SELECT COUNT(*) FROM rt WHERE dt>?';
         EXECUTE rt_stmt USING @rt_value;
         DEALLOCATE PREPARE rt_stmt;
         END",
        "CREATE PROCEDURE rt_nested(IN x INT) BEGIN CALL rt_dynamic(x); END",
    ] {
        engine.execute_with_session(&mut session, query).unwrap();
    }
    let response = engine
        .execute_with_session(&mut session, "CALL rt_nested(1)")
        .and_then(QueryResponse::materialize)
        .unwrap();
    fn check_dynamic_result(response: &QueryResponse) -> usize {
        match response {
            QueryResponse::ResultSet(result) => {
                assert_eq!(result.rows, vec![vec![Value::Int(0)]]);
                1
            }
            QueryResponse::MultiResult(results) => results.iter().map(check_dynamic_result).sum(),
            _ => 0,
        }
    }
    assert_eq!(check_dynamic_result(&response), 1);
    engine
        .execute_with_session(
            &mut session,
            "CREATE FUNCTION rt_function(x INT) RETURNS INT READS SQL DATA
         RETURN (SELECT COUNT(*) FROM rt WHERE dt>x)",
        )
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        "SELECT rt_function(1)",
        &[&["2"]],
        true,
    );
    engine
        .execute_with_session(&mut session, "CREATE TABLE rt_log(n INT)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TRIGGER rt_trigger AFTER INSERT ON rt FOR EACH ROW
         INSERT INTO rt_log SELECT COUNT(*) FROM rt WHERE dt>NEW.id",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO rt VALUES (4,'2022-01-01')")
        .unwrap();
    check_optimizer_rows(
        &engine,
        &mut session,
        "SELECT n FROM rt_log",
        &[&["3"]],
        true,
    );
}

#[test]
fn optimizer1108_year_between_constant_bounds_follow_year_domain() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE yb(id INT PRIMARY KEY,y YEAR,KEY ky(y))",
        "INSERT INTO yb VALUES (1,1999),(2,2000),(3,2003),(4,NULL),(5,0)",
    ]);
    let mut session = SessionContext::default();
    for source in [
        "yb FORCE INDEX(ky)",
        "yb IGNORE INDEX(ky)",
        "(SELECT * FROM yb LIMIT 18446744073709551615) d",
    ] {
        for bounds in ["99 AND 3", "'99' AND '03'", "1999 AND 2003"] {
            check_optimizer_rows(
                &engine,
                &mut session,
                &format!("SELECT id FROM {source} WHERE y BETWEEN {bounds} ORDER BY id"),
                &[&["1"], &["2"], &["3"]],
                true,
            );
            check_optimizer_rows(
                &engine,
                &mut session,
                &format!("SELECT id,y BETWEEN {bounds} FROM {source} ORDER BY id"),
                &[
                    &["1", "1"],
                    &["2", "1"],
                    &["3", "1"],
                    &["4", "NULL"],
                    &["5", "0"],
                ],
                true,
            );
        }
    }
}

#[test]
fn optimizer1108_lifted_enum_set_keeps_numeric_and_string_domains() {
    for mode in ["on", "off"] {
        with_test_env(&[("MYDB_SUBQUERY_PLAN_CACHE", Some(mode))], || {
            let (_dir, engine) = fixture(&[
                "CREATE TABLE eb(id INT PRIMARY KEY,i INT,tu TINYINT UNSIGNED,e ENUM('a','b','c'))",
                "INSERT INTO eb VALUES (1,3,1,'a'),(2,NULL,2,'b'),(3,7,NULL,'c')",
                "CREATE TABLE eo(id INT PRIMARY KEY,e ENUM('a','b','c'),s SET('a','b','c'))",
                "INSERT INTO eo VALUES (1,'c','a,b'),(2,'b','b'),(3,NULL,NULL)",
            ]);
            let mut session = SessionContext::default();
            for residual in ["", " AND EXISTS(SELECT 1)"] {
                for outer_column in ["o.e", "o.s"] {
                    check_optimizer_rows(
                        &engine,
                        &mut session,
                        &format!(
                            "SELECT o.id,
                  (SELECT COUNT(*) FROM eb WHERE eb.i>{outer_column}{residual}),
                  (SELECT COUNT(*) FROM eb WHERE eb.tu>{outer_column}{residual})
                  FROM eo o ORDER BY o.id"
                        ),
                        &[&["1", "1", "0"], &["2", "2", "0"], &["3", "0", "0"]],
                        true,
                    );
                }
                check_optimizer_rows(&engine, &mut session,
            &format!("SELECT o.id,(SELECT COUNT(*) FROM eb WHERE eb.e>o.e{residual}) FROM eo o ORDER BY o.id"),
            &[&["1", "0"], &["2", "1"], &["3", "0"]], true);
            }
        });
    }
}
