"""Run isolated audit SQL probes; emit one JSON record per statement."""
import argparse
import json
import pymysql

p = argparse.ArgumentParser()
p.add_argument('--port', type=int, required=True)
p.add_argument('--password', default='')
a = p.parse_args()
c = pymysql.connect(host='127.0.0.1', port=a.port, user='root', password=a.password, autocommit=True)
probes = [
('setup', 'DROP DATABASE IF EXISTS `auditfix1127:a`'),
('setup', 'DROP DATABASE IF EXISTS auditfix1127'),
('setup', 'CREATE DATABASE auditfix1127'), ('setup', 'USE auditfix1127'),
('version', 'SELECT VERSION()'),
('J1', '''SELECT JSON_SET('{"a":1}', '$.a[0]', 9),JSON_REPLACE('{"a":1}', '$.a[last-1]', 9),JSON_INSERT('{"a":1}', '$.a[last-1]', 9)'''),
('J2', '''SELECT JSON_REMOVE('{"a":1}', '$.a[0]'),JSON_REMOVE('1', '$[0]')'''),
('J3', '''SELECT JSON_ARRAY_INSERT('{"a":1}', '$.a[0]', 0)'''),
('J4', '''SELECT JSON_EXTRACT('null','$') IS NULL,IFNULL(JSON_EXTRACT('null','$'),'d'),COALESCE(JSON_EXTRACT('null','$'),'d')'''),
('J8', '''SELECT JSON_VALUE('{"a":[1,2]}', '$.a[*]')'''),
('J8', '''SELECT JSON_SEARCH('{"a":{"b":"xx"}}', 'one', 'xx', NULL, '$**.b')'''),
('J9', 'SELECT JSON_QUOTE(123)'),
('J10', '''SELECT JSON_UNQUOTE('"a"b"')'''),
('J11', "SELECT JSON_VALUE('abc', '$')"),
('J12', "SELECT JSON_VALUE('2.5', '$' RETURNING SIGNED),CAST(JSON_EXTRACT('[2.5]', '$[0]') AS SIGNED)"),
('J13', 'SELECT JSON_VALID(123),JSON_VALID(1.5),JSON_VALID(TRUE)'),
('J5', "CREATE TABLE mvo(id INT PRIMARY KEY,j JSON,KEY mv((CAST(j->'$.x' AS UNSIGNED ARRAY))))"),
('J5', '''INSERT INTO mvo VALUES(1,'{"x":{"a":1}}')'''),
('J6', 'CREATE TABLE mvc(id INT PRIMARY KEY,j JSON,KEY mv((CAST(j AS CHAR(64) ARRAY))))'),
('J6', '''INSERT INTO mvc VALUES(1,'["a"]'),(2,'["b"]')'''),
('J6', "PREPARE p FROM 'SELECT id FROM mvc WHERE ? MEMBER OF(j)'"),
('J6', "SET @p='a'"),('J6', 'EXECUTE p USING @p'),('J6', "SELECT id FROM mvc WHERE 'a' MEMBER OF(j)"),
('J7', 'CREATE TABLE mvp(id INT PRIMARY KEY,vc VARCHAR(10),j JSON,KEY mv(vc,(CAST(j AS UNSIGNED ARRAY))))'),
('J7', '''INSERT INTO mvp VALUES(1,'01','[7]'),(2,'1','[7]')'''),
('J7', 'SELECT id FROM mvp WHERE vc=1 AND 7 MEMBER OF(j) ORDER BY id'),
('T1', 'CREATE TABLE g1(id INT PRIMARY KEY AUTO_INCREMENT,a INT,b INT AS(a*2) STORED)'),
('T1', 'CREATE TRIGGER g1_bi BEFORE INSERT ON g1 FOR EACH ROW SET NEW.b=99'),
('T1', 'INSERT INTO g1(a) VALUES(2)'),('T1', 'SELECT * FROM g1'),
('T7', 'CREATE TABLE mb(id INT PRIMARY KEY,x INT)'),
('T7', 'INSERT INTO mb VALUES(1,10),(2,20),(3,30)'),
('T7', 'CREATE VIEW vm AS SELECT * FROM mb WHERE x>0 WITH CASCADED CHECK OPTION'),
('T7', 'UPDATE IGNORE vm SET x=x-15'),('T7', 'SHOW WARNINGS'),('T7', 'SELECT * FROM mb ORDER BY id'),
('T7', 'INSERT IGNORE INTO vm VALUES(2,99) ON DUPLICATE KEY UPDATE x=x-15'),
('T7', 'SHOW WARNINGS'),('T7', 'SELECT * FROM mb ORDER BY id'),
('T9', 'CREATE TABLE od(id INT PRIMARY KEY,v INT)'),('T9', 'INSERT INTO od VALUES(1,10)'),
('T9', 'INSERT INTO od VALUES(1,20) ON DUPLICATE KEY UPDATE v=NEW.v+1'),
('T9', 'UPDATE od SET v=NEW.v WHERE id=1'),('T9', 'DELETE FROM od WHERE v=OLD.v'),
('T12', 'CREATE TABLE t1(a INT PRIMARY KEY)'),('T12', 'CREATE TABLE t2(a INT PRIMARY KEY)'),
('T12', 'INSERT INTO t1 VALUES(1),(2)'),('T12', 'INSERT INTO t2 VALUES(1)'),
('T12', 'CREATE VIEW vj AS SELECT t1.a FROM t1 JOIN t2 ON t1.a=t2.a WITH CASCADED CHECK OPTION'),
('T12', 'UPDATE vj SET a=-5 WHERE a=1'),('T12', 'SELECT * FROM t1 ORDER BY a'),
('T13', 'CREATE DATABASE `auditfix1127:a`'),('T13', 'CREATE TABLE `auditfix1127:a`.t(i INT)'),
('T13', 'CREATE TABLE t(i INT)'),
('T13', 'CREATE TRIGGER `auditfix1127:a`.b BEFORE INSERT ON `auditfix1127:a`.t FOR EACH ROW SET @c1=1'),
('T13', 'CREATE TRIGGER `a:b` BEFORE INSERT ON t FOR EACH ROW SET @c2=2'),
('T13', 'DROP TRIGGER `a:b`'),
('T13', "SELECT TRIGGER_SCHEMA,TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='auditfix1127:a'"),
('DEV8', '''SELECT * FROM JSON_TABLE('{}', '$' COLUMNS(v VARCHAR(20) PATH '$.v' DEFAULT 'null' ON EMPTY)) jt'''),
]
for ident, sql in probes:
    result = dict(id=ident, sql=sql)
    try:
        with c.cursor() as s:
            s.execute(sql)
            result.update(rows=s.fetchall(), affected=s.rowcount)
    except pymysql.MySQLError as e:
        result.update(error=e.args)
    print(json.dumps(result, default=str), flush=True)
c.close()
