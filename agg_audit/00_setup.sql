DROP DATABASE IF EXISTS agg_audit;
CREATE DATABASE agg_audit;
USE agg_audit;

-- 基础表: 含 NULL、重复值、多种类型
CREATE TABLE t1 (
  a INT NULL,
  b VARCHAR(20) NULL,
  c INT NULL,
  d DECIMAL(10,2) NULL,
  e DOUBLE NULL,
  f DATE NULL,
  g BIGINT NULL,
  h CHAR(5) NULL
);
INSERT INTO t1 VALUES
 (1,'x',10,1.50,1.5,'2024-01-01',100,'a'),
 (1,'y',20,2.50,2.5,'2024-01-02',200,'b'),
 (1,NULL,30,3.50,3.5,NULL,300,NULL),
 (2,'x',40,4.50,4.5,'2024-01-01',400,''),
 (2,NULL,50,5.50,5.5,'2024-01-03',500,'c'),
 (NULL,'y',60,6.50,6.5,'2024-01-01',600,'d'),
 (NULL,NULL,70,7.50,7.5,NULL,700,NULL),
 (3,'x',-10,-1.50,-1.5,'2024-02-01',-100,'e'),
 (3,'y',NULL,NULL,NULL,NULL,NULL,NULL);

CREATE TABLE t2 (k INT, v VARCHAR(10));
INSERT INTO t2 VALUES (1,'p'),(1,'q'),(2,'r'),(3,NULL),(NULL,'s');

-- 边界类型表
CREATE TABLE tnum (
  id INT,
  ti TINYINT,
  si SMALLINT,
  mi MEDIUMINT,
  bi BIGINT,
  fl FLOAT,
  dl DOUBLE,
  de DECIMAL(20,6),
  du DECIMAL(30,0),
  bl BOOLEAN,
  bt BIT(8)
);
INSERT INTO tnum VALUES
 (1, 127, 32767, 8388607, 9223372036854775807, 1.5, 1.5, 123.456789, 999999999999999999999999999999, TRUE, b'101'),
 (2, -128, -32768, -8388608, -9223372036854775808, -2.5, -2.5, -987.654321, -888888888888888888888888888888, FALSE, b'011'),
 (3, 0, 0, 0, 0, 0.0, 0.0, 0.000001, 0, NULL, b'000'),
 (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- 字符串表（含空串、尾随空格、大小写）
CREATE TABLE tstr (s VARCHAR(10), cs CHAR(10), tx TEXT);
INSERT INTO tstr VALUES
 ('a','a','x'),
 ('a ','a ','y'),
 ('A','A','z'),
 ('',' ',''),
 (NULL,NULL,NULL),
 ('b','b','w');

-- 空表
CREATE TABLE tempty (a INT, b VARCHAR(10));

-- 大表用于向量化/并行路径
CREATE TABLE seq (i INT PRIMARY KEY);
INSERT INTO seq WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < 1000) SELECT i FROM s;
CREATE TABLE seq20 (i INT);
INSERT INTO seq20 VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19);
CREATE TABLE tbig (g INT, h INT, val DECIMAL(10,2), s VARCHAR(8));
INSERT INTO tbig SELECT ((a.i + 1000*b.i) % 97), ((a.i + 1000*b.i) % 13), ((a.i + 1000*b.i) % 1000) * 0.01, CONCAT('v', (a.i + 1000*b.i) % 7)
FROM seq a CROSS JOIN seq20 b;
DROP TABLE seq; DROP TABLE seq20;

-- 聚合函数覆盖表
CREATE TABLE tagg (id INT, x INT, f DOUBLE, de DECIMAL(15,4), s VARCHAR(30), dt DATETIME, blobty VARBINARY(20));
INSERT INTO tagg VALUES
 (1, 10, 1.25, 100.5000, 'apple', '2024-01-01 10:00:00', x'4142'),
 (1, 20, -3.5, -200.2500, 'banana', '2024-01-02 11:00:00', x'43'),
 (1, NULL, 5.0, NULL, NULL, NULL, NULL),
 (2, 30, 2.75, 300.0000, 'cherry', '2024-01-03 12:00:00', x'4142'),
 (2, -40, 0.0, 0.0000, 'Apple', '2024-06-30 23:59:59', x'00'),
 (3, 0, NULL, 999.9999, 'banana', NULL, x'FF');
