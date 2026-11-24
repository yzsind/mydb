-- ============================================================
-- mydb 聚合与 ROLLUP Bug 复现脚本
-- 在 mydb 上执行(建议加 --force 以便出错后继续): 
--   mysql -h127.0.0.1 -P4837 -uroot -proot --force --batch < repro_all_bugs.sql
-- 每个用例标注对应 bug 编号(见审查报告 docs-internal/agg_rollup_audit_20261123.md)
-- ============================================================

DROP DATABASE IF EXISTS agg_repro;
CREATE DATABASE agg_repro;
USE agg_repro;

CREATE TABLE t1 (a INT NULL, b VARCHAR(20) NULL, c INT NULL, s VARCHAR(10) NULL);
INSERT INTO t1 VALUES
 (1,'x',10,'a'),(1,'y',20,'a'),(1,NULL,30,'A'),
 (2,'x',40,'b'),(2,NULL,50,NULL),
 (NULL,'y',60,NULL),(NULL,NULL,70,NULL),
 (3,'x',-10,NULL),(3,'y',NULL,NULL);

-- ============================================================
-- [BUG-1] ROLLUP 输出行序与 MySQL 不一致 (高 / 兼容性-正确性)
-- MySQL: 每组明细后紧跟该组汇总行, 总计行最后
-- mydb:  全部明细行在前, 各级汇总行按层级追加在后, 总计行最后
-- 预期(MySQL 8.0.46 实测输出):
--   NULL NULL 70 / NULL y 60 / NULL NULL 130 / 1 NULL 30 / 1 x 10 / 1 y 20 /
--   1 NULL 60 / 2 ... / 3 ... / NULL NULL 270
-- mydb 实际: NULL NULL 70 / NULL y 60 / 1 NULL 30 / ... / NULL NULL 130 /
--   1 NULL 60 / 2 NULL 90 / 3 NULL -10 / NULL NULL 270
-- ============================================================
SELECT a, b, SUM(c) FROM t1 GROUP BY a, b WITH ROLLUP;

-- 大表向量化/并行路径同样受影响
CREATE TABLE seq (i INT PRIMARY KEY);
INSERT INTO seq WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i < 1000) SELECT i FROM s;
CREATE TABLE tbig (g INT, h INT, val DECIMAL(10,2));
INSERT INTO tbig SELECT (i % 7), (i % 3), i * 0.01 FROM seq;
SELECT g, h, COUNT(*) FROM tbig GROUP BY g, h WITH ROLLUP LIMIT 10;
-- mydb: (0,NULL) 汇总行不在 (0,*) 明细之后, 而是出现在全部明细之后
DROP TABLE seq; DROP TABLE tbig;

-- ============================================================
-- [BUG-2] SELECT * + GROUP BY 绕过 ONLY_FULL_GROUP_BY 校验,
--         并被重写为 group_N 分组键列 (高 / 正确性)
-- MySQL: ERROR 1055 (42000)
-- mydb:  返回单列/两列名为 group_0, group_1 的错误形状结果
-- ============================================================
SELECT * FROM t1 GROUP BY a;
SELECT * FROM t1 GROUP BY a WITH ROLLUP;

-- ============================================================
-- [BUG-2a] 限定通配符(q.*) + GROUP BY + WITH ROLLUP => 协议损坏 (高 / 崩溃-协议)
-- 放在脚本最后: 该语句会使当前连接协议失步, 之后同连接的查询全部失败
-- (Lost connection)。服务器进程不崩溃, 新连接不受影响。
-- MySQL: ERROR 1055 (42000)
-- mydb:  ERROR 2027 Malformed packet —— 结果集元数据列数与行包列数不一致。
--        仅当 q.* 是唯一投影项且带 WITH ROLLUP 时触发。
-- 根因: 元数据按分组键重写(1列), 行物化按 select/mod.rs:774 的 QualifiedWildcard
--        展开输出全部字段 -> 行包字段数超元数据声明
-- ============================================================

-- ============================================================
-- [BUG-3] DECIMAL 大数值 STD/VARIANCE 家族结果错误 (高 / 正确性-数值精度)
-- MySQL: 0 (两个输入按正确舍入转换为同一个 double)
-- mydb:  VARIANCE=0.0000000000018189894035458565, VAR_SAMP=0.000000000003637978807091713
-- 同一数据加 GROUP BY 后 mydb 又返回 0 (内部路径不一致)
-- 根因: hash_agg.rs update_extra_from_typed_lane 的 data[row] as f64 / factor
--       双重舍入转换 (正确做法为 parts_to_f64 的 format+parse)
-- ============================================================
CREATE TABLE tvar (x DECIMAL(20,6));
INSERT INTO tvar VALUES (10000000000.000001),(10000000000.000002);
SELECT STD(x), VARIANCE(x), STDDEV_POP(x), STDDEV_SAMP(x), VAR_POP(x), VAR_SAMP(x) FROM tvar;
SELECT id, VARIANCE(x) FROM (SELECT 1 id, x FROM tvar) t GROUP BY id;  -- 此路径正确(0)

-- ============================================================
-- [BUG-4] BIT_AND/BIT_OR/BIT_XOR 输入全 NULL 时返回初值而非 NULL (中 / 正确性)
-- MySQL: NULL, NULL, NULL
-- mydb:  18446744073709551615, 0, 0
-- 根因: aggregate/mod.rs finalize_aggregate! 中 bits_seen=false 分支
-- ============================================================
SELECT BIT_AND(x), BIT_OR(x), BIT_XOR(x) FROM (SELECT NULL x UNION ALL SELECT NULL) t;
SELECT a, BIT_AND(c) FROM (SELECT 1 a, NULL c UNION ALL SELECT 1, NULL) t GROUP BY a;

-- ============================================================
-- [BUG-5] ROLLUP 汇总行分组键显示值与 MySQL 不同 (中 / 兼容性)
-- 组内值 'a' 与 'A' 在 utf8mb4_0900_ai_ci 下相等:
-- MySQL 组键显示 'a' (HEX 61, 首见值), mydb 显示 'A' (HEX 41)
-- ============================================================
CREATE TABLE tstr (s VARCHAR(10));
INSERT INTO tstr VALUES ('a'),('a '),('A');
SELECT s, HEX(s), COUNT(*) FROM tstr GROUP BY s WITH ROLLUP;
SELECT s, HEX(s), COUNT(*) FROM tstr GROUP BY s;  -- 无 ROLLUP 时 mydb 显示 'a', 正确

-- ============================================================
-- [BUG-6] GROUP BY 序号指向常量投影时, ROLLUP 总计行未置 NULL (低 / 兼容性)
-- MySQL: (1,9) 后跟 (NULL,9);  mydb: (1,9) 后跟 (1,9)
-- 注意: 用别名 GROUP BY x 时 mydb 行为正确, 仅序号指向常量场景受影响
-- ============================================================
SELECT 1 AS x, COUNT(*) FROM t1 GROUP BY 1 WITH ROLLUP;

-- ============================================================
-- [BUG-7] 错误码差异: GROUPING 非法参数 (低 / 兼容性)
-- MySQL: ERROR 3602 "Argument #1 of GROUPING function is not in GROUP BY"
-- mydb:  ERROR 3580 "argument ... of GROUPING function is not in GROUP BY"
-- ============================================================
SELECT a, GROUPING(b) FROM t1 GROUP BY a WITH ROLLUP;

-- ============================================================
-- [BUG-8] GROUP_CONCAT 超长截断缺少 Warning 1260 (低 / 兼容性)
-- 结果一致('a,bb,'), 但 MySQL 额外产生 Warning 1260 "Row 3 was cut by GROUP_CONCAT()",
-- mydb SHOW WARNINGS 为空
-- ============================================================
SET SESSION group_concat_max_len = 5;
SELECT GROUP_CONCAT(s ORDER BY s) FROM (SELECT 'a' s UNION ALL SELECT 'bb' UNION ALL SELECT 'ccc' UNION ALL SELECT 'dddd') t;
SHOW WARNINGS;
SET SESSION group_concat_max_len = DEFAULT;

-- ============================================================
-- [不修复] 各类已核实的行为差异(双方均符合规范或 mydb 方案更优):
-- - GROUP_CONCAT ORDER BY 对 collation 相等键的 tie 顺序 (未定义行为)
-- - 空表 GROUP BY 输出行序 (未定义行为)
-- - CAST(x AS BIGINT) mydb 支持, MySQL 仅 SIGNED/UNSIGNED (mydb 更优)
-- - 错误消息文本措辞差异 (错误码一致, 客户端按码处理)
-- - EXPLAIN 输出格式差异 (mydb 自有算子显示)
-- ============================================================

-- BUG-2a 触发语句(连接将被污染, 必须最后执行):
SELECT s.* FROM t1 s GROUP BY s.a WITH ROLLUP;
SELECT t1.* FROM t1 GROUP BY a WITH ROLLUP;
SELECT d.* FROM (SELECT a, b FROM t1) d GROUP BY d.a WITH ROLLUP;
