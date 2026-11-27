# 四份审计的最终复核与修复验证

日期：2026-11-27。基线 `75fb742`，工作分支 `codex/bugfix-20261127`。所有验证均在本机进行，没有使用测试服务器，没有修改 Cargo 编译配置。

原始报告作为历史证据保留。本次以当前源码和重新执行的 MySQL 8.0.46 对照为准，不直接采用原报告对 MySQL 行为的推断。执行计划见 [plan.md](plan.md)。重现脚本和结果快照位于本目录，完整运行日志及独立实例位于 `/tmp/mydb/audit-fixes/`。

## JSON 数据与索引

| 原条目 | 本次判定 | 处置与验证 |
| --- | --- | --- |
| BUG-1、2、3 | **误报** | MySQL 的标量 SET/REPLACE/REMOVE/ARRAY_INSERT 结果与 mydb 相同。保留正确的标量与数组语义，不按报告建议改变文档结构。 |
| BUG-4 | **当前已正确** | MySQL 与当前 mydb 的 IS NULL 均为 0，IFNULL/COALESCE 均保留 JSON `null`，区别于 SQL NULL。 |
| BUG-5 | **错误身份差异存在，但原建议错误** | MySQL 对 JSON OBJECT→ARRAY 实测报 **1235**，并非报告的 3903。改为明确的不支持错误；失败插入不留行。 |
| BUG-6 | **确认并修复** | prepared 探测与持久索引使用同一个 `index_expression_param_target`。CHAR、DATE、DATETIME、TIME、DECIMAL、SIGNED、UNSIGNED 共 50 次二进制 prepared/扫描对照一致，包括重复绑定、无匹配值和复合前缀参数。 |
| BUG-7 | **确认并修复** | 复合多值索引前缀复用通用比较域守卫。`vc=1` 不再漏掉 `'01'`/`'1.0'`。MySQL 8.0.46 本身也支持复合多值索引，原报告关于其不支持的说明不正确。 |
| BUG-8 | **误报** | MySQL 允许所列 JSON_VALUE/JSON_SEARCH 通配路径；返回结果与 mydb 相同。 |
| BUG-9 | **误报** | MySQL 的 `JSON_QUOTE(123)` 同样报 3064，不会自动字符串化。 |
| BUG-10 | **误报** | MySQL 的 `JSON_UNQUOTE('"a"b"')` 同样报 3141。严格解析保留。 |
| BUG-11 | **误报** | MySQL 的两参数 `JSON_VALUE('abc','$')` 同样返回 SQL NULL。 |
| BUG-12 | **误报** | MySQL 实测 JSON double 2.5→SIGNED 为 2，原有 ties-to-even 行为保留。 |
| BUG-13 | **误报** | MySQL 对数字/布尔 SQL 参数的 JSON_VALID 也返回 0。 |
| DEV-1、2、6、9、10 | **保留设计** | 保留深度上限边界与宽容路径语法、负零规范化、超 u64 整数精度、mydb 编码尺寸及无原地空洞时 STORAGE_FREE=0，不引入精度损失或伪造物理统计。 |
| DEV-3、4、7 | **不需在本次改动** | 索引显示或失败诊断差异，无静默写错；保持明确错误。 |
| DEV-5 | **误报** | MySQL 8.0.46 实际接受常规列与多值键组成的复合索引。 |
| DEV-8 | **确认并修复** | JSON_TABLE 默认值先严格解析 JSON；字符及二进制目标保留 JSON null/boolean 的文本，JSON 字符串仍正确去引号。`DEFAULT 'null'`→字符串 `null`，`DEFAULT '"ab"'`→字符串 `ab`；数值目标的 JSON null 默认值报 3156，时间目标报 3669，JSON 目标保持 JSON null。 |

报告中的历史 MVI 可见性疑点没有被当作已确认 bug；当前已有 `json_multi_valued_repairs_preserve_keys_retained_across_partial_updates` 等历史/回滚覆盖，本次没有通过未经复现的推测修改 MVCC 索引逻辑。

## 视图与触发器

| 原条目 | 本次判定 | 处置与验证 |
| --- | --- | --- |
| T-1 | **确认并修复** | BEFORE 触发器对生成列 NEW 的赋值被忽略，后续语句继续执行，最终基列决定 STORED/VIRTUAL 生成值；AFTER 的 NEW 写保护及直接 DML 3105 保留。更新原先断言错误行为的旧回归。 |
| T-7 | **部分已修复，剩余 ODKU 已修复** | 基线批量 UPDATE IGNORE 已正确；本次补全 ODKU IGNORE 对 CHECK OPTION 的逐行跳过和 1369 warning，覆盖有/无触发器及混合成功/失败行。普通非 IGNORE 仍失败并回滚。 |
| T-9 | **确认并修复** | VALUES ODKU 和单表 UPDATE/DELETE 在扫描之前校验限定符，空表也报 1054；合法表别名和 `AS new` 保留。INSERT SELECT 使用已有 SELECT 源作用域绑定，覆盖合法 `s.col` 与非法 `NEW.col`。 |
| T-13 | **确认并修复** | trigger/event/routine 键在包含冒号的组件上使用长度编码，避免跨库对象碰撞。DD 重建使用相同键函数；DROP 隔离和持久化重开回归通过。 |
| T-12 | **既有明确限制，当前不新增功能** | `docs/mysql80-compatibility.md` 已明确限制连接视图上的 CHECK OPTION/RETURNING 及其嵌套形式；CREATE 返回 1235，没有绕过检查或静默写入。单独放开 CREATE 会使后续多表更新缺少正确的连接后像检查，因此本次 bug 修复保持 fail-loud 边界。这里不是宣称 mydb 比 MySQL 更强，完整连接视图 CHECK OPTION 属于后续功能扩展。 |
| T-4、5、6、11 | **不需修复** | 明确的语法错误、warning/1050 文案及可重执行的规范化 SHOW CREATE 格式差异，不改变数据与权限语义。 |

报告已排除的 definer、SIGNAL、FK 触发器、语句失败回滚、视图星号展开等行为均不按 bug 修改。源码建议 G2（回收站显式 omitted 触发器）、G5（同一 catalog 写锁保护下的消失分支）、N5/N6/N7（未复现的防御分支或内部命名清理）不升级为已确认缺陷，当前没有据推测改动其语义。

## ACID 与内存

**BUG-01 确认并修复，根因比原报告更具体。** 原执行计划是范围索引嵌套循环连接。MRR 结果缓存只限制条目数（默认 8192），每个条目却可保有整个内表范围；数千万个索引项未纳入查询内存记账。

修复为按实际向量容量、变长索引键、范围边界及容器开销记账；每个缓存最多使用查询额度的 1/8，且不超过 64 MiB。可选缓存不足时逐出/不缓存，不把缓存准入失败变成查询错误。LRU 与原有非 LRU 模式均受同一字节上限约束，释放缓存时释放记账。不更改物理采样，不放宽 SQL/进程硬上限，不借用回滚应急额度。

- 基线：8000 行自连接在约 32.67 秒后报 1037，错误中 physical used=16,499,654,136，controlled≈4.3MB；第二次查询仍产生巨大物理压力，KILL QUERY 终止它。vmmap 显示大量真实堆与压缩/交换占用，不能只按 RSS 把它归为采样错误。
- 修复后：在 256 MiB advisory target、32 MiB 查询水位下，同一查询连续两次成功返回 **32,004,000**，耗时约 **46.49 / 45.05 秒**；随后 SELECT、BEGIN/UPDATE/ROLLBACK 均正常，进程 RSS 回落至约 123 MiB。
- 缓存字节边界单元回归覆盖 1000 个大范围结果、LRU/非 LRU 和完全释放后的预算归零。
- ACID 原子性/持久性保护不变，没有通过绕开准入或关闭 fail-stop 掩盖问题。

备注-01 还存在报告错误：MySQL 8.0 的行锁视图是 `performance_schema.data_locks`，访问 `information_schema.data_locks` 也报未知表。额外锁可观测性不作为 ACID 正确性修复。备注-02 原报告已经确认 KILL 测试是客户端错误复用，不需修改。

原报告的 instant ADD COLUMN、heap/RC、RR 无 gap 锁、严格副本只读等设计取舍保留。

## Binlog 与复制

| 原条目 | 判定及修复 | 实测证据 |
| --- | --- | --- |
| P1-01 | **修复**：binlog IO 连接显式使用客户端允许的 1 GiB 包上限；PacketTooLarge 转为明确的 1236 致命错误，不无限重试 | 源默认 64 MiB max_allowed_packet；36MB before+36MB after 的 UPDATE 基线卡在 IO 1235，修复后 IO/SQL 均追平到 108001379，实际存储为 36,000,000 个 `B`；重启后仍正确 |
| P2-02 | **修复**：seeding 槽不计入 eligible/met；等待中 quorum 变为不可达时及时降级 | 确定性槽测试；4 副本初始化实际采样，修复前一次约 0.516s 完整超时，修复后最大约 0.103s，零完整超时 |
| P2-03 | **修复**：仅“并发备份配额满”为瞬态重试；缺少备份服务、身份、认证、格式不匹配仍属明确永久拒绝 | 修复前 4 副本中 3 个退出且不重试；修复后全部重试成功，各有 60,000 行；mock 握手覆盖错误分类 |
| P2-04 | **修复真实的增量配置位点回退** | SOURCE_CONNECT_RETRY 修改前后保留执行位点 1609，不使用 IO 超前位点。报告声称 MySQL 显式改 HOST 也保留坐标不正确；本次同时遵守 MySQL 的显式 HOST/PORT 重置规则及 FILE-only→POS=4 规则 |
| P2-05 | **修复**：复用 binlog 数字文件序号比较，SQL 启动/空闲时先检查已达到的 UNTIL | 当前文件 000006、目标 000005：基线 SQL Running=Yes，修复后立即 No；覆盖同文件位置和 999999→1000000 后缀宽度变化 |
| P2-06 | **修复**：规范 CHAR 空格，给省略尾零的 BINARY 行像补齐声明长度；HASH_SCAN 保留 NULL 与文本 `NULL` 的区别 | 原始 relay 字节证实 BINARY(4) 的 `AB0000` 只携带长度 2 的 `AB`。真实 UPDATE/DELETE、重复行的消费式匹配和扫描对照通过；故障位点重启后能正确回放 |
| P3-07 | **报告语义错误** | MySQL 8.0.46 对仅 SOURCE_LOG_POS 的 UNTIL 返回 1277；mydb 的明确拒绝保留，错误身份差异不扩展为新功能 |
| P3-08 | **修复**：存在 FULL 成员元数据时，在映射 ENUM 序号/SET 掩码前校验成员及顺序 | 基线源 `a/x` 静默变成目标 `b/y`；修复后行未落库，SQL 1235 明确提示 schema mismatch。无成员元数据时仍要求源/目标 schema 匹配 |
| P3-09 | **防御性拒绝** | 当前目标是 MySQL 8.0；在行解码前明确拒绝旧版 TIMESTAMP/TIME 编码，避免 vendor 的旧格式转换缺陷产生静默值损坏。现代 TIMESTAMP2/TIME2 不受影响 |
| P3-10 | **修复**：共用 SQL 词法游标处理 DDL 名称，保留引用中的逗号、空格、点号和转义反引号 | DROP/RENAME 的逐组件回归通过；独立源/副本补测含空格名称的过滤 |
| P3-11 | **未复现，不改正确的解码** | client=latin1/connection=utf8mb4 及反向组合的真实 QUERY_EVENT 均复制出正确的 `café` 表名；SQL 字节应按 client charset 解码，connection collation 由回放 session 恢复 |
| P3-12 | **当前已修复** | 当前提交路径只要有 binlog_commit 就执行 WAL sync，即使 durability=Memory，不再是报告描述的无持久屏障路径 |
| P3-13 | **修复混合 DROP 的错误吞没，保留 fail-stop** | 混合删除永久表和临时表时，binlog 写入失败必须返回 1026；原先返回成功且只产生 warning。故障注入用目录替代 manifest 文件；同时覆盖永久对象名中反引号的重新编码。组写失败后的 fail-stop 保留，以免继续发布不完整日志 |
| P3-14 | **修复** | 保留内部 ENUM/SET 类型用于行编码，TABLE_MAP 线上类型改为 STRING(254)+真实类型元数据。原始类型断言及 mydb→真实 MySQL 的 ENUM/SET/BINARY INSERT/UPDATE 消费通过 |
| P3-15 | **修复** | biased select 中 ready ACK 优先于持续变化的 WAL watch；保留 shutdown 最高优先及完整帧读取，原生复制/碎片 ACK 回归覆盖 |
| P3-16 | **未确认，不按报告推测修改** | 注册/删除在同一槽注册表锁内，connected 检查与删除没有释放该锁；当前源码不足以支持报告所述在线槽被误删的确定结论 |
| P3-17 | **保留一致性边界** | base backup capture 的写冻结是显式 1290+重试语义；不在本次 bugfix 引入新的在线快照协议。初始化采样也记录到两次短暂 1290，副本均完成初始化 |

## 验证结果与局限

- 定向回归：**29/29 通过**（21 条新增回归及 8 条既有相关子查询/INSERT SELECT/深表达式与小栈回归）；旧的生成列错误预期已更正。
- 完整 nextest：**12,843/12,843 通过，64 条跳过**，耗时 228.640 秒；最终源码执行 `cargo nextest run --no-fail-fast`。
- `cargo build`、`cargo fmt --all -- --check`、`git diff --check`、`cargo run -- --help` 均通过。
- 二进制 prepared：50 次索引/全扫对照一致。
- 双向 MySQL 复制：大事件与 CHAR/BINARY 真实落库；mydb→MySQL 的 ENUM/SET/BINARY 正常应用。
- 本机 debug 的 5 轮交错性能对照：点查 -0.06%、点更新 -4.63%、范围连接 +0.60%、重复键连接 -1.22%（中位数，该微测未见明显退化）；没有发现明确通用性能退化。此小规模 debug 对照不代表生产吞吐上限，也没有声称证明所有 workload 不退化。

修复没有包含连接视图 CHECK OPTION 的功能扩展，也没有将报告中的误报强行“修复”为错误语义。完整日志路径和结构化结果见 `results.json`。

## 本机复跑入口

所有脚本只应用于专用测试实例；SQL 探针会重建命名为 `auditfix1127*`、`replfix1127`、`preparedfix` 等测试对象。Python 依赖为 `pymysql`，二进制 prepared 对照额外需要 `mysql-connector-python`。

```bash
cargo nextest run --no-fail-fast
cargo build
cargo fmt --all -- --check
cargo run -- --help
python3 docs-internal/audit-fixes-20261127/reproduce.py --port 14994 --password root
python3 docs-internal/audit-fixes-20261127/verify_prepared.py --port 14994
python3 docs-internal/audit-fixes-20261127/verify_drop_io.py --port 14995 --data-dir /tmp/mydb/audit-fixes/settled-data
python3 docs-internal/audit-fixes-20261127/native_probe.py --binary /tmp/mydb/audit-fixes/mydb-settled --root /tmp/mydb/audit-fixes/native-repeat --port 14985 --repl-port 13485
```

`verify_replication.py` 使用独立 MySQL 源 14981 和 mydb 副本 14992（源需开启 server-id、ROW/FULL、FULL 元数据和 binlog）；`verify_performance.py` 使用基线 14980 与修复版 14994，均为相同 debug 编译配置。`verify_memory.py` 对指定的独立 14994 实例创建 8000 行，启动该实例时需设置 `MYDB_MEMORY_TARGET=256 MYDB_QUERY_MEM_LIMIT_MB=32`；它与一般 SQL 探针的实例可分开使用。

收尾已停止本次创建的专用 mydb/MySQL 测试进程，保留数据与运行日志，删除已停机实例的 8 个应急预留文件以释放 8 GiB 临时空间；再次启动时会自动重建。没有操作原有的用户数据库实例。
