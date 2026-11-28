# 作者 1039515215 开放 issue 分析与验证

日期：2026-11-27。范围：`is:issue state:open author:1039515215`，仓库 `yzsind/mydb`。开始时 HEAD 为 `6bebbc50`，工作区干净。共 9 项：#108、#109、#110、#118、#119、#120、#121、#122、#123。

## 判断与开发测试计划

| Issue | 初步判断 | 方案和验证计划 |
| --- | --- | --- |
| [#108](https://github.com/yzsind/mydb/issues/108) 字符集前缀 DEFAULT | 应修复。合法 MySQL 字面量不应被当成非字面量表达式拒绝。 | 在公共默认值转换处识别有 introducer 标记的字面量，复用现有字符集求值；不接受任意裸函数。覆盖 CREATE、ALTER ADD/MODIFY/CHANGE/SET DEFAULT、已有行回填、持久化重开、SHOW CREATE 重建、无效字符集/字节、普通表达式限制。 |
| [#109](https://github.com/yzsind/mydb/issues/109) NUMERIC UNSIGNED ZEROFILL | 应修复，同义词行为必须一致。代码检查同时发现 DEC UNSIGNED 的遗漏。 | 补全 NUMERIC 后缀、DEC unsigned 类型与标志映射；覆盖 DECIMAL/NUMERIC/DEC × SIGNED/UNSIGNED/ZEROFILL、精度/范围/负数、元数据、ALTER 和非法后缀。 |
| [#110](https://github.com/yzsind/mydb/issues/110) 大写表名折叠 | 保留既定设计 `lower_case_table_names=1`；不为迁移端精确字符串匹配改 catalog。 | 实测折叠、大小写混用查询、系统变量只读；补充迁移端按目标规则匹配对象的说明。 |
| [#118](https://github.com/yzsind/mydb/issues/118) 非法 ANY(ARRAY) CHECK | 报告给出的是外部 NineData 引擎生成的非法目标 SQL；不接受或忽略约束。 | 将 issue 原始错误表达式分别送入 mydb/MySQL 8.0，验证均拒绝；验证正确 IN 表达式可创建并拒绝违规数据。 |
| [#119](https://github.com/yzsind/mydb/issues/119) Vastbase CHECK 运算符 | 缺少实际源表达式、生成 SQL，无法精确复现。不能据错误位置猜测或放宽任意运算符。 | 核查支持边界，用明确标注的 PostgreSQL 方言示例验证拒绝；需要外部引擎提供原始 SQL 后才能判断是否遗漏合法 MySQL 运算符。 |
| [#120](https://github.com/yzsind/mydb/issues/120) 拼接约束名过长 | 保留 64 字符约束名限制；上游生成器应截断并追加稳定 hash，数据库不得静默改名。 | 对照 MySQL 验证 64 成功、65 返回 1059。 |
| [#121](https://github.com/yzsind/mydb/issues/121) PG 分区子表不存在 | mydb/MySQL 分区不将分区暴露成可独立查询的关系，不能按 PG 子表清单直接校验。 | 验证 HASH 分区数据、IS.PARTITIONS、子表式查询返回 1146；迁移器应选择父表和目标分区元数据，或显式创建并填充普通子表等价物。外部任务无法在本机精确复现。 |
| [#122](https://github.com/yzsind/mydb/issues/122) VECTOR 维度丢失 | 先验证本机 DDL/元数据/写入是否丢失维度。不能通过取消维度校验规避错误。 | 检查 VECTOR(3) 的 SHOW CREATE、IS.COLUMNS、重建后 3 维成功与 2 维拒绝；缺 MySQL 9.5 源 DDL、目标实际 DDL、写入参数时，不能定位外部转换问题。 |
| [#123](https://github.com/yzsind/mydb/issues/123) binlog 增量不收敛 | 本机发现实际协议缺陷，应修复；尚不能等同于外部任务的唯一根因。 | 心跳使用 dump 连接已消费的文件和位点，避免空读后提交/rotate 改变全局尾部导致越过未发送事件。新增 NONE/CRC32 定向竞态回归；新建本机 MySQL 8.0 真副本，28 张多类型/大字段/分区表，三轮 INSERT/UPDATE/DELETE、rotate、重连、GTID 追赶、逐表比较，空闲跨越 30 秒并观察实际心跳数、IO/SQL 错误与位点，再用 mysqlbinlog 独立解析。 |

## 验证方式

1. 先增加回归用例，运行修复前 nextest，保留失败证据。
2. 仅修公共解析/默认值转换路径；保持 DML 执行热路径和 Cargo 编译配置不变。
3. 运行专项测试、相邻 parser/DDL/charset/binlog 测试，再运行完整 nextest 与 cargo build。
4. 修复前后均运行本机 MySQL 8.0 差分/复制集成脚本；全部临时实例和输出放 `/tmp/mydb`。

可重复脚本：`python3 docs-internal/issue-author-1039515215/verify.py --binary target/release/mydb --cdc`；修复前加 `--baseline` 记录预期失败。脚本启动独立 mydb、独立本机 MySQL 8.0，退出时终止自己创建的实例，保留日志和数据以便复核。

## 实测结果

### 修复前

- 新增专项 nextest：3/3 失败，分别复现前缀默认值 1067、字符集校验被 1067 遮蔽、NUMERIC UNSIGNED 1064。日志：`/tmp/mydb/issue-audit-20261127/nextest-before.log`。
- 本机 MySQL 8.0.46 差分：#108、#109 mydb 失败而 MySQL 成功；#118 的非法原始 SQL、#120 长度边界、#121 分区对象模型两边行为一致。
- #122 本机 `VECTOR(3)` 在 SHOW CREATE / IS.COLUMNS 中保留维度，重建表后 3 维值成功、2 维值被明确拒绝。此结果不能替代 MySQL 9.5 → NineData → mydb 全链路复现。
- #123：28 表第一轮追赶、内容比较及 IO_THREAD 重连成功，第二轮写入/轮转时真副本报 `MY-013118`，随后 `MY-013122` 停止 IO：心跳 position=197，小于已经接收的 position=5396221。日志目录：`/tmp/mydb/issues-108-123-e4fyl37d`；汇总：`/tmp/mydb/issue-audit-20261127/baseline-integration-v2.log`。
- 独立原始报文检查在旧构建第 10 个事件就捕获跨文件越位：已消费 `mydb-bin.000001:1148`，却收到 `mydb-bin.000002:197` 的心跳。它将协议问题与外部延迟指标彻底分离，直接验证竞态。日志：`/tmp/mydb/issue-audit-20261127/raw-before.log`。
- 根因：dump 线程在空读后等待心跳，提交或 rotate 可以在此期间推进全局文件/位点。旧实现重新读取全局尾部生成心跳，可能先于对应 ROTATE 发出下一文件的低位点，或越过尚未发送的行事件。修复后心跳重复连接已消费的 cursor，不读取可变化的全局尾部。与 [MySQL 8.0 Binlog_sender 的 wait_with_heartbeat / send_heartbeat_event 实现](https://github.com/mysql/mysql-server/blob/8.0/sql/rpl_binlog_sender.cc)的连接游标语义一致。

### 修复后

- 4 个专项测试通过：默认值 CREATE/ALTER/SHOW CREATE 重建/持久化重开、非法字符集与字节、普通裸函数限制、18 组 DECIMAL 同义词/后缀组合及范围验证、NONE/CRC32 两种心跳提交/轮转竞态。日志：`/tmp/mydb/issue-audit-20261127/nextest-after-v2.log`。
- 首轮持久化重开测试使用了不保证重开文件完整性的 Memory 测试夹具，修正为现有 Relaxed 夹具并 checkpoint 后通过；未修改存储实现。
- 完整 `cargo nextest run --no-fail-fast`：**12,856 passed，64 skipped，0 failed**，运行 238.005 秒；覆盖相邻 parser、charset、DDL、binlog 和复制测试。日志：`/tmp/mydb/issue-audit-20261127/nextest-full.log`。
- `cargo build`、`cargo build --release`、`cargo run -- --help` 均成功。没有修改 Cargo.toml 或编译配置。
- MySQL 8.0.46 SQL 差分：4 种字符集默认值及 18 组 DECIMAL/NUMERIC/DEC 后缀均通过；#110、#118/119 方言边界示例、#120、#121 的设计边界全部符合预期。#122 固定维度 DDL/值往返成功，裸 VECTOR 同表存放 2/3 维值成功。
- 原始报文压力：**1,000 INSERT + 200 rotate，6,195 个事件，其中 593 个心跳**；逐个核对 CRC、文件名、心跳位点与连接已消费位点完全一致，无越位。
- 真 MySQL ROW/FULL 副本：**28 表 × 3 轮** INSERT/UPDATE/DELETE，含有/无符号整数、精确小数、DOUBLE、非 ASCII 字符、二进制、JSON、DATETIME/TIMESTAMP/TIME(6)、BIT、大 TEXT/BLOB 和 HASH 分区；每轮全部表数据比较一致，轮转和三次 IO_THREAD 重连通过，GTID wait 采样 0.008–0.026 秒。此测试采用 file/position dump，GTID 用于等待已执行事务。
- 空闲 35 秒，实际接收 **173 个心跳**，持续观察 IO/SQL 线程均为 Yes、`Seconds_Behind_Source=0`；随后新写入继续追赶且内容正确。MySQL `mysqlbinlog --read-from-remote-server --to-last-log -vv` 独立解析所有工作负载文件成功。
- 集成日志：`/tmp/mydb/issue-audit-20261127/after-integration.log`，实例与 mysqlbinlog 输出所在目录见其首行 `root`。精简的修复前后证据、二进制 SHA256 和完整测试结果保存在同目录的 [evidence.json](evidence.json)。

### 通用 SQL 性能检查

在完整测试结束后，将修复前保存的 release 二进制与修复后的 release 二进制交错运行，各 5 次、各自独立实例与相同配置；对 10,000 行表执行相同的点查、范围聚合、UPDATE 和已有合法 DDL。每轮都有结果/行数断言。以下为批次总耗时中位数（毫秒）：

| 工作负载 | 修复前 | 修复后 | 变化 |
| --- | ---: | ---: | ---: |
| 1,000 次主键点查 | 45.279 | 45.209 | -0.16% |
| 100 次范围聚合 | 33.377 | 33.441 | +0.19% |
| 200 次 UPDATE | 17.224 | 17.390 | +0.96% |
| 50 次普通 CREATE TABLE | 19.733 | 19.870 | +0.70% |

该本机短程检查未观察到明显性能回退；不将约 1% 的微小差异解释为优化或回退，也不能据此保证所有硬件和负载。新增字符集求值只在 DDL 保存默认值时发生，INSERT 继续读取已求值的默认 Value；心跳路径移除了读取全局状态的锁。脚本：[performance.py](performance.py)；全部样本：`/tmp/mydb/issue-audit-20261127/performance.log`。

## 外部链路仍需的证据

- #119：实际提交到目标的完整 CREATE TABLE（尤其原始 CHECK）、源表达式及转换结果。文中的 `::integer`、`~` 仅为方言边界示例，不能声称已复现原 issue。
- #121：迁移任务实际选择对象清单、父表/子表映射规则及发往目标的全部建表 SQL。数据库不会自行替 NineData 从 PostgreSQL 枚举对象。
- #122：MySQL 9.5 源 DDL、目标实际 DDL、写入 SQL/参数、二进制 VECTOR 数据和驱动类型映射。不能把正常维度校验改成静默截断/补零。
  当前 mydb 的裸 `VECTOR` 按设计接受变长维度，因此仅凭“维度丢失”这个推测还不能解释固定维度不匹配错误，必须确认失败目标列实际上声明的维度。
- #123：NineData 实际读取位点、事件/重试日志和 syncDelay 计算规则；本次修复已经证实的协议问题不等于已证明外部 30 分钟超时的全部原因。

未访问 issue 中内网端点，也未使用测试服务器、发布版本或向 GitHub 自动回复/关闭 issue。

## 迁移引擎侧的后续开发与验收计划

这些计划适用于外部迁移引擎，当前仓库不包含其源码；不能在 mydb 中通过忽略约束或改写用户对象名替代这些修复。

- **#110**：预检、全量、增量、比较阶段共用源对象 → 目标对象名称映射，按照目标的 `lower_case_table_names` 匹配 SHOW/IS 返回值。验收大小写混合库表名、带引号标识符、重试/重启映射稳定，以及折叠后冲突必须提前报错。
- **#118**：在源端约束 AST → 目标端 AST 的转换层实现合法成员判断，再输出 MySQL SQL；不能对字符串做全局替换或静默丢掉 CHECK。验收合法 M/F、非法值、NULL、引号/反引号转义和空集合的三值逻辑；本机已经验证本 issue 的 `IN ('M','F')` 等价目标约束可执行并强制生效。
- **#119**：先取得完整 SQL，定位具体运算符和操作数类型；仅对经语义验证的方言算子生成目标表达式。不能把 PostgreSQL 正则等算子未经字符集、转义、NULL 语义对照就机械替换。验收正反例、NULL、非 ASCII 和与源端实际结果差分；没有等价实现时预检明确报错并指出约束名。
- **#120**：集中生成标识符，按目标字符数限制保留前缀并追加稳定 hash（例如 47 字符前缀 + `_` + 16 个十六进制字符），再按目标命名空间检查碰撞；不可仅截断。验收长库名/时间戳/表名、64/65 边界、多字节名字、不同源对象截断同前缀、同 schema 多表约束冲突和任务重试幂等。
- **#121**：在建表前生成完整对象映射，明确采用“父表包含全部分区数据 + 目标分区元数据”还是“显式普通子表等价物”。预检/全量/CDC/比较共用映射，避免父子重复拷贝。验收 HASH/RANGE/LIST、空分区、跨分区 UPDATE、父/子表各自查询的数据集合及总量一致性；对象缺失在复制前失败。
- **#122**：从实际源 DDL 读取元素维度，确认没有把字节长度误用成维度，再检查目标 SHOW CREATE。写入层按源 VECTOR 类型解码为元素序列，使用目标原生向量值或明确的 STRING_TO_VECTOR 构造，不依赖未声明的隐式转换。验收 2/3/大维度、NULL、浮点边界、二进制参数、全量与增量、SHOW CREATE 重建和实际错误行；这只是待证据后的开发计划，尚未定位原任务根因。
- **#123**：复跑原 NineData 28 表任务，归档捕获端 file/pos、最后一条真实提交事件时间、心跳时间和重试原因。至少覆盖连续写、跨 30 秒空闲、rotate、断线重连和追赶；比较 28 表内容，确认 syncDelay 对合法 timestamp=0 的 synthetic heartbeat 有正确处理。不得为了压低延迟指标而伪造 binlog 时间戳。mydb 本次负责修复已经实测的心跳位点竞态。
