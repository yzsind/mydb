# RLS 任务完成情况与后续开发交接

更新时间：2026-11-04 15:17（Asia/Shanghai）。本文按实际源码及已保存日志整理，是进度快照，不是验收通过报告。

## 1. 当前结论

**RLS 已完成大量基础实现及漏洞修复，但尚未完成用户要求的全部正确性、安全性、恢复和性能验收，不能报告开发完成。**

本轮已修复原生复制的冷策略版本发布失败，并校正和扩展函数攻击探针。最新冻结 release 已通过 **7 组恢复/复制、18 组访问路径、16 组协议/binlog、2,000 行/12 轮随机参考**；60 项定向 Rust 回归全部通过。最终源码全量 nextest 已通过：**11,011 项成功，49 项跳过**，耗时 405.275 秒；正式性能及其余实现缺口仍未关闭。

本次遵循“SUPER 自动绕过 RLS”的修订规则；没有修改 Cargo 编译配置；测试均在本机进行，没有使用远程测试服务器。尚未创建本次实现的 Git 提交。

用户此次明确指定 `doc-internal`，故本报告保存于该目录。此前方案及持续验证日志位于仓库惯用的 `docs-internal`，对外英文文档位于 `docs`。

## 2. 已实现的主要内容

| 范围 | 当前实现与验证边界 |
| --- | --- |
| SQL 接口 | CREATE/ALTER/DROP POLICY、SHOW CREATE POLICY、表级 ENABLE/DISABLE、策略信息视图；ALL/SELECT/INSERT/UPDATE/DELETE 与 permissive/restrictive 组合；无适用 permissive 默认拒绝，NULL 不授权。完整 SQL 表达式及导出矩阵仍待收尾。 |
| SUPER 与权限 | 当前有效全局 SUPER 或 BYPASS_RLS 自动绕过；使用现有授权及激活角色闭包。全局 ALL 包含 SUPER，库级/表级 ALL 不因此绕过。账号名、建表者和普通系统身份标记均不提供绕过资格。SUPER 不自动获得策略管理、上下文管理权限。 |
| 连接上下文 | RLS_CONTEXT、会话/LOCAL 设置、按键/全部重置、保存点回滚、事务结束及连接重置/重新认证清理；设置权限与顶层入口限制。使用持久化共享映射与内存预算，避免每条语句复制全部上下文。 |
| 元数据 | 表描述符保存标记、版本及不可变策略；紧凑表头按需加载策略；事务化 DD 策略/依赖表接入 WAL、恢复和检查点。首版 catalog 格式 69→70，不提供旧格式迁移。 |
| 表达式绑定 | 目标列使用稳定列 ID，列重命名和 LIKE 复制重绑定已有回归；每个策略子句保存创建时 SQL mode、类型相关设置及排序规则等语义。外部列/函数完整依赖仍未完成。 |
| 查询安全边界 | 在基础访问处注入策略；无法证明安全的用户过滤以短路 CASE 保护。覆盖索引补齐策略所需列；视图逐引用保存有效身份，嵌套与工作线程继承连接租户上下文。 |
| 策略函数 | 策略存储函数经解析、EXECUTE 检查及依赖锁；运行时禁止策略写入、用户变量、序列、锁及外部副作用。用户声明 NO SQL/READS SQL DATA 不能自行证明安全。 |
| 写入 | 旧行过滤、锁后检查、最终类型转换/生成列/BEFORE 触发器后的新行检查；ODKU、REPLACE、RETURNING、IGNORE、导入及 FK 级联已接入基础检查和语句回滚。完整写入口及并发失败矩阵仍待验收。 |
| 优化 | 保留点查、索引、向量化和并行路径；整数上下文使用语句参数化索引边界；受限且经过类型证明的 EXISTS/NOT EXISTS 转换为 semi/anti join。优化已有正确性测试，不能据此认定性能门槛通过。 |
| 诊断 | SHOW/信息视图和详细 EXPLAIN 使用独立管理权限；普通用户的受保护 EXPLAIN 仅显示安全边界概览，SUPER 本身不提供详细策略诊断权限。其他诊断通道仍须继续审计。 |

## 3. 最近几轮重点修复

### 3.1 分区交换

- SQL 入口和存储层入口均检查交换双方；任意一方启用 RLS，都要求有效 SUPER/BYPASS_RLS，仍需满足正常 DDL 权限。
- 修复 Memory 模式在交换前丢弃脏 heap/index/LOB 页而导致数据消失的问题；交换前物化完整文件，并处理旧 Zone Map。
- 物理 storage ID 可以交换，逻辑 object ID 保持稳定；检查双方完整物理行布局，发布时重新验证描述符身份和版本。
- Memory/Relaxed/Strict 模式已有宽行、LOB、DECIMAL、DATETIME、JSON、GEOMETRY、VECTOR 值、索引及重复交换回归；并非所有专用向量索引格式均已验收。
- 新策略版本根最初在处理“删除旧存储主键、插入新存储主键”时可能误删同一逻辑对象的新条目；已修复主库及复制入口的条目协调。本轮进一步复现并修复冷副本的旧描述符加载时序问题，最新冻结候选已通过交换 replay、两次冷重启重复交换、冷策略变更和后续切换/备份查询。

### 3.2 锁定读与多表 DML

- 锁定读在候选选择及用户 WHERE/ORDER/投影之前，对相应引用应用 SELECT/UPDATE 策略；连接来源先授权过滤，再进入 ON/WHERE/投影。
- 多表 UPDATE/DELETE 在来源装载阶段，按每个引用的读需求及目标命令过滤。修复了将隐藏来源行的数据复制到普通目标表、据隐藏来源行删除普通目标行，以及隐藏写目标进入用户变量赋值的问题。
- 自连接按别名区分读权限需求；受保护写目标在锁定后重新读取并检查，再进入触发器。
- 用户变量副作用、隐藏来源复制/删除及锁定引用类别用例已通过。本轮增加隐藏值专用副作用探针和错误探针，覆盖 28 种语句形状 × 2 种探针及正向对照，最新冻结候选均通过。完整写入入口矩阵仍待收尾。

### 3.3 语句级权限、策略和 DEFINER 快照

- 已复现：外层 SELECT 等待存储函数参数期间，授予 SUPER 会令后续嵌套查询读到隐藏值。现已在嵌套作用域及工作线程共享不可变授权根。
- 已复现：等待期间 ALTER POLICY 会让尚未绑定的嵌套表改用新策略。现新增 O(1) 捕获的不可变策略版本根，发布时保留旧语句需要的版本，payload 继续按需加载并计入预算。
- 已复现：等待期间激活 DEFINER 默认 SUPER 角色会改变正在运行语句的绕过决定。运行时 DEFINER 的角色解析已改用语句授权快照。
- 无法证明迟绑定关系的旧安全状态时，返回 50132 / SQLSTATE 40001 重试错误，不能退回当前较宽松策略。完整游标和发布故障生命周期尚未验收。

### 3.4 多语句 DML 缓存

- 已用独立锁持有连接复现：第一条 UPDATE 等待行锁时撤销 SUPER，第二条 UPDATE 仍使用之前编译的绕过计划，修改了隐藏行。
- PreparedDmlPlan 现在保留原始 AST；受保护逻辑语句执行前重新校验身份，重绑定使用原始 AST，不能继续执行带旧策略的 AST；一条语句的锁等待重试沿用同一安全根。
- 最新协议用例覆盖 autocommit 和隐式事务，两种情况下均在锁等待期间授予/撤销 SUPER，验证下一条逻辑语句使用新权限，已通过。
- **当前限制**：受保护表跨语句的分组物理 UPDATE 暂不合并，逐个使用缓存点更新/sparse 执行器，保证独立权限边界。普通表合并及受保护表的普通点/范围/批量语句快路径仍保留。此限制不能作为完整性能方案交付的替代。

### 3.5 性能修复进度

- 诊断发现文本预编译 DML 在安装实际参数类型前编译，RLS 条件因此产生 CASE/full scan。已修复参数作用域与缓存域绑定，真实 scanned-text 稀疏更新回归通过。
- 整数列与无诊断语句常量增加通用标量比较路径，避免将同一上下文值展开成整个 RHS 向量；NULL、signed/unsigned 边界、反向操作数和六种比较已对照逐行参考实现。
- **上述修改已在最新 release 上完成短测：上下文扫描 +14.15%、点 UPDATE +13.62%、批量 UPDATE +2.03%。前两项仍超过 5%；短测不构成正式验收通过。**

## 4. 最新源码与证据

| 项目 | 记录 |
| --- | --- |
| 仓库 | `/Users/william/prog/mydb` |
| 基线 HEAD | `52c8320e6ede776d7814ea6ad955a4a3f3de1fa9`；当前实现为工作区未提交修改 |
| 原始二进制 | `/tmp/mydb/rls/baseline/mydb`；对应 `commit.txt`、`binary.sha256`、`build.log` 在同目录 |
| 最新冻结候选 | `/tmp/mydb/rls/candidates/resume-replay/mydb` |
| 候选 SHA-256 | `903ea206f71e6bbd73196d4d55398dc41251fb32523022f77a3aed6e038bf31d` |
| 源码清单 | `/tmp/mydb/rls/candidates/resume-replay/source.json`；冻结二进制前已核对 Rust/Cargo 源文件摘要 |
| 构建方式 | `CARGO_TARGET_DIR=/tmp/mydb/rls/build-target TMPDIR=/tmp/mydb/rls/tmp cargo build --release --bin mydb`；仅隔离输出目录，未改变编译 profile |
| 本机环境 | macOS / Apple Silicon，10 逻辑 CPU、32 GiB 内存；PostgreSQL 18.3 已安装 |

隔离 target 是因为同一工作区另一个二进制体积任务曾覆盖默认 release 产物；RLS 验证使用自己冻结并记录摘要的候选。

| 验证 | 最新已知结果 | 证据路径（均位于 `/tmp/mydb/rls/`） |
| --- | --- | --- |
| 最终源码完整 nextest | **11,011 成功，49 跳过**，405.275 秒 | `resume-early-delta-full-nextest.log` |
| 最新 release | 构建成功，9 分 14 秒；源码清单已核对 | `resume-early-delta-release.log` |
| 本轮 RLS/分区交换/复制重启定向 | **60/60 成功** | `resume-early-delta-focused.log` |
| 缓存/权限重绑定/普通 prepared DML/multi-query | **116/116 成功** | `batch-security-nextest.log` |
| 文本 DML 参数绑定前一阶段 | **92/92 成功** | `text-dml-security-nextest.log` |
| 最新文本/二进制协议及 binlog | **16 组成功** | `resume-early-delta-protocol.log`、`protocol/run-28ujs2ef/result.json` |
| 最新访问路径 | **18/18 成功**，含 56 条函数攻击语句及探针正向对照 | `resume-early-delta-access.log`、`access-paths/run-momwzipi/result.json` |
| 最新恢复/复制 | **7/7 成功**，含冷重启重复交换、策略变更、SUPER 撤销、promotion、sealed backup | `resume-early-delta-recovery.log`、`recovery/run-mxxw05p0/result.json` |
| 最新冻结版本随机参考 | **2,000 行、12 轮通过** | `resume-early-delta-reference.log`、`reference/run-k51ncr_1/result.json` |
| PostgreSQL 18.3 共有语义 | 早期 7 项参考通过；并非完整差分矩阵 | `postgresql-reference/results.json` |
| 最新三项性能诊断 | **100k、3 轮 × 3 秒，仅诊断；前两项未达 5% 门槛** | `resume-early-delta-performance-diagnostic.log`、`performance/run-_gx8mo4p/results.json` |
| 正式性能 | **未通过／未完成**；完整数据量、倾斜、并发及测量时长尚未完成 | 不能用短测替代正式验收 |
| 普通 build/help/startup | **全部通过**；dev 构建 2 分 34 秒，启动后 SELECT 1 与 VERSION() 成功 | `resume-cached-dev-build.log`、`resume-cached-dev-help.log`、`dev-startup/run-ahl0rzx7/result.json` |

最新 16 组协议包含默认拒绝、SUPER/角色/权限分离、读写隔离、REPLACE 触发器、策略修改/禁用/删除快照、DEFINER 默认角色快照、DML 包逐语句重绑定、交换双方、LOCAL/保存点/重置、COM_STMT、binlog 完整策略 DDL及重启。

**历史通过证据不可冒充最新通过证据。** `snapshot-stage` 的恢复回归已在本轮复现和修复；上表标注最新及本轮的专项结果来自 `resume-replay` 冻结候选。旧诊断及失败日志仍保留，不能将早期全量结果作为本轮全量结果。

## 5. 当前失败、限制与尚未验证事项

### 本轮已关闭：冷副本策略发布失败

- 先用原冻结候选复现 fail-stop：`resume-recovery-repro.log`、`recovery/run-x9938lgo/result.json`。
- 新 Rust 回归先在交换的旧主键加载处失败（LSN 385）；仅处理同版本物理位置移动后，扩展回归又在真正策略变更处失败（LSN 422）。两阶段失败证据为 `resume-cold-replay-repro.log` 和 `resume-replay-focused.log`，未豁免失败。
- 根因：提交前临时加载旧描述符时，物理 DD 行和索引已经 replay，冷描述符的旧主键/旧行可能无法再经索引找到。只依赖提交阶段补载不成立。
- 修复：主库先记录 CatalogDelta，再记录 DD 行修改；副本在收到清单时保留完整旧描述符，按事务计入元数据内存预算，提交用于 MDL/索引/策略根发布，提交或回滚时释放。逻辑对象、策略版本和 enabled 状态均相同时，位置移动也可使用传入的完整同版本 payload 保留旧根；版本变化仍必须捕获旧 payload，版本回退仍拒绝。
- 新回归覆盖双方不同策略、三轮冷副本交换、其中一次真正策略版本变化、旧根 payload 保留、重复发布无变化及版本回退拒绝。最新恢复脚本 7/7 通过；不据此宣称完整取消/OOM/发布中断生命周期验收完成。

### 本轮已关闭：函数探针断言及当前攻击矩阵

原断言将已授权值 11 的合法求值误判为泄露。现在使用仅在收到隐藏值 20 时记录的探针，并增加收到该值即 SIGNAL 的错误探针。28 种语句形状覆盖单表锁定读投影、连接投影/ON/WHERE、FOR UPDATE/FOR SHARE 及 OF 两侧、单表写入、多表 UPDATE SET/ON/WHERE 和 DELETE ON/WHERE；两种探针共 56 条攻击语句全部通过。

额外正向对照验证已授权行仍会调用函数，直接传入隐藏值可触发记录和错误，避免禁用函数造成假通过。完整访问脚本 18/18 通过，见上表最新证据。

### 性能仍是未关闭门槛

最新冻结候选的 100k 数据短测为 **3 轮 × 3 秒**，预热后交替测量；等待编译结束后才开始计时，测量期间没有运行本轮回归测试。结果校验、执行计划、候选行、吞吐、延迟及进程采样保存在 `performance/run-_gx8mo4p/results.json`。它仍达不到正式测量时长：

| 场景 | 诊断额外开销 | 门槛 |
| --- | --- | --- |
| 上下文策略扫描 | **+14.15%，未达门槛** | ≤5% |
| 简单策略点 UPDATE | **+13.62%，未达门槛** | ≤5% |
| 简单策略批量 UPDATE | +2.03%，仅本次诊断满足 | ≤5% |

最新点 UPDATE 两侧均为 candidates=1，批量两侧均为 candidates=1000；上下文策略与等价 WHERE 均使用 tenant_key 索引读取 1,000 行。此前点 UPDATE 的全扫描级退化已不再出现，但上下文和点更新的额外开销仍待优化。此前 +45,931.52% 等失败证据仍保留在 `performance/run-xa974ic3/results.json`，不能与本次结果混用。正式验收必须使用一致硬件/配置/数据/索引，覆盖 100k、1m 数据、倾斜及所需并发，预热后交替至少 7 轮，每组至少 30 秒，核对结果、计划、扫描行、吞吐、延迟、CPU 和内存：未启用 RLS ≤2%，SUPER 绕过 ≤2%，简单策略 ≤5%，复杂策略 ≤10%。

### 其余尚未完成的计划要求

- 外部列和函数的稳定依赖、传递依赖、DROP/ALTER/重建限制、外部重命名及类型/排序规则变更原子重绑定。
- 所有策略子查询依赖的 MDL 与完整递归身份/命令/版本边界；跨表授权快照与锁后目标版本冲突时的重试契约。
- 完整游标生命周期、策略发布中断、描述符淘汰、取消/OOM、锁等待及并发授权数据修改的同步验证。
- 全 SQL 策略的 SHOW CREATE/逻辑导出；部分复杂查询来源和 hint 的打印器仍明确不支持，不能静默丢失。
- 所有专用扫描/排名/历史查询、并行/编码导入、多冲突修改、视图及级联路径的完整验收矩阵。
- 其他详细诊断通道的策略披露审计；不能用 EXPLAIN 已保护推断全部诊断都受保护。
- 最新随机参考与本轮恢复/备份/复制/切换脚本已通过；更完整 PostgreSQL 差分、正式性能及其余故障生命周期仍待完成。最终全量 nextest 已通过，普通 build/help/startup 检查均已通过。

## 6. 后续执行顺序与完成条件

1. **本轮完成** native replay 冷版本失败修复及复制交换/冷描述符/重复发布回归；最新恢复脚本 7/7 通过。
2. **本轮完成** 函数探针校正及当前锁定读/多表 DML 的 56 条攻击语句与正向对照；未降低隐藏值隔离要求。
3. 继续补齐依赖绑定、DDL/MDL、递归、游标及并发冲突契约，逐项关闭上面的实现缺口。
4. 本轮已冻结新 release 并完成三项短测；继续优化上下文扫描和点 UPDATE，再完成 100k/1m、倾斜和并发的正式性能矩阵。保留正常快路径，禁止修改 Cargo 编译配置来满足门槛。
5. 本轮协议、随机参考、恢复、全量 nextest、`cargo build`、`cargo run -- --help` 和 startup 均通过。其余实现收尾后仍须针对最终候选重新验收，并补齐 PostgreSQL 差分、完整故障/性能矩阵和对外英文文档。
6. **所有必需功能、安全、恢复和性能门槛全部通过后，才报告 RLS 开发完成。当前未达到此条件。**

## 7. 代码与文档入口

- SQL/AST：`src/parser/rls.rs`、`src/parser/print_query.rs`。
- 策略运行时/快照/目标列绑定：`src/engine/rls.rs`、`src/engine/rls/catalog.rs`、`src/engine/rls/binding.rs`。
- SQL 管理及执行接入：`src/engine/executor/rls.rs`、`dml.rs`、`locking_join.rs`、`multi_table.rs`、`prepared_dml.rs`、`routines.rs`、`dispatch/views.rs`。
- 元数据/复制：`src/storage/core/database/rls.rs`、`catalog_store.rs`、`catalog_ops.rs`、`wal_apply.rs`、`ddl.rs`、`src/storage/data_dictionary.rs`。
- 优化：`src/planner/row_security.rs`、索引边界/残余模块、`src/engine/expr_program/column_batch.rs`。
- Rust 回归：`src/engine/executor/tests/rls.rs`；协议/访问/恢复/随机/性能脚本：`scripts/test_row_security.py`、`test_rls_access_paths.py`、`test_rls_recovery.py`、`test_rls_reference.py`、`rls_bench.py`。
- 既有内部方案与证据：`docs-internal/rls-design.md`、`docs-internal/rls-validation.md`。
- 对外英文文档：`docs/row-level-security.md`，当前明确标为开发中。

临时数据库、虚拟环境、候选二进制及原始日志均在 `/tmp/mydb/rls/`。后续复现应启动并清理自己拥有的进程，避免操作用户或其他任务的数据库和构建进程。
