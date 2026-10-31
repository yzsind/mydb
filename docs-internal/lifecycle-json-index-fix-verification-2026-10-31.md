# 生命周期、JSON 与函数索引：判定、修复与验证（2026-10-31）

## 基线和范围

当前分支 `codex/bugfix`，起始提交 `bcc0984`。指定的三份审计报告未包含在当前检出中，已从本机 `mydb-backup-20261031/docs-internal/` 恢复原文作为依据。报告来自包含未提交修改的旧工作区，因此逐项检查当前代码，不照搬原报告的行号和建议。

只在本机 macOS arm64 验证，临时数据库、日志、依赖环境和原始提交快照均在 `/tmp/mydb/`。没有连接或修改用户已有数据库，也没有使用远程服务器。没有本地 MySQL oracle 可用；兼容语义查阅官方文档及 MySQL 8.0.46 源码，正确性主要以可执行的预期值、索引与全扫一致性和修复前后的回归为依据。

## 计划及复现证据

1. 在生产代码修改前新增 9 组 SQL 回归，全部失败，实测得到 `[1]` 错误区间结果、JSON 大数饱和到 i128 上限、JSON JOIN 漏配、函数索引漏查和错误唯一冲突等。
2. 用独立进程及原始 COM_QUERY 分包复现会话问题；修复前的检查记录在 `audit1031-process-baseline-expanded.log`。覆盖账号配额、部分包加 KILL QUERY、结果字符集、时区、CLI、信号与 bootstrap 锁。
3. 修复公共的转换和键编码路径，而非按特定 SQL 文本分支；增加错误注入、重启、prepared、真实主备和损坏输入验证。
4. 定向用例通过后执行全量库回归，并用原始提交快照复核与本次修改无关的失败。

初始 SQL 日志：`/tmp/mydb/audit1031-baseline.log`，9/9 失败。HTTP drain 错误跳过 checkpoint 由真实 engine 加 pending 控制的故障注入复现；副本 restartpoint 不可推进时的错误关闭由 Relaxed 存储模式复现（Memory 模式会绕过该路径，不能用它证明此问题）。

## 生命周期报告逐项裁决

| 原编号 | 裁决和处理 |
| --- | --- |
| BUG-1 | 真 bug，修复。账号限制非零时使用账号值；为零时读取实时 GLOBAL 值。全局超限报 1203，账号显式限制保留 1226。不能采用报告建议的 `max(account, global)`，那会放宽较严格的账号限额。测试覆盖限额生效、显式账号值覆盖和回收。 |
| BUG-2 | 真 bug，修复。main 在角色读取、bootstrap、保留控制处理和引擎打开之前持有目录锁；bootstrap 公共入口也独立持锁，保护直接库调用和中断安装恢复。同进程引用计数重入，锁文件不属于被替换的备份子树。进程探针覆盖等待源库时的锁和第二进程拒绝；另完成真实主备 seeding/replay/restart。 |
| BUG-3 | 真 bug，修复。跨 KILL QUERY 唤醒保留同一个读 future 和 idle deadline；只有关闭连接/进程时才放弃读。原始协议分包后能返回 42，随后同连接查询 43 继续成功。 |
| BUG-4 | 真 bug，修复。五个公布的 HTTP 选项开放 CLI，布尔项接受裸开关及显式 ON/OFF；同一条实际启动命令验证全部选项。 |
| BUG-5 | 真 bug，修复。main 在引擎及 bootstrap 前保留监听 socket；绑定成功后才输出 listening。占用端口失败不会初始化 schemas。 |
| BUG-6 | 静默无反馈需要修复，但不伪装支持 reload。消费 SIGHUP 并明确记录不支持配置 reload，提示使用 SQL FLUSH；注册错误记录日志，不再 panic。未实现 MySQL 的隐式 reload 行为。 |
| BUG-7 | 真 bug，修复。配置层限定 tokio_workers 为 1..1024，1025 在创建线程前以配置错误退出。 |
| BUG-8 | 保留严格尾部校验。不接受任意尾随垃圾具有明确价值；已知可选 zstd 字段缺省的兼容处理不等于允许未知尾部。不是必须跟随 oracle 宽松行为的 bug。 |
| BUG-9 | 保留 port=0 以支持临时/测试实例；修正为输出 socket 实际端口。不是禁止动态端口的修复。 |
| BUG-10 | 保留重复参数最后一个生效，与已有 INI/覆盖约定一致；不将常见 CLI 约定改成错误。 |
| BUG-11 / version | 真接口问题，修复。help 在环境/配置读取之前返回；补充 --version/-V。 |

## 生命周期静态观察项

| 编号 | 复核结果 |
| --- | --- |
| O-1 | 故障注入确认并修复。先停事件生产者；HTTP、AI、备份的一个关闭错误不再跳过其他 worker join、archiver 和最终 checkpoint，仍保留首个错误返回并记录后续错误。 |
| O-2 | Relaxed 模式复现并修复。副本关闭执行 restartpoint，不以旧的“刷前脏页数”判断必须八轮收敛。暂无可推进的安全 restartpoint 时保留源 WAL，不追加本地 checkpoint 记录或错误裁剪。真实主备副本正常关闭/重启验证。 |
| O-3 / O-5 | 修复错误/恐慌清理的所有权问题。失败的最终清理保留 Drop 重试资格；账号配额回收与可重试的 SQL 资源清理分离，并以 session 标记保证仅回收一次。注入首次清理失败验证 Drop 第二次清理；两个真实配额占位验证重复 close 不偷减另一个连接计数。COM_CHANGE_USER 新账号补偿与旧账号重试分开。不是声称复现了生产引擎 panic。 |
| O-4 | 未独立复现丢最后响应/RST；代码推断不足以证明 bug。本轮保留已刷包再结束连接的现有路径，不增加可能等待对端的 socket drain。 |
| O-6 | 源码及预算计算确认并修复。普通连接和管理员预留均按 socket 加断连观察副本的两个 fd 计数，已有确定性容量测试更新为真实容量。 |
| O-7 | 原报告的 ConnectionSlot 泄漏不成立：分发失败丢弃 closure 会 Drop slot。connection_id 的预留确实需要显式回收，已在分发失败分支 unregister。 |
| O-8 | 修复，定义 interval=0 为仅禁用时间触发，WAL 大小及显式 checkpoint 保留；测试覆盖两个触发条件，产品配置文档同步。 |
| O-9 | 防御修复。init_connect 分发失败恢复 context 后不提前置 kill_flag，允许按现有协议读取首个命令并发送初始化失败应答；其他命令的失败取消策略不变。未声称以正常负载触发阻塞池失效。 |
| O-10 | 生命周期修复。binlog retention 改为可唤醒停止的 worker，在最终引擎 checkpoint 前停止并 join，不再留 60 秒 sleep 的裸线程。真实主备测试启用 binlog retention 并检查正常退出。 |

## JSON 报告逐项裁决

| 原编号 | 裁决和处理 |
| --- | --- |
| 01 | 真 bug。范围终点 last-N 负下溢返回无匹配；起点继续钳到零。共享求值器同时修复多值索引键。 |
| 02 | 真 bug。JSON 数字文本进入精确十进制/BigInt 重标度；宽 double 的冷路径也按声明精度收敛，不再按 i128 大小饱和。覆盖正负 1e40、DECIMAL(65)、超过声明精度的 1e70。 |
| 03 + 06 | 真 bug。递归规范化 JSON 数字和时间等价键；同时修复 ValueKey、inline key 和 vector 列直接编码三个入口。覆盖 JOIN、COUNT(DISTINCT)、GROUP BY、UNION、嵌套数组/对象与正负零。不是只改了被某一执行路径绕过的 ValueKey。 |
| 04 | 真 bug。文本与二进制结果共用字符集转换；UTF8mb4/binary 仍走直接文本快路径，GBK/latin1 和 utf8mb3 的字符范围按字符串转换器处理。实际驱动验证后续查询不会因包序列错误断裂。 |
| 05 | 真 bug。JSON_VALUE 整型转换先精确舍入再检查 signed/unsigned 边界，错误交 ON ERROR 并进入诊断；覆盖 NULL/DEFAULT/ERROR 三种处理。普通 scalar CAST 的兼容性 wrap/clamp 不被替换。 |
| 07 | 真 bug。保留 MVI 声明的精度/scale，移除元素的有损 serde 来回转换。宽整数字面量通过 JSONB DECIMAL 冷路径保留精确值，65 位合法整数和 1e40 均可建立 DECIMAL(65,0) 键。补齐非全 9 的 65 位数测试，以免饱和恰好掩盖丢精度。JSON_CONTAINS/OVERLAPS 也改用节点比较，不在 DOM 往返中重新丢失精度。 |
| 08 | 真类型问题。运行期 Bytes 在 CAST/JSON_ARRAY 等入口生成 type15 opaque，JSON_TYPE 返回 BLOB，并保留 base64 文本呈现。 |
| 09 | 修复词法整数 -0；含小数/指数的 -0.0、0e0 保持 DOUBLE。避免粗暴把所有负浮点零改成 INTEGER。 |
| 10 | 修复容器层数边界。允许 100 层容器加标量（depth=101），拒绝第 101 层容器，包括空容器情形。 |
| 11 | 防御问题，构造非法 UTF-8 JsonDiff 路径复现解码风险，改为拒绝事件，不再 lossy 改写路径。 |
| 12 | 防御问题，构造坏子节点和 2000 层 JSONB 验证。存储解码边界及递归读/导出入口校验完整结构和深度；坏计数、子节点、UTF-8 不进入内部的无错误迭代器。内部迭代器保留轻量接口，公共/持久化入口负责拒绝坏文档；未声称把所有 Iterator Item 改成 Result。 |
| 13.1 | 修复已验证的 UNSIGNED 数值越界 3904 分类，并为写入转换错误附索引名；非数字元素仍报 3903。 |
| 13.2 | 保留包含原始 path 的清晰错误信息；不为完全复制 oracle 位置号删除有用诊断。 |
| 13.3 | 保留 JSON 统一 wire type=245 / binary charset 元数据，以及驱动可能返回 bytes 的行为；编码正确性已独立修复，不承诺各驱动对表达式结果使用相同宿主类型。 |
| 13.4 | DDL 对不支持的 MVI 定义已有明确错误类别与表达式信息；不单为错误文案逐字对齐修改 DDL 语义。写入元素错误的索引名已补。 |

保留报告所列更合理设计：JSON_TABLE 更宽泛的 DEFAULT、表达式左侧 JSON 箭头、原子 LOAD DATA、明确拒绝 UNIQUE MVI 等。未重新修改报告已证伪的事务回滚、断连锁释放、COM_CHANGE_USER init_connect 等行为。

## 函数索引报告逐项裁决

| 编号 | 裁决和处理 |
| --- | --- |
| F1 | 真 bug。DDL 固定结果 collation 并持久化到数据字典；写入、普通/参数化探测及 INLJ 使用同一个 ParamTarget。覆盖 CI 等值、IN、LIKE、唯一约束、二进制 collation、prepared 和重启。扩大测试另发现 ASCII radix 的候选顺序不一定符合 UCA；bulk load 前按真实比较器验证，必要时退回比较排序，避免建成不可查询的树。 |
| F2 | 真 bug。DECIMAL 运算沿用共享的 precision/scale 推导，字符串数值进入 Double 域，不能统一压成 BigInt。覆盖唯一约束及 >2.0 等区间，避免点查 residual 偶然掩盖问题。 |
| F3 | 真一致性问题。写入时在 DML 严格转换作用域计算函数索引表达式，截断错误报函数索引诊断；不修改普通 SELECT 的警告语义。 |
| F4 | 真问题。SIGNED/UNSIGNED MVI 复用精确舍入和边界核，1.5 不再被当成非法非整数；仍拒绝越界和无法转换的值。查询保留 residual，不能采用 oracle 可能因省略 residual 而匹配错误行的行为。 |
| F5 | 真问题。引用 TIMESTAMP 的表达式索引继续维护，但不提供读侧过滤、ORDER/GROUP 顺序或唯一访问路径；索引提示不能绕过。以读会话的实际表达式结果为准，不把跨时区查询强行解释成写入时区。 |
| F6 | 真优化缺陷。单来源表达式指纹忽略限定列名前缀，覆盖 equality/range/IN/tuple/ORDER/GROUP 匹配。最终直接在哈希过程中归一，不克隆 AST，避免深表达式在小栈上递归 Drop 的回归。 |
| F7 | 真 DDL 回显问题。三参数内部 CONVERT_USING 引入符节点重新输出为可重放的 charset introducer；不再将内部函数名泄漏到 SHOW CREATE。 |

## 存储格式和使用限制

catalog 升为 **66**；物理复制的 collation-key 版本升为 **3**。新增 `mysql.index_column_usage.expr_collation_id` 保存 DDL 时的语义。旧的二进制字符串键、被截断的 DECIMAL 键和旧 path 生成的 MVI 键不能继续被新代码探测，所以显式拒绝旧开发目录，而不是静默使用。

现有 v65 目录须用旧版本逻辑导出，在新目录重新建表/建索引并导入；物理备份不会把旧键语义升级为新键。主备必须同版本部署。没有自动修改或迁移用户已有目录。

JSON 超出 64 位整型范围的合法整数字面量现在以精确 DECIMAL 节点保存；其 JSON_TYPE 可显示 DECIMAL。这是保留数值信息的设计选择，不通过近似浮点来假装更兼容。

## 官方语义依据

- [MySQL 8.0 account resource limits](https://dev.mysql.com/doc/refman/8.0/en/user-resources.html)：账号非零显式值覆盖全局默认值，不是取最大值。
- [MySQL 8.0 JSON search functions](https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html)：JSON_VALUE 的返回转换及 ON ERROR。
- [MySQL 8.0.46 Field_typed_array](https://dev.mysql.com/doc/dev/mysql-server/8.0.46/classField__typed__array.html) 及 [转换源码](https://github.com/mysql/mysql-server/blob/mysql-8.0.46/sql/item_json_func.cc)：多值索引按声明类型转换并保留实际条件检查。

## 最终验证

| 验证 | 实际结果 | 日志 |
| --- | --- | --- |
| 修复前新增 SQL 回归 | 9/9 失败，证明问题存在 | `/tmp/mydb/audit1031-baseline.log` |
| 修复前完整进程比较 | 15 项检查均失败；含真实配额、分包、65 位数、有效复制 token 下的 bootstrap 锁探针 | `/tmp/mydb/audit1031-process-baseline-complete.log` |
| 最终定向库回归 | **347/347 通过**，含全部 22 项新增 audit1031 回归、函数索引、深表达式小栈、会话、配置、原生复制测试 | `/tmp/mydb/audit1031-targeted-complete.log` |
| 最终完整库回归（用户原有本机时区环境） | **10,512 通过，7 失败，44 skipped**；没有新增失败 | `/tmp/mydb/audit1031-full-verified.log` |
| 原始提交独立基线复核 | 同样的 7 项失败；另一个 spill 聚合检查通过 | `/tmp/mydb/audit1031-unrelated-baseline.log` |
| Release 构建 | `cargo build --release --bin mydb` 通过 | `/tmp/mydb/audit1031-release.log` |
| Release 真实进程及协议 | **15/15 通过**，含主备 seeding/replay/干净关闭/重启、prepared、charset、宽整数、动态端口与 CLI 约定 | `/tmp/mydb/audit1031-process-release.log`，详细数据库/日志目录 `/tmp/mydb/audit1031-7ptqjarb/` |
| 格式版本文档 | `bash scripts/render_storage_format_versions.sh --check` 通过 | 由 canonical constants 生成 |
| 补丁完整性 | `git diff --check` 通过，Python 回归脚本语法检查通过 | 工作区 |

完整回归保留的 7 项失败，在原始提交 `bcc0984` 的独立快照中逐项复现：

- `mysql80_compat_temporal::numeric_zero_and_typed_time_follow_mysql_temporal_conversion`：本机 SYSTEM 时区下预期日期不一致。
- `parallel::parallel_covering_index_aggregate_explain_and_count`。
- `parallel::parallel_hint_pushes_through_hash_join`。
- `parallel::parallel_hint_surfaces_in_explain`。
- `parallel::parallel_metrics_increment_on_parallel_query`。
- `parallel::parallel_seq_scan_cost_matches_amdahl_plus_setup`。
- `zone_map::parallel_zone_map_scan_schedules_surviving_pages_and_matches_serial`。

后六项主要是当前并行安全上限产生 2 workers，而测试假设 4 workers；未为“跑绿”修改用户运行环境或这些无关行为。不能把本次完整回归描述为全部通过。

扩大回归曾检出本轮 qualifier 规范化复制 AST 导致的小栈溢出；已在交付前改为无分配的指纹归一。随后 347 项定向及最终全量的对应小栈用例均通过。早期全量出现过一次 MIN/MAX spill 偶发差异；原始提交与当前代码各 10 次定向压力重跑均通过，最终全量也通过，未据此修改无关聚合实现。

可重跑命令（在仓库目录执行）：

```bash
CARGO_TARGET_DIR=/tmp/mydb/target cargo nextest run --lib --no-fail-fast
CARGO_TARGET_DIR=/tmp/mydb/target cargo build --release --bin mydb
/tmp/mydb/audit1031-venv/bin/python scripts/test_lifecycle_audit_1031.py \
  --binary /tmp/mydb/target/release/mydb
bash scripts/render_storage_format_versions.sh --check
git diff --check
```

Python 环境使用 `pymysql==1.1.1`、`cryptography`、`mysql-connector-python`；脚本会为每次运行建立独立目录，选择临时本地端口，最终检查正常 server 退出码。baseline 模式仅记录预期失败；普通模式任一检查失败即返回非零。仅用于 Unix/macOS 本机测试。
