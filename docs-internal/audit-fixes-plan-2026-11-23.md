# 三份审计报告复核、修复与验证

范围：`agg_rollup_audit_20261123.md`、`rest-mcp-release-audit-2026-11-23.md`、`connection-session-release-audit-2026-11-23.md`。全部验证仅使用本机，临时数据与日志位于 `/tmp/mydb/audit-fixes-1123`。

## 判定原则

先查源码，再以原始协议、SQL 差分或故障注入复现；修复前加入能失败的回归测试。保留更明确的错误、更严格的资源治理以及 mydb 已定义的设计差异。不以报告的严重度或“已确认”作为事实依据。

## 聚合检查

- BUG-1：已复现，修复非显式排序时 ROLLUP 的明细/小计嵌套输出；覆盖行式、向量、真实 NULL、HAVING/LIMIT 和现有 spill 回归。
- BUG-2/2a：已复现，分组前按真实可见列展开通配符，复用分组合法性校验，保证元数据与行宽一致；覆盖限定/非限定通配符、派生表、CTE、合法全键分组及关闭 ONLY_FULL_GROUP_BY 的路径。
- BUG-3：补行式/向量差分，统一三个 DECIMAL typed lane 的正确舍入转换。
- BUG-4：报告误判。MySQL 8.0.46 实测 `BIT_AND(x) IS NULL` 等均为 0；NULL 类型派生列显示空二进制中性值，并不是 SQL NULL。数值输入返回 MAX_U64/0/0。保留当前数值中性值语义。
- BUG-5：先复现代表值选择，检查稳定排序和同组 display value；避免为无定义的代表值增加全局开销。
- BUG-6：检查 ordinal 解析后的 BoundValue 与 Literal 匹配，修复 total row 的 NULL 传播。
- BUG-7：复现后改用真正的 GROUPING 参数错误 3602。
- BUG-8：检查流式结果的警告交付，补首次截断行号及 statement diagnostics 回归。

## REST / MCP 检查

- REST-1：以持续失败的 close_session 代理注入复现。close 有界等待，失败保留 context 和许可供 sweep 重试；DELETE/MCP 返回明确失败，不能声称已清理事务；清理错误记录日志。
- REST-2：报告误判。实现已经逐 representation 使用最具体 media range。JSON q=.5、NDJSON 经 */* 得 q=1，选择 NDJSON 正确。依据 RFC 9110 §12.5.1。
- P4-5：q=.0001 超过 RFC 9110 §12.4.2 最多三位小数的合法范围，不以此更改协商规则。
- MCP-1：复核无状态 transport 的 SDK 设计，保留每请求认证的无状态模式；不引入按 TCP 连接伪造的初始化状态。
- 其他 P4：无可达故障的防御性建议及明确设计取舍不做行为回退。

## 连接与会话检查

- C-1/2：已复现。补非 UTF8 字符串与二进制比较、autocommit session track 回归，修复公共语义路径。
- C-3/5：补复制协议回归。压缩 semi-sync 必须正确解码或在命令开始前明确拒绝不支持的组合；NON_BLOCK 正常 EOF 保持可用，取消/错误的半响应必须断连。
- C-4：统一会话显示的 max_allowed_packet 与实际协议准入，测试 GLOBAL 变化对新旧会话的行为。
- C-6/7/8：使用 duplex 故障输入覆盖未发送大载荷、空 envelope 和命令中途压缩 chunk 超时，同时保留命令首字节空闲语义。
- C-9：复核 poll_read 的唤醒语义，不能移除必要的 readiness re-arm 而造成挂死；以背压和 ACK 流测试判断。
- C-10：覆盖批量缓冲区重用时扩容，确保 reserve 基于 len，记账基于 capacity。
- C-11/13/14/15：分别覆盖 reprepare 元数据变化、STATE_CHANGE 实际 wire bytes、metadata NONE 的 cursor terminator、server_id=0 的注册错误。
- C-12：报告对 long data 的前提存疑。现有测试允许 NULL 位未置时省略对应内联值；应对照 MySQL 原始包，不能直接按报告更改消费规则。
- C-16/17/18/19/20：分别核对可达性和已有错误边界；不存在实际故障的断言/平台建议无需为了兼容性修改。
- D-1…D-7：保留报告已说明的设计取舍。

## 验证计划

1. 修复前记录 SQL/协议输出和新增回归失败。
2. 最小范围修复并运行针对性 nextest；所有测试临时目录使用 `/tmp/mydb`。
3. 运行聚合、表达式、MySQL packet/protocol/session、REST/MCP/sql_service 相关测试，之后完整 nextest。
4. cargo build，独立新实例实际重跑 SQL、原始协议和 API 探针；记录确切结果与限制。
5. 检查格式和 diff，不修改 Cargo 编译配置，不使用远程测试服务器。

规范参考：[MySQL 聚合函数](https://dev.mysql.com/doc/refman/8.0/en/aggregate-functions.html)、[RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html)。

## 复核修正（以实测覆盖初始计划）

1. **C-5 不修**：对 MySQL 8.0.46 和修改前 mydb 均发送 NON_BLOCK COM_BINLOG_DUMP，消费 EOF 后 PING 都遇到连接结束。更直接的工具验证：修改前 mydb 成功执行 `mysqlbinlog --read-from-remote-server ... mydb-bin.000001 mydb-bin.000002`，退出码 0，完整输出两个文件；报告所称常规工具用法损坏未复现。因此撤回让 dump 后连接复用的试改，保留原生命周期。
2. **C-12 不修**：原始包发送 LONG_DATA('AAA')，EXECUTE 第一参数 NULL 位不置位且不带第一参数内联值、第二参数 'BBB'；两引擎均返回 AAA/BBB。额外塞入第一参数内联值时，MySQL 会将它当成第二参数并忽略尾部，mydb 明确返回 1210 trailing bytes，后者更严格。MySQL 8.0.46 `protocol_classic.cc` 的 LONG_DATA_VALUE 分支也直接 continue，证实报告的“仅由 NULL bitmap 决定”前提错误。
3. **C-15 不修**：本机 MySQL 8.0.46 和 mydb 对 server_id=0 的 COM_REGISTER_SLAVE 都返回 OK，报告的兼容性依据不成立；保留注册身份顶替语义，撤回试加的拒绝逻辑。
4. **C-9 不修**：8 次 poll_read 是有界工作量；仅当每次都实际读到字节时才会到达自唤醒；一旦读侧 Pending 就由真实 IO 唤醒。删掉自唤醒可能令持续 ACK 背压场景失去进展，现有两项半包/写阻塞 ACK 回归通过。报告描述的是持续有效输入的 CPU 消耗，未证实无输入忙循环。
5. **MCP-1 不修**：rmcp 3.9.0 明确配置无状态 HTTP，SQL 会话是独立显式句柄，不是 MCP transport 会话；每请求仍认证与限额。维护跨 TCP 连接的 initialize 状态会破坏此设计。当前 MCP 协议本身也已移向无状态请求，不引入报告建议的近似状态检查。
6. **BUG-8 部分证伪**：修改前本机独立实例已能稳定给出 1260，但文字是 `Row was cut by GROUP_CONCAT()`。已确认并修复的是缺失首次截断行号；回归同时覆盖普通、ORDER BY、DISTINCT 的警告交付，不声称复现了“总是不发警告”。

复核参考：[MySQL 8.0.46 参数解析源码](https://github.com/mysql/mysql-server/blob/mysql-8.0.46/sql/protocol_classic.cc)、[MCP 无状态 HTTP SDK 说明](https://csharp.sdk.modelcontextprotocol.io/v2/concepts/stateless/stateless.html)。

## 本次改动范围

- 聚合：ROLLUP 输出按前缀嵌套，后代在对应小计之前，总计最后；向量路径即时发出已完成前缀，省去 completed 缓存。通配符在分组校验前展开，复用可见列和原有函数依赖规则。DECIMAL 三种 typed lane 使用统一正确舍入转换。常量 ordinal 的 BoundValue/Literal 绑定一致；GROUPING 参数错误使用 3602。
- 比较：折叠常量保留原始字符集/排序规则，常量 IN 同样绑定二进制比较字节，覆盖 GBK/latin1/utf8mb4 的对称等值、NULL-safe、IN、BETWEEN。
- GROUP_CONCAT：保留有界输出前缀的字节终点及原始长度，准确指出第一次截断的贡献行；跨 partial 合并时组合终点和行号，空值/空字符串不导致无界元数据。
- 协议：SET autocommit 字面量路径补齐 session track；STATE_CHANGE 编码改为 `[02 01 31]`；prepared 结果元数据变化设置 0x400，按受控内存准入刷新缓存；metadata NONE 的旧 EOF 游标补终止包。
- 压缩：拒绝空 envelope，内层大包在载荷收到后才分配，命令中途所有 chunk 的首字节受 net_read_timeout 约束，首命令字节仍使用 wait_timeout。修正 batch reserve 的 len/capacity 混用。
- 会话：普通入站包和 prepared long data 统一使用会话 max_allowed_packet 快照；REST/MCP close 最多等待 500 ms，持续失败返回 503/unknown，保留 context/许可并记录日志，由 sweep 重试。
- 复制：压缩 semi-sync dump 返回明确 1235，且释放对应复制注册，避免进入 quorum 后静默丢 ACK；普通压缩 SQL、异步复制及未压缩 semi-sync 保持可用。

## 不改动的其他项目

- REST P4-2（非有限数报错）、P4-3（XA 两阶段前置条件）保留严格语义。P4-4 只有内部记账不变量破坏才可触发，没有可达故障复现。
- C-16 的 CR_* 编码断言是内部错误域不变量，现有路径没有可达错误；C-17 环境变量 0 明确表示禁用握手超时；C-18 两个调用点均提供正确 nonce；C-19 非 Unix 分支当前不可达；C-20 不支持的参数类型缓存组合明确返回错误。无需为不可达加固建议改变行为。
- 三份报告原有的已接受设计差异全部保留。

## 已记录的修复前证据

- `agg-before.txt`：ROLLUP 层级输出、通配符漏检、2027 malformed packet 和后续 2013。
- `agg-before-safe.txt`：常量总计未 NULL、GROUPING=3580、DECIMAL 方差错误、ROLLUP 代表值 A、GROUP_CONCAT 警告缺行号。
- `baseline-direct.log`：最初 8 项回归，7 项失败；其中旧分配探针检查释放后的池而未捕获问题，已替换为挂起时受控内存断言。
- `protocol-before-full.log`：metadata 改变无 0x400、metadata NONE cursor 超时、压缩空包和 chunk 边界 2s 未结束、会话显示 4096 而实际按 GLOBAL 8192 接受，GLOBAL 降为 4096 又拒绝旧会话和 long data。
- `protocol-mysql.json`：MySQL 8.0.46 差分，纠正 C-5/C-12/C-15，并确认 0x400 与 STATE_CHANGE 编码。
- 针对性回归第一轮 10 项：8 通过，2 失败（常量 IN 缺字符集绑定、测试用临时会话 SET 未持久化），随后完成修正。
- 相关回归第一轮 463 项：462 通过，1 项为旧测试仍断言 3580，已更新为正确的 3602。

所有上述日志路径均相对于 `/tmp/mydb/audit-fixes-1123/`。

## 完整回归揭示的补充问题与判定

- 第一轮全量：12464 项执行，12462 通过，2 失败。其一是通配符展开把隐式列名变成显式别名，导致多表 GROUP BY 同名列的 1052 被 1055 提前覆盖；已将执行期展开与持久视图名称冻结区分，前者不制造显式别名。其二是对 BUG-5 写了过强的“必须显示 a”断言，见下一条修正。
- **BUG-5 不修**：重复实测同一 ROLLUP 查询，15 次中 a/A 分别出现 6/9 次，但二者均为同一 collation 等价类，HEX 始终与选中显示值一致，COUNT 始终为 2。没有 ORDER BY 的输入不承诺扫描顺序，MySQL 的当前 first-seen 拼写不是跨执行计划的 SQL 契约。为兼容特定拼写约束并行归并会增加开销且缺乏语义依据。回归改为验证等价类、计数及显示表达式一致，而不是指定原始字节代表。
- **C-1 补充根因**：最初 scalar/folded-constant 修复在引擎普通调用上通过，但 MySQL 协议真实 COM_QUERY 仍返回 0；新增 cached-text-shape 回归复现此遗漏。PlanRow 的比较、IN、BETWEEN 拿到了 binary 比较域，却直接使用内部 UTF8 字符串字节，未转换回来源 charset。现在这些路径统一使用来源字符集字节，与 scalar lane 的转换规则一致，并保留自动参数化占位符的 text/binary derivation。实际 MySQL 协议验证作为必需终验，不以引擎单测替代。


## 最终逐项处置索引

| 报告 | 已修复 | 无需修复 / 保留 |
|---|---|---|
| 聚合 | BUG-1、BUG-2、BUG-2a、BUG-3、BUG-6、BUG-7、BUG-8（警告行号） | BUG-4（中性值，报告误读 NULL）、BUG-5（collation 等价类代表拼写不构成错误） |
| REST/MCP | REST-1；P4-1 补上传时限文档 | REST-2、P4-5（RFC 语义/非法 q 值）、MCP-1（无状态设计）；其余 P4 保留严格行为与内部不变量 |
| 连接/会话 | C-1、C-2、C-3、C-4、C-6、C-7、C-8、C-10、C-11、C-13、C-14 | C-5、C-12、C-15（本机差分证伪）；C-9（实际输入驱动的有界工作，不是空转）；C-16…C-20（无可达故障或明确配置边界） |

统计按报告 ID：19 项已修复（含 BUG-2a 子项，不把文档补充计入 bug 数）。

## 实际终验（已完成部分）

- `cargo build`：通过。`target/debug/mydb --help`：通过。
- `runtime-final.log`：最终独立实例 32 项 SQL/REST/MCP 检查全部通过，包括 12 条新连接的 GROUP_CONCAT 1260 与精确行号、CTE/派生表/宽列协议形状、非 UTF8 等值/IN/BETWEEN、REST DELETE 回滚与无状态 MCP。
- `protocol-final.log`：全部原始协议断言通过，覆盖 C-3/C-4/C-11/C-13/C-14 与不修项 C-5/C-12/C-15 的真实行为。空压缩 envelope 0.0s 断开，命令中途停顿 1.0s 断开（配置 net_read_timeout=1）。
- 报告原探针 `test_bug_gbk_compare.py`、`test_bug_session_track.py`：最终实例均 PASS；GBK hex 比较 0→1，autocommit OK 状态位 0x4000 与载荷均出现。
- 持续清理故障注入：close 在约 500ms 内返回 SESSION_CLEANUP_FAILED/unknown，许可与事务上下文仍被持有；故障解除后 sweep 正常释放。
- 最终新增/相关定点回归（含 cached text shape、1052 优先级）：13/13 通过。


## 最终验证结论

- 全量 `TMPDIR=/tmp/mydb cargo nextest run --no-fail-fast`：**12465/12465 通过，52 跳过**（`full-final.log`）。
- 最后保留 DECIMAL 预计算除数快速路径后，增量相关测试 **465/465 通过**（`verified-related.log`），最终 `cargo build`、`--help`、`cargo fmt --all -- --check`、`git diff --check` 全部通过。
- 最终构建重启后再次执行实际验证：**32/32** SQL/REST/MCP 检查通过（`runtime-verified.log`）；原始协议全部断言通过（`protocol-verified.log`），并验证从已有测试数据目录正常恢复启动。
- 性能收尾仅改变 typed DECIMAL 方差转换的快速路径：除数在列循环外预计算；mantissa <= 2^53 且 scale <= 22 的常见输入仍直接除法，scale=0 仍直接转换；其余输入使用正确舍入的 canonical converter。

### 本机性能烟测

同一台机器、开发构建、20000 行、每条查询预热 4 次后执行 17 次，取客户端总耗时中位数（毫秒）。这是回归烟测，不是发布性能基准。修改前高精度方差的结果错误，其时间仅供观察精度修复成本。

| 查询 | 修改前 ms | 修改后 ms |
|---|---:|---:|
| 常见精度 DECIMAL 方差 | 4.386 | 4.434 |
| 整数方差 | 4.515 | 4.501 |
| 普通分组 COUNT/SUM | 7.604 | 7.621 |
| 分组 GROUP_CONCAT | 30.722 | 31.367 |
| 大精度 DECIMAL 方差（修复前错答） | 4.627 | 7.121 |
| 两键 ROLLUP | 8.065 | 8.108 |

通用查询路径未见退化；大精度 DECIMAL 方差因正确舍入而增加转换成本，未把修复前的低精度错答路径当成可保留的性能契约。采样日志为 `perf-quiet-before.json` / `perf-quiet-after.json`。

### 清理与边界

所有本次启动的 mydb 测试实例均在终验后停止；复现数据、SQL、脚本与日志保留在 `/tmp/mydb/audit-fixes-1123/` 供复查。本机 MySQL 对照的测试 schema 已清理，读取超时恢复默认。未使用远程测试服务器，未改 Cargo 编译配置。

压缩 semi-sync 尚未实现 cancel-safe 解压 ACK；本次采用显式错误而非静默异步降级，客户端需使用未压缩连接。该限制已经写入正式兼容性文档。
