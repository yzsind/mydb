# 向量审计复核、修复与验证（2026-10-27）

## 复核原则与计划

以当前源码和本机测试为准，不把 ANN 的普通召回波动视为 SQL 正确性错误，也不直接采信原审计的根因。先保留失败证据，再实施修复；临时数据与日志均放在 `/tmp/mydb`。

1. VEC-01：复核重复坐标下的连通性与等距候选截断；覆盖 memory/paged、bulk/chunked/incremental、OPTIMIZE 与重启，另测随机数据召回和耗时。
2. VEC-02：统一字符串常量、短字符串与参数的严格文本转换；验证合法文本、显式二进制转换与非法文本的各执行计划。
3. VEC-03：在不能满足 NULL 顺序时禁止错误的 TopK 下推；比较 NULLS FIRST/LAST、默认顺序、可空值与 cosine 零向量。
4. VEC-04：墓碑仅作为导航节点，不占 live 结果预算；覆盖页式缓存导航和按页回退、删除与更新。
5. VEC-05～07：区分提交后维护失败的必要 fail-stop、冷读错误与持久化损坏；以故障注入、替换 retirement 快照验证需要修复的分支。
6. VEC-08～14：核实产品约定和调用时序；对确认的并发错误、确定性问题增加可控回归，性能观察单独记录。
7. 验证：先运行新回归确认失败，再运行修复后的相关 nextest、构建和本机 SQL 重放；检查已有向量、恢复、复制相关测试。

## 原始复现

- 基线 HEAD：`03ade1d1`，开始时工作树干净。原审计 release binary（12:23）与当前检验脚本保留在 `/tmp/mydb/vecaudit-1027`。
- 命令：`/tmp/mydb/vector-bench-validation/bin/python -u /tmp/mydb/vecaudit-1027/rerun_all.py`。
- 原始输出：`/tmp/mydb/vecaudit-1027/before-fix.log`。
- VEC-01：paged bulk 目标簇 0/100，paged incremental 66/100。
- VEC-02：非法文本查询成功返回三行，projection 拒绝同一文本。
- VEC-03：NULLS LAST 返回 `4,1,2,3`，期望 `1,2,3,4`。
- VEC-04：更新后 5/5 查询少于 LIMIT 50。

## 逐项结论与最终验证

### 已完成的逐项复核

| ID | 是否需要修改 | 复核与处理 |
| --- | --- | --- |
| VEC-01 | 是 | 重复坐标占满邻接表时会挤掉不同方向；仅修连通性还不足以跨过等距平台。邻居选择先保留不同坐标的方向，再用等坐标补位；查询允许等距节点继续导航但不扩大 ef 结果堆；chunked 构建增加连通性检查与桥接，内存纳入既有构建预算。DOT 采用实际坐标判等，不能套用 L2 的零距离判据。非量化内积检索的默认迭代扫描还会在 k-th 行落入混合同分尾部时继续扩大候选池，避免“行数够但被默认坐标占满”时提前停止；全体同分和显式 iterative_scan=off 不触发该扩大。 |
| VEC-02 | 是 | KNN 常量转换绕开严格文本解析。String、SmallString、Decimal、Bytes 与标量转换统一；原始二进制仍需显式 VECTOR_FROM_BINARY，内部 Vector 值保持原生类型。 |
| VEC-03 | 是 | 专用向量 TopK 不能表达 NULLS LAST，截断后排序无法补回丢失行。有效 NULL 顺序不符合该算子能力且距离可能为 NULL 时保留通用关系 TopK，覆盖显式语法和进程默认值；NOT NULL 的非 cosine 距离保持索引快路径，包含 DOT_PRODUCT DESC；存在同列 cosine 索引提供非零约束时，cosine 的 NULLS LAST 也保留索引。 |
| VEC-04 | 是，但缩小结论 | 物理墓碑不应占有效结果预算；页式按页、紧凑导航和 mmap 三条搜索链均修复。原 UPDATE 示例同时带 WHERE 且关闭 iterative scan，过滤后不足 LIMIT 本身属于该设置的已声明行为，不承诺修成精确扫描。无 WHERE 的整个近邻簇删除案例在起始 HEAD 确认为 0 行，修复后首轮验证为 100 行。 |
| VEC-05 | 不修改安全隔离规则 | 提交后维护失败的 fail-stop 是 docs/vector-indexes.md 明确声明的持久化契约，不能仅因单次分配发生在写页前就推断整个已提交批次没有变更。原文将所有 maintenance error 混在一起：私有后台替换失败已有 `.vmaintenance` 状态、保留旧图和启动后重试；已有回归实际通过。缺少通用在线修复入口是运维能力边界，本次没有证据支持安全地自动清除全局失败标记。 |
| VEC-06 | 是 | 取消冷加载也会通过 build guard 持久化 finalized=false。用加载前发布 hook 改变 epoch，随后 checkpoint 刷盘，旧逻辑稳定报 1712；新建私有图或已经开始写入回放的图才保守执行磁盘 build invalidation；纯冷读取、准入或 publication epoch 检查失败只撤销本次句柄，不修改健康文件。 |
| VEC-07 | 是 | 副本不可写源图，所以不应靠主动 purge 清队列；其新加载的源 retirement 快照应替换同索引旧快照。旧逻辑连续导入两个快照时队列从 1 变 2；修改后替换旧代/旧 lineage、同步释放内存记账，保留主库的合并语义。 |
| VEC-08 | 否 | BQ 是有损压缩，默认参数召回不足是质量观察，产品已明确不保证 ANN 召回，不属于静默数据损坏。扩大 ef 的收益也不能转换为默认参数必须精确的承诺。 |
| VEC-09 | 不按所述方案修改 | 原文“固定 4-byte dim header”不成立。官方 Connector/Python 对 VECTOR 先读 length-encoded string，再直接构造 float array。mydb 目标是 MySQL 8.0 协议及自有向量扩展，MySQL 9 原生向量参数是兼容性扩展需求；当前对不支持的裸载荷明确报错，不应按错误格式猜测、吞掉或自动跳过 4 字节。 |
| VEC-10 | 是 | 在 checkpoint 已捕获图后同步 DROP INDEX，旧逻辑在最先的 ensure_valid 阶段就可失败（本次实际为 1030）。刷新前在图 map 读锁下检查当前身份，后续缺失 catalog index 时跳过已撤销对象。 |
| VEC-11 | 是 | 新代发布后不应 invalidate 正持有 Arc 的旧读句柄；已有 retired-generation 弱引用机制负责文件生命周期。回归保留旧句柄，重建并 checkpoint 后继续搜索。真正 DROP/存储替换的失效规则不变。 |
| VEC-12 | 内部一致性修正 | SQL 未声明第二排序键时，不承诺相等距离行的全局顺序；原条目不能算严格 SQL 语义错误。但 fallback 在相同候选集上应遵循其他后端的 row-id tie-break，已统一排序器。测试先复现 `[9,5]`，再验证 `[5,7]`。 |
| VEC-13 | 是，超时加固 | 两个副本写侧等待复用 30 秒截止时间与取消检查；超时明确返回锁等待错误，物理 redo 的 lease 在错误路径释放 fence。短截止时间故障测试验证错误码与正常无读者路径。 |
| VEC-14 | 否，暂不作为 bug | 指向的是 online COPY 的逐行追赶变更，不是整份 COPY/CTAS 初始图的一行一条 WAL；初始持久化图通过有界镜像块发布。逐行 tail journal 是性能优化候选，尚无错误结果或量化回归证据，不在本次修改提交/恢复协议。 |

MySQL 协议复核依据：

- [官方 MySQL 参数序列化](https://github.com/mysql/mysql-server/blob/trunk/sql-common/bind_params.cc)：VECTOR 使用 `store_param_str`，写入 length-encoded 长度和绑定缓冲区，不自动添加维数头。
- [官方 Connector/Python protocol.py](https://github.com/mysql/mysql-connector-python/blob/trunk/mysql-connector-python/lib/mysql/connector/protocol.py)：`_parse_binary_values` 的 VECTOR 分支读取 length-encoded 载荷。
- [官方 Connector/Python conversion.py](https://github.com/mysql/mysql-connector-python/blob/trunk/mysql-connector-python/lib/mysql/connector/conversion.py)：`_vector_to_python` 直接将载荷交给 float array，不读取维数头。

### 确定性旧逻辑失败证据

日志 `/tmp/mydb/vector-audit-code-baseline.log`：为测试加入 hook 后临时恢复对应旧分支，再编译运行，随后自动恢复修复源码。五项全部按预期失败：

- 冷加载取消后 checkpoint：后续打开返回 `ER_INDEX_CORRUPT` / 1712。
- 连续替换两个副本快照：队列实际 2、期望 1。
- 捕获 checkpoint 对象后 DROP INDEX：旧句柄失效 / 1030。
- 按页 fallback 等距排序：实际 `[9,5]`、期望 `[5,7]`。
- 成功重建后旧句柄检查：旧句柄失效 / 1030。

起始 HEAD `03ade1d1` 的无过滤墓碑复现另见 `/tmp/mydb/vecaudit-1027/baseline-head-delete.log`，ef 64/128/512 均返回 0 行。

### 回归边界

- 新增 11 项永久回归：三种指标 × 五种构建/写入方式 × LIMIT 100 × OPTIMIZE/重启；混合同分边界与全体同分的区别；严格文本转换；NULL 顺序；按页和紧凑导航墓碑；冷加载取消；副本快照替换及错误 fence 释放；checkpoint × DROP；旧读句柄；候选集内部 row-id tie-break。
- DOT 判等读取实际坐标；SQ4/PQ 等压缩算子保留其独立的非度量邻居选择规则，不把压缩分数为 0 等同于原始坐标相同。
- 三种指标的永久重复数据测试构造互相分离的簇，检验目标簇不会整体消失；不把普通 ANN 的精确 top-k 一致率作为语义保证。
- 定点复测 `/tmp/mydb/vector-audit-regression-recheck.log`：13/13 通过，包含上述新回归、DOT 降序索引快路径、SQ4 投影与 PQ 复制/提升。

扩展 DOT 复核发现，只有邻接选择修复仍会在 LIMIT 100 时返回 72/100（paged incremental）或 76/100（memory incremental）个目标簇行，OPTIMIZE 后为 100/100。该现象没有被记作修复成功；新增混合同分边界扩大逻辑后，将永久三指标回归由 LIMIT 32 加严至 LIMIT 100，最新定点测试 13/13 通过（`/tmp/mydb/vector-audit-boundary-final.log`），最终 release 已重放，paged/memory incremental 均为 100/100；三种指标的默认设置复现用例均通过。

## 最终验收

验收期间，Rust 修复源码已纳入提交 `3bd7dfec`，后续补充本报告未修改 Rust 源码。

当前源码保持原有 Cargo 编译配置，全部使用本机 macOS arm64。新增 **11 项永久回归**；最终选择的 **358 项相关测试全部通过**，包括恢复、崩溃注入、checkpoint、物理复制及提升、全部原生类型、量化、分区与 prepared statement。没有运行测试服务器。

```sh
TMPDIR=/tmp/mydb cargo nextest run --lib --test-threads 8 \
  -E 'test(engine::vector::) | test(hnsw_regressions::) | test(vector_datatypes::) | test(vector_quantization::) | test(vector_quality::) | test(vector_type::) | test(vector_page_tests::) | test(vector_audit_)' \
  --no-fail-fast
cargo build --release
target/release/mydb --help
git diff --check
```

- [最终 nextest 日志](/tmp/mydb/vector-audit-verification-nextest.log)：358 passed，0 failed，160.660 秒。
- [最终 release 构建](/tmp/mydb/vector-audit-verification-release.log)：成功，没有编译警告。
- [CLI help 输出](/tmp/mydb/vector-audit-help.log)：正常退出。
- 冻结验收二进制：`/tmp/mydb/vector-audit-final-release`，SHA-256 `2eb26984a128d84b6d1729d52495f597a985b40be10a19235812d57e32c2ea6a`，与 `target/release/mydb` 相同。

### SQL 实测

| 复现项 | 修复前 | 最终结果 |
| --- | --- | --- |
| L2 paged auto bulk，9,500 默认坐标 + 500 目标簇 | 目标簇 0/100 | 100/100 |
| L2 paged incremental | 66/100 | 100/100 |
| L2 memory bulk，19,500 默认坐标 + 500 目标簇 | 起始 HEAD 实测 0/100 | 100/100 |
| L2 paged chunked bulk | 起始 HEAD 实测 66/100 | 100/100 |
| L2 memory incremental（对照） | 100/100 | 100/100 |
| COSINE / DOT，五条构建与写入路径，含 OPTIMIZE | 先前 DOT incremental 为 72/100、76/100 | 20 个检查全部为目标簇 100/100 |
| FLOAT16 / BFLOAT16 / INT8，以及 FLOAT32 SQ8 / SQ4，三种指标 | 补充类型覆盖 | 15 个组合全部目标簇 32/32 |
| 错误文本 `abcdefghijklmnop` | KNN 接受、projection 报错 | 两者都明确报 invalid VECTOR text；prepared 错误后可重绑合法值 |
| L2 NULLS LAST | `4,1,2,3` | `1,2,3,4` |
| MYSQL / PG / LAST 默认 NULL 顺序，显式 FIRST/LAST、cosine 零值、DOT DESC | 专用路径不一致 | 三组进程配置与通用 TopK 完全一致 |
| 整个近邻簇删除，iterative=off，ef 64/128/512 | 起始 HEAD 均为 0 行 | 三种页式查询路径均返回 100 行 |
| 无 WHERE 的 UPDATE churn，iterative=off | 5/5 次不足 LIMIT 50 | 0/5 次不足 |

墓碑测试禁用了自动 compaction，保留 **20,000 个节点 / 10,000 个磁盘墓碑**；使用 HNSW hint 并检查 EXPLAIN，排除小表 CBO 自动 exact scan 的干扰。mmap 冷重开报告 `paged_mapped`；禁用映射并给定缓存预算覆盖紧凑导航，禁用映射且缓存预算为 0 覆盖按页路径。ANN 可能与 exact top-100 存在少量成员差异，这不等同于行数不足缺陷。

原审计合并脚本里 `WHERE id < 9000` 且 iterative=off 的 UPDATE 案例最终仍有 3/5 次不足 LIMIT。该设置明确关闭过滤后的候选扩大，因此没有把它伪报为修复成功；不带 WHERE 的配对案例才用于检验物理墓碑是否占 ef 预算。

受影响旧 chunked 索引也已用最终二进制重新打开并执行 OPTIMIZE：两次查询均为 100/100。冷加载不会为所有旧拓扑强制做 O(n) 重建；OPTIMIZE 可明确替换为新构建逻辑。

SQL 证据：

- [原始合并复现的最终重放](/tmp/mydb/vector-audit-sql-final.log)
- [起始 HEAD 的 memory/chunked 基线](/tmp/mydb/vector-audit-extra-head-before.log)
- [L2 三路径](/tmp/mydb/vector-audit-l2-paths-final.log)、[chunked 重复数据](/tmp/mydb/vector-audit-chunked-duplicates-final.log)
- [COSINE / DOT 20 个检查](/tmp/mydb/vector-audit-metrics-final.log)、[原生类型及量化 15 个检查](/tmp/mydb/vector-audit-native-final.log)
- [NULL 顺序与 SQL prepared 重绑定](/tmp/mydb/vector-audit-boundaries-final.log)
- [mmap 墓碑](/tmp/mydb/vector-audit-tombstone-mapped-final.log)、[紧凑导航墓碑](/tmp/mydb/vector-audit-tombstone-compact-final.log)、[按页墓碑](/tmp/mydb/vector-audit-tombstone-fallback-final.log)
- [UPDATE churn，无过滤](/tmp/mydb/vector-audit-update-head.log)、[修复后](/tmp/mydb/vector-audit-update-final.log)
- [旧索引重开及 OPTIMIZE](/tmp/mydb/vector-audit-old-index-optimize.log)

### 通用性能对照

以起始 HEAD `03ade1d1` 独立构建的 `/tmp/mydb/vector-audit-head-release` 为基线；两者使用相同 release 配置。数据为固定种子的 10,000 行、32 维均匀随机 FLOAT32，构建线程数为 1，ef=256、dynamic factor=0、LIMIT 20。

构建采用两个本地进程交替执行：每条路径预热一次，再各构建 5 次，取中位数。查询采用两个进程交错请求并交替先后次序，每版本预热 200 次、测量 2,000 次。这样避免把顺序批次中较大的系统尾延迟波动当作代码变化；顺序批次原始日志仍保留在 `/tmp/mydb/vector-audit-perf-*-final-*.log`。

| 路径 | 构建中位数 s：HEAD → 修复 | 查询 p50 ms：HEAD → 修复 | 查询 p95 ms：HEAD → 修复 | 配对延迟中位数变化 | 抽样 Recall@20：HEAD / 修复 |
| --- | --- | --- | --- | --- | --- |
| paged/auto | 0.671 → 0.656 | 0.249 → 0.252 | 0.340 → 0.349 | +1.6% | 99.5% / 99.5% |
| paged/chunked | 0.964 → 0.977 | 0.267 → 0.272 | 0.379 → 0.394 | +1.9% | 99.5% / 99.5% |
| memory/auto | 0.728 → 0.733 | 0.265 → 0.266 | 0.591 → 0.557 | +0.2% | 99.5% / 99.5% |

在该对照中，构建中位数为 −2.3%～+1.3%，配对延迟中位数为 +0.2%～+1.9%，抽样召回保持一致，未见明显通用性能回退。没有修改 Cargo 编译配置或为某个 benchmark 设置执行特例。

常规候选继续使用原 Algorithm 4 快路径，仅同分候选进入重复坐标处理。额外的混合同分候选扩大仅用于非量化内积 HNSW；Hamming/Jaccard 的自然离散同分保留原扫描策略。零内积查询依据恒零性质跳过额外同分导航。

另用 3,000 行、32 维数据和 64 个符号零组合验证零内积查询，避免结果缓存掩盖完整导航开销：FLOAT32 paged 的 p50 为 0.189 → 0.178 ms，FLOAT32 memory 为 0.178 → 0.167 ms，FLOAT16 paged 为 0.323 → 0.315 ms，均保持原有速度。报告中的 `pre_zero` 是中间验证版本，不是 HEAD 基线。

- [交错查询性能数据](/tmp/mydb/vector-audit-perf-paired-acceptance.log)
- [交替构建 5 次样本](/tmp/mydb/vector-audit-build-paired-acceptance.log)
- [零内积性能与行数检查](/tmp/mydb/vector-audit-zero-performance.log)
- 可重跑脚本：`/tmp/mydb/vector-audit-performance-paired.py`、`/tmp/mydb/vector-audit-build-paired.py`、`/tmp/mydb/vector-audit-zero-performance.py`。
