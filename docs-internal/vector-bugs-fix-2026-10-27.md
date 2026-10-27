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
| VEC-01 | 是 | 重复坐标占满邻接表时会挤掉不同方向；仅修连通性还不足以跨过等距平台。邻居选择先保留不同坐标的方向，再用等坐标补位；查询允许等距节点继续导航但不扩大 ef 结果堆；chunked 构建增加连通性检查与桥接，内存纳入既有构建预算。DOT 采用实际坐标判等，不能套用 L2 的零距离判据。默认迭代扫描还会在 k-th 行落入混合同分尾部时继续扩大候选池，避免“行数够但被默认坐标占满”时提前停止；全体同分和显式 iterative_scan=off 不触发该扩大。 |
| VEC-02 | 是 | KNN 常量转换绕开严格文本解析。String、SmallString、Decimal、Bytes 与标量转换统一；原始二进制仍需显式 VECTOR_FROM_BINARY，内部 Vector 值保持原生类型。 |
| VEC-03 | 是 | 专用向量 TopK 不能表达 NULLS LAST，截断后排序无法补回丢失行。有效 NULL 顺序不符合该算子能力且距离可能为 NULL 时保留通用关系 TopK，覆盖显式语法和进程默认值；NOT NULL 的非 cosine 距离保持索引快路径，包含 DOT_PRODUCT DESC；存在同列 cosine 索引提供非零约束时，cosine 的 NULLS LAST 也保留索引。 |
| VEC-04 | 是，但缩小结论 | 物理墓碑不应占有效结果预算；页式按页、紧凑导航和 mmap 三条搜索链均修复。原 UPDATE 示例同时带 WHERE 且关闭 iterative scan，过滤后不足 LIMIT 本身属于该设置的已声明行为，不承诺修成精确扫描。无 WHERE 的整个近邻簇删除案例在当前 HEAD 确认为 0 行，修复后首轮验证为 100 行。 |
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

- [官方 Connector/Python protocol.py](https://github.com/mysql/mysql-connector-python/blob/trunk/mysql-connector-python/lib/mysql/connector/protocol.py)：`_parse_binary_values` 的 VECTOR 分支读取 length-encoded 载荷。
- [官方 Connector/Python conversion.py](https://github.com/mysql/mysql-connector-python/blob/trunk/mysql-connector-python/lib/mysql/connector/conversion.py)：`_vector_to_python` 直接将载荷交给 float array，不读取维数头。

### 确定性旧逻辑失败证据

日志 `/tmp/mydb/vector-audit-code-baseline.log`：为测试加入 hook 后临时恢复对应旧分支，再编译运行，随后自动恢复修复源码。五项全部按预期失败：

- 冷加载取消后 checkpoint：后续打开返回 `ER_INDEX_CORRUPT` / 1712。
- 连续替换两个副本快照：队列实际 2、期望 1。
- 捕获 checkpoint 对象后 DROP INDEX：旧句柄失效 / 1030。
- 按页 fallback 等距排序：实际 `[9,5]`、期望 `[5,7]`。
- 成功重建后旧句柄检查：旧句柄失效 / 1030。

当前 HEAD 的无过滤墓碑复现另见 `/tmp/mydb/vecaudit-1027/baseline-head-delete.log`，ef 64/128/512 均返回 0 行。

### 回归边界

- 新增 11 项永久回归：三种指标 × 五种构建/写入方式 × LIMIT 100 × OPTIMIZE/重启；混合同分边界与全体同分的区别；严格文本转换；NULL 顺序；按页和紧凑导航墓碑；冷加载取消；副本快照替换及错误 fence 释放；checkpoint × DROP；旧读句柄；候选集内部 row-id tie-break。
- DOT 判等读取实际坐标；SQ4/PQ 等压缩算子保留其独立的非度量邻居选择规则，不把压缩分数为 0 等同于原始坐标相同。
- 三种指标的永久重复数据测试构造互相分离的簇，检验目标簇不会整体消失；不把普通 ANN 的精确 top-k 一致率作为语义保证。
- 最新定点复测 `/tmp/mydb/vector-audit-regression-recheck.log`：13/13 通过，包含上述新回归、DOT 降序索引快路径、SQ4 投影与 PQ 复制/提升。

扩展 DOT 复核发现，只有邻接选择修复仍会在 LIMIT 100 时返回 72/100（paged incremental）或 76/100（memory incremental）个目标簇行，OPTIMIZE 后为 100/100。该现象没有被记作修复成功；新增混合同分边界扩大逻辑后，将永久三指标回归由 LIMIT 32 加严至 LIMIT 100，最新定点测试 13/13 通过（`/tmp/mydb/vector-audit-boundary-final.log`），待最终 release 重放再记录对应黑盒结果。
