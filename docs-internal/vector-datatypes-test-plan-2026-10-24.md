# VECTOR 多 datatype 测试计划

日期：2026-10-24。状态：测试计划；执行状态见独立结果文档。

## 正确性用例

| 编号 | 输入与场景 | 预期 |
| --- | --- | --- |
| VDT-001 | 五类型声明、默认、大小写、CAST、DESCRIBE/SHOW/IS | 类型、维度准确回显 |
| VDT-002 | 多余/缺失 modifier、非法 datatype、上下限±1 | 明确报错，不忽略参数 |
| VDT-003 | f16/bf16 全部 65536 位模式 | 有限值位级往返，NaN/Inf 拒绝 |
| VDT-004 | 舍入中点、最大值、子正规、±0、溢出 | 最近偶数舍入及溢出拒绝 |
| VDT-005 | INT8 全值域、小数、越界、高维平方差 | 严格整数，宽累加不溢出 |
| VDT-006 | BINARY 1/7/8/9/63/64/65/65536/最大维 | bit 维度准确，填充验证 |
| VDT-007 | 混合数值距离、NULL、零向量 | 保留查询精度，零向量规则一致 |
| VDT-008 | binary Hamming/Jaccard、全零、稀疏/稠密、同距 | 与独立 popcount 参考一致 |
| VDT-009 | heap、overflow、spill、undo、参数、缓存、LOAD DATA | 类型与 payload 完整往返 |
| VDT-010 | UNION/CASE、SUBVECTOR、normalize、datatype 改列 | 规则符合设计，失败原子回滚 |
| VDT-011 | datatype×metric×HNSW/IVF×memory/paged，mapped/nav | 合法支持、非法组合报错 |
| VDT-012 | IVF 全探测 | 可见原值精确 TopK |
| VDT-013 | DML/回滚/RR旧快照/并发构建/分区/重建 | 可见性、发布与失效正确 |
| VDT-014 | 中断构建/损坏/重启/WAL/复制/提升 | 清晰拒绝或安全重建 |
| VDT-015 | 分页记录容量前后值 | DDL 发布前准入 |

精确真值按存储后数值和原查询建立。整数及 Hamming 精确相等；浮点以独立宽精度参考和误差界断言。

## 执行命令

所有测试 TMPDIR=/tmp/mydb；先针对性 cargo nextest，再 cargo nextest run --no-fail-fast、cargo build、cargo run -- --help、cargo fmt --all -- --check、git diff --check。日志写入 /tmp/mydb/vector-datatypes-2026-10-24。

## 性能

基线与修改版采用相同 Release 配置，至少五对交替运行，固定种子和配置。矩阵含 10k/100k、128/768/1536 数值维、256/1024/8192 bit 及容量边界；数据含聚类、均匀、重复、极值、稀疏/稠密和不同模长。报告构建/DML/精确扫描/ANN QPS、p50/p95/p99、Recall@10、峰值与受控内存、索引/WAL字节数。二值同距另报告距离阈值召回。

FLOAT32 QPS下降>5%、p99增加>10%、构建/DML下降>10%需复测修复。新类型固定参数 Recall@10≥0.95。纯 payload 压缩比不得当作整体索引压缩比。本机未覆盖平台标为未测试。

## 验收记录模板

每次记录：提交/源码校验、命令、环境、开始结束时间、通过/失败/忽略计数、证据路径、失败复现与修复、未覆盖项。测试计划不等于完成记录。

## 实际自动化覆盖

| 测试 | 覆盖 |
| --- | --- |
| `native_half_all_bit_patterns_validate_and_round_trip` | 两种 16-bit 格式各 65536 位模式、有限性与独立编码往返 |
| `native_half_midpoints_and_adjacent_f64_values_round_to_even` | 64382 个相邻正数中点、两侧 f64 邻值及负号镜像，共 386292 次转换 |
| `native_text_rounding_preserves_decimal_midpoint_sides` | 直接 f64 路径、长十进制中点两侧、舍入溢出边界 |
| `native_integer_text_is_exact_and_binary_padding_is_strict` | 浮点解析会掩盖的小数、范围、超长指数、binary 填充及最大 bit 维度 |
| `vector_datatypes_sql_storage_and_functions` | 五类型、函数、CAST、原始 payload、错误、strict 存储重启 |
| `vector_datatypes_exact_knn_preserves_query_precision` | FLOAT16/BFLOAT16/INT8 字段与非整数 FLOAT32 查询 |
| `vector_datatypes_native_indexes_*_matrix` | 每后端 28 个组合：datatype×metric×HNSW/IVF，原生查询、独立 f64/bit 真值、DML 维护 |
| `vector_datatypes_wide_binary_overflow_and_derived_domains` | 65537 bit、overflow、derived、UNION 提升和跨域拒绝、重启 |
| `vector_datatypes_native_paged_restart_snapshot_and_mapping` | 两种 mmap 模式×四类型×两算法，真实磁盘、RR、删除、重启、物理副本角色切换 |
| `vector_datatypes_prepared_result_domains_follow_runtime_bindings` | 字符串/二进制参数、相同文本长度的不同维度、不同 datatype、结果 metadata |
| `vector_datatypes_cosine_index_rejects_preexisting_zero_rows` | 四数值类型×三算法，关闭 eager warmup 时 DDL 仍拒绝已有零向量 |
| `vector_row_value_preserves_native_envelopes` | 五类型的 ROW/FULL portable envelope 接收往返 |

## 执行结果

实际结果独立记录在 [测试结果](vector-datatypes-test-results-2026-10-25.md)。本计划仅描述覆盖和验收方法，不代表所有项目均已执行。

## Release 测量入口

FLOAT32 配对使用原有 `hnsw_end_to_end_bench`，基线与候选的该部分源码逐字节相同，SHA256 为 `071fba04a45cc2806a203a3146683ac17dd89d03423e117c71c851b77dcb6708`。基线必须由 `baseline.tar` 的原始提交重新编译，不能用未证明源码来源的已有程序代替。

新增忽略基准 `vector_datatypes_end_to_end_bench` 支持 `VDB_DATATYPE`、`VDB_METRIC`、`VDB_ALGO`、`VDB_BACKEND`、`VDB_N`、`VDB_DIM`、`VDB_QUERIES`、`VDB_EFFORT` 和 buffer pool 配置。INT8 的缩放/取整和 BINARY 的 sign projection 仅用于生成合法合成测试数据，不是存储引擎行为。

真值从实际编码后的值计算，数值使用独立 f64 累加，二值使用独立 byte popcount。报告 ID Recall 与距离阈值 Recall；BINARY 不能因为等距 ID 的任意选择被判为召回错误。程序生成数据与真值会占用额外内存，因此进程峰值 RSS 不等于索引内存；同时报告纯 payload、受控内存增量和索引文件大小。

测量脚本与结果 JSONL 位于证据目录的 `perf_runner.py`、`perf/results.jsonl`。正式结论在完成配对后填写，不把入口存在当作性能验收已通过。

新增实际WAL帧复制测试 `vector_datatypes_physical_wal_replication_and_promotion`
覆盖五datatype×HNSW/IVF×memory/paged，验证DML、redo、
提升、重启和datatype保持；通过与否只写入结果文档。数值粗中心另覆盖
高于FLOAT32字段维度上限的INT8输入，以及极大/极小指数的批距离回退。

原生基准还支持 VDB_DISTRIBUTION（clustered/uniform/duplicates/varying_norm/
extreme，BINARY额外sparse）、VDB_NLIST、VDB_TRAIN_ITERS、
VDB_EF_CONSTRUCTION、VDB_NOISE、VDB_EXACT_QUERIES。原生JSON记录
p50/p95/p99、精确扫描、DML吞吐和分阶段WAL字节数。已有FLOAT32基准
保持源代码不变，沿用其p50/p99与DML/WAL输出，不能虚构原基准未记录的p95。

`vector_datatypes_maximum_payload_heap_overflow_round_trip`：五种datatype各自最大维度，
每值65532字节，经heap overflow读取及重启保持完全相同的payload。
`m7_vector_inline_and_overflow_boundaries_round_trip`：FLOAT32 2048/2049/4096/16383维；
`temporary_create_matches_persistent_schema_validation`：普通/临时表拒绝过宽固定行，并接受最大FLOAT32向量。

## 独立重现的环境准备

```bash
mkdir -p /tmp/mydb
TMPDIR=/tmp/mydb cargo nextest run --no-fail-fast
TMPDIR=/tmp/mydb cargo test --release --lib --no-run
```

忽略的性能基准需显式选择，不能把普通nextest跳过它们记成性能通过：

```bash
TMPDIR=/tmp/mydb MYDB_MEMORY_TARGET=8G \
VDB_DATATYPE=FLOAT16 VDB_ALGO=HNSW VDB_BACKEND=memory \
VDB_N=100000 VDB_DIM=768 VDB_EFFORT=1024 \
cargo test --release --lib -- --ignored --exact \
engine::executor::tests::vector_bench::vector_datatypes_end_to_end_bench \
--nocapture --test-threads=1
```

100k高维HNSW预算预设为ef1024，1536数值维度ef2048；
另在100k×1024bit Jaccard上扫描HNSW ef128/256/512/1024/2048及
IVF nprobe4/16/32/128。扫描点未达召回目标需保留为目标未达，
原生IVF全探测不能以近似算法的召回预算豁免正确性。

性能距离阈值召回使用同一独立f64参考直接比较真实距离，不加入固定绝对epsilon；
数值函数的精度断言仍按独立宽参考与累加误差界执行。原生基准的查询类型由
VDB_QUERY_DATATYPE显式选择，容量边界使用VDB_BATCH_ROWS=1，避免巨型SQL批次。

复制测试的行集超过初始重排候选预算：建立127行，插入最近点、把该点移远、
再把原本远处的id127移到最近位置，随后删除、提升、本地写入和重启。
每次查询均与强制精确扫描对照，以检查同样行数下的向量UPDATE是否遗漏候选。
