# VECTOR 多 datatype 开发计划与记录

日期：2026-10-24。基线：2332b811811693c92b15fd4f4d4ca3b1127ea396。

## 阶段与完成条件

1. 类型与编码：datatype、typed value、位级转换、维度预算、严格编码验证及单元测试。
2. SQL 与 heap：解析、CAST、DDL/DML、布局、overflow、函数、协议、SHOW/IS；五种类型往返正确。
3. 精确执行：类型传播、参数/缓存、批执行、LOAD DATA、spill、精确 KNN；独立参考结果一致。
4. HNSW：原生节点、全部后端与维护生命周期；原值不常驻展开 FLOAT32。
5. IVF：通用原生入口、数值 SQ8、二值聚类、内存及分页；全探测与精确真值一致。
6. 恢复与复制：版本、WAL/undo、故障、主备提升、完整回归与性能验收、英文产品文档。

## 约束

只在本机运行；所有临时数据和日志在 /tmp/mydb/vector-datatypes-2026-10-24。保持 Cargo 编译配置。代码注释和产品文档使用英文。不针对基准特化。

## 基线证据

基线源码 baseline.tar、提交 baseline-commit.txt、现有 Release 二进制 baseline-mydb 及 baseline.sha256 保存在上述临时目录。现有二进制的源码来源未被重新构建证明，不能据此作最终性能结论；正式配对测量需要从基线源码重新构建。

## 开发记录

状态：功能实现、最终全量回归及本机预设Release验收完成。尚未执行的性能/平台项目不能记为通过。

## 已完成的实现工作

- 共享原生 datatype/value，AST、catalog、表达式维度 u32、行布局与结果来源 metadata。
- 五类型文本/二进制构造、CAST、距离与切片、范数、展示、严格范围与填充验证。
- 固定 payload 与 varlen/overflow、独立编码、批执行、spill、参数重绑定与计划签名。
- 原生 HNSW 的 memory/paged、SQ8/精确导航、mmap/CSR、宽整数与按块浮点计算。
- 通用 IVF、数值 SQ8、二值 k-modes/采样 k-medoids、受控样本与按列表分页填充。
- 构建追平、提交维护、旧快照精确完成、角色切换失效、ROW/FULL envelope 与接收端。
- 格式升级、英文产品文档和完整边界测试。

## 已复现并修正的问题

1. 批执行 `from_values` 入口只复制 payload，缺失独立类型头；矩阵测试复现后统一编码。
2. UNION 相同 wire type 的 passthrough 跳过了不同向量 datatype 的转换；补齐批、流及 materialized 入口。
3. half 依赖在中点附近丢失低位；补充邻值校正和精确文本比较。
4. COSINE companion registry 原来只读 row header，延迟构图时无法拒绝已有零向量；仅 COSINE 构建投影并验证向量，其他 metric 保留 header-only 路径。
5. 旧测试期待裸 VECTOR 建索引、非严格 ALTER 截断向量；按确认方案调整为先声明维度，以及转换失败原子回滚。

6. 新增真实WAL帧复制矩阵发现通用IVF副本仍读旧resident句柄：heap提交追踪原来仅覆盖HNSW，提交失效又只覆盖支持版本的旧HNSW句柄。改为追踪所有向量表，并在提交前后撤销原生HNSW/IVF句柄与构建epoch。

7. 最大payload复核发现原FLOAT32定长行无法放入单页；对大于8192字节的FLOAT32 payload启用现有overflow路径，常用128/768/1536维保留固定布局。五类型最大payload的heap/重启专项验证。

## 验证进度

- 基线向量回归：126/126。
- 第一轮向量扩展回归：150/150，包括两个内部循环覆盖的 80 个合法 datatype/metric/algo/backend 组合。
- 第一轮全量：10000 通过、4 失败；失败已修正，24 项定向回归全部通过。
- 舍入专项、最新原生类型/磁盘/参数/COSINE 构建专项均已通过。
- 最终10013全量、Release样本与构建检查已完成，结论见独立测试结果。

### 最新全量结果

阶段full02全量 10008/10008 通过；40 skipped、1 slow、2 个非向量测试 LEAK 标记。两项单独复测均 clean pass。四个首轮失败点的 24 项定向回归全部通过。该阶段格式和diff通过；最终Release及build/help结果已另记。

## 主要接口改造清单

| 层 | 入口与职责 | 验收依据 |
| --- | --- | --- |
| 共享值 | `src/vector.rs`、`types.rs` | typed值、u32维度、稳定标签与独立编码 |
| SQL/类型 | parser AST/convert/manual/print、expr_type、planner | 严格modifier、CAST、元数据与类型合并 |
| Heap/执行 | row_codec、overflow、batch、spill_value、load_data、setop | 原生payload、overflow、流/批/物化一致 |
| 算术/函数 | vector codec/distance/sql | 低精度舍入、宽整数/浮点、二值popcount |
| HNSW | native_hnsw、native_quant、graph | 同一拓扑决策与原生memory/paged/mmap/CSR |
| IVF | native_ivf、ivf | 并行数值粗聚类、二值聚类与原生倒排评分 |
| 文件/生命周期 | vector_page/vector_mapped、database/vector_index/index_maintenance | 实际容量、CRC/WAL、发布前沿与失效 |
| 复制/恢复 | binlog、replication、format_versions | MYDBVEC2、物理帧redo、角色变化及版本拒绝 |
| 参数/显示 | auto_param、expr_program、protocol、SHOW/IS | runtime类型签名、结果域与数组显示 |

## 性能复现与修正

Release首轮测量在10k×128 FLOAT16 DOT IVF构建复现约55秒：按负内积直接
训练导致空簇多，逐空簇重新扫描所有样本和所有中心。已改为复用既有并行
L2粗训练的DOT方案，最终行分配及探测仍按负内积。二值初始化与空簇补种
缓存已有距离，避免重复扫描。最终实测和验收记录见独立测试结果文档。

### 最终源码正确性验证

阶段C全量10011/10011通过；最终10013全量及LEAK隔离重测见测试结果。
五类型最大payload（每值65532字节）heap/重启、FLOAT32 inline/overflow边界，以及临时表与普通表最大维度均已通过。
物理复制28组合含真实WAL帧、redo、完整提升步骤、提升后写入及重启。
证据见独立测试结果文档；Release预设样本已完成，未覆盖的平台/指标保留待验证标记。

## 分阶段交付与关闭标准

### 阶段1：共享类型与编码

依赖：无。产物为VectorDataType、VectorValue/VectorRef、u32维度、原生payload预算、
独立值12字节头和MYDBVEC2入口。文本、数值CAST和原始payload各自验证；
INT8保持严格数学整数规则，binary检查填充；half转换采用位级校正和精确中点比较。
关闭标准：两种16-bit格式各65536位模式分类、全部相邻有限值中点及两侧值、
整数边界、binary尾位与独立编码损坏检查通过。状态：完成。

### 阶段2：SQL、行布局与协议

依赖：阶段1。改造AST/modifier解析、catalog、CAST、DDL/DML、SHOW/DESCRIBE/IS、
所有向量函数与结果wire metadata。固定声明把datatype/dim放入行布局，payload原生存储；
大payload走既有overflow。typed和debug-layout两种布局入口共用选择函数。
关闭标准：五种声明和默认值、非法/多余modifier、函数语义、原始I/O、
五类型65532字节最大payload存取/重启，以及普通/临时表容量规则通过。状态：完成。

### 阶段3：精确执行与重写

依赖：阶段2。改造参数/自动参数化/编译缓存签名、表达式结果域、
标量/批执行、UNION/CASE、row spool、spill、LOAD DATA与COPY ALTER。
不同数值向量提升FLOAT32时检查目标维度；BINARY跨域拒绝。
关闭标准：以实际存储值和原查询作真值，混合数值查询不降精度；
参数重绑定、同文本长度不同维度、派生表、流/物化set-op和非严格ALTER原子失败通过。
状态：完成。

### 阶段4：HNSW全部后端

依赖：阶段3。引入typed距离借用视图、按块解码、宽整数、popcount及异常量级回退；
维护原生节点、分页长度、mmap/CSR准入、SQ8导航缓存与精排。
提交维护、失效句柄和快照floor沿用公共发布协议；原生更新删除重插。
关闭标准：合法datatype/metric的memory/paged回归、两种mapping设置、
DML/RR/重启/角色变化通过；源代码检查确认新类型没有逐行常驻FLOAT32副本。
状态：完成。

### 阶段5：原生IVF及SQ8扩展

依赖：阶段4公共接口。新增USING IVF，源payload倒排表，数值复用并行k-means，
binary分别使用k-modes和有界k-medoids；修正分页布局、源类型与编码元数据。
关闭标准：HNSW28、IVF28、IVF_SQ8 24个合法类型/metric/backend组合的功能矩阵通过；
原生IVF全探测与可见heap真值一致；SQ8候选截断与最终精排语义清楚。
状态：完成。

### 阶段6：恢复复制与验收

依赖：阶段1—5。升级格式，严格拒绝旧库/未知metric；
完成binlog五类型envelope和物理WAL帧、提升后写入与重启。
关闭标准：28个复制组合、完整nextest、build/help/fmt/diff均通过；
完成本机Release矩阵、预设参数扫描与FLOAT32交替复测，实际结果独立记录。
状态：恢复复制、全量与本机预设性能矩阵完成；验收结论以独立测试结果为准。

### 最后一次DML路径收敛

原生HNSW/IVF更新采用删除重插，`try_update_in_place`应先按后端分派。
已把原生返回false移到旧FLOAT32解码之前，避免为不能原地更新的向量分配完整FLOAT32临时数组，
尤其避免最大BINARY向量展开成2MiB无用缓冲。FLOAT32算法与数值距离核保持原逻辑。

复制专项已增强为127行，并加入远处id127更新为查询最近点；覆盖28个合法组合，
还验证提升后写入与重启，避免候选集覆盖小表时漏测更新遗漏。增强专项和更新后的全量均通过。
最终性能数据以final-c前缀记录，早期阶段数据仅作诊断，不能替代当前源码的DML验收。

### 重复值召回复现与修正

扩展基准在10k×128 FLOAT16、32个重复簇、ef1024下复现0.6875距离阈值召回。
原生邻接被同位置节点挤满，使部分簇不可达；提高ef不能跨越断开的分量。
增加直接索引回归：512重复行原来只返回10个候选，避免SQL精确回退掩盖问题。

改为导航代表+逻辑成员：保留每行原生记录，按代表值构图/评分，按活动成员展开。
符号零仅规范化导航key，完整payload比较防止hash碰撞误合并。
已通过原生专项12项，以及所有合法metric×memory/paged的删除代表、符号零和重启回归。
最终全量与受影响HNSW重采已完成；未改动的IVF/既有FLOAT32数据按来源单独标注。

## 最终验收状态

10013/10013全量通过，80基础组合、67必测扩展和9参数扫描完成；FLOAT32完整对照与兼容复核符合门限。实际值、来源、失败修正及未覆盖项见 [测试结果](vector-datatypes-test-results-2026-10-25.md)。
