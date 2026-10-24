# VECTOR 多 datatype 设计

日期：2026-10-24。状态：功能实现、全量回归及本机预设性能验收完成。

## 约定

支持 FLOAT32、FLOAT16、BFLOAT16、INT8、BINARY；语法 VECTOR(n, datatype)，省略 datatype 默认 FLOAT32，裸 VECTOR 保留不定维 FLOAT32。索引要求定维、非空、单列、非唯一。

| 类型 | payload | 最大维度 | metric |
| --- | --- | --- | --- |
| FLOAT32 | 4n | 16383 | L2/COSINE/DOT |
| FLOAT16 | 2n | 32766 | L2/COSINE/DOT |
| BFLOAT16 | 2n | 32766 | L2/COSINE/DOT |
| INT8 | n | 65532 | L2/COSINE/DOT |
| BINARY | ceil(n/8) | 524256 bit | HAMMING/JACCARD |

payload 上限 65532 字节；分页记录单独准入，不承诺全部类型上限可建立分页索引。

浮点采用最近偶数舍入，拒绝 NaN/Inf/转换溢出，允许下溢，支持子正规数。INT8 只接受 [-128,127] 的整数，不缩放、不饱和、不截断。BINARY 文本为 [0,1,...]，第 i 维放入第 i/8 字节低位 i%8，尾部填充必须为零。

混合数值距离保留查询精度，不先把查询转换成字段精度。BINARY 不与数值隐式转换。L2 标量返回平方根，DOT 距离为负内积，COSINE 零向量标量为 NULL，COSINE 索引拒绝零向量。默认数值 L2、二值 HAMMING。JACCARD=1-popcount(a&b)/popcount(a|b)，两个空集为零。

## SQL 接口

TO_VECTOR/STRING_TO_VECTOR(text[,datatype])；CAST(value AS VECTOR(n,datatype))；VECTOR_DATATYPE；VECTOR_FROM_BINARY(payload,datatype,dimension)；VECTOR_TO_BINARY；HAMMING_DISTANCE；JACCARD_DISTANCE；DISTANCE(a,b[,metric])。

VECTOR_DIM 的 BINARY 维度单位是 bit。SUBVECTOR 保留 datatype，BINARY 按 bit 切片。L2_NORMALIZE 输出 FLOAT32并检查其维度上限；VECTOR_NORM 和 L2_NORMALIZE 拒绝 BINARY。原始 payload 必须显式构造，不猜测 datatype。

## 类型和编码

共享 VectorDataType、VectorValue { datatype,dimension,payload }。维度使用 u32。定维 heap lane 只保存 payload，行布局保存 datatype；独立值带版本、datatype、维度和长度。spill、复制、缓存不能丢失类型。比较按表示，类型进入哈希和缓存签名。数值 UNION/CASE 提升 FLOAT32，BINARY 与数值合并报错。ALTER 类型/维度走原子 COPY，失败禁止补零、截断或忽略。

## 索引

HNSW 原值保持原生 payload，浮点保留 SQ8 导航与越界精确回退；INT8/BINARY 使用原生距离。BINARY 更新删除重插。所有后端最终使用可见 heap 原值精排。

USING IVF 为原生 payload 倒排表；IVF_SQ8 是四种数值类型的量化入口，拒绝 BINARY。数值粗量化沿用 k-means/归一化训练，中心允许 FLOAT32。Hamming 按位多数更新中心（平票零）。Jaccard 使用有界采样 k-medoids，每簇最多 32 候选、256 评估样本，候选包含旧代表。初始化按相应距离加权采样，空簇取最远样本，实际 nlist 不超过训练样本不同向量数。原生 IVF 全探测必须得到精确 TopK。

元数据分别记录源类型、索引编码、metric、维度。HNSW 按最大层邻接计算准入；IVF 检查单 entry。首版不增加页外索引记录。保留 MVCC、发布前沿、构建追平、失效与重建机制。

## 恢复与复制

升级 catalog/heap/独立值/索引版本，旧库明确拒绝。派生索引缺失或损坏从 heap 重建，未知 metric 编号报错。ROW/FULL binlog 以 MYDBVEC2+带类型值编码的 BLOB envelope 传输；物理复制与提升覆盖五种类型。

## 参考

half 2.7.1：https://docs.rs/half/2.7.1/half/ 。源代码与测试为真源；实际实现差异在开发记录中逐项列出。

## 落地接口与物理布局

`VectorDataType` 的稳定标签为 FLOAT32=0、FLOAT16=1、BFLOAT16=2、INT8=3、BINARY=4。`Value::Vector` 持有 `VectorValue { datatype, dimension: u32, payload }`。标量值、参数、批执行、spill、undo、复制都保留类型；不能通过 payload 长度猜测 datatype。

独立值编码采用以下结构，所有整数使用 little endian：

| 偏移 | 字段 | 规则 |
| --- | --- | --- |
| 0 | version u8 | 1 |
| 1 | datatype u8 | 0..4 |
| 2..4 | reserved u16 | 必须为零 |
| 4..8 | dimension u32 | datatype 对应的合法范围 |
| 8..12 | payload length u32 | 必须与 datatype、dimension 精确匹配 |
| 12.. | payload | 不接受额外尾随字节；浮点有限，binary 填充为零 |

binlog 额外包裹 8 字节 `MYDBVEC2`，外层仍是 MySQL LONG BLOB 的标准长度前缀。接收端根据目标字段和 envelope 验证、转换，不把非 UTF-8 原始值转换成文本。

定维列的 datatype 和 dimension 在行布局中，heap 只保存原生 payload。大于4096字节的非FLOAT32 payload及大于8192字节的FLOAT32 payload使用既有varlen/overflow路径；其声明维度仍然固定。裸 VECTOR 的布局保存 FLOAT32 datatype，值前额外保存实际维度 u32。spill 和独立值使用上面的完整编码，与 heap 布局分开。

本轮格式：catalog=61、MVCC row=8、undo binary=14、paged HNSW file/page=5/2。既有 FLOAT32 IVF_SQ8 文件布局未变化，继续为 2/2；旧库仍由 catalog 版本拒绝。格式表由现有脚本生成。

### 原生分页容器

FLOAT32 的既有 HNSW/IVF_SQ8 实现保留，作为性能与行为基线。新增类型及通用 IVF 使用原生容器，复用 VectorIndexFile 的共享缓存、WAL、校验和、页内 slot、邻接编辑和 mmap 撤销协议。

原生几何信息编码在 metadata 的 geometry u32：bit31 为 native 标记，bit30..28 为 datatype，低28位为逻辑维度。原生记录不分配 FLOAT32 原值或重复 SQ8 lane。HNSW 记录为通用节点头、原生 payload、各层邻接。IVF 使用同一容器的无邻接记录，按倒排列表分别维护当前填充页，使同一列表条目聚集在页内。

memory 原生节点保留紧凑 payload；paged 原生节点保留 row-id/vid 等受控元数据，评分在页 pin 或只读 mmap 的借用作用域内完成。原生类型的 mmap 使用相同的 writer 撤销及 lazy CRC 验证。mmap 不可用时可准入 level-0 CSR；准入失败保留分页查询。INT8/BINARY 的 HNSW 不训练 SQ8，浮点类型可缓存 SQ8 导航，最终按原生值排序。查询超出校准范围时直接使用精确导航。

### 原生 IVF 构建

构建持有已登记读视图，并在声明发布前捕获提交增量：

1. 第一遍计算可见向量数，确定请求 nlist 与样本预算。
2. 第二遍按确定性行序号收集受控训练样本；训练数值中心或二值中心/medoid。
3. 第三遍按最近中心分配，写入原生列表；分页版本按列表填页。
4. 追平同一 key 的提交增量，验证 generation、role/build epoch，最后发布句柄与覆盖前沿。

数值中心使用 FLOAT32，均值/范数使用宽中间算术；它们是内部 coarse metadata，不受 SQL 单个 FLOAT32 值的 payload 上限限制。DOT 复用既有并行数值 k-means 的 L2 粗训练；训练完成后的行分配、探测和候选评分使用负内积。COSINE 对训练方向归一化。Jaccard 始终使用真实位串和 Jaccard 距离，不替换为 Hamming。

原生 IVF_SQ8 的源 payload 保持原生类型，SQ8 仅作为受控候选评分缓存；DOT 直接按原值评分。结果的 `exact_order` 明确区分 SQ8 排序和精确排序。通用 IVF 全探测的精确 TopK 契约不适用于被 SQ8 提前截断的候选集合。

## 舍入正确性

本机复现了 half 2.7.1 的精度边界差异：FLOAT16 的可移植 f64 路径和 BFLOAT16 路径会截掉部分低位；某些 x86 FLOAT16 路径还先转换成 FLOAT32。本机 probe 输出与预期分别记录在 `/tmp/mydb/vector-datatypes-2026-10-24/half-rounding-probe.log`。

处理策略：

- 精确可表示为 FLOAT32 的输入采用 half 的 FLOAT32 硬件路径，转换没有中间精度损失。
- 其他 f64 输入用相邻目标值的精确 f64 中点校正候选，包含上下溢边界和正负零。
- 文本若恰好被 f64 解析到目标中点，则直接比较原始十进制与该中点的精确十进制。只构造有界大小的中点参考，不为输入构造任意长度大整数。
- 测试穷举全部位模式，并覆盖所有正数相邻值中点、两侧 f64 邻值及负号镜像；长十进制单独覆盖解析到同一 f64 中点的两侧值。

## 并发、失败与兼容边界

原生图沿用构建 owner、generation/role epoch、提交增量、覆盖前沿和快照 floor。原生 UPDATE 删除重插；历史读视图走 heap/undo 精确完成，不能把当前图当历史索引。物理 redo 与角色切换撤销原生句柄；副本从自己的已提交 heap 构造 resident 加速器。维护达到阈值后撤销并重建原生加速器，不能保证复用旧图时的查询延迟。

原生持久索引当前在冷启动重新构建、重训；分页文件用于受控存储和运行中的持久维护，不承诺免扫描冷启动。mmap、CSR、训练和 SQ8 缓存均受统一预算控制；容量不满足的 DDL 提前拒绝，可重建性 I/O/内存失败允许精确查询回退。

定维原生列需要已知维度。CTAS 从无法推断维度的文本表达式生成原生列时，应显式 CAST 为 VECTOR(n,datatype)。数值/BINARY 跨域转换拒绝；普通赋值、CAST 和 ALTER 不自动量化。ALTER 失败保持原字段和原值，即便 sql_mode 非严格。

## SQL 完整示例

```sql
CREATE TABLE embeddings (
    id BIGINT PRIMARY KEY,
    v32 VECTOR(768) NOT NULL,
    v16 VECTOR(768,FLOAT16) NOT NULL,
    vbf VECTOR(768,BFLOAT16) NOT NULL,
    vi8 VECTOR(768,INT8) NOT NULL,
    bits VECTOR(769,BINARY) NOT NULL
);
CREATE INDEX v16_hnsw ON embeddings(v16) USING HNSW COMMENT 'METRIC=COSINE';
CREATE INDEX vi8_ivf ON embeddings(vi8) USING IVF COMMENT 'METRIC=DOT NLIST=128';
CREATE INDEX bits_hnsw ON embeddings(bits) USING HNSW COMMENT 'METRIC=HAMMING';
CREATE INDEX bits_ivf ON embeddings(bits) USING IVF COMMENT 'METRIC=JACCARD NLIST=128';
SELECT VECTOR_TO_STRING(VECTOR_FROM_BINARY(X'0501','BINARY',9));
SELECT VECTOR_DATATYPE(CAST('[1,2]' AS VECTOR(2,FLOAT16)));
```

### 容量公式

令 P 为原生 payload 字节数，L 为节点最高层号，M0=2M；节点记录为
`R=10+P+Σ(l=0..L)[2+4*cap(l)]`，其中 cap(0)=M0，其余为 M。
16 KiB 页面有 28 字节公共/节点头，每条目录项 2 字节，因此准入条件为
`R+2<=16384-28`。按最高支持层 L=16 检查，得到原生 HNSW
`P<=16310-72M`。例如 M=16 时 P<=15158，FLOAT16 最多7579维。
原生 IVF 无邻接，R=12+P，故 P<=16342。BINARY 按 ceil(n/8) 代入。
这些公式不替代 SQL datatype 的65532字节预算。既有 FLOAT32 HNSW 的
lane 还含 SQ8/norm，按实际 lane 长度5n+4执行相同容量检查。

### 二值迭代预算

二值中心初始化维护每个训练样本到最近已有中心的距离缓存；每次增加中心
只计算到新中心的距离，保留确定性的相应 metric 加权采样。每轮迭代包含
一次全样本分配和一次中心/medoid更新。分配时保存最近距离，空簇按这个
分配前沿的最远未选样本补种，同轮不重复选同一样本。Hamming平票取零；
Jaccard每簇候选32、评估样本256，旧代表额外参与候选比较。nlist限制和
样本/迭代预算对两种二值metric一致。

### 当前构建并行度与实测代价

原生HNSW在状态写锁下按顺序插入；FLOAT32继续使用既有并行构建路径。
原生payload减少持久字节，并不保证构建更快。最终本机100k×1536 FLOAT16分页HNSW
在M16/EF_CONSTRUCTION200、查询ef2048下，构建约241秒、严格阈值召回0.99921875。
完整数据以独立测试结果为准；不能把FLOAT32的并行构建吞吐套用于新增类型。

### 重复值的导航代表与逻辑成员

每个原生HNSW图为相同导航坐标建立一个代表节点，逻辑成员仍各自保留原生payload、
row-id和tombstone；paged成员仍有独立记录。运行时成员表通过带随机种子的hash查找，
命中后逐字节核验，hash碰撞不会合并不同值。浮点符号零仅在导航key中规范化，
heap和索引原值的符号位不改变，SQL向量按表示比较的规则也不改变。

构图只把代表节点加入拓扑，重复成员不再挤掉跨簇邻接。查询按代表值评分，
按距离展开活动成员，再由公共执行器做heap可见性检查和精排。
导航ef计不同代表值，返回/精排预算仍计逻辑行；目录和成员输出都纳入受控内存。
SQ8导航码只缓存代表值。代表行删除后其导航payload保留用于路由，
成员tombstone决定哪些row-id可以返回；维护重建回收失效成员。

该运行时目录从已登记heap视图重建，不增加页外记录或逐行FLOAT32副本。
本修正只涉及原生HNSW模块，既有FLOAT32 HNSW与IVF实现沿用原代码。

## 最终验收状态

10013/10013全量通过，80基础组合、67必测扩展和9参数扫描完成；FLOAT32完整对照与兼容复核符合门限。实际值、来源、失败修正及未覆盖项见 [测试结果](vector-datatypes-test-results-2026-10-25.md)。
