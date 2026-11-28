# 四份审计的复核、修复与验证（2026-11-27）

基线：`75fb742`，分支 `codex/bugfix-20261127`。仅本机验证。临时数据、日志和二进制位于 `/tmp/mydb/audit-fixes/`。MySQL 8.0.46 独立参考实例端口 14981，基线 mydb 端口 14980。原审计报告保留，以下结论以当前源码与重新执行的结果为准。

## 判定与计划

| 原编号 | 首轮复核结果 | 修复与测试计划 |
| --- | --- | --- |
| JSON BUG-1/2/3 | 误报：参考 MySQL 与 mydb 的标量路径修改结果一致 | 保留语义；用标量、数组、last/out-of-range 矩阵回归锁定 |
| JSON BUG-4 | 当前基线 COALESCE/IFNULL/IS NULL 均与 MySQL 一致 | 保留 JSON null 与 SQL NULL 的区别并回归 |
| JSON BUG-5 | 错误码差异真实，但报告建议 3903 不正确；MySQL 实测 1235 | 对齐不支持 object→array 的明确错误，验证 INSERT/UPDATE 原子性 |
| JSON BUG-6 | 已复现：CHAR ARRAY prepared probe 返回空集 | 统一写入与参数探测的目标编码；覆盖 CHAR/DATE/DATETIME/TIME/DECIMAL、二进制 prepared、重启 |
| JSON BUG-7 | 已复现：复合索引 vc=1 漏掉 '01' | 使用通用比较域守卫；覆盖常量/参数、字符串/数字、索引与全扫对照 |
| JSON BUG-8/9/10/11/12/13 | 误报：重新执行参考 MySQL 结果与 mydb 一致 | 不按报告建议改变正确行为；覆盖行式/向量路径 |
| JSON DEV-8 | 已复现：JSON_TABLE VARCHAR DEFAULT 'null' 错变 SQL NULL | 按目标类型解释默认值；测试 null/字符串/数值及 EMPTY/ERROR |
| T-1 | 已复现：生成列 NEW 赋值阻断 INSERT | BEFORE 忽略生成列赋值、保持 AFTER 写保护；覆盖 INSERT/UPDATE、后续语句及直接 DML 错误 |
| T-7 | UPDATE IGNORE 批量路径已正确，ODKU 仍报错 | 修复 ODKU IGNORE 跳行及 warning；覆盖触发器与批量路径、行计数与副作用 |
| T-9 | 已复现：无效限定符静默绑定当前行 | 通用校验 DML 作用域，保留合法表名/别名及 trigger OLD/NEW；覆盖空表和 prepared |
| T-12 | 已复现：join view CHECK OPTION 创建被拒 | 复核产品文档后确认是既有明确不支持的功能边界；维持 1235，当前 bugfix 不新增完整连接视图后像检查 |
| T-13 | 已复现：跨库 trigger key 碰撞，DROP 删除错误对象 | trigger/event/routine 使用无歧义组件编码；覆盖隔离、DROP、重启与 DD 重建 |
| ACID BUG-01 | 待复现；当前代码已有 allocator owner reclaim 与补偿准入机制 | 在受限独立实例重跑大 join、错误后恢复、KILL、新连接与事务回滚，不以 RSS 单独否定 footprint |
| 复制 P1-01 | 源码候选仍在 | 大事件超源 max_allowed_packet 复现；显式流上限与确定性错误分类；实际 >64MB 回放 |
| 复制 P2-02 | 源码候选仍在 | seeding 槽不参加 ACK quorum；确定性状态机测试与原生初始化验证 |
| 复制 P2-03 | 源码候选仍在 | 瞬态备份拒绝可重试，身份/认证失败仍终止；协议 mock 与实际冷启动 |
| 复制 P2-04 | 源码候选仍在，需核实 MySQL HOST 修改的坐标语义 | 验证未给坐标的增量配置、显式坐标与重启 |
| 复制 P2-05 | 源码候选仍在 | 以文件序号比较 UNTIL；启动前已达到目标时立即停止，覆盖跨文件与事务边界 |
| 复制 P2-06 | 源码候选仍在 | 统一 CHAR/BINARY 行像规范；真实 ROW/FULL UPDATE/DELETE、重复行、HASH_SCAN/全扫对照 |

## 执行顺序

1. 建立基线 SQL 差异与复制/内存复现，逐项确定需要修复的范围。
2. 每组先添加能失败的回归，再完成实现；保持未受影响的设计取舍。
3. 执行定向 nextest、完整 nextest、cargo build、CLI help、格式检查。
4. 独立进程完成 SQL、prepared、复制、恢复与性能对照；记录实测值及剩余边界。

P3 与已排除项也逐项登记最终处置：优于 MySQL 或符合存储模型的行为保持；明确的正确性问题修复；仅显示格式差异不引入无必要的 catalog 状态。本文将在执行过程中补充最终证据。

## 最终处置

逐项判定、真实根因、实际验证结果及保留边界见 [verification.md](verification.md)。原报告多项 MySQL 判断已由本次实际对照纠正；MRR 内存问题按缓存字节治理修复，没有修改采样或豁免硬限。
