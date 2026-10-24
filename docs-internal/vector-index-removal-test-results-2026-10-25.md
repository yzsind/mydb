# 向量索引清理开发与验收记录

日期：2026-10-25。状态：**全量功能、构建与格式验收通过**。

## 最终接口与实现范围

- SQL 向量算法只保留 HNSW 和原生 IVF；退役算法名在 CREATE INDEX、ALTER TABLE ADD INDEX 和 CREATE TABLE 内联索引中明确返回不支持。
- 删除专用内存/分页后端、标量量化倒排缓存、可选导航器、独立分页文件格式及其备份、恢复、校验和、WAL 接收分派。
- 原生 IVF 不量化倒排 payload，数值和 BINARY 保留各自类型，最终仍执行 heap MVCC 重检与原值精排。
- HNSW 的 SQ8 导航保留。原 IVF 共用的训练采样、并行 k-means 和数值核保留；只属于退役后端的内核及包装器删除。
- 共用 IVF 参数、nprobe 提示、事务、分区、复制和运行统计路径保留；原 SQL 回归改为测试原生 IVF，文件与后端断言更新为原生容器语义。
- catalog 版本从 61 升至 62，防止旧目录中已退役的 catalog 算法被当作普通索引加载。不提供旧格式迁移；分页向量容器继续使用 5/2。
- 产品文档与说明图已统一为 HNSW/IVF。历史性能记录保留原始运行计数，已移除退役组合明细，不将其重新标注为原生 IVF 测量。

## 验证方式

所有测试在本机进行，临时目录和日志位于 `/tmp/mydb/remove-ivf-sq8`。
五 datatype、合法 metric、HNSW/IVF、memory/paged 共 56 个组合由现有矩阵覆盖；复制矩阵同步去除退役分支。

```bash
TMPDIR=/tmp/mydb cargo nextest run --no-fail-fast -E 'test(vector) | test(ivf) | test(hnsw) | test(backup) | test(partition) | test(ddl_audit)'
TMPDIR=/tmp/mydb cargo nextest run --no-fail-fast
cargo build
cargo run -- --help
cargo fmt --all -- --check
bash scripts/render_storage_format_versions.sh
git diff --check
```

## 执行结果

| 项目 | 状态 | 证据 |
| --- | --- | --- |
| 定向回归首轮 | 882/883通过，1个移植断言失败；1个LEAK待隔离复测 | `focused-final.log` |
| 专项收口 | 统计与退役算法拒绝2/2通过；专项LEAK用例隔离clean pass | `recheck-final.log` / `recheck.log` |
| 全量 nextest | **9980/9980通过**，227.436秒；2 slow、1 leaky、38 skipped | `full.log` |
| 全量LEAK隔离复测 | 空间函数 `st_within` 1/1 clean pass | `leak-recheck.log` |
| build / help | 通过 | `build.log` / `help.log` |
| 格式 / 版本表 / diff | 通过（另显式检查所有改动Rust源文件） | `format.log` / `versions.log` / `diff.log` |



### 首轮专项发现

`ivf_information_schema_reports_algo_and_nlist` 使用 Memory 持久化 fixture，却要求物理文件长度大于零。旧后端返回逻辑估算大小，原生容器返回真实文件大小，Memory fixture 没有 OS 文件。
首次只加入 checkpoint 的复测仍失败，确认不是延迟刷盘。用例改为真实 Strict 持久化 fixture，并先 checkpoint，再验证物理文件长度和零 SQ8 覆盖数，保留真实落盘断言。
HNSW 分页 LEAK 标记的用例已隔离 clean pass（`recheck.log`）。
三种 DDL 的退役算法拒绝测试已通过。


## 最终交付检查

- 9980项全量测试全部通过，38项默认忽略；全量1个LEAK和专项1个LEAK均隔离clean pass。
- `cargo build`、`cargo run -- --help`、Cargo格式检查、逐文件Rust格式检查、版本表一致性和`git diff --check`通过。
- 54个源码/配置文件的SHA256与全量运行时一致，见证据目录 `source-manifest.json`；最终补丁见 `final.diff`。
- 退役算法名只存在于三种SQL拒绝用例及其错误断言；产品文档不再提供该入口或专用配置说明。
- 本机测试，无远程服务器测试；未重新运行完整性能矩阵，不将历史性能数据当作本次验收。
