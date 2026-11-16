#!/usr/bin/env python3
"""Build the reviewable full-result appendix and immutable evidence bundle."""

from __future__ import annotations

import argparse
import collections
import gzip
import json
from pathlib import Path
import shutil
import statistics

from function_expression_bench import (
    digest_file,
    dump,
    parallel_evidence,
    report,
    report_result,
    sample_summary,
)


def latency(summary):
    value = summary.get("median_ms")
    status = summary.get("status", "missing")
    return (
        f"{value:.3f}"
        if value is not None and status == "ok"
        else f"{value:.3f} ({status})" if value is not None else status
    )


def render(directory, destination=None):
    args = argparse.Namespace(directory=directory)
    report(args)
    manifest = json.loads((directory / "manifest.json").read_text())
    context = json.loads((directory / "context.json").read_text())
    provenance = (
        json.loads((directory / "provenance.json").read_text())
        if (directory / "provenance.json").exists()
        else {}
    )
    results = {
        p.parent.name: report_result(p)
        for p in sorted((directory / "cases").glob("*/result.json"))
    }
    for identifier, result in results.items():
        result["source_result_sha256"] = digest_file(
            directory / "cases" / identifier / "result.json"
        )
        for name, engine in result["engines"].items():
            if engine.get("plan"):
                engine["parallel_evidence"] = parallel_evidence(engine["plan"], name)
    expected = {case["id"] for case in manifest["cases"]}
    missing = sorted(expected - set(results))
    unexpected = sorted(set(results) - expected)
    complete = (
        not missing
        and not unexpected
        and set(context["modes"]) == {"mysql", "serial", "parallel2"}
    )
    summaries = {}
    for identifier, result in results.items():
        for name, engine in result["engines"].items():
            for metric in ("end_to_end", "analyze", "statement"):
                summaries[identifier, name, metric] = sample_summary(
                    engine.get("metrics", {}).get(
                        "analyze" if metric == "statement" else metric, {}
                    ),
                    metric == "statement",
                    context["repeats"],
                )
    all_lines = [
        "# 全部用例结果",
        "",
        "时间单位为毫秒。括号表示测量未全部成功；timeout 不是成功耗时，期限见对应列。倍数仅限逐值/声明语义校验通过且三次测量均成功的用例。",
        "",
        "## 端到端延迟",
        "",
        "| 用例 | 类别 | 期限s | 串行校验 | 并行校验 | MySQL | mydb 串行 | mydb 度数2 | MySQL/串行 | MySQL/度数2 | 实际 worker |",
        "| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    parallel_rows = []
    fastest = []
    for case in manifest["cases"]:
        identifier = case["id"]
        result = results.get(identifier, {})
        checks = {
            name: result.get("correctness", {}).get(name, {}).get("status", "missing")
            for name in ("serial", "parallel2")
        }
        values = {
            name: summaries.get(
                (identifier, name, "end_to_end"),
                {
                    "status": result.get("engines", {})
                    .get(name, {})
                    .get("status", "missing")
                },
            )
            for name in ("mysql", "serial", "parallel2")
        }
        ratios = {}
        for name in ("serial", "parallel2"):
            if (
                checks[name] == "match"
                and values[name].get("status") == "ok"
                and values["mysql"].get("status") == "ok"
                and result.get("correctness", {})
                .get("mysql", {})
                .get("status", "match")
                == "match"
            ):
                ratios[name] = values["mysql"]["median_ms"] / values[name]["median_ms"]
                fastest.append(
                    (
                        ratios[name],
                        identifier,
                        name,
                        values["mysql"]["median_ms"],
                        values[name]["median_ms"],
                        result.get("timeout_s", context["timeout_s"]),
                    )
                )
        evidence = (
            result.get("engines", {}).get("parallel2", {}).get("parallel_evidence", {})
        )
        workers = evidence.get("observed_workers", [])
        all_lines.append(
            "| "
            + " | ".join(
                [
                    identifier,
                    case["category"],
                    str(result.get("timeout_s", context["timeout_s"])),
                    checks["serial"],
                    checks["parallel2"],
                    *[
                        latency(values[name])
                        for name in ("mysql", "serial", "parallel2")
                    ],
                    *[
                        f"{ratios[name]:.3f}" if name in ratios else "—"
                        for name in ("serial", "parallel2")
                    ],
                    str(workers) if workers else "未观测/串行",
                ]
            )
            + " |"
        )
        if (
            result.get("correctness", {}).get("serial_vs_parallel2", {}).get("status")
            == "match"
            and values["serial"].get("status") == "ok"
            and values["parallel2"].get("status") == "ok"
        ):
            parallel_rows.append(
                dict(
                    case_id=identifier,
                    category=case["category"],
                    timeout_s=result.get("timeout_s", context["timeout_s"]),
                    observed_workers=workers,
                    serial_over_parallel2=values["serial"]["median_ms"]
                    / values["parallel2"]["median_ms"],
                )
            )
    for metric, title in [
        ("statement", "ANALYZE 的服务端整句计时"),
        ("analyze", "ANALYZE 的客户端等待时间"),
    ]:
        all_lines.extend(
            [
                "",
                f"## {title}",
                "",
                "| 用例 | MySQL | mydb 串行 | mydb 度数2 |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for case in manifest["cases"]:
            all_lines.append(
                "| "
                + case["id"]
                + " | "
                + " | ".join(
                    latency(
                        summaries.get(
                            (case["id"], name, metric), {"status": "unavailable"}
                        )
                    )
                    for name in ("mysql", "serial", "parallel2")
                )
                + " |"
            )
    all_lines.extend(["", "## 用例说明", "", "| 用例 | 说明 |", "| --- | --- |"])
    for case in manifest["cases"]:
        if case.get("workload_note"):
            all_lines.append(f"| {case['id']} | {case['workload_note']} |")
    (directory / "all-cases.md").write_text("\n".join(all_lines) + "\n")
    dump(directory / "parallel-summary.json", parallel_rows)
    original = (directory / "report.md").read_text()
    host_path = directory / "host-samples.jsonl"
    host = (
        [json.loads(line) for line in host_path.read_text().splitlines()]
        if host_path.exists()
        else []
    )
    host_lines = []
    if host:
        first, last = host[0], host[-1]
        host_lines = [
            "",
            "## 本机内存与换页条件",
            "",
            f"监测区间 {first['timestamp']} 至 {last['timestamp']}，覆盖本轮运行的部分时段。最后一次系统状态：`{last['swap']}`。",
            "",
            "| 全机累计计数的区间增量 | MiB |",
            "| --- | ---: |",
        ]
        for key in ("Swapins", "Swapouts", "Pageins", "Pageouts"):
            delta = last["vm"].get(key, 0) - first["vm"].get(key, 0)
            host_lines.append(f"| {key} | {delta*last['page_size']/1024**2:.2f} |")
        host_lines.extend(
            [
                "",
                "这些计数包含其他应用、校验文件读写和数据库，不能归因于某个引擎。较高的交换空间使用及换页使本轮结果只能作为本机负载下的观测，不能作为隔离硬件或纯函数性能结论。",
            ]
        )
    measured = {
        name: collections.Counter() for name in ("mysql", "serial", "parallel2")
    }
    observations = collections.Counter()
    for result in results.values():
        for name in measured:
            samples = (
                result.get("engines", {})
                .get(name, {})
                .get("metrics", {})
                .get("end_to_end", {})
                .get("samples", [])
            )
            measured[name].update(
                sample["status"] for sample in samples if not sample.get("warmup")
            )
        evidence = (
            result.get("engines", {}).get("parallel2", {}).get("parallel_evidence", {})
        )
        observations[
            "workers2" if 2 in evidence.get("observed_workers", []) else "no_workers2"
        ] += 1
        if evidence.get("parallel_expression_operator_observed"):
            observations["parallel_expression_operator"] += 1
    extra = [
        "",
        "## 构建与数据身份",
        "",
        f"- Source commit：`{provenance.get('source_commit','unknown')}`；包含当时工作区改动，源码快照 SHA-256：`{provenance.get('source_tree_sha256','unknown')}`。",
        f"- mydb binary SHA-256：`{context.get('mydb_binary_sha256')}`；协议 worker SHA-256：`{context.get('worker_sha256')}`。",
        f"- 平台：`{context.get('machine')}`；两种 mydb 模式为同一个 release 二进制。",
        "- 数据由统一 ID 规则生成；DECIMAL 和 DOUBLE 使用精确八分之一倍数。初始 10 秒预检的百万行全列（含标准 WKB）均匹配，证据见 initial-data-validation.json；1 秒阶段的宽行基线超时，未声称该阶段完成全列校验。重启后的指纹证据见 restart-data-verification.json。",
        "- 本机其他应用和已有数据库实例保持运行；未设置 CPU 亲和性或操作系统硬内存上限。重复测量不能消除所有调度、温度和缓存影响。",
        "",
        "## 正式样本状态",
        "",
        "| 配置 | 成功 | 超时 | SQL错误 |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name, counts in measured.items():
        extra.append(
            f"| {name} | {counts['ok']} | {counts['timeout']} | {counts['error']} |"
        )
    extra.extend(
        [
            "",
            "## 并行效果",
            "",
            f"{observations['workers2']} 个用例的 ANALYZE 计划观察到 workers=2；{observations['no_workers2']} 个未观察到该证据（包括串行回退、失败和指标不可用）。其中 {observations['parallel_expression_operator']} 个用例的同一个算子同时提供 worker 和表达式模式指标。扫描的 vector 标记不作为表达式执行或并行的证据；其余项目仅能确认所显示的扫描/交换并行。",
            "",
            "| 类别 | 可比项 | 串行/度数2中位数 |",
            "| --- | ---: | ---: |",
        ]
    )
    diagnostic_notes = host_lines + [
        "",
        "## 需要区分的差异",
        "",
        "- `COMPRESS` 的压缩字节流可因压缩实现而不同；百万行解压回环另行核对。原始字节不匹配仍保留，不将其自动视为数据损坏或结果一致的加速项。",
        "- `FORMAT_BYTES` / `FORMAT_PICO_TIME` 使用 MySQL 原生实现；零值前导空格差异不做 trim。旧的 sys/扩展分类结果已撤销，修订过程保存在 context 的 extensions 中。",
        "- `TIMEDIFF` 混合 DATE/DATETIME 与两个 DATETIME 的用例分别保留；nullable / nonnull POLYGON 也分别保留。",
        "- `SETVAL` 的取消确认超过清理窗口，已确认旧连接退出后重建连接。此类结果记录为 timeout，停止该配置的后续重复，不填造三个成功样本。",
        "- 系统辅助接口中的 sys 映射、环境依赖或兼容响应不作为同等功能的性能胜负；不能用接口耗时替代功能完整性结论。",
        "- prepared-*.json 的历史 load_sql 日志字段保存了最后一个 ANALYZE；实际生成规则见 data-generation.sql，数据完整性由百万行全列比较和指纹验证。",
    ]
    groups = collections.defaultdict(list)
    for row in parallel_rows:
        groups[row["category"] + f" / {row['timeout_s']}s"].append(
            row["serial_over_parallel2"]
        )
    for category, ratios in sorted(groups.items()):
        extra.append(
            f"| {category} | {len(ratios)} | {statistics.median(ratios):.3f} |"
        )
    extra.extend(diagnostic_notes)
    for title, rows in [
        ("观测加速最大的项目", sorted(fastest, reverse=True)[:12]),
        ("观测相对较慢的项目", sorted(fastest)[:12]),
    ]:
        extra.extend(
            [
                "",
                f"## {title}",
                "",
                "按端到端中位数排序，未检验统计显著性；常量折叠、嵌套包装和各类函数的工作量说明见完整用例表。",
                "",
                "| 用例 | mydb 模式 | 期限s | MySQL ms | mydb ms | MySQL/mydb |",
                "| --- | --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for ratio, identifier, name, mysql, mydb, budget in rows:
            extra.append(
                f"| {identifier} | {name} | {budget} | {mysql:.3f} | {mydb:.3f} | {ratio:.3f} |"
            )
    extra.extend(
        [
            "",
            "## 覆盖缺口与不可比条目",
            "",
            "| 来源 | 条目 | 原因 |",
            "| --- | --- | --- |",
        ]
    )
    for item in manifest["items"]:
        if not item["cases"]:
            extra.append(
                f"| {item['source']} | {item['name'].replace('|','&#124;')} | {item.get('reason',item['disposition'])} |"
            )
    extra.extend(
        [
            "",
            "## 证据文件",
            "",
            "- `all-cases.md`：全部用例的三种口径及实际 worker；`summary.csv`：便于筛选的逐项汇总。",
            "- `manifest.json`：每个函数/表达式的覆盖映射、SQL、初始化、比较规则和用例说明。",
            "- `raw-results.jsonl.gz`：全部逐项原始结果，包含每次计时、SQL、计划、错误和校验差异。",
            "- `context.json`、`provenance.json`、`prepared-*.json`：计时配置、构建和数据身份；`evidence-sha256.json`：交付文件哈希。",
            f"- 完整性：`{complete}`；缺少 `{missing}`；额外 `{unexpected}`。",
            "",
        ]
    )
    (directory / "report.md").write_text(original + "\n".join(extra))
    with gzip.open(
        directory / "raw-results.jsonl.gz", "wt", encoding="utf-8"
    ) as output:
        for identifier in sorted(results):
            output.write(
                json.dumps(
                    results[identifier], ensure_ascii=False, separators=(",", ":")
                )
                + "\n"
            )
    names = [
        "manifest.json",
        "context.json",
        "cohorts.json",
        "audit.json",
        "provenance.json",
        "mysql-capabilities.json",
        "host-samples.jsonl",
        "restart-data-verification.json",
        "initial-data-validation.json",
        "data-generation.sql",
        "summary.csv",
        "summary.json",
        "parallel-summary.json",
        "all-cases.md",
        "report.md",
        "raw-results.jsonl.gz",
    ]
    names += [p.name for p in directory.glob("prepared-*.json")]
    for extension in context.get("extensions", []):
        names.extend(
            [
                f'manifest-{extension["previous_sha256"]}.json',
                f'context-{extension["previous_sha256"]}.json',
            ]
        )
    dump(
        directory / "evidence-sha256.json",
        {
            name: digest_file(directory / name)
            for name in names
            if (directory / name).exists()
        },
    )
    if destination:
        destination.mkdir(parents=True, exist_ok=True)
        for name in names + ["evidence-sha256.json"]:
            if (directory / name).exists():
                shutil.copy2(directory / name, destination / name)
    return complete


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--destination", type=Path)
    args = parser.parse_args()
    complete = render(args.directory.resolve(), args.destination)
    print("full coverage complete:", complete)


if __name__ == "__main__":
    main()
