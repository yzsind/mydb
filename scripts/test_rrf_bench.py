"""Offline tests for the benchmark's independent oracle and workload contract."""
import datetime
import copy
import io
from decimal import Decimal
from types import SimpleNamespace
import queue
import unittest
from unittest.mock import Mock, patch

import rrf_bench as bench


class RrfBenchTests(unittest.TestCase):
    def test_duplicates_keep_original_rank_and_zero_weight_is_absent(self):
        rows = bench.oracle([[(1,), (1,), (2,)], [(2,), (3,)], [(999,)]], [1, 2, 0], 0)
        self.assertEqual(rows, [(2, 2 + 1 / 3, 1, 2), (1, 1.0, 1, 1), (3, 1.0, 2, 1)])

    def test_ties_offset_limit_and_typed_keys(self):
        for key in [Decimal("1.25"), b"a", "中文", datetime.datetime(2026, 1, 1), .5]:
            rows = bench.oracle([[(key,)], [(key,)]], [1, 1], 60)
            bench.validate_rows(rows, [(key, 2 / 61, 1, 2)], 1)
        self.assertEqual(bench.oracle([[(2,), (1,)], [(1,), (2,)]], [1, 1], 60, 1, 1)[0][0], 2)
        self.assertEqual(bench.oracle([[(1,)], []], [1, 1], 60, 0), [])

    def test_incorrect_score_identity_count_or_order_fails(self):
        expected = [(1, .1, 1, 1), (2, .05, 2, 1)]
        for actual in [expected[::-1], expected[:1], [(1, .2, 1, 1), expected[1]],
                       [(1, .1, 1, 2), expected[1]]]:
            with self.assertRaises(AssertionError):
                bench.validate_rows(actual, expected, 1)

    def test_workload_covers_types_branches_details_and_hybrid(self):
        args = SimpleNamespace(scenarios=["relational", "edge", "hybrid"], branches=[2, 4, 8],
                               candidates=[20, 200], key_types=["int", "composite", "text", "decimal",
                               "binary", "datetime", "float", "expression"], details=[False, True],
                               topk=20, rank_constant=60, selectivities=[1, .01], dim=16,
                               index="ivf", metric="cosine")
        cases = bench.CaseBuilder("docs", 1000, args).cases()
        self.assertEqual(len(cases), 111)
        self.assertEqual(len({c["name"] for c in cases}), len(cases))
        for case in cases:
            if "branches" in case:
                self.assertEqual(len(case["branches"]), len(case["weights"]))
                self.assertIn("ORDER BY rrf_score DESC,best_rank", case["sql"])
        dense = next(c for c in cases if "/dense/" in c["name"])
        self.assertIn("IVF_NPROBE(8)", dense["sql"])
        self.assertIn("COSINE_DISTANCE", dense["sql"])

    def test_empty_samples_fail_and_nearest_rank_percentiles(self):
        with self.assertRaises(RuntimeError):
            bench.metrics([], 1)
        result = bench.metrics(list(range(1, 101)), 2)
        self.assertEqual((result["p95_ms"], result["p99_ms"], result["qps"]), (95, 99, 50))

    def test_worker_surfaces_sql_errors_without_retry(self):
        ready, results = queue.Queue(), queue.Queue()
        connection = Mock()
        with patch.object(bench, "connect", return_value=connection), patch.object(
                bench, "execute", side_effect=[[(1,)], RuntimeError("SQL failed")]) as query:
            bench.worker({}, "db", {"sql": "SELECT 1"}, 1, 3, 1,
                         ready, Mock(wait=Mock(return_value=True)), results)
        self.assertIsNone(ready.get_nowait())
        result = results.get_nowait()
        self.assertEqual(result["samples_ms"], [])
        self.assertIn("SQL failed", result["errors"][0])
        self.assertEqual(query.call_count, 2)
        connection.close.assert_called_once()

    def test_comparison_rejects_different_data_or_sql(self):
        old = dict(status="passed", manifest=dict(datasets=[dict(rows=1, sha256="abc")],
                   cases=[dict(name="case", sql="SELECT 1")]),
                   results=[dict(name="case", concurrency=1, qps=100, p50_ms=10, p99_ms=20)])
        new = copy.deepcopy(old)
        new["results"][0].update(qps=200, p50_ms=5)
        comparison = bench.compare_reports(old, new)["results"][0]
        self.assertEqual(comparison["qps_change_percent"], 100)
        self.assertEqual(comparison["p50_change_percent"], -50)
        for changed in ["sha256", "sql"]:
            bad = copy.deepcopy(new)
            if changed == "sha256":
                bad["manifest"]["datasets"][0][changed] = "other"
            else:
                bad["manifest"]["cases"][0][changed] = "SELECT 2"
            with self.assertRaises(ValueError):
                bench.compare_reports(old, bad)
        old["validation"] = {"case": {"result_sha256": "first"}}
        new["validation"] = {"case": {"result_sha256": "changed"}}
        with self.assertRaises(ValueError):
            bench.compare_reports(old, new)

    def test_sql_prepare_is_outside_measured_execute_loop(self):
        ready, results = queue.Queue(), queue.Queue()
        connection = Mock()
        with patch.object(bench, "connect", return_value=connection), patch.object(
                bench, "execute", side_effect=[(), [(1,)], [(1,)]]) as query:
            bench.worker({}, "db", {"sql": "SELECT 1"}, 1, 1, 1,
                         ready, Mock(wait=Mock(return_value=True)), results, prepared=True)
        self.assertEqual(query.call_args_list[0].args[1:],
                         ("PREPARE rrf_bench_statement FROM %s", ("SELECT 1",)))
        self.assertEqual(query.call_args_list[-1].args[1], "EXECUTE rrf_bench_statement")
        result = results.get_nowait()
        self.assertEqual(len(result["samples_ms"]), 1)
        self.assertEqual(result["errors"], [])

    def test_worker_rejects_stable_result_different_from_preflight(self):
        ready, results = queue.Queue(), queue.Queue()
        case = dict(sql="SELECT 1", expected_digest=bench.digest([(1,)]))
        with patch.object(bench, "connect", return_value=Mock()), patch.object(
                bench, "execute", side_effect=[[(2,)], [(2,)]]):
            bench.worker({}, "db", case, 1, 1, 1, ready,
                         Mock(wait=Mock(return_value=True)), results)
        result = results.get_nowait()
        self.assertEqual(result["samples_ms"], [])
        self.assertIn("preflight", result["errors"][0])

    def test_text_only_external_corpus_and_cpu_day_format(self):
        args = SimpleNamespace(seed=1, dataset="corpus.jsonl", vector_field="vector", text_field="text")
        with patch.object(bench.Path, "open", return_value=io.StringIO('{"text":"alpha"}\n')):
            self.assertEqual(list(bench.input_rows(args, 1)), [(0, "alpha", [])])
        with patch.object(bench.subprocess, "check_output", return_value="2048 1-02:03:04.50\n"):
            self.assertEqual(bench.process_resources(123), dict(rss_kib=2048, cpu_seconds=93784.5))


if __name__ == "__main__":
    unittest.main()
