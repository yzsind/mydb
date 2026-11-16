import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import storage_search_bench as bench


class CacheBenchmarkTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir="/tmp/mydb")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text(json.dumps({
            "database": "db", "cases": [{"name": "query", "sql": "SELECT id FROM t"}]
        }))
        self.config = dict(data_dirs=[str(self.root / "data")],
                           workloads=[dict(manifest=str(self.manifest), only="query", concurrency=[1])])

    def test_first_wave_is_cold_and_reference_digest_is_checked(self):
        group = bench.groups(self.config)[0]
        calls = []

        def measured(settings, database, case, concurrency, args):
            calls.append((case, args))
            return dict(qps=1)

        with patch.object(bench, "buffer_status", return_value={}), \
                patch.object(bench.rrf, "benchmark", side_effect=measured), \
                patch.object(bench.rrf, "verify", side_effect=AssertionError("preflight warms cache")):
            bench.measure_pair({}, group, 123, dict(result_sha256="reference"), 3, 10)
        self.assertEqual(len(calls), 2)
        self.assertEqual((calls[0][1].warmup, calls[0][1].requests), (0, 1))
        self.assertEqual((calls[1][1].warmup, calls[1][1].requests), (10, 0))
        self.assertEqual(calls[1][1].duration, 3)
        self.assertTrue(all(case["expected_digest"] == "reference" for case, _ in calls))

    def test_server_is_stopped_after_a_workload_failure(self):
        process = MagicMock()
        process.poll.return_value = None
        process.wait.return_value = 0
        with patch.object(bench.subprocess, "Popen", return_value=process), \
                patch.object(bench.socket, "create_connection", return_value=MagicMock()):
            with self.assertRaisesRegex(RuntimeError, "workload failed"):
                with bench.server("mydb", self.root, 14999, self.root / "server.log", {}):
                    raise RuntimeError("workload failed")
        process.terminate.assert_called_once()
        process.wait.assert_called_once_with(timeout=30)
        process.kill.assert_not_called()

    def test_unresponsive_shutdown_is_not_reported_as_success(self):
        process = MagicMock()
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired("mydb", 30), 0]
        with patch.object(bench.subprocess, "Popen", return_value=process), \
                patch.object(bench.socket, "create_connection", return_value=MagicMock()):
            with self.assertRaisesRegex(RuntimeError, "forced termination"):
                with bench.server("mydb", self.root, 14999, self.root / "server.log", {}):
                    pass
        process.kill.assert_called_once()

    def test_group_validation_rejects_empty_duplicate_and_invalid_concurrency(self):
        for setting in [dict(only="absent"), dict(concurrency=[0]), dict(concurrency=[-1])]:
            config = copy.deepcopy(self.config)
            config["workloads"][0].update(setting)
            with self.assertRaises(ValueError):
                bench.groups(config)
        config = copy.deepcopy(self.config)
        config["workloads"] *= 2
        with self.assertRaisesRegex(ValueError, "duplicate"):
            bench.groups(config)

    def test_references_are_bound_to_sql_and_dataset_manifest(self):
        group = bench.groups(self.config)[0]
        changed = copy.deepcopy(group)
        changed["case"]["sql"] = "SELECT other FROM t"
        self.assertNotEqual(bench.reference_key(group), bench.reference_key(changed))
        changed = copy.deepcopy(group)
        changed["manifest_sha256"] = "other-dataset"
        self.assertNotEqual(bench.reference_key(group), bench.reference_key(changed))

    def test_missing_references_fail_before_any_server_is_started(self):
        config = self.root / "config.json"
        config.write_text(json.dumps(self.config))
        (self.root / "references.json").write_text("{}")
        with patch.object(sys, "argv", ["bench", "--config", str(config), "--output", str(self.root)]), \
                patch.object(bench, "server") as start:
            with self.assertRaisesRegex(ValueError, "missing reference"):
                bench.main()
            start.assert_not_called()

    def test_summary_rejects_missing_repetitions(self):
        bench.rrf.save(self.root / "measurement.json", dict(config=self.config, repeats=2))
        with self.assertRaisesRegex(ValueError, "incomplete"):
            bench.summarize(self.root)

    def test_summary_keeps_cold_and_warm_metrics_separate(self):
        group = bench.groups(self.config)[0]
        bench.rrf.save(self.root / "measurement.json", dict(config=self.config, repeats=2))
        for repeat in range(2):
            for version, qps, cold in [("before", 100, 10), ("after", 150, 5)]:
                warm = dict(qps=qps, p50_ms=2, p99_ms=3, operations=100,
                            server=dict(cpu_seconds=.1))
                record = dict(status="passed", group=0, data_dir="data", version=version,
                              repeat=repeat, measurement_group=group, warm=warm,
                              cold_first_wave=dict(p50_ms=cold, max_ms=cold + 1))
                bench.rrf.save(self.root / f"run-{repeat}-{version}.json", record)
        rows = bench.summarize(self.root)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["warm_qps_change_percent"], 50)
        self.assertEqual(rows[0]["cold_p50_change_percent"], -50)
        self.assertEqual(rows[0]["before"]["cpu_ms_per_operation"], 1)

    @unittest.skipUnless(sys.platform == "darwin", "Darwin file-scoped cold mode")
    def test_os_cold_copy_preserves_content_path_and_metadata(self):
        data = self.root / "data"
        data.mkdir()
        path = data / "payload.heap"
        content = bytes(range(256)) * 256
        path.write_bytes(content)
        previous = path.stat()
        result = bench.cold_data_files(data)
        self.assertEqual(result["files"], 1)
        self.assertEqual(result["bytes"], len(content))
        self.assertEqual(result["resident_pages_after"], 0)
        self.assertNotEqual(path.stat().st_ino, previous.st_ino)
        self.assertEqual(path.stat().st_mtime_ns, previous.st_mtime_ns)
        self.assertEqual(path.read_bytes(), content)

    @unittest.skipUnless(sys.platform == "darwin", "Darwin file-scoped cold mode")
    def test_os_cold_mode_does_not_rewrite_outside_benchmark_directory(self):
        with self.assertRaisesRegex(ValueError, "restricted"):
            bench.cold_data_files(Path("/Users"))


if __name__ == "__main__":
    unittest.main()
