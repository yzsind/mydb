"""Tests of benchmark accounting, comparison and workload coverage."""

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace

from function_expression_bench import (
    InventoryParser,
    check_contract,
    compare_captures,
    equivalent,
    effective_comparison,
    numeric_equal,
    parallel_evidence,
    sample_summary,
    report_result,
    sql_for,
    Worker,
    run,
)
from function_expression_cases import build_cases, registered_names
from function_expression_extend import unchanged_cases
from function_expression_merge import merge


class BenchmarkTests(unittest.TestCase):
    def test_unordered_release_lock_checks_ownership_and_exactly_one_release(self):
        with tempfile.TemporaryDirectory(dir="/tmp/mydb") as directory:
            path = Path(directory) / "release.capture"
            path.write_text('16\t["16","1"]\n1\t["1",null]\n8\t["8",null]\n')
            self.assertEqual(check_contract(path, "RELEASE_LOCK")["status"], "contract_valid")
            path.write_text('16\t["16","1"]\n8\t["8","1"]\n')
            self.assertEqual(check_contract(path, "RELEASE_LOCK")["status"], "contract_invalid")
            path.write_text('1\t["1","1"]\n')
            self.assertEqual(check_contract(path, "RELEASE_LOCK")["status"], "contract_invalid")

    def test_single_execution_warms_each_shared_dataset_once_and_never_replays_functions(self):
        with tempfile.TemporaryDirectory(dir="/tmp/mydb") as directory:
            root = Path(directory)
            worker = root / "worker"
            worker.write_text("worker")
            (root / "mydb").write_text("engine")
            cases = [dict(id=name, name=name.upper(), category="test", comparison="exact",
                          sql=f"SELECT id,{name}(x) FROM bench") for name in ["f", "g"]]
            (root / "manifest.json").write_text(json.dumps(dict(cases=cases)))
            calls = []

            def endpoint(name, args):
                ep = Mock()
                ep.port = 34926 if name.startswith("baseline_") else 34927
                ep.worker.connection_id = 7
                ep.control.query.return_value = dict(status="ok", rows=0, sample=[])
                def execute(sql, **options):
                    calls.append(sql)
                    if options.get("capture"):
                        Path(options["capture"]).write_text('1\t["1","value"]\n')
                    return dict(status="ok", rows=1, elapsed_ms=7, sample=[])
                ep.execute.side_effect = execute
                return ep

            args = SimpleNamespace(directory=root, worker=worker, rows=100000,
                timeout=1, repeats=1, warmup=0,
                modes=["baseline_serial", "baseline_parallel2", "serial", "parallel2"],
                filter=None, limit=None, skip_invalid=True, plan_once=False,
                single_pass=True, dataset_warmup=True, rerun=False)
            with patch("function_expression_bench.Endpoint", side_effect=endpoint):
                run(args)
            self.assertEqual(sum(sql.startswith("SELECT * FROM") for sql in calls), 6)
            self.assertFalse(any(sql.startswith("EXPLAIN") for sql in calls))
            self.assertEqual(len(calls), 14)  # Six fixture reads and eight measured executions.
            for case in cases:
                result = json.loads((root / "cases" / case["id"] / "result.json").read_text())
                for mode in ("serial", "parallel2"):
                    self.assertEqual(result["correctness"]["baseline_vs_" + mode]["status"], "match")
                for engine in result["engines"].values():
                    samples = engine["metrics"]["end_to_end"]["samples"]
                    self.assertEqual(len(samples), 1)
                    self.assertFalse(samples[0]["warmup"])
                    self.assertEqual(samples[0]["elapsed_ms"], 7)
                    self.assertNotIn("plan_observation", engine)

    def test_single_plan_observation_preserves_end_to_end_samples_and_resume_identity(self):
        with tempfile.TemporaryDirectory(dir="/tmp/mydb") as directory:
            root = Path(directory)
            worker = root / "worker"
            worker.write_text("unchanged worker")
            (root / "mydb").write_text("frozen engine")
            (root / "manifest.json").write_text(json.dumps({"cases": [{
                "id": "a", "name": "F", "category": "test", "comparison": "exact",
                "sql": "SELECT id,F(x) FROM bench",
            }]}))
            endpoint = Mock()
            endpoint.worker.connection_id = 7
            endpoint.control.query.return_value = dict(status="ok", rows=0, sample=[])

            def execute(sql, **options):
                if options.get("capture"):
                    Path(options["capture"]).write_text('1\t["1","value"]\n')
                return dict(status="ok", rows=1, elapsed_ms=10, sample=[["plan"]])

            endpoint.execute.side_effect = execute
            args = SimpleNamespace(directory=root, worker=worker, rows=1000000,
                timeout=10, repeats=3, warmup=1, modes=["mysql"], filter=None,
                limit=None, skip_invalid=True, plan_once=True, rerun=False)
            with patch("function_expression_bench.Endpoint", return_value=endpoint):
                run(args)
            result = json.loads((root / "cases/a/result.json").read_text())["engines"]["mysql"]
            samples = result["metrics"]["end_to_end"]["samples"]
            self.assertEqual([s["warmup"] for s in samples], [True, False, False, False])
            self.assertEqual(result["metrics"]["analyze"]["status"], "unavailable")
            self.assertNotIn("samples", result["metrics"]["analyze"])
            self.assertEqual(result["plan_observation"]["elapsed_ms"], 10)
            self.assertEqual(sum(call.args[0].startswith("EXPLAIN ANALYZE") for call in endpoint.execute.call_args_list), 1)
            args.plan_once = False
            with self.assertRaisesRegex(RuntimeError, "plan_once"):
                run(args)

    def test_timeout_cohorts_keep_their_original_budgets(self):
        with tempfile.TemporaryDirectory(dir="/tmp/mydb") as directory:
            root = Path(directory)
            sources = [root / "old", root / "new"]
            identity = {
                "manifest_sha256": "same",
                "rows": 1000000,
                "worker_sha256": "worker",
                "mydb_binary_sha256": "engine",
                "modes": ["mysql", "serial", "parallel2"],
                "prepared": {},
                "warmup": 1,
                "repeats": 3,
            }
            for source, identifier, budget in zip(sources, ["a", "b"], [10, 3]):
                path = source / "cases" / identifier
                path.mkdir(parents=True)
                (source / "context.json").write_text(
                    json.dumps(dict(identity, timeout_s=budget))
                )
                (path / "result.json").write_text(json.dumps({"case_id": identifier}))
            target = root / "combined"
            self.assertEqual(merge(sources, target), 2)
            self.assertEqual(
                json.loads((target / "cases/a/result.json").read_text())["timeout_s"],
                10,
            )
            self.assertEqual(
                json.loads((target / "cases/b/result.json").read_text())["timeout_s"], 3
            )
            bad = dict(identity, timeout_s=3, rows=999)
            (sources[1] / "context.json").write_text(json.dumps(bad))
            with self.assertRaises(ValueError):
                merge(sources, target)

    def test_json_protocol_values_are_compared_structurally(self):
        columns = [{"type": "MYSQL_TYPE_LONGLONG"}, {"type": "MYSQL_TYPE_JSON"}]
        comparison = effective_comparison("exact", columns)
        self.assertEqual(comparison, "json")
        self.assertTrue(
            equivalent('{"coordinates":[2.0,2.0]}', '{"coordinates":[2,2]}', comparison)
        )

    def test_post_validation_preserves_timings_and_rejects_changed_sql(self):
        with tempfile.TemporaryDirectory(dir="/tmp/mydb") as directory:
            path = Path(directory) / "result.json"
            original = {
                "case_id": "a",
                "comparison": "contract",
                "engines": {
                    "serial": {
                        "sql": "SELECT /*feb:aaa*/ id,F() FROM bench",
                        "metrics": {"elapsed_ms": 42},
                    }
                },
                "correctness": {"serial": {"status": "contract_valid"}},
            }
            path.write_text(json.dumps(original))
            os.utime(path, ns=(1, 1))
            post = {
                "case_id": "a",
                "comparison": "contract",
                "engines": {"serial": {"sql": "SELECT /*feb:bbb*/ id,F() FROM bench"}},
                "correctness": {"serial": {"status": "contract_invalid"}},
            }
            post_path = path.with_name("validation.json")
            post_path.write_text(json.dumps(post))
            combined = report_result(path)
            self.assertEqual(combined["engines"]["serial"]["metrics"]["elapsed_ms"], 42)
            self.assertEqual(
                combined["correctness"]["serial"]["status"], "contract_invalid"
            )
            self.assertEqual(
                combined["initial_correctness"]["serial"]["status"], "contract_valid"
            )
            post["engines"]["serial"]["sql"] = "SELECT id,OTHER() FROM bench"
            post_path.write_text(json.dumps(post))
            with self.assertRaises(ValueError):
                report_result(path)

    def test_missing_cancel_ack_is_a_timeout_not_a_success(self):
        worker = Worker.__new__(Worker)
        worker.connection_id = 7
        worker.process = Mock()
        worker.line = Mock(
            side_effect=[{"event": "started"}, TimeoutError(), TimeoutError()]
        )
        worker.close = Mock()
        control = Mock()
        control.query.return_value = {"status": "ok"}
        result = worker.query("SELECT slow_function()", timeout=0.01, control=control)
        self.assertEqual(result["status"], "timeout")
        self.assertTrue(result["requires_reconnect"])
        self.assertFalse(result["cancellation_acknowledged"])
        worker.close.assert_called_once()

    def test_registry_is_accounted_for(self):
        cases = build_cases()
        missing = registered_names() - {case["name"] for case in cases}
        self.assertTrue(
            all(name.startswith(("HTTP_", "AI_")) for name in missing), missing
        )
        self.assertEqual(len(cases), len({case["id"] for case in cases}))

    def test_native_mysql_helpers_are_not_replaced_by_sys_routines(self):
        cases = {case["id"]: case for case in build_cases()}
        for name in (
            "format_bytes",
            "format_pico_time",
            "ps_current_thread_id",
            "ps_thread_id",
            "validate_password_strength",
        ):
            self.assertNotIn("mysql_unavailable", cases[name])
            self.assertNotIn("mysql_sql", cases[name])
        self.assertIn("u*1024", cases["format_bytes"]["sql"])

    def test_reference_aliases_remain_separate(self):
        parser = InventoryParser()
        parser.feed(
            '<th scope="row"><a href="x"><code>SHA1()</code>, <code>SHA()</code></a></th>'
        )
        self.assertEqual([item["name"] for item in parser.items], ["SHA1", "SHA"])

    def test_hint_precedes_tracking_comment(self):
        sql = sql_for({"sql": "SELECT id,ABS(n) FROM bench"}, "parallel2", "abc")
        self.assertTrue(sql.startswith("SELECT /*+ PARALLEL(2) */ /*feb:abc*/"))

    def test_decimal_comparison_is_not_rounded(self):
        self.assertFalse(equivalent("1.0000000000001", "1.0000000000002", "exact"))

    def test_vector_scan_is_not_expression_parallelism(self):
        evidence = parallel_evidence(
            [["Exchange workers=2 execution_mode=vector"]], "parallel2"
        )
        self.assertEqual(evidence["observed_workers"], [2])
        self.assertEqual(evidence["observed_expression_modes"], [])
        self.assertFalse(evidence["parallel_expression_operator_observed"])
        evidence = parallel_evidence(
            [["Parallel aggregate workers=2 expression_mode=compiled"]], "parallel2"
        )
        self.assertTrue(evidence["parallel_expression_operator_observed"])

    def test_float_relative_absolute_and_nonfinite(self):
        self.assertTrue(numeric_equal("0", "1e-13"))
        self.assertTrue(numeric_equal("1000000", "1000000.0001"))
        self.assertFalse(numeric_equal("1", "1.01"))
        self.assertFalse(numeric_equal("NaN", "1"))

    def test_json_object_and_array_multiplicity(self):
        self.assertTrue(equivalent('{"a":1,"b":null}', '{"b":null,"a":1}', "json"))
        self.assertTrue(equivalent("[1,2,1]", "[2,1,1]", "json_multiset"))
        self.assertFalse(equivalent("[1,2,1]", "[2,1,2]", "json_multiset"))

    def test_null_is_distinct_from_text(self):
        self.assertFalse(equivalent(None, "null", "json"))
        self.assertFalse(equivalent(None, "NULL", "exact"))

    def test_json_boolean_is_not_a_number_and_large_numbers_remain_exact(self):
        self.assertFalse(equivalent('{"v":true}', '{"v":1}', "json"))
        self.assertFalse(equivalent("[false]", "[0]", "json_multiset"))
        self.assertFalse(equivalent("9007199254740992.0", "9007199254740993.0", "json"))

    def test_multipoint_semantics_and_multiplicity(self):
        self.assertTrue(
            equivalent(
                "MULTIPOINT((2 2),(3 2),(2 2))",
                "MULTIPOINT(3 2,2 2,2 2)",
                "geometry_multiset",
            )
        )
        self.assertFalse(
            equivalent(
                "MULTIPOINT((2 2),(3 2))", "MULTIPOINT(3 2,3 2)", "geometry_multiset"
            )
        )

    def test_partial_samples_cannot_be_ranked(self):
        samples = [
            dict(status="ok", elapsed_ms=5, warmup=False),
            dict(status="timeout", elapsed_ms=10001, warmup=False),
        ]
        summary = sample_summary({"samples": samples})
        self.assertEqual(summary["status"], "partial")
        self.assertEqual(summary["median_ms"], 5)

    def test_warmup_is_excluded(self):
        samples = [
            dict(status="ok", elapsed_ms=x, warmup=x == 100) for x in (100, 1, 2, 3)
        ]
        self.assertEqual(sample_summary({"samples": samples})["median_ms"], 2)

    def test_missing_statement_timer_is_not_successful_measurement(self):
        samples = [dict(status="ok", elapsed_ms=1, warmup=False) for _ in range(3)]
        self.assertEqual(
            sample_summary({"samples": samples}, True)["status"], "unavailable"
        )

    def test_failed_warmup_retains_the_error_status(self):
        self.assertEqual(
            sample_summary({"samples": [dict(status="error", warmup=True)]})["status"],
            "error",
        )

    def test_nondefault_repeat_count_is_not_partial(self):
        samples = [dict(status="ok", elapsed_ms=x, warmup=False) for x in range(1, 6)]
        self.assertEqual(
            sample_summary({"samples": samples}, expected=5)["status"], "ok"
        )

    def test_capture_order_duplicates_and_count(self):
        base = Path("/tmp/mydb")
        base.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=base) as directory:
            left, right = Path(directory) / "left", Path(directory) / "right"
            left.write_text('2\t["2","a"]\n1\t["1",null]\n2\t["2","a"]\n')
            right.write_text('2\t["2","a"]\n2\t["2","a"]\n1\t["1",null]\n')
            self.assertEqual(compare_captures(left, right, "exact")["status"], "match")
            right.write_text('2\t["2","a"]\n1\t["1",null]\n')
            self.assertEqual(
                compare_captures(left, right, "exact")["status"], "mismatch"
            )

    def test_identifier_contract_checks_uniqueness_and_sequence_order(self):
        with tempfile.TemporaryDirectory(dir="/tmp/mydb") as directory:
            path = Path(directory) / "capture"
            path.write_text('2\t["2","1"]\n1\t["1","2"]\n')
            self.assertEqual(
                check_contract(path, "NEXTVAL")["status"], "contract_valid"
            )
            path.write_text('1\t["1","1"]\n2\t["2","1"]\n')
            self.assertEqual(
                check_contract(path, "UUID_SHORT")["status"], "contract_invalid"
            )

    def test_primary_arguments_are_not_out_of_domain_fixtures(self):
        cases = {case["name"]: case for case in build_cases()}
        self.assertIn("f/1000", cases["ACOS"]["sql"])
        self.assertIn(",256)", cases["SHA2"]["sql"])
        self.assertIn("d", cases["ROUND"]["sql"])

    def test_extension_refuses_changed_existing_sql(self):
        old = [dict(id="a", sql="SELECT 1")]
        self.assertEqual(
            unchanged_cases(old, old + [dict(id="b", sql="SELECT 2")]), ["b"]
        )
        with self.assertRaises(ValueError):
            unchanged_cases(old, [dict(id="a", sql="SELECT 3")])


if __name__ == "__main__":
    unittest.main()
