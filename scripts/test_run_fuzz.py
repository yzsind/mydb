"""Failure injection tests for fuzz orchestration and evidence gates."""
from pathlib import Path
import json
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import run_fuzz


class RunnerTests(unittest.TestCase):
    def test_timeout_is_incomplete(self):
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as directory:
            with patch.object(run_fuzz.subprocess, 'run', side_effect=subprocess.TimeoutExpired(['fuzz'], 1)):
                result = run_fuzz.run_logged(['fuzz'], Path(directory) / 'log', {}, 1)
            self.assertEqual('incomplete', result['status'])
            self.assertEqual('wall-timeout', result['error'])
            self.assertIsNone(result['exit_code'])

    def exercise(self, outcomes, **kwargs):
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as directory:
            output = Path(directory)
            commands = []
            timeouts = []
            def logged(command, log, env, timeout):
                commands.append(command)
                timeouts.append(timeout)
                result = outcomes.pop(0)
                log.write_text(result.pop('log', 'stat::number_of_executed_units: 3\n#3 DONE cov: 12\n'))
                if result.pop('write_minimized', False):
                    exact = [argument.split('=', 1)[1] for argument in command
                             if argument.startswith('-exact_artifact_path=')]
                    Path(exact[0]).write_bytes(b'\x24')
                return result
            args = ['--output', directory]
            for target in kwargs.get('targets', ['jsonb', 'wal_frame']):
                args.extend(['--target', target])
            for target in kwargs.get('skip_targets', []):
                args.extend(['--skip-target', target])
            for option in ('input_timeout', 'wall_timeout', 'jobs'):
                if option in kwargs:
                    args.extend(['--' + option.replace('_', '-'), str(kwargs[option])])
            if kwargs.get('skip_slow_triage'):
                args.append('--skip-slow-triage')
            if kwargs.get('skip_build', True):
                args.append('--skip-build')
            if kwargs.get('prepare'):
                kwargs['prepare'](output)
            rustc_version = kwargs.get('rustc_version', 'rustc nightly\nhost: x86_64-unknown-linux-gnu\n')
            def version(command, **options):
                if command[0] == 'rustc':
                    self.assertEqual(['rustc', '+nightly', '-vV'], command)
                    return rustc_version
                return 'cargo-fuzz 0.13.2'
            with patch.object(run_fuzz, 'source_identity', return_value={}), \
                 patch.object(run_fuzz.subprocess, 'check_output', side_effect=version), \
                 patch.object(run_fuzz, 'seeds', side_effect=kwargs.get('seed_error')), \
                 patch.object(run_fuzz, 'run_logged', side_effect=logged):
                code = run_fuzz.main(args)
            if 'evidence' in kwargs:
                kwargs['evidence'].update(commands=commands, timeouts=timeouts,
                                          manifest=json.loads((output / 'manifest.json').read_text()))
            return code, json.loads((output / 'results.json').read_text())

    def test_sql_stress_budget_covers_initial_corpus_and_crash_triage(self):
        target = 'sql_execution_differential'
        def prepare(output):
            corpus = output / 'corpus' / target
            corpus.mkdir(parents=True)
            for family in range(16):
                for stress in (0, 1):
                    (corpus / f'{family}-{stress}').write_bytes(bytes([family, stress]))
            artifacts = output / 'artifacts' / target
            artifacts.mkdir(parents=True)
            (artifacts / 'timeout-ci').write_bytes(bytes([6, 1, 7, 13, 0, 255, 128]))
        evidence = {}
        code, _ = self.exercise(
            [{'status': 'fail', 'exit_code': 70},
             {'status': 'fail', 'exit_code': 70},
             {'status': 'pass', 'exit_code': 0, 'write_minimized': True}],
            targets=[target], prepare=prepare, evidence=evidence)
        self.assertEqual(1, code)
        for command in evidence['commands']:
            self.assertIn('-timeout=180', command)
        campaign, replay, minimize = evidence['timeouts']
        self.assertGreaterEqual(campaign, 2 * 33 * 180)
        self.assertGreater(replay, 2 * 180)
        self.assertGreater(minimize, 2 * 180)

    def test_explicit_timeouts_apply_to_campaign_replay_and_minimize(self):
        def prepare(output):
            artifacts = output / 'artifacts' / 'jsonb'
            artifacts.mkdir(parents=True)
            (artifacts / 'timeout-custom').write_bytes(b'\x01')
        evidence = {}
        code, _ = self.exercise(
            [{'status': 'fail', 'exit_code': 70},
             {'status': 'fail', 'exit_code': 70},
             {'status': 'pass', 'exit_code': 0, 'write_minimized': True}],
            targets=['jsonb'], prepare=prepare, evidence=evidence,
            input_timeout=240, wall_timeout=900)
        self.assertEqual(1, code)
        self.assertEqual(900, evidence['timeouts'][0])
        self.assertGreater(evidence['timeouts'][1], 2 * 240)
        self.assertGreater(evidence['timeouts'][2], 2 * 240)
        self.assertTrue(all('-timeout=240' in command for command in evidence['commands']))

    def test_decoder_keeps_short_input_timeout(self):
        evidence = {}
        code, _ = self.exercise([{'status': 'pass', 'exit_code': 0}], targets=['jsonb'], evidence=evidence)
        self.assertEqual(0, code)
        self.assertIn('-timeout=30', evidence['commands'][0])

    def test_nonpositive_input_timeout_is_rejected(self):
        for timeout in (0, -1):
            with self.subTest(timeout=timeout), self.assertRaises(SystemExit) as error:
                self.exercise([], input_timeout=timeout)
            self.assertEqual(2, error.exception.code)

    def test_toolchain_host_is_explicit_for_build_run_replay_and_minimize(self):
        def prepare(output):
            artifacts = output / 'artifacts' / 'jsonb'
            artifacts.mkdir(parents=True)
            (artifacts / 'crash-host').write_bytes(b'\xff')
        for host in ('x86_64-unknown-linux-gnu', 'aarch64-unknown-linux-gnu', 'aarch64-apple-darwin'):
            with self.subTest(host=host):
                evidence = {}
                code, _ = self.exercise(
                    [{'status': 'pass', 'exit_code': 0},
                     {'status': 'fail', 'exit_code': 77},
                     {'status': 'fail', 'exit_code': 77},
                     {'status': 'pass', 'exit_code': 0, 'write_minimized': True},
                     {'status': 'pass', 'exit_code': 0}],
                    skip_build=False, prepare=prepare, evidence=evidence,
                    rustc_version=f'rustc nightly\nhost: {host}\nLLVM version: 22\n')
                self.assertEqual(1, code)
                commands = evidence['commands']
                self.assertEqual(['build', 'run', 'run', 'run', 'run'], [c[3] for c in commands])
                for command in commands:
                    self.assertEqual(['cargo', '+nightly', 'fuzz'], command[:3])
                    self.assertEqual(1, command.count('--target'))
                    self.assertEqual(host, command[command.index('--target') + 1])
                self.assertEqual(host, evidence['manifest']['build_target'])

    def test_missing_toolchain_host_stops_before_build(self):
        evidence = {}
        code, results = self.exercise([], skip_build=False, rustc_version='rustc nightly\n', evidence=evidence)
        self.assertEqual(1, code)
        self.assertEqual([], evidence['commands'])
        self.assertTrue(all(r['status'] == 'incomplete' for r in results))
        self.assertIn('host', evidence['manifest']['error'])

    def test_timeout_does_not_skip_later_targets(self):
        code, results = self.exercise([{'status': 'incomplete', 'exit_code': None, 'error': 'wall-timeout'},
                                       {'status': 'pass', 'exit_code': 0}])
        self.assertEqual(1, code)
        self.assertEqual(['incomplete', 'pass'], [r['status'] for r in results])

    def test_exit_zero_without_executions_is_not_success(self):
        code, results = self.exercise([{'status': 'pass', 'exit_code': 0, 'log': ''},
                                       {'status': 'pass', 'exit_code': 0}])
        self.assertEqual(1, code)
        self.assertEqual('missing-execution-or-coverage-evidence', results[0]['error'])

    def test_build_and_seed_failures_retain_entire_plan(self):
        for outcomes, options in [([{'status': 'fail', 'exit_code': 1}], {'skip_build': False}),
                                   ([], {'seed_error': RuntimeError('seed failed')})]:
            with self.subTest(options=options):
                code, results = self.exercise(outcomes, **options)
                self.assertEqual(1, code)
                self.assertEqual(['jsonb', 'wal_frame'], [r['target'] for r in results])
                self.assertTrue(all(r['status'] == 'incomplete' for r in results))

    def test_only_complete_success_is_zero(self):
        code, results = self.exercise([{'status': 'pass', 'exit_code': 0}, {'status': 'pass', 'exit_code': 0}])
        self.assertEqual(0, code)
        self.assertTrue(all(r['executions'] == 3 and r['edges'] == 12 for r in results))

    def test_crash_artifacts_are_replayed_and_minimized(self):
        def prepare(output):
            artifacts = output / 'artifacts' / 'jsonb'
            artifacts.mkdir(parents=True)
            (artifacts / 'crash-abc').write_bytes(b'\xff' * 8)
        code, results = self.exercise(
            [{'status': 'fail', 'exit_code': 77},
             {'status': 'fail', 'exit_code': 77},
             {'status': 'pass', 'exit_code': 0, 'write_minimized': True},
             {'status': 'pass', 'exit_code': 0}], prepare=prepare)
        self.assertEqual(1, code)
        [crash] = results[0]['crashes']
        self.assertEqual('reproduced', crash['replay'])
        self.assertEqual('minimized', crash['minimize'])
        self.assertEqual('minimized-crash-abc', crash['minimized_file'])
        self.assertEqual(1, crash['minimized_bytes'])
        self.assertEqual([], results[1]['crashes'])

    def test_irreproducible_crash_skips_minimization(self):
        def prepare(output):
            artifacts = output / 'artifacts' / 'jsonb'
            artifacts.mkdir(parents=True)
            (artifacts / 'oom-xyz').write_bytes(b'\x00')
        code, results = self.exercise(
            [{'status': 'fail', 'exit_code': 70},
             {'status': 'pass', 'exit_code': 0},
             {'status': 'pass', 'exit_code': 0}], prepare=prepare)
        self.assertEqual(1, code)
        [crash] = results[0]['crashes']
        self.assertEqual('irreproducible', crash['replay'])
        self.assertNotIn('minimize', crash)

    def test_replay_timeout_is_recorded_without_minimization(self):
        def prepare(output):
            artifacts = output / 'artifacts' / 'jsonb'
            artifacts.mkdir(parents=True)
            (artifacts / 'timeout-late').write_bytes(b'\x01' * 4)
        code, results = self.exercise(
            [{'status': 'fail', 'exit_code': 71},
             {'status': 'incomplete', 'exit_code': None, 'error': 'wall-timeout'},
             {'status': 'pass', 'exit_code': 0}], prepare=prepare)
        self.assertEqual(1, code)
        [crash] = results[0]['crashes']
        self.assertEqual('replay-incomplete', crash['replay'])

    def test_slow_unit_triage_can_be_skipped(self):
        def prepare(output):
            artifacts = output / 'artifacts' / 'jsonb'
            artifacts.mkdir(parents=True)
            (artifacts / 'slow-unit-late').write_bytes(b'\x01' * 4)
            (artifacts / 'crash-fast').write_bytes(b'\x02' * 4)
        # Artifacts triage in sorted order: crash-fast first, then slow-unit-late.
        outcomes = {'skip': [{'status': 'pass', 'exit_code': 0},
                             {'status': 'pass', 'exit_code': 0}],
                    'triage': [{'status': 'pass', 'exit_code': 0},
                               {'status': 'fail', 'exit_code': 77},
                               {'status': 'pass', 'exit_code': 0},
                               {'status': 'pass', 'exit_code': 0}]}
        expected = {'skip': ['irreproducible', 'skipped-slow'],
                    'triage': ['reproduced', 'irreproducible']}
        for skip_slow, key in ((True, 'skip'), (False, 'triage')):
            with self.subTest(skip_slow=skip_slow):
                evidence = {}
                code, results = self.exercise(outcomes[key], targets=['jsonb'], prepare=prepare,
                                              evidence=evidence, skip_slow_triage=skip_slow)
                self.assertEqual(0, code)  # campaigns pass; triage details land in crashes
                self.assertEqual(expected[key], [crash.get('triage') or crash['replay']
                                                 for crash in results[0]['crashes']])
                # 2 = campaign + crash replay; 4 = adds minimize + slow-unit replay
                self.assertEqual(2 if skip_slow else 4, len(evidence['commands']))

    def test_concurrent_jobs_still_record_every_target(self):
        code, results = self.exercise([{'status': 'pass', 'exit_code': 0},
                                       {'status': 'pass', 'exit_code': 0}], jobs=2)
        self.assertEqual(0, code)
        self.assertEqual(['jsonb', 'wal_frame'], [result['target'] for result in results])
        self.assertTrue(all(result['status'] == 'pass' for result in results))

    def test_skip_target_removes_campaign_but_keeps_selection_valid(self):
        code, results = self.exercise([{'status': 'pass', 'exit_code': 0}],
                                      targets=['jsonb', 'wal_frame'], skip_targets=['jsonb'])
        self.assertEqual(0, code)
        self.assertEqual(['wal_frame'], [result['target'] for result in results])

    def test_skipping_every_selected_target_is_rejected(self):
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as directory:
            with self.assertRaises(SystemExit) as error:
                run_fuzz.main(['--output', directory, '--target', 'jsonb', '--skip-target', 'jsonb'])
            self.assertEqual(2, error.exception.code)

    def test_skip_unknown_target_is_rejected(self):
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as directory:
            with self.assertRaises(SystemExit) as error:
                run_fuzz.main(['--output', directory, '--skip-target', 'not-a-target'])
            self.assertEqual(2, error.exception.code)


if __name__ == '__main__':
    unittest.main()
