"""Cold-cache and public-checkout prerequisites must pass before release builds."""
import ast
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import verify_linux_compat as compat

ROOT = Path(__file__).resolve().parent.parent


class ReleasePrerequisitesTests(unittest.TestCase):
    def test_required_protocol_gates_exist_and_help_does_not_connect(self):
        for name in ('verify_expression_protocol.py', 'verify_json_protocol.py'):
            path = ROOT / 'scripts' / name
            self.assertTrue(path.is_file(), f'public release dependency missing: {name}')
            result = subprocess.run([sys.executable, str(path), '--help'], capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn('scripts-internal', path.read_text())

    def test_public_protocol_imports_work_without_private_tools_or_git(self):
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            root = Path(temporary)
            scripts = root / 'scripts'; scripts.mkdir()
            for name in (*compat.PROTOCOL_GATES, 'protocol_helpers.py', '_checks.py'):
                shutil.copy(ROOT / 'scripts' / name, scripts / name)
            for name in compat.PROTOCOL_GATES:
                result = subprocess.run([sys.executable, str(scripts / name), '--help'], cwd=root,
                    env=os.environ | {'PYTHONPATH': ''}, capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
            compat.check_public_protocol_gates(root)
            (scripts / '_checks.py').unlink()
            with self.assertRaisesRegex(FileNotFoundError, '_checks.py'):
                compat.check_public_protocol_gates(root)

    def test_native_bootstrap_client_architecture_need_not_match_server_architecture(self):
        local = 'sha256:' + '1' * 64
        info = {'Id': local, 'Architecture': 'arm64', 'Os': 'linux'}
        with patch.object(compat, 'checked', return_value=json.dumps([info])):
            self.assertEqual(compat.ensure_runtime_image(local, require_amd64=False), info)

    def test_missing_gate_fails_preflight_before_any_container_operation(self):
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            root = Path(temporary)
            with patch.object(compat, 'checked') as docker, self.assertRaisesRegex(FileNotFoundError, 'protocol'):
                compat.check_public_protocol_gates(root)
            docker.assert_not_called()

    def test_cold_remote_image_is_pulled_before_it_is_inspected_again(self):
        image = compat.IMAGES['centos7']
        info = {'Id': 'sha256:' + '1' * 64, 'RepoDigests': [image], 'Architecture': 'amd64', 'Os': 'linux'}
        with patch.object(compat, 'checked', side_effect=[RuntimeError('missing image'), '', json.dumps([info])]) as checked:
            self.assertEqual(compat.ensure_runtime_image(image), info)
        self.assertEqual(checked.call_args_list[0].args[0], ['docker', 'image', 'inspect', image])
        self.assertEqual(checked.call_args_list[1].args[0], ['docker', 'pull', '--platform', 'linux/amd64', image])
        self.assertEqual(checked.call_args_list[2].args[0], ['docker', 'image', 'inspect', image])

    def test_cached_image_is_reused_and_local_ids_are_never_pulled(self):
        local = 'sha256:' + '1' * 64
        info = {'Id': local, 'Architecture': 'amd64', 'Os': 'linux', 'RepoDigests': []}
        with patch.object(compat, 'checked', return_value=json.dumps([info])) as checked:
            self.assertEqual(compat.ensure_runtime_image(local), info)
            self.assertEqual(checked.call_count, 1)
        with patch.object(compat, 'checked', side_effect=RuntimeError('missing local image')) as checked:
            with self.assertRaisesRegex(RuntimeError, 'missing local image'):
                compat.ensure_runtime_image(local)
            self.assertEqual(checked.call_count, 1)

    def test_pull_failure_and_wrong_platform_cannot_pass(self):
        with patch.object(compat, 'checked', side_effect=RuntimeError('registry unavailable')):
            with self.assertRaises(RuntimeError):
                compat.ensure_runtime_image(compat.IMAGES['centos7'])
        info = {'Id': 'sha256:' + '1' * 64, 'Architecture': 'arm64', 'Os': 'linux'}
        with patch.object(compat, 'checked', return_value=json.dumps([info])):
            with self.assertRaisesRegex(RuntimeError, 'amd64'):
                compat.ensure_runtime_image(compat.IMAGES['centos7'])

    def test_workflow_prefetches_before_building_and_verifying(self):
        workflow = (ROOT / '.github/workflows/linux-glibc217.yml').read_text()
        self.assertIn('--prepare-images-only', workflow)
        self.assertLess(workflow.index('--prepare-images-only'), workflow.index('scripts/build_linux_glibc217.py \\\n'))
        self.assertIn('--images centos7,centos8,centos9', workflow)


if __name__ == '__main__':
    unittest.main()
