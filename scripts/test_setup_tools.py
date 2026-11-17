"""Ubuntu setup installs pinned, checksum-verified tools without floating URLs."""
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from build_linux_glibc217 import NEXT, NEXT_SHA256
import package_release

ROOT = Path(__file__).resolve().parent


class SetupToolTests(unittest.TestCase):
    def test_all_architectures_resolve_pinned_nextest_assets(self):
        script = 'set -euo pipefail\nsource "$1"\nmydb_nextest_asset "$2"\nprintf "%s\\n%s\\n" "$NEXTEST_URL" "$NEXTEST_SHA256"\n'
        for architecture in ['x86_64', 'aarch64', 'arm64', 'riscv64']:
            result = subprocess.run(['bash', '-c', script, 'test', str(ROOT / '_nextest_release.sh'), architecture],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            url, digest = result.stdout.splitlines()
            self.assertIn('/cargo-nextest-' + NEXT + '/', url)
            self.assertRegex(digest, '^[0-9a-f]{64}$')
            if architecture == 'x86_64':
                self.assertEqual(digest, NEXT_SHA256)

    def test_bad_tool_archive_is_rejected_before_extraction(self):
        Path('/tmp/mydb').mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            archive = Path(temporary) / 'archive with spaces.tar.gz'
            archive.write_bytes(b'test bytes')
            script = 'set -euo pipefail\nsource "$1"\nmydb_verify_nextest_archive "$2" "$3"\n'
            for digest, expected in [(hashlib.sha256(archive.read_bytes()).hexdigest(), 0), ('0' * 64, 1)]:
                result = subprocess.run(['bash', '-c', script, 'test', str(ROOT / '_nextest_release.sh'), str(archive), digest],
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, expected, result.stderr)

    def test_native_release_provenance_comes_from_git(self):
        with patch.object(package_release, 'command', side_effect=['commit', 'status']) as command:
            self.assertEqual(package_release.build_provenance(), ('commit', 'status'))
        self.assertEqual([call.args for call in command.call_args_list],
                         [('git', 'rev-parse', 'HEAD'), ('git', 'status', '--short')])


if __name__ == '__main__':
    unittest.main()
