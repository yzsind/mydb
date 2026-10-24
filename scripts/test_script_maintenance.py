#!/usr/bin/env python3
"""Check that the public crash regression rejects heap/index inconsistencies."""
import os
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parent


class CrashConsistencyTests(unittest.TestCase):
    def test_crash_consistency_mismatch_fails(self):
        source = (ROOT / "crash_test_index_defer.sh").read_text()
        body = source[source.index("consistency() {"):source.index('\necho "############ TEST 1')]
        for index_count, missing, expected in [("2 10", "0", 0), ("1 5", "0", 1), ("2 10", "1", 1)]:
            script = r'''set -euo pipefail
myq() {
 case "$*" in
  *NOT\ EXISTS*) echo "$missing" ;;
  *FORCE\ INDEX*) echo "$index_count" ;;
  *) echo '2 10' ;;
 esac
}
''' + body + "\nconsistency\n"
            result = subprocess.run(["bash", "-c", script], env=os.environ | {
                "missing": missing, "index_count": index_count}, capture_output=True, text=True)
            self.assertEqual(result.returncode, expected, result.stderr)


if __name__ == "__main__":
    unittest.main()
