#!/usr/bin/env python3
"""Run the unified repository acceptance suite (the former split formats retired)."""
import sys
sys.dont_write_bytecode = True
from test_backup_recovery import main
if __name__ == '__main__':
    main()
