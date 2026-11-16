"""Shared local evidence helpers for safety runners; no credentials are captured."""
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, default=str, indent=2) + '\n')
    temporary.replace(path)


def source_identity(root, paths):
    root = Path(root)
    files = sorted({p for path in paths for p in ([path] if path.is_file() else path.rglob('*')) if p.is_file()})
    return {'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
            'working_tree': subprocess.check_output(['git', 'status', '--short', '--untracked-files=all'], cwd=root, text=True),
            'platform': platform.platform(),
            'sources': {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files
                        if p.suffix in ('.rs', '.py', '.clj', '.toml', '.lock', '.dict', '.json', '.yml', '.c', '.h')}}


def snapshot(root, output, paths):
    import shutil
    for source in paths:
        for path in ([source] if source.is_file() else source.rglob('*')):
            if path.is_file() and path.suffix in ('.py', '.clj', '.toml', '.rs', '.lock', '.c', '.h'):
                destination = output / 'harness' / path.relative_to(root)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, destination)
