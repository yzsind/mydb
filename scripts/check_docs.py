#!/usr/bin/env python3
"""Check local links and heading anchors in the public documentation."""

from __future__ import annotations

import html
from pathlib import Path
import re
import sys
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(
    r'!?\[(?:[^\[\]\n]|\[[^\[\]\n]*\])*\]\(\s*'
    r'(<[^>\n]*>|(?:\\.|[^\s()\\]|\([^()\n]*\))+)'
    r'''(?:[ \t]+(?:"[^"\n]*"|'[^'\n]*'|\([^()\n]*\)))?\s*\)'''
)
REFERENCE = re.compile(r'^ {0,3}\[[^\]\n]+\]:\s*(<[^>\n]*>|\S+)', re.M)
HTML_LINK = re.compile(r'''<(?:a|img)\b[^>]*?\b(?:href|src)=["']([^"']+)["']''', re.I)
PRIVATE = {"docs-internal", "scripts-internal"}


def prose(source: str) -> str:
    """Exclude fenced examples while preserving line numbers for diagnostics."""
    result = []
    fence = None
    for line in source.splitlines(keepends=True):
        match = re.match(r'^ {0,3}(`{3,}|~{3,})(.*)$', line)
        if fence:
            if (match and match[1][0] == fence[0]
                    and len(match[1]) >= len(fence) and not match[2].strip()):
                fence = None
            result.append('\n')
        elif match:
            fence = match[1]
            result.append('\n')
        else:
            result.append(line)
    return ''.join(result)


def anchors(source: str) -> set[str]:
    source = prose(source)
    result = set(re.findall(r'<[^>]+\b(?:id|name)=["\']([^"\']+)["\']', source))
    for match in re.finditer(r'^ {0,3}#{1,6}\s+(.+?)\s*#*\s*$', source, re.M):
        title = re.sub(r'!?\[([^\]]*)\]\([^)]*\)', r'\1', match[1])
        title = html.unescape(re.sub(r'<[^>]*>', '', title))
        slug = re.sub(r'[^\w\- ]', '', title.lower()).replace(' ', '-')
        anchor = slug
        suffix = 0
        while anchor in result:
            suffix += 1
            anchor = f'{slug}-{suffix}'
        result.add(anchor)
    return result


def main() -> int:
    pages = [ROOT / 'README.md', *sorted((ROOT / 'docs').rglob('*.md'))]
    known_anchors = {}
    errors = []
    checked = 0
    for page in pages:
        source = prose(page.read_text(encoding='utf-8'))
        for match in [*LINK.finditer(source), *REFERENCE.finditer(source),
                      *HTML_LINK.finditer(source)]:
            destination = html.unescape(match[1].strip('<>'))
            url = urlsplit(destination)
            if url.scheme or url.netloc:
                continue
            checked += 1
            target = (page.parent / unquote(url.path)).resolve() if url.path else page
            line = source.count('\n', 0, match.start()) + 1
            location = f'{page.relative_to(ROOT)}:{line}'
            if not target.is_relative_to(ROOT):
                errors.append(f'{location}: link leaves repository: {destination}')
            elif PRIVATE.intersection(target.relative_to(ROOT).parts):
                errors.append(f'{location}: public link requires private files: {destination}')
            elif not target.exists():
                errors.append(f'{location}: missing target: {destination}')
            elif url.fragment and target.suffix == '.md':
                if target not in known_anchors:
                    known_anchors[target] = anchors(target.read_text(encoding='utf-8'))
                if unquote(url.fragment) not in known_anchors[target]:
                    errors.append(f'{location}: missing heading: {destination}')
    if errors:
        print('\n'.join(errors), file=sys.stderr)
        return 1
    print(f'Checked {checked} local links in {len(pages)} public Markdown files.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
