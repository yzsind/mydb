#!/usr/bin/env python3
"""Regenerate factual BMP primary-weight mappings from a local MySQL oracle."""
import argparse
from pathlib import Path
import pymysql

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--host', default='127.0.0.1')
p.add_argument('--port', type=int, default=3306)
p.add_argument('--user', default='root')
p.add_argument('--output', type=Path, default=Path('src/charset/legacy_general.rs'))
a = p.parse_args()
c = pymysql.connect(host=a.host, port=a.port, user=a.user, charset='utf8mb4')
changes = []
with c.cursor() as cur:
    for first in range(128, 65536, 256):
        points = [cp for cp in range(first, min(first+256, 65536)) if not 0xd800 <= cp <= 0xdfff]
        if not points:
            continue
        expressions = [f"HEX(WEIGHT_STRING(CONVERT(X'{chr(cp).encode('utf8').hex()}' USING utf8mb4) COLLATE utf8mb4_general_ci))" for cp in points]
        cur.execute('SELECT '+','.join(expressions))
        for cp, value in zip(points, cur.fetchone()):
            assert len(value) == 4, (cp, value)
            weight = int(value, 16)
            if cp != weight:
                changes.append((cp, weight))
c.close()
ranges, exceptions = [], []
i = 0
while i < len(changes):
    j = i+1
    step = changes[j][0]-changes[i][0] if j < len(changes) else 0
    delta = changes[j][1]-changes[i][1] if j < len(changes) else 0
    if 0 < step <= 2 and 0 <= delta <= 2:
        while j < len(changes) and changes[j][0] == changes[j-1][0]+step and changes[j][1] == changes[j-1][1]+delta:
            j += 1
    if j-i >= 3:
        ranges.append((changes[i][0], changes[j-1][0], step, changes[i][1], delta))
    else:
        exceptions.extend(changes[i:j])
    i = j
expanded = dict(exceptions)
for lo, hi, step, base, delta in ranges:
    expanded.update((cp, base + ((cp-lo)//step)*delta) for cp in range(lo, hi+1, step))
assert expanded == dict(changes)
source = '''//! Generated factual Unicode BMP primary weights for MySQL legacy general_ci.
//! Regenerate with scripts/generate_legacy_general_weights.py against MySQL.
//! Identity mappings are omitted; this table is independent of Unicode library
//! upgrades and does not decompose Hangul or expand multicharacter case folds.

pub(super) fn primary(ch: char) -> u16 {
    let cp = ch as u32;
    if cp < 128 { return (cp as u8).to_ascii_uppercase() as u16; }
    if cp > 0xffff { return 0xfffd; }
    let cp = cp as u16;
    let position = RANGES.partition_point(|&(start, _, _, _, _)| start <= cp);
    if let Some(&(start, end, step, base, delta)) = position.checked_sub(1).map(|i| &RANGES[i]) {
        if cp <= end && (cp-start) % step == 0 { return base + ((cp-start) / step) * delta; }
    }
    EXCEPTIONS.binary_search_by_key(&cp, |&(key, _)| key)
        .map(|i| EXCEPTIONS[i].1).unwrap_or(cp)
}

const RANGES: &[(u16, u16, u16, u16, u16)] = &[
'''
source += ''.join(f'    (0x{lo:04x}, 0x{hi:04x}, {step}, 0x{base:04x}, {delta}),\n' for lo,hi,step,base,delta in ranges)
source += '];\n\nconst EXCEPTIONS: &[(u16, u16)] = &[\n'
source += ''.join(f'    (0x{cp:04x}, 0x{weight:04x}),\n' for cp,weight in exceptions)
source += '];\n'
a.output.write_text(source)
print(f'{len(changes)} nonidentity weights: {len(ranges)} ranges, {len(exceptions)} exceptions')
