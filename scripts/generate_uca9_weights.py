#!/usr/bin/env python3
"""Generate Unicode 9 primary collation tables. Pass the official allkeys.txt path.
Source: https://www.unicode.org/Public/UCA/9.0.0/allkeys.txt
Run from the repository root. Output is deterministic and network-free.
"""

from pathlib import Path
import sys
import subprocess
import re, struct

if "@version 9.0.0" not in Path(sys.argv[1]).read_text():
    raise SystemExit("Expected the Unicode 9.0.0 allkeys.txt file")
rows = []
for line in Path(sys.argv[1]).read_text().splitlines():
    line = line.split("#")[0].strip()
    if ";" not in line or line.startswith("@"):
        continue
    key, weights = line.split(";")
    key = tuple(int(c, 16) for c in key.split())
    primary = [int(x, 16) for x in re.findall(r"\[[.*]([0-9A-F]+)\.", weights)]
    primary = [x for x in primary if x]
    rows.append((key, primary))
rows.sort()
singles = [r for r in rows if len(r[0]) == 1]
multis = [r for r in rows if len(r[0]) > 1]
out = bytearray()
weights = []
for key, ws in singles:
    out += struct.pack("<IIH", key[0], len(weights), len(ws))
    weights += ws
out += b"".join(struct.pack("<H", w) for w in weights)
Path("src/charset_uca9_primary.bin").write_bytes(out)
f = Path("src/charset_uca9.rs")
f.write_text(
    """//! Unicode 9.0 DUCET primary weights for utf8mb4_0900_ai_ci.
//! Generated from https://www.unicode.org/Public/UCA/9.0.0/allkeys.txt.
//! Copyright 2016 Unicode, Inc.; Unicode data license: https://www.unicode.org/license.txt.
//! Packed index (code point, weight offset, count), followed by little-endian u16 weights.
const DATA: &[u8] = include_bytes!("charset_uca9_primary.bin");
const COUNT: usize = """
    + str(len(singles))
    + """;
const ASCII: &[&[u16]] = &[
"""
    + "".join(
        "    &[" + ",".join(hex(x) for x in dict(singles).get((cp,), [])) + "],\n"
        for cp in range(128)
    )
    + """];
fn u32_at(offset: usize) -> u32 { u32::from_le_bytes(DATA[offset..offset+4].try_into().unwrap()) }
fn emit(out: &mut Vec<u8>, weight: u16) { out.extend_from_slice(&weight.to_be_bytes()); }
fn single(cp: u32, out: &mut Vec<u8>) {
    let mut lo=0; let mut hi=COUNT;
    while lo<hi { let mid=(lo+hi)/2; if u32_at(mid*10)<cp {lo=mid+1;} else {hi=mid;} }
    if lo<COUNT && u32_at(lo*10)==cp {
        let offset=u32_at(lo*10+4) as usize;
        let count=u16::from_le_bytes(DATA[lo*10+8..lo*10+10].try_into().unwrap()) as usize;
        for i in offset..offset+count { let p=COUNT*10+i*2; emit(out,u16::from_le_bytes([DATA[p],DATA[p+1]])); }
    } else {
        if (0x17000..=0x18aff).contains(&cp) {
            emit(out, 0xfb00); emit(out, 0x8000 | (cp - 0x17000) as u16); return;
        }
        let base = if (0x4e00..=0x9fd5).contains(&cp) || matches!(cp, 0xfa0e | 0xfa0f | 0xfa11 | 0xfa13 | 0xfa14 | 0xfa1f | 0xfa21 | 0xfa23 | 0xfa24 | 0xfa27 | 0xfa28 | 0xfa29) {0xfb40}
            else if (0x3400..=0x4db5).contains(&cp) || (0x20000..=0x2a6d6).contains(&cp)
                || (0x2a700..=0x2b734).contains(&cp) || (0x2b740..=0x2b81d).contains(&cp)
                || (0x2b820..=0x2cea1).contains(&cp) {0xfb80} else {0xfbc0};
        emit(out,base + (cp >> 15) as u16); emit(out,0x8000 | (cp & 0x7fff) as u16);
    }
}
pub(super) fn primary(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len()*2);
    if value.is_ascii() {
        for b in value.bytes() { for &w in ASCII[b as usize] { emit(&mut out,w); } }
        return out;
    }
    // MySQL's base 0900 collation uses code-point weights without the
    // DUCET contraction table or general normalization. Hangul syllables
    // have algorithmic Jamo expansions in the generated MySQL UCA table.
    for ch in value.chars() {
        let cp = ch as u32;
        if (0xac00..=0xd7a3).contains(&cp) {
            let index = cp - 0xac00;
            single(0x1100 + index / 588, &mut out);
            single(0x1161 + (index % 588) / 28, &mut out);
            if index % 28 != 0 { single(0x11a7 + index % 28, &mut out); }
        } else { single(cp, &mut out); }
    }
    out
}
"""
)
print(len(singles), len(multis), len(out))

subprocess.run(["rustfmt", "--edition", "2021", "src/charset_uca9.rs"], check=True)
