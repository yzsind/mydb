"""Independent physical heap-version oracle for local publication gates.

Keep explicit format guards aligned with src/storage/format_versions.rs. Unknown
formats must fail the gate rather than silently weakening its physical checks.
"""

import os
import struct
import zlib


PAGE_BYTES = 16384
HEAP_MAGIC = 0x4D485032
HEAP_META_MAGIC = 0x4D484D31
HEAP_VERSION = 15
ROW_VERSION = 9
HEADER_BYTES = 64
SLOT_BYTES = 5
WAL_VERSION = 40
WAL_HEADER_BYTES = 64
WAL_FRAME_BYTES = 48
UNDO_HEADER_BYTES = 64


def _read_wal_frames(root):
    for path in sorted((root / "system/wal").glob("*.log")):
        raw = path.read_bytes()
        assert len(raw) >= WAL_HEADER_BYTES and raw[:8] == b"MYDBWAL1", (path, "invalid WAL header")
        assert struct.unpack_from("<I", raw, 8)[0] == WAL_VERSION, (path, "unsupported WAL version")
        assert struct.unpack_from("<II", raw, 52) == (WAL_HEADER_BYTES, 0), (path, "invalid WAL header size or flags")
        assert zlib.crc32(raw[:60]) == struct.unpack_from("<I", raw, 60)[0], (path, "WAL header CRC")
        offset = WAL_HEADER_BYTES
        while offset < len(raw):
            if not any(raw[offset:offset + WAL_FRAME_BYTES]):
                break
            assert offset + WAL_FRAME_BYTES <= len(raw), (path, offset, "truncated WAL frame header")
            magic, size, lsn, prev, txn, kind, reserved, timestamp, crc = struct.unpack_from("<IIQQQHHQI", raw, offset)
            assert magic == 0x4D57414C and reserved == 0, (path, offset, "invalid WAL frame header")
            payload = raw[offset + WAL_FRAME_BYTES:offset + WAL_FRAME_BYTES + size]
            assert len(payload) == size, (path, offset, "truncated WAL payload")
            assert zlib.crc32(raw[offset:offset + 44] + payload) == crc, (path, offset, "WAL frame CRC")
            yield ({"lsn": lsn, "txn": txn, "kind": kind, "file": path.name, "payload": payload},
                   memoryview(raw)[offset:offset + WAL_FRAME_BYTES + size])
            offset += WAL_FRAME_BYTES + size


def wal_frames(root):
    """Read physical frames without interpreting or decompressing their payloads."""
    return sorted((frame for frame, _ in _read_wal_frames(root)), key=lambda frame: frame["lsn"])


def wal_frame_bytes(root, first_lsn, last_lsn=None):
    selected = [(frame["lsn"], raw) for frame, raw in _read_wal_frames(root)
                if frame["lsn"] >= first_lsn and (last_lsn is None or frame["lsn"] <= last_lsn)]
    selected.sort(key=lambda item: item[0])
    return b"".join(raw for _, raw in selected)


def truncate_undo_tail(path):
    """Discard only an owned fixture's undo body, retaining its valid identity."""
    with path.open("r+b") as output:
        header = output.read(UNDO_HEADER_BYTES)
        assert len(header) == UNDO_HEADER_BYTES and header[:8] == b"MYUNDO1\0", (path, "invalid undo header")
        version, segment = struct.unpack_from("<II", header, 8)
        assert version == 3, (path, "unsupported undo version", version)
        assert segment == int(path.stem.removeprefix("undo-"), 16), (path, "undo segment identity")
        assert zlib.crc32(header[:60]) == struct.unpack_from("<I", header, 60)[0], (path, "undo header CRC")
        original = output.seek(0, 2)
        output.truncate(UNDO_HEADER_BYTES)
        output.flush()
        os.fsync(output.fileno())
    return original


def heap_versions(root, count):
    matches = []
    for path in sorted(root.rglob("*.heap")):
        raw = path.read_bytes()
        assert len(raw) % PAGE_BYTES == 0, (path, "partial heap page", len(raw))
        rows = {}
        for page_id in range(len(raw) // PAGE_BYTES):
            page = raw[page_id * PAGE_BYTES:(page_id + 1) * PAGE_BYTES]
            if not any(page):
                continue
            magic, version = struct.unpack_from("<IH", page)
            assert magic in (HEAP_MAGIC, HEAP_META_MAGIC), (path, page_id, "unknown heap page magic", magic)
            assert version == HEAP_VERSION, (path, page_id, "unsupported heap page version", version)
            assert zlib.crc32(page[:16] + bytes(4) + page[20:]) == struct.unpack_from("<I", page, 16)[0], (path, page_id, "CRC")
            if magic == HEAP_META_MAGIC:
                assert page_id == 0, (path, page_id, "misplaced heap allocation page")
                continue
            slots, free_start, free_end = struct.unpack_from("<HHH", page, 20)
            directory_end = HEADER_BYTES + slots * SLOT_BYTES
            assert directory_end == free_start <= free_end <= PAGE_BYTES, (path, page_id, "invalid slot directory bounds")
            for slot in range(slots):
                offset, size, flags = struct.unpack_from("<HHB", page, HEADER_BYTES + slot * SLOT_BYTES)
                if not size or flags & 3:
                    continue
                assert size >= 2 and free_end <= offset <= PAGE_BYTES - size, (path, page_id, slot, "invalid row bounds")
                row = page[offset:offset + size]
                assert row[0] != 0xFF, (path, page_id, slot, "forwarded row")
                assert row[0] == ROW_VERSION, (path, page_id, slot, "unsupported row version", row[0])
                if row[1] & 2:
                    continue
                assert not row[1] & 4 or len(row) >= 10, (path, page_id, slot, "truncated transaction ID")
                txn = struct.unpack_from("<Q", row, 2)[0] if row[1] & 4 else 0
                rows[f"{page_id}:{slot}"] = (row[1] & 3, txn)
        if len(rows) == count:
            matches.append((str(path.relative_to(root)), rows))
    assert len(matches) == 1, [(name, len(rows)) for name, rows in matches]
    return matches[0]
