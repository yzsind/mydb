#!/usr/bin/env python3
"""Negative controls for the physical-version oracle used by recovery gates."""

from pathlib import Path
import struct
import tempfile
import unittest
import zlib

from storage_oracle import heap_versions, wal_frames


def checksum(page):
    struct.pack_into("<I", page, 16, 0)
    struct.pack_into("<I", page, 16, zlib.crc32(page))
    return page


def fixture():
    page = bytearray(16384)
    struct.pack_into("<IH", page, 0, 0x4D485032, 15)
    struct.pack_into("<HHH", page, 20, 2, 74, 16370)
    # Nonzero owner metadata occupies the former slot-directory position.
    page[32:64] = bytes(range(1, 33))
    struct.pack_into("<HHB", page, 64, 16370, 12, 0)
    struct.pack_into("<HHB", page, 69, 16382, 2, 0)
    page[16370:16382] = bytes([9, 4]) + struct.pack("<Q", 123) + bytes(2)
    page[16382:16384] = bytes([9, 0])
    return checksum(page)


class HeapVersionOracleTests(unittest.TestCase):
    def setUp(self):
        Path("/tmp/mydb").mkdir(exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(dir="/tmp/mydb")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.path = self.root / "table.heap"

    def inspect(self, page):
        self.path.write_bytes(page)
        return heap_versions(self.root, 2)

    def test_current_owner_header_and_transaction_versions(self):
        self.assertEqual(self.inspect(fixture()), ("table.heap", {"0:0": (0, 123), "0:1": (0, 0)}))

    def test_crc_damage_is_rejected(self):
        page = fixture()
        page[-1] ^= 1
        with self.assertRaisesRegex(AssertionError, "CRC"):
            self.inspect(page)

    def test_unsupported_page_version_is_rejected(self):
        page = fixture()
        struct.pack_into("<H", page, 4, 65535)
        with self.assertRaisesRegex(AssertionError, "unsupported heap page version"):
            self.inspect(checksum(page))

    def test_out_of_page_slot_is_rejected(self):
        page = fixture()
        struct.pack_into("<HHB", page, 64, 16380, 12, 0)
        with self.assertRaisesRegex(AssertionError, "invalid row bounds"):
            self.inspect(checksum(page))

    def test_short_transaction_id_is_rejected(self):
        page = fixture()
        page[-1] = 4
        with self.assertRaisesRegex(AssertionError, "truncated transaction ID"):
            self.inspect(checksum(page))

    def test_unknown_page_magic_is_not_silently_skipped(self):
        page = fixture()
        struct.pack_into("<I", page, 0, 0xDEADBEEF)
        with self.assertRaisesRegex(AssertionError, "unknown heap page magic"):
            self.inspect(checksum(page))


class WalFrameOracleTests(unittest.TestCase):
    def setUp(self):
        Path("/tmp/mydb").mkdir(exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(dir="/tmp/mydb")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        folder = self.root / "system/wal"
        folder.mkdir(parents=True)
        self.path = folder / "wal-0000000000000001.log"
        header = bytearray(b"MYDBWAL1" + struct.pack("<IQQQ", 40, 1, 1, 0) + bytes(range(16)) + struct.pack("<II", 64, 0))
        header += struct.pack("<I", zlib.crc32(header))
        payload = b"test payload"
        frame = struct.pack("<IIQQQHHQ", 0x4D57414C, len(payload), 8, 7, 5, 11, 0, 123)
        self.raw = header + frame + struct.pack("<I", zlib.crc32(frame + payload)) + payload

    def inspect(self, raw):
        self.path.write_bytes(raw)
        return wal_frames(self.root)

    def test_current_segment_header_and_zero_preallocation(self):
        frames = self.inspect(self.raw + bytes(128))
        self.assertEqual(frames, [{"lsn": 8, "txn": 5, "kind": 11,
                                   "file": self.path.name, "payload": b"test payload"}])

    def test_segment_header_corruption_is_rejected(self):
        self.raw[20] ^= 1
        with self.assertRaisesRegex(AssertionError, "WAL header CRC"):
            self.inspect(self.raw)

    def test_frame_payload_corruption_is_rejected(self):
        self.raw[-1] ^= 1
        with self.assertRaisesRegex(AssertionError, "WAL frame CRC"):
            self.inspect(self.raw)

    def test_partial_frame_payload_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "truncated WAL payload"):
            self.inspect(self.raw[:-1])

    def test_partial_frame_header_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "truncated WAL frame header"):
            self.inspect(self.raw[:70])

    def test_unknown_wal_format_is_rejected(self):
        struct.pack_into("<I", self.raw, 8, 65535)
        with self.assertRaisesRegex(AssertionError, "unsupported WAL version"):
            self.inspect(self.raw)


if __name__ == "__main__":
    unittest.main()
