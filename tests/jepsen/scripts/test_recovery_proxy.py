"""Fault targeting must work for negotiated Connector/J query attributes."""
import socket
import threading
import unittest

from run_recovery import is_commit, packet


def wire(payload):
    return len(payload).to_bytes(3, 'little') + b'\x00' + payload


class PacketTests(unittest.TestCase):
    def test_commit_with_and_without_empty_query_attributes(self):
        for sql in (b'COMMIT', b'commit', b' COMMIT; '):
            for attributes in (b'', b'\x00\x01'):
                self.assertTrue(is_commit(wire(b'\x03' + attributes + sql)))
        for payload in (b'\x03ROLLBACK', b'\x03SELECT \'COMMIT\'',
                        b'\x03COMMITMENT', b'\x16COMMIT', b'\x03\x00\x01UPDATE t SET a=1'):
            self.assertFalse(is_commit(wire(payload)))

    def test_fragmented_and_coalesced_packets(self):
        first, second = wire(b'\x03\x00\x01COMMIT'), wire(b'\x00')
        left, right = socket.socketpair()
        with left, right:
            left.settimeout(2)
            def send():
                for byte in first + second:
                    right.sendall(bytes([byte]))
            thread = threading.Thread(target=send)
            thread.start()
            self.assertEqual(first, packet(left))
            self.assertEqual(second, packet(left))
            thread.join(timeout=2)

    def test_truncated_packet_is_an_error(self):
        left, right = socket.socketpair()
        with left, right:
            right.sendall(b'\x04\x00\x00\x00ab')
            right.shutdown(socket.SHUT_WR)
            with self.assertRaises(EOFError):
                packet(left)


if __name__ == '__main__':
    unittest.main()
