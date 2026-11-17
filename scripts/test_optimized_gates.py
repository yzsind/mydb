"""Optimization must preserve validation and protocol read/authentication calls."""
import ast
from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parent
GATES = ['auth_protocol_gate.py', 'storage_oracle.py', 'protocol_helpers.py',
         'verify_linux_compat.py', 'verify_expression_protocol.py', 'verify_json_protocol.py']


class OptimizedGateTests(unittest.TestCase):
    def test_runtime_gates_have_no_optimization_sensitive_assertions(self):
        for name in GATES:
            with self.subTest(name=name):
                self.assertFalse(any(isinstance(node, ast.Assert) for node in ast.walk(ast.parse((ROOT / name).read_text()))))

    def test_all_physical_corruption_controls_also_pass_with_optimization(self):
        result = subprocess.run([sys.executable, '-O', str(ROOT / 'test_storage_oracle.py')],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('Ran 20 tests', result.stderr)

    def test_authentication_and_packet_consumption_are_unchanged_with_optimization(self):
        code = r'''
import struct
from unittest.mock import Mock, patch
from pymysql.constants import FIELD_TYPE
from pymysql.protocol import MysqlPacket
import protocol_helpers as protocol
import auth_protocol_gate as auth
field=Mock(type_code=FIELD_TYPE.LONGLONG,flags=0,scale=0)
prepare=MysqlPacket(b'\x00'+struct.pack('<IHH',7,1,0)+bytes(4),'utf8')
count=MysqlPacket(b'\x01','utf8')
row=MysqlPacket(b'\x00\x00'+struct.pack('<q',42),'utf8')
eof=lambda: MysqlPacket(b'\xfe\x00\x00\x02\x00','utf8')
cn=Mock()
cn._read_packet.side_effect=[prepare,Mock(get_all_data=lambda:b'field'),eof(),count,Mock(get_all_data=lambda:b'field'),eof(),row,eof()]
with patch.object(protocol,'FieldDescriptorPacket',return_value=field):
    value=protocol.prepared(cn,'SELECT 42')
    if value['values'] != [42] or cn._read_packet.call_count != 8: raise RuntimeError('packet reads skipped')
for payload,expected in [(b'\xff'+struct.pack('<H',1045),1045),(b'\xff'+struct.pack('<H',999),1045)]:
    with patch.object(auth,'packet_read',return_value=(1,payload)):
        try: accepted=auth.finish_authentication(Mock(),'user','password','sha256_password','tls',b'seed',b'',expected)
        except AssertionError:
            if int.from_bytes(payload[1:3],'little')==expected: raise
        else:
            if int.from_bytes(payload[1:3],'little')!=expected or accepted: raise RuntimeError('wrong error accepted')
'''
        for flags in [[], ['-O']]:
            result = subprocess.run([sys.executable, *flags, '-c', code], cwd=ROOT,
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
