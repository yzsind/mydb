"""MySQL prepared-statement metadata and binary-row helpers for protocol regressions."""
import struct
from pymysql.constants import COMMAND, FIELD_TYPE, FLAG
from pymysql.protocol import FieldDescriptorPacket


def prepared(cn, sql):
    cn._execute_command(COMMAND.COM_STMT_PREPARE, sql)
    response = cn._read_packet().get_all_data()
    assert response[0] == 0, response
    statement, count, parameters = struct.unpack_from("<IHH", response, 1)
    assert parameters == 0
    for _ in range(count):
        cn._read_packet()
    if count:
        assert cn._read_packet().is_eof_packet()
    try:
        cn._execute_command(
            COMMAND.COM_STMT_EXECUTE, struct.pack("<IBI", statement, 0, 1)
        )
        columns = cn._read_packet().read_length_encoded_integer()
        fields = [
            FieldDescriptorPacket(cn._read_packet().get_all_data(), "utf8")
            for _ in range(columns)
        ]
        assert cn._read_packet().is_eof_packet()
        row = cn._read_packet()
        assert row.read_uint8() == 0
        bitmap = row.read((columns + 9) // 8)
        values = []
        for index, field in enumerate(fields):
            if bitmap[(index + 2) // 8] & (1 << ((index + 2) % 8)):
                values.append(None)
            elif field.type_code == FIELD_TYPE.LONGLONG:
                values.append(
                    struct.unpack(
                        "<Q" if field.flags & FLAG.UNSIGNED else "<q", row.read(8)
                    )[0]
                )
            elif field.type_code in (FIELD_TYPE.DECIMAL, FIELD_TYPE.NEWDECIMAL):
                values.append(row.read_length_coded_string().decode("ascii"))
            else:
                raise AssertionError(("unexpected wire type", field.type_code))
        assert row._position == len(row.get_all_data()), "binary row has trailing bytes"
        assert cn._read_packet().is_eof_packet()
        return dict(
            types=[f.type_code for f in fields],
            scales=[f.scale for f in fields],
            values=values,
        )
    finally:
        cn._execute_command(COMMAND.COM_STMT_CLOSE, struct.pack("<I", statement))


