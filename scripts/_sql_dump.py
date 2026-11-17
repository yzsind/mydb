"""Stream semicolon-terminated dump statements without losing executable comments."""
import re


def statements(source):
    buffer = []
    quote = None
    block = None
    escaped = False
    for line in source:
        if quote is None and block is None and line.lstrip().upper().startswith('DELIMITER '):
            raise ValueError('DELIMITER/routine dumps are unsupported; export tables and views separately')
        index = 0
        while index < len(line):
            char = line[index]
            pair = line[index:index + 2]
            if block == 'comment':
                if pair == '*/':
                    block = None; buffer.append(' '); index += 2
                else:
                    index += 1
                continue
            if quote:
                buffer.append(char)
                if escaped:
                    escaped = False
                elif char == '\\':
                    escaped = True
                elif char == quote:
                    if index + 1 < len(line) and line[index + 1] == quote:
                        buffer.append(quote); index += 1
                    else:
                        quote = None
                index += 1
                continue
            if block == 'executable' and pair == '*/':
                block = None; buffer.append(' '); index += 2
            elif pair == '/*':
                buffer.append(' ')
                if line[index:index + 3] == '/*!':
                    block = 'executable'
                    marker = re.match(r'/\*!\d*\s*', line[index:])
                    index += len(marker[0])
                else:
                    block = 'comment'; index += 2
            elif char == '#' or (pair == '--' and (index + 2 == len(line) or line[index + 2].isspace())):
                buffer.append('\n')
                break
            elif char in "'\"`":
                quote = char; buffer.append(char); index += 1
            elif char == ';':
                statement = ''.join(buffer).strip()
                buffer.clear()
                if statement:
                    yield statement
                index += 1
            else:
                buffer.append(char); index += 1
    if quote or block or ''.join(buffer).strip():
        raise ValueError('truncated or unterminated SQL dump statement')
