# `CREATE RESOURCE GROUP`

## Description

mydb does not currently support RESOURCE GROUP; valid statements return error 1235. The syntax below exists to recognize these statements and return an explicit error.

## Syntax

```ebnf
<create-resource-group> ::= 'CREATE RESOURCE GROUP' <identifier>
                            'TYPE' [ '=' ] ( 'SYSTEM' | 'USER' )
                            [ 'VCPU' [ '=' ] <vcpu-list> ]
                            [ 'THREAD_PRIORITY' [ '=' ] <signed-integer> ]
                            [ 'ENABLE' | 'DISABLE' ]
```

## Behavior and Limitations

- Valid statements uniformly return 1235 (ER_NOT_SUPPORTED_YET), including the case where the target name does not exist.
- No names, attributes, or dedicated catalog entries are recorded; no compatibility success or warning is produced.
- Syntax errors still return parse errors.

## Examples

```sql
CREATE RESOURCE GROUP batch TYPE=USER VCPU=0-3 THREAD_PRIORITY=-5 DISABLE;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
