# `ALTER RESOURCE GROUP`

## Description

mydb does not currently support RESOURCE GROUP; valid statements return error 1235. The syntax below exists to recognize these statements and return an explicit error.

## Syntax

```ebnf
<alter-resource-group> ::= 'ALTER RESOURCE GROUP' <identifier>
                           { 'VCPU' [ '=' ] <vcpu-list>
                           | 'THREAD_PRIORITY' [ '=' ] <signed-integer>
                           | 'ENABLE' | 'DISABLE' }
```

## Behavior and Limitations

- Valid statements uniformly return 1235 (ER_NOT_SUPPORTED_YET), including the case where the target name does not exist.
- No names, attributes, or dedicated catalog entries are recorded; no compatibility success or warning is produced.
- Syntax errors still return parse errors.

## Examples

```sql
ALTER RESOURCE GROUP batch VCPU=4-7 ENABLE;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
