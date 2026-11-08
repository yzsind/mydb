# `DROP RESOURCE GROUP`

## Description

mydb does not currently support RESOURCE GROUP; valid statements return error 1235. The syntax below exists to recognize these statements and return an explicit error.

## Syntax

```ebnf
<drop-resource-group> ::= 'DROP RESOURCE GROUP' <identifier> [ 'FORCE' ]
```

## Behavior and Limitations

- With sufficient statement privileges, recognized valid forms return 1235 (ER_NOT_SUPPORTED_YET), including a nonexistent target. Authentication/authorization errors can precede that unsupported error.
- No names, attributes, or dedicated catalog entries are recorded; no compatibility success or warning is produced.
- Syntax errors still return parse errors.

## Examples

```sql
DROP RESOURCE GROUP batch FORCE;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
