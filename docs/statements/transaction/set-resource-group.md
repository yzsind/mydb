# `SET RESOURCE GROUP`

## Description

mydb does not currently support RESOURCE GROUP; valid statements return error 1235. The syntax below is recognized in order to provide a clear error.

## Syntax

```ebnf
<set-resource-group> ::= 'SET RESOURCE GROUP' <identifier>
                         [ 'FOR' <thread-id> { ',' <thread-id> } ]
```

## Behavior and Limitations

- Valid statements uniformly return 1235 (ER_NOT_SUPPORTED_YET), including the case where the target name does not exist.
- No names, attributes, or dedicated catalog display are registered; no compatibility success or warning is produced.
- Syntax errors still return a parse error.

## Examples

```sql
SET RESOURCE GROUP batch FOR 101, 102;
SET RESOURCE GROUP interactive;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
