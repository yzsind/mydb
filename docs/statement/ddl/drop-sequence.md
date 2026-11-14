# `DROP SEQUENCE`

## Description

Drops one or more sequence objects.

## Syntax

```ebnf
<drop-sequence> ::= 'DROP SEQUENCE' [ 'IF EXISTS' ]
                    <qualified-name> { ',' <qualified-name> }
```

## Behavior and Limitations

- After the drop, calls such as nextval/currval return an object-not-exist error.
- IF EXISTS turns missing objects into Notes; tables with the same name are not dropped.
- Sequence values already written to business tables are unaffected.

## Privileges, Transactions, and Dependencies

Requires DROP on each sequence and follows the DDL implicit-commit boundary. Session history is tied to stable identity; creating a new sequence with the same name does not restore old CURRVAL/LASTVAL history. OWNED BY lets the owning table/column drop its dependent sequence automatically. Values stored in ordinary rows remain unchanged; defaults or queries calling the removed object fail when resolved. See [sequence state](../../sequences.md).

## Examples

```sql
DROP SEQUENCE invoice_seq, legacy_seq;
DROP SEQUENCE IF EXISTS missing_seq;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE SEQUENCE](../metadata/show-create-sequence.md)
