# `ALTER SEQUENCE`

## Description

Modifies sequence options, restarts the current value, or renames a sequence.

## Syntax

```ebnf
<alter-sequence> ::= 'ALTER SEQUENCE' [ 'IF EXISTS' ] <qualified-name>
                     { <sequence-option> }
                     [ 'RESTART' [ 'WITH' <signed-integer> ] ]
                     [ 'RENAME TO' <identifier> ]
```

The `<sequence-option>` production is defined in [CREATE SEQUENCE](create-sequence.md),
including the supported integer backing types, CACHE, bounds, CYCLE and
`OWNED BY table.column|NONE`. An owner is a table/column dependency, not an account.

## Behavior and Limitations

- Only the options given explicitly are modified; omitted items keep their original values.
- The RESTART value must satisfy the new bounds; renaming and option modifications commit as a single catalog operation.
- RESTART publishes a new physical counter identity and invalidates previously reserved session CACHE blocks. The logical sequence identity and the session's CURRVAL remain unchanged until its next NEXTVAL.
- RENAME is limited to the same database. A differently qualified target is rejected instead of silently discarding its database name.
- Dropping an owning table or column drops the dependent sequence; `OWNED BY NONE` detaches it. Permanent sequences cannot depend on temporary tables, and cross-database dependency moves are rejected.
- IF EXISTS only handles a missing target. Concurrent nextval and DDL are serialized through sequence/metadata synchronization.

## Examples

```sql
CREATE SEQUENCE invoice_seq START 1000;
ALTER SEQUENCE invoice_seq INCREMENT BY 10 RESTART WITH 2000;
SELECT NEXTVAL('invoice_seq'); -- 2000
ALTER SEQUENCE invoice_seq RENAME TO invoice_number_seq;
DROP SEQUENCE invoice_number_seq;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
