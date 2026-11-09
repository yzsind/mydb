# `SHOW CREATE TABLE / VIEW`

## Description

Returns the canonical DDL to recreate a table, temporary table, or view.

## Syntax

```ebnf
<show-create-table> ::= 'SHOW CREATE'
                         ( 'TABLE' | 'VIEW' ) <qualified-name>
```

## Behavior, Privileges, and Results

- Table output includes columns, constraints, indexes, partitions, and saved table options; saved compatibility metadata is rendered in normalized DDL.
- View output includes algorithm, definer, SQL SECURITY, column names, and check option.
- Requires the privilege to view the object definition.

## Result and Visibility Details

Requires SELECT on the resolved object. Definitions are normalized and quoted; saved options are rendered as catalog DDL, not original input text. Views have the distinct four-column SHOW CREATE VIEW shape. Session temporary tables take precedence for table lookup; data is not copied by this statement.

See [ordered columns and wire types](result-columns.md#show-create-table). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW CREATE TABLE orders;
SHOW CREATE VIEW active_customer;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
