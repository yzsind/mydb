# Data Manipulation Statements

| Statement | Description |
| --- | --- |
| [INSERT](insert.md) | Single-row, multi-row, SET, INSERT SELECT, and conflict updates |
| [REPLACE](replace.md) | Deletes conflicting rows, then inserts the new row |
| [UPDATE](update.md) | Single-table and joined updates, ORDER/LIMIT, and RETURNING |
| [DELETE](delete.md) | Single-table and multi-table deletes, ORDER/LIMIT, and RETURNING |
| [LOAD DATA](load-data.md) | Bulk-loads text from a server-side or LOCAL file |
| [LOAD XML](load-xml.md) | Bulk-loads from XML row elements |

All DML uses the [statement failure and privilege rules](../execution-contracts.md). [RETURNING](../../dml-returning.md) is a mydb extension; IGNORE downgrades only eligible row errors. See [shared fixtures](../examples.md) for independent runnable examples and [type conversion](../../data-types.md#12-type-conversion-and-sql_mode) for strict mode.
