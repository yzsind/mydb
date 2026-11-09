# Data Definition Statements

| Object | Statements |
| --- | --- |
| Database | [CREATE](create-database.md) · [ALTER](alter-database.md) · [DROP](drop-database.md) |
| Table | [CREATE](create-table.md) · [LIKE](create-table-like.md) · [CTAS](create-table-as-select.md) · [ALTER](alter-table.md) · [DROP](drop-table.md) · [TRUNCATE](truncate-table.md) · [RENAME](rename-table.md) |
| Index | [CREATE](create-index.md) · [DROP](drop-index.md) |
| View | [CREATE](create-view.md) · [ALTER](alter-view.md) · [DROP](drop-view.md) |
| Sequence | [CREATE](create-sequence.md) · [ALTER](alter-sequence.md) · [DROP](drop-sequence.md) |
| Tablespace | [CREATE](create-tablespace.md) · [ALTER](alter-tablespace.md) · [DROP](drop-tablespace.md) |

For column types, see [Data Types](../../data-types.md). Triggers, routines, and events are covered in [Stored Programs](../programmatic/README.md).

Policy objects are in [Row Security](../security/README.md); AI objects are in [Integration](../integration/README.md). TABLESPACE pages describe recognition followed by rejection, not storage isolation. Check [privileges and implicit commits](../execution-contracts.md) before grouping DDL with user writes; online DDL can still wait at publication barriers.
