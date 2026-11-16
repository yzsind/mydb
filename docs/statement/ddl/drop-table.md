# `DROP TABLE`

## Description

Drops one or more persistent or temporary tables.

## Syntax

```ebnf
<drop-table> ::= 'DROP' [ 'TEMPORARY' ] 'TABLE'
                 [ 'IF EXISTS' ] <table-list>
                 [ 'RESTRICT' | 'CASCADE' ]
```

## Behavior and Limitations

- Requires the DROP privilege and acquires exclusive metadata locks; missing objects produce an error or a Note depending on IF EXISTS.
- TEMPORARY resolves only session temporary tables and never accidentally drops a persistent table of the same name.
- Foreign-key dependencies are checked by the storage drop path. RESTRICT/CASCADE are accepted spellings but do not select a separate cascading-drop algorithm; CASCADE does not authorize deleting dependent foreign-key tables.
- The drop is DDL and cannot be rolled back. By default, persistent tables enter the [recycle bin](../../recycle-bin.md); after permanent purge, recovery relies on backups/PITR.

## Privileges, Transactions, and Dependencies

Explicit DROP TEMPORARY TABLE searches only the session namespace and does not implicitly commit, though the successful drop itself is not restored by ordinary ROLLBACK. Ordinary DROP TABLE first resolves temporary shadowing but still implicitly commits even if every resolved object is temporary. Mixed persistent/temporary targets retain those distinct namespaces.

DROP requires the target DROP privilege. Referencing foreign keys and policy/sequence dependencies are checked; CASCADE does not disable those checks. IF EXISTS changes missing-object diagnostics, not system-table protections. Physical cleanup can follow publication; success does not wait for every retired file to disappear.

## Examples

```sql
DROP TABLE old_orders, old_customer;
DROP TEMPORARY TABLE IF EXISTS tmp_result;
```

## See Also

- [DDL category index](README.md)
- [Syntax notation](../syntax-conventions.md)
- [SHOW CREATE TABLE](../metadata/show-create-table.md)
