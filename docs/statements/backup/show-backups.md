# `SHOW MYDB BACKUPS`

## Syntax

```ebnf
<show-backups> ::= 'SHOW MYDB BACKUPS FROM' <string-literal>
```

## Behavior

```sql
SHOW MYDB BACKUPS FROM 'file:///tmp/mydb/repository';
```

Lists live, published baselines in the unified repository catalog. Columns:
Base_ID, Source_ID, Stream_ID, Source_End_LSN and Source_End_Time. Standalone
baselines have no stream. Unpublished staging files and pruned baselines are
excluded. Catalog/identity errors fail explicitly. The operation holds a shared
repository lease and modifies neither the source nor the repository.

See [Backup and Recovery](../../backup-and-recovery.md).

## Repository and Results

Repository is an absolute server-side path, optionally prefixed with `file://`; a relative path is rejected. It is not a client upload or remote object-store URI. Requires global `BACKUP_ADMIN`. Returns the published catalog listing under a shared repository lease; no backup is started. The listing is bounded to 100,000 rows.

## Result Columns

| Column | Wire type | Meaning |
| --- | --- | --- |
| Base_ID | VARCHAR | Published baseline ID |
| Source_ID | VARCHAR | Source identity |
| Stream_ID | VARCHAR | Continuation stream, or NULL for standalone baseline |
| Source_End_LSN | BIGINT | Sealed source end LSN |
| Source_End_Time | BIGINT | Sealed source end time, Unix microseconds |

An initialized repository with no live bases returns these columns with zero rows. Invalid repository identity/catalog state is an error rather than an empty success. Results follow publication catalog order; no SQL ORDER BY/LIMIT extension is defined.
