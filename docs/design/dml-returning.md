# DML RETURNING (PostgreSQL 18 Semantics)

mydb provides PostgreSQL 18-style `RETURNING` for `INSERT`, `UPDATE`, and `DELETE`
over the MySQL protocol. Output is sent as an ordinary result set; the storage
format is unchanged.

## Row Images

- The default image is NEW for INSERT/UPDATE and OLD for DELETE.
- `OLD.col`, `NEW.col`, `OLD.*`, and `NEW.*` select the image explicitly;
  `RETURNING WITH (OLD AS o, NEW AS n)` hides the original names that were renamed.
- OLD is NULL for a plain INSERT and NEW is NULL for DELETE; an ODKU conflict update
  provides both the pre- and post-conflict rows. REPLACE may delete multiple
  conflicting rows, so OLD is fixed as NULL.
- A physical no-op UPDATE is still one successful logical update: the row is
  returned, but the existing affected/matched/changed statistics are unchanged.
- Rows skipped by IGNORE, failed rows, and indirect rows produced by FK cascades are
  not returned.

## Execution and Compatibility Boundaries

The returning projection is evaluated inside the transaction, before commit; a
projection error rolls back the whole statement. The final row after defaults,
auto-increment, coercion, generated columns, and BEFORE triggers is what goes into
NEW. INSERT…SELECT continues to be processed in chunks of 4,096 rows and preserves
output order.

Prepared statements describe the output columns at PREPARE time and reuse the same
AST and metadata at EXECUTE time. An UPDATE/DELETE that actually modifies multiple
target tables is rejected with 1235 for now; this feature does not introduce MERGE,
PostgreSQL ON CONFLICT, UPDATE FROM, or data-modifying CTEs.
