# `CANCEL DDL JOB`

## Description

Requests cancellation of a specific job before an online DDL performs its final catalog swap.

## Syntax

```ebnf
<cancel-ddl> ::= 'CANCEL DDL JOB' <unsigned-integer>
```

## Behavior, Privileges, and Limitations

- Job IDs come from information_schema.mydb_ddl_jobs.
- Only cancellable phases accept the request; jobs that are finished, already swapped, or nonexistent return an explicit status error.
- Cancellation cleans up the shadow generation and reservations; it does not roll back other committed DDL.
- COPY, online BTREE builds, online column/constraint validation, and supported partition jobs expose cancellable phases. An accepted cancellation prevents publication even with an empty final delta; requests after the finalization gate are rejected.
- Completed results remain in bounded in-memory history with `CANCEL_ALLOWED=0`. Legacy special-index progress without a job ID does not imply cancellation support.

## Examples

```sql
SELECT * FROM information_schema.mydb_ddl_jobs;
CANCEL DDL JOB 42;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
