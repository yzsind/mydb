# `CANCEL DDL JOB`

## Description

Requests cancellation of a specific job before an online DDL performs its final catalog swap.

## Syntax

```ebnf
<cancel-ddl> ::= 'CANCEL DDL JOB' <unsigned-integer>
```

## Behavior, Privileges, and Limitations

- Job IDs come from information_schema.MYDB_DDL_JOBS.
- Only cancellable phases accept the request; jobs that are finished, already swapped, or nonexistent return an explicit status error.
- Cancellation cleans up the shadow generation and reservations; it does not roll back other committed DDL.
- COPY, online BTREE builds, online column/constraint validation, and supported partition jobs expose cancellable phases. An accepted cancellation prevents publication even with an empty final delta; requests after the finalization gate are rejected.
- Completed results remain in bounded in-memory history with `CAN_CANCEL=0`. Legacy special-index progress without a job ID does not imply cancellation support.

## Privileges, Transaction, and Result Details

Requires global SYSTEM_VARIABLES_ADMIN. Returning OK means cancellation was accepted before the finalization gate, not that the worker has completed cleanup. Poll information_schema.MYDB_DDL_JOBS for its terminal state/error. Job IDs identify this process's bounded recent history, not a durable backup ID. A completed/published/unknown job is rejected rather than undone.

## Examples

```sql
SELECT * FROM information_schema.MYDB_DDL_JOBS;
CANCEL DDL JOB 42;
```

## See Also

- [Maintenance and administration index](README.md)
- [Syntax conventions](../syntax-conventions.md)
