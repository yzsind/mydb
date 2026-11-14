# `SET`

## Description

Modifies system variables, user-defined variables, or session assignment targets.

## Syntax

```ebnf
<set> ::= 'SET' <set-assignment> { ',' <set-assignment> }
<set-assignment> ::= [ 'GLOBAL' | 'SESSION' | 'LOCAL'
                       | 'PERSIST' | 'PERSIST_ONLY' ]
                     <identifier> [ ':' ] '=' <expr>
                     | <user-variable> [ ':' ] '=' <expr>
                     | <system-variable> '=' <expr>
```

## Behavior and Limitations

- The validity of GLOBAL/SESSION/LOCAL/PERSIST/PERSIST_ONLY is determined by the variable registry metadata; read-only variables or scope mismatches return an error.
- PERSIST updates both the runtime value and mysqld-auto.cnf; PERSIST_ONLY updates only the persisted value. Lowering a memory target at runtime requires bounded reclamation first; if it cannot converge, the old value is kept and the operation fails.
- User-defined variables belong to the current connection and can hold NULL and dynamically typed values.
- Variables that affect semantics or plans participate in the plan-cache key; system-variable values are validated before application. User-variable assignments are evaluated and applied left to right immediately; a later error does not restore earlier user-variable values.

## Transaction and Error Details

SESSION/LOCAL and user-variable assignments need no blanket administration privilege. GLOBAL/PERSIST requires SYSTEM_VARIABLES_ADMIN; PERSIST_ONLY additionally requires PERSIST_RO_VARIABLES_ADMIN. Scalar subqueries need source SELECT privileges.

For a system variable with session and global scope, SET SESSION var=DEFAULT reads the current global value; SET GLOBAL var=DEFAULT restores the compiled default. PERSIST writes runtime and mysqld-auto.cnf; PERSIST_ONLY only writes the file. Removing a persisted override requires RESET PERSIST. A session state change is not undone by ordinary ROLLBACK.

```sql
SET @x=0;
SET @x=1, sql_mode='INVALID_MODE'; -- Error 1231.
SELECT @x; -- 1: the earlier user assignment remains.
```

## Examples

```sql
SET @batch_id := 42, sql_mode = 'STRICT_TRANS_TABLES';
SET GLOBAL max_connections = 200;
SET PERSIST mydb_buffer_pool_pages = 8192;
```

## See Also

- [Transaction and session index](README.md)
- [Syntax conventions](../syntax-conventions.md)
