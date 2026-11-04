# Row-level security

Row-level security (RLS) adds row visibility and write checks to normal SQL
privileges. This implementation is under development. The complete security,
recovery, replication, and performance acceptance matrix is still pending;
passing the initial regression tests is not a completed isolation audit.

## Privileges and bypass

An effective global `SUPER` privilege automatically bypasses RLS, including
when it comes from an activated role. Global `ALL PRIVILEGES ON *.*` includes
`SUPER`; database-level and table-level `ALL` do not. An account name or table
creator identity does not grant a bypass.

`BYPASS_RLS` permits a non-SUPER account to bypass row checks. Both forms of
bypass preserve ordinary SQL privilege, constraint, and transaction checks.
Revocation and role changes are rechecked for subsequent statements, including
prepared statements. Trusted internal replication replay has a Rust-only
bypass capability, not a SQL switch.

Two additional global privileges are separate from SUPER:

| Privilege | Purpose |
| --- | --- |
| `ROW_SECURITY_ADMIN` | Create, alter, drop, enable, disable, and inspect policies |
| `RLS_CONTEXT_ADMIN` | Set or reset the trusted connection context |

Bootstrap root receives these administrative privileges explicitly. Granting
SUPER alone does not grant them.

## Policies

```sql
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_access ON accounts
    AS PERMISSIVE FOR ALL TO app_role
    USING (tenant_id = CAST(RLS_CONTEXT('tenant_id') AS UNSIGNED))
    WITH CHECK (tenant_id = CAST(RLS_CONTEXT('tenant_id') AS UNSIGNED));

ALTER POLICY tenant_access ON accounts
    USING (tenant_id = 42) WITH CHECK (tenant_id = 42);
SHOW CREATE POLICY tenant_access ON accounts;
SELECT * FROM information_schema.mydb_row_security_policies;
ALTER POLICY tenant_access ON accounts RENAME TO account_access;
DROP POLICY account_access ON accounts;
ALTER TABLE accounts DISABLE ROW LEVEL SECURITY;
```

Policies default to `PERMISSIVE`, `ALL`, and `PUBLIC`. Supported commands are
`ALL`, `SELECT`, `INSERT`, `UPDATE`, and `DELETE`. Account targets accept the
existing account syntax, including explicit hosts.

For each required command, applicable permissive policies combine with OR and
restrictive policies combine with AND. At least one permissive policy must
authorize access. An enabled table with no applicable policy denies access.
Different required command categories combine with AND.

`USING` filters existing rows. FALSE and NULL do not authorize a row.
`WITH CHECK` checks new row contents and rejects the statement on failure.
For ALL and UPDATE, an omitted WITH CHECK inherits USING. An omitted expression
otherwise means TRUE. INSERT cannot have USING; SELECT and DELETE cannot have
WITH CHECK. Policies do not grant object or column privileges.

Policy expressions cannot use parameters, mutable session variables, external
effects, or write operations. Directly recursive policy queries are rejected.
Subqueries must have the necessary referenced-table/column privileges. Policy
account references prevent the account from being dropped or renamed without
first removing the dependency. Target-column references use stable column IDs,
so renaming a referenced column preserves its policy. Dropping such a column is
rejected. Tables referenced by policy subqueries cannot be dropped or renamed
until their dependent policies are altered or removed. This includes
`RENAME TABLE` and `ALTER TABLE ... RENAME`; a multi-table rename is rejected
before changing any names. Renaming the table that owns a policy preserves it.
Each clause retains its creation-time SQL mode, type-related session
settings and connection collation; ALTER replaces these settings only for a
changed clause. External-column and function dependency rebinding still needs
complete acceptance coverage.

Policy administration uses the existing MySQL-style DDL implicit-commit
behavior. Its dictionary publication is atomic; it does not rewrite business
table data. Policy DDL is emitted as query events when MySQL binlogging is
enabled. MySQL receivers do not understand this extension; use native mydb
replication when policy enforcement must follow the replica.

Table owners have no implicit exemption, so FORCE/NO FORCE ROW LEVEL SECURITY
is rejected instead of silently doing nothing.

## Connection pools

```sql
GRANT RLS_CONTEXT_ADMIN ON *.* TO app_service;

BEGIN;
SET LOCAL RLS CONTEXT 'tenant_id' = '42';
SELECT * FROM accounts;
COMMIT;
```

RLS_CONTEXT reads nullable text. Missing keys return NULL. Cast values to the
intended type in the policy. A context assignment accepts a literal or prepared
parameter. LOCAL requires an explicit transaction and is restored at transaction
completion or savepoint rollback.

Session-scoped settings use `SET RLS CONTEXT`. `RESET RLS CONTEXT 'tenant_id'`
clears a key, and `RESET RLS CONTEXT` clears all keys. Connection reset and
reauthentication start with an empty context. Stored routines and triggers may
not change context through these statements.

The application must authenticate the business user and verify the tenant
before setting context. The database trusts the account holding
RLS_CONTEXT_ADMIN; this interface does not authenticate a tenant claim supplied
by that account. Transaction-local context is recommended for pooled connections.

## Views and execution

Expanded read views retain a separate security identity for each reference.
DEFINER uses the defining account and its effective roles; INVOKER inherits
the invoking identity. Both inherit tenant context from the original connection.
The current development implementation retains a non-merge boundary around
RLS view bodies. Locking reads apply SELECT and UPDATE policies to locked
references before user predicates and expressions. Multi-table DML filters
each source and target reference before evaluating joins, WHERE or SET, then
rechecks protected targets after locking. Complete view and concurrency
acceptance are still pending.

A statement captures immutable authorization and policy-version roots. Nested
queries, DEFINER role activation and workers inherit those roots; changes take
effect on the following statement, including the next child of a multi-statement
packet. A late-discovered relation whose old security metadata cannot be proven
returns a retryable SQLSTATE 40001 error rather than using newer permissions.

EXPLAIN shows ordinary users the position of the security boundary. Full plans,
policy expressions, dependencies and detailed execution counters require
ROW_SECURITY_ADMIN. SUPER alone does not grant that diagnostic privilege.

Simple policies can use normal index and batch-filter paths. Integer casts of
RLS_CONTEXT with a literal key use deferred index bounds, so a reusable plan
does not retain the first tenant value. A restricted, type-proven class of
EXISTS/NOT EXISTS policies can use the existing semi/anti join kernels.
These optimizations do not establish the required performance thresholds.

## Writes and operational behavior

Write checks inspect coerced/generated final values. RLS violations are not
converted to warnings by IGNORE. INSERT batches, conflicting inserts, REPLACE,
RETURNING, and FK cascades participate in statement rollback. Referential
integrity probes still see the full table; unauthorized cascaded modifications
fail rather than silently skipping child rows.

Whole-table changes to an enabled table require SUPER or BYPASS_RLS together
with the normal DDL privileges. In the current development implementation this
also applies conservatively to structural ALTER actions; index-only actions
and column comments retain their ordinary privilege checks.

`SET SESSION row_security = OFF` does not disable enforcement. A statement
that would need RLS instead reports an error. SUPER and BYPASS_RLS retain their
normal bypass, allowing an explicitly privileged full export.

Unique and foreign-key constraints remain global. Constraint errors and timing
can reveal existence information; RLS does not promise to hide every such
side channel. Existing RC/RR read-view semantics still apply to authorization
data consulted by a policy.

RLS metadata is stored in transactional dictionary tables and is included in
the development storage format. See [Storage Format Versions](storage-format-versions.md)
before opening an existing data directory. No silent format migration is provided.

## Validation status

Development and test evidence is recorded in the repository's internal
`rls-design.md` and `rls-validation.md`. Complete view identity and concurrency
coverage, dependency rebinding, all special scan/write paths, and the required
performance thresholds still need acceptance coverage.
This page does not claim completion of those requirements.
