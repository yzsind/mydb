# Row Security Statements

Row security is an additional authorization boundary on persistent user base tables. It does not replace ordinary table/column privileges. Its current development and acceptance limits are stated in [Row-level security](../../row-level-security.md).

| Statement | Purpose |
| --- | --- |
| [CREATE POLICY](create-policy.md) | Defines command, roles and row predicates |
| [ALTER POLICY](alter-policy.md) | Changes predicates/roles or renames a policy |
| [DROP POLICY](drop-policy.md) | Removes a policy |
| [SHOW CREATE POLICY](show-create-policy.md) | Reads canonical policy SQL |
| [ALTER TABLE ENABLE/DISABLE ROW LEVEL SECURITY](alter-row-security.md) | Switches enforcement for a table |
| [SET RLS CONTEXT](set-rls-context.md) | Sets trusted session or transaction context |
| [RESET RLS CONTEXT](reset-rls-context.md) | Clears context keys |

Policy management needs ROW_SECURITY_ADMIN, context changes need RLS_CONTEXT_ADMIN, and bypass needs SUPER or BYPASS_RLS. SUPER alone does not grant the two management capabilities. Enabled tables with no applicable permissive policy deny rows. See [complete policy composition and dependencies](../../row-level-security.md#policies).
