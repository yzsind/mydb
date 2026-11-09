# `UUID`

## Description

Generates a UUID in the standard text format.

## Syntax

```sql
UUID()
```

## Parameters

This function takes no arguments.

## Return Value

Returns a newly generated version-1 UUID as hyphenated text. Each evaluation can differ; this is not an authorization secret or a source of cryptographic random bytes.

## Examples

```sql
SELECT UUID();
```

Result: returns a standard UUID text string; the result may differ on each call.

## Notes

- The result depends on the current session, server state, execution time, privileges, or the external environment; do not treat it as a constant reusable across statements.
- Argument-count errors and invalid argument types are SQL errors; consult the function-specific rules above.

## Related Functions

[`UUID_SHORT`](uuid_short.md), [`UUID_TO_BIN`](uuid_to_bin.md), [`BIN_TO_UUID`](bin_to_uuid.md), [`IS_UUID`](is_uuid.md).
