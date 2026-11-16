# `SHOW AI CONNECTIONS`

## Description

Reads AI object metadata.

## Syntax

```ebnf
<show-ai-connection> ::= 'SHOW AI CONNECTIONS'
```

## Behavior, Privileges, and Results

Returns two string columns, `Name` and `Definition`. Definition is diagnostic catalog text/JSON, not guaranteed replayable CREATE SQL. Reading does not invoke a model or modify the catalog. Requires global AI_ADMIN. Secret values remain redacted, including in SHOW CREATE AI SECRET; encrypted credential bytes are not returned. Names are instance-scoped; a schema-qualified Secret or Connection name is rejected.

## Example

```sql
SHOW AI CONNECTIONS;
```

## See Also

- [AI administration and visibility](../../ai.md#administration-and-visibility)
- [AI and HTTP index](README.md)
