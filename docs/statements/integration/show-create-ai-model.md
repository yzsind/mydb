# `SHOW CREATE AI MODEL`

## Description

Reads AI object metadata.

## Syntax

```ebnf
<show-create-ai-model> ::= 'SHOW CREATE AI MODEL' <qualified-name>
```

## Behavior, Privileges, and Results

Returns two string columns, `Name` and `Definition`. Definition is diagnostic catalog text/JSON, not guaranteed replayable CREATE SQL. Reading does not invoke a model or modify the catalog. Requires global AI_ADMIN. Secret values remain redacted, including in SHOW CREATE AI SECRET; encrypted credential bytes are not returned. Model names are schema-scoped; an unqualified name uses the current database.

## Example

```sql
SHOW CREATE AI MODEL statement_examples.local_chat;
```

## See Also

- [AI administration and visibility](../../ai.md#administration-and-visibility)
- [AI and HTTP index](README.md)
