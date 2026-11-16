# `SHOW AI MODELS`

## Description

Reads AI object metadata.

## Syntax

```ebnf
<show-ai-model> ::= 'SHOW AI MODELS' [ 'FROM' <identifier> ]
```

## Behavior, Privileges, and Results

Returns two string columns, `Name` and `Definition`. Definition is diagnostic catalog text/JSON, not guaranteed replayable CREATE SQL. Reading does not invoke a model or modify the catalog. AI_ADMIN sees full metadata; other accounts see only models authorized by global or model-specific AI_USE, with a reduced non-sensitive definition. FROM filters by schema. Model names are schema-scoped; an unqualified name uses the current database.

## Example

```sql
SHOW AI MODELS;
```

## See Also

- [AI administration and visibility](../../ai.md#administration-and-visibility)
- [AI and HTTP index](README.md)
