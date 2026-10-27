# `AI_EMBED_DETAIL`

## Description

Calls a configured model with one nonempty text string or a nonempty JSON array of text strings. Returns JSON with ordered embeddings, dimensions, model revision, usage and status.

## Syntax

```sql
AI_EMBED_DETAIL(model, input [, options])
```

## Arguments and Errors

`model` is a literal, placeholder, or user variable naming a model object.
`options` is a JSON object or JSON text; omitted or SQL NULL options use defaults.
SQL NULL input makes no HTTP request after non-NULL model resolution and authorization.
Invalid inputs, unsupported capabilities, provider failures, and budget violations
are errors. DETAIL functions alone accept explicit `on_error='return'` for
convertible call errors. Query-level errors and access denials remain SQL errors.

Only final SELECT projections and single user-variable SET assignments support
external calls. The model capability, encrypted Secret, transport, retry, cancellation,
and transactional contracts are defined in [LLM and Embedding Calls](../../ai.md).

## Example

The example requires the local models created in the setup guide.

```sql
SELECT AI_EMBED_DETAIL('test.local_embed', JSON_ARRAY('first document','second document'));
```
