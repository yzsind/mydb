# `AI_EMBED`

## Description

Calls a configured model with one nonempty text string. Returns a native FLOAT32 VECTOR of the model-configured dimension.

The `mysql` command-line client displays native VECTOR payloads as hex. Run
`SET SESSION vector_result_format = 'TEXT'` once per connection to display
coordinate lists for text-protocol queries, or wrap an individual result in
`VECTOR_TO_STRING`. The default `NATIVE` format and prepared-statement binary
results use the native VECTOR wire type.

## Syntax

```sql
AI_EMBED(model, input [, options])
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
SELECT VECTOR_DIM(AI_EMBED('test.local_embed', 'Database transactions'));

SET SESSION vector_result_format = 'TEXT';
SELECT AI_EMBED('test.local_embed', 'Database transactions') AS embedding;
```
