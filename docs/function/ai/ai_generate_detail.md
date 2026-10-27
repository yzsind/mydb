# `AI_GENERATE_DETAIL`

## Description

Calls a configured model with prompt or JSON text-message array. Returns JSON with generated text, optional output_json, model revision, usage, timing and status.

## Syntax

```sql
AI_GENERATE_DETAIL(model, input [, options])
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
SELECT AI_GENERATE_DETAIL('test.local_chat', 'Summarize MVCC.', JSON_OBJECT('on_error','return'));
```
