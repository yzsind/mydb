# `AI_GENERATE`

## Description

Calls a configured model with prompt or JSON text-message array. Returns UTF-8 generated text.

## Syntax

```sql
AI_GENERATE(model, input [, options])
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
SELECT AI_GENERATE('test.local_chat', 'Summarize MVCC.');
```

## Parameter and execution reference

Requires AI_USE on the selected model as well as any table/column SELECT
permissions used to obtain input. Model names are literals, bound parameters or
user variables, fixed per call position rather than selected from a data row.

A SQL string is a prompt, even if it happens to contain JSON text. A native JSON array is a message history with system/user/assistant text messages and at least one user message. Use JSON_ARRAY/JSON_OBJECT or an explicit JSON conversion to construct it.

Omitted or SQL NULL options means defaults; a native JSON null option is invalid. See [call options](../../ai.md#calling-functions) for temperature, token limits, timeout, retry and structured-output capability checks. Unknown/conflicting options are errors.

Execution is limited to the outermost final SELECT projection or a single user-variable SET assignment. Explicit transactions, autocommit=0, DML, views and stored programs cannot contain model calls. Provider work/charges cannot be undone by local cancellation. See [execution boundaries](../../ai.md#execution-boundaries).
