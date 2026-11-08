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

## Parameter and execution reference

Requires AI_USE on the selected model as well as any table/column SELECT
permissions used to obtain input. Model names are literals, bound parameters or
user variables, fixed per call position rather than selected from a data row.

A SQL string is a prompt, even if it happens to contain JSON text. A native JSON array is a message history with system/user/assistant text messages and at least one user message. Use JSON_ARRAY/JSON_OBJECT or an explicit JSON conversion to construct it.

Omitted or SQL NULL options means defaults; a native JSON null option is invalid. See [call options](../../ai.md#calling-functions) for temperature, token limits, timeout, retry and structured-output capability checks. Unknown/conflicting options are errors.

Execution is limited to the outermost final SELECT projection or a single user-variable SET assignment. Explicit transactions, autocommit=0, DML, views and stored programs cannot contain model calls. Provider work/charges cannot be undone by local cancellation. See [execution boundaries](../../ai.md#execution-boundaries).

## Detail response

The common envelope includes ok, model, model_revision, provider_model,
request_id, provider_request_id, attempts, cache_hit, timing_ms, usage,
finish_reason and error. Provider-dependent IDs, timing and usage can be JSON null;
missing usage is not invented as zero.

For generation, text contains the generated text and output_json is populated only after a requested structured response passes local validation.

A successful call has ok=true and error=null. With explicit on_error='return', a
convertible provider/call failure has ok=false and structured error data. Access
denial, invalid SQL/context, resource limits, KILL and the SQL deadline remain SQL
errors. Inspect ok and the final SQL statement status before accepting results.
The complete provider, error and budget contracts are in the shared AI guide.
