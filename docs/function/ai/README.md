# AI Functions

These functions require configured model objects and explicit model privileges.
See [LLM and Embedding Calls](../../ai.md) for setup, encryption, execution,
limits, and provider contracts.

- [AI_GENERATE](ai_generate.md)
- [AI_GENERATE_DETAIL](ai_generate_detail.md)
- [AI_EMBED](ai_embed.md)
- [AI_EMBED_DETAIL](ai_embed_detail.md)


## Choosing a call and reading results

| Function | Input | Result |
| --- | --- | --- |
| AI_GENERATE | Prompt text or native JSON message array | UTF-8 generated text |
| AI_GENERATE_DETAIL | Same generation input | JSON envelope including text/output_json, usage and status |
| AI_EMBED | One nonempty text | Native FLOAT32 VECTOR |
| AI_EMBED_DETAIL | One text or native JSON array of texts | Ordered JSON data array with embedding metadata |

Configure the models and AI_USE grants in the [setup guide](../../ai.md) first.
These pages do not contact a provider during PREPARE/ordinary EXPLAIN; actual SELECT
execution and EXPLAIN ANALYZE can call/charge a provider. Model output text and
provider metadata are variable, so examples specify their shape rather than an
invented fixed response. Use VECTOR_TO_STRING to display an embedding in clients
that render the native wire payload as hexadecimal bytes. Equal dimensions do
not prove that vectors from different model revisions share an embedding space.
