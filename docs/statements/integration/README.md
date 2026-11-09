# AI and HTTP Administration

These mydb extensions manage external-call policy and AI catalog objects. HTTP/AI SQL functions are in the [function reference](../../function/README.md). External calls can have side effects outside the database; SQL rollback cannot undo an upstream operation.

| Object or action | Statements |
| --- | --- |
| AI Secret | [CREATE](create-ai-secret.md), [ALTER](alter-ai-secret.md), [DROP](drop-ai-secret.md), [SHOW](show-ai-secrets.md), [SHOW CREATE](show-create-ai-secret.md) |
| AI Connection | [CREATE](create-ai-connection.md), [ALTER](alter-ai-connection.md), [DROP](drop-ai-connection.md), [SHOW](show-ai-connections.md), [SHOW CREATE](show-create-ai-connection.md) |
| AI Model | [CREATE](create-ai-model.md), [ALTER](alter-ai-model.md), [DROP](drop-ai-model.md), [SHOW](show-ai-models.md), [SHOW CREATE](show-create-ai-model.md) |
| AI policy and encryption | [RELOAD AI CONFIG](reload-ai-config.md), [ROTATE AI ENCRYPTION KEY](rotate-ai-encryption-key.md) |
| HTTP policy and status | [RELOAD HTTP CONFIG](reload-http-config.md), [SHOW HTTP STATUS](show-http-status.md) |

`AI_ADMIN` controls catalog administration; `AI_USE` controls calls. `HTTP_ADMIN` controls policy reload/status, while `HTTP_USE` controls calls. SUPER alone is not these extension capabilities. See [AI configuration](../../ai.md), [HTTP policy](../../http.md), and [GRANT scopes](../account/grant.md#behavior-and-limitations).
