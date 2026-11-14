# `DROP AI SECRET`

## Description

Drops an AI secret.

## Syntax

```ebnf
<drop-ai-secret> ::= 'DROP AI SECRET' [ 'IF EXISTS' ] <identifier>
```

## Behavior, Privileges, and Results

Requires global `AI_ADMIN`. AI catalog mutations implicitly commit the existing transaction and are rejected while outgoing MySQL binary logging is enabled. Native WAL replication carries encrypted metadata without repeating remote model calls. See [AI administration](../../ai.md#administration-and-visibility) for configuration schemas and dependency rules.

Names are instance-scoped; a schema-qualified Secret or Connection name is rejected. Returns OK after validated publication. Referenced objects cannot be dropped; first alter or drop their dependents.

## Example

```sql
DROP AI SECRET IF EXISTS provider_key;
```

## See Also

- [AI configuration and examples](../../ai.md)
- [AI and HTTP index](README.md)
