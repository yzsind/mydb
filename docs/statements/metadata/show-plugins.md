# `SHOW PLUGINS`

## Description

Lists the compatibility catalog of plugins/components visible to the server.

## Syntax

```ebnf
<show-plugins> ::= 'SHOW PLUGINS'
```

## Behavior, Privileges, and Results

- Returns compatibility columns such as Name, Status, Type, Library, and License.
- Component installation capability is defined by the INSTALL COMPONENT documentation; catalog entries do not mean arbitrary shared libraries can be dynamically loaded.

## Examples

```sql
SHOW PLUGINS;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
