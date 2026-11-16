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

## Result and Visibility Details

The rows describe built-in compatibility plugins. Library can be NULL for built-ins. The validate_password component is managed separately with INSTALL COMPONENT; no arbitrary plugin shared-library ABI is available.

See [ordered columns and wire types](result-columns.md#show-plugins). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW PLUGINS;
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
