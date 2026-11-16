# `EXTRACTVALUE`

## Description

Returns direct child text of selected nodes, joined in document order with spaces, or a scalar XPath result as text. A valid path with no text returns an empty string.

## Syntax

```sql
EXTRACTVALUE(xml_fragment, xpath_expression)
```

## Parameters

- `xml_fragment`: source XML fragment text.
- `xpath_expression`: literal, bound parameter or other execution-constant XPath; row-dependent paths are rejected.

## Return Value

Returns direct child text of selected nodes, joined in document order with spaces, or a scalar XPath result as text. A valid path with no text returns an empty string. A malformed source returns SQL NULL with warning 1525; invalid XPath normally raises 1105. SQL NULL propagates subject to earlier XPath/type validation.

## Examples

```sql
SELECT EXTRACTVALUE('<a><b>X</b><b>Y</b></a>','//b');
```

Result:

```text
X Y
```

## Notes

See [XPath dialect and diagnostics](../../xml-functions.md#xpath-dialect-and-diagnostics) for supported axes/functions, variable bindings and collation. XML names remain case-sensitive; external entities are not fetched.

## Related Functions

[`UPDATEXML`](updatexml.md).
