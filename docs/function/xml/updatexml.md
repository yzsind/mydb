# `UPDATEXML`

## Description

Returns XML text with exactly one selected node replaced. Zero or multiple selected nodes preserve the input; a scalar XPath result returns SQL NULL. Replacement text is inserted verbatim without XML validation.

## Syntax

```sql
UPDATEXML(xml_fragment, xpath_expression, replacement)
```

## Parameters

- `xml_fragment`: source XML fragment text.
- `xpath_expression`: literal, bound parameter or other execution-constant XPath; row-dependent paths are rejected.
- `replacement`: replacement fragment text, inserted verbatim.

## Return Value

Returns XML text with exactly one selected node replaced. Zero or multiple selected nodes preserve the input; a scalar XPath result returns SQL NULL. Replacement text is inserted verbatim without XML validation. A malformed source returns SQL NULL with warning 1525; invalid XPath normally raises 1105. SQL NULL propagates subject to earlier XPath/type validation.

## Examples

```sql
SELECT UPDATEXML('<a><b>X</b></a>','/a/b','<c>Y</c>');
```

Result:

```text
<a><c>Y</c></a>
```

## Notes

See [XPath dialect and diagnostics](../../xml-functions.md#xpath-dialect-and-diagnostics) for supported axes/functions, variable bindings and collation. XML names remain case-sensitive; external entities are not fetched.

## Related Functions

[`EXTRACTVALUE`](extractvalue.md).
