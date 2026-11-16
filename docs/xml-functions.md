# XML functions

`EXTRACTVALUE()` and `UPDATEXML()` implement the MySQL 8.0 XML-function
interface. Compatibility tests use MySQL 8.0.46 as the reference.

## ExtractValue

```sql
EXTRACTVALUE(xml_fragment, xpath_expression)
```

A node-set result returns the selected nodes' direct child text in document
order, separated by spaces. Descendant elements' text is not included unless
those elements are themselves selected. Entity references retain their source
spelling. A valid path with no matching text returns an empty string; scalar
XPath expressions return their scalar value as a string.

```sql
SELECT EXTRACTVALUE('<a>A<b>B</b>C</a>', '/a');      -- 'A C'
SELECT EXTRACTVALUE('<a><b>X</b><b>Y</b></a>', '//b'); -- 'X Y'
SELECT EXTRACTVALUE('<a><b/></a>', 'count(/a/b)');    -- '1'
```

## UpdateXML

```sql
UPDATEXML(xml_fragment, xpath_expression, replacement)
```

Exactly one selected node is replaced. Zero or multiple selected nodes return
the original fragment. Selecting `/` replaces the whole fragment; selecting
an attribute replaces its attribute fragment. Unchanged portions retain their
original spelling and whitespace. The replacement is inserted verbatim and
is not validated as XML. A scalar XPath result returns `NULL`.

```sql
SELECT UPDATEXML('<a><b>X</b></a>', '/a/b', '<c>Y</c>'); -- '<a><c>Y</c></a>'
SELECT UPDATEXML('<a><b/><b/></a>', '//b', '<c/>');       -- unchanged
```

## XPath dialect and diagnostics

- Both functions require an XPath expression that is constant during one
  execution. Literals, constant SQL expressions, prepared parameters and
  statement-constant user variables are accepted. A column-dependent XPath
  produces error 1105, including on an empty table.
- Paths support `/`, `//`, `.`, `..`, attributes, wildcards, colon-containing
  names, unions and predicates. Predicates support comparisons, arithmetic,
  Boolean expressions, positions and nested paths. Relative paths start at
  the fragment root, or at the current predicate context.
- XPath functions include `boolean`, `ceiling`, `concat`, `contains`, `count`,
  `false`, `floor`, `last`, `not`, `number`, `position`, `round`, `string-length`,
  `substring`, `sum` and `true`. Their coercion, formatting and argument rules
  follow MySQL, including its differences from the XPath standard.
- `$@name` reads a session user variable. `$name` reads a local variable or
  parameter in the current stored-program scope; an unknown local name is
  an error. Cached paths never retain variable values.
- String comparisons use the SQL arguments' aggregated collation. XML names
  remain case-sensitive. Namespace prefixes are literal parts of names;
  namespace URI resolution is not performed.
- MySQL's node-type tests and its following/preceding/namespace axis spellings
  retain MySQL's existing behavior; these functions are not a general XPath
  1.0 implementation. Node-set-to-node-set comparisons and unsupported XPath
  functions produce MySQL-compatible diagnostics.
- A malformed source fragment returns `NULL` with warning 1525. An invalid
  XPath produces an error, normally 1105. Argument counts, character-set
  conversion failures and numeric conversion diagnostics are checked too.
- A `NULL` argument returns `NULL`, subject to MySQL's earlier XPath and type
  validation. `ucs2`, `utf16`, `utf16le` and `utf32` are not supported as the
  aggregated XPath character set.
- XML fragments use MySQL's fragment-reader rules, including multiple root
  elements. External entities are not fetched or expanded.

See the [MySQL 8.0 XML-function reference](https://dev.mysql.com/doc/refman/8.0/en/xml-functions.html)
for the original interface and dialect examples.
