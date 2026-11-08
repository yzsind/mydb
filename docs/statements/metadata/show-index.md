# `SHOW INDEX`

## Description

Shows a table's index definitions and estimated cardinality; KEYS and INDEXES are synonyms.

## Syntax

```ebnf
<show-index> ::= 'SHOW' ( 'INDEX' | 'INDEXES' | 'KEYS' )
                ( 'FROM' | 'IN' ) <qualified-name>
                [ ( 'FROM' | 'IN' ) <identifier> ]
                [ 'WHERE' <expr> ]
```

## Behavior, Privileges, and Results

- Returns compatibility columns such as Non_unique, Key_name, Seq_in_index, Column_name, Collation, Cardinality, Index_type, and Visible.
- Expressions/types of functional, multi-valued, full-text, spatial, and vector indexes are presented according to the current catalog.

## Result and Visibility Details

Requires SELECT on the table. Seq_in_index is one-based. Non_unique=0 identifies uniqueness; Cardinality is an estimate. Sub_part is a prefix length when present, Expression identifies a functional key, and Visible describes optimizer visibility. WHERE uses the displayed metadata columns.

See [ordered columns and wire types](result-columns.md#show-index). Reading this metadata does not itself commit user DML.

## Examples

```sql
SHOW INDEX FROM orders WHERE Key_name = 'idx_order_customer';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
