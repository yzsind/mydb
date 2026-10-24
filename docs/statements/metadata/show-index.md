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

## Examples

```sql
SHOW INDEX FROM orders WHERE Key_name = 'idx_order_customer';
```

## See Also

- [Metadata and diagnostics index](README.md)
- [Syntax conventions](../syntax-conventions.md)
