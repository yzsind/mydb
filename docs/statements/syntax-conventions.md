# SQL Syntax Notation and Conventions

This manual describes the user-visible SQL syntax currently implemented by mydb. "Supported" means a statement can be parsed and executed within its documented
boundaries. Syntax acceptance alone does not imply support for every option.

## EBNF Notation

Syntax blocks use readable EBNF:

| Symbol | Meaning |
| --- | --- |
| `<name>` | A non-terminal, defined by other productions or in prose |
| `::=` | Defines |
| `A \| B` | Either of the two alternatives |
| `[ A ]` | `A` may appear zero or one times |
| `{ A }` | `A` may appear zero or more times |
| `( A B )` | Groups several elements together |
| `'TOKEN'` | An SQL keyword or fixed symbol; matching is usually case-insensitive |
| `...` | Indicates omission in examples or prose only; not part of the formal productions |

Comma-separated lists are written explicitly as `<item> { ',' <item> }`. Mutually exclusive options use `|`; you cannot specify several alternatives from the
same group at once, unless the corresponding documentation explicitly allows repetition.

## Common Non-terminals

```ebnf
<identifier>          ::= <unquoted-identifier> | '`' <quoted-identifier-body> '`'
<qualified-name>      ::= [ <identifier> '.' ] <identifier>
<account>             ::= <string-literal> [ '@' <string-literal> ]
<expr>                ::= <literal> | <column-reference> | <function-call>
                          | <operator-expression> | <subquery-expression>
<query-expression>    ::= <select-query> | <table-query> | <values-query>
                          | <query-expression> <set-operator> <query-expression>
<set-operator>        ::= ( 'UNION' | 'INTERSECT' | 'EXCEPT' )
                          [ 'ALL' | 'DISTINCT' ]
<table-list>          ::= <qualified-name> { ',' <qualified-name> }
<assignment>          ::= <identifier> '=' <expr>
<user-variable>       ::= '@' <identifier>
<system-variable>     ::= '@@' [ 'GLOBAL' '.' | 'SESSION' '.' ] <identifier>
```

The complete rules for identifiers, strings, numeric values, date and time values, JSON, spatial
values, and vector values are described in [Data Types](../data-types.md). For function calls, see
[`function/README.md`](../function/README.md); for operators, expression precedence, and `NULL` evaluation,
see [`operators-and-expressions.md`](../operators-and-expressions.md); for optimizer hints, see
[`hints.md`](../hints.md).

## Keywords, Semicolons, and Comments

- Keywords are usually case-insensitive; this documentation uses uppercase throughout.
- The semicolon is a client-side or multi-statement delimiter and is not part of the core productions of a single statement.
- `-- `, `#`, and `/* ... */` comments are handled by the lexical layer; `/*! ... */` executable
  comments are expanded according to the compatible MySQL version threshold.
- `sql_mode` bits such as `ANSI_QUOTES`, `NO_BACKSLASH_ESCAPES`, and `IGNORE_SPACE` affect
  quotation, escaping, or function-name parsing; individual statement pages list only the
  additional restrictions.
- `DELIMITER` is originally a client directive. The mydb server accepts it for import-script
  compatibility, but it does not change the delimiter rules inside MySQL protocol packets;
  see the stored programs documentation.

## Privileges, Transactions, and Errors

- Unless stated otherwise, reads require the `SELECT` privilege on the object and writes
  require the corresponding DML privileges; DDL, account, administration, and replication
  statements additionally check dedicated privileges.
- Whether a DDL statement implicitly commits, whether a statement can be rolled back, and
  whether it acquires metadata locks is governed by each page's "Behavior and Limitations" section.
- Unsupported syntax is never silently ignored. Compatibility metadata operations, deprecated
  syntax, and a few client-probing statements that produce only a warning or an empty result
  set are explicitly marked in the corresponding documentation.
- Common errors use MySQL errno numbers; syntax errors are usually 1064, unsupported execution
  capabilities are usually 1235, and insufficient privileges are usually 1044, 1045, 1142,
  1227, or a statement-specific error.

## Example Conventions

Examples assume a connection to the `test` database using a session that has the privileges
the example requires. Examples involving files, replication, backup, restart, or shutdown
only describe the command and its preconditions and should not be copied directly into a
production environment. Result text may vary with object names, session state, and data;
the documentation focuses on guaranteeing column shape, transaction effects, and warning
and error semantics.
