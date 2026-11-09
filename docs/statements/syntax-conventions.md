# SQL Syntax Notation and Conventions

This manual describes the user-visible SQL syntax currently implemented by mydb. "Supported" means a statement can be parsed and executed within its documented
boundaries. Syntax acceptance alone does not imply support for every option.

## EBNF Notation

Syntax blocks use readable EBNF summaries of the supported forms. They are not a complete lexical grammar. Common structural names and the chapters defining them are indexed in [Grammar terms](grammar.md); object-specific options are defined on their owning pages.

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
- `DELIMITER` is a client script directive. The server rejects it with 1235. API clients
  send an entire stored-program definition as one SQL statement, retaining inner semicolons;
  see [DELIMITER](programmatic/delimiter.md).

## Privileges, Transactions, and Errors

- Unless stated otherwise, reads require the `SELECT` privilege on the object and writes
  require the corresponding DML privileges; DDL, account, administration, and replication
  statements additionally check dedicated privileges.
- Whether a DDL statement implicitly commits, whether a statement can be rolled back, and
  whether it acquires metadata locks is governed by each page's "Behavior and Limitations" section.
- Unsupported capabilities return errors. Accepted compatibility forms are documented with
  their actual effect, including metadata-only output and options that currently have no
  distinct effect. Check the owning page before relying on a physical storage or locking option.
- Common errors use MySQL errno numbers; syntax errors are usually 1064, unsupported execution
  capabilities are usually 1235, and insufficient privileges are usually 1044, 1045, 1142,
  1227, or a statement-specific error.

## Reference Contracts

See [Execution contracts](execution-contracts.md) for privilege scopes, implicit commits, statement rollback, diagnostics, and result conventions. See [Grammar terms](grammar.md) for the shared syntax vocabulary. Successful parsing alone does not establish that an operation is executable in a stored function, trigger, read-only transaction, native replica, or REST session.

## Example Conventions

Examples use the schemas in [Example setup](examples.md), or explicitly identify their own setup and placeholders. Run each example independently after resetting its objects. A connection must have the privileges the example requires. Examples involving files, replication, backup, restart, or shutdown
only describe the command and its preconditions and should not be copied directly into a
production environment. Result text may vary with object names, session state, and data;
the documentation focuses on guaranteeing column shape, transaction effects, and warning
and error semantics.
