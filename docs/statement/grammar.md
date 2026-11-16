# Shared SQL Grammar Terms

Statement syntax blocks are readable summaries. This glossary resolves their structural names and links to the detailed owner. It does not mean every combination of individually valid clauses is executable. Statement-specific restrictions take precedence.

## Lexical Values and Lists

```ebnf
<identifier-list> ::= <identifier> { ',' <identifier> }
<role-list> ::= <account> { ',' <account> }
<privilege-list> ::= <privilege> [ '(' <identifier-list> ')' ]
                     { ',' <privilege> [ '(' <identifier-list> ')' ] }
<assignment> ::= [ <identifier> '.' ] <identifier> '=' <expr>
<table-query> ::= 'TABLE' <qualified-name>
<values-query> ::= 'VALUES' <query-row> { ',' <query-row> }
<query-row> ::= 'ROW' '(' <expr> { ',' <expr> } ')'
<order-by> ::= 'ORDER BY' <order-item> { ',' <order-item> }
<order-item> ::= <expr> [ 'ASC' | 'DESC' ]
<limit-clause> ::= <row-count> [ 'OFFSET' <offset> ]
                   | <offset> ',' <row-count>
```

| Name | Definition |
| --- | --- |
| identifier, unquoted-identifier, quoted-identifier-body | SQL name; backticks quote names and doubled backticks represent a backtick. ANSI_QUOTES additionally permits double-quoted identifiers. Object limits are described in [DDL](ddl/create-table.md). |
| string-literal, literal | SQL string or typed literal, with escaping controlled by sql_mode; [types and conversion](../data-types.md) and [expressions](../operators-and-expressions.md). |
| unsigned-integer, signed-integer, positive-integer | Integer token without a sign, with an optional sign, or greater than zero, respectively. The owning option defines its numeric range. |
| row-count, offset | Nonnegative integer row cap or number of rows skipped. SELECT accepts parameterized limits; DML LIMIT has no offset. |
| account | User/role name with optional @host; quoted strings or permitted identifier spelling. Omitted host defaults to `%`; see [accounts](account/README.md). |
| charset, collation | Registered character set/collation name; inspect [SHOW CHARACTER SET](metadata/show-character-set.md) and [SHOW COLLATION](metadata/show-collation.md). |
| data-type | A type from [Data Types](../data-types.md), including its permitted length, precision, scale and modifiers. |
| expr, column-reference, function-call, operator-expression, subquery-expression | Scalar expression forms described in [Operators and Expressions](../operators-and-expressions.md) and the [function catalog](../function/README.md). |
| name, value | Object-specific option key/value, not arbitrary SQL. For index WITH parameters see [vector indexes](../vector-indexes.md). |
| connection-id, thread-id | Unsigned connection identifier from [SHOW PROCESSLIST](metadata/show-processlist.md). |
| seconds | Integer wait duration; native role switching accepts 1 through 3600. |
| size | Size token accepted by the owning option; the TABLESPACE forms remain unsupported. |
| json-string | A single-quoted SQL string containing a JSON object, not a bare SQL JSON expression. |
| job-id | A quoted backup job identifier from SHOW MYDB BACKUP JOBS; DDL job IDs are instead unsigned integers. |
| amount, seed | Sampling expressions with the value/range constraints in [SELECT sampling](query/select.md#table-sampling). |
| delimiter-token | A nonempty separator consumed by a script client, not a server SQL token; [DELIMITER](programmatic/delimiter.md). |

## Queries and Data Manipulation

| Name | Definition and owner |
| --- | --- |
| query-expression, select-query, select-statement | SELECT, TABLE, VALUES, parenthesized queries and UNION/INTERSECT/EXCEPT; see [SELECT](query/select.md). INTERSECT binds more tightly than UNION/EXCEPT; parentheses override grouping. |
| select-modifier | DISTINCT/ALL, DISTINCTROW and recognized MySQL result/priority modifiers; effects and diagnostics in [SELECT](query/select.md). |
| table-reference-list | Comma-separated base tables/views, CTE references, derived queries or table functions, with aliases and supported JOIN/ON/USING/NATURAL/LATERAL clauses; [query syntax](query/select.md#syntax). |
| group-by | GROUP BY expressions, WITH ROLLUP, ROLLUP/CUBE/GROUPING SETS; [query syntax](query/select.md#syntax). |
| window-clause | WINDOW name AS (...) definitions with partition/order/frame specifications; [aggregate and window functions](../function/aggregate-window/README.md). |
| select-prefix, select-tail | The select list and the remaining WITH/query/FROM/WHERE/GROUP/HAVING/WINDOW/ORDER/LIMIT portions around INTO; [variable assignment](query/select-into-variables.md). |
| insert-statement, update-statement, delete-statement | The corresponding [INSERT](dml/insert.md), [UPDATE](dml/update.md), and [DELETE](dml/delete.md) productions. |
| delete-target | A table name or alias, optionally followed by `.*`, identifying a DELETE target in the joined source. |
| field-option, line-option | Delimiters/enclosure/escape and line prefix/terminator in [LOAD DATA](dml/load-data.md#syntax); export uses the same spellings. |
| load-target | A column name or @user variable; [LOAD DATA](dml/load-data.md). |

## Object and Account Options

| Name | Definition and owner |
| --- | --- |
| column-definition, column-option, table-element | Column type/attributes, constraint or index in [CREATE TABLE](ddl/create-table.md#syntax). |
| table-constraint, table-constraint-or-index | PRIMARY/UNIQUE/FOREIGN KEY/CHECK or an index definition; [CREATE TABLE](ddl/create-table.md#syntax). |
| index-definition, index-key, index-option | Inline or standalone index keys, prefix/functional expressions and options; [CREATE INDEX](ddl/create-index.md#syntax). |
| table-option | Supported engine, charset/collation, AUTO_INCREMENT, COMMENT and compatibility metadata options; [CREATE TABLE](ddl/create-table.md#behavior-and-limitations). |
| partition-clause, partition-action | Partition/subpartition definitions and ALTER operations; [partitioning](../mysql80-compatibility.md#partitioning) and [ALTER TABLE](ddl/alter-table.md#syntax). |
| option | In ALTER DATABASE, another DEFAULT CHARACTER SET/CHARSET or COLLATE option; see [ALTER DATABASE](ddl/alter-database.md). |
| tablespace-option | Recognized TABLESPACE option spelling from [CREATE TABLESPACE](ddl/create-tablespace.md); recognition still ends in an unsupported error. |
| tls-option, resource-options, password-options | REQUIRE TLS clauses, account quotas and password policy options in [CREATE USER](account/create-user.md#syntax). These account quotas are separate from unsupported RESOURCE GROUP. |
| privilege, grant-scope | Registered privilege and permitted target; [GRANT](account/grant.md#behavior-and-limitations). |
| vcpu-list | Comma-separated CPU numbers/ranges in recognized RESOURCE GROUP syntax; no scheduling or resource-group catalog is implemented. |

## Stored Programs and Replication

| Name | Definition and owner |
| --- | --- |
| routine-body, routine-statement | A supported ordinary SQL statement or compound block containing declarations/control flow; [program contexts](programmatic/README.md). |
| declaration | DECLARE variable/condition/cursor/handler, in that order before executable block statements; [BEGIN END](programmatic/begin-end.md). |
| routine-characteristic | COMMENT, LANGUAGE SQL, [NOT] DETERMINISTIC, data-access declaration, or SQL SECURITY; [CREATE PROCEDURE](programmatic/create-procedure.md). |
| label, cursor-name, condition-name | Identifier resolved in the enclosing stored-program block; [scope](programmatic/begin-end.md). |
| routine-target | Local or @user variable; SET additionally recognizes writable NEW.column in a permitted BEFORE trigger. |
| condition-assignment | Target = a supported condition information item; [GET DIAGNOSTICS](programmatic/get-diagnostics.md#behavior-and-limitations). |
| condition-value, signal-item | SQLSTATE or named condition and supported information assignments; [SIGNAL](programmatic/signal.md). |
| interval-value, interval-unit, event-schedule | Positive event interval or timestamp schedule; [CREATE EVENT](programmatic/create-event.md). |
| gtrid, bqual, format-id | XA binary identifiers and nonnegative format-number token; [XA START](transaction/xa-start.md#syntax). |
| replication-filter-option | do/ignore database/table/wild-table filter assignments; [CHANGE SOURCE](replication/change-replication-source.md#syntax). |
| thread-selector, until-condition | IO_THREAD/SQL_THREAD selection and stop condition; [START REPLICA](replication/start-replica.md). |
| profile-type | ALL, BLOCK IO, CONTEXT SWITCHES, CPU, IPC, MEMORY, PAGE FAULTS, SOURCE, or SWAPS; [legacy profiler](replication/show-profiles.md). |
