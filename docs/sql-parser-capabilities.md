# SQL Syntax Capabilities

This matrix describes supported SQL syntax and its boundaries. It complements
[MySQL Compatibility](mysql80-compatibility.md); support for a syntax family
does not imply that every MySQL combination is accepted. Unsupported syntax
returns an error before the statement changes data.

| Contract | Status and boundary |
| --- | --- |
| Query clauses | CONNECT BY, FOR JSON/XML and BY NAME set operations explicitly rejected; ordinary set operations remain supported |
| Direct INSERT VALUES | Query ORDER/LIMIT/OFFSET/FETCH/locking tails rejected before writes |
| Identifiers | User identifiers and projection aliases do not enable extension syntax |
| DEFAULT | Bare DEFAULT and DEFAULT(column) supported; invalid argument shapes rejected |
| Function modifiers | Validated by function family; unsupported ORDER/LIMIT/FILTER/null treatment and invalid DISTINCT/wildcards rejected |
| Administrative names | Quoted whitespace preserved |
| Mixed Unicode identifiers | Mixed Unicode names are recognized consistently across supported statements |
| Executable comments | MySQL 8.0 five-digit gate; missing separator emits warning 4164; inactive payload excluded |
| Index options | CREATE/ALTER preserve supported primary-key options; UNIQUE constraint symbol supplies fallback index name |
| Native function whitespace | Checked in expression-call context; table/column/foreign-key names unaffected; IGNORE_SPACE respected |
| Diagnostics | Errors refer to byte offsets and Unicode coordinates in the submitted SQL, including supported normalized syntax |
| SQL batches | Each next statement parses using mode left by preceding statement; completed prefix retained on later failure |
| Derived column aliases | SELECT/VALUES, wildcard and set-operation sources; width and duplicates checked after output expansion; joins/lateral/CTE retain metadata |
| Targeted row locks | FOR UPDATE/SHARE OF alias list, multiple clauses and per-source NOWAIT/SKIP LOCKED; ambiguous/unbound/duplicate targets rejected |
| Lock execution limits | Existing locking join restrictions still apply, including unsupported NATURAL/USING/lateral/grouped shapes; error rather than ignored lock |
| Partition bounds | Common expression parsing, typed literals and exact integer/decimal values; existing partition constant/function/type whitelist still enforced |
| Stored-program labels | Quoted labels/cursor names; labels only on BEGIN/LOOP/WHILE/REPEAT; malformed names rejected |
| Nested query tails | Each parenthesized ORDER/LIMIT scope is preserved; original inner qualifiers do not leak |
| Parameters | Placeholder order is preserved through CTE/functions/windows/RETURNING and LIMIT; SQL PREPARE retains mode snapshot and runtime type for unresolved parameter domains |
| Hex spelling | Valid 0x hex payloads stay binary; 0x/0xZZ/0xABZZ remain digit-leading identifiers; quoted aliases are separate |
| Persistent expressions | Unary NOT preserves precedence; reserved/quoted function names retain required quoting; ROW requires at least two expressions |
| DML parameters and hints | SELECT/DELETE/UPDATE preserve placeholder order and optimizer hints |
| Large statements | Parsing is subject to memory budgets and cancellation; large literals are admitted by their size without treating every byte as a syntax node |
| mydb extensions | RETURNING OLD/NEW aliases, empty grouping sets, index INCLUDE/visibility are recognized as explicit SQL syntax |
