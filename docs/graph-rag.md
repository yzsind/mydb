# Property graphs and GraphRAG

mydb supports an open property graph in an existing MySQL database. Use the
same MySQL connection for GQL, SQL, and transaction control. A graph contains
nodes, directed and undirected edges, labels, and scalar properties. Parallel
edges and self-loops have distinct identities. Graph names are
case-insensitive ASCII identifiers up to 51 bytes. Labels and property names
are case-sensitive.

```sql
CREATE GRAPH knowledge ANY PROPERTY GRAPH;
SESSION SET GRAPH knowledge;
CREATE UNIQUE GRAPH INDEX entity_key ON knowledge NODE LABEL Entity (key STRING);

INSERT (source:Entity & Passage {key: 'p1'}),
       (target:Entity & Passage {key: 'p2'}),
       (source)-[:CITES {reference: 'guide'}]->(target);

MATCH p = TRAIL (start:Entity {key: 'p1'})-[:CITES]->{1,3}(related:Passage)
RETURN DISTINCT CAST(related.key AS STRING) AS passage_key,
                PATH_LENGTH(p) AS hops
ORDER BY hops, passage_key LIMIT 20;
```

## Supported GQL subset

The syntax follows [ISO/IEC 39075:2024](https://www.iso.org/standard/76120.html)
and its [technical corrigendum](https://www.iso.org/standard/93701.html).
The table describes the supported forms. Unsupported forms return a syntax or
feature error.

| Area | Supported forms |
| --- | --- |
| Graph catalog and session | Open `CREATE GRAPH`, `DROP GRAPH`, `SESSION SET GRAPH`, `SESSION RESET GRAPH`. Graph names resolve in the selected MySQL database. A graph selection stays attached to its original object identity. Select the replacement explicitly after dropping and recreating a graph. |
| Graph updates | Node and edge `INSERT`, including edges between nodes bound in a preceding `MATCH`. `SET` and `REMOVE` support individual labels and properties. `DELETE` rejects nodes with incident edges. `DETACH DELETE` removes those edges in the same statement. |
| Patterns | `MATCH`, `OPTIONAL MATCH`, comma-separated and consecutive patterns, named paths, conjunctive labels, inline properties and predicates. Match edges with `-[e]->`, `<-[e]-`, `-[e]-`, or `~[e]~` for outgoing, incoming, any direction, or undirected, respectively. |
| Traversal | Bounded `{n}` or `{lower,upper}` quantifiers after an edge pattern. `WALK` allows repeated elements, `TRAIL` forbids repeated edges, and `ACYCLIC` forbids repeated nodes. `ANY SHORTEST` returns one path of minimum hop count per endpoint pair. Path predicates are applied before selecting the shortest paths. The matching clause's trailing `WHERE` filters the selected paths. |
| Input and results | Scalar literals, graph properties, comparisons, Boolean predicates, `FOR name IN [values]` with scalars or records, `RETURN`, `DISTINCT`, `ORDER BY`, `LIMIT`, `ELEMENT_ID`, `PATH_LENGTH`, and `CAST` to `STRING`, `INT64`, `FLOAT64`, or `BOOLEAN`. |

MySQL prepared statements accept positional `?` markers. mydb returns dynamic
properties and composite graph values as JSON. Use a scalar `CAST` for a typed
MySQL column. Element IDs are opaque strings scoped to one graph generation.
Use a business key to join graph results to document tables. Paths contain
ordered `nodes` and `edges` arrays of element IDs.

mydb adds `CREATE [UNIQUE] GRAPH INDEX name ON graph NODE LABEL label (property STRING)`
and `DROP GRAPH INDEX name ON graph`. A graph index targets one property on
nodes with the specified label. Indexes accept UTF-8 strings up to 512 bytes
or signed `INT64` values. A missing or null property has no index entry.
Non-null values on indexed nodes must match the declared index type.

Grouped `RETURN` supports `COUNT(*)` and `COUNT(expression)`. The other
projected expressions form the grouping key. A null expression contributes
zero to its count, including the missing side of an optional match. A grouped
result can order by a projected expression or its alias.

For repeated traversals over a large graph, use a property projection to
pack label membership, heap locations and selected scalar fields into blocks:

```sql
CREATE GRAPH PROJECTION passage_times ON knowledge NODE LABEL Passage (created, key);
CREATE GRAPH PROJECTION citation_scores ON knowledge EDGE LABEL CITES (weight);
DROP GRAPH PROJECTION citation_scores ON knowledge;
```

Creating a projection takes the graph's exclusive metadata lock and builds
the blocks from the current data. New elements use normal indexed traversal.
Changing an element retires its block transactionally. Recreate a projection
to cover new or changed elements. Projections use the database memory target
and may be omitted on a particular access.

## Transactions and privileges

Graph statements participate in the connection's SQL transaction. They support
autocommit, explicit transactions, and savepoints at `READ COMMITTED`,
`REPEATABLE READ`, and `SERIALIZABLE`. Graph DDL requires autocommit outside an
active transaction. Drop a graph explicitly before dropping its database.

The server uses the database-level SQL privileges for the equivalent
operations, including `CREATE`, `DROP`, `INDEX`, `SELECT`, `INSERT`,
`UPDATE`, and `DELETE`. Combined graph statements need the privileges for
each clause. Graph writes require the session's MySQL binary logging to
be off. mydb stores graph data in the same heap, index, WAL, and undo formats
as other transactional data.

Each path pattern can examine up to 16 hops. A statement fails when it exceeds
one million adjacency entries, its query memory allowance, or its deadline.

## Retrieve passages in one snapshot

Start mydb against a fresh local data directory. Install
`mysql-connector-python` in a Python environment and run the example:

```sh
python examples/graph-rag/retrieve.py --port 4837
```

The example creates a graph, a document table, and a vector index. It opens a
`REPEATABLE READ` read-only transaction, retrieves vector seeds with SQL,
expands those seeds with parameterized GQL, and fetches document passages and
citations with SQL. It uses fixed vectors and runs without external model
services. The data is intentionally small, so the vector optimizer may choose
an exact scan. The example uses fixed object names and expects a fresh
database.

## Benchmark indexed traversal

The benchmark compares complete 1–3 hop result multisets on a seeded,
degree-skewed graph against indexed SQL joins over the same edges. It reports
P50 and P95 for one and sixteen clients in separate hotspot and mixed-degree
runs. It also times graph and SQL ingestion. On a fresh local release instance,
load the default one million nodes and ten million edges:

```sh
python scripts/graph_rag_bench.py load --port 4837 --data-dir /tmp/mydb/graph-bench
python scripts/graph_rag_bench.py measure --port 4837
```

For first-touch measurements, restart the server between load and measure.
The repeated pass reuses the same seeds. The operating system can retain file
pages across a process restart. Pass `--server-pid` to sample resident memory.
