# Hybrid search with reciprocal rank fusion

mydb provides an SQL `RRF` query expression for combining independently ordered
retrieval lists. It supports weighted fusion, composite identities, prepared
statements, CTEs, relational consumers, and optional per-branch explanations.
This is a native mydb extension over the MySQL wire protocol. MySQL servers do
not accept this syntax. The implementation is a **technical preview**; broad
multilingual quality and production workload certification remain separate
release gates.

## Query and schema

Create ordinary tables and indexes. Generate embeddings outside the database.
All vectors in a COSINE index must satisfy its dimension, datatype, and nonzero
domain constraints. Model version identifiers use a binary, NO PAD collation
so case and trailing spaces cannot join incompatible embedding spaces. An `embedding_ready` flag does not exempt a row from those
constraints. Stage pending embeddings separately, or retain a valid older-model
vector while excluding it from the new model's dense branch.

```sql
CREATE TABLE chunks (
  tenant_id BIGINT UNSIGNED NOT NULL,
  chunk_id BIGINT UNSIGNED NOT NULL,
  document_id BIGINT UNSIGNED NOT NULL,
  body TEXT NOT NULL,
  embedding VECTOR(384) NOT NULL,
  embedding_model_version VARCHAR(64) COLLATE utf8mb4_0900_bin NOT NULL,
  embedding_ready BOOLEAN NOT NULL,
  is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
  expires_at DATETIME(6),
  PRIMARY KEY (tenant_id, chunk_id),
  FULLTEXT KEY body_ft (body)
);
CREATE INDEX dense_hnsw ON chunks(embedding)
  USING HNSW WITH(metric=cosine);
```

Every projected branch column is part of the identity. Fetch body and source
metadata after choosing the fusion window. Use the same trusted tenant and
authorization scope in every relevant branch, before its `LIMIT`.

```sql
SELECT d.document_id, d.body, s.*
FROM (
  SELECT h.* FROM (
    (SELECT tenant_id, chunk_id FROM chunks
     WHERE tenant_id=? AND is_deleted=0
       AND (expires_at IS NULL OR expires_at>CURRENT_TIMESTAMP)
       AND MATCH(body) AGAINST(?)
     ORDER BY MATCH(body) AGAINST(?) DESC, chunk_id LIMIT ?) AS lexical
    RRF
    (SELECT /*+ HNSW_EF(200) */ tenant_id, chunk_id FROM chunks
     WHERE tenant_id=? AND is_deleted=0
       AND (expires_at IS NULL OR expires_at>CURRENT_TIMESTAMP)
       AND embedding_ready=1 AND embedding_model_version=?
     ORDER BY COSINE_DISTANCE(embedding, TO_VECTOR(?)), chunk_id LIMIT ?) AS dense
    WITH(RANK_CONSTANT=60, WEIGHTS=(1.0,1.0), SCORE_DETAILS=TRUE)
  ) AS h
  ORDER BY h.rrf_score DESC, h.best_rank, h.tenant_id, h.chunk_id LIMIT ?
) AS s
JOIN chunks AS d ON d.tenant_id=s.tenant_id AND d.chunk_id=s.chunk_id
ORDER BY s.rrf_score DESC, s.best_rank, s.tenant_id, s.chunk_id;
```

Bind the repeated text parameters to the same query, and both tenant parameters
to the authenticated tenant. This minimal schema has no ACL: add the appropriate
`EXISTS` or join predicate to **both** branches. A tenant predicate alone is not
document authorization. The Python example below includes document ACLs.

## Grammar and parameters

```text
rrf_query  := (ordered_select_or_with) [AS branch_name]
             RRF (ordered_select_or_with) [AS branch_name]
             { RRF (ordered_select_or_with) [AS branch_name] }
             [WITH(option {, option})]
option     := RANK_CONSTANT = numeric_literal_or_parameter
            | WEIGHTS = (numeric_literal_or_parameter {, numeric_literal_or_parameter})
            | SCORE_DETAILS = TRUE | SCORE_DETAILS = FALSE
```

There are 2–8 branches in one node. Branch names must be unique, including
generated names `branch_1`, `branch_2`, etc. Adjacent branches form one N-way
fusion. `RRF` and option names remain ordinary identifiers outside this context.

| Option | Default | Contract |
| --- | --- | --- |
| `RANK_CONSTANT` | `60` | Exact integer in `0..4294967295` |
| `WEIGHTS` | One `1.0` per branch | Finite nonnegative numeric values; at least one positive; finite worst-case score sum |
| `SCORE_DETAILS` | `FALSE` | Boolean literal; cannot be a parameter because it changes result metadata |

Unknown or repeated options and invalid values produce errors. Anonymous `?`
parameters follow lexical SQL order, including branch and option parameters.
Prepared executions rebind filters, vectors, weights, constants, and limits;
they do not cache previous candidate lists. A zero-weight branch is still
parsed, bound, and checked for privileges, but does not execute its row input.

Each branch must explicitly specify `ORDER BY` and `LIMIT`. `LIMIT 0` is valid.
There is no implicit candidate limit. Branch offsets must be absent or zero;
outer SQL offsets are supported. HNSW/IVF hints belong to their own branch.

## Identity, rank, and output

Branch projections must be explicit, with matching column counts and logical
types at each position. There is no implicit cross-branch type promotion: use
`CAST` for different signedness, precision, temporal precision, or collation.
Nullable declarations are allowed, but an actual NULL identity component fails
the statement. Supported keys are integers, finite floating point, DECIMAL,
text, binary, and date/time scalars. JSON, VECTOR, geometry, untyped parameters,
and plain ENUM/SET keys are rejected; explicitly cast ENUM/SET to a supported
domain. Identity expressions must be repeatable.

String equality follows SQL collation and trailing-space semantics; floating
`-0` and `+0` merge. Equivalent representations use a stable representative.
Include tenant/collection/source keys when numeric IDs do not share a global
identity domain. Identity output names come from the first branch, must be
unique, and must not collide with fusion output names.

Ranks start at one after branch filtering, sorting, and limiting. Missing
identity sort keys are appended as ascending tie breakers before the branch
limit. Duplicate identities retain their best rank without renumbering:
`[A,A,B]` gives B rank 3. The limit counts input rows. Use explicit branch
deduplication if the desired retrieval entity is distinct.

```text
rrf_score(x) = sum(weight_i / (rank_constant + best_rank_i(x)))
```

Only positive-weight branches containing the identity contribute. DOUBLE
addition follows stable branch order. The node orders by score descending,
best rank ascending, then identity ascending. Outer SQL should explicitly order
its result, especially after joins.

| Column | Type and meaning |
| --- | --- |
| Identity columns | First branch's names and the agreed types |
| `rrf_score` | DOUBLE NOT NULL; sum of reciprocal rank contributions |
| `best_rank` | BIGINT UNSIGNED NOT NULL; best positive-weight rank |
| `match_count` | BIGINT UNSIGNED NOT NULL; positive-weight branches containing the key |
| `rrf_details` | JSON NOT NULL; present only with `SCORE_DETAILS=TRUE` |

Details are an array in branch order. Each element has `branch_id`, `name`,
`weight`, `disabled`, nullable `rank`, `contribution`, typed `ordered_values`,
sort directions, and a recognized BM25/vector metric or generic SQL ranker.
DECIMAL values use lossless strings and binary values use hex. Missing and
disabled branches have NULL rank/values and zero contribution. Details require
repeatable order expressions; volatile ordering with details is rejected.

RRF scores are query-local ranking scores, not answer confidence. Adding similar
branches can double-count evidence. Hard pinning and mandatory exact identity
matches need explicit application policies. Fusion cannot recover documents
missing from all input lists.

## Relational semantics, consistency, and resources

The fusion result is an ordinary relation consumable by outer filtering, joins,
CTEs, grouping, and windows. An outer predicate applies after fusion unless
written explicitly into each input. An outer join/WHERE/aggregate/window is a
barrier to fusion-window pushdown. A proven simple projection and canonical
ordering can pass `LIMIT + OFFSET` to the final fusion heap, after all branch
contributions have been accumulated. Branch limits are never reduced by this
optimization. There is no hidden 1,000-result cap.

All branches use the statement's transaction view, including read-your-writes.
Under READ COMMITTED a concurrent commit cannot give the two branches different
statement views. Under REPEATABLE READ the transaction retains its snapshot.
BM25 base corpus statistics are shared across the statement; this does not turn
the global index statistics into historical MVCC statistics.

The first implementation executes branches sequentially using the existing
child execution paths. Fusion state and explanations use the shared query
memory budget. The fusion map does not spill. Cancellation, timeouts, branch
errors, and hard resource failures fail the statement; no successful partial
fusion is returned. Use bounded candidate windows for predictable resource use.

Inputs containing nested fusion, set operations (including transitive CTE/view
dependencies), recursion, locking reads, side effects, or external correlated
references are unsupported and fail explicitly. Stored routines in inputs are
conservatively rejected until purity can be established. Outer independent set
operations retain their normal semantics.

## Plans and ANN interpretation

Use `EXPLAIN FORMAT=TREE` or `JSON` to see the RRF node, weights, constant,
output window, and actual child access paths. `EXPLAIN ANALYZE` executes the
query and reports operator time/rows/loops, fusion input and unique counts,
union and overlap counts, conservative reserved memory, and fusion work time.
The child tree distinguishes FulltextScan, VectorIndexScan, and exact paths.
The fusion work timer excludes child stream execution and is an instrumented
measurement, not a production latency promise.

Full-text retrieval needs explicit BM25 ordering. `WHERE MATCH(...) LIMIT L`
alone does not select the BM25 top L. A constrained vector query with MATCH
uses the ordinary full-text-aware execution path instead of bypassing its score
provider. This is different from fusing two independently ranked lists.

Vector retrieval sorts its rerank pool by exact distance and identity before
limiting, including when the session requests relaxed ANN order. This produces
valid ranks within that pool; it does not prove global exact nearest neighbors.
Filtering can trigger more ANN candidates or exact completion. Compare with
`NO_VECTOR_INDEX` under the **same eligibility predicates** to measure ANN
recall. See [vector indexes](vector-indexes.md) and [hints](hints.md#8-vector-search-hints).

## Manual SQL reference

The following template preserves duplicate ordinal ranks. Replace `a`/`b` with
frozen candidate tables or ordered retrieval queries. For text/composite keys,
use matching SQL domains and group/join on the complete identity.

```sql
WITH
  ar AS (SELECT id, ROW_NUMBER() OVER(ORDER BY relevance DESC,id) AS rk
         FROM candidate_a ORDER BY relevance DESC,id LIMIT 100),
  br AS (SELECT id, ROW_NUMBER() OVER(ORDER BY distance,id) AS rk
         FROM candidate_b ORDER BY distance,id LIMIT 100),
  all_ranks AS (
    SELECT id, 1 AS branch_id, rk FROM ar
    UNION ALL SELECT id, 2 AS branch_id, rk FROM br
  ),
  best AS (SELECT id,branch_id,MIN(rk) AS rk FROM all_ranks GROUP BY id,branch_id)
SELECT id, SUM(1e0/(60+rk)) AS rrf_score, MIN(rk) AS best_rank,
       COUNT(*) AS match_count
FROM best GROUP BY id
ORDER BY rrf_score DESC,best_rank,id LIMIT 20;
```

Use the same frozen inputs for SQL/native/mathematical comparisons. Two free
ANN executions need not produce identical candidates, especially under writes.

## Python, external reranking, and evidence

[hybrid.py](../examples/python/hybrid.py) supplies a DB-API retriever and an
extended schema with ACLs, source locations, document/chunk order, content and
document versions. [hybrid_demo.py](../examples/python/hybrid_demo.py) is a
self-checking integration example. The demo creates and drops its own schema.

```python
from hybrid import HybridRetriever, Principal

# principal comes from authenticated application state, never an agent argument.
retriever = HybridRetriever(connection, Principal(tenant_id=1, principal_id=7))
result = retriever.search(
    query, query_embedding, model_version="v1",
    branch_limit=100, fusion_limit=100, limit=20, per_document=2,
    rerank=batch_reranker, details=True,
)
evidence = retriever.fetch_evidence(result.candidates)
```

`batch_reranker(query, candidates)` returns one finite score per candidate.
Inference and network calls stay outside the storage engine. Candidate bodies
are fetched in the retrieval statement's view. Later evidence fetching checks
authorization, expiry/deletion, content version, and document version; stale or
unavailable evidence raises `EvidenceUnavailable`. Neighbor context reapplies
eligibility and document version. For fresh revocation checks, use a new READ
COMMITTED statement; a long REPEATABLE READ transaction intentionally keeps its
old view. This application example is not database row-level security.

Document caps run after fusion TopF and optional reranking. They can return fewer
than K results; `insufficient_candidates` signals this, and `at_fusion_limit`
indicates a full retrieval window. Increase F explicitly or implement a bounded
retry policy. Capping documents before retrieval TopF is a different policy.

Install `examples/python/requirements.txt` for DB-API use. The optional
[LangChain adapter](../examples/python/hybrid_langchain.py) uses
`requirements-hybrid.txt` (Python 3.10+) and accepts a supplied embedding
function. The [Rust prepared example](../examples/rust/examples/hybrid_prepared.rs)
demonstrates standard MySQL binary prepared metadata and rebinding.

Default full-text token length is 3. Short identifiers, punctuation, CJK, model
versions, and language-specific tokenization need workload evaluation; RRF does
not replace tokenizer configuration. See [full-text functions](function/README.md).
