# Full-Text Search

mydb supports `FULLTEXT` indexes over `CHAR`, `VARCHAR`, and `TEXT` columns,
natural-language BM25 ranking, boolean search, and the `ngram` parser.

```sql
CREATE TABLE articles (
  id BIGINT PRIMARY KEY,
  title VARCHAR(200),
  body TEXT,
  FULLTEXT KEY ft_article (title, body)
);

INSERT INTO articles VALUES
  (1, 'Storage guide', 'database systems and query execution'),
  (2, 'Index guide', 'database index design');

SELECT id, MATCH(title, body) AGAINST('database systems') AS score
FROM articles
WHERE MATCH(title, body) AGAINST('database systems')
ORDER BY score DESC, id;
```

The `MATCH` column set must exactly match a queryable `FULLTEXT` index.
Column order does not matter. A missing matching index returns error 1191.
`CREATE FULLTEXT INDEX` and `ALTER TABLE ... ADD FULLTEXT KEY` can also build
indexes over existing rows. Prepared statements support `AGAINST(?)`.

Persistent offline builds use the index-build worker budget for projected
scanning, tokenization, sorting, spill consolidation, and leaf-page writes.
Use `CREATE /*+ PARALLEL(8) */ FULLTEXT INDEX ft ON articles(body)` to request
a degree, or `NO_PARALLEL` to force serial execution. The default degree is
`MYDB_PARALLEL_DEGREE` (2 when unset); `MYDB_PARALLEL_DISABLE` takes precedence.
Workers share a governed scratch envelope and spill bounded runs when it is
exhausted. Repeated posting keys can use compact dictionary records, while
high-cardinality inputs use the external merge path. Query execution, online
backfills, and memory-only builds keep their existing execution strategies.

## Search modes

Natural-language mode returns the union of indexed query terms. Repeated query
terms contribute once. The default parser splits Unicode text at
non-alphanumeric boundaries while retaining underscores, normalizes terms
using the indexed column's collation, and applies token
length and stopword filtering. Default minimum/maximum token lengths are
3/84 characters, and the built-in English stopword list is enabled.

Boolean mode supports these operators:

| Expression | Meaning |
|---|---|
| `+database` | Required term |
| `-design` | Excluded term |
| `database systems` | Optional terms; their union supplies candidates when no required clause exists |
| `"database systems"` | Adjacent terms in order within one indexed column |
| `data*` | Any indexed term starting with `data` |

```sql
SELECT id FROM articles
WHERE MATCH(title, body)
      AGAINST('+database -design' IN BOOLEAN MODE);
```

An all-negative query matches no documents. An unmatched boolean expression
returns score `0`, including when its positive terms occur but a required,
excluded, or phrase condition fails. Projection, comparison, `NOT`, `OR`,
and multiple `MATCH` expressions use the same membership rules as a bare
`WHERE MATCH(...)` predicate.

Punctuation inside one boolean operand splits it into required tokens:
`alpha.beta` requires both `alpha` and `beta`. Whitespace-separated optional
operands retain union semantics. The built-in stopword table exposes 35 distinct
words. The identifier `fulltext` requires backticks when used as a column name.

Grouping `()`, weights `> < ~`, and proximity `@` return error 1235.
Signs may be separated from their operand by whitespace. An internal `*`
terminates a prefix operand (`data*base` is `data* base`); an unclosed quoted
phrase extends to the end of the search string, matching MySQL's tolerance.
Malformed operands and repeated leading signs return error 1064. `WITH QUERY EXPANSION` is not supported and returns a parse error.
`AGAINST` accepts a string literal or prepared parameter; arbitrary SQL
expressions and implicit numeric/NULL coercions are explicitly rejected.

## Ngram search

```sql
CREATE TABLE chinese_articles (
  id BIGINT PRIMARY KEY,
  body TEXT,
  FULLTEXT KEY ft_body (body) WITH PARSER ngram
);
INSERT INTO chinese_articles VALUES (1, '数据库全文检索');
SELECT id FROM chinese_articles WHERE MATCH(body) AGAINST('全文');
```

The default ngram length is two Unicode characters. Ngrams are generated
within alphanumeric runs; token length limits from the default parser do not
apply. In boolean mode, a multi-token ngram operand requires adjacent indexed
ngrams in order. Phrase adjacency uses the filtered token sequence, rather
than byte-for-byte source-text equality. Punctuation and discarded tokens
do not retain their original character offsets.

With `like_ngram_prefilter=on`, a suitable single-column ngram index can also
prefilter unanchored `LIKE` patterns. The original `LIKE` predicate is always
rechecked. An ordinary full-text index is not a substring index.

## Ranking and configuration

BM25 uses posting term frequency and document length, together with document
count, total token length and term document frequencies from the whole visible
corpus. These counts share a bounded cache keyed by the table definition,
column set, tokenizer and read snapshot. A cold cache streams the corpus;
`ANALYZE TABLE` remains useful for optimizer cost estimates.

Scores do not promise numeric equality with MySQL. Complete and partial
indexes share the same snapshot baseline; filtering candidates does not
redefine the corpus.
Term equality uses the indexed column's collation, including case and accent
sensitivity. Index construction, writes, natural-language searches, boolean
terms, phrases, and wildcard prefixes share the same normalization rules.

The `MYDB_FT_MIN_TOKEN_SIZE`, `MYDB_FT_MAX_TOKEN_SIZE`, `MYDB_FT_NGRAM_SIZE`,
`MYDB_FT_STOPWORDS`, `MYDB_FT_POSITIONS`, and `MYDB_FT_POSITION_GAP` process
defaults are captured in each new index. Later process-default changes do not
change that index's key encoding or tokenizer. Read-only global variables
`innodb_ft_min_token_size`, `innodb_ft_max_token_size`,
`innodb_ft_enable_stopword`, and `ngram_token_size` expose the defaults used
by new indexes. `ft_min_word_len` and `ft_max_word_len` are aliases for the
same defaults; they do not describe a separate MyISAM tokenizer. `IGNORE INDEX`
does not hide the FULLTEXT index required by a natural-language `MATCH`.
Boolean mode honors JOIN-scope index hints and reports 1191 when they exclude
the required index; GROUP BY/ORDER BY hints do not exclude it.
The inter-column position gap
is at least one. Filtered stopwords and out-of-range words retain their original
word offsets. Phrases with internal filtered words also verify the original
words against the visible heap row; filtering never makes separated words adjacent. Indexes without stored positions verify phrases against
visible row text. `MYDB_FT_BM25_K1` and `MYDB_FT_BM25_B` configure process-wide
ranking parameters, defaulting to 1.2 and 0.75.

Document lengths and stored token positions use unsigned 32-bit fields.
Writes and index builds return error 1690 when a document exceeds these
limits, including through a large inter-column gap. They do not saturate or
wrap positions into an invalid posting or a cross-column phrase match.

## Transactional scoring statistics

BM25 uses the complete visible table corpus, including rows excluded from a
partial FULLTEXT index. Compatible indexes over the same column set and
persisted tokenizer configuration share a corpus identity. Document count,
total token count and each term's document frequency are maintained
incrementally in internal MVCC counter rows. A document contributes once to a
term's frequency, even when the term repeats or appears in multiple columns.
NULL and empty documents still contribute to the document count.

Counters are written in the data transaction and participate in savepoints,
rollback, XA, WAL recovery, physical replication, checkpoints and backups.
Readers see counters from their own statement or transaction snapshot,
including their own writes. Changing only a partial-index predicate column
does not change the corpus. New FULLTEXT indexes initialize counters during
DDL; queries never initialize them by scanning user rows. For views without
uncommitted changes, cold-cache and write-first scoring read only the requested
terms' counter shards. Inside a write transaction, scoring combines the
snapshot counters with that transaction's modified document versions. This
extra work scales with its own changes and preserves RR, savepoint and
read-your-writes semantics without scanning unrelated rows. Statistics space
and write cost depend on the token vocabulary; reads touch at most 64 counter
rows per requested term and physical partition, plus the document count and
length counters. Different tokenizer configurations retain separate corpora.
Index builds, physical rewrites and partition exchanges retain a publication
fence. A snapshot older than that fence uses the visible-document oracle,
including rows removed before the index was built. It does not read a newer
corpus as though it existed in the old snapshot. The normal current-view
statistics path remains incremental.

Temporary FULLTEXT tables keep their counters in session temporary storage,
with the owning table's storage and undo quotas. Temporary counters are
removed with DROP/TRUNCATE and are not persisted or replicated.

## Execution paths and limits

Ordinary nonpartitioned queries scan B-tree inverted postings. Membership-only
queries skip scoring state, and phrase clauses share posting and position
caches across repeated terms. Multi-column indexing prevents phrases from
crossing column boundaries.

Partitioned and `FOR SYSTEM_TIME AS OF` full-text queries scan MVCC-visible
text and build query-local postings. This preserves partition identity and
historical membership and computes their corpus statistics from visible
documents. These paths cost a corpus scan and can require more memory than
an ordinary inverted-index lookup. `EXPLAIN` identifies the heap oracle path;
memory admission failures return an explicit error. Partitioned indexes also
maintain per-partition postings on writes, so applications should account for
that write cost even when searches use the visible-text heap oracle.

`MATCH` supports UPDATE/DELETE filtering and ordering, including transactional
rollback, and ordinary JOIN filtering, score projection, and ordering. Aggregated
MATCH in HAVING remains unsupported and returns an explicit error.

Single-table `FOR UPDATE` and `FOR SHARE` reads support MATCH filtering and
score projection through the same full-text runtime. MATCH in a locking JOIN
query remains unsupported and returns error 1235.

Offline ADD FULLTEXT builds hold an exclusive table barrier for the build.
Concurrent readers and writers may wait or receive a lock timeout; this path
does not promise MySQL's concurrent-reader behavior during an InnoDB FTS build.

Online `OPTIMIZE TABLE` for partitioned tables with `FULLTEXT` indexes remains
unsupported and returns an explicit error. Immutable backup queries support
filtering and score projection; see [Backup and Recovery](backup-and-recovery.md#51-fulltext-queries).

## Persistence and failures

FULLTEXT indexes are important persistent objects. Their posting B-trees,
scoring payloads, persisted corpus counters, tokenizer configuration and positional data participate in
transactional maintenance, checkpoints, backup and recovery. The ability to
reconstruct an index does not make a failed write or flush optional.

Malformed posting payloads return an index corruption error; token, prefix,
phrase and scoring paths never silently skip malformed entries. Detected
corruption fences the persistent index and preserves its files for explicit
repair or restore. A normal open does not silently replace a damaged FULLTEXT
index with a heap reconstruction. A missing derived index file without a
corruption fence can be rebuilt from the heap during recovery. A deliberately scheduled logical redo rebuild
is a separately recorded recovery obligation whose failure prevents opening.

## Diagnostic tables

Set `innodb_ft_aux_table='database/table'` to inspect every FULLTEXT index on a
table through `INNODB_FT_INDEX_TABLE` and `INNODB_FT_CONFIG`. The optional mydb
extension `database/table/index` selects one index. DOC_ID is a diagnostic
identity synthesized from storage and row identifiers and can change on row
migration; POSITION counts tokens rather than InnoDB byte offsets.

## Partial indexes and corpus statistics

FULLTEXT supports `WHERE` predicates on index definitions; see [partial indexes](partial-indexes.md).
BM25 uses the whole visible table for document count, average token length and term document frequency. Candidate filtering does not redefine the scoring corpus. Complete indexes, partial indexes and visible-heap oracles use the same baseline. Corpus metadata is cached by table definition, indexed column set, tokenizer configuration and read snapshot. Cold metadata is computed with a streaming scan; cache entries retain counts rather than documents and are released when their database closes.
