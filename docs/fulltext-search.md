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

## Search modes

Natural-language mode returns the union of indexed query terms. Repeated query
terms contribute once. The default parser lowercases Unicode text, splits at
non-alphanumeric boundaries while retaining underscores, and applies token
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

Grouping `()`, weights `> < ~`, and proximity `@` return error 1235.
Malformed operands, repeated leading signs, and unclosed double quotes return
error 1064. `WITH QUERY EXPANSION` is not supported and returns a parse error.

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

BM25 uses posting term frequency and document length. Indexed reads estimate
the document count from table statistics and committed row deltas. They use
the corpus average length collected by `ANALYZE TABLE` when available, and
otherwise approximate it from matching documents. Run `ANALYZE TABLE` after
large corpus changes to refresh these estimates.

Scores do not promise numeric equality with MySQL. Ordinary index statistics
and posting document frequencies are not fully versioned; concurrent corpus
changes can affect ranking even when a document's visible text stays the same.
Token normalization currently uses Unicode lowercasing and does not implement
every SQL collation's accent and case rules.

The `MYDB_FT_MIN_TOKEN_SIZE`, `MYDB_FT_MAX_TOKEN_SIZE`, `MYDB_FT_NGRAM_SIZE`,
`MYDB_FT_STOPWORDS`, `MYDB_FT_POSITIONS`, and `MYDB_FT_POSITION_GAP` process
defaults are captured in each new index. Later process-default changes do not
change that index's key encoding or tokenizer. The inter-column position gap
is at least one. Indexes without stored positions verify phrases against
visible row text. `MYDB_FT_BM25_K1` and `MYDB_FT_BM25_B` configure process-wide
ranking parameters, defaulting to 1.2 and 0.75.

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
memory admission failures return an explicit error.

Online `OPTIMIZE TABLE` for partitioned tables with `FULLTEXT` indexes remains
unsupported and returns an explicit error. Immutable backup queries support
filtering and score projection; see [Backup and Recovery](backup-and-recovery.md#51-fulltext-queries).

## Persistence and failures

FULLTEXT indexes are important persistent objects. Their posting B-trees,
scoring payloads, tokenizer configuration and positional data participate in
transactional maintenance, checkpoints, backup and recovery. The ability to
reconstruct an index does not make a failed write or flush optional.

Malformed posting payloads return an index corruption error; token, prefix,
phrase and scoring paths never silently skip malformed entries. Detected
corruption fences the persistent index and preserves its files for explicit
repair or restore. A normal open does not silently replace a damaged FULLTEXT
index with a heap reconstruction. A deliberately scheduled logical redo rebuild
is a separately recorded recovery obligation whose failure prevents opening.
