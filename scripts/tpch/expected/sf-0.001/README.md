# TPC-H sf-0.001 Expected Results

- Source engine: PostgreSQL 18.3
- Data source: `/tmp/mydb-tpch-dbgen-cache/sf-0.001`
- Scale factor: `0.001`
- Note: this tiny fixture deduplicates `partsupp` on `(ps_partkey, ps_suppkey)` so the data can load through the current schema.
- Generated at: `2026-05-17T13:55:24Z`
- Format: tab-separated rows without headers. Cells are trimmed; numeric values are canonicalized to integer form or 4 decimal places with half-up rounding.
