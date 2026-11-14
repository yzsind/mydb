# Documentation Scope and Maintenance

Documentation in this checkout describes its source revision. MyDB 1.2.1 is a
public preview, while the first production-ready version remains in development.
Use the matching release tag for release-specific instructions and physical
formats. The feature and compatibility references describe supported behavior;
listing a MySQL-shaped object does not promise an implemented subsystem.

## Sources and publication

| Reference | Source and update trigger |
| --- | --- |
| Startup options | startup_config registry; regenerate option reference after changing registration |
| Extension errors | error catalog registration; regenerate error table after adding/removing an error |
| Storage versions | format_versions constants; regenerate whenever physical format changes |
| Metadata matrix | information_schema registry plus explicit per-object security/replication classification |
| Protocol matrix | protocol/server/session/binlog sources and named regression suites; refresh evidence after review |
| REST schema | SQL-service codec/runtime and REST routes; validate schema and live JSON/NDJSON examples |
| Function index | scalar/math/GIS/vector/aggregate/window registries and explicit internal/unsupported exclusions |
| Statement index | public parser variants and explicit family-to-reference mapping |
| Performance claims | one identified experiment, effective settings and public result table |
| Docker Hub copy | release publication text, updated with README and linked performance evidence |

Run from the repository root:

```bash
python3 scripts/check_docs.py
python3 scripts/check_function_test_coverage.py --check
python3 scripts/check_documentation_contracts.py
python3 scripts/generate_mysql80_metadata_compat_matrix.py --check
python3 scripts/generate_public_references.py --check
bash scripts/render_storage_format_versions.sh --check
```

The contract checker requires jsonschema and openapi-spec-validator. The local
smoke runner additionally requires PyMySQL and cryptography; install them in a
virtual environment. Run `python3 scripts/test_documentation_examples.py --help`
for a bounded rehearsal on fresh local instances. It never attaches to an
existing database. Temporary data and logs go under /tmp/mydb.

## Example classes

Pure expressions may have fixed outputs. Stateful DDL/DML needs a resettable
schema. File examples need explicit server/client paths. AI/HTTP examples need
configured services and egress policy. Backup/replication/restart/purge examples
need isolated instances and asynchronous completion checks. Stored-program body
snippets are not standalone statements. No tool should execute every fenced block
indiscriminately; the smoke runner selects named complete examples, while the
function fixture suites cover the registered pure function surface.

Link validation does not establish SQL correctness, external-link availability,
benchmark reproducibility, or production readiness. Record command output and
remaining environment-specific checks in docs-internal after a documentation change.
