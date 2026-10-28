#!/usr/bin/env bash
set -euo pipefail

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

cd "$(dirname "$0")/.."
source_file="src/storage/format_versions.rs"
output_file="docs/storage-format-versions.md"

version() {
  local name="$1"
  local value
  value="$(sed -nE "s/^pub const ${name}: [^=]+ = ([0-9]+);$/\\1/p" "$source_file")"
  if [[ -z "$value" ]]; then
    echo "missing format constant ${name} in ${source_file}" >&2
    exit 1
  fi
  echo "$value"
}

text_format() {
  local value
  value="$(sed -nE "s/^pub const ${1}: .* = \"([^\"]+)\";$/\\1/p" "$source_file")"
  if [[ -z "$value" ]]; then
    echo "missing format constant ${1} in ${source_file}" >&2
    exit 1
  fi
  echo "$value"
}

tmp_file="$(mktemp /tmp/mydb/storage-format-versions.XXXXXX)"
trap 'rm -f "$tmp_file"' EXIT

cat >"$tmp_file" <<EOF
<!-- Automatically generated; do not edit. -->
# Storage Format Versions

The current development format is incompatible with earlier versions. If versions do not match, use a new \`MYDB_DATA_DIR\` or rebuild the data directory; mydb does not migrate it silently.

| Format | Version |
| --- | --- |
| catalog / WAL | $(version CATALOG_VERSION) / $(version WAL_VERSION) |
| heap page / MVCC row | $(version HEAP_PAGE_VERSION) / $(version MVCC_ROW_VERSION) |
| B-tree index file / page | $(version INDEX_VERSION) / $(version INDEX_PAGE_VERSION) |
| undo binary / tablespace | $(version UNDO_BINARY_VERSION) / $(version UNDO_TABLESPACE_VERSION) |
| overflow | $(version OVERFLOW_VERSION) |
| paged vector container (HNSW / IVF) index / page | $(version PAGED_HNSW_INDEX_VERSION) / $(version PAGED_HNSW_PAGE_VERSION) |
| Zone Map file / page | $(version ZONE_MAP_VERSION) / $(version PAGED_ZONE_MAP_PAGE_VERSION) |
| legacy in-memory HNSW snapshot | $(version IN_MEMORY_HNSW_VERSION) |
| native replication metadata | $(version NATIVE_REPLICATION_METADATA_VERSION) |
| commit clock watermark | $(version COMMIT_CLOCK_VERSION) |
| native backup image / mode pointer | $(version NATIVE_IMAGE_VERSION) / $(version IMAGE_MODE_VERSION) |
| backup repository | $(text_format BACKUP_REPOSITORY_FORMAT) |
| backup bundle | $(version BACKUP_BUNDLE_VERSION) |
| backup catalog checkpoint / packed lineage | $(version BACKUP_CHECKPOINT_VERSION) / $(version BACKUP_LINEAGE_VERSION) |

Spatial indexes use persistent R-tree pages (format version 1), covered by the WAL and replication format boundary.
EOF

case "${1:---check}" in
  --write)
    cp "$tmp_file" "$output_file"
    ;;
  --check)
    if ! cmp -s "$tmp_file" "$output_file"; then
      diff -u "$output_file" "$tmp_file" || true
      echo "storage format version documentation is stale; run $0 --write" >&2
      exit 1
    fi
    ;;
  *)
    echo "usage: $0 [--check|--write]" >&2
    exit 2
    ;;
esac
