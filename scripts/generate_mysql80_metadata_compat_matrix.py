#!/usr/bin/env python3
"""Generate the information_schema compatibility matrix from the registry."""

from __future__ import annotations

import argparse
import json
import pathlib
import re


ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src/engine/information_schema.rs"
OUTPUT = ROOT / "docs/mysql80-metadata-compat-matrix.json"

STATIC_OBJECTS = {
    "CHARACTER_SETS", "COLLATIONS", "COLLATION_CHARACTER_SET_APPLICABILITY",
    "ENGINES", "KEYWORDS", "PLUGINS", "ST_SPATIAL_REFERENCE_SYSTEMS",
    "ST_UNITS_OF_MEASURE", "INNODB_FT_DEFAULT_STOPWORD",
}
AUTH_OBJECTS = {
    "USER_PRIVILEGES", "SCHEMA_PRIVILEGES", "TABLE_PRIVILEGES",
    "COLUMN_PRIVILEGES", "APPLICABLE_ROLES",
    "ADMINISTRABLE_ROLE_AUTHORIZATIONS", "ENABLED_ROLES",
    "ROLE_TABLE_GRANTS", "ROLE_COLUMN_GRANTS", "ROLE_ROUTINE_GRANTS",
    "USER_ATTRIBUTES",
}
NODE_LOCAL_OBJECTS = {
    "PROCESSLIST", "OPTIMIZER_TRACE", "INNODB_TRX",
    "INNODB_LOCK_WAITS", "INNODB_BUFFER_PAGE", "INNODB_BUFFER_PAGE_LRU",
    "INNODB_BUFFER_POOL_STATS", "INNODB_CACHED_INDEXES", "INNODB_METRICS",
    "MYDB_CURSORS", "MYDB_CACHES", "MYDB_PLAN_CACHE_ENTRIES",
    "MYDB_MEMORY_ADVISOR", "MYDB_UNDO_HISTORY",
    "GLOBAL_VARIABLES", "SESSION_VARIABLES",
}
PROCESS_OBJECTS = {
    "FILES",
    *{name for name in NODE_LOCAL_OBJECTS if name.startswith("INNODB_")},
}


def registry_entries(source: str) -> list[str]:
    start = source.index("const INFORMATION_SCHEMA_REGISTRY")
    start = source.index("= &[", start) + 4
    end = source.index("\n];", start)
    body = source[start:end]
    entries: list[str] = []
    entry_start = 0
    depth = 0
    in_string = False
    escaped = False
    for index, char in enumerate(body):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            entry = body[entry_start:index].strip()
            if entry:
                entries.append(entry)
            entry_start = index + 1
    tail = body[entry_start:].strip()
    if tail:
        entries.append(tail)
    return entries


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Check the committed matrix without writing it."
    )
    args = parser.parse_args()
    source = SOURCE.read_text(encoding="utf-8")
    surfaces = []
    class_counts: dict[str, int] = {}
    for entry in registry_entries(source):
        function = entry.split("(", 1)[0].strip()
        literals = [
            json.loads('"' + value + '"')
            for value in re.findall(r'"((?:\\.|[^"\\])*)"', entry)
        ]
        if not literals:
            raise SystemExit(f"registry entry has no table name: {entry}")
        name = literals[0]
        compatibility_class = {
            "def": "mysql80_community",
            "empty_def": "mysql80_community",
            "legacy_def": "mysql_legacy_compat",
            "plugin_empty_def": "mysql_optional_plugin_compat",
            "mydb_def": "mydb_native",
        }.get(function)
        if compatibility_class is None:
            raise SystemExit(f"unknown registry constructor {function}")
        if compatibility_class == "mydb_native" and not re.fullmatch(
            r"MYDB_[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*", name
        ):
            raise SystemExit(f"invalid native metadata name: {name}")
        producer = "honest_empty" if "empty_def" in function else "real_producer"
        surface = {
            "schema": "information_schema",
            "object": name,
            "compatibility_class": compatibility_class,
            "shape": (
                "mysql80_46_exact"
                if compatibility_class == "mysql80_community"
                else "extension_stable"
            ),
            "producer": producer,
            "visibility": "object_policy",
        }
        if producer == "honest_empty":
            surface["reason"] = literals[-1]
            surface["data_source"] = "unsupported_subsystem"
            surface["replication_semantics"] = "not_applicable"
            surface["pushdown"] = []
            surface["test_coverage"] = ["registry_contract", "honest_empty_shape"]
        else:
            upper = name.upper()
            if upper in STATIC_OBJECTS:
                surface["data_source"] = "static_mysql80_catalog"
                surface["replication_semantics"] = "deterministic_static"
                surface["visibility"] = "public_static"
            elif upper in AUTH_OBJECTS:
                surface["data_source"] = "auth_catalog"
                surface["replication_semantics"] = "replicated_catalog"
                surface["visibility"] = "current_account_and_roles"
            elif upper.startswith("MYDB_AI_"):
                surface["data_source"] = (
                    "ai_runtime" if upper == "MYDB_AI_RUNTIME_STATUS" else "ai_catalog"
                )
                surface["replication_semantics"] = (
                    "node_local" if upper == "MYDB_AI_RUNTIME_STATUS"
                    else "replicated_catalog_with_node_local_key_availability"
                )
                surface["visibility"] = (
                    "ai_use_or_model_use_or_ai_admin" if upper == "MYDB_AI_MODELS"
                    else "ai_admin"
                )
            elif upper == "MYDB_ROW_SECURITY_POLICIES":
                surface["data_source"] = "row_security_catalog"
                surface["replication_semantics"] = "replicated_catalog"
                surface["visibility"] = "object_policy"
            elif upper == "MYDB_PREPARED_XA_TRANSACTIONS":
                surface["data_source"] = "prepared_xa_transactions"
                surface["replication_semantics"] = "node_local_recoverable_state"
                surface["visibility"] = "xa_recover_admin"
            elif upper in NODE_LOCAL_OBJECTS:
                surface["data_source"] = "node_runtime"
                surface["replication_semantics"] = "node_local"
                surface["visibility"] = (
                    "self_or_process" if upper in {"PROCESSLIST", "MYDB_CURSORS"}
                    else "process"
                )
            elif upper.startswith("INNODB_") or upper in {"FILES", "TABLESPACES", "TABLESPACES_EXTENSIONS"}:
                surface["data_source"] = "storage_catalog_mapping"
                surface["replication_semantics"] = "derived_physical"
                surface["visibility"] = "process"
            elif upper.startswith("MYDB_"):
                surface["data_source"] = "mydb_catalog_or_runtime"
                surface["replication_semantics"] = "classified_by_row"
            else:
                surface["data_source"] = "data_dictionary_catalog"
                surface["replication_semantics"] = "replicated_catalog"
            surface["pushdown"] = ["TABLE_SCHEMA", "TABLE_NAME"] if upper in {
                "TABLES", "COLUMNS", "STATISTICS", "MYDB_ZONE_MAPS"
            } else []
            if upper in {"COLUMNS", "MYDB_ZONE_MAPS"}:
                surface["pushdown"].append("COLUMN_NAME")
            surface["test_coverage"] = ["registry_contract", "producer_scenario"]
        surfaces.append(surface)
        class_counts[compatibility_class] = class_counts.get(compatibility_class, 0) + 1

    # Pin the external contract exactly; native extensions are validated and
    # generated from registrations without a second hard-coded inventory.
    fixture = json.loads((ROOT / "tests/fixtures/mysql80/mysql80-information-schema-8.0.46.json").read_text())
    excluded = {"RESOURCE_GROUPS", "TABLESPACES", "TABLESPACES_EXTENSIONS"}
    expected_core = {table["name"] for table in fixture["tables"]} - excluded
    actual_core = {s["object"] for s in surfaces if s["compatibility_class"] == "mysql80_community"}
    if actual_core != expected_core:
        raise SystemExit(f"MySQL contract mismatch: missing={expected_core - actual_core}, extra={actual_core - expected_core}")
    names = [s["object"].lower() for s in surfaces]
    if len(names) != len(set(names)):
        raise SystemExit("duplicate normalized information_schema name")
    payload = {
        "version": 2,
        "oracle_mysql_baseline": "8.0.46 Community",
        "description": (
            "Per-object information_schema compatibility matrix. Extra legacy, "
            "optional-plugin and mydb-native views do not weaken the 76-table supported core gate."
        ),
        "counts": class_counts,
        "surfaces": surfaces,
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text(encoding="utf-8") != rendered:
            parser.exit(1, f"{OUTPUT.relative_to(ROOT)} is out of date; rerun {parser.prog}\n")
    else:
        OUTPUT.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
