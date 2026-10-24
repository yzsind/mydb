#!/usr/bin/env python3
"""Rewrite core information_schema column constants from the 8.0.46 fixture.

This is a checked-in-code generator, not a build-time dependency.  It only
replaces constants referenced by MySQL 8.0 Community registry entries and
leaves legacy/plugin/mydb extension definitions untouched.
"""

from __future__ import annotations

import argparse
import collections
import json
import pathlib
import re
import subprocess

from _mysql80_metadata import (
    COLUMN_METADATA_DEFAULTS,
    render_column_metadata_lookup,
    system_column,
)


ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src/engine/information_schema.rs"
FIXTURE = ROOT / "docs/fixtures/mysql80-information-schema-8.0.46.json"
CHARACTER_METADATA_SOURCE = (
    ROOT / "src/engine/information_schema_mysql80_character_metadata.rs"
)
KEYWORDS_SOURCE = ROOT / "src/engine/information_schema_mysql80_keywords.rs"


def rust_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def option(value: object) -> str:
    return "None" if value is None else f"Some({value})"


def result_type(column: dict[str, object]) -> str:
    data_type = str(column["data_type"])
    if data_type == "tinyint":
        return "ColumnType::Tiny"
    if data_type == "smallint":
        return "ColumnType::Short"
    if data_type == "mediumint":
        return "ColumnType::MediumInt"
    if data_type == "int":
        return "ColumnType::Long"
    if data_type == "bigint":
        return "ColumnType::LongLong"
    if data_type == "float":
        return "ColumnType::Float"
    if data_type == "double":
        return "ColumnType::Double"
    if data_type == "decimal":
        precision = int(column["numeric_precision"] or 65)
        scale = int(column["numeric_scale"] or 0)
        return f"ColumnType::Decimal {{ precision: {precision}, scale: {scale} }}"
    if data_type == "datetime":
        return "ColumnType::DateTime"
    if data_type == "timestamp":
        return "ColumnType::Timestamp"
    if data_type == "json":
        return "ColumnType::Json"
    if data_type in {"blob", "text", "mediumtext", "longtext"}:
        return "ColumnType::Blob"
    if data_type in {"binary", "varbinary", "char", "varchar", "enum", "set"}:
        return "ColumnType::VarString"
    raise SystemExit(f"unsupported information_schema data type {data_type!r}")


def render_constant(name: str, columns: list[dict[str, object]]) -> str:
    rendered = [f"const {name}: &[SystemColumn] = &["]
    for column in columns:
        length = column["character_maximum_length"]
        octets = column["character_octet_length"]
        if (
            column["data_type"] == "varchar"
            and length is not None and octets is not None
            and column["column_type"] == f"varchar({length})"
            and all(column[key] is None for key in
                    ("numeric_precision", "numeric_scale", "datetime_precision"))
        ):
            nullable = str(column["is_nullable"] == "YES").lower()
            rendered.append(
                f'    varchar_column!({rust_string(column["column_name"])}, '
                f'{length}, {octets}, {nullable}),'
            )
        elif (column["data_type"] in {"char", "varchar"}
              and length is not None and octets is not None):
            nullable = str(column["is_nullable"] == "YES").lower()
            rendered.append(
                f'    string_column!({rust_string(column["column_name"])}, '
                f'{rust_string(column["data_type"])}, {rust_string(column["column_type"])}, '
                f'{length}, {octets}, {nullable}),'
            )
        else:
            rendered.extend(system_column(column, result_type(column)))
    rendered.append("];\n")
    return "\n".join(rendered)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    source = SOURCE.read_text(encoding="utf-8")
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    if fixture.get("oracle_mysql_version") != "8.0.46":
        raise SystemExit("fixture is not Oracle MySQL 8.0.46")

    columns_by_table: dict[str, list[dict[str, object]]] = collections.defaultdict(list)
    for column in fixture["columns"]:
        columns_by_table[str(column["table_name"]).lower()].append(column)

    registry_start = source.index("const INFORMATION_SCHEMA_REGISTRY")
    registry_end = source.index("const fn registration", registry_start)
    registry = source[registry_start:registry_end]
    mapping: dict[str, list[str]] = collections.defaultdict(list)
    call = re.compile(
        r"\b(?:def|empty_def)\s*\(\s*\"([^\"]+)\"\s*,\s*([A-Z0-9_]+_COLUMNS)",
        re.S,
    )
    for table_name, constant_name in call.findall(registry):
        normalized_table = table_name.lower()
        if normalized_table in columns_by_table:
            mapping[constant_name].append(normalized_table)

    mapped_tables = {table for tables in mapping.values() for table in tables}
    # Deliberately unsupported facilities are absent from the runtime registry.
    excluded_tables = {"resource_groups", "tablespaces", "tablespaces_extensions"}
    expected_tables = set(columns_by_table) - excluded_tables
    if mapped_tables != expected_tables:
        missing = sorted(expected_tables - mapped_tables)
        raise SystemExit(f"registry/fixture mismatch, missing tables: {missing}")

    canonical_names = {
        str(table["name"]).lower(): str(table["name"]) for table in fixture["tables"]
    }
    for normalized, canonical in canonical_names.items():
        registry = re.sub(
            rf'(\b(?:def|empty_def)\s*\(\s*)"{re.escape(normalized)}"',
            lambda match: f'{match.group(1)}{rust_string(canonical)}',
            registry,
            count=1,
            flags=re.I,
        )
    source = source[:registry_start] + registry + source[registry_end:]

    for constant_name, tables in mapping.items():
        reference = columns_by_table[tables[0]]
        reference_shape = [
            (
                column["column_name"],
                column["column_type"],
                column["is_nullable"],
            )
            for column in reference
        ]
        for table in tables[1:]:
            shape = [
                (
                    column["column_name"],
                    column["column_type"],
                    column["is_nullable"],
                )
                for column in columns_by_table[table]
            ]
            if shape != reference_shape:
                raise SystemExit(
                    f"shared constant {constant_name} has divergent tables {tables}"
                )

        pattern = re.compile(
            rf"const {re.escape(constant_name)}: &\[SystemColumn\]\s*=\s*&\[.*?\];(?:\n|$)",
            re.S,
        )
        source, replacements = pattern.subn(
            lambda _: render_constant(constant_name, reference), source, count=1
        )
        if replacements != 1:
            raise SystemExit(f"could not uniquely replace {constant_name}")

    metadata = [
        "// @generated by scripts/generate_mysql80_information_schema_rust.py",
        "// Oracle MySQL 8.0.46 Community per-column metadata.",
        "",
        "#[derive(Clone, Copy, Debug, PartialEq, Eq)]",
        "pub(crate) struct ColumnMetadata {",
        "    pub default_value: Option<&'static str>,",
        "    pub describe_default: Option<&'static str>,",
        "    pub character_set: Option<&'static str>,",
        "    pub collation: Option<&'static str>,",
        "    pub column_key: &'static str,",
        "    pub extra: &'static str,",
        "    pub privileges: &'static str,",
        "    pub comment: &'static str,",
        "    pub generation_expression: &'static str,",
        "    pub srs_id: Option<i64>,",
        "}",
        "",
        "impl ColumnMetadata {",
        "    pub(crate) const EMPTY: Self = Self {",
        *(f"        {key}: {value}," for key, value in COLUMN_METADATA_DEFAULTS.items()),
        "    };",
        "}",
        "",
    ]
    metadata.extend(render_column_metadata_lookup(
        (str(column["table_name"]), column, column.get("describe_default"))
        for column in fixture["columns"]
    ))
    keywords = [
        "// @generated by scripts/generate_mysql80_information_schema_rust.py",
        "// Oracle MySQL 8.0.46 Community KEYWORDS rows.",
        "",
        "pub(crate) const KEYWORDS: &[(&str, bool)] = &[",
    ]
    for keyword in fixture["keywords"]:
        keywords.append(
            f"    ({rust_string(str(keyword['word']))}, "
            f"{str(bool(keyword['reserved'])).lower()}),"
        )
    keywords.extend(["];", ""])
    outputs = {
        SOURCE: source,
        CHARACTER_METADATA_SOURCE: "\n".join(metadata),
        KEYWORDS_SOURCE: "\n".join(keywords),
    }
    # Format and validate all artifacts before publishing any of them.
    formatted = {
        path: subprocess.run(
            ["rustfmt", "--edition", "2021", "--emit", "stdout"],
            input=text, text=True, capture_output=True, cwd=ROOT, check=True,
        ).stdout
        for path, text in outputs.items()
    }
    for path, text in formatted.items():
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != text:
                raise SystemExit(f"generated Information Schema catalog is stale: {path}")
        else:
            path.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
