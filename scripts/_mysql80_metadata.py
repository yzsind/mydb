"""Shared Rust rendering for fixed MySQL metadata fixtures."""
from __future__ import annotations

from collections.abc import Iterable
import json


COLUMN_METADATA_DEFAULTS = {
    "default_value": "None",
    "describe_default": "None",
    "character_set": "None",
    "collation": "None",
    "column_key": '""',
    "extra": '""',
    "privileges": '"select"',
    "comment": '""',
    "generation_expression": '""',
    "srs_id": "None",
}

# Exact constructor contracts; argument-dependent types retain the full form.
SYSTEM_COLUMN_CONSTRUCTORS = {
    ("ColumnType::LongLong", "bigint", "bigint unsigned"): "unsigned_bigint",
    ("ColumnType::LongLong", "bigint", "bigint"): "bigint",
    ("ColumnType::Long", "int", "int"): "int",
    ("ColumnType::Long", "int", "int unsigned"): "unsigned_int",
    ("ColumnType::DateTime", "datetime", "datetime"): "datetime",
    ("ColumnType::Timestamp", "timestamp", "timestamp"): "timestamp",
    ("ColumnType::VarString", "binary", "binary(0)"): "empty_binary",
    ("ColumnType::Blob", "text", "text"): "text",
    ("ColumnType::Blob", "longtext", "longtext"): "longtext",
}


def rust_string(value: object) -> str:
    return json.dumps(str(value), ensure_ascii=False)


def option(value: object, *, string: bool = False) -> str:
    if value is None:
        return "None"
    return f"Some({rust_string(value) if string else int(str(value))})"


def system_column(column: dict[str, object], result_type: str) -> list[str]:
    fields = [
        rust_string(column["column_name"]),
        result_type,
        rust_string(column["data_type"]),
        rust_string(column["column_type"]),
        str(column["is_nullable"] == "YES").lower(),
    ]
    constructor = SYSTEM_COLUMN_CONSTRUCTORS.get(
        (result_type, str(column["data_type"]), str(column["column_type"]))
    )
    if constructor:
        expr = f"SystemColumn::{constructor}({fields[0]}, {fields[-1]})"
    else:
        expr = "SystemColumn::new(" + ", ".join(fields) + ")"
    attributes = [
        ("character_lengths", ("character_maximum_length", "character_octet_length")),
        ("numeric", ("numeric_precision", "numeric_scale")),
        ("temporal_precision", ("datetime_precision",)),
    ]
    for method, keys in attributes:
        if any(column[key] is not None for key in keys):
            args = ", ".join(option(column[key]) for key in keys)
            expr += f".{method}({args})"
    return ["    " + expr + ","]


def column_metadata(
    column: dict[str, object], *, describe_default: object,
) -> list[str]:
    values = {
        "default_value": option(column["column_default"], string=True),
        "describe_default": option(describe_default, string=True),
        "character_set": option(column["character_set_name"], string=True),
        "collation": option(column["collation_name"], string=True),
        "column_key": rust_string(column["column_key"]),
        "extra": rust_string(column["extra"]),
        "privileges": rust_string(column["privileges"]),
        "comment": rust_string(column["column_comment"]),
        "generation_expression": rust_string(column["generation_expression"]),
        "srs_id": option(column["srs_id"]),
    }
    fields = [
        f"            {key}: {value},"
        for key, value in values.items()
        if value != COLUMN_METADATA_DEFAULTS[key]
    ]
    return fields + ["            ..ColumnMetadata::EMPTY"]


def render_column_metadata_lookup(
    entries: Iterable[tuple[str, dict[str, object], object]],
) -> list[str]:
    """Share identical values while preserving every fixture lookup key.

    Preserve the original arm order and struct-literal expansion. Sharing the
    source through macros leaves runtime lookup code unchanged.
    """
    groups: dict[tuple[str, ...], int] = {}
    lookups: list[tuple[tuple[str, str], int]] = []
    keys: set[tuple[str, str]] = set()
    for table, column, describe_default in entries:
        key = (table, str(column["column_name"]))
        if key in keys:
            raise ValueError(f"duplicate column metadata key: {key!r}")
        keys.add(key)
        fields = tuple(column_metadata(column, describe_default=describe_default))
        index = groups.setdefault(fields, len(groups))
        lookups.append((key, index))

    out: list[str] = ["macro_rules! column_metadata_value {"]
    for index, fields in enumerate(groups):
        out.append(f"    ({index}) => {{ ColumnMetadata {{")
        out.extend(fields)
        out.append("    } };")
    out.extend(["}", ""])
    out.extend([
        "pub(crate) fn metadata(table: &str, column: &str) -> Option<ColumnMetadata> {",
        "    match (table, column) {",
    ])
    for (table, column), index in lookups:
        out.append(
            f"        ({rust_string(table)}, {rust_string(column)}) => Some(column_metadata_value!({index})),"
        )
    out.extend(["        _ => None,", "    }", "}", ""])
    return out
