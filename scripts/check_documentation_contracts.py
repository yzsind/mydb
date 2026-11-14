#!/usr/bin/env python3
"""Check public SQL coverage, protocol evidence and REST schema contracts."""

import hashlib
import json
import re
import sys
from pathlib import Path

from check_function_test_coverage import (
    check_documentation_pages,
    documented_names,
    registered_names,
)
from jsonschema import Draft202012Validator
from openapi_spec_validator import validate

ROOT = Path(__file__).resolve().parents[1]


def main():
    errors = []
    missing = registered_names() - documented_names()
    if missing:
        errors.append(f"Undocumented public functions: {sorted(missing)}")
    check_documentation_pages(errors)
    source = (
        (ROOT / "src/parser/ast.rs")
        .read_text()
        .split("pub enum Statement {", 1)[1]
        .split("\n}", 1)[0]
    )
    variants = set(re.findall(r"^    ([A-Z]\w*)\s*(?:\{|\(|,)", source, re.MULTILINE))
    mapping = json.loads(
        (ROOT / "scripts/documentation-statement-map.json").read_text()
    )
    if variants != mapping.keys():
        errors.append(
            f"Statement coverage drift: new={variants - mapping.keys()}, stale={mapping.keys() - variants}"
        )
    for name, entry in mapping.items():
        if not entry["references"]:
            errors.append(f"{name}: no reference")
        for p in entry["references"]:
            if not (ROOT / p).is_file():
                errors.append(f"{name}: missing {p}")
    spec = json.loads((ROOT / "docs/rest-openapi.json").read_text())
    validate(spec)

    def validator(name):
        return Draft202012Validator(
            {"$ref": "#/components/schemas/" + name, "components": spec["components"]}
        )

    valid = [
        {"sql": "SELECT 1"},
        {"sql": None, "statements": [{"sql": "SELECT 1"}], "params": []},
        {
            "sql": "SELECT ?",
            "params": [{"type": "uint64", "value": "18446744073709551615"}],
        },
    ]
    invalid = [
        {"sql": "SELECT 1", "statements": [{"sql": "SELECT 2"}]},
        {"statements": [{"sql": "SELECT 1"}], "params": [2]},
        {"sql": "SELECT ?", "params": [9007199254740992]},
        {"statements": []},
        {"sql": "SELECT 1", "timeout_ms": 0},
    ]
    for item in valid:
        validator("SqlRequest").validate(item)
    for item in invalid:
        if validator("SqlRequest").is_valid(item):
            errors.append(f"Invalid request accepted by schema: {item}")
    for name, schema in spec["components"]["schemas"].items():
        Draft202012Validator.check_schema(schema)
    matrix = json.loads((ROOT / "docs/mysql80-protocol-compat-matrix.json").read_text())
    for path, digest in matrix["evidence"]["source_sha256"].items():
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest:
            errors.append(f"Protocol evidence needs review: {path}")
    for path in matrix["evidence"]["test_sources"]:
        if not (ROOT / path).is_file():
            errors.append(f"Missing protocol test source: {path}")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(
        f"Checked {len(variants)} statement families, {len(registered_names())} function registrations and OpenAPI contracts."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
