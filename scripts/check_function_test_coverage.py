#!/usr/bin/env python3
"""Check function docs, registrations, oracle fixtures and Rust tests agree."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC_ROOT = ROOT / "docs/function"
TEST_ROOT = ROOT / "src/engine/executor/tests/function_compat"
FIXTURE_ROOT = TEST_ROOT / "fixtures"
INTERNAL_SCALAR_NAMES = {"CHAR_USING", "EXTRACT_COMPOUND", "WEIGHT_STRING_TYPED"}
UNSUPPORTED_SPATIAL = {
    "ST_BUFFER",
    "ST_UNION",
    "ST_INTERSECTION",
    "ST_DIFFERENCE",
    "ST_SYMDIFFERENCE",
}
WINDOW_NAMES = {
    "ROW_NUMBER",
    "RANK",
    "DENSE_RANK",
    "NTILE",
    "PERCENT_RANK",
    "CUME_DIST",
    "LAG",
    "LEAD",
    "FIRST_VALUE",
    "LAST_VALUE",
    "NTH_VALUE",
}


def names_from_const(path: Path, const_name: str) -> set[str]:
    text = path.read_text()
    block = text[text.index(f"const {const_name}") :]
    block = block[: block.index("];")]
    return set(re.findall(r'"([A-Z0-9_]+)"', block))


def registered_names() -> set[str]:
    expr = (ROOT / "src/engine/expr.rs").read_text()
    body = expr[
        expr.index("pub(crate) fn eval_function") : expr.index(
            "\npub(crate) fn expect_arg_count"
        )
    ]
    scalar: set[str] = set()
    for line in body.splitlines():
        match = re.match(r'^        ((?:"[A-Z0-9_]+"(?: \| )?)+) =>', line)
        if match:
            scalar.update(re.findall(r'"([A-Z0-9_]+)"', match.group(1)))
    scalar = {
        name for name in scalar if not name.startswith("__")
    } - INTERNAL_SCALAR_NAMES
    # Transcendental functions dispatch through the shared math registry before
    # the scalar match; keep this inventory tied to its from_name mapping.
    math = (ROOT / "src/engine/expr/math.rs").read_text()
    math = math[math.index("fn from_name(") : math.index("\n    pub(crate) fn name(")]
    scalar.update(re.findall(r'"([A-Z0-9_]+)"\s*=>\s*Self::', math))

    geo_source = (ROOT / "src/engine/geo/sql.rs").read_text()
    geo_match = re.search(r"spatial_functions!\s*\{(.*?)\n\}", geo_source, re.S)
    if geo_match is None:
        raise ValueError("missing spatial_functions registry invocation")
    geo = set(re.findall(r'"([A-Z0-9_]+)"', geo_match.group(1)))
    geo -= UNSUPPORTED_SPATIAL
    vector = names_from_const(ROOT / "src/engine/vector/sql.rs", "VEC_FUNCTIONS")

    convert = (ROOT / "src/parser/convert.rs").read_text()
    aggregate_block = convert[convert.index("pub fn is_aggregate_function") :]
    aggregate_block = aggregate_block[: aggregate_block.index("\n}")]
    aggregate = set(re.findall(r'"([A-Z0-9_]+)"', aggregate_block))
    aggregate.discard("__HIDDEN_FIRST_VALUE")
    return scalar | geo | vector | aggregate | WINDOW_NAMES


def documented_names() -> set[str]:
    names: set[str] = set()
    for path in DOC_ROOT.glob("*/README.md"):
        names.update(re.findall(r'^\| \[`([A-Z][A-Z0-9_]*)`\]', path.read_text(), re.M))
    return names


def test_names() -> list[str]:
    names = []
    for path in TEST_ROOT.glob("*.rs"):
        if path.name == "mod.rs":
            continue
        names.extend(
            re.findall(
                r'function_fixture_test!\(\s*[a-z0-9_]+,\s*"([A-Z][A-Z0-9_]*)",\s*FIXTURE\s*\);',
                path.read_text(),
                re.S,
            )
        )
    return names


def fixture_functions(errors: list[str]) -> dict[str, dict[str, object]]:
    functions: dict[str, dict[str, object]] = {}
    for path in sorted(FIXTURE_ROOT.glob("*.json")):
        fixture = json.loads(path.read_text())
        if fixture.get("schema_version") != 1:
            errors.append(f"{path}: schema_version must be 1")
        if fixture.get("mysql_version") != "8.0.46":
            errors.append(f"{path}: mysql_version must be 8.0.46")
        for function in fixture.get("functions", []):
            name = function.get("name")
            if name in functions:
                errors.append(f"duplicate fixture function {name}")
            functions[name] = function
    return functions


def required_tags(profile: str) -> set[str]:
    if profile == "alias":
        return {"nominal", "metadata", "boundary", "invalid", "wrong_arity"}
    if profile == "no_arg":
        return {"nominal", "metadata", "boundary", "invalid", "wrong_arity"}
    if profile in {"aggregate", "window"}:
        return {"nominal", "metadata", "null", "boundary", "invalid"}
    if profile == "stateful":
        return {"nominal", "metadata", "boundary", "invalid"}
    return {"nominal", "metadata", "null", "boundary", "invalid", "column"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail on any mismatch")
    args = parser.parse_args()
    errors: list[str] = []
    registered = registered_names()
    documented = documented_names()
    tests = test_names()
    fixtures = fixture_functions(errors)

    if len(tests) != len(set(tests)):
        errors.append("duplicate function_fixture_test invocation")
    surfaces = {
        "registered": registered,
        "documented": documented,
        "tests": set(tests),
        "fixtures": set(fixtures),
    }
    union = set().union(*surfaces.values())
    for label, names in surfaces.items():
        missing = sorted(union - names)
        extra = sorted(names - union)
        if missing:
            errors.append(f"{label} missing: {' '.join(missing)}")
        if extra:
            errors.append(f"{label} extra: {' '.join(extra)}")

    for name, function in fixtures.items():
        cases = function.get("cases", [])
        ids = [case.get("id") for case in cases]
        if len(ids) != len(set(ids)):
            errors.append(f"{name}: duplicate case id")
        tags = {tag for case in cases for tag in case.get("tags", [])}
        required = required_tags(str(function.get("profile", "")))
        if missing := sorted(required - tags):
            errors.append(f"{name}: missing tags {' '.join(missing)}")
        if function.get("profile") == "alias" and not function.get("canonical"):
            errors.append(f"{name}: alias lacks canonical function")
        for case in cases:
            if case.get("expected", {}).get("status") == "uncaptured":
                errors.append(f"{name}/{case.get('id')}: oracle outcome is uncaptured")
            comparison = case.get("comparison", "exact")
            if comparison != "exact" and not case.get("difference_reason"):
                errors.append(
                    f"{name}/{case.get('id')}: {comparison} comparison lacks a reason"
                )
            if "metadata" in case.get("tags", []):
                metadata_comparison = case.get("metadata_comparison", "exact")
                if metadata_comparison != "exact" and not case.get(
                    "metadata_difference_reason"
                ):
                    errors.append(
                        f"{name}/{case.get('id')}: metadata contract lacks a reason"
                    )
                if (
                    case.get("comparison") != "offline_contract"
                    and case.get("mydb_probe", {}).get("status") == "ok"
                    and not case.get("mydb_probe", {}).get("columns")
                ):
                    errors.append(
                        f"{name}/{case.get('id')}: missing captured mydb column metadata"
                    )

    print(
        f"registered={len(registered)} documented={len(documented)} "
        f"fixtures={len(fixtures)} tests={len(tests)} errors={len(errors)}"
    )
    for error in errors:
        print(f"ERROR: {error}")
    return 1 if errors and args.check else 0


if __name__ == "__main__":
    raise SystemExit(main())
