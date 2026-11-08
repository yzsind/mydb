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
FIXTURE_ROOT = ROOT / "tests/fixtures/function-compat"
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

# Extensions with dedicated SQL integration suites rather than oracle fixtures.
# Keep these explicit so a newly registered function still requires coverage.
INTEGRATION_SUITES = {
    "ai.rs": {"AI_GENERATE", "AI_GENERATE_DETAIL", "AI_EMBED", "AI_EMBED_DETAIL"},
    "xml_functions.rs": {"EXTRACTVALUE", "UPDATEXML"},
    "rls.rs": {"RLS_CONTEXT"},
    "vector_datatypes.rs": {
        "VECTOR_DATATYPE", "VECTOR_FROM_BINARY", "VECTOR_TO_BINARY", "JACCARD_DISTANCE",
    },
    "vector_quality.rs": {"HAMMING_DISTANCE"},
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
    # These handlers dispatch before the scalar match and must not disappear
    # from the inventory merely because they have a separate implementation.
    ai_source = (ROOT / "src/engine/ai/runtime.rs").read_text()
    ai_body = ai_source.split("pub(crate) fn is_function(", 1)[1].split("\n}", 1)[0]
    ai = set(re.findall(r'"(AI_[A-Z_]+)"', ai_body))
    xml_source = (ROOT / "src/engine/expr/xml.rs").read_text()
    xml_body = xml_source.split("pub(crate) fn is_xml(", 1)[1].split("\n}", 1)[0]
    xml = set(re.findall(r'"([A-Z_]+)"', xml_body))
    return scalar | geo | vector | aggregate | WINDOW_NAMES | ai | xml


def documented_names() -> set[str]:
    names: set[str] = set()
    for path in DOC_ROOT.glob("*/README.md"):
        names.update(re.findall(
            r'^(?:\| |[-*] )\[`?([A-Z][A-Z0-9_]*)`?\]', path.read_text(), re.M
        ))
    return names


def check_documentation_pages(errors: list[str]) -> None:
    linked: set[Path] = set()
    for index in DOC_ROOT.glob("*/README.md"):
        for name, destination in re.findall(
            r'^(?:\| |[-*] )\[`?([A-Z][A-Z0-9_]*)`?\]\(([^)]+)\)',
            index.read_text(), re.M,
        ):
            page = (index.parent / destination).resolve()
            linked.add(page)
            if not page.is_file():
                errors.append(f"{index}: missing function page {destination}")
            elif not re.match(rf"# `?{re.escape(name)}`?\s*\n", page.read_text()):
                errors.append(f"{page}: title does not match index name {name}")
    for page in DOC_ROOT.glob("*/*.md"):
        if page.name != "README.md" and page.resolve() not in linked:
            errors.append(f"{page}: function page is not linked by its category")


def integration_names(errors: list[str]) -> set[str]:
    names: set[str] = set()
    for filename, expected in INTEGRATION_SUITES.items():
        path = TEST_ROOT.parent / filename
        source = path.read_text()
        if "#[test]" not in source:
            errors.append(f"{path}: no integration tests")
        for name in expected:
            if not re.search(rf"\b{re.escape(name)}\s*\(", source, re.I):
                errors.append(f"{path}: missing SQL coverage for {name}")
        names.update(expected)
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
    check_documentation_pages(errors)
    # External HTTP functions have deterministic loopback integration tests,
    # rather than MySQL oracle fixtures (MySQL has no corresponding functions).
    catalog = (ROOT / "src/engine/builtin_catalog.rs").read_text()
    external = set(re.findall(r'\("(HTTP_[A-Z_]+)",\s*"http_[a-z_]+"\)', catalog))
    registered |= external
    external_tests = names_from_const(ROOT / "src/engine/executor/tests/http.rs", "HTTP_TEST_FUNCTIONS")
    if external_tests != external:
        errors.append("HTTP function registrations and live integration coverage differ")
    if not external.issubset(documented):
        errors.append("HTTP function documentation is incomplete")
    integration = integration_names(errors) | external

    if len(tests) != len(set(tests)):
        errors.append("duplicate function_fixture_test invocation")
    surfaces = {
        "registered": registered,
        "documented": documented,
        "tests": set(tests) | integration,
        "fixtures or integration": set(fixtures) | integration,
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
        f"fixtures={len(fixtures)} tests={len(tests)} "
        f"integration={len(integration)} errors={len(errors)}"
    )
    for error in errors:
        print(f"ERROR: {error}")
    return 1 if errors and args.check else 0


if __name__ == "__main__":
    raise SystemExit(main())
