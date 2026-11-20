"""Strict TSV comparison with explicit column types and ordering contracts."""
from __future__ import annotations
import base64
import csv
import decimal
import hashlib
import itertools
import json
import math
import re
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path


@dataclass
class CanonicalResult:
    rows: list[tuple[object, ...]]
    digest: str
    metadata: tuple = ()


def normalize_cell(value: str, kind: str = 'text', null_marker: str | None = None,
                   *, trim_text: bool = False, decimal_places: int | None = None) -> str | None:
    if null_marker is not None and value == null_marker:
        return None
    if kind == 'text':
        return value.strip() if trim_text else value
    if value != value.strip():
        raise ValueError(f'whitespace in numeric cell: {value!r}')
    try:
        number = decimal.Decimal(value)
    except decimal.InvalidOperation as error:
        raise ValueError(f'invalid {kind} cell: {value!r}') from error
    if not number.is_finite() or (kind == 'integer' and number != number.to_integral_value()):
        raise ValueError(f'invalid {kind} cell: {value!r}')
    if decimal_places is not None and kind != 'integer':
        # Lossy reference fixtures must explicitly declare their precision.
        # Size the local context for both the integer and fractional digits,
        # including a possible carry; never inherit the process-wide precision.
        with decimal.localcontext() as context:
            context.prec = max(1, len(number.as_tuple().digits), number.adjusted() + 1) + decimal_places + 1
            number = number.quantize(decimal.Decimal((0, (1,), -decimal_places)),
                                     rounding=decimal.ROUND_HALF_UP)
    # Formatting does not round to the process-wide Decimal context precision.
    if not number:
        return '0'
    text = format(number, 'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text


def validate_contract(contract: dict) -> None:
    if isinstance(contract, dict) and 'result_sets' in contract:
        if set(contract) != {'result_sets'}:
            raise ValueError('result_sets cannot be combined with top-level comparison options')
        sets = contract['result_sets']
        if not isinstance(sets, list) or not sets:
            raise ValueError('result_sets must be a nonempty list')
        for spec in sets:
            if (not isinstance(spec, dict) or type(spec.get('row_count')) is not int
                    or spec['row_count'] < 0 or not isinstance(spec.get('columns'), list)
                    or spec.get('format', 'tsv') != 'tsv' or 'result_sets' in spec):
                raise ValueError('each TSV result set requires a nonnegative row_count and explicit columns')
            validate_contract({key: value for key, value in spec.items() if key != 'row_count'})
        return
    if not isinstance(contract, dict) or set(contract) - {
            'columns', 'order_keys', 'unordered', 'null_marker', 'format', 'trim_text',
            'decimal_places', 'float_relative_tolerance'}:
        raise ValueError('unknown comparison contract fields')
    if contract.get('format', 'tsv') not in ('tsv', 'typed-json'):
        raise ValueError('format must be tsv or typed-json')
    if contract.get('format') == 'typed-json' and any(
            field in contract for field in ('columns', 'null_marker', 'trim_text', 'decimal_places')):
        raise ValueError('typed-json already carries types and NULL values')
    columns = contract.get('columns')
    if columns is not None and (not isinstance(columns, list) or not columns or
            any(kind not in ('text', 'integer', 'decimal', 'float') for kind in columns)):
        raise ValueError('columns must be a nonempty list of explicit scalar types')
    if type(contract.get('trim_text', False)) is not bool:
        raise ValueError('trim_text must be a boolean')
    if 'decimal_places' in contract and (type(contract['decimal_places']) is not int or
                                       contract['decimal_places'] < 0 or columns is None):
        raise ValueError('decimal_places requires explicit columns and a nonnegative integer')
    if 'float_relative_tolerance' in contract:
        tolerance = contract['float_relative_tolerance']
        if type(tolerance) not in (int, float) or not 0 < tolerance < 1:
            raise ValueError('float_relative_tolerance must be a number between zero and one')
        if (not columns or 'float' not in columns or contract.get('order_keys')
                or contract.get('unordered') or 'decimal_places' in contract):
            raise ValueError('float_relative_tolerance requires ordered TSV with float columns and no rounding')
    keys = contract.get('order_keys')
    if keys is not None and (not isinstance(keys, list) or not keys or
            any(type(key) is not int or key < 0 for key in keys) or len(keys) != len(set(keys))):
        raise ValueError('order_keys must contain distinct nonnegative column indices')
    if type(contract.get('unordered', False)) is not bool:
        raise ValueError('unordered must be a boolean')
    if contract.get('unordered') and keys is not None:
        raise ValueError('unordered and order_keys are mutually exclusive')
    if 'null_marker' in contract and not isinstance(contract['null_marker'], str):
        raise ValueError('null_marker must be a string')
    if keys and columns and max(keys) >= len(columns):
        raise ValueError('order key is outside the result columns')


def comparison_contracts(path: Path | None) -> dict[str, dict]:
    if path is None:
        return {}
    contracts = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(contracts, dict):
        raise ValueError('comparison manifest must map query numbers to contracts')
    for query, contract in contracts.items():
        if not re.fullmatch(r'[1-9][0-9]*', query):
            raise ValueError(f'invalid query number in comparison manifest: {query}')
        validate_contract(contract)
    return contracts


def reject_constant(value):
    raise ValueError(f'nonfinite JSON number: {value}')


def typed_cell(value):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, dict) and len(value) == 1:
        if 'decimal' in value and isinstance(value['decimal'], str):
            return {'decimal': normalize_cell(value['decimal'], 'decimal')}
        if 'bytes' in value and isinstance(value['bytes'], str):
            return {'bytes': base64.b64encode(base64.b64decode(value['bytes'], validate=True)).decode('ascii')}
        from clickbench_results import decode, encode
        if next(iter(value)) in ('datetime', 'date', 'time', 'timedelta'):
            return encode(decode(value))
    raise ValueError(f'unsupported typed-json cell: {value!r}')


def _ordered_rows(rows: list[tuple], contract: dict) -> list[tuple]:
    def serialized(row):
        return json.dumps(row, ensure_ascii=False, separators=(',', ':'))
    if contract.get('unordered'):
        rows.sort(key=serialized)
        return rows
    keys = contract.get('order_keys')
    if keys:
        # Keep the reference's group sequence. Reordering distinct ORDER BY
        # keys, including separating a tie group, must still fail comparison.
        return [row for _, group in itertools.groupby(rows, lambda row: serialized(tuple(row[i] for i in keys)))
                for row in sorted(group, key=serialized)]
    return rows


def _tsv_rows(source, path: Path, contract: dict) -> list[tuple]:
    columns = contract.get('columns')
    keys = contract.get('order_keys')
    width = len(columns) if columns else None
    rows = []
    # Only newline terminators are removed: blank records are real empty rows.
    for line in source:
        record = line[:-2] if line.endswith('\r\n') else line.removesuffix('\n')
        cells = record.split('\t')
        if width is None:
            width = len(cells)
        if len(cells) != width:
            raise ValueError(f'{path}: inconsistent result width: {len(cells)} != {width}')
        rows.append(tuple(normalize_cell(cell, columns[i] if columns else 'text',
                                        contract.get('null_marker'),
                                        trim_text=contract.get('trim_text', False),
                                        decimal_places=contract.get('decimal_places'))
                          for i, cell in enumerate(cells)))
    if keys and width is not None and max(keys) >= width:
        raise ValueError('order key is outside the result columns')
    return _ordered_rows(rows, contract)


def canonicalize(path: Path, *, contract: dict | None = None) -> CanonicalResult:
    contract = {} if contract is None else contract
    validate_contract(contract)
    keys = contract.get('order_keys')
    rows = []
    metadata = ()
    if contract.get('format') == 'typed-json':
        data = json.loads(path.read_text(encoding='utf-8'), parse_constant=reject_constant)
        if (not isinstance(data, dict) or not isinstance(data.get('columns'), list)
                or not all(isinstance(name, str) for name in data['columns'])
                or not isinstance(data.get('types'), list)
                or not all(type(code) is int for code in data['types'])
                or not data['columns'] or len(data['columns']) != len(data['types'])
                or not isinstance(data.get('rows'), list)):
            raise ValueError('typed-json requires column names, wire types and a row array')
        metadata = tuple(zip(data['columns'], data['types']))
        width = len(metadata)
        if 'sql_sha256' in data:
            if not isinstance(data['sql_sha256'], str) or not re.fullmatch('[0-9a-f]{64}', data['sql_sha256']):
                raise ValueError('invalid capture SQL hash')
            metadata += (('sql_sha256', data['sql_sha256']),)
        for row in data['rows']:
            if not isinstance(row, list) or len(row) != width:
                raise ValueError('typed-json row width does not match metadata')
            rows.append(tuple(typed_cell(cell) for cell in row))
        if keys and max(keys) >= width:
            raise ValueError('order key is outside the result columns')
        rows = _ordered_rows(rows, contract)
    elif 'result_sets' in contract:
        # Headerless batch TSV has no set delimiters. The reference contract
        # must provide the boundaries, including empty and equal-width sets.
        with path.open(encoding='utf-8', newline='') as source:
            for index, spec in enumerate(contract['result_sets'], 1):
                batch = _tsv_rows(itertools.islice(source, spec['row_count']), path, spec)
                if len(batch) != spec['row_count']:
                    raise ValueError(f'{path}: result set {index}: row count {len(batch)} != {spec["row_count"]}')
                rows.extend(batch)
            if next(source, None) is not None:
                raise ValueError(f'{path}: extra rows after declared result sets')
        metadata = ('result_sets', tuple((spec['row_count'], len(spec['columns']))
                                         for spec in contract['result_sets']))
    else:
        with path.open(encoding='utf-8', newline='') as source:
            rows = _tsv_rows(source, path, contract)
    payload = json.dumps([metadata, rows], ensure_ascii=False, separators=(',', ':'), sort_keys=True).encode()
    return CanonicalResult(rows, hashlib.sha256(payload).hexdigest(), metadata)


def _ordered_rows_match(expected: list[tuple], actual: list[tuple], contract: dict) -> bool:
    if 'float_relative_tolerance' not in contract:
        return expected == actual

    tolerance = Fraction(str(contract['float_relative_tolerance']))
    for expected_row, actual_row in zip(expected, actual):
        for left, right, kind in zip(expected_row, actual_row, contract['columns']):
            if left == right:
                continue
            if kind != 'float' or left is None or right is None:
                return False
            a, b = Fraction(decimal.Decimal(left)), Fraction(decimal.Decimal(right))
            if abs(a - b) > tolerance * max(abs(a), abs(b)):
                return False
    return True


def results_match(expected: CanonicalResult, actual: CanonicalResult, contract: dict) -> bool:
    if expected.metadata != actual.metadata or len(expected.rows) != len(actual.rows):
        return False
    if 'result_sets' not in contract:
        return _ordered_rows_match(expected.rows, actual.rows, contract)

    start = 0
    for spec in contract['result_sets']:
        end = start + spec['row_count']
        if not _ordered_rows_match(expected.rows[start:end], actual.rows[start:end], spec):
            return False
        start = end
    return True


def query_numbers(value: str, *, maximum: int | None = None) -> list[int]:
    numbers = []
    seen = set()
    for part in value.replace(',', ' ').split():
        if not re.fullmatch(r'[1-9][0-9]*(?:-[1-9][0-9]*)?', part):
            raise ValueError(f'invalid query selection: {part!r}')
        bounds = [int(n) for n in part.split('-')]
        start, end = bounds[0], bounds[-1]
        if end < start or (maximum is not None and end > maximum):
            raise ValueError(f'query range is reversed or out of bounds: {part}')
        for number in range(start, end + 1):
            if number in seen:
                raise ValueError(f'duplicate query: {number}')
            numbers.append(number)
            seen.add(number)
    if not numbers:
        raise ValueError('query selection must not be empty')
    return numbers


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fields = ['query_num', 'status', 'expected_rows', 'actual_rows',
              'expected_hash', 'actual_hash', 'message']
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
