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
from pathlib import Path


@dataclass
class CanonicalResult:
    rows: list[tuple[object, ...]]
    digest: str
    metadata: tuple = ()


def normalize_cell(value: str, kind: str = 'text', null_marker: str | None = None) -> str | None:
    if null_marker is not None and value == null_marker:
        return None
    if kind == 'text':
        return value
    if value != value.strip():
        raise ValueError(f'whitespace in numeric cell: {value!r}')
    try:
        number = decimal.Decimal(value)
    except decimal.InvalidOperation as error:
        raise ValueError(f'invalid {kind} cell: {value!r}') from error
    if not number.is_finite() or (kind == 'integer' and number != number.to_integral_value()):
        raise ValueError(f'invalid {kind} cell: {value!r}')
    # Formatting does not round to the process-wide Decimal context precision.
    if not number:
        return '0'
    text = format(number, 'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text


def validate_contract(contract: dict) -> None:
    if not isinstance(contract, dict) or set(contract) - {'columns', 'order_keys', 'unordered', 'null_marker', 'format'}:
        raise ValueError('unknown comparison contract fields')
    if contract.get('format', 'tsv') not in ('tsv', 'typed-json'):
        raise ValueError('format must be tsv or typed-json')
    if contract.get('format') == 'typed-json' and ('columns' in contract or 'null_marker' in contract):
        raise ValueError('typed-json already carries types and NULL values')
    columns = contract.get('columns')
    if columns is not None and (not isinstance(columns, list) or not columns or
            any(kind not in ('text', 'integer', 'decimal', 'float') for kind in columns)):
        raise ValueError('columns must be a nonempty list of explicit scalar types')
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


def canonicalize(path: Path, *, contract: dict | None = None) -> CanonicalResult:
    contract = {} if contract is None else contract
    validate_contract(contract)
    columns = contract.get('columns')
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
    else:
        width = len(columns) if columns else None
        # Only newline terminators are removed: blank records are real empty rows.
        with path.open(encoding='utf-8', newline='') as source:
            for line in source:
                record = line[:-2] if line.endswith('\r\n') else line.removesuffix('\n')
                cells = record.split('\t')
                if width is None:
                    width = len(cells)
                if len(cells) != width:
                    raise ValueError(f'{path}: inconsistent result width: {len(cells)} != {width}')
                rows.append(tuple(normalize_cell(cell, columns[i] if columns else 'text',
                                                contract.get('null_marker')) for i, cell in enumerate(cells)))
    if keys and width is not None and max(keys) >= width:
        raise ValueError('order key is outside the result columns')
    def serialized(row):
        return json.dumps(row, ensure_ascii=False, separators=(',', ':'))
    if contract.get('unordered'):
        rows.sort(key=serialized)
    elif keys:
        # Keep the reference's group sequence. Reordering distinct ORDER BY
        # keys, including separating a tie group, must still fail comparison.
        rows = [row for _, group in itertools.groupby(rows, lambda row: serialized(tuple(row[i] for i in keys)))
                for row in sorted(group, key=serialized)]
    payload = json.dumps([metadata, rows], ensure_ascii=False, separators=(',', ':'), sort_keys=True).encode()
    return CanonicalResult(rows, hashlib.sha256(payload).hexdigest(), metadata)


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
