#!/usr/bin/env python3
"""Convert the WebTables JSONL sample into DIS-IND-compatible CSV tables."""

from __future__ import annotations

import argparse
import itertools
import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable


DELIMITER = ";"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="WebTables JSONL input file")
    parser.add_argument("output", type=Path, help="Directory for generated CSV files")
    parser.add_argument(
        "--table-types",
        nargs="+",
        default=["RELATION"],
        help="Accepted tableType values (default: RELATION)",
    )
    parser.add_argument(
        "--orientations",
        nargs="+",
        default=["HORIZONTAL"],
        help="Accepted tableOrientation values (default: HORIZONTAL)",
    )
    parser.add_argument(
        "--allow-no-header",
        action="store_true",
        help="Include tables without a declared header and generate column names",
    )
    parser.add_argument("--min-columns", type=int, default=2)
    parser.add_argument("--min-rows", type=int, default=1, help="Minimum data rows after removing the header")
    parser.add_argument("--max-tables", type=int, help="Stop after writing this many tables")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow output into a directory that already contains generated webtable_*.csv files",
    )
    return parser.parse_args()


def clean_cell(value: Any) -> str:
    """Produce a single physical field that the Java loader can split safely."""
    if value is None:
        return ""
    text = str(value).replace("\r", " ").replace("\n", " ").replace(DELIMITER, ",")
    return re.sub(r"\s+", " ", text).strip()


def unique_headers(values: Iterable[Any], width: int) -> list[str]:
    headers: list[str] = []
    counts: dict[str, int] = {}
    for index, value in enumerate(itertools.islice(values, width), start=1):
        base = clean_cell(value) or f"column_{index}"
        count = counts.get(base, 0) + 1
        counts[base] = count
        headers.append(base if count == 1 else f"{base}_{count}")
    while len(headers) < width:
        headers.append(f"column_{len(headers) + 1}")
    return headers


def relation_to_rows(relation: Any) -> list[list[str]]:
    """WebTables stores relation as columns; transpose and pad ragged columns."""
    if not isinstance(relation, list) or not relation:
        return []
    columns = [column for column in relation if isinstance(column, list)]
    if not columns:
        return []
    return [list(row) for row in itertools.zip_longest(*columns, fillvalue="")]


def prepare_table(record: dict[str, Any], args: argparse.Namespace) -> tuple[list[str], list[list[str]]] | None:
    if record.get("tableType") not in set(args.table_types):
        return None
    if record.get("tableOrientation") not in set(args.orientations):
        return None

    rows = relation_to_rows(record.get("relation"))
    if not rows:
        return None
    width = len(rows[0])
    if width < args.min_columns:
        return None

    header_index = record.get("headerRowIndex", -1)
    has_header = record.get("hasHeader") is True and isinstance(header_index, int) and 0 <= header_index < len(rows)
    if not has_header and not args.allow_no_header:
        return None

    if has_header:
        headers = unique_headers(rows[header_index], width)
        data_rows = rows[:header_index] + rows[header_index + 1 :]
    else:
        headers = [f"column_{index}" for index in range(1, width + 1)]
        data_rows = rows

    if len(data_rows) < args.min_rows:
        return None
    return headers, [[clean_cell(value) for value in row] for row in data_rows]


def write_table(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    # Deliberately avoid CSV quoting: DIS-IND currently splits input lines directly.
    with path.open("w", encoding="utf-8", newline="\n") as output:
        output.write(DELIMITER.join(headers) + "\n")
        for row in rows:
            output.write(DELIMITER.join(row) + "\n")


def main() -> int:
    args = parse_args()
    if not args.input.is_file():
        print(f"Input file not found: {args.input}", file=sys.stderr)
        return 2
    if args.min_columns < 1 or args.min_rows < 0 or (args.max_tables is not None and args.max_tables < 1):
        print("Invalid minimum/maximum value", file=sys.stderr)
        return 2

    args.output.mkdir(parents=True, exist_ok=True)
    existing = list(args.output.glob("webtable_*.csv"))
    if existing and not args.overwrite:
        print(
            f"Output contains {len(existing)} generated CSV files; use --overwrite or choose an empty directory",
            file=sys.stderr,
        )
        return 2

    accepted = 0
    malformed = 0
    scanned = 0
    manifest_path = args.output / "manifest.jsonl"
    with args.input.open(encoding="utf-8") as source, manifest_path.open("w", encoding="utf-8") as manifest:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                continue
            scanned += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue

            prepared = prepare_table(record, args)
            if prepared is None:
                continue
            headers, rows = prepared
            accepted += 1
            filename = f"webtable_{accepted:06d}.csv"
            write_table(args.output / filename, headers, rows)
            manifest.write(
                json.dumps(
                    {
                        "file": filename,
                        "source_line": line_number,
                        "url": record.get("url"),
                        "title": record.get("title") or record.get("pageTitle"),
                        "rows": len(rows),
                        "columns": len(headers),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            if args.max_tables is not None and accepted >= args.max_tables:
                break

    print(f"Scanned: {scanned:,}")
    print(f"Written: {accepted:,} CSV tables to {args.output}")
    print(f"Malformed JSON lines skipped: {malformed:,}")
    print(f"Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
