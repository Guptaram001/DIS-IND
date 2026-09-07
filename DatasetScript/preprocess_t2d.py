#!/usr/bin/env python3
"""Convert extracted T2Dv2 Web Table JSON files to DIS-IND-safe CSV."""

from __future__ import annotations

import argparse
import itertools
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable


DELIMITER = ";"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input",
        type=Path,
        help="Extracted T2Dv2 directory, or its tables/ directory",
    )
    parser.add_argument("output", type=Path, help="Directory for generated CSV tables")
    parser.add_argument(
        "--workers",
        type=int,
        default=min(16, (os.cpu_count() or 1) * 2),
        help="Parallel file workers (default: min(16, 2 x CPU count))",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace CSV files already present in the output directory",
    )
    parser.add_argument(
        "--only-relational",
        action="store_true",
        help="Keep only tableType=RELATION (do not use for the full T2Dv2 benchmark)",
    )
    return parser.parse_args()


def clean_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    text = str(value).replace("\r", " ").replace("\n", " ").replace(DELIMITER, ",")
    return re.sub(r"\s+", " ", text).strip()


def read_json(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
        encoding = "utf-8"
    except UnicodeDecodeError:
        # Some files in the official archive contain Windows-1252 bytes.
        text = raw.decode("cp1252")
        encoding = "cp1252"
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("top-level JSON value is not an object")
    return value, encoding


def transpose_relation(relation: Any) -> tuple[list[list[Any]], int]:
    if not isinstance(relation, list) or not relation:
        return [], 0
    columns = [column for column in relation if isinstance(column, list)]
    if not columns:
        return [], 0
    rows = [list(row) for row in itertools.zip_longest(*columns, fillvalue="")]
    return rows, len(columns)


def unique_headers(values: Iterable[Any], width: int) -> list[str]:
    result: list[str] = []
    counts: dict[str, int] = {}
    supplied = list(itertools.islice(values, width))
    for index in range(width):
        base = clean_cell(supplied[index]) if index < len(supplied) else ""
        base = base or f"column_{index + 1}"
        occurrence = counts.get(base, 0) + 1
        counts[base] = occurrence
        result.append(base if occurrence == 1 else f"{base}_{occurrence}")
    return result


def prepare_table(record: dict[str, Any]) -> tuple[list[str], list[list[str]], bool]:
    rows, width = transpose_relation(record.get("relation"))
    if not rows or width == 0:
        raise ValueError("missing or empty relation")

    header_index = record.get("headerRowIndex", -1)
    has_usable_header = (
        record.get("hasHeader") is True
        and isinstance(header_index, int)
        and 0 <= header_index < len(rows)
    )
    if has_usable_header:
        headers = unique_headers(rows[header_index], width)
        data = rows[:header_index] + rows[header_index + 1 :]
    else:
        headers = [f"column_{index + 1}" for index in range(width)]
        data = rows

    normalized = [[clean_cell(value) for value in row] for row in data]
    return headers, normalized, has_usable_header


def convert_one(source: Path, output: Path, overwrite: bool) -> dict[str, Any]:
    destination = output / f"{source.stem}.csv"
    temporary = destination.with_suffix(f".csv.{os.getpid()}.{id(source)}.tmp")
    if destination.exists() and not overwrite:
        raise FileExistsError(f"output already exists: {destination}")

    started = time.monotonic()
    try:
        record, encoding = read_json(source)
        headers, rows, used_source_header = prepare_table(record)
        with temporary.open("w", encoding="utf-8", newline="\n") as output_file:
            output_file.write(DELIMITER.join(headers) + "\n")
            for row in rows:
                output_file.write(DELIMITER.join(row) + "\n")
        os.replace(temporary, destination)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise

    return {
        "source": source.name,
        "file": destination.name,
        "rows": len(rows),
        "columns": len(headers),
        "table_type": record.get("tableType"),
        "orientation": record.get("tableOrientation"),
        "source_header_used": used_source_header,
        "source_encoding": encoding,
        "seconds": time.monotonic() - started,
    }


def main() -> int:
    args = parse_args()
    tables_dir = args.input / "tables" if (args.input / "tables").is_dir() else args.input
    if not tables_dir.is_dir():
        print(f"T2D tables directory not found: {tables_dir}", file=sys.stderr)
        return 2
    if args.workers < 1:
        print("--workers must be at least 1", file=sys.stderr)
        return 2

    sources = sorted(tables_dir.glob("*.json"))
    if not sources:
        print(f"No JSON table files found in {tables_dir}", file=sys.stderr)
        return 2

    if args.only_relational:
        selected: list[Path] = []
        for source in sources:
            try:
                record, _ = read_json(source)
            except Exception as error:
                print(f"Cannot inspect {source}: {error}", file=sys.stderr)
                return 1
            if record.get("tableType") == "RELATION":
                selected.append(source)
        sources = selected

    args.output.mkdir(parents=True, exist_ok=True)
    existing = [args.output / f"{source.stem}.csv" for source in sources]
    existing = [path for path in existing if path.exists()]
    if existing and not args.overwrite:
        print(
            f"{len(existing)} output CSV files already exist; use --overwrite or an empty directory",
            file=sys.stderr,
        )
        return 2

    print(f"Converting {len(sources):,} T2D tables with {min(args.workers, len(sources))} workers")
    results: list[dict[str, Any]] = []
    failures: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=min(args.workers, len(sources))) as executor:
        futures = {
            executor.submit(convert_one, source, args.output, args.overwrite): source
            for source in sources
        }
        for future in as_completed(futures):
            source = futures[future]
            try:
                results.append(future.result())
            except Exception as error:
                failures.append((source.name, str(error)))
                print(f"FAILED {source.name}: {error}", file=sys.stderr)

    results.sort(key=lambda item: item["file"])
    manifest = args.output / "conversion_manifest.json"
    manifest.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    total_rows = sum(item["rows"] for item in results)
    total_columns = sum(item["columns"] for item in results)
    cp1252_files = sum(item["source_encoding"] == "cp1252" for item in results)
    generated_headers = sum(not item["source_header_used"] for item in results)
    print(f"Written tables: {len(results):,}")
    print(f"Total data rows: {total_rows:,}")
    print(f"Total columns: {total_columns:,}")
    print(f"Files decoded as Windows-1252: {cp1252_files:,}")
    print(f"Tables using generated headers: {generated_headers:,}")
    print(f"Manifest: {manifest}")

    if failures:
        print(f"Failed tables: {len(failures):,}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
