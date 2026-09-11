#!/usr/bin/env python3
r"""Convert an extracted MusicBrainz mbdump directory to CSV tables.

The MusicBrainz core dump contains one headerless, tab-separated file per
relation. This script streams each relation into a corresponding CSV file,
adds stable synthetic column names, and converts PostgreSQL's ``\N`` NULL
marker to an empty CSV field.
"""

from __future__ import annotations

import argparse
import csv
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path


def convert_table(
    source_text: str,
    output_dir_text: str,
    delimiter: str,
    overwrite: bool,
) -> dict[str, object]:
    source = Path(source_text)
    output_dir = Path(output_dir_text)
    output = output_dir / f"{source.name}.csv"
    temporary = output_dir / f".{source.name}.csv.tmp"

    if output.exists() and not overwrite:
        return {
            "table": source.name,
            "status": "skipped",
            "rows": 0,
            "columns": 0,
            "output": str(output),
        }

    rows = 0
    columns = 0

    try:
        with (
            source.open(
                "r", encoding="utf-8", errors="replace", newline=""
            ) as input_file,
            temporary.open("w", encoding="utf-8", newline="") as output_file,
        ):
            reader = csv.reader(input_file, delimiter="\t", quoting=csv.QUOTE_NONE)
            writer = csv.writer(
                output_file,
                delimiter=delimiter,
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                lineterminator="\n",
            )

            first_row = next(reader, None)
            if first_row is None:
                return {
                    "table": source.name,
                    "status": "empty",
                    "rows": 0,
                    "columns": 0,
                    "output": "",
                }

            columns = len(first_row)
            writer.writerow(
                [f"{source.name}_column_{index}" for index in range(1, columns + 1)]
            )

            def write_row(row: list[str], line_number: int) -> None:
                if len(row) != columns:
                    raise ValueError(
                        f"line {line_number:,} has {len(row):,} fields; "
                        f"expected {columns:,}"
                    )
                # PostgreSQL COPY represents NULL using the literal \N.
                writer.writerow(["" if value == r"\N" else value for value in row])

            write_row(first_row, 1)
            rows = 1

            for line_number, row in enumerate(reader, start=2):
                write_row(row, line_number)
                rows += 1

        os.replace(temporary, output)
        return {
            "table": source.name,
            "status": "converted",
            "rows": rows,
            "columns": columns,
            "output": str(output),
        }
    except Exception:
        # Leave an existing completed CSV untouched. Only remove this run's
        # incomplete temporary output.
        temporary.unlink(missing_ok=True)
        raise


def discover_tables(input_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in input_dir.iterdir()
        if path.is_file()
        and not path.name.startswith(".")
        and path.stat().st_size > 0
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert MusicBrainz mbdump tables to CSV files."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/mb/mbdump"),
        help="Extracted mbdump directory (default: data/mb/mbdump)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/mb/csv"),
        help="Output directory (default: data/mb/csv)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Parallel conversion processes (default: 2)",
    )
    parser.add_argument(
        "--delimiter",
        choices=[",", ";"],
        default=",",
        help="Output CSV delimiter (default: comma)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace already completed CSV files",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    input_dir = args.input.resolve()
    output_dir = args.output.resolve()

    if not input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist: {input_dir}")
    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")

    output_dir.mkdir(parents=True, exist_ok=True)
    tables = discover_tables(input_dir)

    if not tables:
        raise SystemExit(f"No non-empty tables found in: {input_dir}")

    print(f"Input directory : {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Tables found    : {len(tables):,}")
    print(f"Workers         : {args.workers:,}")
    print(f"CSV delimiter   : {args.delimiter!r}")

    converted = 0
    skipped = 0
    empty = 0
    failed = 0
    total_rows = 0

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        jobs = {
            executor.submit(
                convert_table,
                str(table),
                str(output_dir),
                args.delimiter,
                args.overwrite,
            ): table
            for table in tables
        }

        for job in as_completed(jobs):
            table = jobs[job]
            try:
                result = job.result()
                status = str(result["status"])
                rows = int(result["rows"])
                columns = int(result["columns"])
                total_rows += rows

                if status == "converted":
                    converted += 1
                    print(
                        f"[converted] {table.name}: "
                        f"{rows:,} rows, {columns:,} columns"
                    )
                elif status == "skipped":
                    skipped += 1
                    print(f"[skipped]   {table.name}: output already exists")
                else:
                    empty += 1
                    print(f"[empty]     {table.name}")
            except Exception as error:
                failed += 1
                print(f"[failed]    {table.name}: {error}")

    print()
    print("Conversion summary")
    print(f"Converted : {converted:,}")
    print(f"Skipped   : {skipped:,}")
    print(f"Empty     : {empty:,}")
    print(f"Failed    : {failed:,}")
    print(f"Rows read : {total_rows:,}")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()