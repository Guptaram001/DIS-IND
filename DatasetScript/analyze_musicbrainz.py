#!/usr/bin/env python3
"""Analyze MusicBrainz CSV tables with parallel, disk-backed aggregation."""

from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def read_expression(path: Path) -> str:
    return (
        "read_csv("
        f"{sql_string(str(path))}, "
        "header = true, all_varchar = true, "
        "delim = ',', quote = '\"', escape = '\"', nullstr = '', "
        "parallel = true, strict_mode = true)"
    )


def inspect_table(path_text: str) -> dict[str, object]:
    path = Path(path_text)
    connection = duckdb.connect()
    expression = read_expression(path)
    description = connection.execute(
        f"DESCRIBE SELECT * FROM {expression}"
    ).fetchall()
    rows = connection.execute(f"SELECT count(*) FROM {expression}").fetchone()[0]
    connection.close()
    return {
        "path": path,
        "table": path.name,
        "rows": rows,
        "attributes": len(description),
        "columns": [str(row[0]) for row in description],
        "size_bytes": path.stat().st_size,
    }


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze MusicBrainz CSVs and count exact value-to-attribute "
            "occurrence signatures."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/mb/csv"),
        help="CSV directory (default: data/mb/csv)",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=Path("data/mb/mb_analysis.duckdb"),
        help="Disk-backed working database",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(8, os.cpu_count() or 2),
        help="Worker/SQL threads (default: up to 8)",
    )
    parser.add_argument(
        "--memory-limit",
        default="8GB",
        help="DuckDB memory limit; excess spills to disk (default: 8GB)",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Rebuild cached value-attribute pairs",
    )
    # Jupyter/ipykernel injects its own --f=<kernel.json> argument. Ignore
    # arguments not owned by this analyzer so the same code works both as a
    # Python script and in a notebook cell.
    args, _unknown = parser.parse_known_args()
    return args


def make_unpivot_query(path: Path) -> str:
    table_prefix = f"{path.stem}."
    return (
        "SELECT DISTINCT value, "
        f"{sql_string(table_prefix)} || column_name AS attribute "
        "FROM ("
        f"UNPIVOT {read_expression(path)} "
        "ON COLUMNS(*) INTO NAME column_name VALUE value"
        ") "
        "WHERE value IS NOT NULL AND trim(value) <> ''"
    )


def main() -> None:
    args = parse_arguments()
    input_dir = args.input.resolve()
    database = args.database.resolve()

    if not input_dir.is_dir():
        raise SystemExit(f"CSV directory does not exist: {input_dir}")
    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")

    files = sorted(input_dir.glob("*.csv"))
    if not files:
        raise SystemExit(f"No CSV files found in: {input_dir}")

    print(f"CSV directory : {input_dir}")
    print(f"Tables        : {len(files):,}")
    print(f"Workers       : {args.workers:,}")
    print(f"Memory limit  : {args.memory_limit}")
    print(f"Working DB    : {database}")
    print()

    # Count rows and inspect schemas concurrently. Each worker owns its
    # connection; DuckDB connections are not shared between threads.
    reports = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        jobs = {executor.submit(inspect_table, str(path)): path for path in files}
        for job in as_completed(jobs):
            path = jobs[job]
            try:
                report = job.result()
                reports.append(report)
                print(
                    f"[scanned] {path.name}: "
                    f"{int(report['rows']):,} rows, "
                    f"{int(report['attributes']):,} attributes"
                )
            except Exception as error:
                raise RuntimeError(f"Failed to scan {path}: {error}") from error

    reports.sort(key=lambda report: str(report["table"]))
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database))
    connection.execute(f"SET threads = {args.workers}")
    connection.execute(f"SET memory_limit = {sql_string(args.memory_limit)}")
    connection.execute(
        f"SET temp_directory = {sql_string(str(database.parent / 'mb_duckdb_tmp'))}"
    )
    connection.execute("SET preserve_insertion_order = false")

    cache_exists = bool(
        connection.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_name = 'value_attributes'"
        ).fetchone()[0]
    )

    if args.rebuild and cache_exists:
        connection.execute("DROP TABLE value_attributes")
        connection.execute("DROP TABLE IF EXISTS processed_tables")
        cache_exists = False

    if not cache_exists:
        print()
        print("Building disk-backed distinct value-to-attribute index ...")
        connection.execute(
            "CREATE TABLE value_attributes (value VARCHAR, attribute VARCHAR)"
        )
        connection.execute(
            "CREATE TABLE processed_tables "
            "(table_name VARCHAR PRIMARY KEY, size_bytes UBIGINT)"
        )
    else:
        print()
        print("Continuing cached value_attributes index (use --rebuild to replace it).")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS processed_tables "
            "(table_name VARCHAR PRIMARY KEY, size_bytes UBIGINT)"
        )

    processed = {
        str(row[0]): int(row[1])
        for row in connection.execute(
            "SELECT table_name, size_bytes FROM processed_tables"
        ).fetchall()
    }

    pending_files = []
    for path in files:
        current_size = path.stat().st_size
        cached_size = processed.get(path.name)
        if cached_size is None:
            pending_files.append(path)
        elif cached_size != current_size:
            raise RuntimeError(
                f"{path.name} changed after it was indexed. Run again with --rebuild."
            )

    if pending_files:
        print(
            f"Indexing {len(pending_files):,} remaining tables "
            f"({len(files) - len(pending_files):,} already complete) ..."
        )
        for index, path in enumerate(pending_files, start=1):
            print(f"[index {index:,}/{len(pending_files):,}] {path.name}", flush=True)
            query = make_unpivot_query(path)
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute(
                    "INSERT INTO value_attributes "
                    f"{query}"
                )
                connection.execute(
                    "INSERT INTO processed_tables VALUES (?, ?)",
                    [path.name, path.stat().st_size],
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

            # Periodic checkpoints make completed work durable without paying
            # checkpoint overhead after every tiny relation.
            if index % 10 == 0:
                connection.execute("CHECKPOINT")

        connection.execute("CHECKPOINT")
    else:
        print("All tables are already present in the cached index.")

    print("Computing attribute cardinalities and clusters ...")
    attribute_stats = connection.execute(
        "SELECT attribute, count(*) AS distinct_count "
        "FROM value_attributes GROUP BY attribute"
    ).fetchall()

    distinct_counts = [int(row[1]) for row in attribute_stats]
    total_distinct_occurrences = sum(distinct_counts)
    global_distinct_values = int(
        connection.execute(
            "SELECT count(DISTINCT value) FROM value_attributes"
        ).fetchone()[0]
    )

    # One cluster is one unique set of attributes associated with a value.
    # Materializing a sorted LIST for every value can require hundreds of GiB
    # on MusicBrainz. Use two independently salted, order-independent 64-bit
    # fingerprints plus the set cardinality. The probability that two
    # different attribute sets collide on all three fields is negligible.
    number_of_clusters = int(
        connection.execute(
            "SELECT count(*) FROM ("
            "  SELECT attribute_count, signature_1, signature_2 FROM ("
            "    SELECT value, count(*) AS attribute_count, "
            "      bit_xor(hash(attribute)) AS signature_1, "
            "      bit_xor(hash(attribute || '#musicbrainz-cluster-v2')) "
            "        AS signature_2 "
            "    FROM value_attributes GROUP BY value"
            "  ) GROUP BY attribute_count, signature_1, signature_2"
            ")"
        ).fetchone()[0]
    )

    total_rows = sum(int(report["rows"]) for report in reports)
    max_rows = max(int(report["rows"]) for report in reports)
    total_attributes = sum(int(report["attributes"]) for report in reports)
    total_size_bytes = sum(int(report["size_bytes"]) for report in reports)
    max_distinct = max(distinct_counts, default=0)
    average_distinct = (
        total_distinct_occurrences / total_attributes
        if total_attributes
        else 0
    )

    print()
    print("=" * 80)
    print("DATASET SUMMARY")
    print("=" * 80)
    print(f"Number of tables                       : {len(files):,}")
    print(f"Dataset size (decimal MB)              : {total_size_bytes / 1_000_000:,.2f}")
    print(f"Dataset size (MiB)                     : {total_size_bytes / 1_048_576:,.2f}")
    print(f"Total rows                             : {total_rows:,}")
    print(f"Max # rows per table                   : {max_rows:,}")
    print(f"Total # attributes                     : {total_attributes:,}")
    print(f"Max # distinct values per attribute    : {max_distinct:,}")
    print(f"Average # distinct values per attribute: {average_distinct:,.2f}")
    print(f"Sum distinct values over attributes |V|: {total_distinct_occurrences:,}")
    print(f"Global distinct values (union)         : {global_distinct_values:,}")
    print(f"Unique value-to-attribute signatures   : {number_of_clusters:,}")
    print("Cluster signature method                : dual 64-bit + cardinality")

    print()
    print("=" * 80)
    print("TABLE SUMMARY")
    print("=" * 80)
    counts_by_attribute = {str(row[0]): int(row[1]) for row in attribute_stats}
    for report in reports:
        prefix = f"{Path(str(report['table'])).stem}."
        table_counts = [
            counts_by_attribute.get(f"{prefix}{column}", 0)
            for column in report["columns"]
        ]
        table_max = max(table_counts, default=0)
        table_average = sum(table_counts) / len(table_counts) if table_counts else 0
        print(
            f"{str(report['table']):<40} "
            f"Rows={int(report['rows']):<13,} "
            f"Attributes={int(report['attributes']):<5,} "
            f"MaxDistinct={table_max:<13,} "
            f"AvgDistinct={table_average:,.2f}"
        )

    connection.close()


if __name__ == "__main__":
    main()
