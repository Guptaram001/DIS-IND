from pathlib import Path
from collections import defaultdict
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

DATASET_PATH = "/Users/gupta/Documents/DIS-IND/data/census"
CHUNK_SIZE = 200_000


# ============================================================
# ANALYZER
# ============================================================

def analyze_dataset(dataset_path, chunk_size=100_000):
    dataset_path = Path(dataset_path)

    csv_files = sorted(dataset_path.glob("*.csv"))
    tbl_files = sorted(dataset_path.glob("*.tbl"))

    if csv_files and tbl_files:
        raise ValueError(
            "Dataset contains both .csv and .tbl files. "
            "Use only one format per dataset."
        )

    if csv_files:
        files = csv_files
        file_type = "csv"
        # The downloaded Sawfish CENSUS CSV files are semicolon-separated.
        separator = ";"
    elif tbl_files:
        files = tbl_files
        file_type = "tbl"
        separator = "|"
    else:
        raise FileNotFoundError(f"No .csv or .tbl files found in {dataset_path}")

    total_rows = 0
    max_rows_per_table = 0
    total_attributes = 0
    all_attribute_distinct_counts = []
    all_dataset_values = set()
    value_to_attributes = defaultdict(set)
    table_reports = []

    total_size_bytes = sum(file.stat().st_size for file in files)
    total_size_mb = total_size_bytes / (1024 * 1024)

    for file_path in files:
        print(f"Analyzing: {file_path.name}")
        table_rows = 0
        table_size_mb = file_path.stat().st_size / (1024 * 1024)

        if file_type == "csv":
            header = pd.read_csv(file_path, sep=separator, nrows=0)
            columns = list(header.columns)
        else:
            sample = pd.read_csv(
                file_path, sep=separator, header=None, nrows=1, dtype=str
            )
            if len(sample.columns) > 0 and sample.iloc[:, -1].isna().all():
                number_of_columns = len(sample.columns) - 1
            else:
                number_of_columns = len(sample.columns)
            columns = [f"column_{i + 1}" for i in range(number_of_columns)]

        number_of_attributes = len(columns)
        total_attributes += number_of_attributes
        distinct_values = {column: set() for column in columns}

        if file_type == "csv":
            reader = pd.read_csv(
                file_path,
                sep=separator,
                chunksize=chunk_size,
                dtype=str,
                keep_default_na=False,
            )
        else:
            reader = pd.read_csv(
                file_path,
                sep=separator,
                header=None,
                chunksize=chunk_size,
                dtype=str,
                keep_default_na=False,
            )

        for chunk in reader:
            if file_type == "tbl" and len(chunk.columns) > len(columns):
                chunk = chunk.iloc[:, :len(columns)]

            chunk.columns = columns
            table_rows += len(chunk)

            for column in columns:
                unique_values = chunk[column].unique()
                distinct_values[column].update(unique_values)
                all_dataset_values.update(unique_values)
                qualified_attribute = f"{file_path.stem}.{column}"
                for value in unique_values:
                    value_to_attributes[value].add(qualified_attribute)

        total_rows += table_rows
        max_rows_per_table = max(max_rows_per_table, table_rows)
        table_distinct_counts = []

        for column in columns:
            count = len(distinct_values[column])
            table_distinct_counts.append(count)
            all_attribute_distinct_counts.append(count)

        table_max_distinct = max(table_distinct_counts, default=0)
        table_avg_distinct = (
            sum(table_distinct_counts) / len(table_distinct_counts)
            if table_distinct_counts
            else 0
        )
        table_reports.append(
            {
                "table": file_path.name,
                "size_mb": table_size_mb,
                "rows": table_rows,
                "attributes": number_of_attributes,
                "max_distinct": table_max_distinct,
                "average_distinct": table_avg_distinct,
            }
        )

    max_distinct_values_per_attribute = max(
        all_attribute_distinct_counts, default=0
    )
    average_distinct_values_per_attribute = (
        sum(all_attribute_distinct_counts) / len(all_attribute_distinct_counts)
        if all_attribute_distinct_counts
        else 0
    )
    total_distinct_values_dataset = len(all_dataset_values)
    number_of_clusters = len(
        {frozenset(attributes) for attributes in value_to_attributes.values()}
    )

    print()
    print("=" * 80)
    print("DATASET SUMMARY")
    print("=" * 80)
    print(f"Number of tables                       : {len(files):,}")
    print(f"Dataset size (MB)                      : {total_size_mb:,.2f}")
    print(f"Total rows                             : {total_rows:,}")
    print(f"Max # rows per table                   : {max_rows_per_table:,}")
    print(f"Total # attributes                     : {total_attributes:,}")
    print(
        "Max # distinct values per attribute    : "
        f"{max_distinct_values_per_attribute:,}"
    )
    print(
        "Average # distinct values per attribute: "
        f"{average_distinct_values_per_attribute:,.2f}"
    )
    print(
        "Total distinct values in dataset       : "
        f"{total_distinct_values_dataset:,}"
    )
    print(f"Total # clusters                       : {number_of_clusters:,}")

    print()
    print("=" * 80)
    print("TABLE SUMMARY")
    print("=" * 80)
    for table in table_reports:
        print(
            f"{table['table']:<35} "
            f"Rows={table['rows']:<12,} "
            f"Attributes={table['attributes']:<6,} "
            f"Size={table['size_mb']:>10,.2f} MB "
            f"MaxDistinct={table['max_distinct']:>12,} "
            f"AvgDistinct={table['average_distinct']:>12,.2f}"
        )

    return {
        "number_of_tables": len(files),
        "dataset_size_mb": total_size_mb,
        "total_rows": total_rows,
        "max_rows_per_table": max_rows_per_table,
        "total_attributes": total_attributes,
        "max_distinct_values_per_attribute": max_distinct_values_per_attribute,
        "average_distinct_values_per_attribute": average_distinct_values_per_attribute,
        "total_distinct_values_dataset": total_distinct_values_dataset,
        "number_of_clusters": number_of_clusters,
        "tables": table_reports,
    }


report = analyze_dataset(DATASET_PATH, chunk_size=CHUNK_SIZE)
