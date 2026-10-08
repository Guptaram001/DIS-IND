import pandas as pd
from pathlib import Path

project_dir = Path(__file__).resolve().parent.parent

input_dir = project_dir / "data/imdb"
output_base_dir = project_dir / "data/imdb/imdb-r/scaled"

target_pcts = [0.10, 0.20, 0.40, 0.60, 0.80]

input_files = sorted(input_dir.glob("*.csv"))

for pct in target_pcts:
    output_dir = output_base_dir / f"{int(pct * 100)}pct"
    output_dir.mkdir(parents=True, exist_ok=True)

    total_original_rows = 0
    total_reduced_rows = 0

    print(f"\n=== {pct:.0%} row scaling ===")

    for input_file in input_files:
        df = pd.read_csv(input_file)

        total_rows = len(df)
        target_rows = int(total_rows * pct)
        target_rows = max(target_rows, 1)
        target_rows = min(target_rows, total_rows)

        subset = df.iloc[:target_rows]

        output_file = output_dir / input_file.name
        subset.to_csv(output_file, index=False)

        total_original_rows += total_rows
        total_reduced_rows += target_rows

        print(
            f"{input_file.name}: "
            f"{total_rows:,} rows x {len(df.columns)} cols "
            f"-> {len(subset):,} rows x {len(subset.columns)} cols"
        )

    print(
        f"Total rows: "
        f"{total_original_rows:,} -> {total_reduced_rows:,} "
        f"({total_reduced_rows / total_original_rows:.1%})"
    )