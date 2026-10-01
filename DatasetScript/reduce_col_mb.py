import pandas as pd
from pathlib import Path

project_dir = Path(__file__).resolve().parent.parent

input_dir = project_dir / "data/mb"
output_base_dir = project_dir / "data/mb-c/scaled"

target_pcts = [0.1,0.20, 0.40, 0.60, 0.80]

# Find all CSV tables
input_files = list(input_dir.glob("*.csv"))

for pct in target_pcts:
    output_dir = output_base_dir / f"{int(pct * 100)}pct"
    output_dir.mkdir(parents=True, exist_ok=True)

    total_original_cols = 0
    total_reduced_cols = 0

    print(f"\n=== {pct:.0%} column scaling ===")

    for input_file in input_files:
        df = pd.read_csv(input_file)

        total_cols = len(df.columns)

        # Keep the same percentage of columns from each table
        n = int(total_cols * pct)
        n = max(n, 1)
        n = min(n, total_cols)

        subset = df.iloc[:, :n]

        output_file = output_dir / input_file.name
        subset.to_csv(output_file, index=False)

        total_original_cols += total_cols
        total_reduced_cols += n

        print(
            f"{input_file.name}: "
            f"{len(df):,} rows × {total_cols} cols "
            f"-> {len(subset):,} rows × {n} cols"
        )

    print(
        f"Total columns: "
        f"{total_original_cols} -> {total_reduced_cols}"
    )