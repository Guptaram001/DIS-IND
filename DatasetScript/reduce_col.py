import pandas as pd
from pathlib import Path

project_dir = Path(__file__).resolve().parent.parent

input_file = project_dir / "data/imdb/IMDB.csv"
output_dir = project_dir / "data/imdb"
output_dir.mkdir(parents=True, exist_ok=True)

# Column percentages instead of fixed counts
target_pcts = [0.10, 0.20, 0.40, 0.60, 0.80]

# Load fixed number of rows
df = pd.read_csv(input_file, sep=",")

total_cols = len(df.columns)
print(f"Loaded {len(df):,} rows and {total_cols} columns")

for pct in target_pcts:
    n = int(total_cols * pct)
    n = max(n, 1)                          # always keep at least 1 column
    n = min(n, total_cols)                 # never exceed total

    subset = df.iloc[:, :n]

    output_file = output_dir / f"{input_file.stem}_{int(pct*100)}pct_cols.csv"
    subset.to_csv(output_file, index=False)

    print(
        f"Created {output_file.name}: "
        f"{len(subset):,} rows × {n} columns "
        f"({pct:.0%} of {total_cols} columns)"
    )