import pandas as pd
from pathlib import Path

project_dir = Path(__file__).resolve().parent.parent

input_file = project_dir / "data/btc/1m_BC_2021.csv"
output_dir = project_dir / "data/btc"
output_dir.mkdir(parents=True, exist_ok=True)

# Percentages of the total file to generate
percentages = [0.10, 0.20, 0.40, 0.60, 0.80]

# Total file size and total row count
total_size_bytes = input_file.stat().st_size
total_rows = sum(1 for _ in open(input_file, encoding="utf-8", errors="replace")) - 1  # minus header

print(f"Total file size : {total_size_bytes / 1e6:,.2f} MB")
print(f"Total rows      : {total_rows:,}")

# Read the file once (or in chunks if it's large)
df = pd.read_csv(input_file)
print(f"Loaded {len(df):,} rows")

for pct in percentages:
    target_bytes = total_size_bytes * pct
    target_rows = int(len(df) * pct)
    output_file = output_dir / f"{input_file.stem}_{int(pct*100)}pct.csv"
    df.iloc[:target_rows].to_csv(output_file, index=False)
    actual_bytes = output_file.stat().st_size
    print(
        f"Created {output_file.name}: "
        f"{target_rows:,} rows, "
        f"{actual_bytes / 1e6:,.2f} MB "
        f"({actual_bytes / total_size_bytes:.1%} of original)"
    )