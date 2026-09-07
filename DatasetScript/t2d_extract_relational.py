import json
from glob import glob
from pathlib import Path

import pandas as pd
import numpy as np


output_dir = Path("data/t2d/processed")
output_dir.mkdir(parents=True, exist_ok=True)

files = glob("data/t2d/tables/*.json")
print(f"Found {len(files)} JSON files")

for file in files:
    try:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except UnicodeDecodeError:
        with open(file, "r", encoding="cp1252") as f:
            data = json.load(f)

    print(f"Processing: {file}")

    new_name = output_dir / f"{Path(file).stem}.csv"

    relation = np.array(data["relation"], dtype=object)

    df = pd.DataFrame(
        data=relation[:, 1:].T,
        columns=relation[:, 0]
    )

    df.to_csv(new_name, index=False)

    print(f"Saved: {new_name}")