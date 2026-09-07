#!/usr/bin/env python3
"""Reduce MusicBrainz CSVs to a target size while retaining broad signature coverage.

Small relations are copied completely. Large relations are sampled uniformly over
their full row range, with a deterministic per-table seed. Values are never
rewritten, so equality and value-to-column membership remain exact for retained
rows. The target is approximate because CSV records have variable byte lengths.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import random
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Job:
    source: str
    destination: str
    probability: float
    seed: int
    overwrite: bool


def table_seed(global_seed: int, name: str) -> int:
    digest = hashlib.blake2b(
        name.encode("utf-8"), digest_size=8, person=b"mb-reduce"
    ).digest()
    return global_seed ^ int.from_bytes(digest, "big")


def reduce_table(job: Job) -> dict[str, object]:
    source = Path(job.source)
    destination = Path(job.destination)
    temporary = destination.with_name(f".{destination.name}.tmp")

    if destination.exists() and not job.overwrite:
        return {
            "table": source.name,
            "status": "skipped",
            "input_bytes": source.stat().st_size,
            "output_bytes": destination.stat().st_size,
            "input_rows": 0,
            "output_rows": 0,
            "probability": job.probability,
        }

    if job.probability >= 1.0:
        shutil.copyfile(source, temporary)
        os.replace(temporary, destination)
        return {
            "table": source.name,
            "status": "copied",
            "input_bytes": source.stat().st_size,
            "output_bytes": destination.stat().st_size,
            "input_rows": 0,
            "output_rows": 0,
            "probability": 1.0,
        }

    rng = random.Random(job.seed)
    input_rows = 0
    output_rows = 0
    first_data_row: list[str] | None = None

    try:
        with (
            source.open("r", encoding="utf-8", errors="replace", newline="") as src,
            temporary.open("w", encoding="utf-8", newline="") as dst,
        ):
            reader = csv.reader(src)
            writer = csv.writer(dst, lineterminator="\n")
            header = next(reader, None)
            if header is None:
                raise ValueError(f"empty CSV file: {source}")
            writer.writerow(header)

            for row in reader:
                input_rows += 1
                if first_data_row is None:
                    first_data_row = row
                if rng.random() < job.probability:
                    writer.writerow(row)
                    output_rows += 1

            # Never turn a non-empty relation into a header-only relation.
            if output_rows == 0 and first_data_row is not None:
                writer.writerow(first_data_row)
                output_rows = 1

        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    return {
        "table": source.name,
        "status": "sampled",
        "input_bytes": source.stat().st_size,
        "output_bytes": destination.stat().st_size,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "probability": job.probability,
    }


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", type=Path, default=Path("data/mb/processed"),
        help="source CSV directory (default: data/mb/processed)",
    )
    parser.add_argument(
        "--output", type=Path, default=Path("data/mb/reduced-10gb"),
        help="destination directory (default: data/mb/reduced-10gb)",
    )
    parser.add_argument(
        "--target-gb", type=float, default=10.0,
        help="target decimal GB, where 1 GB = 1,000,000,000 bytes",
    )
    parser.add_argument(
        "--keep-small-mb", type=float, default=16.0,
        help="copy tables at or below this size completely (default: 16 MB)",
    )
    parser.add_argument("--seed", type=int, default=20260901)
    parser.add_argument(
        "--workers", type=int, default=min(6, os.cpu_count() or 2),
        help="parallel CSV workers (default: up to 6)",
    )
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = arguments()
    source_dir = args.input.resolve()
    output_dir = args.output.resolve()
    if not source_dir.is_dir():
        raise SystemExit(f"Input directory does not exist: {source_dir}")
    if args.target_gb <= 0 or args.keep_small_mb < 0 or args.workers < 1:
        raise SystemExit("target-gb and workers must be positive; keep-small-mb cannot be negative")
    if source_dir == output_dir:
        raise SystemExit("Input and output directories must be different")

    files = sorted(source_dir.glob("*.csv"))
    if not files:
        raise SystemExit(f"No CSV files found in {source_dir}")

    target = int(args.target_gb * 1_000_000_000)
    small_limit = int(args.keep_small_mb * 1_000_000)
    total = sum(path.stat().st_size for path in files)
    small = [path for path in files if path.stat().st_size <= small_limit]
    large = [path for path in files if path.stat().st_size > small_limit]
    small_bytes = sum(path.stat().st_size for path in small)
    large_bytes = total - small_bytes

    if target >= total:
        large_probability = 1.0
    elif small_bytes >= target:
        raise SystemExit(
            f"Fully retained small tables use {small_bytes / 1e9:.3f} GB, above the "
            "target. Lower --keep-small-mb."
        )
    else:
        large_probability = (target - small_bytes) / large_bytes

    output_dir.mkdir(parents=True, exist_ok=True)
    jobs = [
        Job(
            str(path),
            str(output_dir / path.name),
            1.0 if path in small else large_probability,
            table_seed(args.seed, path.name),
            args.overwrite,
        )
        for path in files
    ]

    print(f"Input             : {source_dir}")
    print(f"Output            : {output_dir}")
    print(f"Tables            : {len(files):,}")
    print(f"Input size        : {total / 1e9:,.3f} GB")
    print(f"Target size       : {target / 1e9:,.3f} GB")
    print(f"Fully kept tables : {len(small):,} ({small_bytes / 1e9:,.3f} GB)")
    print(f"Large-table rate  : {large_probability:.6%}")
    print(f"Workers           : {args.workers:,}")

    results: list[dict[str, object]] = []
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        pending = {executor.submit(reduce_table, job): job for job in jobs}
        for future in as_completed(pending):
            result = future.result()
            results.append(result)
            print(
                f"[{result['status']}] {result['table']}: "
                f"{int(result['output_bytes']) / 1e6:,.2f} MB",
                flush=True,
            )

    output_bytes = sum(int(result["output_bytes"]) for result in results)
    print("\nReduction complete")
    print(f"Actual output size: {output_bytes / 1e9:,.3f} GB")
    print(f"Retained fraction : {output_bytes / total:.3%}")
    print(
        "Run the signature analyzer on the reduced directory to measure exact "
        "signature retention."
    )


if __name__ == "__main__":
    main()
