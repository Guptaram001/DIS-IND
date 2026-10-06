#!/usr/bin/env python3
"""Split a dataset into a deterministic sample and its remainder with bounded memory."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import random
import sys


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def records(path, delimiter, header):
    with path.open(encoding='utf-8', newline='') as stream:
        reader = csv.reader(stream, delimiter=delimiter, strict=True)
        try:
            if header:
                next((row for row in reader if row), None)
            for row in reader:
                if row:  # Commons CSV ignores empty physical lines too.
                    yield row
        except csv.Error as error:
            raise ValueError(
                f"{path}: CSV parse error near physical line {reader.line_num} "
                f"with delimiter {delimiter!r}: {error}. "
                "Check --delimiter against the source format (IMDb: ';', mb: ',')."
            ) from error


def create_sample(source, output, rows=2_000_000, seed=12345, delimiter=';', header=True, remainder_dir=None):
    source, output = Path(source).resolve(), Path(output).resolve()
    remainder = Path(remainder_dir).resolve() if remainder_dir is not None else output.with_name(output.name + '-remaining')
    if rows <= 0 or len(delimiter) != 1:
        raise ValueError('rows must be positive and delimiter must be one character')
    for left, right in ((source, output), (source, remainder), (output, remainder)):
        if left == right or left in right.parents or right in left.parents:
            raise ValueError('Source, sample and remainder directories must be separate and not nested')
    for destination in (output, remainder):
        if destination.exists():
            raise FileExistsError(f'Output directory already exists: {destination}')
    files = sorted(p for p in source.iterdir() if p.suffix.lower() in {'.csv', '.tbl', '.tsv'})
    counts = [sum(1 for _ in records(p, delimiter, header)) for p in files]
    total = sum(counts)
    if total < rows:
        raise ValueError(f'Requested {rows} rows but source contains {total}')
    quotas = [rows * count // total for count in counts]
    order = sorted(range(len(files)), key=lambda i: (-(rows * counts[i] % total), i))
    for i in order[:rows - sum(quotas)]:
        quotas[i] += 1
    output.mkdir(parents=True, exist_ok=False)
    remainder.mkdir(parents=True, exist_ok=False)
    remainder_manifest = dict(version=1, kind='remainder', rows=total - rows, seed=seed,
                              delimiter=delimiter, header=header, tables=[])
    manifest = dict(version=1, rows=rows, seed=seed, delimiter=delimiter, header=header, tables=[])
    rng = random.Random(seed)
    for path, count, quota in zip(files, counts, quotas):
        source_hash = digest(path)
        target = output / path.name
        remainder_target = remainder / path.name
        remaining, needed = count, quota
        with target.open('w', encoding='utf-8', newline='') as stream, \
                remainder_target.open('w', encoding='utf-8', newline='') as remainder_stream:
            writer = csv.writer(stream, delimiter=delimiter, lineterminator='\n')
            remainder_writer = csv.writer(remainder_stream, delimiter=delimiter, lineterminator='\n')
            if header:
                with path.open(encoding='utf-8', newline='') as original:
                    heading = next((row for row in csv.reader(original, delimiter=delimiter) if row), None)
                    if heading is not None:
                        writer.writerow(heading)
                        remainder_writer.writerow(heading)
            for row in records(path, delimiter, header):
                if remaining <= 0:
                    raise ValueError(f'Source changed during sampling: {path}')
                if rng.randrange(remaining) < needed:
                    writer.writerow(row)
                    needed -= 1
                else:
                    remainder_writer.writerow(row)
                remaining -= 1
        if needed or remaining or digest(path) != source_hash:
            raise ValueError(f'Source changed during sampling: {path}')
        manifest['tables'].append(dict(file=path.name, source_rows=count, rows=quota,
                                      source_sha256=source_hash, sample_sha256=digest(target)))
        remainder_manifest['tables'].append(dict(file=path.name, source_rows=count, rows=count - quota,
                                                source_sha256=source_hash, remainder_sha256=digest(remainder_target)))
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    remainder_manifest['sample_manifest_sha256'] = digest(output / 'manifest.json')
    (remainder / 'remainder-manifest.json').write_text(json.dumps(remainder_manifest, indent=2) + '\n', encoding='utf-8')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--remainder-dir', help='Default: <output-dir>-remaining; writes D minus the sample')
    parser.add_argument('--rows', type=int, default=2_000_000)
    parser.add_argument('--seed', type=int, default=12345)
    parser.add_argument('--delimiter', default=';', help='Default: semicolon (IMDb configuration)')
    parser.add_argument('--no-header', action='store_true')
    args = parser.parse_args()
    try:
        result = create_sample(args.input_dir, args.output_dir, args.rows, args.seed, args.delimiter, not args.no_header, args.remainder_dir)
    except (ValueError, OSError) as error:
        parser.exit(2, f"error: {error}\n")
    print(f"Saved {result['rows']:,} sampled rows to {args.output_dir}")
    remainder = Path(args.remainder_dir) if args.remainder_dir else Path(args.output_dir).resolve().with_name(Path(args.output_dir).resolve().name + '-remaining')
    remaining_rows = sum(table['source_rows'] - table['rows'] for table in result['tables'])
    print(f"Saved {remaining_rows:,} remaining rows to {remainder}")


if __name__ == '__main__':
    csv.field_size_limit(sys.maxsize)
    main()
