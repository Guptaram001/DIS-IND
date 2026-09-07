#!/usr/bin/env python3
"""Download the Ensembl 111 TSV collection used as UniProt by SPIND."""

from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import os
import re
import shutil
import sys
import urllib.parse
import urllib.request
from pathlib import Path


BASE_URL = "https://ftp.ensembl.org/pub/release-111/tsv/"
AVAILABLE_SCHEMAS = ("ena", "entrez", "refseq", "uniprot")
HREF = re.compile(r'href="([^"]+)"', re.IGNORECASE)


def read_url(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "DIS-IND-dataset-downloader/1.0"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read().decode("utf-8", errors="replace")


def links(url: str) -> list[str]:
    return [urllib.parse.urljoin(url, href) for href in HREF.findall(read_url(url))]


def discover(schemas: tuple[str, ...]) -> list[str]:
    species_urls = sorted(
        url for url in links(BASE_URL)
        if url.startswith(BASE_URL) and url.endswith("/") and url != BASE_URL
    )
    wanted_suffixes = tuple(f".{schema}.tsv.gz" for schema in schemas)
    files: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        future_to_species = {executor.submit(links, url): url for url in species_urls}
        for index, future in enumerate(concurrent.futures.as_completed(future_to_species), 1):
            species_url = future_to_species[future]
            try:
                files.extend(url for url in future.result() if url.endswith(wanted_suffixes))
            except Exception as error:
                raise RuntimeError(f"Could not list {species_url}: {error}") from error
            if index % 50 == 0 or index == len(species_urls):
                print(f"Discovered files in {index}/{len(species_urls)} species", flush=True)
    return sorted(files)


def download_and_extract(url: str, output_dir: Path) -> tuple[str, int]:
    archive_name = urllib.parse.unquote(Path(urllib.parse.urlparse(url).path).name)
    output_name = archive_name[:-3]
    output_path = output_dir / output_name
    if output_path.is_file() and output_path.stat().st_size > 0:
        return output_name, output_path.stat().st_size

    archive_path = output_dir / f".{archive_name}.part"
    request = urllib.request.Request(url, headers={"User-Agent": "DIS-IND-dataset-downloader/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=300) as response, archive_path.open("wb") as target:
            shutil.copyfileobj(response, target, length=1024 * 1024)
        extracted_path = output_dir / f".{output_name}.part"
        with gzip.open(archive_path, "rb") as source, extracted_path.open("wb") as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
        os.replace(extracted_path, output_path)
    finally:
        archive_path.unlink(missing_ok=True)
    return output_name, output_path.stat().st_size


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("data/uniprot"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--schemas",
        nargs="+",
        choices=AVAILABLE_SCHEMAS,
        default=["uniprot"],
        help="Ensembl mapping variants to fetch; SPIND's UniProt benchmark uses only 'uniprot'",
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)

    urls = discover(tuple(args.schemas))
    if not urls:
        raise RuntimeError("No Ensembl release-111 mapping files were discovered")
    print(f"Downloading {len(urls)} mapping tables into {args.output}", flush=True)

    completed: list[tuple[str, int, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_url = {
            executor.submit(download_and_extract, url, args.output): url for url in urls
        }
        for index, future in enumerate(concurrent.futures.as_completed(future_to_url), 1):
            url = future_to_url[future]
            try:
                name, size = future.result()
            except Exception as error:
                print(f"FAILED {url}: {error}", file=sys.stderr, flush=True)
                raise
            completed.append((name, size, url))
            if index % 25 == 0 or index == len(urls):
                mib = sum(size for _, size, _ in completed) / (1024 * 1024)
                print(f"Prepared {index}/{len(urls)} tables ({mib:.1f} MiB extracted)", flush=True)

    # Keep metadata out of .tsv because DIS-IND treats every top-level TSV as a
    # relational input table.
    manifest = args.output / "MANIFEST.txt"
    with manifest.open("w", encoding="utf-8", newline="") as output:
        output.write("file\tbytes\tsource\n")
        for name, size, url in sorted(completed):
            output.write(f"{name}\t{size}\t{url}\n")
    total = sum(size for _, size, _ in completed)
    print(f"Complete: {len(completed)} TSV tables, {total / (1024 * 1024):.1f} MiB", flush=True)
    print(f"Manifest: {manifest}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
