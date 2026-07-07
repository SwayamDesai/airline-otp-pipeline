"""Land raw BTS on-time performance files in the S3 raw zone.

Extract-and-load only — no transformation. Each month: download BTS's
prezipped CSV, recompress as gzip (byte-identical content, ~10x smaller,
directly readable by Spark), upload to s3://<bucket>/raw/flights/<year>/,
and delete the local temp file. Months already present in S3 are skipped,
so re-runs and interrupted backfills are safe.

Local temp files are reused if present (e.g. from earlier dev downloads)
and cleaned up after upload.

Usage:
    uv run python scripts/ingest_raw.py --start 2015-01 --end 2024-12 \
        --bucket <bucket-name>
"""

from __future__ import annotations

import argparse
import gzip
import logging
import shutil
import sys
import time
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.download_bts import download_month, month_range, target_csv_path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ingest_raw")


def raw_key(year: int, month: int) -> str:
    return f"raw/flights/{year}/ontime_{year}_{month:02d}.csv.gz"


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def ingest_month(s3, bucket: str, year: int, month: int) -> None:
    key = raw_key(year, month)
    if object_exists(s3, bucket, key):
        log.info("skip %s (already in S3)", key)
        return

    download_month(year, month)  # no-op if the CSV is already local
    csv_path = target_csv_path(year, month)
    gz_path = csv_path.with_suffix(".csv.gz")

    with open(csv_path, "rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst)

    size_mb = gz_path.stat().st_size / 1024 / 1024
    s3.upload_file(str(gz_path), bucket, key)
    log.info("uploaded s3://%s/%s (%.1f MB)", bucket, key, size_mb)

    csv_path.unlink()
    gz_path.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="first month, YYYY-MM")
    parser.add_argument("--end", required=True, help="last month (inclusive), YYYY-MM")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()

    s3 = boto3.client("s3")
    months = month_range(args.start, args.end)
    log.info("ingesting %d month(s) -> s3://%s/raw/flights/", len(months), args.bucket)

    started = time.time()
    for i, (year, month) in enumerate(months, 1):
        ingest_month(s3, args.bucket, year, month)
        if i % 12 == 0:
            log.info("[%d/%d] elapsed %.1f min", i, len(months),
                     (time.time() - started) / 60)

    log.info("done: %d month(s) in %.1f min", len(months),
             (time.time() - started) / 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
