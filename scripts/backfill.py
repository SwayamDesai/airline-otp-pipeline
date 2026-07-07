"""Incremental transform: raw zone → cleaned Parquet, one month at a time.

Reads raw gzipped CSVs landed by ingest_raw.py (S3 raw zone, or a local
directory for dev), applies the structural cleaning in
spark_jobs/clean_flights.py, and publishes Parquet partitioned by
year/month. Transform only — landing raw data is ingest_raw.py's job.

Idempotency: dynamic partition overwrite means re-running a month replaces
exactly that year/month partition and touches nothing else.

Usage:
    # dev: local raw dir -> local parquet
    uv run python scripts/backfill.py --start 2015-01 --end 2015-03 \
        --raw-root data/raw --target data/processed/flights

    # production: S3 raw zone -> S3 lake
    uv run python scripts/backfill.py --start 2015-01 --end 2024-12 \
        --raw-root s3a://<bucket>/raw/flights \
        --target s3a://<bucket>/lake/flights
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import SparkSession

from scripts.download_bts import download_month, month_range, target_csv_path
from spark_jobs.clean_flights import clean_flights


def resolve_raw_path(raw_root: str, year: int, month: int) -> str:
    """Path to one month's raw file in the S3 raw zone or a local dev dir.

    Local dev falls back to downloading from BTS if the month is missing;
    the S3 raw zone is expected to be fully landed by ingest_raw.py first.
    """
    if raw_root.startswith("s3a://"):
        return f"{raw_root}/{year}/ontime_{year}_{month:02d}.csv.gz"
    download_month(year, month)  # no-op if already present
    return str(target_csv_path(year, month))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backfill")

# Must match the Hadoop client version bundled with the pinned PySpark.
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"


def get_spark(target: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName("backfill_flights")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Re-running a month overwrites only that partition.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if target.startswith("s3a://"):
        builder = (
            builder.config("spark.jars.packages", HADOOP_AWS_PACKAGE)
            # Full AWS default chain: env vars -> ~/.aws profile -> instance
            # role. Works identically on a laptop and on EMR/Databricks.
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
            )
        )
    return builder.getOrCreate()


def process_month(
    spark: SparkSession, year: int, month: int, raw_root: str, target: str
) -> int:
    """Clean and publish one month from the raw zone; returns rows published."""
    raw_path = resolve_raw_path(raw_root, year, month)

    raw = spark.read.csv(raw_path, header=True, inferSchema=False)
    cleaned = clean_flights(raw).cache()
    rows = cleaned.count()

    (
        # One month is ~500-650K rows -> a single well-sized Parquet file.
        cleaned.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(target)
    )
    cleaned.unpersist()
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="first month, YYYY-MM")
    parser.add_argument("--end", required=True, help="last month (inclusive), YYYY-MM")
    parser.add_argument(
        "--raw-root",
        required=True,
        help="raw zone root: s3a://bucket/raw/flights or a local dir",
    )
    parser.add_argument(
        "--target",
        required=True,
        help="Parquet root: local path or s3a://bucket/prefix",
    )
    args = parser.parse_args()

    months = month_range(args.start, args.end)
    spark = get_spark(args.target if args.target.startswith("s3a://") else args.raw_root)
    log.info("backfilling %d month(s) -> %s", len(months), args.target)

    total_rows = 0
    started = time.time()
    for i, (year, month) in enumerate(months, 1):
        month_started = time.time()
        rows = process_month(spark, year, month, args.raw_root, args.target)
        total_rows += rows
        log.info(
            "[%d/%d] %d-%02d published %s rows in %.0fs (total %s)",
            i, len(months), year, month,
            f"{rows:,}", time.time() - month_started, f"{total_rows:,}",
        )

    log.info("done: %s rows across %d month(s) in %.1f min",
             f"{total_rows:,}", len(months), (time.time() - started) / 60)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
