"""Download BTS Reporting Carrier On-Time Performance data.

Fetches monthly prezipped CSVs from transtats.bts.gov, extracts them into
data/raw/, and skips months that are already present (idempotent re-runs).

Usage:
    uv run python scripts/download_bts.py --start 2024-01 --end 2024-02
    uv run python scripts/download_bts.py --start 2019-01 --end 2024-12
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
import tempfile
import time
import urllib.request
import zipfile
from pathlib import Path

BASE_URL = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)
RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("download_bts")


def month_range(start: str, end: str) -> list[tuple[int, int]]:
    """Expand 'YYYY-MM' bounds into an inclusive list of (year, month)."""
    start_year, start_month = (int(p) for p in start.split("-"))
    end_year, end_month = (int(p) for p in end.split("-"))
    if (start_year, start_month) > (end_year, end_month):
        raise ValueError(f"start {start} is after end {end}")

    months = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append((year, month))
        month += 1
        if month > 12:
            year, month = year + 1, 1
    return months


def target_csv_path(year: int, month: int) -> Path:
    return RAW_DIR / f"ontime_{year}_{month:02d}.csv"


def download_month(year: int, month: int) -> None:
    """Download and extract one month, retrying transient failures."""
    csv_path = target_csv_path(year, month)
    if csv_path.exists():
        log.info("skip %s (already downloaded)", csv_path.name)
        return

    url = BASE_URL.format(year=year, month=month)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with tempfile.TemporaryDirectory() as tmp:
                zip_path = Path(tmp) / "month.zip"
                log.info("downloading %d-%02d (attempt %d)", year, month, attempt)
                urllib.request.urlretrieve(url, zip_path)

                with zipfile.ZipFile(zip_path) as zf:
                    csv_members = [n for n in zf.namelist() if n.endswith(".csv")]
                    if len(csv_members) != 1:
                        raise RuntimeError(
                            f"expected exactly one CSV in {url}, got {csv_members}"
                        )
                    # Extract to a temp name, then rename: readers scanning
                    # data/raw must never see a partially written CSV.
                    tmp_csv = csv_path.with_suffix(".csv.tmp")
                    with zf.open(csv_members[0]) as src, open(tmp_csv, "wb") as dst:
                        shutil.copyfileobj(src, dst)
                    tmp_csv.rename(csv_path)

            size_mb = csv_path.stat().st_size / 1024 / 1024
            log.info("wrote %s (%.1f MB)", csv_path.name, size_mb)
            return
        except Exception as exc:  # noqa: BLE001 - retry any transient failure
            csv_path.unlink(missing_ok=True)
            if attempt == MAX_RETRIES:
                raise
            log.warning("attempt %d failed (%s), retrying...", attempt, exc)
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="first month, YYYY-MM")
    parser.add_argument("--end", required=True, help="last month (inclusive), YYYY-MM")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    months = month_range(args.start, args.end)
    log.info("downloading %d month(s) into %s", len(months), RAW_DIR)

    for year, month in months:
        download_month(year, month)

    log.info("done: %d month(s) available", len(months))
    return 0


if __name__ == "__main__":
    sys.exit(main())
