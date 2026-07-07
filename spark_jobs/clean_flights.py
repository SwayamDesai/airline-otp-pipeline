"""Structurally clean raw BTS on-time performance CSVs into partitioned Parquet.

Responsibilities of this job (and deliberately nothing more):
  - select the analytically relevant subset of BTS's 110 columns
  - cast every column explicitly (raw files are read as all-string; no inference)
  - normalize flag columns ("1.00"/"0.00") to booleans
  - drop records missing the natural flight key, and exact-key duplicates
  - write Parquet partitioned by year/month for incremental warehouse loads

Business logic (KPI definitions, delay-cause classification, lookups) lives
downstream in dbt — this job only guarantees typed, deduplicated records.

Usage:
    uv run python spark_jobs/clean_flights.py \
        --input-dir data/raw --output-dir data/processed/flights
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("clean_flights")

# Natural key identifying one scheduled flight leg.
FLIGHT_KEY = [
    "flight_date",
    "carrier_code",
    "flight_number",
    "origin",
    "dest",
    "crs_dep_time",
]

# source column -> (target name, target type)
# HHMM wall-clock fields stay as zero-padded strings; parsing them into
# timestamps needs timezone context that belongs in dbt, not here.
COLUMNS: dict[str, tuple[str, T.DataType]] = {
    "FlightDate": ("flight_date", T.DateType()),
    "Reporting_Airline": ("carrier_code", T.StringType()),
    "Flight_Number_Reporting_Airline": ("flight_number", T.StringType()),
    "Tail_Number": ("tail_number", T.StringType()),
    "Origin": ("origin", T.StringType()),
    "OriginCityName": ("origin_city", T.StringType()),
    "OriginState": ("origin_state", T.StringType()),
    "Dest": ("dest", T.StringType()),
    "DestCityName": ("dest_city", T.StringType()),
    "DestState": ("dest_state", T.StringType()),
    "Distance": ("distance_miles", T.DoubleType()),
    "CRSDepTime": ("crs_dep_time", T.StringType()),
    "DepTime": ("dep_time", T.StringType()),
    "DepDelay": ("dep_delay_min", T.DoubleType()),
    "DepDel15": ("dep_delayed_15", T.BooleanType()),
    "TaxiOut": ("taxi_out_min", T.DoubleType()),
    "TaxiIn": ("taxi_in_min", T.DoubleType()),
    "CRSArrTime": ("crs_arr_time", T.StringType()),
    "ArrTime": ("arr_time", T.StringType()),
    "ArrDelay": ("arr_delay_min", T.DoubleType()),
    "ArrDel15": ("arr_delayed_15", T.BooleanType()),
    "CRSElapsedTime": ("crs_elapsed_min", T.DoubleType()),
    "ActualElapsedTime": ("actual_elapsed_min", T.DoubleType()),
    "AirTime": ("air_time_min", T.DoubleType()),
    "Cancelled": ("cancelled", T.BooleanType()),
    "CancellationCode": ("cancellation_code", T.StringType()),
    "Diverted": ("diverted", T.BooleanType()),
    "CarrierDelay": ("carrier_delay_min", T.DoubleType()),
    "WeatherDelay": ("weather_delay_min", T.DoubleType()),
    "NASDelay": ("nas_delay_min", T.DoubleType()),
    "SecurityDelay": ("security_delay_min", T.DoubleType()),
    "LateAircraftDelay": ("late_aircraft_delay_min", T.DoubleType()),
    "Year": ("year", T.IntegerType()),
    "Month": ("month", T.IntegerType()),
}


def get_spark(app_name: str = "clean_flights") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def _cast(source: str, target: str, dtype: T.DataType) -> F.Column:
    col = F.col(source)
    if isinstance(dtype, T.BooleanType):
        # BTS encodes flags as "0.00"/"1.00"; empty means unknown -> null
        return (col.cast(T.DoubleType()) == 1.0).alias(target)
    return col.cast(dtype).alias(target)


def clean_flights(raw: DataFrame) -> DataFrame:
    """Typed, deduplicated flight records from raw all-string BTS rows."""
    typed = raw.select(
        *[_cast(src, name, dtype) for src, (name, dtype) in COLUMNS.items()]
    )
    return (
        typed.dropna(subset=FLIGHT_KEY)
        .dropDuplicates(FLIGHT_KEY)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", default="data/raw")
    parser.add_argument("--output-dir", default="data/processed/flights")
    parser.add_argument(
        "--pattern",
        default="ontime_*.csv",
        help="filename glob within input-dir (e.g. 'ontime_2024_*.csv')",
    )
    args = parser.parse_args()

    input_glob = str(Path(args.input_dir) / args.pattern)
    started = time.time()
    spark = get_spark()

    raw = spark.read.csv(input_glob, header=True, inferSchema=False)
    cleaned = clean_flights(raw)

    (
        cleaned.repartition("year", "month")
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(args.output_dir)
    )

    # Row counts read back from the written dataset so the numbers reflect
    # what actually landed, not a pre-write estimate.
    raw_count = raw.count()
    clean_count = spark.read.parquet(args.output_dir).count()
    elapsed = time.time() - started
    log.info(f"raw rows in:    {raw_count:,}")
    log.info(f"clean rows out: {clean_count:,}")
    log.info(f"dropped:        {raw_count - clean_count:,}")
    log.info(f"elapsed:        {elapsed:.1f}s")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
