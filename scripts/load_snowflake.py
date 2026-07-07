"""Load cleaned Parquet from the S3 lake into Snowflake via COPY INTO.

Runs as PIPELINE_SVC (key-pair auth) with the LOADER role. The target table
partition columns (year, month) are not stored inside the Parquet files —
Spark encodes them in the S3 path — so they are parsed from
METADATA$FILENAME during the COPY.

Load semantics:
  --full            truncate raw.flights, then COPY the whole lake
  --month YYYY-MM   delete that month's rows, then COPY just its partition
                    (delete + insert: safe to re-run after a re-transform,
                    never duplicates)

COPY itself skips files Snowflake has already loaded (load-history
metadata), so plain re-runs are also free.

Usage:
    uv run python scripts/load_snowflake.py --full
    uv run python scripts/load_snowflake.py --month 2025-01
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import snowflake.connector
from cryptography.hazmat.primitives import serialization

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("load_snowflake")

ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "QRFICHI-TQ35414")
USER = os.environ.get("SNOWFLAKE_USER", "PIPELINE_SVC")
KEY_PATH = os.environ.get(
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    os.path.expanduser("~/.snowflake/keys/pipeline_svc_rsa.p8"),
)

CREATE_TABLE = """
create table if not exists raw.flights (
    flight_date              date,
    carrier_code             varchar(8),
    flight_number            varchar(8),
    tail_number              varchar(12),
    origin                   varchar(5),
    origin_city              varchar(64),
    origin_state             varchar(4),
    dest                     varchar(5),
    dest_city                varchar(64),
    dest_state               varchar(4),
    distance_miles           float,
    crs_dep_time             varchar(6),
    dep_time                 varchar(6),
    dep_delay_min            float,
    dep_delayed_15           boolean,
    taxi_out_min             float,
    taxi_in_min              float,
    crs_arr_time             varchar(6),
    arr_time                 varchar(6),
    arr_delay_min            float,
    arr_delayed_15           boolean,
    crs_elapsed_min          float,
    actual_elapsed_min       float,
    air_time_min             float,
    cancelled                boolean,
    cancellation_code        varchar(2),
    diverted                 boolean,
    carrier_delay_min        float,
    weather_delay_min        float,
    nas_delay_min            float,
    security_delay_min       float,
    late_aircraft_delay_min  float,
    year                     integer,
    month                    integer,
    _loaded_at               timestamp_ntz default current_timestamp()
)
"""

# Data columns present inside the Parquet files, in table order.
PARQUET_COLUMNS = [
    "flight_date", "carrier_code", "flight_number", "tail_number",
    "origin", "origin_city", "origin_state",
    "dest", "dest_city", "dest_state", "distance_miles",
    "crs_dep_time", "dep_time", "dep_delay_min", "dep_delayed_15",
    "taxi_out_min", "taxi_in_min",
    "crs_arr_time", "arr_time", "arr_delay_min", "arr_delayed_15",
    "crs_elapsed_min", "actual_elapsed_min", "air_time_min",
    "cancelled", "cancellation_code", "diverted",
    "carrier_delay_min", "weather_delay_min", "nas_delay_min",
    "security_delay_min", "late_aircraft_delay_min",
]


def copy_statement(pattern: str | None) -> str:
    select_cols = ",\n        ".join(f"$1:{c}" for c in PARQUET_COLUMNS)
    pattern_clause = f"pattern = '{pattern}'" if pattern else ""
    return f"""
    copy into raw.flights ({", ".join(PARQUET_COLUMNS)}, year, month)
    from (
        select
        {select_cols},
        regexp_substr(metadata$filename, 'year=([0-9]+)', 1, 1, 'e')::integer,
        regexp_substr(metadata$filename, 'month=([0-9]+)', 1, 1, 'e')::integer
        from @raw.lake_stage
    )
    {pattern_clause}
    file_format = (type = parquet)
    """


def connect() -> snowflake.connector.SnowflakeConnection:
    with open(KEY_PATH, "rb") as f:
        pkey = serialization.load_pem_private_key(f.read(), password=None)
    der = pkey.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        private_key=der,
        role="LOADER",
        warehouse="LOAD_WH",
        database="AIRLINE_OTP",
        schema="RAW",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full", action="store_true", help="truncate + load all")
    group.add_argument("--month", help="delete + load one month, YYYY-MM")
    args = parser.parse_args()

    conn = connect()
    cur = conn.cursor()
    cur.execute(CREATE_TABLE)

    if args.full:
        log.info("full load: truncating raw.flights")
        cur.execute("truncate table if exists raw.flights")
        copy_sql = copy_statement(pattern=None)
    else:
        year, month = (int(p) for p in args.month.split("-"))
        log.info("month load %d-%02d: delete + copy", year, month)
        cur.execute(
            "delete from raw.flights where year = %s and month = %s",
            (year, month),
        )
        copy_sql = copy_statement(pattern=f".*year={year}/month={month}/.*")

    cur.execute(copy_sql)
    files_loaded = len(cur.fetchall())

    cur.execute("select count(*) from raw.flights")
    total_rows = cur.fetchone()[0]
    log.info("copied %d file(s); raw.flights now has %s rows",
             files_loaded, f"{total_rows:,}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
