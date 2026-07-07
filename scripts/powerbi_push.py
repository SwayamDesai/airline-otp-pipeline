"""Push Snowflake mart tables into a Power BI push dataset.

Runs as the final pipeline step, after the Great Expectations gate opens.
Authentication is a delegated user token via Microsoft's device-code flow
(no app registration needed — uses the well-known Azure CLI public client).
First run: `--auth-only` prompts a one-time browser login; the refresh
token is cached at ~/.powerbi/token_cache.json and renews silently on
every subsequent run.

The BI layer stays thin by design: all business logic lives in dbt, and
this script only ships finished KPI tables. Push-dataset limits (10K rows
per request, 120 requests/min) are respected via batching.

Usage:
    uv run python scripts/powerbi_push.py --auth-only   # one-time login
    uv run python scripts/powerbi_push.py               # full push
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from decimal import Decimal
from pathlib import Path

import msal
import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("powerbi_push")

# Azure CLI's public client id — pre-consented in effectively every tenant.
PUBLIC_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
AUTHORITY = "https://login.microsoftonline.com/organizations"
SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]
TOKEN_CACHE = Path(
    os.environ.get("POWERBI_TOKEN_CACHE",
                   os.path.expanduser("~/.powerbi/token_cache.json"))
)

PBI = "https://api.powerbi.com/v1.0/myorg"
DATASET_NAME = "airline_otp"
ROWS_PER_REQUEST = 10_000

ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "QRFICHI-TQ35414")
USER = os.environ.get("SNOWFLAKE_USER", "PIPELINE_SVC")
KEY_PATH = os.environ.get(
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    os.path.expanduser("~/.snowflake/keys/pipeline_svc_rsa.p8"),
)

# mart -> Power BI table schema. Datetime columns are ISO-formatted at push.
TABLES: dict[str, list[tuple[str, str]]] = {
    "fct_monthly_otp": [
        ("month_start", "Datetime"), ("carrier_code", "String"),
        ("carrier_name", "String"), ("scheduled_flights", "Int64"),
        ("cancelled_flights", "Int64"), ("diverted_flights", "Int64"),
        ("completed_flights", "Int64"), ("on_time_flights", "Int64"),
        ("otp_pct", "Double"), ("cancellation_pct", "Double"),
        ("avg_arr_delay_min", "Double"), ("avg_dep_delay_min", "Double"),
    ],
    "fct_route_performance": [
        ("month_start", "Datetime"), ("origin", "String"),
        ("origin_city", "String"), ("origin_state", "String"),
        ("dest", "String"), ("dest_city", "String"), ("dest_state", "String"),
        ("distance_miles", "Double"), ("scheduled_flights", "Int64"),
        ("cancelled_flights", "Int64"), ("completed_flights", "Int64"),
        ("otp_pct", "Double"), ("cancellation_pct", "Double"),
        ("avg_arr_delay_min", "Double"),
    ],
    "fct_airport_congestion": [
        ("month_start", "Datetime"), ("airport_code", "String"),
        ("city", "String"), ("state", "String"), ("departures", "Int64"),
        ("arrivals", "Int64"), ("total_movements", "Int64"),
        ("avg_taxi_out_min", "Double"), ("avg_taxi_in_min", "Double"),
    ],
    "fct_delay_causes": [
        ("month_start", "Datetime"), ("carrier_code", "String"),
        ("carrier_name", "String"), ("carrier_delay_min", "Double"),
        ("weather_delay_min", "Double"), ("nas_delay_min", "Double"),
        ("security_delay_min", "Double"), ("late_aircraft_delay_min", "Double"),
        ("total_delay_min", "Double"), ("carrier_pct", "Double"),
        ("weather_pct", "Double"), ("nas_pct", "Double"),
        ("security_pct", "Double"), ("late_aircraft_pct", "Double"),
    ],
}


def get_token(interactive_ok: bool) -> str:
    TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
    cache = msal.SerializableTokenCache()
    if TOKEN_CACHE.exists():
        cache.deserialize(TOKEN_CACHE.read_text())

    app = msal.PublicClientApplication(
        PUBLIC_CLIENT_ID, authority=AUTHORITY, token_cache=cache
    )

    result = None
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])

    if not result and interactive_ok:
        flow = app.initiate_device_flow(scopes=SCOPES)
        if "user_code" not in flow:
            raise RuntimeError(f"device flow failed: {flow}")
        print(f"\n>>> {flow['message']}\n", flush=True)
        result = app.acquire_token_by_device_flow(flow)

    if not result or "access_token" not in result:
        raise RuntimeError(
            f"no token (run with --auth-only first): {result and result.get('error_description')}"
        )

    if cache.has_state_changed:
        TOKEN_CACHE.write_text(cache.serialize())
        TOKEN_CACHE.chmod(0o600)
    return result["access_token"]


def snowflake_conn():
    with open(KEY_PATH, "rb") as f:
        pkey = serialization.load_pem_private_key(f.read(), password=None)
    der = pkey.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=ACCOUNT, user=USER, private_key=der,
        role="TRANSFORMER", warehouse="TRANSFORM_WH",
        database="AIRLINE_OTP", schema="ANALYTICS_MARTS",
    )


def ensure_dataset(session: requests.Session) -> str:
    """Return the push dataset's id, creating it if absent."""
    existing = session.get(f"{PBI}/datasets").json().get("value", [])
    for ds in existing:
        if ds["name"] == DATASET_NAME:
            return ds["id"]

    definition = {
        "name": DATASET_NAME,
        "defaultMode": "Push",
        "tables": [
            {
                "name": table,
                "columns": [{"name": c, "dataType": t} for c, t in columns],
            }
            for table, columns in TABLES.items()
        ],
    }
    resp = session.post(f"{PBI}/datasets?defaultRetentionPolicy=None",
                        json=definition)
    resp.raise_for_status()
    dataset_id = resp.json()["id"]
    log.info("created push dataset %s (%s)", DATASET_NAME, dataset_id)
    return dataset_id


def request_with_retry(session, method: str, url: str, max_attempts: int = 5,
                       **kwargs) -> requests.Response:
    """Retry 429 (throttle) and 5xx (transient) with exponential backoff."""
    for attempt in range(1, max_attempts + 1):
        resp = session.request(method, url, **kwargs)
        if resp.status_code < 500 and resp.status_code != 429:
            resp.raise_for_status()
            return resp
        if attempt == max_attempts:
            resp.raise_for_status()
        delay = int(resp.headers.get("Retry-After", 2 ** attempt))
        log.warning("%s %s -> %d, retrying in %ds (attempt %d/%d)",
                    method, url.rsplit('/', 2)[-2], resp.status_code,
                    delay, attempt, max_attempts)
        time.sleep(delay)
    raise RuntimeError("unreachable")


def push_table(session, cur, dataset_id: str, table: str,
               columns: list[tuple[str, str]]) -> int:
    names = [c for c, _ in columns]
    cur.execute(f"select {', '.join(names)} from {table}")

    # Replace-all semantics: clear existing rows, then repush.
    request_with_retry(
        session, "DELETE", f"{PBI}/datasets/{dataset_id}/tables/{table}/rows"
    )

    total = 0
    while True:
        batch = cur.fetchmany(ROWS_PER_REQUEST)
        if not batch:
            break
        rows = [
            {
                name: (
                    value.isoformat() if hasattr(value, "isoformat")
                    else float(value) if isinstance(value, Decimal)
                    else value
                )
                for name, value in zip(names, row)
            }
            for row in batch
        ]
        request_with_retry(
            session, "POST",
            f"{PBI}/datasets/{dataset_id}/tables/{table}/rows",
            json={"rows": rows},
        )
        total += len(rows)

    log.info("%s: pushed %s rows", table, f"{total:,}")
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--auth-only", action="store_true",
                        help="acquire + cache a token, then exit")
    args = parser.parse_args()

    token = get_token(interactive_ok=args.auth_only)
    if args.auth_only:
        log.info("token cached at %s", TOKEN_CACHE)
        return 0

    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {token}"

    dataset_id = ensure_dataset(session)
    conn = snowflake_conn()
    cur = conn.cursor()

    grand_total = sum(
        push_table(session, cur, dataset_id, table, columns)
        for table, columns in TABLES.items()
    )
    conn.close()
    log.info("done: %s rows across %d tables", f"{grand_total:,}", len(TABLES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
