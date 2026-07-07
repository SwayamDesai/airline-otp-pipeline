"""Great Expectations release gate over the Snowflake marts.

Runs AFTER dbt build and BEFORE the dashboard refresh. dbt tests already
guarantee structural correctness (grain uniqueness, nulls, row-level
ranges); this checkpoint guards statistical plausibility — the failure
class dbt tests cannot see:

  - row-count bands        -> catches partial loads (80 of 120 months)
  - distribution checks    -> catches silent logic regressions that produce
                              well-formed but implausible numbers
  - freshness              -> catches a pipeline that "succeeded" without
                              actually landing the newest month

Exit code 0 = safe to publish; non-zero = block the refresh. That contract
is what Airflow and CI consume.

Usage:
    uv run python quality/run_checkpoint.py
"""

from __future__ import annotations

import os
import sys
from datetime import date

import great_expectations as gx
import great_expectations.expectations as gxe
from cryptography.hazmat.primitives import serialization

ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "QRFICHI-TQ35414")
USER = os.environ.get("SNOWFLAKE_USER", "PIPELINE_SVC")
KEY_PATH = os.environ.get(
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    os.path.expanduser("~/.snowflake/keys/pipeline_svc_rsa.p8"),
)


def private_key_b64() -> str:
    """Key as base64-encoded DER string — the form the Snowflake connector
    accepts when the key is passed through as text."""
    with open(KEY_PATH) as f:
        lines = [l for l in f.read().splitlines() if not l.startswith("-")]
    return "".join(lines)

# The backfill covers 2015-01 .. 2024-12; the newest month present must be
# at least 2024-12. When live monthly loads begin, tighten this to a
# rolling "within N months of today".
NEWEST_MONTH_FLOOR = date(2024, 12, 1)

# table -> expectations guarding it
SUITES: dict[str, list] = {
    "fct_monthly_otp": [
        # 120 months x ~15 reporting carriers; a partial load lands far
        # below this band.
        gxe.ExpectTableRowCountToBeBetween(min_value=1_500, max_value=2_600),
        gxe.ExpectColumnValuesToBeBetween(column="otp_pct", min_value=0, max_value=100),
        gxe.ExpectColumnValuesToBeBetween(column="cancellation_pct", min_value=0, max_value=100),
        # US-wide OTP has lived in the 70s-80s for decades; a mean outside
        # this band means the definition broke, not the airlines.
        gxe.ExpectColumnMeanToBeBetween(column="otp_pct", min_value=60, max_value=95),
        gxe.ExpectColumnMaxToBeBetween(column="month_start", min_value=NEWEST_MONTH_FLOOR),
        gxe.ExpectColumnValuesToNotBeNull(column="carrier_code"),
    ],
    "fct_route_performance": [
        gxe.ExpectTableRowCountToBeBetween(min_value=450_000, max_value=800_000),
        gxe.ExpectColumnValuesToBeBetween(column="otp_pct", min_value=0, max_value=100),
        gxe.ExpectColumnValuesToBeBetween(column="scheduled_flights", min_value=1),
    ],
    "fct_airport_congestion": [
        gxe.ExpectTableRowCountToBeBetween(min_value=30_000, max_value=55_000),
        gxe.ExpectColumnValuesToBeBetween(column="total_movements", min_value=1),
        # sanity band, not a hard rule: tiny airports produce odd taxi
        # times, so tolerate 1% outliers
        gxe.ExpectColumnValuesToBeBetween(
            column="avg_taxi_out_min", min_value=0, max_value=120, mostly=0.99
        ),
    ],
    "fct_delay_causes": [
        gxe.ExpectTableRowCountToBeBetween(min_value=1_500, max_value=2_600),
        gxe.ExpectColumnValuesToBeBetween(column="total_delay_min", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="carrier_pct", min_value=0, max_value=100),
        gxe.ExpectColumnValuesToBeBetween(column="late_aircraft_pct", min_value=0, max_value=100),
    ],
}


def main() -> int:
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_snowflake(
        name="snowflake_marts",
        account=ACCOUNT,
        user=USER,
        private_key=private_key_b64(),
        database="AIRLINE_OTP",
        schema="ANALYTICS_MARTS",
        warehouse="TRANSFORM_WH",
        role="TRANSFORMER",
    )

    validation_definitions = []
    for table, expectations in SUITES.items():
        asset = datasource.add_table_asset(name=table, table_name=table)
        batch_def = asset.add_batch_definition_whole_table(f"{table}_whole")
        suite = context.suites.add(gx.ExpectationSuite(name=f"{table}_suite"))
        for expectation in expectations:
            suite.add_expectation(expectation)
        validation_definitions.append(
            context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=f"{table}_validation", data=batch_def, suite=suite
                )
            )
        )

    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name="marts_release_gate",
                      validation_definitions=validation_definitions)
    )
    result = checkpoint.run()

    print()
    for vd_id, run_result in result.run_results.items():
        stats = run_result["statistics"]
        table = run_result["suite_name"].removesuffix("_suite")
        status = "PASS" if run_result["success"] else "FAIL"
        print(f"  {status}  {table}: "
              f"{stats['successful_expectations']}/{stats['evaluated_expectations']} expectations")
        if not run_result["success"]:
            for r in run_result["results"]:
                if not r["success"]:
                    print(f"        failed: {r['expectation_config']['type']} "
                          f"{r['expectation_config']['kwargs']}")

    print(f"\n  release gate: {'OPEN — safe to publish' if result.success else 'CLOSED — publish blocked'}")
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
