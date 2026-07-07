# Airline On-Time Performance Analytics Pipeline

End-to-end data engineering pipeline processing **10 years of US flight data
(2015–2024, ~65M records, ~26GB raw)** from the Bureau of Transportation
Statistics into warehouse-modeled KPIs behind an auto-refreshing Power BI
dashboard.

## Architecture

```
BTS monthly CSVs (transtats.bts.gov)
      │  scripts/download_bts.py — idempotent, atomic writes, retry w/ backoff
      ▼
data/raw/ (~26GB, 110-col CSVs)
      │  spark_jobs/clean_flights.py — PySpark 4.x
      │  explicit casts (no schema inference), flag normalization,
      │  natural-key dedup, structural cleaning only
      ▼
data/processed/flights/ — Parquet, partitioned by year/month
      │  COPY INTO                                    [Phase 2]
      ▼
Snowflake  raw → staging → marts, modeled with dbt    [Phase 2]
      │  dbt tests + Great Expectations checkpoint    [Phase 2-3]
      ▼
Power BI (import mode)                                 [Phase 7]
      ▲
      └─ GitHub Actions CI/CD → Power BI REST API refresh [Phase 6]

Orchestration: Airflow DAG                             [Phase 4]
Infrastructure: Terraform-provisioned Snowflake        [Phase 5]
```

## KPIs modeled

- **On-time performance (OTP%)** — flights arriving within 15 min of schedule,
  by carrier / route / month (DOT's standard OTP definition)
- **Delay-cause breakdown** — carrier vs weather vs NAS vs security vs
  late-aircraft, as share of total delay minutes
- **Route-level cancellation rate**
- **Airport congestion** — taxi-out times and flight volume by airport

## Design decisions

| Decision | Rationale |
|---|---|
| Read CSVs all-string, cast explicitly | Schema inference double-scans 26GB and can guess differently across 120 monthly files; explicit casts fail loudly and survive year-over-year schema drift |
| Spark does *structural* cleaning only | Types, dedup, null-key filtering. Business logic (KPI definitions, lookups) lives in dbt where it is SQL-reviewable, tested, and documented |
| Parquet partitioned by `year/month` | Matches BTS's monthly grain — enables incremental warehouse loads and predicate pushdown |
| HHMM times kept as strings | Parsing wall-clock times into timestamps needs timezone context; deferred to dbt rather than half-done in Spark |
| Booleans from BTS's "1.00"/"0.00" flags | Typed at the earliest boundary; empty flags become NULL (unknown), not false |
| Atomic downloads (`.tmp` + rename) | A scheduler re-running ingestion must never let Spark read a partially written CSV |
| `local[*]` Spark master | Dataset exceeds laptop RAM but not laptop disk; the identical job runs unchanged on Databricks/EMR by swapping the master |

## Data notes

- Cancelled/diverted flights have NULL arrival delays by definition — handled
  explicitly in dbt marts, not imputed.
- Natural flight key: `flight_date, carrier_code, flight_number, origin, dest,
  crs_dep_time`; exact-duplicate records are dropped at the Spark layer.

## Running

```bash
# 1. Download raw data (idempotent; re-runs skip existing months)
uv run python scripts/download_bts.py --start 2015-01 --end 2024-12

# 2. Clean to partitioned Parquet
JAVA_HOME=$(/usr/libexec/java_home -v 17) \
  uv run python spark_jobs/clean_flights.py

# Validated sample-run metrics (Jan–Feb 2024): 1,066,492 rows in,
# 1,066,492 out, 12.7s on local[*]
```

Requirements: Python 3.12 (managed by [uv](https://docs.astral.sh/uv/)),
Java 17 for Spark.

## Project status

- [x] Phase 1 — BTS ingestion + PySpark cleaning to partitioned Parquet
- [ ] Phase 2 — Snowflake load + dbt models (staging → marts)
- [ ] Phase 3 — Great Expectations quality gate
- [ ] Phase 4 — Airflow orchestration
- [ ] Phase 5 — Terraform-provisioned Snowflake
- [ ] Phase 6 — GitHub Actions CI/CD + Power BI REST API refresh
- [ ] Phase 7 — Power BI dashboard
