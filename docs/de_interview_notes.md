# Data engineering interview notes

Every answer here is grounded in this project, so in an interview you can say
*"in my airline pipeline I handled that by..."* instead of reciting theory.
This file grows as the project does — each phase adds its questions.

---

## Q1. "Your pipeline normally takes 3 hours. Today it's been running 20. What do you do?"

**During the incident (in order):**
- Check if it's *progressing* or *hung* — look at task logs / Spark UI. Progressing slowly and hung are different problems.
- Check what's downstream: will stale data break something at 9am? Decide kill-vs-wait based on the SLA, not panic.
- If killed: the pipeline must be safe to re-run (see Q2). If re-running isn't safe, that's the real bug.

**Diagnosing (what usually causes it):**
- Input volume spiked (upstream sent 50x data — did anyone check row counts?)
- Data skew — one task gets most of the data (see Q7)
- Spill to disk — data no longer fits in memory, everything goes 10x slower
- A join went wrong — accidental cross join / broadcast that stopped broadcasting
- Cluster contention — someone else's job ate the resources
- Small-files explosion — job spends hours listing/opening files (see Q6)

**Preventing it (what this project does):**
- **Bounded work per run**: each run processes exactly one month (~550K rows, ~10s).
  A 20-hour run is impossible *by construction* — the unit of work can't grow.
- **Timeouts**: Airflow tasks get `execution_timeout` — a task running 5x its
  normal time is killed and alerted, not left running overnight. *(Phase 4)*
- **Row-count logging every run**: we log rows in/out per month, so a volume
  spike is visible before it becomes a runtime problem.
- **Runtime trending**: Airflow tracks task durations — a task that took 10s
  for 100 runs and suddenly takes 4 min gets noticed.

```
bad:  [ whole history every night ] ← work grows forever, one day it's 20h
ours: [ Jan ] [ Feb ] [ Mar ] ...   ← fixed-size units, runtime can't drift far
```

---

## Q2. "Your pipeline died halfway. Is it safe to just re-run it?"

Yes — because everything is **idempotent** (running twice = running once):

- **Downloads**: skip months already present; files are written to a `.tmp`
  name then renamed, so a killed download never leaves a half-file that a
  reader might trust.
- **Spark writes**: `partitionOverwriteMode=dynamic` — re-running March
  *replaces* the March partition. No duplicates, nothing else touched.
- **S3 ingestion**: checks `head_object` first — already-uploaded months skip.

```
re-run month 2015-02:
  year=2015/month=1/   untouched
  year=2015/month=2/   ← replaced, not appended
  year=2015/month=3/   untouched
```

**Interview line:** "I don't try to make failures impossible — I make re-runs
free. Then failure handling is just 'retry'."

**War story from this project:** during the 120-month backfill, the BTS
server stalled one download mid-connection. The code used a network call
with **no timeout**, so it didn't fail — it slept forever at 0% CPU while
logs went silent. Retry logic never fired because nothing ever *failed*.
Fix: socket timeout on every network call, so a stall becomes an exception,
and the existing retry/backoff handles it. Lesson: **silence is not
progress — a pipeline without timeouts hangs instead of failing loudly.**
Restart cost nothing because ingestion is idempotent (skips months already
in S3).

---

## Q3. "How do you handle duplicate data?"

- Define the **natural key** — the columns that identify one real-world event.
  Here: `flight_date + carrier + flight_number + origin + dest + scheduled_dep_time`.
- Dedup on that key at the *earliest* layer (our Spark job), so nothing
  downstream ever needs to worry about it.
- Idempotent writes (Q2) prevent *self-inflicted* duplicates from re-runs —
  most real-world dupes come from the pipeline itself, not the source.

---

## Q4. "What if the source schema changes?" (schema drift)

BTS has 110 columns and has changed them over the years. Our defenses:

- **Never `inferSchema`** — inference guesses per-file, so two months can
  silently disagree. We read everything as string and **cast explicitly**.
- **Select by name, not position** — if BTS *adds* columns, nothing breaks;
  we simply don't read them.
- If BTS *removes or renames* a column we need → the job **fails loudly** at
  the select. Loud failure at ingestion beats silent nulls in a dashboard.
- dbt tests + Great Expectations downstream catch subtler changes (e.g. a
  column suddenly all-null). *(Phases 2–3)*

---

## Q5. "Why Parquet? Why partition by year/month?"

- **Parquet = columnar**: a query touching 3 of 34 columns reads ~1/10 of the
  bytes. Plus heavy compression (our 26GB CSV → ~2-3GB).
- **Partitioning = pruning**: `WHERE year=2023 AND month=3` reads one folder,
  not the whole lake.
- Partition by what queries filter on, at the granularity data arrives in.
  Monthly data + monthly loads + monthly dashboards → `year/month`.
- **Don't over-partition**: partitioning by day or by airport would create
  thousands of tiny files (see Q6).

---

## Q6. "What's the small-files problem?"

- Object stores and Spark pay a fixed cost *per file* (listing, opening,
  task scheduling). A million 10KB files is far slower than a hundred 100MB
  files holding the same data.
- Classic cause: streaming or over-parallel jobs writing thousands of part
  files per partition.
- **Here**: we `coalesce(1)` per month — one ~30MB file per partition.
  120 months → 120 well-sized files. Snowflake and Spark both love this.

---

## Q7. "What is data skew and how do you fix it?"

- Skew = work is split by key, but keys aren't equal. Partition flights by
  airport and ATL has 100x the flights of a small regional — one task does
  everything while 99 idle.

```
task 1: ATL  ████████████████████  (everyone waits for this one)
task 2: XNA  █
task 3: BTV  █
```

- Fixes, in order of preference:
  - **AQE** (Adaptive Query Execution — on by default in Spark 3+): splits
    oversized partitions automatically at shuffle time.
  - **Broadcast join**: if one side is small (our airport/carrier lookups),
    ship it to every worker — no shuffle of the big side at all.
  - **Salting**: append a random suffix to hot keys (`ATL_1..ATL_8`) to force
    a spread, aggregate in two steps.
- **Here**: we partition work by *month* (naturally even — every month has
  roughly 400-650K flights), not by a skewed key like airport.

---

## Q8. "Why is this a batch pipeline and not streaming?"

- Match pipeline cadence to **source cadence**: BTS publishes once a month.
  Streaming a monthly source buys nothing and costs complexity 24/7.
- Choose streaming when consumers need sub-minute freshness (fraud checks,
  live ops dashboards) — not because it sounds more advanced.
- **Interview line:** "Freshness is a product requirement, not a tech choice.
  The cheapest pipeline that meets the SLA wins."

---

## Q9. "ETL or ELT? Where does transformation belong?"

This project deliberately splits T into two kinds:

- **Structural T in Spark** (before the warehouse): types, dedup, file
  format. Belongs early — everything downstream benefits.
- **Business T in dbt/Snowflake** (ELT): KPI definitions, joins, filters.
  Belongs in SQL in the warehouse where analysts can read, review, and test
  it — and where changing a definition doesn't mean reprocessing files.

```
BTS ──EL──▶ S3 raw ──T(structure)──▶ S3 parquet ──L──▶ Snowflake ──T(business)──▶ marts
```

- Rule of thumb: *would an analyst ever want to change this logic?*
  Yes → dbt. No → Spark.

---

## Q10. "How do you know your pipeline is healthy?" (observability)

Four layers, weakest to strongest:

- **Logs** — every run logs rows in/out per month, elapsed time.
- **Metrics/trends** — Airflow task durations + row counts over time; a
  volume spike or slowdown is visible *before* it breaks anything.
- **Quality gates** — dbt tests (nulls, uniqueness, accepted values) and a
  Great Expectations checkpoint (OTP% ∈ [0,100], sane row counts) that
  **block** publishing, not just warn.
- **Freshness SLAs** — "the dashboard must reflect last month by the 15th";
  alert on breach, not on someone noticing.

**Interview line:** "A dashboard silently showing wrong numbers is worse
than a pipeline that fails loudly. Gates block, alerts page, and bad data
never reaches stakeholders."

---

## Q11. "Can you guarantee exactly-once processing?"

- True exactly-once *delivery* is near-impossible in distributed systems.
  The practical answer: **at-least-once delivery + idempotent writes =
  exactly-once *end state***.
- Here: a month may be downloaded or processed twice (retry, re-run), but
  since writes replace partitions (Q2), the final data is identical either
  way. That's what actually matters.

---

## Q12. "This runs on your laptop. How does it scale 100x?"

Every component was chosen so the answer is "change config, not code":

| Component | Laptop today | 100x scale |
|---|---|---|
| Spark | `local[*]` | Databricks / EMR cluster — same job, different master |
| Storage | S3 | S3 (already infinitely scalable) |
| Warehouse | Snowflake XS warehouse | Resize to L / multi-cluster — one line |
| Orchestration | Airflow in Docker | MWAA / Astronomer — same DAGs |
| Transform | dbt Core | dbt Core, unchanged — Snowflake does the compute |

- The one real change at scale: stop `coalesce(1)` per month and let file
  counts grow with data (aim for ~128MB-1GB files).

---

## Q13. "Do you write a new DAG for each month's data?"

No — **one DAG, many DAG runs**. Classic confusion worth nailing:

- The **DAG** = the definition. One Python file, written once: steps + schedule.
- A **DAG run** = one execution, stamped with a **logical date** ("which data
  period this run owns"). The scheduler creates one automatically each month.
- Tasks use the logical date as a parameter: download month X, transform
  partition X, load partition X. Same code every month.

```
flights_pipeline.py (one file, @monthly)
  ├─ run 2026-05 → May data
  ├─ run 2026-06 → June data
  └─ run 2026-07 → July data   ← created automatically
```

- **Catchup**: scheduler was down 3 months? It creates the 3 missing runs by
  itself. Our 120-month history load = catchup for 120 runs.
- **Surgical re-runs**: bad March data → clear March's run in the UI; only
  March reprocesses, because every write is partition-scoped (Q2).

---

## Q14. "You already have dbt tests. Why also Great Expectations?"

Different failure classes, different moments:

| | dbt tests | GE checkpoint |
|---|---|---|
| Validates | structure (nulls, uniqueness, row-level ranges) | plausibility (volumes, distributions, freshness) |
| Catches | broken grain, bad joins, invalid values | partial loads, silent logic regressions |
| Runs | at build time, per model | as the final release gate before publishing |

- Example dbt can't catch: all 120 months load, OTP is 0-100 everywhere,
  grain is unique — but a definition change made **mean OTP 96%**. Perfectly
  well-formed, obviously wrong. GE's distribution check
  (`mean otp_pct between 60 and 95`) blocks it.
- Example GE-only: pipeline "succeeds" but only 80 of 120 months landed —
  every dbt test passes on the partial data. GE's **row-count band** fails.
- The gate's contract is its exit code: 0 = publish, 1 = the dashboard
  refresh never fires. Verified both directions in this project.

**Interview line:** "dbt tests ask *is this table well-formed?* — the GE
gate asks *does this data look like reality?* Both, because each catches
what the other can't."

---

*Added per phase: Snowflake/dbt questions (Phase 2), data quality (Phase 3),
Airflow scheduling/retries/backfills (Phase 4), IaC (Phase 5), CI/CD (Phase 6).*
