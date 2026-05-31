# Parallel-ETL-Converging-on-One-Analytical-Store-DuckDB-Quack-
Many ETL workers run at the same time, each cleaning a different shard of messy raw data, all loading into one DuckDB store 'no staging database, no post-hoc merge step.The classic embedded-DuckDB problem is that only one
process can write the file; the usual workaround is to stage each worker's
output separately and merge later. [Quack](https://duckdb.org/quack/) removes
that: one server serializes the commits, and every worker loads in parallel.

```
 raw/orders_shard_00.csv ─► worker w1 ─┐  Extract → Transform → Load
 raw/orders_shard_01.csv ─► worker w2 ─┤      (clean in each worker's
 raw/orders_shard_02.csv ─► worker w3 ─┤       OWN process, in parallel)
 raw/orders_shard_03.csv ─► worker w1 ─┼──►  Quack server ──► warehouse.db
 raw/orders_shard_04.csv ─► worker w2 ─┤     serializes commits   ▲
 raw/orders_shard_05.csv ─► worker w3 ─┘                          │
                                              report.py ──────────┘
                                       (server-side analytics via remote.query)
```

## Run it

```bash
python run_etl.py                    # generate raw if missing, 3 workers, 6 shards
python run_etl.py --workers 6        # one worker per shard (max parallelism)
python run_etl.py --regen --shards 8 --rows 10000   # bigger, fresh raw data
python run_etl.py --fresh            # wipe warehouse.db and reload from scratch
```

You'll see each worker report its per-shard `in / loaded / rejected / dupe`
counts as it finishes, then a converged report: revenue by region & category,
a monthly trend, top customers, and a data-quality audit.
<img width="1108" height="703" alt="lnkdn7" src="https://github.com/user-attachments/assets/739dbaed-3fb5-4091-9f13-6ad0086a39df" />
<img width="548" height="707" alt="lnkdn8" src="https://github.com/user-attachments/assets/0825bad8-48dc-4495-9830-c72326b1e5dd" />
<img width="1270" height="397" alt="lnkdn9" src="https://github.com/user-attachments/assets/5559ee2f-913d-48f9-b17f-c45d57e8abfb" />

### The pieces

| File | Role |
|------|------|
| `generate_raw.py` | Writes N messy CSV shards (3 date formats, `$`/`USD`/comma prices, bad quantities, duplicate IDs, missing fields). |
| `server.py`       | Quack server owning `warehouse.db` with the clean `orders` + `load_audit` tables. |
| `worker.py`       | **The ETL.** Extract a shard → Transform (clean/validate/dedup) → Load into the shared store. Run many in parallel. |
| `report.py`       | Final analytics, every query pushed **server-side** via `remote.query()`. |
| `run_etl.py`      | Orchestrates: raw data → server → K parallel workers → report. |

## The transform (what real ETL actually does)

Each worker reads its shard with everything as text (so dirty values don't
break the read), then in one SQL pipeline:

1. **Parse dates** from `2026-05-01`, `05/01/2026`, *and* `01-05-2026` via
   `try_strptime(...)` + `coalesce`; blanks become `NULL` → rejected.
2. **Clean prices** — strip `$`, `USD`, and convert the European decimal comma
   (`265,86` → `265.86`) with `regexp_replace` + `replace`.
3. **Normalise** region (UPPER) and category (lower), trim whitespace.
4. **Validate** — drop rows with no `order_id`, unparseable date, or
   non-positive quantity/price.
5. **Compute** `revenue = quantity * unit_price`.
6. **De-duplicate** by `order_id` with `row_number() OVER (PARTITION BY ...)`.
7. **Load** clean rows + provenance columns (`source_shard`, `loaded_by`,
   `load_ts`) into `remote.orders`, and write a `load_audit` row.

The `load_audit` table makes the run **observable**: how many rows each worker
read, kept, rejected, and de-duplicated — exactly the kind of data-quality
metrics a production pipeline tracks.

## Idempotency (safe re-runs)

Before loading a shard, the worker **deletes that shard's prior rows**, then
inserts. So re-running never double-counts — the converged total stays the same
whether you run with 2 workers or 6. This is the standard *delete-insert by
partition* pattern (the same idea dbt uses for incremental models). Verified:
running twice keeps the store at exactly the same row count.

## Quack gotchas this project hit (beta)

1. **Only `INSERT` works through the attached `remote.*` catalog.**
   `DELETE`/`UPDATE` there raise `Can only delete from base table`. Route them
   server-side via `remote.query($q$ DELETE FROM ... $q$)` instead (see
   `worker.remote_exec`). `INSERT ... SELECT` into `remote.orders` works fine.
2. **One server per port.** Only one Quack server can own port **9494**. If a
   previous demo is still running, a new server silently fails to bind and your
   clients hit the *old* server → `Authentication failed` (wrong token). Stop
   prior demos first. On Windows: `Get-Process python | Stop-Process`.

## Where this maps in the real world

This is the shape of a real **ELT/ETL fan-out**: partition the input, transform
each partition in parallel, converge into one analytical table. Raw files in S3 → parallel **dbt** models / Snowflake `COPY INTO`→ one fact table → **Sigma** dashboards. Here the same pattern runs on one
laptop with no warehouse — Quack supplies the concurrent-load capability that
embedded DuckDB lacks.

