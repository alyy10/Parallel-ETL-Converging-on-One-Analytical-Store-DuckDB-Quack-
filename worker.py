"""One ETL worker — Extract, Transform, Load for a set of raw shards.

Run many of these at once (run_etl.py does). Each is its own OS process and
loads into the same Quack-served store concurrently. Per shard it:

  EXTRACT    read the raw CSV with DuckDB (all columns as text, so dirty
             values don't blow up on read)
  TRANSFORM  parse dates from 3 formats, strip currency junk off prices,
             normalise casing, validate, compute revenue, de-duplicate
  LOAD       delete this shard's old rows (idempotent re-runs), then
             INSERT ... SELECT the clean rows into remote.orders, and
             record a row in remote.load_audit

    python worker.py --worker w1 --shards raw/orders_shard_00.csv raw/orders_shard_01.csv
"""
import argparse
import os
import time

import duckdb

import config


def connect_client() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("LOAD quack;")
    con.execute("CREATE SECRET (TYPE quack, TOKEN ?);", [config.QUACK_TOKEN])
    con.execute(
        f"ATTACH '{config.QUACK_URI}' AS remote "
        f"(DISABLE_SSL {str(config.DISABLE_SSL).lower()});"
    )
    return con


def remote_exec(con, sql):
    """Run a mutating statement SERVER-SIDE.

    Quack beta only supports INSERT through the attached `remote.*` catalog —
    DELETE/UPDATE there raise 'Can only delete from base table'. Routing the
    statement through remote.query() executes it on the server instead.
    """
    con.execute(f"FROM remote.query($q${sql}$q$)").fetchall()


# The transform. Reads the raw shard and emits clean, typed, de-duplicated
# rows. Everything except the LOAD runs client-side in this worker's process.
TRANSFORM_SQL = """
CREATE OR REPLACE TEMP TABLE clean AS
WITH raw AS (
    SELECT * FROM read_csv('{path}', header=true, all_varchar=true,
                           ignore_errors=true)
),
typed AS (
    SELECT
        trim(order_id) AS order_id,
        CAST(coalesce(
            try_strptime(trim(order_date), '%Y-%m-%d'),
            try_strptime(trim(order_date), '%m/%d/%Y'),
            try_strptime(trim(order_date), '%d-%m-%Y')
        ) AS DATE) AS order_date,
        upper(trim(region))   AS region,
        lower(trim(category)) AS category,
        trim(customer)        AS customer,
        try_cast(regexp_replace(trim(quantity), '[^0-9-]', '', 'g') AS INTEGER) AS quantity,
        try_cast(regexp_replace(replace(trim(unit_price), ',', '.'), '[^0-9.]', '', 'g') AS DOUBLE) AS unit_price,
        nullif(upper(trim(currency)), '') AS currency
    FROM raw
),
valid AS (
    SELECT *, quantity * unit_price AS revenue
    FROM typed
    WHERE order_id IS NOT NULL AND order_id <> ''
      AND order_date IS NOT NULL
      AND quantity   IS NOT NULL AND quantity   > 0
      AND unit_price IS NOT NULL AND unit_price > 0
),
deduped AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY order_date) AS rn
        FROM valid
    ) WHERE rn = 1
)
SELECT order_id, order_date, region, category, customer,
       quantity, unit_price, coalesce(currency, 'USD') AS currency, revenue
FROM deduped;
"""


def process_shard(con, worker_id, path):
    shard = os.path.basename(path)
    p = path.replace("\\", "/")          # DuckDB wants forward slashes

    rows_in = con.execute(
        f"SELECT count(*) FROM read_csv('{p}', header=true, all_varchar=true, ignore_errors=true)"
    ).fetchone()[0]

    # Transform into a local temp table, then derive the audit counts.
    con.execute(TRANSFORM_SQL.format(path=p))
    rows_loaded = con.execute("SELECT count(*) FROM clean").fetchone()[0]
    # Count rows that passed validation but before de-dup, to split the buckets.
    rows_valid = con.execute("""
        WITH raw AS (SELECT * FROM read_csv('{p}', header=true, all_varchar=true, ignore_errors=true)),
        typed AS (
            SELECT try_cast(regexp_replace(trim(quantity),'[^0-9-]','','g') AS INTEGER) AS q,
                   try_cast(regexp_replace(replace(trim(unit_price),',','.'),'[^0-9.]','','g') AS DOUBLE) AS up,
                   trim(order_id) AS oid,
                   CAST(coalesce(try_strptime(trim(order_date),'%Y-%m-%d'),
                                 try_strptime(trim(order_date),'%m/%d/%Y'),
                                 try_strptime(trim(order_date),'%d-%m-%Y')) AS DATE) AS od
            FROM raw)
        SELECT count(*) FROM typed
        WHERE oid IS NOT NULL AND oid <> '' AND od IS NOT NULL
          AND q IS NOT NULL AND q > 0 AND up IS NOT NULL AND up > 0
    """.replace("{p}", p)).fetchone()[0]

    rows_rejected = rows_in - rows_valid
    rows_dupe = rows_valid - rows_loaded

    # LOAD — idempotent: clear any prior rows for this shard, then push clean
    # rows + provenance columns into the shared store. The SELECT runs here;
    # the INSERT commits on the server.
    remote_exec(con, f"DELETE FROM orders WHERE source_shard = '{shard}'")
    con.execute(
        "INSERT INTO remote.orders "
        "(order_id, order_date, region, category, customer, quantity, "
        " unit_price, currency, revenue, source_shard, loaded_by, load_ts) "
        "SELECT order_id, order_date, region, category, customer, quantity, "
        "       unit_price, currency, revenue, ?, ?, now() FROM clean",
        [shard, worker_id],
    )

    # Audit (also idempotent).
    remote_exec(con, f"DELETE FROM load_audit WHERE source_shard = '{shard}'")
    con.execute(
        "INSERT INTO remote.load_audit "
        "(loaded_by, source_shard, rows_in, rows_loaded, rows_rejected, rows_dupe, load_ts) "
        "VALUES (?, ?, ?, ?, ?, ?, now())",
        [worker_id, shard, rows_in, rows_loaded, rows_rejected, rows_dupe],
    )

    print(f"[{worker_id}] {shard}: in={rows_in} loaded={rows_loaded} "
          f"rejected={rows_rejected} dupe={rows_dupe}", flush=True)
    return rows_loaded


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--worker", required=True)
    ap.add_argument("--shards", nargs="+", required=True)
    args = ap.parse_args()

    con = connect_client()
    t0 = time.time()
    total = 0
    for path in args.shards:
        try:
            total += process_shard(con, args.worker, path)
        except Exception as e:
            print(f"[{args.worker}] ERROR on {path}: {e!r}", flush=True)
    dt = time.time() - t0
    print(f"[{args.worker}] done -- {total} rows from {len(args.shards)} shard(s) in {dt:.1f}s", flush=True)


if __name__ == "__main__":
    main()
