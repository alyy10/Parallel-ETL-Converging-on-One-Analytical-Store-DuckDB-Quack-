"""Quack server for the ETL demo — the single converged analytical store.

All worker processes load into the ONE database this server owns. Embedded
DuckDB would force them to take turns on a file lock (or stage somewhere else
and merge later). Quack lets every worker load in parallel; the server
serializes the commits.

    python server.py
"""
import time
import duckdb

import config

# Plain columns, NO defaults / sequences — Quack beta can't ATTACH a remote
# table whose columns carry DEFAULT expressions. We set load_ts via now() in
# the worker's INSERT instead.
SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    order_id      VARCHAR,
    order_date    DATE,
    region        VARCHAR,
    category      VARCHAR,
    customer      VARCHAR,
    quantity      INTEGER,
    unit_price    DOUBLE,
    currency      VARCHAR,
    revenue       DOUBLE,
    source_shard  VARCHAR,    -- which raw file this row came from
    loaded_by     VARCHAR,    -- which worker process loaded it
    load_ts       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS load_audit (
    loaded_by      VARCHAR,
    source_shard   VARCHAR,
    rows_in        INTEGER,   -- raw rows read from the shard
    rows_loaded    INTEGER,   -- clean rows actually written
    rows_rejected  INTEGER,   -- failed validation (bad date/qty/price/missing id)
    rows_dupe      INTEGER,   -- dropped as duplicate order_id
    load_ts        TIMESTAMP
);
"""


def main() -> None:
    con = duckdb.connect(config.DB_PATH)
    con.execute("LOAD quack;")
    con.execute(SCHEMA)
    # Literal CALL — named args don't bind from prepared-statement '?'.
    con.execute(
        f"CALL quack_serve('{config.QUACK_URI}', "
        f"disable_ssl={str(config.DISABLE_SSL).lower()}, "
        f"token='{config.QUACK_TOKEN}');"
    )
    print("SERVER_UP", flush=True)
    print(f"[server] serving {config.QUACK_URI} -> {config.DB_PATH}", flush=True)

    try:
        while True:
            time.sleep(2.0)
            n = con.execute("SELECT count(*) FROM orders").fetchone()[0]
            shards = con.execute(
                "SELECT count(*) FROM load_audit").fetchone()[0]
            print(f"[server] orders={n:>8}  shards_loaded={shards}", flush=True)
    except KeyboardInterrupt:
        print("\n[server] stopping...", flush=True)
        try:
            con.execute(f"CALL quack_stop('{config.QUACK_URI}');")
        except Exception:
            pass


if __name__ == "__main__":
    main()
