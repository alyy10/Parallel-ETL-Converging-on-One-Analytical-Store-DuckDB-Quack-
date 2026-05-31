"""Final report over the converged store — proves all shards merged into one.

Every query runs SERVER-SIDE via remote.query(); only the small aggregated
results come back. This is the payoff: N workers loaded in parallel, and we
query the single unified `orders` table as if one process had built it.

    python report.py
"""
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


def q(con, inner_sql):
    cur = con.execute(f"FROM remote.query($q${inner_sql}$q$)")
    cols = [d[0] for d in cur.description]
    return cols, cur.fetchall()


def table(con, title, inner_sql):
    cols, rows = q(con, inner_sql)
    print(f"\n=== {title} ===")
    widths = [max(len(str(c)), *(len(str(r[i])) for r in rows)) if rows else len(str(c))
              for i, c in enumerate(cols)]
    print("  ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols)))
    print("  ".join("-" * widths[i] for i in range(len(cols))))
    for r in rows:
        print("  ".join(str(v).ljust(widths[i]) for i, v in enumerate(r)))


def main() -> None:
    con = connect_client()

    cols, rows = q(con, "SELECT count(*) n, count(DISTINCT source_shard) shards, "
                        "count(DISTINCT loaded_by) workers FROM orders")
    n, shards, workers = rows[0]
    print("=" * 60)
    print(f"CONVERGED STORE: {n:,} clean rows from {shards} shards "
          f"loaded by {workers} workers")
    print("=" * 60)

    table(con, "Revenue by region", """
        SELECT region,
               count(*) AS orders,
               round(sum(revenue), 2) AS revenue,
               round(avg(revenue), 2) AS avg_order
        FROM orders GROUP BY region ORDER BY revenue DESC
    """)

    table(con, "Revenue by category", """
        SELECT category,
               count(*) AS orders,
               round(sum(revenue), 2) AS revenue
        FROM orders GROUP BY category ORDER BY revenue DESC
    """)

    table(con, "Monthly trend", """
        SELECT date_trunc('month', order_date) AS month,
               count(*) AS orders,
               round(sum(revenue), 2) AS revenue
        FROM orders GROUP BY month ORDER BY month
    """)

    table(con, "Top 5 customers", """
        SELECT customer,
               count(*) AS orders,
               round(sum(revenue), 2) AS spend
        FROM orders GROUP BY customer ORDER BY spend DESC LIMIT 5
    """)

    # Data-quality / provenance view straight from the audit table.
    table(con, "Load audit (per shard / per worker)", """
        SELECT loaded_by, source_shard,
               rows_in, rows_loaded, rows_rejected, rows_dupe,
               round(100.0 * rows_loaded / nullif(rows_in, 0), 1) AS pct_kept
        FROM load_audit ORDER BY source_shard
    """)

    table(con, "Data-quality summary", """
        SELECT sum(rows_in) AS total_raw,
               sum(rows_loaded) AS total_clean,
               sum(rows_rejected) AS total_rejected,
               sum(rows_dupe) AS total_dupe,
               round(100.0 * sum(rows_loaded) / nullif(sum(rows_in), 0), 1) AS pct_kept
        FROM load_audit
    """)
    print()


if __name__ == "__main__":
    main()
