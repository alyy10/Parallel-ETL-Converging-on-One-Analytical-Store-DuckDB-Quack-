"""Orchestrate the whole parallel-ETL run.

    python run_etl.py                      # 3 workers over the default 6 shards
    python run_etl.py --workers 6          # one worker per shard
    python run_etl.py --regen --shards 8   # regenerate raw data first

Steps: (1) ensure raw shards exist, (2) start the Quack server, (3) fan out
K worker processes that each own a round-robin slice of the shards and load
in parallel, (4) wait for all workers to finish, (5) print the converged
report. The workers genuinely run at the same time, all writing to one store.
"""
import argparse
import glob
import os
import subprocess
import sys
import time

import config

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable


def spawn(script, *script_args, capture=True):
    kw = dict(text=True)
    if capture:
        kw.update(stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1)
    return subprocess.Popen([PY, os.path.join(HERE, script), *map(str, script_args)], **kw)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--shards", type=int, default=config.N_SHARDS)
    ap.add_argument("--rows", type=int, default=config.ROWS_PER_SHARD)
    ap.add_argument("--regen", action="store_true", help="regenerate raw shards")
    ap.add_argument("--fresh", action="store_true", help="wipe warehouse.db first")
    args = ap.parse_args()

    # 1) raw data
    existing = sorted(glob.glob(os.path.join(config.RAW_DIR, "orders_shard_*.csv")))
    if args.regen or not existing:
        print("[etl] generating raw shards...")
        subprocess.run([PY, os.path.join(HERE, "generate_raw.py"),
                        "--shards", str(args.shards), "--rows", str(args.rows)], check=True)
    shard_files = sorted(glob.glob(os.path.join(config.RAW_DIR, "orders_shard_*.csv")))
    print(f"[etl] {len(shard_files)} shards to process")

    if args.fresh:
        for s in ("", ".wal"):
            p = config.DB_PATH + s
            if os.path.exists(p):
                os.remove(p)

    server = None
    workers = []
    try:
        # 2) server
        print("[etl] starting Quack server...")
        server = spawn("server.py")
        for line in server.stdout:
            print("   ", line.rstrip())
            if "SERVER_UP" in line:
                break
        time.sleep(0.5)

        # 3) fan out workers, round-robin shard assignment
        assignment = {f"w{i+1}": [] for i in range(args.workers)}
        for idx, sf in enumerate(shard_files):
            assignment[f"w{(idx % args.workers) + 1}"].append(sf)

        print(f"[etl] launching {args.workers} workers in parallel...")
        t0 = time.time()
        for wid, files in assignment.items():
            if not files:
                continue
            p = spawn("worker.py", "--worker", wid, "--shards", *files)
            workers.append((wid, p))

        # 4) wait for all workers, streaming their output
        for wid, p in workers:
            for line in p.stdout:
                print("   ", line.rstrip())
            p.wait()
        dt = time.time() - t0
        print(f"\n[etl] all workers finished in {dt:.1f}s -- running report...\n")

        # 5) report
        subprocess.run([PY, os.path.join(HERE, "report.py")], check=True)

    finally:
        if server and server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=5)
            except Exception:
                server.kill()
        print("\n[etl] server stopped.")


if __name__ == "__main__":
    main()
