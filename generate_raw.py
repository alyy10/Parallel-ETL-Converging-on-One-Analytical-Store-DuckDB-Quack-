"""Generate N messy raw CSV shards — the 'extract' side's input.

Real source data is never clean. Each shard gets a controlled dose of the
defects you actually meet in the wild, so the workers' transform step has
something real to do and the final report can show how much was rejected:

  * order_date in three different formats, plus some blanks
  * region / category with random casing and stray whitespace
  * unit_price with currency symbols / text ("$12.50", "USD 12.5", "12,50")
  * quantity that is negative, zero, blank, or non-numeric
  * duplicate order_ids (within and across shards)
  * some rows missing a critical field entirely (dropped on load)

    python generate_raw.py            # uses config defaults
    python generate_raw.py --shards 8 --rows 10000
"""
import argparse
import csv
import os
import random

import config

HEADER = ["order_id", "order_date", "region", "category",
          "customer", "quantity", "unit_price", "currency"]


def messy_case(s: str, rng: random.Random) -> str:
    """Randomly mangle casing + whitespace the way dirty exports do."""
    choice = rng.random()
    if choice < 0.25:
        s = s.upper()
    elif choice < 0.45:
        s = s.title()
    if rng.random() < 0.3:
        s = "  " + s + " "          # stray whitespace
    return s


def messy_date(y, m, d, rng: random.Random) -> str:
    # Three valid formats dominate; a small slice is blank (-> rejected).
    fmt = rng.choices(["iso", "us", "dash", "blank"], weights=[46, 25, 22, 7])[0]
    if fmt == "iso":
        return f"{y:04d}-{m:02d}-{d:02d}"
    if fmt == "us":
        return f"{m:02d}/{d:02d}/{y:04d}"
    if fmt == "dash":
        return f"{d:02d}-{m:02d}-{y:04d}"
    return ""                        # blank -> should be rejected


def messy_price(p: float, rng: random.Random) -> str:
    choice = rng.random()
    if choice < 0.5:
        return f"{p:.2f}"
    if choice < 0.7:
        return f"${p:.2f}"
    if choice < 0.85:
        return f"USD {p:.2f}"
    return f"{p:.2f}".replace(".", ",")   # european decimal comma -> messy


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", type=int, default=config.N_SHARDS)
    ap.add_argument("--rows", type=int, default=config.ROWS_PER_SHARD)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    os.makedirs(config.RAW_DIR, exist_ok=True)
    rng = random.Random(args.seed)
    oid = 1000000

    for s in range(args.shards):
        path = os.path.join(config.RAW_DIR, f"orders_shard_{s:02d}.csv")
        recent_ids = []                      # pool to draw duplicates from
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(HEADER)
            for _ in range(args.rows):
                dirty = rng.random() < config.DIRT_RATE
                dupe = recent_ids and rng.random() < config.DUPE_RATE

                if dupe:
                    order_id = rng.choice(recent_ids)
                else:
                    oid += 1
                    order_id = f"ORD-{oid}"
                    recent_ids.append(order_id)
                    if len(recent_ids) > 50:
                        recent_ids.pop(0)

                y, m, d = 2026, rng.randint(1, 5), rng.randint(1, 28)
                region = rng.choice(config.REGIONS)
                category = rng.choice(config.CATEGORIES)
                customer = f"CUST-{rng.randint(1, 800):04d}"
                qty = rng.randint(1, 12)
                price = round(rng.uniform(4.0, 400.0), 2)
                currency = "USD"

                # baseline clean-ish row
                row = [order_id, messy_date(y, m, d, rng),
                       region, category, customer, str(qty),
                       messy_price(price, rng), currency]

                if dirty:
                    defect = rng.choice(
                        ["case", "badqty", "badprice", "blankcur", "missing"])
                    if defect == "case":
                        row[2] = messy_case(region, rng)
                        row[3] = messy_case(category, rng)
                    elif defect == "badqty":
                        row[5] = rng.choice(["-3", "0", "", "N/A"])
                    elif defect == "badprice":
                        row[6] = rng.choice(["", "free", "0.00"])
                    elif defect == "blankcur":
                        row[7] = ""
                    elif defect == "missing":
                        row[0] = ""          # no order_id -> must be dropped
                w.writerow(row)
        print(f"[gen] wrote {path}  ({args.rows} rows)")

    print(f"[gen] done -- {args.shards} shards in {config.RAW_DIR}")


if __name__ == "__main__":
    main()
