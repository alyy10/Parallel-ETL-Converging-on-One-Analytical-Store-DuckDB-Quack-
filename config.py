"""Shared configuration for the parallel-ETL-into-one-store demo."""
import os

# --- Quack connection -------------------------------------------------------
QUACK_URI = "quack:localhost"
QUACK_TOKEN = "etl_demo_token"
DISABLE_SSL = True            # localhost demo; put TLS in front in production

# --- Paths ------------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "warehouse.db")   # the converged analytical store
RAW_DIR = os.path.join(HERE, "raw")            # messy input shards live here

# --- Synthetic raw data -----------------------------------------------------
N_SHARDS = 6                  # how many raw files to split the data into
ROWS_PER_SHARD = 5000         # raw rows per shard (before cleaning)
DIRT_RATE = 0.18              # fraction of rows that get some defect injected
DUPE_RATE = 0.04              # fraction that are duplicate order_ids

REGIONS = ["us-east", "us-west", "eu-central", "ap-south"]
CATEGORIES = ["electronics", "apparel", "home", "grocery", "toys", "books"]
