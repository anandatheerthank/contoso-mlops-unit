# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Contoso sales snapshots
# MAGIC Writes two CSV snapshots into the ADLS **raw** zone:
# MAGIC * `snapshot=001` – good quality  (~98.5% valid  → gate PASSES)
# MAGIC * `snapshot=002` – poor quality  (~89.0% valid  → gate FAILS)
# MAGIC
# MAGIC Run this once, at the start of the lab.

# COMMAND ----------

dbutils.widgets.text("storage_account", "stcontosomlopsXX", "ADLS Gen2 account name")
dbutils.widgets.text("ingest_date", "2026-08-20", "Ingest date (yyyy-MM-dd)")

STORAGE = dbutils.widgets.get("storage_account")
INGEST_DATE = dbutils.widgets.get("ingest_date")
RAW_ROOT = f"abfss://raw@{STORAGE}.dfs.core.windows.net/sales/ingest_date={INGEST_DATE}"
print("Raw root:", RAW_ROOT)

# COMMAND ----------

import random
from datetime import datetime, timedelta, timezone

import pandas as pd

COLUMNS = ["transaction_id", "store_id", "transaction_ts", "sku",
           "quantity", "unit_price", "currency"]


def make_snapshot(n_rows, n_blank_store, n_bad_qty, n_duplicate, n_future, seed):
    rng = random.Random(seed)
    base = datetime.now(timezone.utc) - timedelta(days=1)
    rows = []
    for i in range(n_rows):
        rows.append([
            f"TXN-{seed}-{i:05d}",
            f"S{rng.randint(1, 120):03d}",
            (base - timedelta(minutes=rng.randint(0, 1400))).strftime("%Y-%m-%dT%H:%M:%SZ"),
            f"SKU-{rng.randint(1000, 1999)}",
            rng.randint(1, 12),
            round(rng.uniform(20, 4500), 2),
            "INR",
        ])

    # Inject defects into non-overlapping row ranges so counts are exact.
    cursor = 0

    def take(count):
        nonlocal cursor
        chosen = list(range(cursor, cursor + count))
        cursor += count
        return chosen

    for i in take(n_blank_store):
        rows[i][1] = ""                                   # Rule 1
    for i in take(n_bad_qty):
        rows[i][4] = 0 if i % 2 else -rng.randint(1, 5)   # Rule 2
    for i in take(n_future):
        future = datetime.now(timezone.utc) + timedelta(days=rng.randint(2, 30))
        rows[i][2] = future.strftime("%Y-%m-%dT%H:%M:%SZ")  # Rule 4
    dup_source = n_rows - 1
    for i in take(n_duplicate):
        rows[i][0] = rows[dup_source][0]                  # Rule 3
        dup_source -= 1

    return pd.DataFrame(rows, columns=COLUMNS)


good = make_snapshot(1000, n_blank_store=4, n_bad_qty=4, n_duplicate=4, n_future=3, seed=1)
bad = make_snapshot(1000, n_blank_store=40, n_bad_qty=30, n_duplicate=20, n_future=20, seed=2)
print("good rows:", len(good), " bad rows:", len(bad))

# COMMAND ----------

dbutils.fs.put(f"{RAW_ROOT}/snapshot=001/sales_daily.csv", good.to_csv(index=False), True)
dbutils.fs.put(f"{RAW_ROOT}/snapshot=002/sales_daily.csv", bad.to_csv(index=False), True)

display(dbutils.fs.ls(RAW_ROOT))
