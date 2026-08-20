# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest & Validate — Contoso daily sales
# MAGIC Raw CSV → data-quality rules → Quarantine (invalid) + Validated Delta (valid) + Evidence (quality report)

# COMMAND ----------

dbutils.widgets.text("storage_account", "stcontosomlopsXX")
dbutils.widgets.text("ingest_date", "2026-08-20")
dbutils.widgets.text("snapshot", "001")
dbutils.widgets.text("run_id", "manual-run")
dbutils.widgets.text("git_commit", "manual")
dbutils.widgets.text("owner", "retail-data-team")

STORAGE = dbutils.widgets.get("storage_account")
INGEST_DATE = dbutils.widgets.get("ingest_date")
SNAPSHOT = dbutils.widgets.get("snapshot")
RUN_ID = dbutils.widgets.get("run_id")
GIT_COMMIT = dbutils.widgets.get("git_commit")
OWNER = dbutils.widgets.get("owner")

SOURCE_SNAPSHOT = f"sales/ingest_date={INGEST_DATE}/snapshot={SNAPSHOT}"
INPUT_PATH = f"abfss://raw@{STORAGE}.dfs.core.windows.net/{SOURCE_SNAPSHOT}/sales_daily.csv"
VALIDATED_PATH = f"abfss://validated@{STORAGE}.dfs.core.windows.net/sales_validated"
QUARANTINE_PATH = f"abfss://quarantine@{STORAGE}.dfs.core.windows.net/{SOURCE_SNAPSHOT}"
EVIDENCE_PATH = f"abfss://evidence@{STORAGE}.dfs.core.windows.net/runs/{RUN_ID}"

for label, value in [("INPUT", INPUT_PATH), ("VALIDATED", VALIDATED_PATH),
                     ("QUARANTINE", QUARANTINE_PATH), ("EVIDENCE", EVIDENCE_PATH)]:
    print(f"{label:<11}{value}")

# COMMAND ----------

# MAGIC %md ## 1. Put the repo's validation module on the Python path
# MAGIC This notebook lives in a Databricks **Git folder**, so `src/validate_sales.py`
# MAGIC from the same commit is two levels up. One rule set, used by tests and by the job.

import os
import sys

repo_root = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
sys.path.append(os.path.join(repo_root, "src"))
print("repo root:", repo_root)

import validate_sales as vs
print("rules loaded:", vs.R_BLANK_STORE, vs.R_BAD_QTY, vs.R_DUPLICATE, vs.R_FUTURE_TS)

# COMMAND ----------

# MAGIC %md ## 2. Read the raw snapshot

raw_sdf = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(INPUT_PATH)
)
print("input rows:", raw_sdf.count())
display(raw_sdf.limit(5))

# COMMAND ----------

# MAGIC %md ## 3. Apply the data-quality rules

import pandas as pd

run_ts = pd.Timestamp.now(tz="UTC")
raw_pdf = raw_sdf.toPandas()

schema_compliance, missing = vs.check_schema(raw_pdf)
if missing:
    raise ValueError(f"SCHEMA_COMPLIANCE_FAILED — missing columns: {missing}")

flagged = vs.add_reason_codes(raw_pdf, run_ts)
valid_pdf, invalid_pdf = vs.split_records(flagged)

print(f"valid={len(valid_pdf)}  invalid={len(invalid_pdf)}  schema_compliance={schema_compliance}")
display(invalid_pdf.head(10))

# COMMAND ----------

# MAGIC %md ## 4. Write invalid records to Quarantine (with reason codes)

if len(invalid_pdf):
    dbutils.fs.put(f"{QUARANTINE_PATH}/rejected_records.csv",
                   invalid_pdf.to_csv(index=False), True)
    print("quarantined:", len(invalid_pdf))
else:
    print("nothing to quarantine")

# COMMAND ----------

# MAGIC %md ## 5. Write valid records to the Validated Delta table, partitioned by transaction date

valid_sdf = spark.createDataFrame(valid_pdf)

(valid_sdf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("transaction_date")
    .save(VALIDATED_PATH))

delta_version = (spark.sql(f"DESCRIBE HISTORY delta.`{VALIDATED_PATH}`")
                 .selectExpr("max(version) as v").collect()[0]["v"])
print("delta version:", delta_version)
display(spark.read.format("delta").load(VALIDATED_PATH).limit(5))

# COMMAND ----------

# MAGIC %md ## 6. Build and publish the quality report (Evidence zone)

import json

report = vs.build_quality_report(
    input_count=len(raw_pdf),
    valid_count=len(valid_pdf),
    schema_compliance=schema_compliance,
    input_path=INPUT_PATH,
    validated_path=VALIDATED_PATH,
    quarantine_path=QUARANTINE_PATH,
    run_id=RUN_ID,
    git_commit=GIT_COMMIT,
    source_snapshot=SOURCE_SNAPSHOT,
    owner=OWNER,
)
report["delta_version"] = int(delta_version)

dbutils.fs.put(f"{EVIDENCE_PATH}/quality_report.json", json.dumps(report, indent=2), True)
print(json.dumps(report, indent=2))

# COMMAND ----------

# MAGIC %md ## 7. Return the report to the caller
# MAGIC The notebook does **not** fail here. The Azure DevOps *Quality Gate* stage
# MAGIC decides pass/fail, so quarantine and evidence always survive a bad run.

dbutils.notebook.exit(json.dumps(report))
