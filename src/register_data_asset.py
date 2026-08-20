"""Register the validated Delta folder as a versioned Azure ML data asset.

Stage 4 of pipelines/data_quality.yml. Re-checks the quality report before
doing anything: bad data must never reach model training.
"""
from __future__ import annotations

import json
import os
import sys

from azure.ai.ml import MLClient
from azure.ai.ml.constants import AssetTypes
from azure.ai.ml.entities import Data
from azure.identity import DefaultAzureCredential

MIN_VALID_ROW_RATE = 0.98
MIN_SCHEMA_COMPLIANCE = 1.0

report_path = sys.argv[1]
with open(report_path, encoding="utf-8") as fh:
    report = json.load(fh)

# --- Guard: never register data that did not clear the gate ------------------
rate = float(report["valid_row_rate"])
schema = float(report["schema_compliance"])
if rate < MIN_VALID_ROW_RATE or schema < MIN_SCHEMA_COMPLIANCE:
    print("##vso[task.logissue type=error]Quality requirements not met — "
          f"valid_row_rate={rate:.2%}, schema_compliance={schema:.2%}. "
          "No Azure ML data asset will be created.")
    sys.exit(1)

ml_client = MLClient(
    credential=DefaultAzureCredential(),
    subscription_id=os.environ["AZURE_SUBSCRIPTION_ID"],
    resource_group_name=os.environ["AZURE_RESOURCE_GROUP"],
    workspace_name=os.environ["AZURE_ML_WORKSPACE"],
)

asset = Data(
    name="contoso_sales_validated",
    path=report["validated_path"],          # abfss:// path to the Delta folder
    type=AssetTypes.URI_FOLDER,
    description="Validated Contoso daily sales. Passed the Unit 2 data-quality gate.",
    tags={
        "git_commit": str(report["git_commit"]),
        "azure_devops_run_id": str(report["run_id"]),
        "source_snapshot": str(report["source_snapshot"]),
        "validation_rate": f"{rate:.4f}",
        "owner": str(report["owner"]),
        "delta_version": str(report.get("delta_version", "na")),
        "quarantine_path": str(report["quarantine_path"]),
    },
)

created = ml_client.data.create_or_update(asset)
print(f"Registered Azure ML data asset: {created.name} version {created.version}")
print(f"##vso[task.setvariable variable=amlAssetVersion;isOutput=true]{created.version}")
