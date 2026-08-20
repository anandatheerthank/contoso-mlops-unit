"""Submit the Databricks ingest_validate notebook and save its quality report.

Called by Stage 2 of pipelines/data_quality.yml.
Secrets arrive as environment variables that Azure DevOps maps from a
Key Vault-linked variable group. Nothing is printed that could leak them.
"""
from __future__ import annotations

import json
import os
import sys
import time

import requests

HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
TOKEN = os.environ["DATABRICKS_TOKEN"]
CLUSTER_ID = os.environ["DATABRICKS_CLUSTER_ID"]
NOTEBOOK_PATH = os.environ["DATABRICKS_NOTEBOOK_PATH"]

PARAMS = {
    "storage_account": os.environ["STORAGE_ACCOUNT"],
    "ingest_date": os.environ["INGEST_DATE"],
    "snapshot": os.environ["SNAPSHOT"],
    "run_id": os.environ["BUILD_BUILDID"],
    "git_commit": os.environ["BUILD_SOURCEVERSION"],
    "owner": os.environ.get("DATA_OWNER", "retail-data-team"),
}

HEADERS = {"Authorization": f"Bearer {TOKEN}"}
OUT_FILE = sys.argv[1] if len(sys.argv) > 1 else "quality_report.json"


def submit() -> int:
    payload = {
        "run_name": f"ingest-validate-{PARAMS['run_id']}",
        "existing_cluster_id": CLUSTER_ID,
        "notebook_task": {"notebook_path": NOTEBOOK_PATH, "base_parameters": PARAMS},
    }
    r = requests.post(f"{HOST}/api/2.1/jobs/runs/submit", headers=HEADERS,
                      json=payload, timeout=60)
    r.raise_for_status()
    return r.json()["run_id"]


def wait(run_id: int, timeout_s: int = 1800) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = requests.get(f"{HOST}/api/2.1/jobs/runs/get", headers=HEADERS,
                         params={"run_id": run_id}, timeout=60)
        r.raise_for_status()
        state = r.json()["state"]
        if state.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return state
        print("  state:", state.get("life_cycle_state"))
        time.sleep(20)
    raise TimeoutError("Databricks run did not finish in time")


def fetch_output(run_id: int) -> str:
    r = requests.get(f"{HOST}/api/2.1/jobs/runs/get-output", headers=HEADERS,
                     params={"run_id": run_id}, timeout=60)
    r.raise_for_status()
    return r.json().get("notebook_output", {}).get("result", "")


if __name__ == "__main__":
    rid = submit()
    print(f"Databricks run submitted: {HOST}/#job/run/{rid}")
    state = wait(rid)
    print("result state:", state.get("result_state"))
    if state.get("result_state") != "SUCCESS":
        print("state message:", state.get("state_message"))
        sys.exit(1)

    report = json.loads(fetch_output(rid))
    with open(OUT_FILE, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    print(json.dumps(report, indent=2))
