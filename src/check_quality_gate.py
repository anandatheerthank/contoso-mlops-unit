"""Quality gate: valid-row rate >= 98% AND schema compliance = 100%.

Stage 3 of pipelines/data_quality.yml. Exit code 1 stops the pipeline,
which is what blocks Azure ML registration.
"""
from __future__ import annotations

import json
import sys

MIN_VALID_ROW_RATE = 0.98
MIN_SCHEMA_COMPLIANCE = 1.0

report_path = sys.argv[1]
with open(report_path, encoding="utf-8") as fh:
    report = json.load(fh)

rate = float(report["valid_row_rate"])
schema = float(report["schema_compliance"])

print("=" * 56)
print("QUALITY GATE")
print(f"  input records     : {report['input_record_count']}")
print(f"  valid records     : {report['valid_record_count']}")
print(f"  rejected records  : {report['rejected_record_count']}")
print(f"  valid-row rate    : {rate:.2%}   (required >= {MIN_VALID_ROW_RATE:.0%})")
print(f"  schema compliance : {schema:.2%}  (required = {MIN_SCHEMA_COMPLIANCE:.0%})")
print(f"  source snapshot   : {report['source_snapshot']}")
print(f"  git commit        : {report['git_commit']}")
print(f"  pipeline run id   : {report['run_id']}")
print("=" * 56)

failures = []
if rate < MIN_VALID_ROW_RATE:
    failures.append(f"valid-row rate {rate:.2%} is below {MIN_VALID_ROW_RATE:.0%}")
if schema < MIN_SCHEMA_COMPLIANCE:
    failures.append(f"schema compliance {schema:.2%} is below 100%")

if failures:
    for f in failures:
        print(f"##vso[task.logissue type=error]QUALITY GATE FAILED — {f}")
    print("Azure ML registration is BLOCKED. Rejected records remain in quarantine.")
    sys.exit(1)

print("QUALITY GATE PASSED — proceeding to Azure ML registration.")
