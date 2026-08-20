# Data Contract — Contoso Daily Sales

**Dataset:** `contoso_sales_validated`
**Version:** 1.0
**Owner:** retail-data-team (data-owner@contoso.com)
**Consumers:** Data Science team (Azure ML training pipelines)
**Change process:** any change to this contract requires a pull request approved by the data owner.

---

## 1. Source

| Item | Value |
|---|---|
| Producer | Contoso store POS export (120 stores) |
| Frequency | Daily |
| Format | CSV, UTF-8, header row |
| Landing zone | `abfss://raw@<storage>.dfs.core.windows.net/sales/ingest_date=<yyyy-MM-dd>/snapshot=<nnn>/sales_daily.csv` |
| Snapshot rule | Every load writes to a **new** `snapshot=<nnn>` folder. Existing snapshots are never overwritten. |

## 2. Required schema

Schema compliance must be **100%**. A missing column fails the pipeline immediately.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| transaction_id | string | No | Unique within a snapshot |
| store_id | string | No | Non-blank, e.g. `S042` |
| transaction_ts | timestamp (ISO 8601, UTC) | No | Must not be in the future |
| sku | string | No | e.g. `SKU-1234` |
| quantity | integer | No | > 0 |
| unit_price | decimal(10,2) | No | >= 0 |
| currency | string | No | ISO 4217, e.g. `INR` |

## 3. Data-quality rules

| Rule | Check | Reason code |
|---|---|---|
| Rule 1 | `store_id` must not be blank or whitespace | `BLANK_STORE_ID` |
| Rule 2 | `quantity` must be greater than 0 | `NON_POSITIVE_QTY` |
| Rule 3 | `transaction_id` must be unique (first occurrence kept) | `DUPLICATE_TXN_ID` |
| Rule 4 | `transaction_ts` must not be later than pipeline run time | `FUTURE_TRANSACTION_TS` |

A record may carry more than one reason code; codes are stored pipe-separated.

## 4. Quality gates

| Gate | Threshold | On failure |
|---|---|---|
| Valid-row rate | >= 98% | Pipeline fails, Azure ML registration blocked |
| Schema compliance | = 100% | Pipeline fails, Azure ML registration blocked |

**Business rule: bad data must never reach model training.**

## 5. Zones

| Zone | Container | Contents |
|---|---|---|
| Raw | `raw` | Immutable source snapshots |
| Validated | `validated` | Delta table `sales_validated`, partitioned by `transaction_date` |
| Quarantine | `quarantine` | Rejected records with reason codes, per snapshot |
| Evidence | `evidence` | `quality_report.json` and `run_manifest.json`, per pipeline run |

## 6. Traceability

Every registered Azure ML data asset version carries these tags:

`git_commit`, `azure_devops_run_id`, `source_snapshot`, `validation_rate`, `owner`, `delta_version`, `quarantine_path`

## 7. Service level

| Item | Commitment |
|---|---|
| Freshness | Validated table available by 06:00 IST for the previous day |
| Retention | Raw 365 days, Quarantine 90 days, Evidence 365 days |
| Breaking change notice | 10 working days, via pull request |
