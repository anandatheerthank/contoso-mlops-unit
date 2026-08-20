"""Data-quality validation for Contoso daily sales snapshots.

Pure pandas so the same logic runs in unit tests (Azure DevOps agent)
and inside the Databricks notebook. No Spark import here on purpose.
"""
from __future__ import annotations

import pandas as pd

# --- Data contract -----------------------------------------------------------
REQUIRED_COLUMNS = [
    "transaction_id",
    "store_id",
    "transaction_ts",
    "sku",
    "quantity",
    "unit_price",
    "currency",
]

MIN_VALID_ROW_RATE = 0.98
MIN_SCHEMA_COMPLIANCE = 1.0

# --- Reason codes ------------------------------------------------------------
R_BLANK_STORE = "BLANK_STORE_ID"        # Rule 1
R_BAD_QTY = "NON_POSITIVE_QTY"          # Rule 2
R_DUPLICATE = "DUPLICATE_TXN_ID"        # Rule 3
R_FUTURE_TS = "FUTURE_TRANSACTION_TS"   # Rule 4


def check_schema(df: pd.DataFrame):
    """Return (compliance_ratio, missing_columns)."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    present = len(REQUIRED_COLUMNS) - len(missing)
    return present / len(REQUIRED_COLUMNS), missing


def add_reason_codes(df: pd.DataFrame, run_ts: pd.Timestamp) -> pd.DataFrame:
    """Add reason_codes (pipe-separated) and is_valid columns.

    A row can collect more than one reason code.
    """
    out = df.copy()

    ts = pd.to_datetime(out["transaction_ts"], errors="coerce", utc=True)
    qty = pd.to_numeric(out["quantity"], errors="coerce")
    store = out["store_id"].astype("string").str.strip()
    txn = out["transaction_id"].astype("string").str.strip()

    checks = {
        R_BLANK_STORE: store.isna() | (store == ""),
        R_BAD_QTY: qty.isna() | (qty <= 0),
        R_DUPLICATE: txn.duplicated(keep="first"),
        R_FUTURE_TS: ts.isna() | (ts > run_ts),
    }

    codes = [[] for _ in range(len(out))]
    for code, mask in checks.items():
        for position, hit in enumerate(mask.to_numpy()):
            if hit:
                codes[position].append(code)

    out["reason_codes"] = ["|".join(c) for c in codes]
    out["is_valid"] = out["reason_codes"] == ""
    out["transaction_date"] = ts.dt.date.astype("string")
    return out


def split_records(flagged: pd.DataFrame):
    """Return (valid_df, invalid_df)."""
    valid = flagged[flagged["is_valid"]].drop(columns=["is_valid", "reason_codes"])
    invalid = flagged[~flagged["is_valid"]].drop(columns=["is_valid"])
    return valid.reset_index(drop=True), invalid.reset_index(drop=True)


def build_quality_report(
    input_count: int,
    valid_count: int,
    schema_compliance: float,
    input_path: str,
    validated_path: str,
    quarantine_path: str,
    run_id: str,
    git_commit: str,
    source_snapshot: str,
    owner: str,
) -> dict:
    rejected = input_count - valid_count
    rate = (valid_count / input_count) if input_count else 0.0
    passed = rate >= MIN_VALID_ROW_RATE and schema_compliance >= MIN_SCHEMA_COMPLIANCE
    return {
        "input_record_count": input_count,
        "valid_record_count": valid_count,
        "rejected_record_count": rejected,
        "valid_row_rate": round(rate, 4),
        "schema_compliance": round(schema_compliance, 4),
        "min_valid_row_rate": MIN_VALID_ROW_RATE,
        "input_path": input_path,
        "validated_path": validated_path,
        "quarantine_path": quarantine_path,
        "run_id": run_id,
        "git_commit": git_commit,
        "source_snapshot": source_snapshot,
        "owner": owner,
        "quality_gate_passed": bool(passed),
        "generated_utc": pd.Timestamp.now(tz="UTC").isoformat(),
    }


def validate(df: pd.DataFrame, run_ts: pd.Timestamp, **report_kwargs):
    """One-call convenience wrapper: returns (valid, invalid, report)."""
    schema_compliance, missing = check_schema(df)
    if missing:
        raise ValueError(f"Schema compliance failed. Missing columns: {missing}")
    flagged = add_reason_codes(df, run_ts)
    valid, invalid = split_records(flagged)
    report = build_quality_report(
        input_count=len(df),
        valid_count=len(valid),
        schema_compliance=schema_compliance,
        **report_kwargs,
    )
    return valid, invalid, report
