import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from validate_sales import (  # noqa: E402
    R_BAD_QTY,
    R_BLANK_STORE,
    R_DUPLICATE,
    R_FUTURE_TS,
    add_reason_codes,
    build_quality_report,
    check_schema,
    split_records,
)

RUN_TS = pd.Timestamp("2026-08-20T00:00:00Z")


def frame(rows):
    return pd.DataFrame(
        rows,
        columns=[
            "transaction_id",
            "store_id",
            "transaction_ts",
            "sku",
            "quantity",
            "unit_price",
            "currency",
        ],
    )


def test_schema_compliance_is_one_when_all_columns_present():
    ratio, missing = check_schema(frame([]))
    assert ratio == 1.0
    assert missing == []


def test_schema_compliance_detects_missing_column():
    df = frame([]).drop(columns=["currency"])
    ratio, missing = check_schema(df)
    assert ratio < 1.0
    assert missing == ["currency"]


def test_clean_row_is_valid():
    df = frame([["T1", "S01", "2026-08-19T10:00:00Z", "SKU-1", 3, 25.0, "INR"]])
    out = add_reason_codes(df, RUN_TS)
    assert out.loc[0, "is_valid"]
    assert out.loc[0, "reason_codes"] == ""


def test_blank_store_id_is_rejected():
    df = frame([["T1", "   ", "2026-08-19T10:00:00Z", "SKU-1", 3, 25.0, "INR"]])
    out = add_reason_codes(df, RUN_TS)
    assert R_BLANK_STORE in out.loc[0, "reason_codes"]


def test_non_positive_quantity_is_rejected():
    df = frame([["T1", "S01", "2026-08-19T10:00:00Z", "SKU-1", 0, 25.0, "INR"]])
    out = add_reason_codes(df, RUN_TS)
    assert R_BAD_QTY in out.loc[0, "reason_codes"]


def test_duplicate_transaction_id_keeps_first_only():
    df = frame(
        [
            ["T1", "S01", "2026-08-19T10:00:00Z", "SKU-1", 3, 25.0, "INR"],
            ["T1", "S02", "2026-08-19T11:00:00Z", "SKU-2", 5, 15.0, "INR"],
        ]
    )
    out = add_reason_codes(df, RUN_TS)
    assert out.loc[0, "reason_codes"] == ""
    assert R_DUPLICATE in out.loc[1, "reason_codes"]


def test_future_timestamp_is_rejected():
    df = frame([["T1", "S01", "2027-01-01T10:00:00Z", "SKU-1", 3, 25.0, "INR"]])
    out = add_reason_codes(df, RUN_TS)
    assert R_FUTURE_TS in out.loc[0, "reason_codes"]


def test_row_can_collect_multiple_reason_codes():
    df = frame([["T1", "", "2027-01-01T10:00:00Z", "SKU-1", -2, 25.0, "INR"]])
    out = add_reason_codes(df, RUN_TS)
    codes = out.loc[0, "reason_codes"].split("|")
    assert set(codes) == {R_BLANK_STORE, R_BAD_QTY, R_FUTURE_TS}


def test_split_records_separates_valid_and_invalid():
    df = frame(
        [
            ["T1", "S01", "2026-08-19T10:00:00Z", "SKU-1", 3, 25.0, "INR"],
            ["T2", "", "2026-08-19T10:00:00Z", "SKU-2", 3, 25.0, "INR"],
        ]
    )
    valid, invalid = split_records(add_reason_codes(df, RUN_TS))
    assert len(valid) == 1 and len(invalid) == 1
    assert "reason_codes" in invalid.columns


@pytest.mark.parametrize(
    "valid_count,expected",
    [(980, True), (985, True), (979, False), (900, False)],
)
def test_quality_gate_threshold(valid_count, expected):
    report = build_quality_report(
        input_count=1000,
        valid_count=valid_count,
        schema_compliance=1.0,
        input_path="raw",
        validated_path="validated",
        quarantine_path="quarantine",
        run_id="test",
        git_commit="abc123",
        source_snapshot="snapshot=001",
        owner="retail-data-team",
    )
    assert report["quality_gate_passed"] is expected
