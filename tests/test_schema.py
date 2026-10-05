"""Tests for schema enforcement."""

from __future__ import annotations

from wave_pipeline.schema import CANONICAL_COLUMNS, enforce_schema


def _raw(spark):
    # Rows as strings (as they'd arrive from CSV). Deliberately omits
    # brand_used / fieldwork_date to test missing-column fill, and includes
    # bad rows for each range rule.
    cols = ["respondent_id", "wave", "region", "age_group", "weight",
            "nps_score", "satisfaction", "aware"]
    data = [
        ("R1", "1", "North", "25-34", "1.0", "9", "5", "1"),   # good
        ("R2", "1", "South", "35-44", "1.2", "99", "4", "1"),  # nps out of range
        ("R3", "1", "East", "18-24", "-1", "5", "3", "0"),     # weight <= 0
        ("R4", "1", "West", "55+", "0.8", "6", "7", "1"),      # satisfaction out of range
        (None, "1", "North", "25-34", "1.0", "8", "4", "0"),   # missing id
    ]
    return spark.createDataFrame(data, cols)


def test_clean_and_reject_counts(spark):
    clean, rejects = enforce_schema(_raw(spark))
    assert clean.count() == 1
    assert rejects.count() == 4


def test_canonical_columns_and_types(spark):
    clean, _ = enforce_schema(_raw(spark))
    assert clean.columns == CANONICAL_COLUMNS
    dtypes = dict(clean.dtypes)
    assert dtypes["wave"] == "int"
    assert dtypes["nps_score"] == "int"
    assert dtypes["weight"] == "double"
    # Missing source columns were filled as null (string) columns.
    row = clean.collect()[0]
    assert row["brand_used"] is None


def test_reject_reasons(spark):
    _, rejects = enforce_schema(_raw(spark))
    reasons = {r["reject_reason"] for r in rejects.collect()}
    assert "nps_score out of range 0-10" in reasons
    assert "weight not positive" in reasons
    assert "satisfaction out of range 1-5" in reasons
    assert "missing respondent_id" in reasons
