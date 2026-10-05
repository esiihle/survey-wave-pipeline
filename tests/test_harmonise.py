"""Tests for wave harmonisation."""

from __future__ import annotations

from wave_pipeline.harmonise import harmonise


def test_rename_and_region_recode(spark):
    df = spark.createDataFrame(
        [("R1", "1", "N", "9"), ("R2", "1", "S", "3")],
        ["respondent_id", "wave", "region", "nps"],
    )
    out = harmonise(df, rename_map={"nps": "nps_score"},
                    region_map={"N": "North", "S": "South"})
    assert "nps_score" in out.columns
    assert "nps" not in out.columns
    regions = sorted(r["region"] for r in out.collect())
    assert regions == ["North", "South"]


def test_noop_on_clean_wave(spark):
    df = spark.createDataFrame([("R1", "North")], ["respondent_id", "region"])
    out = harmonise(df, rename_map={}, region_map={})
    assert out.columns == ["respondent_id", "region"]
    assert out.collect()[0]["region"] == "North"


def test_trims_whitespace(spark):
    df = spark.createDataFrame([("R1", "  North  ")], ["respondent_id", "region"])
    out = harmonise(df)
    assert out.collect()[0]["region"] == "North"
