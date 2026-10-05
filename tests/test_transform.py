"""Tests for respondent-level feature transforms."""

from __future__ import annotations

from wave_pipeline.config import PipelineConfig
from wave_pipeline.transform import add_features


def _df(spark):
    # wave 1 weights average 2.0; wave 2 weights average 1.0.
    cols = ["respondent_id", "wave", "region", "weight", "nps_score",
            "satisfaction", "aware"]
    data = [
        ("R1", 1, "North", 1.0, 10, 5, 1),
        ("R2", 1, "North", 3.0, 6, 2, 0),
        ("R3", 2, "South", 1.0, 8, 4, 1),
    ]
    return spark.createDataFrame(data, cols)


def test_weight_normalised_within_wave(spark):
    out = add_features(_df(spark), PipelineConfig())
    rows = {r["respondent_id"]: r for r in out.collect()}
    # Wave 1 mean weight = 2.0 -> norms are 0.5 and 1.5, averaging 1.0.
    assert abs(rows["R1"]["weight_norm"] - 0.5) < 1e-9
    assert abs(rows["R2"]["weight_norm"] - 1.5) < 1e-9
    # Wave 2 single respondent -> norm 1.0.
    assert abs(rows["R3"]["weight_norm"] - 1.0) < 1e-9


def test_nps_segment_and_top2box(spark):
    out = add_features(_df(spark), PipelineConfig())
    rows = {r["respondent_id"]: r for r in out.collect()}
    assert rows["R1"]["nps_segment"] == "promoter"   # 10
    assert rows["R2"]["nps_segment"] == "detractor"  # 6
    assert rows["R3"]["nps_segment"] == "passive"    # 8
    assert rows["R1"]["top2box"] == 1   # satisfaction 5
    assert rows["R2"]["top2box"] == 0   # satisfaction 2
