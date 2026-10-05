"""Tests for wave-by-segment metrics and wave-over-wave deltas."""

from __future__ import annotations

from wave_pipeline.aggregate import wave_metrics
from wave_pipeline.config import PipelineConfig
from wave_pipeline.transform import add_features


def _metrics(spark):
    cols = ["respondent_id", "wave", "region", "weight", "nps_score",
            "satisfaction", "aware"]
    data = [
        # Wave 1, North: NPS = (2 prom - 1 detr)/4 = 25.0
        ("R1", 1, "North", 1.0, 10, 5, 1),
        ("R2", 1, "North", 1.0, 9, 4, 1),
        ("R3", 1, "North", 1.0, 8, 2, 0),
        ("R4", 1, "North", 1.0, 3, 1, 1),
        # Wave 2, North: NPS = (3 prom - 1 detr)/4 = 50.0
        ("R5", 2, "North", 1.0, 10, 5, 1),
        ("R6", 2, "North", 1.0, 10, 5, 1),
        ("R7", 2, "North", 1.0, 9, 4, 1),
        ("R8", 2, "North", 1.0, 3, 1, 0),
    ]
    df = add_features(spark.createDataFrame(data, cols), PipelineConfig())
    out = wave_metrics(df, PipelineConfig())
    return {(r["wave"], r["region"]): r for r in out.collect()}


def test_weighted_nps(spark):
    m = _metrics(spark)
    assert m[(1, "North")]["nps"] == 25.0
    assert m[(2, "North")]["nps"] == 50.0


def test_wave_over_wave_delta(spark):
    m = _metrics(spark)
    assert m[(1, "North")]["nps_delta"] is None   # no prior wave
    assert m[(2, "North")]["nps_delta"] == 25.0


def test_top2box_and_awareness(spark):
    m = _metrics(spark)
    assert m[(1, "North")]["top2box_pct"] == 50.0   # 2 of 4
    assert m[(1, "North")]["awareness_pct"] == 75.0  # 3 of 4


def test_rank_present(spark):
    m = _metrics(spark)
    assert m[(1, "North")]["nps_rank_in_wave"] == 1  # single segment
