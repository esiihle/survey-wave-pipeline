"""Aggregate — wave-by-segment headline metrics, with wave-over-wave change.

Rolls respondent rows up to one row per (wave, segment), computing weighted
tracker metrics, then uses a second window — partitioned by segment, ordered by
wave — to attach the change versus the previous wave (``lag``). That
wave-over-wave delta is the number a tracker actually reports ("NPS up 4 points
since last wave").

All metrics are weighted by ``weight_norm`` (see :mod:`wave_pipeline.transform`)
and take their base over respondents who actually answered that metric, so a
null answer never drags an average toward zero.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from .config import PipelineConfig


def _weighted_share(flag_col: str, base_filter) -> "F.Column":
    """Weighted % of a 0/1 flag over the rows where it is answered."""
    w_num = F.sum(F.when(F.col(flag_col) == 1, F.col("weight_norm")))
    w_base = F.sum(F.when(base_filter, F.col("weight_norm")))
    return F.when(w_base > 0, F.round(w_num / w_base * 100, 1)).otherwise(F.lit(None))


def wave_metrics(df: DataFrame, config: PipelineConfig) -> DataFrame:
    """Compute weighted metrics per wave x segment, plus wave-over-wave deltas.

    Expects the feature columns added by :func:`transform.add_features`.
    """
    group_cols = ["wave"] + list(config.segment_cols)

    # Weighted NPS = (promoter% - detractor%) over the NPS-answering base.
    promoters_w = F.sum(F.when(F.col("nps_segment") == "promoter", F.col("weight_norm")))
    detractors_w = F.sum(F.when(F.col("nps_segment") == "detractor", F.col("weight_norm")))
    nps_base_w = F.sum(F.when(F.col("nps_segment").isNotNull(), F.col("weight_norm")))
    nps = F.when(
        nps_base_w > 0,
        F.round((promoters_w - detractors_w) / nps_base_w * 100, 1),
    ).otherwise(F.lit(None))

    agg = df.groupBy(*group_cols).agg(
        F.count(F.lit(1)).alias("n"),
        F.round(F.sum("weight_norm"), 1).alias("weighted_base"),
        nps.alias("nps"),
        _weighted_share("top2box", F.col("top2box").isNotNull()).alias("top2box_pct"),
        _weighted_share("aware", F.col("aware").isNotNull()).alias("awareness_pct"),
    )

    # Wave-over-wave change in NPS, within each segment.
    change_window = (
        Window.partitionBy(*config.segment_cols).orderBy("wave")
        if config.segment_cols
        else Window.orderBy("wave")
    )
    prev_nps = F.lag("nps").over(change_window)
    agg = agg.withColumn(
        "nps_delta",
        F.when(prev_nps.isNotNull(), F.round(F.col("nps") - prev_nps, 1)),
    )

    # Rank segments by NPS within each wave (1 = best). Showcases dense_rank
    # and answers "which segment leads this wave?".
    if config.segment_cols:
        rank_window = Window.partitionBy("wave").orderBy(F.col("nps").desc_nulls_last())
        agg = agg.withColumn("nps_rank_in_wave", F.dense_rank().over(rank_window))

    return agg.orderBy(*group_cols)
