"""Harmonisation — making every wave speak the same language.

This is the "wave-agnostic" heart of the pipeline. Different waves of the same
tracker often arrive with cosmetic differences: an early wave calls the metric
``nps`` where later waves call it ``nps_score``; one fieldwork house codes
region as ``N``/``S`` where another spells out ``North``/``South``. The business
meaning is identical — only the surface form differs.

Harmonisation applies config-driven maps so that, after this step, every wave
looks the same and the rest of the pipeline never needs to know which wave a
row came from. Both maps are optional: with empty maps this is a no-op, so a
clean wave passes straight through.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def harmonise(
    df: DataFrame,
    rename_map: dict[str, str] | None = None,
    region_map: dict[str, str] | None = None,
) -> DataFrame:
    """Rename wave-specific columns and recode region values to canonical form.

    Args:
        df: A single raw wave frame.
        rename_map: ``{source_name: canonical_name}``. Only columns present are
            renamed; unknown source names are ignored.
        region_map: ``{raw_region_value: canonical_region_value}`` applied to
            the ``region`` column after any rename.
    """
    rename_map = rename_map or {}
    region_map = region_map or {}

    # 1. Column renames. Guard against a rename that would collide with an
    #    existing canonical column (keep the already-canonical one).
    for src, dst in rename_map.items():
        if src in df.columns and src != dst and dst not in df.columns:
            df = df.withColumnRenamed(src, dst)

    # 2. Trim stray whitespace on key string columns.
    for col in ("region", "age_group", "brand_used"):
        if col in df.columns:
            df = df.withColumn(col, F.trim(F.col(col)))

    # 3. Region value recode via a chained CASE WHEN built from the map.
    if region_map and "region" in df.columns:
        recoded = F.col("region")
        for raw, canon in region_map.items():
            recoded = F.when(F.col("region") == raw, F.lit(canon)).otherwise(recoded)
        df = df.withColumn("region", recoded)

    return df
