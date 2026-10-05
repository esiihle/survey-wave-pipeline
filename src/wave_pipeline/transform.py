"""Transform — respondent-level derived features.

Adds the columns the metrics layer needs, demonstrating window functions and
conditional logic:

* ``weight_norm`` - the survey weight rescaled so it averages 1.0 *within each
  wave*. Using a window partitioned by wave means each wave is normalised
  against its own mean, so waves of different sizes stay comparable. This is a
  textbook use of a partitioned window aggregate.
* ``nps_segment`` - promoter / passive / detractor from the NPS score.
* ``top2box``     - 1 if satisfaction is in the top two box, else 0.

Every feature here is either row-local or computed *within a single wave*, so
processing one wave in isolation gives the same answer as processing all of
them — which is what makes incremental runs safe. Cross-wave calculations
(ranking, wave-over-wave change) live in the aggregate layer instead.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from .config import PipelineConfig


def add_features(df: DataFrame, config: PipelineConfig) -> DataFrame:
    """Attach derived respondent-level columns used by the metrics layer."""
    # Normalise weight within each wave: weight / mean(weight over wave).
    # A null/zero wave-mean would divide badly, so guard it.
    wave_window = Window.partitionBy("wave")
    wave_mean_weight = F.avg("weight").over(wave_window)
    df = df.withColumn(
        "weight_norm",
        F.when(
            (wave_mean_weight.isNull()) | (wave_mean_weight == 0),
            F.lit(1.0),
        ).otherwise(F.col("weight") / wave_mean_weight),
    )

    # NPS segmentation.
    df = df.withColumn(
        "nps_segment",
        F.when(F.col("nps_score") >= config.promoter_min, F.lit("promoter"))
        .when(F.col("nps_score") <= config.detractor_max, F.lit("detractor"))
        .when(F.col("nps_score").isNotNull(), F.lit("passive"))
        .otherwise(F.lit(None).cast("string")),
    )

    # Top-2-box satisfaction as a 0/1 indicator (null stays null = off-base).
    df = df.withColumn(
        "top2box",
        F.when(F.col("satisfaction").isNull(), F.lit(None).cast("int"))
        .when(F.col("satisfaction") >= config.top2box_min, F.lit(1))
        .otherwise(F.lit(0)),
    )

    return df
