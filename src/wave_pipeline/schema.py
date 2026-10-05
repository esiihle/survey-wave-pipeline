"""Canonical schema and schema enforcement.

A survey tracker runs the same study repeatedly ("waves"). Over time, wave
files drift: a column gets renamed, a type arrives as text instead of a number,
an out-of-range value slips in. The pipeline pins a single *canonical* schema
and forces every incoming wave to conform to it — casting types, filling
missing columns, and quarantining rows that violate the data contract instead
of letting them corrupt the output.

``enforce_schema`` returns two frames: the clean rows, and the rejected rows
tagged with a human-readable reason. Nothing is dropped silently.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# The data contract. Every stage downstream can rely on exactly these columns
# and types existing.
CANONICAL_SCHEMA = StructType([
    StructField("respondent_id", StringType(), False),
    StructField("wave", IntegerType(), False),
    StructField("fieldwork_date", StringType(), True),
    StructField("region", StringType(), True),
    StructField("age_group", StringType(), True),
    StructField("weight", DoubleType(), True),
    StructField("nps_score", IntegerType(), True),   # 0..10
    StructField("satisfaction", IntegerType(), True),  # 1..5
    StructField("aware", IntegerType(), True),        # 0/1
    StructField("brand_used", StringType(), True),
])

CANONICAL_COLUMNS = [f.name for f in CANONICAL_SCHEMA.fields]


def _range_rules() -> list[tuple]:
    """Validity rules, as (is-valid condition, reason-if-invalid).

    Built inside a function (not at import) because Spark Column expressions
    require an active Spark session. Null is tolerated for optional columns; the
    required keys are checked separately in :func:`enforce_schema`.
    """
    return [
        ((F.col("nps_score").isNull()) | F.col("nps_score").between(0, 10),
         "nps_score out of range 0-10"),
        ((F.col("satisfaction").isNull()) | F.col("satisfaction").between(1, 5),
         "satisfaction out of range 1-5"),
        ((F.col("aware").isNull()) | F.col("aware").isin(0, 1),
         "aware not in {0,1}"),
        ((F.col("weight").isNull()) | (F.col("weight") > 0),
         "weight not positive"),
    ]


def enforce_schema(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Coerce a raw wave frame to the canonical schema.

    Steps: add any missing canonical columns as null, cast every column to its
    canonical type, then split rows into clean vs rejected. A row is rejected
    if a required key is null or any range rule fails.

    Returns:
        ``(clean, rejects)`` where ``clean`` has exactly the canonical columns
        and ``rejects`` additionally carries a ``reject_reason`` column.
    """
    # 1. Ensure every canonical column exists (missing ones become null).
    for field in CANONICAL_SCHEMA.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))

    # 2. Cast each column to its canonical type. A non-numeric string in a
    #    numeric column becomes null here (Spark's safe cast) and is caught by
    #    the null/range checks below rather than crashing the job.
    for field in CANONICAL_SCHEMA.fields:
        df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))

    df = df.select(*CANONICAL_COLUMNS)

    # 3. Build a reject reason: first failing rule wins.
    reason = F.lit(None).cast(StringType())
    # Required keys must be present.
    reason = F.when(F.col("respondent_id").isNull(), F.lit("missing respondent_id")).otherwise(reason)
    reason = F.when(reason.isNull() & F.col("wave").isNull(), F.lit("missing wave")).otherwise(reason)
    for is_valid, msg in _range_rules():
        reason = F.when(reason.isNull() & (~is_valid), F.lit(msg)).otherwise(reason)

    tagged = df.withColumn("reject_reason", reason)
    clean = tagged.filter(F.col("reject_reason").isNull()).select(*CANONICAL_COLUMNS)
    rejects = tagged.filter(F.col("reject_reason").isNotNull())
    return clean, rejects
