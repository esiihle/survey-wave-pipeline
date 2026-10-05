"""Delta Lake I/O — ACID upserts via MERGE, with time travel.

The parquet path (:mod:`wave_pipeline.io`) gets idempotency from *dynamic
partition overwrite* — it replaces a whole wave partition. Delta goes finer and
safer: it upserts **by key** with a single atomic ``MERGE``, so reprocessing a
wave updates the rows that changed and inserts the new ones, without rewriting
untouched rows and without any window where the table is half-written. Every
run is a new table *version*, so you also get history / time travel for free.

Merge key for the respondent store: ``(wave, respondent_id)`` — unique per
respondent per wave.

Delta needs its JARs (pulled from Maven on first use) and a Delta-enabled
session (``get_spark(delta=True)``). Everything here imports ``delta`` lazily so
the package is only required when the delta format is actually used.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession


def is_delta_table(spark: SparkSession, path: str) -> bool:
    """True if ``path`` is an existing Delta table."""
    from delta.tables import DeltaTable
    return DeltaTable.isDeltaTable(spark, path)


def upsert_delta(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: list[str],
    partition_col: str | None = "wave",
) -> None:
    """Upsert ``df`` into a Delta table by ``keys`` using MERGE.

    Creates the table on first write (partitioned by ``partition_col``);
    thereafter runs an atomic MERGE: matched rows are updated, new rows are
    inserted. Re-running with identical data is a no-op in effect — idempotent.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        writer = df.write.format("delta").mode("overwrite")
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.save(path)
        return

    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def overwrite_delta(df: DataFrame, path: str) -> None:
    """Replace a Delta table wholesale (used for the small metrics table)."""
    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(path)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Read the current snapshot of a Delta table."""
    return spark.read.format("delta").load(path)


def existing_waves_delta(spark: SparkSession, path: str) -> list[int]:
    """Wave numbers already in a Delta respondent store (``[]`` if none yet)."""
    from delta.tables import DeltaTable

    if not os.path.exists(path) or not DeltaTable.isDeltaTable(spark, path):
        return []
    rows = spark.read.format("delta").load(path).select("wave").distinct().collect()
    return sorted(int(r["wave"]) for r in rows)


def version_count(spark: SparkSession, path: str) -> int:
    """Number of committed versions (table history length) — proof of time travel."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        return 0
    return DeltaTable.forPath(spark, path).history().count()
