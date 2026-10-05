"""I/O — partitioned, idempotent parquet reads and writes.

Respondent output is partitioned by ``wave`` on disk (``.../wave=3/...``). Two
payoffs, both things a production pipeline needs:

* **Pruning** - a query for one wave reads only that wave's folder.
* **Idempotency** - re-running a wave overwrites *only* that wave's partition
  (dynamic partition overwrite), so reprocessing never duplicates rows and
  never disturbs other waves. Running the pipeline twice yields the same table.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, functions as F


def write_partitioned(
    df: DataFrame,
    path: str,
    partition_col: str = "wave",
    mode: str = "overwrite",
) -> None:
    """Write parquet partitioned by ``partition_col``.

    With the session's ``partitionOverwriteMode=dynamic`` (set in
    :mod:`wave_pipeline.spark`), ``mode="overwrite"`` replaces only the
    partitions present in ``df`` — the basis for idempotent incremental runs.
    """
    (df.write.mode(mode).partitionBy(partition_col).parquet(path))


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read a parquet dataset; the partition column is recovered automatically."""
    return spark.read.parquet(path)


def existing_waves(spark: SparkSession, path: str) -> list[int]:
    """Return the wave numbers already present in a partitioned output.

    Reads just the partition values (cheap), returning ``[]`` if the dataset
    doesn't exist yet — which is how an incremental run knows what's new.
    """
    if not os.path.exists(path):
        return []
    try:
        df = spark.read.parquet(path)
    except Exception:  # pragma: no cover - empty/not-yet-written dataset
        return []
    if "wave" not in df.columns:
        return []
    rows = df.select("wave").distinct().collect()
    return sorted(int(r["wave"]) for r in rows)
