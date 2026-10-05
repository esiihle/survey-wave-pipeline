"""Spark session management.

A single place to build a SparkSession so every entry point (CLI, tests,
examples) gets the same tuned, *local* configuration. Nothing here depends on
a cluster or a cloud workspace — the whole pipeline runs on your laptop against
synthetic data, which is exactly the point: the transformation logic mirrors a
production wave pipeline, minus any environment it was tied to.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from pyspark.sql import SparkSession


def get_spark(
    app_name: str = "survey-wave-pipeline",
    shuffle_partitions: int = 8,
    master: str = "local[*]",
) -> SparkSession:
    """Build (or fetch) a tuned local SparkSession.

    Args:
        app_name: Name shown in the Spark UI / logs.
        shuffle_partitions: Partitions for shuffles. The default 200 is far too
            many for laptop-scale data; a small number keeps jobs snappy.
        master: Spark master URL. ``local[*]`` uses all cores.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Idempotent partition writes: overwriting re-writes only the affected
        # partitions, not the whole table (see io.write_partitioned).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        # Quieter, faster startup for small local jobs.
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@contextmanager
def spark_session(**kwargs) -> Iterator[SparkSession]:
    """Context manager that builds a session and stops it on exit."""
    spark = get_spark(**kwargs)
    try:
        yield spark
    finally:
        spark.stop()
