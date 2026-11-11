"""wave_pipeline: a wave-agnostic PySpark survey tracker pipeline.

A clean-room version of the survey data-engineering I do in Spark. It ingests
any number of survey waves, harmonises their quirks to one canonical schema,
enforces a data contract (bad rows get quarantined rather than dropped),
derives weighted metrics with window functions, and writes an idempotent,
partitioned output that supports incremental runs.

Everything runs locally against synthetic data. No cluster, no cloud, nothing
proprietary.

Public API::

    from wave_pipeline import get_spark, load_config, run

    spark = get_spark()
    config = load_config("config/pipeline.example.yaml")
    summary = run(spark, config, "data/raw", "data/out")
"""

from __future__ import annotations

from .aggregate import wave_metrics
from .config import ConfigError, PipelineConfig, load_config
from .harmonise import harmonise
from .ingest import discover_wave_files, ingest_waves
from .io import existing_waves, read_parquet, write_partitioned
from .io_delta import (
    existing_waves_delta,
    read_delta,
    upsert_delta,
    version_count,
)
from .pipeline import RunSummary, run
from .schema import CANONICAL_COLUMNS, CANONICAL_SCHEMA, enforce_schema
from .spark import get_spark, spark_session
from .transform import add_features

__version__ = "0.2.0"

__all__ = [
    "get_spark",
    "spark_session",
    "load_config",
    "PipelineConfig",
    "ConfigError",
    "harmonise",
    "enforce_schema",
    "CANONICAL_SCHEMA",
    "CANONICAL_COLUMNS",
    "discover_wave_files",
    "ingest_waves",
    "add_features",
    "wave_metrics",
    "write_partitioned",
    "read_parquet",
    "existing_waves",
    "upsert_delta",
    "read_delta",
    "existing_waves_delta",
    "version_count",
    "run",
    "RunSummary",
    "__version__",
]
