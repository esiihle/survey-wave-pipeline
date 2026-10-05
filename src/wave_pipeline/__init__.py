"""wave_pipeline — a wave-agnostic PySpark survey tracker pipeline.

A clean-room showcase of survey data-engineering in Spark: ingest any number of
survey waves, harmonise their quirks to one canonical schema, enforce a data
contract (quarantining bad rows), derive weighted metrics with window
functions, and write an idempotent, partitioned output that supports
incremental runs.

Everything runs locally against synthetic data — no cluster, no cloud, nothing
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
