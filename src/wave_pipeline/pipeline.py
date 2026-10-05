"""Pipeline orchestration — tie the stages together, full or incremental.

Flow: ingest -> harmonise -> enforce schema -> add features -> write respondent
store (partitioned by wave) -> recompute metrics over the full store.

Two modes:

* **full** - process every wave file, overwrite the whole output.
* **incremental** - process only waves not already in the respondent store and
  dynamic-overwrite just those partitions (idempotent: re-running changes
  nothing). Metrics are recomputed over the full store each time, because a
  wave-over-wave delta depends on the neighbouring wave — the metrics table is
  tiny, so this is cheap and always correct.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import SparkSession

from . import io_delta
from .aggregate import wave_metrics
from .config import PipelineConfig
from .ingest import ingest_waves
from .io import existing_waves, read_parquet, write_partitioned
from .transform import add_features

# Merge key for the respondent store (unique per respondent per wave).
RESPONDENT_KEYS = ["wave", "respondent_id"]


@dataclass
class RunSummary:
    """What a run did — handy for logging and for asserting in tests."""

    mode: str
    waves_processed: list[int]
    clean_rows: int
    reject_rows: int
    metric_rows: int


def _paths(output_dir: str) -> tuple[str, str, str]:
    return (
        os.path.join(output_dir, "respondents"),
        os.path.join(output_dir, "metrics"),
        os.path.join(output_dir, "rejects"),
    )


def run(
    spark: SparkSession,
    config: PipelineConfig,
    input_dir: str,
    output_dir: str,
    incremental: bool = False,
    output_format: str = "parquet",
) -> RunSummary:
    """Run the pipeline. See module docstring for the two modes.

    Args:
        output_format: ``"parquet"`` (default) writes a partitioned parquet
            store with dynamic partition overwrite; ``"delta"`` writes a Delta
            table and upserts respondents by key with MERGE (ACID, time travel).
            Both are idempotent; delta is finer-grained. Rejects are always
            written as a small parquet diagnostic side-output.
    """
    if output_format not in ("parquet", "delta"):
        raise ValueError("output_format must be 'parquet' or 'delta'")
    use_delta = output_format == "delta"
    resp_path, metrics_path, reject_path = _paths(output_dir)

    clean_all, rejects_all = ingest_waves(spark, input_dir, config)
    available = sorted(int(r["wave"]) for r in clean_all.select("wave").distinct().collect())

    # Which waves are already stored (depends on the output format).
    if incremental:
        done = (io_delta.existing_waves_delta(spark, resp_path) if use_delta
                else existing_waves(spark, resp_path))
    else:
        done = []
    to_process = [w for w in available if w not in done]

    if not to_process:
        # Nothing new — a no-op run. Report the current metric count.
        reader = io_delta.read_delta if use_delta else read_parquet
        metric_rows = reader(spark, metrics_path).count() if os.path.exists(metrics_path) else 0
        return RunSummary("incremental", [], 0, 0, metric_rows)

    clean_new = clean_all.filter(clean_all.wave.isin(to_process))
    rejects_new = rejects_all.filter(rejects_all.wave.isin(to_process))

    features = add_features(clean_new, config)

    # Respondent store. Both paths are idempotent:
    #  - parquet: dynamic overwrite replaces only the new wave partitions.
    #  - delta:   MERGE upserts by (wave, respondent_id) — updates changed rows,
    #             inserts new ones, atomically, and records a new table version.
    if use_delta:
        io_delta.upsert_delta(spark, features, resp_path, RESPONDENT_KEYS, "wave")
    else:
        write_partitioned(features, resp_path, "wave", mode="overwrite")

    if rejects_new.take(1):
        write_partitioned(rejects_new, reject_path, "wave", mode="overwrite")

    # Metrics recomputed over the *full* respondent store so deltas are correct.
    all_respondents = io_delta.read_delta(spark, resp_path) if use_delta else read_parquet(spark, resp_path)
    metrics = wave_metrics(all_respondents, config)
    if use_delta:
        io_delta.overwrite_delta(metrics, metrics_path)
    else:
        metrics.write.mode("overwrite").parquet(metrics_path)

    return RunSummary(
        mode="incremental" if incremental else "full",
        waves_processed=to_process,
        clean_rows=clean_new.count(),
        reject_rows=rejects_new.count(),
        metric_rows=metrics.count(),
    )
