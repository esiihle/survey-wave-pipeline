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

from .aggregate import wave_metrics
from .config import PipelineConfig
from .ingest import ingest_waves
from .io import existing_waves, read_parquet, write_partitioned
from .transform import add_features


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
) -> RunSummary:
    """Run the pipeline. See module docstring for the two modes."""
    resp_path, metrics_path, reject_path = _paths(output_dir)

    clean_all, rejects_all = ingest_waves(spark, input_dir, config)
    available = sorted(int(r["wave"]) for r in clean_all.select("wave").distinct().collect())

    done = existing_waves(spark, resp_path) if incremental else []
    to_process = [w for w in available if w not in done]

    if not to_process:
        # Nothing new — a no-op run. Report the current metric count.
        metric_rows = read_parquet(spark, metrics_path).count() if os.path.exists(metrics_path) else 0
        return RunSummary("incremental", [], 0, 0, metric_rows)

    clean_new = clean_all.filter(clean_all.wave.isin(to_process))
    rejects_new = rejects_all.filter(rejects_all.wave.isin(to_process))

    features = add_features(clean_new, config)

    # Respondent store: dynamic overwrite writes only the new wave partitions,
    # so an incremental run leaves existing waves untouched (and a repeat run of
    # the same wave simply replaces its partition — idempotent).
    write_partitioned(features, resp_path, "wave", mode="overwrite")
    if rejects_new.take(1):
        write_partitioned(rejects_new, reject_path, "wave", mode="overwrite")

    # Metrics recomputed over the *full* respondent store so deltas are correct.
    all_respondents = read_parquet(spark, resp_path)
    metrics = wave_metrics(all_respondents, config)
    metrics.write.mode("overwrite").parquet(metrics_path)

    return RunSummary(
        mode="incremental" if incremental else "full",
        waves_processed=to_process,
        clean_rows=clean_new.count(),
        reject_rows=rejects_new.count(),
        metric_rows=metrics.count(),
    )
