"""Ingest — read any number of wave files into one clean, canonical frame.

"Wave-agnostic" in practice: the pipeline points at a directory and processes
*every* wave file it finds, however many there are, with no per-wave code. Each
file is harmonised (:mod:`wave_pipeline.harmonise`) and forced to the canonical
schema (:mod:`wave_pipeline.schema`); the clean rows are unioned and the
rejected rows are collected separately for a data-quality report.
"""

from __future__ import annotations

import glob
import os

from pyspark.sql import DataFrame, SparkSession, functions as F

from .config import PipelineConfig
from .harmonise import harmonise
from .schema import CANONICAL_COLUMNS, enforce_schema


def discover_wave_files(input_dir: str) -> list[str]:
    """Return sorted wave files in a directory (.csv). Sorted for determinism."""
    patterns = [os.path.join(input_dir, "*.csv")]
    files: list[str] = []
    for pat in patterns:
        files.extend(glob.glob(pat))
    return sorted(files)


def read_wave_file(spark: SparkSession, path: str) -> DataFrame:
    """Read one raw wave CSV as all-string columns (we cast during enforcement)."""
    # inferSchema is off on purpose: we own the schema, and reading as text
    # means a stray non-numeric value becomes a catchable reject, not a crash.
    df = spark.read.option("header", True).csv(path)
    return df.withColumn("source_file", F.lit(os.path.basename(path)))


def ingest_waves(
    spark: SparkSession,
    input_dir: str,
    config: PipelineConfig,
    only_waves: list[int] | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Read, harmonise and schema-enforce every wave file in ``input_dir``.

    Args:
        spark: Active session.
        input_dir: Folder of wave CSVs.
        config: Harmonisation maps etc.
        only_waves: If given, keep only these wave numbers (used by incremental
            runs to process just the new waves).

    Returns:
        ``(clean, rejects)`` unioned across all processed files.
    """
    files = discover_wave_files(input_dir)
    if not files:
        raise FileNotFoundError(f"no wave files (*.csv) found in {input_dir!r}")

    clean_parts: list[DataFrame] = []
    reject_parts: list[DataFrame] = []
    for path in files:
        raw = read_wave_file(spark, path)
        harmonised = harmonise(raw, config.rename_map, config.region_map)
        clean, rejects = enforce_schema(harmonised)
        clean_parts.append(clean)
        reject_parts.append(rejects)

    clean = clean_parts[0]
    for part in clean_parts[1:]:
        clean = clean.unionByName(part)
    rejects = reject_parts[0]
    for part in reject_parts[1:]:
        rejects = rejects.unionByName(part)

    if only_waves is not None:
        clean = clean.filter(F.col("wave").isin(only_waves))
        rejects = rejects.filter(F.col("wave").isin(only_waves))

    return clean.select(*CANONICAL_COLUMNS), rejects
