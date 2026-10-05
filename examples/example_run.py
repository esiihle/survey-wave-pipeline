"""End-to-end example: generate synthetic waves, run the pipeline, show metrics.

Run from the repo root::

    python examples/example_run.py
"""

from __future__ import annotations

import os
import subprocess
import sys

from wave_pipeline import get_spark, load_config, run

RAW = "data/raw"
OUT = "data/out"


def main() -> None:
    # 1. Generate four synthetic waves (legacy first wave + a few dirty rows).
    subprocess.run(
        [sys.executable, "scripts/generate_synthetic_waves.py",
         "--waves", "4", "--rows", "250", "--seed", "11", "--dirty", "--out", RAW],
        check=True,
    )

    config = load_config("config/pipeline.example.yaml")
    spark = get_spark(app_name="wave-pipeline-example")
    try:
        # 2. Full run.
        summary = run(spark, config, RAW, OUT, incremental=False)
        print(f"\nProcessed waves {summary.waves_processed}: "
              f"{summary.clean_rows} clean, {summary.reject_rows} rejected.\n")

        # 3. Show the headline metrics, newest wave first.
        metrics = spark.read.parquet(os.path.join(OUT, "metrics"))
        (metrics.orderBy("wave", "region")
         .select("wave", "region", "n", "nps", "nps_delta",
                 "top2box_pct", "awareness_pct", "nps_rank_in_wave")
         .show(40, truncate=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
