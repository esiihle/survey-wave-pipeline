"""Command-line interface.

    # Full run over every wave file in data/raw
    wave-pipeline run -i data/raw -o data/out -c config/pipeline.example.yaml

    # Later, after new wave files land: process only the new waves
    wave-pipeline run -i data/raw -o data/out -c config/pipeline.example.yaml --incremental

    # Write a Delta Lake table instead (respondents upserted by MERGE)
    wave-pipeline run -i data/raw -o data/out -c config/pipeline.example.yaml --format delta
"""

from __future__ import annotations

import argparse
import logging
import sys

from .config import load_config
from .pipeline import run
from .spark import spark_session

log = logging.getLogger("wave_pipeline")


def _configure_logging(verbose: bool, quiet: bool) -> None:
    level = logging.WARNING if quiet else (logging.DEBUG if verbose else logging.INFO)
    logging.basicConfig(level=level, format="%(message)s", stream=sys.stderr)


def _cmd_run(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    use_delta = args.format == "delta"
    with spark_session(app_name="wave-pipeline", delta=use_delta) as spark:
        summary = run(spark, config, args.input, args.output,
                      incremental=args.incremental, output_format=args.format)
    if not summary.waves_processed:
        log.info("Nothing to do — no new waves to process.")
    else:
        log.info(f"[{summary.mode}] processed waves {summary.waves_processed}")
        log.info(f"  clean rows:   {summary.clean_rows}")
        log.info(f"  rejected:     {summary.reject_rows}")
        log.info(f"  metric rows:  {summary.metric_rows}")
        log.info(f"  output:       {args.output}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wave-pipeline",
        description="Wave-agnostic PySpark survey tracker pipeline.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("run", help="run the pipeline (full, or --incremental)")
    p.add_argument("-i", "--input", required=True, help="folder of raw wave CSVs")
    p.add_argument("-o", "--output", required=True, help="output folder")
    p.add_argument("-c", "--config", required=True, help="pipeline config YAML")
    p.add_argument("--incremental", action="store_true",
                   help="process only waves not already in the output")
    p.add_argument("--format", choices=["parquet", "delta"], default="parquet",
                   help="output format: parquet (default) or delta (MERGE upserts)")
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    p.add_argument("-q", "--quiet", action="store_true", help="warnings only")
    p.set_defaults(func=_cmd_run)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _configure_logging(getattr(args, "verbose", False), getattr(args, "quiet", False))
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
