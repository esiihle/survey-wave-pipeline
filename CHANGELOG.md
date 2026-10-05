# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [0.1.0]

### Added
- Initial release: a wave-agnostic PySpark survey tracker pipeline.
  - Canonical schema enforcement with row quarantine and reasons (`schema.py`).
  - Config-driven cross-wave harmonisation of column names and region codes
    (`harmonise.py`).
  - Wave-agnostic ingest of any number of wave CSVs (`ingest.py`).
  - Respondent features via window functions — weight normalised within wave,
    NPS segment, top-2-box (`transform.py`).
  - Weighted wave × segment metrics with wave-over-wave `nps_delta` (`lag`) and
    within-wave `nps_rank` (`dense_rank`) (`aggregate.py`).
  - Partitioned, idempotent parquet I/O and incremental runs via dynamic
    partition overwrite (`io.py`, `pipeline.py`).
  - CLI (`wave-pipeline run [--incremental]`), synthetic multi-wave data
    generator, a session-scoped Spark test suite, and CI with Java + Spark.
