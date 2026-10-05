# Overview & design notes

## What this is

A clean-room, synthetic-data reimplementation of a **survey tracker pipeline**
in PySpark. It demonstrates the data-engineering patterns a wave-based tracker
needs — schema enforcement, cross-wave harmonisation, weighted metrics with
window functions, partitioned storage, and incremental/idempotent processing —
without any dependency on a cluster, a cloud lakehouse, or any proprietary data.

## Background: what a "tracker" is

A tracker runs the same survey repeatedly over time. Each run is a **wave**.
Analysts report metrics per wave (NPS, satisfaction top-2-box, awareness) and,
crucially, the **change between waves**. A pipeline that serves a tracker has to
treat waves uniformly ("wave-agnostic") while coping with the fact that the data
for each wave is never quite identical in shape.

## Input contract

Each wave is a CSV of respondent rows. Expected canonical columns:
`respondent_id`, `wave`, `fieldwork_date`, `region`, `age_group`, `weight`,
`nps_score` (0–10), `satisfaction` (1–5), `aware` (0/1), `brand_used`. Waves may
arrive with legacy column names or region codes; harmonisation fixes those.

## Design principles

- **Wave-agnostic by construction.** The pipeline globs a folder and processes
  whatever waves it finds; no code changes per wave.
- **Fail loud, not silent.** Rows that break the schema contract are quarantined
  with a reason, never dropped quietly.
- **Correct weighting.** Every metric is weighted and taken over the base of
  respondents who actually answered it.
- **Idempotent.** Re-running produces identical output; incremental runs touch
  only new partitions.
- **Local-first.** Runs on one machine against synthetic data, so it's
  reproducible by anyone.

## Key decisions and trade-offs

- **Respondents incremental, metrics recomputed.** Respondent partitions are
  written incrementally (cheap, append-like via dynamic overwrite). Metrics are
  recomputed over the full store every run because a wave-over-wave delta needs
  the neighbouring wave — and the metrics table is tiny, so the cost is
  negligible and correctness is guaranteed.
- **Weight normalised within wave.** Each wave's weights are rescaled to mean
  1.0 within that wave, so differing wave sizes don't distort pooled figures.
- **Cross-wave calculations live only in the aggregate layer.** Respondent-level
  features are all row-local or within-wave, which is what makes processing a
  single wave in isolation safe for incremental runs.
- **CSV in, parquet out.** Raw waves are read as text (so a stray value becomes
  a catchable reject rather than a crash); output is columnar parquet.

## Clean-room / confidentiality approach

- Reimplemented from concept against `faker`-style synthetic data — nothing real
  to leak.
- **No notebook.** Notebooks are the main leakage vector when publishing work
  from a lakehouse environment: they carry environment paths, workspace and
  lakehouse identifiers, and saved cell outputs. The logic lives in plain `.py`
  modules operating on synthetic Spark DataFrames instead.
- No cloud paths, connection strings, or environment metadata anywhere.

## Shipped since 0.1.0

See [CHANGELOG.md](../CHANGELOG.md). In brief, 0.2.0 added a **Delta Lake**
output mode (`--format delta`): respondents are upserted by key with an atomic
`MERGE` (finer-grained than partition overwrite), and every run is a new table
version, giving history and time travel.

## Possible extensions

- Great Expectations / `pydeequ` style data-quality assertions alongside the
  built-in schema enforcement.
- Significance testing on wave-over-wave deltas.
- A small dashboard reading the metrics table.
- Config-driven metric definitions beyond NPS / top-2-box.
