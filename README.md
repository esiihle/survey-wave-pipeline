# survey-wave-pipeline

A **wave-agnostic PySpark pipeline** for survey tracker data;  ingest any number
of survey waves, harmonise their quirks to one canonical schema, enforce a data
contract, derive weighted metrics with window functions, and write an
idempotent, partitioned output that supports incremental runs.

Everything runs **locally on synthetic data**; no cluster, no cloud workspace,
nothing proprietary. It mirrors the shape of a production tracker pipeline
(the kind you'd run on Spark in a lakehouse) with the environment stripped out,
so anyone can clone it and run the whole thing in a couple of minutes.

[![CI](https://github.com/esiihle/survey-wave-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/esiihle/survey-wave-pipeline/actions/workflows/ci.yml)
![python](https://img.shields.io/badge/python-3.10+-blue) ![pyspark](https://img.shields.io/badge/PySpark-3.5+-orange) ![license](https://img.shields.io/badge/license-MIT-green) ![tests](https://img.shields.io/badge/tests-pytest-blue)

## What problem it solves

A tracker is the *same* survey run repeatedly over time ("waves"). The recurring
data-engineering headaches are always the same:

- **Waves drift.** An early wave calls a column `nps`; later ones call it
  `nps_score`. One fieldwork house codes region as `N`; another writes `North`.
- **Bad data creeps in.** An out-of-range score, a negative weight, a missing id.
- **New waves keep arriving.** You want to process only the new ones, and a
  re-run must never duplicate or corrupt what's already there.
- **The headline is change over time.** "NPS up 4 points since last wave."

This pipeline handles all four, the same way for 2 waves or 50.

## Skills on display

| Area | Where |
| --- | --- |
| **Schema enforcement / data contracts** | `schema.py` — cast to a canonical `StructType`, quarantine bad rows with reasons |
| **Harmonisation across sources** | `harmonise.py` — config-driven column renames + value recodes |
| **Window functions** | `transform.py` — weight normalised *within wave*; `aggregate.py` — `lag` for wave-over-wave delta, `dense_rank` for segment ranking |
| **Weighted aggregation** | `aggregate.py` — weighted NPS, top-2-box, awareness over the answering base |
| **Partitioning** | `io.py` — parquet partitioned by `wave` for pruning |
| **Incremental & idempotent processing** | `pipeline.py` + `io.py` — dynamic partition overwrite; re-runs change nothing |
| **Delta Lake (ACID / MERGE / time travel)** | `io_delta.py` — `--format delta` upserts respondents by key with `MERGE`; every run is a new, queryable table version |
| **Testing Spark code** | `tests/` — session-scoped fixture, unit + end-to-end tests (incl. Delta) |

## Quickstart

```bash
# 1. Install (PySpark needs a JVM — Java 17+ — on your machine)
pip install -e ".[dev]"

# 2. Generate four synthetic waves
#    (wave 1 uses legacy names/codes; --dirty injects a few bad rows)
python scripts/generate_synthetic_waves.py --waves 4 --rows 300 --seed 42 --dirty --out data/raw

# 3. Run the pipeline
wave-pipeline run -i data/raw -o data/out -c config/pipeline.example.yaml
```

Output:

```
[full] processed waves [1, 2, 3, 4]
  clean rows:   1188
  rejected:     12
  metric rows:  16
  output:       data/out
```

Later, when new wave files land, process only the new ones:

```bash
wave-pipeline run -i data/raw -o data/out -c config/pipeline.example.yaml --incremental
```

Or run the whole thing (generate → run → print metrics) in one go:

```bash
python examples/example_run.py
```

### Delta Lake output (ACID upserts + time travel)

The default output is partitioned parquet. Pass `--format delta` to write a
**Delta Lake** table instead:

```bash
pip install -e ".[delta]"   # delta-spark (pulls the Delta JARs from Maven on first run)
wave-pipeline run -i data/raw -o data/out -c config/pipeline.example.yaml --format delta
```

With Delta, respondents are **upserted by key (`wave`, `respondent_id`) in a
single atomic `MERGE`** rather than by replacing a partition: reprocessing a
wave updates the rows that changed and inserts new ones, with no half-written
state. Every run commits a new table **version**, so you get history and time
travel:

```python
from delta.tables import DeltaTable
spark = get_spark(delta=True)
DeltaTable.forPath(spark, "data/out/respondents").history().show()   # all versions
spark.read.format("delta").option("versionAsOf", 0).load("data/out/respondents")  # as first written
```

## How it works

```
raw wave CSVs ──► harmonise ──► enforce schema ──► add features ──► write respondents
 (any number)     (rename/recode)  (cast + quarantine)  (window fns)    (parquet, by wave)
                                                                              │
                                                              recompute metrics over full store
                                                                              ▼
                                                                   metrics (wave × segment)
```

1. **Ingest** every `*.csv` in the input folder — no per-wave code.
2. **Harmonise** wave-specific names and codes to canonical form (config-driven).
3. **Enforce schema**: cast to the canonical types; rows that violate the
   contract (bad range, missing key) are written to a separate `rejects/` set
   with a reason; never dropped silently.
4. **Add features** with window functions: weight normalised within each wave,
   NPS segment, top-2-box flag.
5. **Write respondents** partitioned by `wave`.
6. **Aggregate** to weighted metrics per wave × segment, with wave-over-wave
   `nps_delta` and a within-wave `nps_rank`.

**Incremental & idempotent:** an incremental run processes only waves not
already in the output and dynamic-overwrites just those partitions. Running the
same wave again replaces its partition rather than appending; so re-runs are
safe and produce identical output. Metrics are recomputed over the full store
each run (it's tiny) so the wave-over-wave deltas are always correct.

## Configuration

`config/pipeline.example.yaml` — everything is optional with sensible defaults:

```yaml
harmonise:
  rename:
    nps: nps_score          # legacy column name -> canonical
  region:
    N: North                # legacy region code -> canonical label
    S: South
segment_cols: [region]      # break metrics down by these, alongside wave
metrics:
  promoter_min: 9           # NPS 9-10 = promoter
  detractor_max: 6          # NPS 0-6  = detractor
  top2box_min: 4            # satisfaction 4-5 = top-2-box
```

## Outputs

| Path | Contents |
| --- | --- |
| `data/out/respondents/` | Respondent-level rows with derived features — partitioned parquet (`wave=*/`), or a Delta table upserted by key with `--format delta` |
| `data/out/metrics/` | Weighted metrics per wave × segment: `nps`, `nps_delta`, `top2box_pct`, `awareness_pct`, `nps_rank_in_wave` |
| `data/out/rejects/wave=*/` | Rows that failed the data contract, with a `reject_reason` |

## Project structure

```
survey-wave-pipeline/
├── src/wave_pipeline/
│   ├── spark.py         # tuned local SparkSession builder
│   ├── schema.py        # canonical StructType + enforcement / quarantine
│   ├── harmonise.py     # wave-specific names & codes -> canonical
│   ├── config.py        # YAML pipeline config
│   ├── ingest.py        # read + harmonise + enforce + union all waves
│   ├── transform.py     # respondent features (within-wave window fns)
│   ├── aggregate.py     # weighted metrics + wave-over-wave delta + rank
│   ├── io.py            # partitioned, idempotent parquet I/O
│   ├── io_delta.py      # Delta Lake I/O — MERGE upserts, time travel
│   ├── pipeline.py      # orchestration: full & incremental runs
│   └── cli.py           # command-line interface
├── scripts/
│   └── generate_synthetic_waves.py
├── config/pipeline.example.yaml
├── tests/               # session-scoped Spark fixture + unit/e2e tests
├── examples/example_run.py
└── docs/OVERVIEW.md
```

## Testing

```bash
pytest -q
```

Tests build one small local Spark session and cover schema enforcement,
harmonisation, window-function transforms, weighted metrics/deltas, and an
end-to-end full → incremental → idempotency check.

## A note on the clean-room approach

This is a from-scratch reimplementation against **synthetic** data generated by
`scripts/generate_synthetic_waves.py`. It carries no real respondents, brands,
or client content, and deliberately no notebook, no workspace/lakehouse
identifiers, and no environment paths. The engineering is the point; the
environment it might otherwise run in is not part of the repo.

## License

MIT  see [LICENSE](LICENSE).
