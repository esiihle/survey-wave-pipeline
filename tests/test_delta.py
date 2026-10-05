"""Delta Lake output tests: MERGE upserts, incremental, idempotency, history.

These run only where the Delta JARs are available (CI, a normal laptop). In an
offline sandbox the ``delta_available`` fixture is False and the whole module
skips — see tests/conftest.py.
"""

from __future__ import annotations

import os

import pytest

from wave_pipeline.config import PipelineConfig
from wave_pipeline.io_delta import existing_waves_delta, read_delta, version_count
from wave_pipeline.pipeline import run

HEADER = ("respondent_id,wave,fieldwork_date,region,age_group,weight,"
          "nps_score,satisfaction,aware,brand_used\n")


@pytest.fixture(autouse=True)
def _require_delta(delta_available):
    if not delta_available:
        pytest.skip("Delta runtime unavailable in this environment")


def _write_rows(raw_dir, wave, rows):
    """rows: list of (respondent_id, nps_score). Other fields are fixed."""
    lines = [HEADER]
    for rid, nps in rows:
        lines.append(f"{rid},{wave},2026-0{wave}-15,North,25-34,1.0,"
                     f"{nps},4,1,Crispa\n")
    with open(os.path.join(raw_dir, f"wave_{wave:02d}.csv"), "w", encoding="utf-8") as fh:
        fh.write("".join(lines))


def test_delta_full_incremental_idempotent(spark, tmp_path):
    raw = tmp_path / "raw"; out = tmp_path / "out"; raw.mkdir()
    _write_rows(str(raw), 1, [("R1", 9), ("R2", 3)])
    _write_rows(str(raw), 2, [("R3", 9), ("R4", 3)])
    cfg = PipelineConfig()
    resp = os.path.join(str(out), "respondents")

    s1 = run(spark, cfg, str(raw), str(out), output_format="delta")
    assert s1.waves_processed == [1, 2]
    assert read_delta(spark, resp).count() == 4
    assert existing_waves_delta(spark, resp) == [1, 2]

    # Idempotent: a repeat full run MERGEs the same keys -> no duplicates.
    run(spark, cfg, str(raw), str(out), output_format="delta")
    assert read_delta(spark, resp).count() == 4

    # Incremental: only the new wave is processed.
    _write_rows(str(raw), 3, [("R5", 10), ("R6", 0)])
    s3 = run(spark, cfg, str(raw), str(out), incremental=True, output_format="delta")
    assert s3.waves_processed == [3]
    assert read_delta(spark, resp).count() == 6
    assert existing_waves_delta(spark, resp) == [1, 2, 3]

    # Time travel: each write is a new table version.
    assert version_count(spark, resp) >= 2


def test_delta_merge_updates_changed_rows(spark, tmp_path):
    raw = tmp_path / "raw"; out = tmp_path / "out"; raw.mkdir()
    resp = os.path.join(str(out), "respondents")

    _write_rows(str(raw), 1, [("R1", 10), ("R2", 10)])   # both promoters
    run(spark, cfg := PipelineConfig(), str(raw), str(out), output_format="delta")
    assert read_delta(spark, resp).count() == 2

    # Same ids, changed answer: MERGE should update in place, not duplicate.
    _write_rows(str(raw), 1, [("R1", 0), ("R2", 10)])    # R1 now a detractor
    run(spark, cfg, str(raw), str(out), output_format="delta")
    df = read_delta(spark, resp)
    assert df.count() == 2  # no duplicate rows
    r1 = {r["respondent_id"]: r["nps_score"] for r in df.collect()}
    assert r1["R1"] == 0    # updated value
