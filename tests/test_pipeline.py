"""End-to-end tests: full run, incremental run, and idempotency."""

from __future__ import annotations

import os

from wave_pipeline.config import PipelineConfig
from wave_pipeline.io import existing_waves, read_parquet
from wave_pipeline.pipeline import run

HEADER = ("respondent_id,wave,fieldwork_date,region,age_group,weight,"
          "nps_score,satisfaction,aware,brand_used\n")


def _write_wave(raw_dir, wave, n=4):
    lines = [HEADER]
    for i in range(n):
        nps = 9 if i % 2 == 0 else 3        # alternating promoter / detractor
        lines.append(f"W{wave}-{i},{wave},2026-0{wave}-15,North,25-34,1.0,"
                     f"{nps},4,1,Crispa\n")
    path = os.path.join(raw_dir, f"wave_{wave:02d}.csv")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("".join(lines))


def test_full_then_incremental_then_idempotent(spark, tmp_path):
    raw = tmp_path / "raw"
    out = tmp_path / "out"
    raw.mkdir()
    _write_wave(str(raw), 1)
    _write_wave(str(raw), 2)
    cfg = PipelineConfig()

    # --- full run over waves 1 and 2 ---
    s1 = run(spark, cfg, str(raw), str(out), incremental=False)
    assert s1.waves_processed == [1, 2]
    assert s1.clean_rows == 8
    resp_path = os.path.join(str(out), "respondents")
    assert read_parquet(spark, resp_path).count() == 8
    assert existing_waves(spark, resp_path) == [1, 2]

    # --- full run again is idempotent: no duplicated rows ---
    run(spark, cfg, str(raw), str(out), incremental=False)
    assert read_parquet(spark, resp_path).count() == 8

    # --- a new wave arrives; incremental processes only wave 3 ---
    _write_wave(str(raw), 3)
    s3 = run(spark, cfg, str(raw), str(out), incremental=True)
    assert s3.waves_processed == [3]
    assert s3.clean_rows == 4
    assert read_parquet(spark, resp_path).count() == 12
    assert existing_waves(spark, resp_path) == [1, 2, 3]

    # --- incremental with nothing new is a no-op ---
    s4 = run(spark, cfg, str(raw), str(out), incremental=True)
    assert s4.waves_processed == []
    assert read_parquet(spark, resp_path).count() == 12


def test_rejects_quarantined(spark, tmp_path):
    raw = tmp_path / "raw"
    out = tmp_path / "out"
    raw.mkdir()
    # One good row, one with an out-of-range NPS.
    with open(raw / "wave_01.csv", "w", encoding="utf-8") as fh:
        fh.write(HEADER)
        fh.write("R1,1,2026-01-15,North,25-34,1.0,9,5,1,Crispa\n")
        fh.write("R2,1,2026-01-15,North,25-34,1.0,99,5,1,Crispa\n")
    s = run(spark, PipelineConfig(), str(raw), str(out), incremental=False)
    assert s.clean_rows == 1
    assert s.reject_rows == 1
