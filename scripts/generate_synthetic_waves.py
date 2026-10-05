"""Generate synthetic survey-tracker waves.

Writes one CSV per wave into an output folder. The data is entirely synthetic —
no real respondents, brands, or client content — so the whole pipeline can be
cloned and run by anyone.

Two deliberate wrinkles make the pipeline's job realistic:

* **A legacy first wave** (``--legacy-first``, on by default) uses old column
  names (``nps`` instead of ``nps_score``) and short region codes
  (``N``/``S``/``E``/``W``). The example config's harmonisation maps fix these,
  demonstrating the "wave-agnostic" ingest.
* **Dirty rows** (``--dirty``) injects out-of-range values (nps 99, negative
  weight, satisfaction 7) so the schema-enforcement / quarantine path has
  something to catch.

NPS drifts gently upward across waves so wave-over-wave deltas are non-trivial.
"""

from __future__ import annotations

import argparse
import os
import random

import pandas as pd

REGIONS = ["North", "South", "East", "West"]
REGION_LEGACY = {"North": "N", "South": "S", "East": "E", "West": "W"}
AGE_GROUPS = ["18-24", "25-34", "35-44", "45-54", "55+"]
BRANDS = ["Crispa", "Muncho", "Snaxi", "Krunch", None]


def _nps_score(rng: random.Random, promoter_bias: float) -> int:
    """Draw an NPS score; higher promoter_bias shifts the distribution up."""
    r = rng.random()
    if r < 0.15 + promoter_bias:          # promoter
        return rng.randint(9, 10)
    if r < 0.45 + promoter_bias:          # passive
        return rng.randint(7, 8)
    return rng.randint(0, 6)              # detractor


def _one_wave(rng: random.Random, wave: int, n: int, promoter_bias: float) -> pd.DataFrame:
    rows = []
    for i in range(n):
        region = rng.choice(REGIONS)
        nps = _nps_score(rng, promoter_bias)
        rows.append({
            "respondent_id": f"W{wave:02d}-{i+1:05d}",
            "wave": wave,
            "fieldwork_date": f"2026-{wave:02d}-15",
            "region": region,
            "age_group": rng.choice(AGE_GROUPS),
            "weight": round(rng.uniform(0.5, 1.8), 3),
            "nps_score": nps,
            "satisfaction": rng.randint(1, 5),
            "aware": 1 if rng.random() < 0.7 else 0,
            "brand_used": rng.choice(BRANDS),
        })
    return pd.DataFrame(rows)


def _apply_legacy_format(df: pd.DataFrame) -> pd.DataFrame:
    """Rename/recode a wave to the old format the harmoniser must fix."""
    df = df.rename(columns={"nps_score": "nps"})
    df["region"] = df["region"].map(REGION_LEGACY)
    return df


def _inject_dirty(rng: random.Random, df: pd.DataFrame) -> pd.DataFrame:
    """Corrupt a few rows so schema enforcement has rejects to quarantine."""
    df = df.copy()
    if len(df) >= 3:
        df.loc[df.index[0], "nps_score" if "nps_score" in df.columns else "nps"] = 99
        df.loc[df.index[1], "weight"] = -1.0
        df.loc[df.index[2], "satisfaction"] = 7
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic survey-tracker waves.")
    ap.add_argument("--waves", type=int, default=4, help="number of waves")
    ap.add_argument("--rows", type=int, default=300, help="respondents per wave")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data/raw", help="output folder for wave CSVs")
    ap.add_argument("--legacy-first", dest="legacy_first", action="store_true", default=True,
                    help="make wave 1 use legacy column/region names (default on)")
    ap.add_argument("--no-legacy-first", dest="legacy_first", action="store_false")
    ap.add_argument("--dirty", action="store_true", help="inject out-of-range rows")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    rng = random.Random(args.seed)

    for wave in range(1, args.waves + 1):
        promoter_bias = 0.03 * (wave - 1)   # gentle upward NPS drift
        df = _one_wave(rng, wave, args.rows, promoter_bias)
        if args.dirty:
            df = _inject_dirty(rng, df)
        if args.legacy_first and wave == 1:
            df = _apply_legacy_format(df)
        path = os.path.join(args.out, f"wave_{wave:02d}.csv")
        df.to_csv(path, index=False)
        print(f"wrote {len(df)} rows -> {path}")


if __name__ == "__main__":
    main()
