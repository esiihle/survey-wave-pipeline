"""Pipeline configuration.

Keeps survey-specific choices (how to harmonise waves, which dimensions to
report by) out of the code and in one YAML file, so the same engine serves any
tracker. Everything has a sensible default, so an empty/minimal config still
runs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


class ConfigError(ValueError):
    """Raised when the pipeline config is structurally invalid."""


@dataclass
class PipelineConfig:
    """Resolved pipeline settings.

    Attributes:
        rename_map: Source-column -> canonical-column renames (harmonisation).
        region_map: Raw-region-value -> canonical-value recodes.
        segment_cols: Dimensions to break metrics down by, alongside ``wave``.
        promoter_min: Minimum NPS score counted as a promoter (default 9).
        detractor_max: Maximum NPS score counted as a detractor (default 6).
        top2box_min: Minimum satisfaction counted as "top-2-box" (default 4).
    """

    rename_map: dict[str, str] = field(default_factory=dict)
    region_map: dict[str, str] = field(default_factory=dict)
    segment_cols: list[str] = field(default_factory=lambda: ["region"])
    promoter_min: int = 9
    detractor_max: int = 6
    top2box_min: int = 4


def load_config(path: str | Path) -> PipelineConfig:
    """Load pipeline config from YAML, falling back to defaults for anything absent."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as fh:
        raw: dict[str, Any] = yaml.safe_load(fh) or {}

    if not isinstance(raw, dict):
        raise ConfigError("top level of pipeline config must be a mapping")

    harmonise = raw.get("harmonise", {}) or {}
    rename_map = harmonise.get("rename", {}) or {}
    region_map = harmonise.get("region", {}) or {}
    if not isinstance(rename_map, dict) or not isinstance(region_map, dict):
        raise ConfigError("harmonise.rename and harmonise.region must be mappings")

    metrics = raw.get("metrics", {}) or {}
    segment_cols = raw.get("segment_cols", ["region"]) or ["region"]
    if not isinstance(segment_cols, list):
        raise ConfigError("segment_cols must be a list")

    return PipelineConfig(
        rename_map={str(k): str(v) for k, v in rename_map.items()},
        region_map={str(k): str(v) for k, v in region_map.items()},
        segment_cols=[str(c) for c in segment_cols],
        promoter_min=int(metrics.get("promoter_min", 9)),
        detractor_max=int(metrics.get("detractor_max", 6)),
        top2box_min=int(metrics.get("top2box_min", 4)),
    )
