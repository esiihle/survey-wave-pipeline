"""Shared test fixtures.

One Spark session is built for the whole test session (startup is the slow
part), tuned tiny so jobs finish fast.
"""

from __future__ import annotations

import pytest

from wave_pipeline.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark(app_name="wave-pipeline-tests", shuffle_partitions=2,
                        master="local[2]")
    yield session
    session.stop()
