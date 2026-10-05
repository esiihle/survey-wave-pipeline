"""Shared test fixtures.

One Spark session is built for the whole test session (startup is the slow
part), tuned tiny so jobs finish fast.

The session is **Delta-enabled when possible**. Delta needs JARs pulled from
Maven on first use; where that network isn't available (e.g. an offline
sandbox), we fall back to a plain session and the Delta tests skip themselves
via the ``delta_available`` fixture. CI and a normal laptop have the network,
so there the Delta tests run for real.
"""

from __future__ import annotations

import os
import tempfile
import uuid

import pytest
from pyspark.sql import SparkSession

from wave_pipeline.spark import get_spark


def _build_session():
    """Return ``(spark, delta_available)``.

    Try a Delta-enabled session and prove it with a trivial Delta write; if
    anything fails (missing JARs / no network), tear down and build a plain
    session instead.
    """
    try:
        spark = get_spark(app_name="wave-tests", shuffle_partitions=2,
                          master="local[2]", delta=True)
        probe = os.path.join(tempfile.gettempdir(), "delta_probe_" + uuid.uuid4().hex)
        spark.range(1).write.format("delta").mode("overwrite").save(probe)
        return spark, True
    except Exception:
        active = SparkSession.getActiveSession()
        if active is not None:
            try:
                active.stop()
            except Exception:
                pass
        spark = get_spark(app_name="wave-tests", shuffle_partitions=2,
                          master="local[2]", delta=False)
        return spark, False


@pytest.fixture(scope="session")
def _ctx():
    spark, delta_ok = _build_session()
    yield spark, delta_ok
    spark.stop()


@pytest.fixture(scope="session")
def spark(_ctx):
    return _ctx[0]


@pytest.fixture(scope="session")
def delta_available(_ctx) -> bool:
    return _ctx[1]
