"""Tests for the cleaning logic and dataset invariants. Uses a small local
Spark session -- no HDFS required."""
import pytest

from src import data_processing as dp


@pytest.fixture(scope="module")
def spark():
    session = dp.get_spark_session(app_name="pytest")
    yield session
    session.stop()


def _sample_rows(spark):
    data = [
        ("REC_1", "Zone A", "RD_1", "INT_1", 40.0, -74.0, "Arterial", 2, 40, 100,
         35.0, 5, 5, 5, 20.0, 5.0, 1.0, "2026-01-01 08:00:00", 8, 3, 0, 0, 1,
         "Morning Peak", 20.0, 0.0, 10.0, 50, "Clear", 0, 0, 0, 0, "Good", 40.0,
         0, 0, 1, "Optimal", 60, 18, 0, 100.0, 20.0, 10.0, 2.0, "Low"),
        ("REC_2", "Zone A", "RD_1", "INT_1", 40.0, -74.0, "Arterial", 2, 40, 100,
         35.0, 5, 5, 5, 20.0, 5.0, 1.0, "not-a-timestamp", 8, 3, 0, 0, 1,
         "Morning Peak", 20.0, 0.0, 10.0, 50, "Clear", 0, 0, 0, 0, "Good", 40.0,
         0, 0, 1, "Optimal", 60, 18, 0, 100.0, 20.0, 10.0, 2.0, "Low"),
        ("REC_1", "Zone A", "RD_1", "INT_1", 40.0, -74.0, "Arterial", 2, 40, 100,
         35.0, 5, 5, 5, 20.0, 5.0, 1.0, "2026-01-01 08:00:00", 8, 3, 0, 0, 1,
         "Morning Peak", 20.0, 0.0, 10.0, 50, "Clear", 0, 0, 0, 0, "Good", 40.0,
         0, 0, 1, "Optimal", 60, 18, 0, 100.0, 20.0, 10.0, 2.0, "Low"),
    ]
    return spark.createDataFrame(data, schema=dp.TRAFFIC_SCHEMA)


def test_clean_drops_bad_timestamp_and_duplicate(spark):
    df = _sample_rows(spark)
    cleaned = dp.clean(df)
    assert cleaned.count() == 1  # duplicate record_id and bad timestamp both removed


def test_clean_adds_derived_columns(spark):
    df = _sample_rows(spark)
    cleaned = dp.clean(df)
    for col in ("event_ts", "event_date", "month", "weekday_name"):
        assert col in cleaned.columns


def test_hdfs_unreachable_host_detected():
    assert dp.check_hdfs_reachable(host="127.0.0.1", port=59999, timeout=0.5) is False
