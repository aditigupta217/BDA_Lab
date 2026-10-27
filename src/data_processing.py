"""Spark session creation, HDFS helpers, and data cleaning.

Two execution modes are supported end to end:
  - "local": Spark runs as a local[*] process and reads the CSV straight off
    the local filesystem. No Hadoop/HDFS service is required.
  - "hdfs": the dataset is uploaded to a running HDFS NameNode with the
    `hadoop` CLI, then read back through Spark via an hdfs:// path, proving a
    genuine HDFS round trip.

Local Spark execution (local[*]) is NOT distributed cluster execution -- it
parallelizes across local CPU cores only.
"""
import os
import socket
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src import config


class HdfsUnavailableError(RuntimeError):
    """Raised when HDFS mode is requested but the NameNode cannot be reached."""


@dataclass
class ModeResult:
    mode: str
    dataset_path_used: str
    notes: list = field(default_factory=list)


def _set_java_env():
    """Point the JVM Spark launches at the verified Java 8 install, without
    touching the user's shell profile or system-wide JAVA_HOME."""
    if config.JAVA_HOME:
        os.environ["JAVA_HOME"] = config.JAVA_HOME
    if config.HADOOP_HOME:
        os.environ["HADOOP_HOME"] = config.HADOOP_HOME


def check_hdfs_reachable(host: str = None, port: int = None, timeout: float = 2.0) -> bool:
    host = host or config.HDFS_NAMENODE_HOST
    port = port or config.HDFS_NAMENODE_PORT
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _run_hadoop_cli(args: list) -> subprocess.CompletedProcess:
    hadoop_bin = str(Path(config.HADOOP_HOME) / "bin" / "hadoop") if config.HADOOP_HOME else "hadoop"
    env = os.environ.copy()
    env["JAVA_HOME"] = config.JAVA_HOME
    env["HADOOP_HOME"] = config.HADOOP_HOME
    return subprocess.run(
        [hadoop_bin] + args,
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )


def upload_to_hdfs(local_csv_path: Path) -> str:
    """Upload the dataset into HDFS using the `hadoop fs` CLI. Raises
    HdfsUnavailableError with a clear message on any failure."""
    if not check_hdfs_reachable():
        raise HdfsUnavailableError(
            f"HDFS NameNode not reachable at {config.HDFS_NAMENODE_HOST}:"
            f"{config.HDFS_NAMENODE_PORT}. Start it with "
            f"'$HADOOP_HOME/sbin/start-dfs.sh' (format it first with "
            f"'hdfs namenode -format' if this is the first run), then retry."
        )

    mkdir_result = _run_hadoop_cli(["fs", "-mkdir", "-p", config.HDFS_BASE_DIR])
    if mkdir_result.returncode != 0:
        raise HdfsUnavailableError(f"Failed to create {config.HDFS_BASE_DIR} on HDFS: {mkdir_result.stderr}")

    put_result = _run_hadoop_cli(
        ["fs", "-put", "-f", str(local_csv_path), config.HDFS_DATASET_PATH]
    )
    if put_result.returncode != 0:
        raise HdfsUnavailableError(f"Failed to upload dataset to HDFS: {put_result.stderr}")

    return config.HDFS_DATASET_PATH


def get_spark_session(app_name: str = "SmartTrafficAnalysis") -> SparkSession:
    _set_java_env()
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
    )
    if config.MODE == "hdfs":
        builder = builder.config("spark.hadoop.fs.defaultFS", config.HDFS_DEFAULT_FS)
    return builder.getOrCreate()


TRAFFIC_SCHEMA = StructType(
    [
        StructField("record_id", StringType(), True),
        StructField("city_zone", StringType(), True),
        StructField("road_id", StringType(), True),
        StructField("intersection_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("road_type", StringType(), True),
        StructField("lanes", IntegerType(), True),
        StructField("speed_limit", IntegerType(), True),
        StructField("vehicle_count", IntegerType(), True),
        StructField("average_speed", DoubleType(), True),
        StructField("heavy_vehicle_count", IntegerType(), True),
        StructField("motorcycle_count", IntegerType(), True),
        StructField("public_transport_count", IntegerType(), True),
        StructField("traffic_density", DoubleType(), True),
        StructField("queue_length", DoubleType(), True),
        StructField("average_wait_time", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("hour", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("is_weekend", IntegerType(), True),
        StructField("is_holiday", IntegerType(), True),
        StructField("rush_hour", IntegerType(), True),
        StructField("peak_period", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("rainfall_mm", DoubleType(), True),
        StructField("visibility_km", DoubleType(), True),
        StructField("air_quality_index", IntegerType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("nearby_school", IntegerType(), True),
        StructField("nearby_hospital", IntegerType(), True),
        StructField("nearby_market", IntegerType(), True),
        StructField("construction_activity", IntegerType(), True),
        StructField("road_condition", StringType(), True),
        StructField("parking_occupancy", DoubleType(), True),
        StructField("public_event", IntegerType(), True),
        StructField("accident_reported", IntegerType(), True),
        StructField("traffic_camera_count", IntegerType(), True),
        StructField("iot_sensor_health", StringType(), True),
        StructField("signal_cycle_seconds", IntegerType(), True),
        StructField("green_light_duration", IntegerType(), True),
        StructField("emergency_vehicle_detected", IntegerType(), True),
        StructField("traffic_flow_rate", DoubleType(), True),
        StructField("congestion_score", DoubleType(), True),
        StructField("emission_estimate", DoubleType(), True),
        StructField("fuel_waste_estimate", DoubleType(), True),
        StructField("congestion_level", StringType(), True),
    ]
)


def load_raw(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .schema(TRAFFIC_SCHEMA)
        .csv(path)
    )


def clean(df: DataFrame) -> DataFrame:
    """Parse timestamps, drop unparseable/duplicate/incomplete rows, and
    derive calendar fields used throughout the analysis."""
    df = df.withColumn("event_ts", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
    df = df.dropDuplicates(["record_id"])
    df = df.na.drop(subset=["event_ts", "vehicle_count", "city_zone"])
    df = df.withColumn("event_date", F.to_date("event_ts"))
    df = df.withColumn("month", F.month("event_ts"))
    df = df.withColumn(
        "weekday_name",
        F.date_format("event_ts", "EEEE"),
    )
    return df
