"""Central configuration loaded from environment variables / .env.

Keeps machine-specific paths (Java, Hadoop, dataset location) out of code,
per the project's "no hardcoded paths" requirement.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def _bool(val: str, default: bool = False) -> bool:
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


MODE = os.getenv("TRAFFIC_MODE", "local").strip().lower()  # "local" or "hdfs"

TRAFFIC_CSV_PATH = Path(os.getenv("TRAFFIC_CSV_PATH", "Traffic.csv"))
if not TRAFFIC_CSV_PATH.is_absolute():
    TRAFFIC_CSV_PATH = PROJECT_ROOT / TRAFFIC_CSV_PATH

HDFS_NAMENODE_HOST = os.getenv("HDFS_NAMENODE_HOST", "127.0.0.1")
HDFS_NAMENODE_PORT = int(os.getenv("HDFS_NAMENODE_PORT", "9000"))
HDFS_BASE_DIR = os.getenv("HDFS_BASE_DIR", "/user/traffic")
HDFS_DEFAULT_FS = f"hdfs://{HDFS_NAMENODE_HOST}:{HDFS_NAMENODE_PORT}"
HDFS_DATASET_PATH = f"{HDFS_BASE_DIR}/Traffic.csv"

JAVA_HOME = os.getenv("JAVA_HOME", "")
HADOOP_HOME = os.getenv("HADOOP_HOME", "")

SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")

OUTPUTS_DIR = PROJECT_ROOT / "outputs"
OUTPUTS_DIR.mkdir(exist_ok=True)

PIPELINE_STATUS_PATH = OUTPUTS_DIR / "pipeline_status.json"
