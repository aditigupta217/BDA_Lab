"""Orchestrates the end-to-end pipeline: load -> (HDFS round trip) -> clean
-> aggregate -> write outputs -> report status.

Run directly: `python -m src.pipeline` (respects TRAFFIC_MODE from .env).
"""
import json
import sys
import traceback
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src import config, data_processing as dp


def _write_csv(df, name: str) -> str:
    """Collect a (small, already-aggregated) Spark DataFrame to pandas and
    write a single CSV file for the dashboard to consume."""
    out_path = config.OUTPUTS_DIR / f"{name}.csv"
    df.toPandas().to_csv(out_path, index=False)
    return str(out_path)


def run() -> dict:
    status = {
        "mode": config.MODE,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "dataset_source": None,
        "row_count": None,
        "column_count": None,
        "outputs": [],
        "errors": [],
        "success": False,
    }

    try:
        spark = dp.get_spark_session()
    except Exception as exc:
        status["errors"].append(f"Failed to start Spark: {exc}")
        _finalize(status)
        return status

    try:
        if config.MODE == "hdfs":
            hdfs_path = dp.upload_to_hdfs(config.TRAFFIC_CSV_PATH)
            status["dataset_source"] = f"{config.HDFS_DEFAULT_FS}{hdfs_path}"
            raw = dp.load_raw(spark, hdfs_path)
        else:
            status["dataset_source"] = f"local://{config.TRAFFIC_CSV_PATH}"
            raw = dp.load_raw(spark, str(config.TRAFFIC_CSV_PATH))

        cleaned = dp.clean(raw).cache()
        status["row_count"] = cleaned.count()
        status["column_count"] = len(cleaned.columns)

        _aggregate_and_write(cleaned, status)
        status["success"] = True

    except dp.HdfsUnavailableError as exc:
        status["errors"].append(str(exc))
    except Exception as exc:
        status["errors"].append(f"{exc}\n{traceback.format_exc()}")
    finally:
        spark.stop()

    _finalize(status)
    return status


def _aggregate_and_write(df, status: dict):
    outputs = status["outputs"]

    by_hour = (
        df.groupBy("hour")
        .agg(
            F.sum("vehicle_count").alias("total_vehicles"),
            F.avg("vehicle_count").alias("avg_vehicles"),
            F.avg("average_speed").alias("avg_speed"),
            F.avg("congestion_score").alias("avg_congestion_score"),
        )
        .orderBy("hour")
    )
    outputs.append(_write_csv(by_hour, "agg_by_hour"))

    by_date = (
        df.groupBy("event_date")
        .agg(
            F.sum("vehicle_count").alias("total_vehicles"),
            F.avg("vehicle_count").alias("avg_vehicles"),
            F.avg("congestion_score").alias("avg_congestion_score"),
        )
        .orderBy("event_date")
    )
    outputs.append(_write_csv(by_date, "agg_by_date"))

    by_weekday = (
        df.groupBy("weekday_name", "is_weekend")
        .agg(
            F.sum("vehicle_count").alias("total_vehicles"),
            F.avg("vehicle_count").alias("avg_vehicles"),
        )
        .orderBy("avg_vehicles", ascending=False)
    )
    outputs.append(_write_csv(by_weekday, "agg_by_weekday"))

    by_month = (
        df.groupBy("month")
        .agg(
            F.sum("vehicle_count").alias("total_vehicles"),
            F.avg("vehicle_count").alias("avg_vehicles"),
        )
        .orderBy("month")
    )
    outputs.append(_write_csv(by_month, "agg_by_month"))

    by_zone = (
        df.groupBy("city_zone")
        .agg(
            F.sum("vehicle_count").alias("total_vehicles"),
            F.avg("vehicle_count").alias("avg_vehicles"),
            F.avg("average_speed").alias("avg_speed"),
            F.avg("congestion_score").alias("avg_congestion_score"),
            F.avg("queue_length").alias("avg_queue_length"),
        )
        .orderBy("total_vehicles", ascending=False)
    )
    outputs.append(_write_csv(by_zone, "agg_by_zone"))

    by_intersection = (
        df.groupBy("city_zone", "road_id", "intersection_id", "latitude", "longitude")
        .agg(
            F.sum("vehicle_count").alias("total_vehicles"),
            F.avg("vehicle_count").alias("avg_vehicles"),
            F.avg("average_speed").alias("avg_speed"),
            F.avg("congestion_score").alias("avg_congestion_score"),
            F.avg("queue_length").alias("avg_queue_length"),
            F.sum("accident_reported").alias("accident_count"),
        )
        .orderBy("total_vehicles", ascending=False)
    )
    outputs.append(_write_csv(by_intersection, "agg_by_intersection"))

    by_weather = (
        df.groupBy("weather_condition")
        .agg(
            F.avg("vehicle_count").alias("avg_vehicles"),
            F.avg("average_speed").alias("avg_speed"),
            F.avg("congestion_score").alias("avg_congestion_score"),
        )
        .orderBy("avg_congestion_score", ascending=False)
    )
    outputs.append(_write_csv(by_weather, "agg_by_weather"))

    by_road_type = (
        df.groupBy("road_type")
        .agg(
            F.sum("vehicle_count").alias("total_vehicles"),
            F.avg("vehicle_count").alias("avg_vehicles"),
            F.avg("average_speed").alias("avg_speed"),
        )
        .orderBy("total_vehicles", ascending=False)
    )
    outputs.append(_write_csv(by_road_type, "agg_by_road_type"))

    vehicle_mix = df.agg(
        F.sum("vehicle_count").alias("total_vehicles"),
        F.sum("heavy_vehicle_count").alias("total_heavy"),
        F.sum("motorcycle_count").alias("total_motorcycle"),
        F.sum("public_transport_count").alias("total_public_transport"),
    )
    outputs.append(_write_csv(vehicle_mix, "vehicle_mix_totals"))

    vehicle_by_hour = df.groupBy("hour").agg(
        F.sum("heavy_vehicle_count").alias("heavy"),
        F.sum("motorcycle_count").alias("motorcycle"),
        F.sum("public_transport_count").alias("public_transport"),
    ).orderBy("hour")
    outputs.append(_write_csv(vehicle_by_hour, "vehicle_mix_by_hour"))

    # Hotspot ranking: documented, transparent weighted score combining
    # normalized congestion_score, queue_length, and inverse average_speed,
    # within each zone. These are proxy indicators, not verified ground truth
    # (see data/data_dictionary.csv).
    zone_stats = by_zone.toPandas()
    if not zone_stats.empty:
        for col in ["avg_congestion_score", "avg_queue_length"]:
            rng = zone_stats[col].max() - zone_stats[col].min()
            zone_stats[f"{col}_norm"] = (
                (zone_stats[col] - zone_stats[col].min()) / rng if rng else 0.0
            )
        spd_rng = zone_stats["avg_speed"].max() - zone_stats["avg_speed"].min()
        zone_stats["inv_speed_norm"] = (
            (zone_stats["avg_speed"].max() - zone_stats["avg_speed"]) / spd_rng if spd_rng else 0.0
        )
        zone_stats["hotspot_score"] = (
            0.4 * zone_stats["avg_congestion_score_norm"]
            + 0.3 * zone_stats["avg_queue_length_norm"]
            + 0.3 * zone_stats["inv_speed_norm"]
        )
        zone_stats = zone_stats.sort_values("hotspot_score", ascending=False)
        hotspot_path = config.OUTPUTS_DIR / "hotspot_ranking.csv"
        zone_stats.to_csv(hotspot_path, index=False)
        outputs.append(str(hotspot_path))


def _finalize(status: dict):
    status["finished_at"] = datetime.now(timezone.utc).isoformat()
    with open(config.PIPELINE_STATUS_PATH, "w") as f:
        json.dump(status, f, indent=2)
    print(json.dumps(status, indent=2))


if __name__ == "__main__":
    result = run()
    sys.exit(0 if result["success"] else 1)
