# Project Report: Smart Traffic Analysis

## Abstract

This project implements a Big Data Analytics pipeline for traffic sensor data, combining Apache
Hadoop HDFS for distributed storage, Apache Spark (PySpark) for scalable processing, and an
interactive Streamlit dashboard for presentation. It analyzes 204,000 hourly traffic
observations across 100 intersections in 6 city zones to surface temporal, spatial, and
environmental traffic patterns, and to rank zones as potential congestion hotspots using a
transparent, documented proxy metric.

## Problem Statement

Traffic authorities collect large volumes of per-intersection, per-hour sensor data but lack an
accessible way to turn it into actionable insight: which hours and days see the heaviest
traffic, which locations are most congested, how weather affects flow, and where to prioritize
attention. Manual inspection of hundreds of thousands of rows is impractical; this project
automates that analysis at scale and presents it through clear visuals.

## Aim and Objectives

**Aim:** Build a working Big Data pipeline (Hadoop + Spark) and a presentation layer (notebook +
dashboard) that together answer concrete traffic-pattern questions from real sensor data.

**Objectives:**
1. Verify and respect the existing Java/Hadoop environment without upgrading it.
2. Implement genuine HDFS storage and PySpark processing, with a local fallback.
3. Clean and aggregate the dataset by time, location, vehicle category, and weather.
4. Rank locations as potential hotspots using a documented, transparent method.
5. Present findings in a professional interactive dashboard and an explanatory notebook.

## Existing System vs. Proposed System

**Existing system:** traffic data is collected by sensors but reviewed, if at all, through raw
spreadsheets or ad-hoc scripts, with no scalable processing layer and no interactive
presentation.

**Proposed system:** a two-mode (local/HDFS) PySpark pipeline performs all cleaning and
aggregation in a single, reusable codebase (`src/`), emits aggregated CSVs, and feeds both a
Jupyter notebook (for narrative analysis) and a Streamlit dashboard (for interactive
exploration) — see `docs/ARCHITECTURE.md` for the full data flow.

## Data Processing Methodology

1. **Schema validation** — the raw CSV is read into Spark with an explicit schema
   (`TRAFFIC_SCHEMA` in `src/data_processing.py`) rather than relying on type inference.
2. **Cleaning** — timestamps are parsed and unparseable rows dropped; duplicate `record_id`s are
   removed; rows missing core fields (timestamp, vehicle_count, city_zone) are dropped.
   (On this dataset, verified cleaning found 0 nulls and 0 duplicates — see notebook Section 8.)
3. **Derivation** — `event_date`, `month`, and `weekday_name` are derived from the parsed
   timestamp for grouping.
4. **Aggregation** — Spark DataFrame `groupBy`/`agg` operations compute totals and averages by
   hour, date, weekday, month, city zone, intersection, weather condition, road type, and
   vehicle category (11 output tables total, in `outputs/`).
5. **Hotspot ranking** — a weighted, documented score (`0.4 × congestion_score_norm + 0.3 ×
   queue_length_norm + 0.3 × inverse_speed_norm`, all min-max normalized per zone) ranks zones;
   the formula and its proxy-metric caveat are stated directly in the dashboard's Hotspots tab.

## Module Descriptions

- `src/config.py` — loads all configuration from `.env`, no hardcoded machine paths.
- `src/data_processing.py` — Spark session creation, the explicit schema, HDFS reachability
  check and upload helper, and the `clean()` transformation.
- `src/pipeline.py` — orchestrates load → (optional HDFS round trip) → clean → 11 aggregations →
  CSV outputs → `pipeline_status.json`.
- `app.py` — Streamlit dashboard; pure presentation layer over `outputs/*.csv`.
- `Smart_Traffic_Analysis.ipynb` — 20-section narrative notebook (title through conclusion),
  executed end-to-end with zero cell errors as part of this project's testing.
- `tests/test_data_processing.py` — pytest suite covering cleaning logic and HDFS-reachability
  detection, using a local Spark session (no HDFS required).

## Analysis Results (from actual execution, local mode)

Pipeline run against the full 204,000-row dataset on this machine:

- Rows processed: 204,000 (0 dropped — dataset was already clean).
- 11 aggregated output tables generated successfully (`outputs/*.csv`).
- Highest-ranked hotspot zone: **Downtown Core** (hotspot_score = 1.00), followed closely by
  **Financial District** (0.90) — both driven by high `avg_congestion_score` and `queue_length`
  relative to the other 4 zones.
- Adverse weather (`Heavy Rain`, `Fog`) showed the highest average `congestion_score` and lowest
  average speed among the 4 recorded weather conditions.

Full numeric outputs are in `outputs/*.csv`; `outputs/pipeline_status.json` records the exact
run metadata (mode, timestamps, row/column counts).

## Limitations

- Dataset provenance and license are undocumented; several metrics (`congestion_score`,
  `traffic_flow_rate`, `emission_estimate`, `fuel_waste_estimate`) are pre-computed by an unknown
  method and used as supplied, proxy indicators rather than verified ground truth.
- No road-capacity data exists, so vehicle counts alone are not equated with congestion severity.
- HDFS mode depends on a running single-node Hadoop service on the demonstration machine; it was
  implemented and code-verified, but a live end-to-end HDFS run requires the service to be
  started (and, the first time, the NameNode formatted) by the user.
- Local Spark execution (`local[*]`) is single-machine parallelism, not distributed-cluster
  execution.

## Future Scope

- Deploy against a real multi-node Hadoop/Spark cluster.
- Add a properly trained and evaluated forecasting model before making predictive claims.
- Incorporate road-capacity/lane-count data to validate congestion metrics independently.

## Conclusion

The project delivers a working, tested Hadoop/PySpark pipeline and an interactive dashboard over
a real traffic dataset, with all claims — dataset provenance, HDFS availability, and proxy
metrics — stated accurately rather than assumed.

## Dataset Attribution and References

- `Traffic.csv` — present in the project workspace at the start of this project; no
  accompanying license or source documentation was found. See `data/data_dictionary.csv`.
- Apache Hadoop: https://hadoop.apache.org/
- Apache Spark / PySpark: https://spark.apache.org/docs/latest/api/python/
- Streamlit: https://docs.streamlit.io/
