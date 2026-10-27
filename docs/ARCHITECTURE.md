# System Architecture

## Components and Responsibilities

| Component | Handles |
|---|---|
| **HDFS (NameNode + DataNode)** | Storage only — holds `Traffic.csv` as a block-replicated file in HDFS mode. |
| **PySpark (`src/data_processing.py`, `src/pipeline.py`)** | Processing — schema validation, cleaning, deduplication, and all aggregations (Spark DataFrame API). |
| **Pandas** | Small, already-aggregated result sets: converts Spark output to CSV for the dashboard, and powers notebook-side EDA. |
| **Streamlit + Plotly (`app.py`)** | Presentation — reads `outputs/*.csv` and `pipeline_status.json`; renders KPIs, charts, filters. Runs no Spark/Hadoop itself. |
| **Jupyter Notebook** | Narrative analysis — EDA with Matplotlib/Seaborn, plus a live demonstration of the same Spark pipeline. |

## Data Flow

```
┌────────────┐
│ Traffic.csv│  (original file, never modified)
└─────┬──────┘
      │
      ├── LOCAL MODE ──────────────────────────────┐
      │   PySpark reads the CSV directly off disk    │
      │                                               ▼
      │                                     ┌───────────────────┐
      └── HDFS MODE ───────────────────────►│ clean() (Spark)    │
          1. hadoop fs -mkdir -p /user/traffic        │
          2. hadoop fs -put Traffic.csv               │
          3. PySpark reads hdfs://127.0.0.1:9000/...   │
                                             └─────────┬──────────┘
                                                        ▼
                                          ┌─────────────────────────┐
                                          │ Spark aggregations:      │
                                          │ by hour / date / weekday │
                                          │ / month / zone /         │
                                          │ intersection / weather / │
                                          │ road_type / vehicle mix │
                                          └─────────┬────────────────┘
                                                     ▼
                                       outputs/*.csv + pipeline_status.json
                                                     ▼
                                          Streamlit dashboard (app.py)
```

## Execution Modes

- **Local mode** (`TRAFFIC_MODE=local`): Spark runs as `local[*]` and reads the CSV from the
  local filesystem. No Hadoop service required. This is local parallelism across CPU cores, not
  distributed cluster execution.
- **HDFS mode** (`TRAFFIC_MODE=hdfs`): the dataset is uploaded to a running single-node HDFS
  service with the `hadoop fs -put` CLI, then PySpark reads it back via an `hdfs://` path —
  demonstrating a genuine storage/processing separation. If the configured NameNode is not
  reachable, the pipeline fails explicitly (`HdfsUnavailableError`) rather than silently
  continuing or claiming HDFS processing succeeded.

## Configuration

All machine-specific values (Java/Hadoop paths, NameNode host/port, dataset path, Spark memory)
are read from `.env` by `src/config.py` — nothing is hardcoded, so the same code runs on a
different machine by editing `.env` alone.
