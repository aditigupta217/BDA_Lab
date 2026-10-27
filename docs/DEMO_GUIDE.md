# Demonstration Script

A short, practical walkthrough for presenting this project to faculty.

## 1. Environment check (30 seconds)

```bash
cd /Users/aditi/Desktop/BDA-project
source venv/bin/activate
./scripts/check_environment.sh
```

Say: *"This confirms Java 8, Hadoop 3.4.3, and a dedicated Python 3.11 virtual environment —
chosen specifically because PySpark 3.5 doesn't yet support Python 3.13, which is this Mac's
default."*

## 2. Run the pipeline (1–2 minutes)

```bash
./scripts/run_pipeline.sh
```

Say: *"This is PySpark, not pandas, doing the cleaning and aggregation — it reads the dataset
with an explicit schema, drops bad/duplicate rows, and produces 11 aggregated tables."* Point out
the printed JSON status at the end: row count, mode, output file list.

If running in HDFS mode, first show:
```bash
jps                               # NameNode / DataNode running
$HADOOP_HOME/bin/hadoop fs -ls /user/traffic   # the uploaded file, in HDFS
```

## 3. Open the notebook (2–3 minutes)

```bash
jupyter notebook Smart_Traffic_Analysis.ipynb
```

Walk through: dataset inspection → cleaning (Section 7–8, show 0 nulls/duplicates found) →
hourly/daily EDA charts (Section 12) → the live PySpark cells (Section 15) → the HDFS check cell
(Section 16, shows honestly whether HDFS was reachable at run time).

## 4. Launch the dashboard (3–4 minutes)

```bash
./scripts/run_dashboard.sh
```

Walk through each sidebar section:
- **Overview** — KPI cards (total records, vehicles, peak hour, date range).
- **Traffic Patterns** — hourly/weekday/weather charts.
- **Location Analysis** — zone ranking + intersection map.
- **Vehicle Analysis** — vehicle-category mix.
- **Traffic Hotspots** — read the on-screen formula explanation aloud; it's the same formula
  described in `docs/PROJECT_REPORT.md`.
- **Data Explorer** — apply a date/zone/hour filter live, then click "Download filtered CSV."
- **Pipeline Status** — shows the exact mode (local/HDFS), row count, and any errors from the
  last pipeline run — proof nothing here is faked.

## 5. Closing point

Say: *"Every number on this dashboard traces back to a Spark aggregation in `outputs/*.csv`,
and the pipeline status page shows exactly how and when it was produced — nothing is
hardcoded or simulated."*
