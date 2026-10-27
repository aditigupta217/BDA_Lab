# Smart Traffic Analysis

Big Data Processing and Traffic Pattern Visualization using **Apache Hadoop (HDFS)**,
**PySpark**, **Streamlit**, and **Jupyter Notebook**, built on a real 204,000-row traffic
sensor dataset (`Traffic.csv`).

Verified on: macOS, Apple Silicon, Java 8 (`1.8.0_501`), Hadoop 3.4.3, Python 3.11 (venv),
PySpark 3.5.3.

## 1. Verified Environment & Compatibility Decisions

| Component | Found on this machine | Decision |
|---|---|---|
| Java | `1.8.0_501` at `/Library/Java/JavaVirtualMachines/jdk-1.8.jdk/Contents/Home` | Used as-is. Spark 3.5.x fully supports Java 8. |
| Hadoop | `3.4.3` at `$HOME/Downloads/hadoop-3.4.3`, configured for `hdfs://127.0.0.1:9000` | Used as-is, not upgraded/reformatted destructively. |
| System Python | `3.13.3` (default `python3`) | **Not used for PySpark** — PySpark 3.5.x does not officially support 3.13. |
| Project Python | `3.11.15` via Homebrew (`/opt/homebrew/bin/python3.11`), isolated in `venv/` | Chosen specifically for verified PySpark 3.5.x compatibility. |
| PySpark | `3.5.3` | Latest stable 3.5.x release; compatible with Java 8 and Hadoop 3.x clients. |

**HDFS status:** at the start of this project HDFS was configured (`core-site.xml` points at
`127.0.0.1:9000`) but no NameNode/DataNode process was running (`Connection refused`, confirmed
with `jps` and a port check). The daemons were started during development
(`$HADOOP_HOME/sbin/start-dfs.sh`) and **HDFS mode was verified end-to-end**: `Traffic.csv`
(47.2 MB) was uploaded to `/user/traffic/Traffic.csv` in HDFS and read back by PySpark
(`dataset_source: hdfs://127.0.0.1:9000/user/traffic/Traffic.csv`, 204,000 rows processed
successfully — see `outputs/pipeline_status.json` for the exact run record). The project ships
with `TRAFFIC_MODE=local` as the default in `.env` so the notebook and dashboard work without
requiring the HDFS service to stay running between sessions — switch to `TRAFFIC_MODE=hdfs` and
start the daemons (Section 4) to re-run against real HDFS.

## 2. One-Time Setup (macOS + VS Code)

```bash
# Check versions
java -version
echo $HADOOP_HOME && $HADOOP_HOME/bin/hadoop version
/opt/homebrew/bin/python3.11 --version

# Create and activate the virtual environment (Python 3.11, NOT the system 3.13)
cd /Users/aditi/Desktop/BDA-project
/opt/homebrew/bin/python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Create your local .env from the template
cp .env.example .env
```

Edit `.env` if your paths differ from the defaults (`JAVA_HOME`, `HADOOP_HOME`,
`TRAFFIC_MODE=local|hdfs`).

## 3. Running in Local Mode (no HDFS required)

```bash
source venv/bin/activate
./scripts/run_pipeline.sh          # runs src/pipeline.py, writes outputs/*.csv
./scripts/run_dashboard.sh         # launches Streamlit at http://localhost:8501
```

## 4. Running in HDFS Mode

### 4a. Verify the NameNode address

```bash
cat $HADOOP_HOME/etc/hadoop/core-site.xml   # look for fs.default.name / fs.defaultFS
```

On this machine it is `hdfs://127.0.0.1:9000` (matches `.env`'s `HDFS_NAMENODE_HOST`/`_PORT`).

### 4b. First time only: format the NameNode

**Back up `$HADOOP_HOME/etc/hadoop/` before doing this if you have any doubt.** Only run this if
`dfs.data.dir`'s configured directories (see `hdfs-site.xml`) don't already contain formatted
metadata:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-1.8.jdk/Contents/Home
$HADOOP_HOME/bin/hdfs namenode -format
```

### 4c. Start HDFS and verify

```bash
$HADOOP_HOME/sbin/start-dfs.sh
jps                                   # expect NameNode + DataNode (+ SecondaryNameNode)
$HADOOP_HOME/bin/hadoop fs -ls /      # should succeed, not "Connection refused"
```

Web UI: http://localhost:9870

### 4d. Run the pipeline in HDFS mode

```bash
# in .env, set:
TRAFFIC_MODE=hdfs

source venv/bin/activate
./scripts/run_pipeline.sh
```

This uploads `Traffic.csv` to `hdfs:///user/traffic/Traffic.csv` with the `hadoop fs -put` CLI,
then has PySpark read it back from HDFS and process it — a genuine HDFS round trip. If the
NameNode isn't reachable, the pipeline fails with a clear error and does **not** silently fall
back or claim success.

### 4e. Stop HDFS when done

```bash
$HADOOP_HOME/sbin/stop-dfs.sh
```

## 5. Jupyter Notebook

```bash
source venv/bin/activate
python -m ipykernel install --user --name=bda-traffic --display-name "BDA Traffic (venv)"
jupyter notebook Smart_Traffic_Analysis.ipynb
```

In VS Code: open `Smart_Traffic_Analysis.ipynb`, click the kernel picker (top-right), and select
**"BDA Traffic (venv)"**.

## 6. Streamlit Dashboard

```bash
source venv/bin/activate
streamlit run app.py
```

Run the pipeline (`./scripts/run_pipeline.sh`) at least once first — the dashboard reads its
`outputs/*.csv` files.

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `UnsupportedClassVersionError` / Spark won't start | Wrong JDK picked up | `export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-1.8.jdk/Contents/Home` before running, or set it in `.env` |
| `Connection refused` to `127.0.0.1:9000` | HDFS daemons not running | `$HADOOP_HOME/sbin/start-dfs.sh`, confirm with `jps` |
| `ModuleNotFoundError: pyspark` etc. | venv not activated, or wrong Python used | `source venv/bin/activate`; confirm with `which python` → should point into `venv/` |
| PySpark import errors on Python 3.12/3.13 | Using system Python instead of the 3.11 venv | Recreate venv with `/opt/homebrew/bin/python3.11 -m venv venv` |
| `Address already in use` (Streamlit) | Another Streamlit instance running | `streamlit run app.py --server.port 8502`, or `pkill -f streamlit` |
| Notebook kernel shows wrong Python version | Kernel not registered / wrong one selected | Re-run the `ipykernel install` command above, then re-select the kernel |

## 8. Project Structure

```
BDA-project/
├── app.py                     # Streamlit dashboard
├── requirements.txt
├── .env.example / .env
├── Smart_Traffic_Analysis.ipynb
├── Traffic.csv                 # original dataset (untouched)
├── data/
│   └── data_dictionary.csv
├── src/
│   ├── config.py               # env-driven configuration
│   ├── data_processing.py      # Spark session, HDFS helpers, cleaning
│   └── pipeline.py             # orchestrates load -> clean -> aggregate -> write
├── scripts/
│   ├── check_environment.sh
│   ├── run_pipeline.sh
│   └── run_dashboard.sh
├── docs/
│   ├── ARCHITECTURE.md
│   ├── PROJECT_REPORT.md
│   └── DEMO_GUIDE.md
├── tests/
│   └── test_data_processing.py
└── outputs/                    # generated aggregation CSVs + pipeline_status.json
```

## 9. Dataset Attribution

No license or source documentation accompanied `Traffic.csv` in the workspace. Its provenance
cannot be verified and is not claimed here. See `data/data_dictionary.csv` for the full column
list and this caveat restated.
