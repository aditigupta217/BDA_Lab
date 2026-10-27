#!/usr/bin/env bash
# Verifies Java, Hadoop, Python and the project virtualenv before running
# anything. Read-only -- makes no changes to your system.
set -uo pipefail

echo "== Java =="
java -version 2>&1 || echo "Java not found on PATH"
echo "JAVA_HOME (recommended): /Library/Java/JavaVirtualMachines/jdk-1.8.jdk/Contents/Home"

echo
echo "== Hadoop =="
if [ -n "${HADOOP_HOME:-}" ]; then
  "$HADOOP_HOME/bin/hadoop" version 2>&1 | head -1
else
  echo "HADOOP_HOME not set in this shell. Expected (per .env): /Users/aditi/Downloads/hadoop-3.4.3"
fi

echo
echo "== HDFS NameNode reachability (127.0.0.1:9000) =="
if nc -z -w2 127.0.0.1 9000 2>/dev/null; then
  echo "NameNode is reachable."
else
  echo "NameNode NOT reachable. Start it with: \$HADOOP_HOME/sbin/start-dfs.sh"
  echo "(First time only: hdfs namenode -format)"
fi

echo
echo "== Python / venv =="
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -d "$PROJECT_DIR/venv" ]; then
  "$PROJECT_DIR/venv/bin/python" --version
  "$PROJECT_DIR/venv/bin/python" -c "import pyspark; print('pyspark', pyspark.__version__)" 2>&1
else
  echo "venv not found at $PROJECT_DIR/venv -- run: python3.11 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
fi
