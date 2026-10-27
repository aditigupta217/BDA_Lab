#!/usr/bin/env bash
# Runs the PySpark pipeline in the mode set by TRAFFIC_MODE in .env
# (defaults to "local" if no .env is present).
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

export JAVA_HOME="${JAVA_HOME:-/Library/Java/JavaVirtualMachines/jdk-1.8.jdk/Contents/Home}"

if [ ! -d venv ]; then
  echo "venv not found. Run: python3.11 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

source venv/bin/activate
python -m src.pipeline
