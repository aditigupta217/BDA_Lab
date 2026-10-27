#!/usr/bin/env bash
# Launches the Streamlit dashboard. Run scripts/run_pipeline.sh first so the
# aggregated output CSVs exist.
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

if [ ! -d venv ]; then
  echo "venv not found. Run: python3.11 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

source venv/bin/activate
# --server.headless avoids Streamlit's first-run "send usage stats?" prompt,
# which otherwise blocks startup waiting for Enter in the terminal.
streamlit run app.py --server.headless true
