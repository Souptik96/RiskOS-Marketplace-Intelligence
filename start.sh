#!/usr/bin/env bash
set -euo pipefail

export PORT="${PORT:-7860}"           # HF sets PORT; default to 7860 locally
export STREAMLIT_BROWSER_GATHERUSAGESTATS=false

# 1) Start FastAPI (agent) on 7861
uvicorn api.main:app --host 0.0.0.0 --port 7861 &

# 2) Start Streamlit on $PORT (must be 7860 on HF)
exec python -m streamlit run app.py --server.address 0.0.0.0 --server.port "$PORT"