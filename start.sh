#!/usr/bin/env bash
set -euxo pipefail

export PORT="${PORT:-7860}"
export STREAMLIT_BROWSER_GATHERUSAGESTATS=false

echo "PORT=$PORT  AGENT_API_URL=${AGENT_API_URL:-unset}"

# Start FastAPI (don’t crash UI if uvicorn missing)
if command -v uvicorn >/dev/null 2>&1; then
  uvicorn api.main:app --host 0.0.0.0 --port 7861 &
else
  echo "WARN: uvicorn not found; skipping agent API"
fi

# Start Streamlit on $PORT (HF health check)
exec python -m streamlit run app.py --server.address 0.0.0.0 --server.port "$PORT"