#!/usr/bin/env bash
set -euxo pipefail

export PORT="${PORT:-7860}"
echo "PORT=$PORT  AGENT_API_URL=${AGENT_API_URL:-unset}"

# Start FastAPI (agent) on 7861
uvicorn api.main:app --host 0.0.0.0 --port 7861 &

# Start Gradio UI on $PORT (no Streamlit)
if [ -f "ui/gradio_app.py" ]; then
  exec python -m ui.gradio_app
else
  # fallback if you kept Gradio UI in app.py
  exec python app.py
fi