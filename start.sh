#!/bin/bash
set -e
PORT=${PORT:-7860}
export STREAMLIT_SERVER_HEADLESS=true
export STREAMLIT_SERVER_ADDRESS=0.0.0.0
export STREAMLIT_SERVER_PORT=$PORT
export STREAMLIT_SERVER_ENABLECORS=false
export STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=false
streamlit run app.py \
  --server.address=0.0.0.0 \
  --server.port=$PORT \
  --server.headless=true \
  --server.enableCORS=false \
  --server.enableXsrfProtection=false
