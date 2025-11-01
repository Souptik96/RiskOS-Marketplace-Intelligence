#!/bin/bash

# Start both Streamlit and FastAPI services
# FastAPI (Agent Backend) on port 7861
uvicorn api.main:app --host 0.0.0.0 --port 7861 &

# Streamlit UI on port 7860
streamlit run app.py --server.port 7860 --server.address 0.0.0.0

# Wait for all background processes
wait
