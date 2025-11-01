FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Ensure dbt packages are explicitly installed
RUN pip install --no-cache-dir dbt-core>=1.7 dbt-duckdb>=1.7

COPY . .

ENV HOME=/tmp
ENV XDG_CACHE_HOME=/tmp/.cache
ENV HF_HOME=/tmp/.cache/hf
ENV HF_HUB_CACHE=/tmp/.cache/hf/hub
ENV TRANSFORMERS_CACHE=/tmp/.cache/transformers
ENV TORCH_HOME=/tmp/.cache/torch
ENV LLM_PROVIDER=fireworks
ENV LLM_MODEL_GEN=accounts/fireworks/models/qwen3-coder-30b-a3b-instruct
ENV LLM_MODEL_REV=accounts/fireworks/models/qwen3-coder-30b-a3b-instruct
ENV AGENT_API_URL=http://localhost:7861
ENV DBT_PROFILES_DIR=./dbt_project/profiles

RUN mkdir -p $XDG_CACHE_HOME $HF_HOME $HF_HUB_CACHE $TRANSFORMERS_CACHE $TORCH_HOME

# Pre-download model with error handling and debug
RUN python -c "import sys; sys.stderr = open('/dev/null', 'w'); from transformers import AutoModelForSeq2SeqLM, AutoTokenizer; AutoModelForSeq2SeqLM.from_pretrained('google/flan-t5-small'); AutoTokenizer.from_pretrained('google/flan-t5-small')" || echo "Model pre-download failed, will lazy-load at runtime"

# Make start script executable
COPY start.sh .
RUN chmod +x start.sh

# Expose both Streamlit and FastAPI ports
EXPOSE 7860 7861

# Use the start script to run both services
CMD ["./start.sh"]
