FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV HOME=/tmp
ENV XDG_CACHE_HOME=/tmp/.cache
ENV HF_HOME=/tmp/.cache/hf
ENV HF_HUB_CACHE=/tmp/.cache/hf/hub
ENV TRANSFORMERS_CACHE=/tmp/.cache/transformers
ENV TORCH_HOME=/tmp/.cache/torch

RUN mkdir -p $XDG_CACHE_HOME $HF_HOME $HF_HUB_CACHE $TRANSFORMERS_CACHE $TORCH_HOME

# Pre-download model with error handling and debug
RUN python -c "import sys; sys.stderr = open('/dev/null', 'w'); from transformers import AutoModelForSeq2SeqLM, AutoTokenizer; AutoModelForSeq2SeqLM.from_pretrained('google/flan-t5-small'); AutoTokenizer.from_pretrained('google/flan-t5-small')" || echo "Model pre-download failed, will lazy-load at runtime"

CMD ["bash", "-lc", "PORT=${PORT:-8501}; SCR_PORT=$PORT; STREAMLIT_SERVER_HEADLESS=true STREAMLIT_SERVER_ADDRESS=0.0.0.0 STREAMLIT_SERVER_PORT=$SCR_PORT STREAMLIT_SERVER_ENABLECORS=false STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=false streamlit run app.py --server.address=0.0.0.0 --server.port=$SCR_PORT --server.headless=true --server.enableCORS=false --server.enableXsrfProtection=false"]
