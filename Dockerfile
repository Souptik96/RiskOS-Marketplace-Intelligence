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

CMD ["python", "app.py"]