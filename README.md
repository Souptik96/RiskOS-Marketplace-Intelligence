---
title: Marketplace Intelligence
emoji: 🛒
colorFrom: red
colorTo: red
sdk: docker
app_port: 8501
tags:
- streamlit
- duckdb
- nl2sql
pinned: false
short_description: NL → see SQL + results + citations.
license: mit
---

# Marketplace Intelligence — Streamlit Demo

Natural-language to SQL over an aggregated daily sales table (`daily_product_sales`), with two modes:
- **Local (built-in data)**: uses the CSV in `data/`.
- **Remote API**: calls your deployed API at `/ask?q=...` (ECS/API GW etc.).

## Run locally
```bash
python -m venv .venv && source .venv/Scripts/activate       # on Windows Git Bash
pip install -r requirements.txt
# ensure CSV exists under data/daily_product_sales.csv (see below)
streamlit run streamlit_app.py