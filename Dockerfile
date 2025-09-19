# Minimal Docker image for Hugging Face Space or local container runs
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app and data
COPY . .

# Fallback: generate a tiny dataset if data/daily_product_sales.csv is missing
RUN python - <<'PY'
import os, pandas as pd, numpy as np
os.makedirs('data', exist_ok=True)
p = 'data/daily_product_sales.csv'
if not os.path.exists(p):
    rng = np.random.default_rng(7)
    cats = ["electronics","home","beauty","sports","toys"]
    products = pd.DataFrame({"product_id": range(1,21)})
    products["product_title"] = [f"Product {i}" for i in range(1,21)]
    products["category"] = rng.choice(cats, len(products))
    products["price"] = np.round(rng.gamma(4, 20, len(products))+5, 2)
    orders = []
    for oid in range(1,501):
        pid = int(rng.integers(1,21))
        qty = int(rng.integers(1,4))
        ts = pd.Timestamp("2024-01-01") + pd.to_timedelta(int(rng.integers(0,365)), unit="D")
        orders.append({"order_id": oid, "product_id": pid, "qty": qty, "ts": ts})
    orders = pd.DataFrame(orders)
    tmp = orders.merge(products, on="product_id")
    tmp["day"] = pd.to_datetime(tmp["ts"]).dt.floor("D")
    tmp["revenue"] = tmp["qty"] * tmp["price"]
    daily = (tmp.groupby(["product_id","product_title","category","day"], as_index=False)
               .agg(units=("qty","sum"), revenue=("revenue","sum")))
    daily.to_csv(p, index=False)
    print("Generated fallback", p)
PY

EXPOSE 8501
CMD ["streamlit", "run", "streamlit_app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]