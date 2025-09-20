# ---------- set writable caches BEFORE importing gradio/transformers ----------
import os
os.environ.setdefault("HOME", "/tmp")
os.environ.setdefault("XDG_CACHE_HOME", "/tmp/.cache")
os.environ.setdefault("HF_HOME", "/tmp/.cache/hf")
os.environ.setdefault("HF_HUB_CACHE", "/tmp/.cache/hf/hub")
os.environ.setdefault("TRANSFORMERS_CACHE", "/tmp/.cache/transformers")
os.environ.setdefault("TORCH_HOME", "/tmp/.cache/torch")
for d in [
    os.environ["XDG_CACHE_HOME"],
    os.environ["HF_HOME"],
    os.environ["HF_HUB_CACHE"],
    os.environ["TRANSFORMERS_CACHE"],
    os.environ["TORCH_HOME"],
]:
    os.makedirs(d, exist_ok=True)

import re
import pandas as pd
import duckdb
import gradio as gr
from functools import lru_cache

SCHEMA_TEXT = (
    "Table daily_product_sales(product_title TEXT, category TEXT, "
    "day DATE, units INT, revenue DOUBLE)."
)

# ----------------------------- data loading -----------------------------
@lru_cache(maxsize=None)
def load_df():
    """Load CSV; ensure 'day' is DATE; create tiny fallback if missing."""
    path = "data/daily_product_sales.csv"
    if not os.path.exists(path):
        import numpy as np
        os.makedirs("data", exist_ok=True)
        rng = np.random.default_rng(7)
        cats = ["electronics", "home", "beauty", "sports", "toys"]
        prods = pd.DataFrame({"product_id": range(1, 21)})
        prods["product_title"] = [f"Product {i}" for i in range(1, 21)]
        prods["category"] = rng.choice(cats, len(prods))
        prods["price"] = np.round(rng.gamma(4, 20, len(prods)) + 5, 2)
        orders = []
        for oid in range(1, 501):
            pid = int(rng.integers(1, 21))
            qty = int(rng.integers(1, 4))
            ts = pd.Timestamp("2024-01-01") + pd.to_timedelta(int(rng.integers(0, 365)), "D")
            orders.append({"order_id": oid, "product_id": pid, "qty": qty, "ts": ts})
        orders = pd.DataFrame(orders)
        tmp = orders.merge(prods, on="product_id")
        tmp["day"] = pd.to_datetime(tmp["ts"]).dt.floor("D")
        tmp["revenue"] = tmp["qty"] * tmp["price"]
        daily = (
            tmp.groupby(["product_id", "product_title", "category", "day"], as_index=False)
            .agg(units=("qty", "sum"), revenue=("revenue", "sum"))
        )
        daily.to_csv(path, index=False)

    df = pd.read_csv(path, parse_dates=["day"])
    df["day"] = pd.to_datetime(df["day"]).dt.date
    return df[["product_title", "category", "day", "units", "revenue"]]

def run_local(sql):
    con = duckdb.connect()
    con.register("daily_product_sales", load_df())
    return con.execute(sql).df()

# --------------------------- heuristic NL → SQL --------------------------
def parse_nl_to_sql(q):
    ql = q.lower()
    topn = 5
    for tok in q.split():
        if tok.isdigit():
            topn = int(tok); break

    cat = next((c for c in ["electronics", "home", "beauty", "sports", "toys"] if c in ql), None)
    quarter = next((f"Q{n}" for n in [1, 2, 3, 4] if f"q{n}" in ql), None)

    def qrange(qr):
        return {
            "Q1": ("2024-01-01", "2024-03-31"),
            "Q2": ("2024-04-01", "2024-06-30"),
            "Q3": ("2024-07-01", "2024-09-30"),
            "Q4": ("2024-10-01", "2024-12-31"),
        }[qr]

    where = []
    if cat:
        where.append(f"category = '{cat}'")
    if quarter:
        lo, hi = qrange(quarter)
        where.append(f"CAST(day AS DATE) BETWEEN DATE '{lo}' AND DATE '{hi}'")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    if "rolling" in ql and ("7" in ql or "7d" in ql):
        return f"""
SELECT day, category,
       SUM(revenue) AS revenue_day,
       SUM(SUM(revenue)) OVER (
         PARTITION BY category
         ORDER BY day
         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) AS revenue_7d
FROM daily_product_sales
{where_sql}
GROUP BY day, category
ORDER BY day ASC;
"""
    if "cumulative" in ql or "running total" in ql:
        return f"""
SELECT day, category,
       SUM(revenue) AS revenue_day,
       SUM(SUM(revenue)) OVER (
         PARTITION BY category
         ORDER BY day
       ) AS revenue_cum
FROM daily_product_sales
{where_sql}
GROUP BY day, category
ORDER BY day ASC;
"""
    if "rank" in ql or "top" in ql:
        return f"""
WITH g AS (
  SELECT product_title, category,
         SUM(revenue) AS revenue,
         ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(revenue) DESC) AS rnk
  FROM daily_product_sales
  {where_sql}
  GROUP BY 1,2
)
SELECT * FROM g
{"WHERE rnk <= " + str(topn) if topn else ""}
ORDER BY revenue DESC;
"""
    return f"""
SELECT product_title, category, SUM(units) AS units, SUM(revenue) AS revenue
FROM daily_product_sales
{where_sql}
GROUP BY 1,2
ORDER BY revenue DESC
LIMIT {topn};
"""

# ---------------------------- SQL safety helpers -------------------------
def extract_sql(text):
    m = re.search(r"```sql\s*(.*?)```", text, flags=re.I | re.S)
    if m: return m.group(1).strip().rstrip(";")
    m = re.search(r"```(.*?)```", text, flags=re.S)
    if m: text = m.group(1)
    m = re.search(r"(?is)\b(select|with)\b.*", text)
    return m.group(0).strip() if m else text.strip()

def enforce_table(sql, table="daily_product_sales"):
    s = sql.strip().strip(";")
    low = s.lower()
    if table in low:
        return s
    m = re.search(r"(?is)\bfrom\s+([a-zA-Z_][\w\.\"`]*)\s*(?:as\s+([a-zA-Z_]\w*))?", s)
    if m and not m.group(1).strip().startswith("("):
        alias = m.group(2) or ""
        prefix, suffix = s[:m.start()], s[m.end():]
        return f"{prefix}FROM {table} {alias} {suffix}".strip()
    cut = len(s)
    for kw in [" where ", "\nwhere ", " group ", "\ngroup ", " order ", "\norder ", " limit ", "\nlimit "]:
        p = low.find(kw)
        if p != -1:
            cut = min(cut, p)
    return (s[:cut] + f" FROM {table} " + s[cut:]).strip()

def sanitize_sql(sql):
    s = sql.strip().strip(";")
    low = s.lower()
    if not (low.startswith("select") or low.startswith("with")):
        raise ValueError("Only SELECT/CTE queries are allowed.")
    banned = r"(?is)\b(drop|delete|update|insert|merge|create|alter|truncate|attach|copy|vacuum)\b"
    if re.search(banned, low):
        raise ValueError("Dangerous statement blocked.")
    if "daily_product_sales" not in low:
        raise ValueError("Query must reference 'daily_product_sales'.")
    return s

# --------------------------- lazy LLM loader -----------------------------
@lru_cache(maxsize=None)
def get_llm():
    try:
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, pipeline
    except Exception as e:
        raise RuntimeError(
            "LLM deps missing. Add to requirements: transformers, sentencepiece, torch (CPU)."
        ) from e
    model_id = "google/flan-t5-small"
    tok = AutoTokenizer.from_pretrained(model_id)
    mdl = AutoModelForSeq2SeqLM.from_pretrained(model_id)
    return pipeline("text2text-generation", model=mdl, tokenizer=tok)

# --------------------------------- UI -----------------------------------
def run_query(mode, q):
    try:
        if mode == "SQL (manual)":
            if not re.match(r"(?is)^\s*(select|with)\b", q.strip()):
                gr.Info("Detected natural language — routing to heuristic.")
                sql = parse_nl_to_sql(q)
            else:
                sql = enforce_table(q)
                sql = sanitize_sql(sql)
            df = run_local(sql)
            return sql, df.to_html(), "Citations: daily_product_sales"

        elif mode == "LLM (flan-t5-small)":
            prompt = (
                f"Return ONLY DuckDB SQL (no prose/backticks). "
                f"Start with SELECT or WITH. Use {SCHEMA_TEXT} and year 2024.\nQ: {q}"
            )
            gen = get_llm()(prompt, max_new_tokens=180, do_sample=False, num_beams=1)[0]["generated_text"]
            sql = extract_sql(gen)
            sql = enforce_table(sql)
            try:
                sql = sanitize_sql(sql)
            except Exception:
                prompt2 = "ONLY SQL. FIRST CHAR MUST BE S or W.\n" + prompt
                gen2 = get_llm()(prompt2, max_new_tokens=160, do_sample=False, num_beams=1)[0]["generated_text"]
                sql = enforce_table(extract_sql(gen2))
                sql = sanitize_sql(sql)
            df = run_local(sql)
            return sql, df.to_html(), "Citations: daily_product_sales"

        else:  # Local Heuristic
            sql = parse_nl_to_sql(q)
            df = run_local(sql)
            return sql, df.to_html(), "Citations: daily_product_sales"

    except Exception as e:
        return "Error", str(e), ""

iface = gr.Interface(
    fn=run_query,
    inputs=[gr.Radio(["Local Heuristic", "LLM (flan-t5-small)", "SQL (manual)"]), gr.Textbox()],
    outputs=[gr.Code(label="SQL"), gr.HTML(label="Results"), gr.Text(label="Citations")],
    title="🛒 Marketplace Intelligence — NL→SQL",
    description="Query marketplace data with heuristic, LLM, or manual SQL.",
    examples=[["Top 3 selling electronics products in Q3"]],
    allow_flagging="never",
    cache_examples=False,
)
iface.launch(server_name="0.0.0.0", server_port=7860, share=False, debug=True)