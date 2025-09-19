# ---------- MUST come before importing streamlit/transformers ----------
# Put every cache/config in a writable place on Spaces/Docker
import os
os.environ.setdefault("HOME", "/tmp")
os.environ.setdefault("XDG_CACHE_HOME", "/tmp/.cache")
os.environ.setdefault("HF_HOME", "/tmp/.cache/hf")
os.environ.setdefault("HF_HUB_CACHE", "/tmp/.cache/hf/hub")
os.environ.setdefault("TRANSFORMERS_CACHE", "/tmp/.cache/transformers")
os.environ.setdefault("TORCH_HOME", "/tmp/.cache/torch")
os.environ.setdefault("STREAMLIT_BROWSER_GATHER_USAGE_STATS", "false")
os.environ.setdefault("STREAMLIT_USER_SETTINGS_DIR", "/tmp/.streamlit")
for d in [
    os.environ["XDG_CACHE_HOME"],
    os.environ["HF_HOME"],
    os.environ["HF_HUB_CACHE"],
    os.environ["TRANSFORMERS_CACHE"],
    os.environ["TORCH_HOME"],
    os.environ["STREAMLIT_USER_SETTINGS_DIR"],
]:
    os.makedirs(d, exist_ok=True)
# ----------------------------------------------------------------------

import re
import requests
import pandas as pd
import duckdb
import streamlit as st

# ---------- App config ----------
st.set_page_config(page_title="🛒 Marketplace Intelligence", layout="wide")
TABLE = "daily_product_sales"
COLS = ["product_title", "category", "day", "units", "revenue"]
SCHEMA_TEXT = (
    "Table daily_product_sales(product_title TEXT, category TEXT, "
    "day DATE, units INT, revenue DOUBLE)."
)

# ---------- Data ----------
@st.cache_data
def load_df() -> pd.DataFrame:
    """Load CSV; force day as DATE. Generate tiny fallback if missing."""
    path = "data/daily_product_sales.csv"
    if not os.path.exists(path):
        # tiny fallback so the Space still runs
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
    df["day"] = pd.to_datetime(df["day"]).dt.date  # ensure DATE (not timestamp)
    return df[["product_title", "category", "day", "units", "revenue"]]


def run_local(sql: str) -> pd.DataFrame:
    con = duckdb.connect()
    con.register(TABLE, load_df())
    return con.execute(sql).df()


# ---------- Heuristic NL → SQL with window patterns ----------
def parse_nl_to_sql(q: str) -> str:
    ql = q.lower()
    topn = 5
    for tok in q.split():
        if tok.isdigit():
            topn = int(tok)
            break

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

    # rolling 7d
    if "rolling" in ql and ("7" in ql or "7d" in ql):
        return f"""
SELECT day, category,
       SUM(revenue) AS revenue_day,
       SUM(SUM(revenue)) OVER (
         PARTITION BY category
         ORDER BY day
         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) AS revenue_7d
FROM {TABLE}
{where_sql}
GROUP BY day, category
ORDER BY day ASC;
"""

    # cumulative
    if "cumulative" in ql or "running total" in ql:
        return f"""
SELECT day, category,
       SUM(revenue) AS revenue_day,
       SUM(SUM(revenue)) OVER (
         PARTITION BY category
         ORDER BY day
       ) AS revenue_cum
FROM {TABLE}
{where_sql}
GROUP BY day, category
ORDER BY day ASC;
"""

    # rank / top by category
    if "rank" in ql or "top" in ql:
        return f"""
WITH g AS (
  SELECT product_title, category,
         SUM(revenue) AS revenue,
         ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(revenue) DESC) AS rnk
  FROM {TABLE}
  {where_sql}
  GROUP BY 1,2
)
SELECT * FROM g
{"WHERE rnk <= " + str(topn) if topn else ""}
ORDER BY revenue DESC;
"""

    # default aggregate
    return f"""
SELECT product_title, category, SUM(units) AS units, SUM(revenue) AS revenue
FROM {TABLE}
{where_sql}
GROUP BY 1,2
ORDER BY revenue DESC
LIMIT {topn};
"""


# ---------- Manual SQL safety ----------
def sanitize_sql(sql: str) -> str:
    s = sql.strip().strip(";")
    if not re.match(r"(?is)^\s*select\b", s):
        raise ValueError("Only SELECT queries are allowed.")
    banned = r"(?is)\b(drop|delete|update|insert|merge|create|alter|truncate|attach|copy|vacuum)\b"
    if re.search(banned, s):
        raise ValueError("Dangerous statement blocked.")
    if TABLE not in s.lower():
        raise ValueError(f"Query must reference '{TABLE}'.")
    return s


# ---------- Lazy LLM loader ----------
@st.cache_resource
def get_llm():
    try:
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, pipeline
    except Exception as e:
        raise RuntimeError(
            "LLM deps missing. Add to requirements: transformers, sentencepiece, torch (CPU)."
        ) from e
    model_id = "google/flan-t5-small"  # free, public
    tok = AutoTokenizer.from_pretrained(model_id)
    mdl = AutoModelForSeq2SeqLM.from_pretrained(model_id)
    return pipeline("text2text-generation", model=mdl, tokenizer=tok)


# ---------- UI ----------
st.title("🛒 Marketplace Intelligence — NL→SQL (Heuristic / LLM) or Raw SQL")
mode = st.radio("Engine", ["Local Heuristic", "LLM (flan-t5-small)", "SQL (manual)"])
q = st.text_input(
    "Ask or write SQL",
    value="Top 3 selling electronics products in Q3 (rolling 7d also supported)",
)

if st.button("Run"):
    try:
        if mode == "SQL (manual)":
            sql = sanitize_sql(q)
            st.subheader("SQL")
            st.code(sql, language="sql")
            df = run_local(sql)
            st.subheader("Results")
            st.dataframe(df, width="stretch")
            if set(["product_title", "revenue"]).issubset(df.columns):
                st.bar_chart(df.set_index("product_title")["revenue"])

        elif mode == "LLM (flan-t5-small)":
            prompt = (
                f"Generate DuckDB SQL for this question ONLY using {SCHEMA_TEXT} "
                "and data from year 2024. Return ONLY the SQL without explanation.\nQ: " + q
            )
            out = get_llm()(prompt, max_new_tokens=180)[0]["generated_text"]
            sel = out.lower().find("select")
            if sel < 0:
                raise ValueError("LLM did not return SQL starting with SELECT.")
            sql = sanitize_sql(out[sel:])
            st.subheader("SQL (from LLM)")
            st.code(sql, language="sql")
            df = run_local(sql)
            st.subheader("Results")
            st.dataframe(df, width="stretch")
            if set(["product_title", "revenue"]).issubset(df.columns):
                st.bar_chart(df.set_index("product_title")["revenue"])

        else:  # Local Heuristic
            sql = parse_nl_to_sql(q)
            st.subheader("SQL (heuristic)")
            st.code(sql, language="sql")
            df = run_local(sql)
            st.subheader("Results")
            st.dataframe(df, width="stretch")
            if set(["product_title", "revenue"]).issubset(df.columns):
                st.bar_chart(df.set_index("product_title")["revenue"])

        st.caption("Citations: daily_product_sales")

    except Exception as e:
        st.error(f"{type(e).__name__}: {e}")