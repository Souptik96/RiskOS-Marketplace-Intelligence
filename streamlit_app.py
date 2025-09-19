import os
import requests
import pandas as pd
import duckdb
import streamlit as st

st.set_page_config(page_title="🛒 Marketplace Intelligence", layout="wide")

API_URL = os.getenv("API_URL")  # optional; when set + 'Remote API' mode is chosen, we call /ask

st.title("🛒 Marketplace Intelligence — NL → SQL → Results + Citations")
st.caption("Tip: set an environment variable API_URL to point at your deployed API (e.g., ECS/API GW).")

@st.cache_data
def load_df():
    # expects data/daily_product_sales.csv (see CSV content below)
    return pd.read_csv("data/daily_product_sales.csv", parse_dates=["day"])

def parse_nl_to_sql(q: str) -> str:
    """Very small heuristic parser for demo purposes (top N, category, quarter)."""
    ql = q.lower()
    topn = 5
    for tok in q.split():
        if tok.isdigit():
            topn = int(tok)
            break
    cat = None
    for c in ["electronics","home","beauty","sports","toys"]:
        if c in ql:
            cat = c
            break
    quarter = None
    for qx in ["q1","q2","q3","q4"]:
        if qx in ql:
            quarter = qx.upper()
            break
    where = []
    if cat:
        where.append(f"category = '{cat}'")
    if quarter:
        rng = {
            "Q1": ("2024-01-01","2024-03-31"),
            "Q2": ("2024-04-01","2024-06-30"),
            "Q3": ("2024-07-01","2024-09-30"),
            "Q4": ("2024-10-01","2024-12-31"),
        }[quarter]
        where.append(f"day BETWEEN DATE '{rng[0]}' AND DATE '{rng[1]}'")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    return f"""
SELECT product_title, category, SUM(units) AS units, SUM(revenue) AS revenue
FROM daily_product_sales
{where_sql}
GROUP BY 1,2
ORDER BY revenue DESC
LIMIT {topn};
"""

def run_local(sql: str) -> pd.DataFrame:
    con = duckdb.connect()
    con.register("daily_product_sales", load_df())
    return con.execute(sql).df()

mode = st.radio("Engine", ["Local (built-in data)", "Remote API"])
q = st.text_input(
    "Ask a question",
    value="Top 3 selling electronics products in Q3",
    help="Examples: 'Top 5 in Q2 home', 'Top 10 beauty products', 'Top 3 in Q1 electronics'"
)

go = st.button("Ask")

if go:
    if mode == "Remote API":
        if not API_URL:
            st.error("Set API_URL in your environment or as a Space secret to use Remote API.")
        else:
            try:
                r = requests.get(f"{API_URL}/ask", params={"q": q}, timeout=30)
                r.raise_for_status()
                data = r.json()
                st.subheader("Generated SQL")
                st.code(data["sql"], language="sql")
                df = pd.DataFrame(data["rows"])
                if not df.empty:
                    st.subheader("Results")
                    st.dataframe(df, use_container_width=True)
                    st.subheader("Revenue by Product")
                    st.bar_chart(df.set_index("product_title")["revenue"])
                st.caption("Citations: " + ", ".join(data.get("citations", [])))
            except Exception as e:
                st.error(f"Remote API call failed: {e}")
    else:
        # Local mode
        sql = parse_nl_to_sql(q)
        st.subheader("Generated SQL")
        st.code(sql, language="sql")
        try:
            df = run_local(sql)
            if not df.empty:
                st.subheader("Results")
                st.dataframe(df, use_container_width=True)
                st.subheader("Revenue by Product")
                st.bar_chart(df.set_index("product_title")["revenue"])
            st.caption("Citations: daily_product_sales")
        except Exception as e:
            st.error(f"Query failed: {e}")