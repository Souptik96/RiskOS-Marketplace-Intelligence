import os, re, requests, pandas as pd, duckdb, streamlit as st

st.set_page_config(page_title="🛒 Marketplace Intelligence", layout="wide")
API_URL = os.getenv("API_URL")

TABLE = "daily_product_sales"
COLS = ["product_title","category","day","units","revenue"]
SCHEMA_TEXT = "Table daily_product_sales(product_title TEXT, category TEXT, day DATE, units INT, revenue DOUBLE)."

@st.cache_data
def load_df():
    df = pd.read_csv("data/daily_product_sales.csv", parse_dates=["day"])
    df["day"] = df["day"].dt.date  # ensure DATE not TIMESTAMP
    return df

@st.cache_resource
def get_llm():
    # loaded only if LLM mode is selected; free/public model
    from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, pipeline
    model_id = "google/flan-t5-base"
    tok = AutoTokenizer.from_pretrained(model_id)
    mdl = AutoModelForSeq2SeqLM.from_pretrained(model_id)
    return pipeline("text2text-generation", model=mdl, tokenizer=tok)

def sanitize_sql(sql: str) -> str:
    s = sql.strip().strip(";")
    if not re.match(r"(?is)^\s*select\b", s): raise ValueError("Only SELECT allowed")
    bad = re.compile(r"(?is)\b(drop|delete|update|insert|create|alter|truncate|attach|copy)\b")
    if bad.search(s): raise ValueError("Dangerous statement blocked")
    if TABLE not in s.lower(): raise ValueError("Must query daily_product_sales")
    for ident in re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", s):
        if ident.lower() in [TABLE] or ident.lower() in [c.lower() for c in COLS] \
           or ident.upper() in ["SELECT","FROM","WHERE","GROUP","BY","ORDER","LIMIT",
                                "SUM","COUNT","AVG","ROW_NUMBER","OVER","PARTITION",
                                "RANK","DENSE_RANK","ROWS","RANGE","BETWEEN","AND",
                                "CURRENT","PRECEDING","ASC","DESC","CAST","DATE"]:
            continue
    return s

def parse_nl_to_sql(q: str) -> str:
    ql = q.lower()
    topn = 5
    for tok in q.split():
        if tok.isdigit(): topn = int(tok); break
    cat = next((c for c in ["electronics","home","beauty","sports","toys"] if c in ql), None)
    quarter = next((f"Q{n}" for n in [1,2,3,4] if f"q{n}" in ql), None)

    def qrange(qr):
        return {"Q1":("2024-01-01","2024-03-31"),
                "Q2":("2024-04-01","2024-06-30"),
                "Q3":("2024-07-01","2024-09-30"),
                "Q4":("2024-10-01","2024-12-31")}[qr]

    where = []
    if cat: where.append(f"category = '{cat}'")
    if quarter:
        lo, hi = qrange(quarter)
        where.append(f"CAST(day AS DATE) BETWEEN DATE '{lo}' AND DATE '{hi}'")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # --- window patterns ---
    if "rolling" in ql and ("7" in ql or "7d" in ql):
        # rolling 7-day revenue by category
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
{f"WHERE rnk <= {topn}" if topn else ""}
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

def run_local(sql: str) -> pd.DataFrame:
    con = duckdb.connect()
    con.register(TABLE, load_df())
    return con.execute(sql).df()

st.title("🛒 Marketplace Intelligence — NL→SQL (Heuristic / LLM) or Raw SQL")
mode = st.radio("Engine", ["Local Heuristic", "LLM (flan-t5-base)", "SQL (manual)"])
q = st.text_input("Ask or write SQL",
                  value="Top 3 selling electronics products in Q3 (rolling 7d also supported)")

if st.button("Run"):
    try:
        if mode == "SQL (manual)":
            sql = sanitize_sql(q)
            st.code(sql, language="sql")
            st.dataframe(run_local(sql), use_container_width=True)

        elif mode == "LLM (flan-t5-base)":
            prompt = (f"Generate DuckDB SQL for this question ONLY using {SCHEMA_TEXT}. "
                      "Return ONLY the SQL without explanation.\nQ: " + q)
            out = get_llm()(prompt, max_new_tokens=160)[0]["generated_text"]
            sql = sanitize_sql(out[out.lower().find("select"):])  # crude extract
            st.code(sql, language="sql")
            st.dataframe(run_local(sql), use_container_width=True)

        else:  # Local Heuristic
            sql = parse_nl_to_sql(q)
            st.code(sql, language="sql")
            st.dataframe(run_local(sql), use_container_width=True)

    except Exception as e:
        st.error(f"{type(e).__name__}: {e}")