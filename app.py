import os, io, re, json
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import duckdb
import chardet
import requests
import gradio as gr
from dotenv import load_dotenv
from api.inference import _call_llm

load_dotenv()

# ---- Globals ----
DFS: Dict[str, pd.DataFrame] = {}
CON: Optional[duckdb.DuckDBPyConnection] = None
AGENT_API_URL = os.getenv("AGENT_API_URL", "http://localhost:7861")
PROVIDER = (os.getenv("LLM_PROVIDER") or "fireworks").lower()
FW_MODEL = os.getenv("FIREWORKS_MODEL_ID", "accounts/fireworks/models/gpt-oss-20b")


def _sanitize(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).lower().strip("_") or "table"


def _load_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    name = filename.lower()
    buf = io.BytesIO(file_bytes)
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(buf)
    if name.endswith(".jsonl"):
        buf.seek(0)
        return pd.read_json(buf, lines=True)
    if name.endswith((".txt", ".tsv")):
        buf.seek(0)
        return pd.read_csv(buf, sep=None, engine="python")
    try:
        buf.seek(0)
        return pd.read_csv(buf, sep=None, engine="python")
    except Exception:
        buf.seek(0)
        enc = chardet.detect(file_bytes).get("encoding") or "utf-8"
        return pd.read_csv(buf, sep=None, engine="python", encoding=enc)


def _register_tables(dfs: Dict[str, pd.DataFrame]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    for t, df in dfs.items():
        con.register(t, df)
    return con


def _schema_for_prompt(con: Optional[duckdb.DuckDBPyConnection]) -> str:
    if con is None:
        return ""
    rows = con.execute(
        """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='main'
        ORDER BY table_name, ordinal_position
        """
    ).fetchall()
    layout: Dict[str, List[str]] = {}
    for table, col, dtype in rows:
        layout.setdefault(table, []).append(f"{col} {dtype}")
    return "\n".join([f"Table {t}({', '.join(cols)})." for t, cols in layout.items()])


def _enforce_limits(sql: str) -> str:
    stmt = (sql or "").split(";")[0].strip()
    if not stmt:
        raise ValueError("Empty SQL")
    lo = stmt.lower()
    if not (lo.startswith("select") or lo.startswith("with")):
        raise ValueError("Only SELECT/CTE statements are allowed.")
    if " limit " not in f" {lo} ":
        stmt += " LIMIT 200"
    return stmt


def _extract_sql_from_text(text: str) -> str:
    fenced = re.search(r"```sql\s*(.*?)```", text, flags=re.I | re.S)
    if fenced:
        return fenced.group(1).strip()
    fallback = re.search(r"(?is)\b(select|with)\b.*", text)
    return (fallback.group(0).strip() if fallback else text.strip())


def upload_files(files: List[gr.File]) -> Tuple[str, str]:
    global DFS, CON
    DFS = {}
    for f in files or []:
        with open(f.name, "rb") as fh:
            data = fh.read()
        df = _load_file(data, os.path.basename(f.name))
        t = _sanitize(os.path.basename(f.name).rsplit(".", 1)[0])
        while t in DFS:
            t += "_u"
        DFS[t] = df
    CON = _register_tables(DFS) if DFS else None
    schema = _schema_for_prompt(CON)
    return (", ".join(DFS.keys()) or "none"), (schema or "No tables registered.")


def gen_sql_and_run(question: str) -> Tuple[str, pd.DataFrame, str]:
    if not question.strip():
        return "Question empty.", pd.DataFrame(), ""
    schema = _schema_for_prompt(CON)
    prompt = (
        "You convert business questions into DuckDB SQL ONLY.\n"
        f"Use these tables:\n{schema or 'No tables were provided.'}\n"
        "Return only SQL without commentary.\n"
        f"Question: {question}\nSQL:"
    )
    raw = _call_llm(prompt, max_tokens=400, temperature=0.0, model=FW_MODEL, provider=PROVIDER)
    sql = _enforce_limits(_extract_sql_from_text(raw))
    df = pd.DataFrame()
    err = ""
    try:
        df = CON.execute(sql).df() if CON else pd.DataFrame()
        if df.empty:
            err = "Query executed but returned no rows."
    except Exception as e:
        err = str(e)
    return sql, (df.head(200) if not df.empty else pd.DataFrame()), err


def review_sql(question: str, sql: str) -> str:
    schema = _schema_for_prompt(CON)
    prompt = (
        "You are a senior BI reviewer. Assess SQL for intent, correctness, and produce fixes when needed.\n"
        f"Schema:\n{schema or 'No schema provided.'}\n"
        f"Question: {question or 'N/A'}\n"
        f"SQL:\n{sql}\n"
        'Return JSON with keys reasoning, ok (true/false), fixed_sql.'
    )
    try:
        return _call_llm(prompt, max_tokens=400, temperature=0.0, model=FW_MODEL, provider=PROVIDER)
    except Exception as e:
        return json.dumps({"ok": False, "reason": str(e)})


def generate_metric(ask: str, slug: str) -> str:
    try:
        payload = {"ask": ask, "metric_slug": slug or None}
        r = requests.post(f"{AGENT_API_URL}/agent/generate_metric", json=payload, timeout=120)
        r.raise_for_status()
        return json.dumps(r.json(), indent=2)
    except Exception as e:
        return f"ERROR: {e}"


with gr.Blocks(title="Marketplace Intelligence (Gradio)") as demo:
    gr.Markdown("# Marketplace Intelligence — Upload → NL→SQL → Metric")
    with gr.Row():
        files = gr.File(label="Upload CSV/XLSX/TSV/TXT/JSONL (multi)", file_count="multiple")
    with gr.Row():
        btn_up = gr.Button("Register Tables")
        tables_out = gr.Textbox(label="Tables")
        schema_out = gr.Textbox(label="Schema")
    btn_up.click(upload_files, inputs=[files], outputs=[tables_out, schema_out])

    gr.Markdown("## Ask (Business → SQL → Run)")
    q = gr.Textbox(label="Business Question", value="Top 5 electronics products by revenue in Q3")
    btn_run = gr.Button("Generate SQL & Run")
    sql_out = gr.Code(label="Generated SQL", language="sql")
    df_out = gr.Dataframe(label="Preview (first 200 rows)")
    err_out = gr.Textbox(label="Errors / Notes")
    btn_run.click(gen_sql_and_run, inputs=[q], outputs=[sql_out, df_out, err_out])

    gr.Markdown("## Review SQL")
    rq = gr.Textbox(label="Optional context", value="Top 10 beauty products in Q2")
    rsql = gr.Code(label="SQL to review", value="SELECT category, SUM(revenue) AS revenue FROM daily_product_sales GROUP BY category ORDER BY revenue DESC LIMIT 5;", language="sql")
    btn_rev = gr.Button("Review")
    rev_out = gr.Code(label="AI Review (JSON)")
    btn_rev.click(review_sql, inputs=[rq, rsql], outputs=[rev_out])

    gr.Markdown("## Generate Metric (dbt + Dashboard)")
    mask = gr.Textbox(label="Free-text ask", value="Gross margin by category last quarter")
    mslug = gr.Textbox(label="Optional metric slug", value="gross_margin_by_category")
    btn_metric = gr.Button("Generate Metric via Agent")
    metric_out = gr.Code(label="Agent Result (JSON)")
    btn_metric.click(generate_metric, inputs=[mask, mslug], outputs=[metric_out])


def launch():
    port = int(os.getenv("PORT", "7860"))
    demo.launch(server_name="0.0.0.0", server_port=port, show_error=True, inbrowser=False)


if __name__ == "__main__":
    launch()