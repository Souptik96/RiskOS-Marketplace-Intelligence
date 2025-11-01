import os
import time
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Any

from .providers import extract_json, extract_sql, llm_call
from .sql_safety import sanitize

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("bizsql")

DATA_CSV = os.getenv("DATA_CSV", "data/daily_product_sales.csv")

SCHEMA = (
    "Table daily_product_sales("
    "product_title TEXT, category TEXT, day DATE, units INT, revenue DOUBLE). "
    "Use DATE functions like date_trunc('quarter', day). "
    "For date literals, use CAST('YYYY-MM-DD' AS DATE). "
    "Assume current year (2024) for quarters like Q3 (July-Sep), e.g., CAST('2024-07-01' AS DATE) for Q3 start."
)

PROMPT_GEN = '''You are a SQL expert. Convert this business question to DuckDB SQL. Output ONLY the SQL query.
- Use column names exactly: product_title, category, day, units, revenue
- Use DATE functions like date_trunc('quarter', day).
- For date literals, use CAST('YYYY-MM-DD' AS DATE).
- Assume current year (2024) for quarters like Q3 (July-Sep), e.g., CAST('2024-07-01' AS DATE) for Q3 start.
- If aggregation is implied, aggregate and sort appropriately.
Schema: {schema}
Question: {q}
SQL:'''

PROMPT_REV = '''You are a senior BI reviewer. Given schema, question and SQL:
1) Check intent & correctness (filters, groupings, windows).
2) List issues (if any).
3) Provide corrected SQL if needed.
Return JSON with keys: reasoning, ok (true/false), fixed_sql.
Schema: {schema}
Question: {q}
SQL:
{sql}
JSON:'''

CATEGORY_TOKENS = ["electronics", "home", "beauty", "sports", "toys"]
QUARTER_WINDOWS = {
    "Q1": ("2024-01-01", "2024-03-31"),
    "Q2": ("2024-04-01", "2024-06-30"),
    "Q3": ("2024-07-01", "2024-09-30"),
    "Q4": ("2024-10-01", "2024-12-31"),
}

app = FastAPI(title="BizSQL API", version="0.2")


class GenRequest(BaseModel):
    q: str


class ReviewRequest(BaseModel):
    q: str
    sql: str


class ExecResponse(BaseModel):
    sql: str
    rows: List[Dict[str, Any]]
    review: Dict[str, Any]


def _con():
    df = pd.read_csv(DATA_CSV, parse_dates=["day"])
    df["day"] = df["day"].dt.date
    con = duckdb.connect()
    con.register("daily_product_sales", df)
    return con


def _is_sql(text: str) -> bool:
    stripped = text.strip().lower()
    return stripped.startswith("select") or stripped.startswith("with")


def _fallback_sql_from_question(q: str) -> str:
    ql = q.lower()
    topn = 5
    for tok in q.split():
        if tok.isdigit():
            topn = int(tok)
            break
    category = next((c for c in CATEGORY_TOKENS if c in ql), None)
    quarter = next((qt for qt in QUARTER_WINDOWS if qt.lower() in ql), None)

    where_clauses: List[str] = []
    if category:
        where_clauses.append(f"category = '{category}'")
    if quarter:
        start, finish = QUARTER_WINDOWS[quarter]
        where_clauses.append(
            f"CAST(day AS DATE) BETWEEN DATE '{start}' AND DATE '{finish}'"
        )

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    return (
        "SELECT product_title, category, SUM(units) AS units, SUM(revenue) AS revenue\n"
        "FROM daily_product_sales\n"
        f"{where_sql}\n"
        "GROUP BY product_title, category\n"
        "ORDER BY revenue DESC\n"
        f"LIMIT {topn};"
    )


def _generate_sql_from_question(question: str) -> Tuple[str, bool, str]:
    try:
        raw_response = llm_call("gen", PROMPT_GEN.format(q=question, schema=SCHEMA))
        sql = sanitize(extract_sql(raw_response))
        if not sql:
            raise ValueError("Empty SQL from LLM response")
        return sql, False, raw_response
    except Exception as err:  # fallback to deterministic heuristic
        log.warning(json.dumps({
            "event": "llm_fallback",
            "reason": str(err),
        }))
        fallback_sql = sanitize(_fallback_sql_from_question(question))
        return fallback_sql, True, fallback_sql


def _review_sql_payload(question: str, sql: str) -> Dict[str, Any]:
    prompt_question = question if question.strip() else "Direct SQL review"
    return extract_json(
        llm_call("rev", PROMPT_REV.format(schema=SCHEMA, q=prompt_question, sql=sql))
    )


@app.get("/health")
def health():
    return {"ok": True, "provider": os.getenv("LLM_PROVIDER", "hf")}


@app.get("/schema")
def schema():
    return {"schema": SCHEMA}


@app.post("/nl2sql")
def nl2sql(req: GenRequest):
    sql, used_fallback, raw_text = _generate_sql_from_question(req.q)
    if used_fallback:
        log.info(json.dumps({"event": "nl2sql_fallback", "question": req.q}))
    return {"sql": sql, "raw": raw_text}


@app.post("/review")
def review(req: ReviewRequest):
    source_sql = req.sql or ""
    used_fallback = False

    if source_sql and _is_sql(source_sql):
        sql = sanitize(source_sql)
    elif req.q:
        sql, used_fallback, _ = _generate_sql_from_question(req.q)
    else:
        raise HTTPException(status_code=422, detail="Provide SQL text or a question to review.")

    if used_fallback:
        log.info(json.dumps({"event": "review_fallback", "question": req.q}))

    review_payload = _review_sql_payload(req.q or source_sql, sql)
    return {"sql": sql, "review": review_payload}


@app.get("/execute", response_model=ExecResponse)
def execute(q: str = Query(..., description="Business question or SQL to execute")):
    if not q.strip():
        raise HTTPException(status_code=422, detail="Query text is required.")

    fallback_used = False
    if _is_sql(q):
        sql = sanitize(q)
    else:
        sql, fallback_used, _ = _generate_sql_from_question(q)

    try:
        rows = _con().execute(sql).df().to_dict(orient="records")
    except Exception as exc:
        if not _is_sql(q) and not fallback_used:
            sql = sanitize(_fallback_sql_from_question(q))
            fallback_used = True
            rows = _con().execute(sql).df().to_dict(orient="records")
            log.info(json.dumps({"event": "execute_fallback_sql", "question": q}))
        else:
            log.error(f"SQL Execution Error: {exc}")
            log.error(f"Problematic SQL: {sql}")
            raise HTTPException(status_code=400, detail=f"SQL Execution Error: {exc}") from exc

    review_payload = _review_sql_payload(
        q if not _is_sql(q) else "Direct SQL execution", sql
    )
    return ExecResponse(sql=sql, rows=rows, review=review_payload)


# -------------------- New BI Agent Endpoints --------------------

class GenerateMetricRequest(BaseModel):
    ask: str
    metric_slug: Optional[str] = None


class DBTRunRequest(BaseModel):
    model: str


def _agent_fallback(ask: str, metric_slug: Optional[str] = None) -> Dict[str, Any]:
    # Fallback path that mimics the agent flow without langgraph dependency
    from agent import nodes as N

    state: Dict[str, Any] = {"ask": ask, "metric_slug": metric_slug}
    state.update(N.intent_parse(state))
    state.update(N.read_schema(state))
    state.update(N.gen_sql(state))
    state.update(N.validate_sql(state))
    state.update(N.exec_sql(state))
    N.write_dbt(state)
    N.run_dbt(state)
    return state


@app.post("/agent/generate_metric")
def generate_metric(req: GenerateMetricRequest):
    try:
        from agent import graph as agent_graph  # lazy import so tests not requiring langgraph can run
        result = agent_graph.app.invoke({"ask": req.ask, "metric_slug": req.metric_slug})
        # Pydantic State returns as model; convert to dict
        state = result.model_dump() if hasattr(result, "model_dump") else dict(result)
    except Exception:
        state = _agent_fallback(req.ask, req.metric_slug)

    # Build response
    slug = state.get("metric_slug")
    columns = state.get("columns") or []
    # convert preview_csv to head_rows (list of dicts)
    try:
        import io, csv

        rows: List[Dict[str, Any]] = []
        csv_text = state.get("preview_csv") or ""
        if csv_text:
            reader = csv.DictReader(io.StringIO(csv_text))
            rows = list(reader)
    except Exception:
        rows = []
    return {"slug": slug, "columns": columns, "head_rows": rows}


@app.post("/dbt/run")
def dbt_run(req: DBTRunRequest):
    import os
    import subprocess
    start = time.time()
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", os.path.abspath("profiles"))
    args = ["dbt", "run", "-s", req.model, "--project-dir", os.path.abspath("dbt_project")]
    try:
        proc = subprocess.run(args, env=env, capture_output=True, text=True)
        ok = proc.returncode == 0
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
    except FileNotFoundError:
        ok = False
        out = "dbt command not found. Ensure dbt-core is installed."
    return {"ok": ok, "seconds": round(time.time() - start, 3), "output": out[-4000:]}


@app.post("/dbt/test")
def dbt_test(req: DBTRunRequest):
    import os
    import subprocess
    start = time.time()
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", os.path.abspath("profiles"))
    args = ["dbt", "test", "-s", req.model, "--project-dir", os.path.abspath("dbt_project")]
    try:
        proc = subprocess.run(args, env=env, capture_output=True, text=True)
        ok = proc.returncode == 0
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
    except FileNotFoundError:
        ok = False
        out = "dbt command not found. Ensure dbt-core is installed."
    return {"ok": ok, "seconds": round(time.time() - start, 3), "output": out[-4000:]}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.main:app", host="0.0.0.0", port=7861)
