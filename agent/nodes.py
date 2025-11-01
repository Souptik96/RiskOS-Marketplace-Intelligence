import os
import re
import subprocess
from typing import Dict, List, Optional, Tuple

import pandas as pd

from tools.warehouse import get_duckdb_conn, preview as wh_preview, schema_for_prompt
from tools.sqlglot_checks import sanitize as sql_sanitize
from tools.dbt_writer import write_metric


def _slugify(text: str, fallback: str = "metric") -> str:
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", text or "").strip("_").lower()
    s = s or fallback
    # avoid leading digits for model names
    if s and s[0].isdigit():
        s = f"m_{s}"
    # keep it reasonably short
    return s[:64]


def intent_parse(state: Dict) -> Dict:
    ask: str = state.get("ask", "").strip()
    if not ask:
        raise ValueError("ask is required")
    slug = state.get("metric_slug") or _slugify(ask)
    return {"metric_slug": slug}


def read_schema(state: Dict) -> Dict:
    con = get_duckdb_conn()
    try:
        prompt = schema_for_prompt(con)
    finally:
        con.close()
    return {"schema_prompt": prompt}


def _heuristic_sql(ask: str, schema_prompt: str) -> str:
    # Simple heuristic tailored to the included demo dataset daily_product_sales
    ask_l = ask.lower()
    table = "daily_product_sales"
    columns = {
        "product": "product_title",
        "category": "category",
        "day": "day",
        "units": "units",
        "revenue": "revenue",
    }

    agg_expr = "SUM(revenue) AS total_revenue" if "revenue" in ask_l else "SUM(units) AS total_units"
    group_col = None
    if "product" in ask_l:
        group_col = columns["product"]
    elif "category" in ask_l:
        group_col = columns["category"]

    where_clauses: List[str] = []
    # last N days
    m = re.search(r"last\s+(\d{1,3})\s+day", ask_l)
    if m:
        days = int(m.group(1))
        where_clauses.append(f"{columns['day']} >= CURRENT_DATE - INTERVAL {days} DAY")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    if group_col:
        sql = (
            f"SELECT {group_col} AS group_key, {agg_expr}\n"
            f"FROM {table}\n"
            f"{where_sql}\n"
            f"GROUP BY {group_col}\n"
            f"ORDER BY 2 DESC"
        )
    else:
        sql = (
            f"SELECT {agg_expr}\n"
            f"FROM {table}\n"
            f"{where_sql}"
        )
    return sql


def gen_sql(state: Dict) -> Dict:
    ask: str = state["ask"]
    schema_prompt: str = state.get("schema_prompt", "")
    # For this Space, rely on a deterministic heuristic; LLMs handled elsewhere
    sql = _heuristic_sql(ask, schema_prompt)
    return {"sql_duckdb": sql}


def validate_sql(state: Dict) -> Dict:
    sql: str = state["sql_duckdb"]
    cleaned = sql_sanitize(sql, dialect="duckdb")
    return {"sql_duckdb": cleaned}


def exec_sql(state: Dict) -> Dict:
    sql: str = state["sql_duckdb"]
    df, cols = wh_preview(sql)
    csv_text = df.to_csv(index=False)
    return {"preview_csv": csv_text, "columns": cols}


def write_dbt(state: Dict) -> Dict:
    slug: str = state["metric_slug"]
    sql: str = state["sql_duckdb"]
    cols: List[str] = state.get("columns") or []
    write_metric(
        dbt_root=os.path.join("dbt_project"),
        slug=slug,
        sql=sql,
        columns_meta=cols,
    )
    return {}


def _run_dbt(subcmd: str, model: str) -> Tuple[bool, str]:
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", os.path.abspath("profiles"))
    args = [
        "dbt",
        subcmd,
        "-s",
        model,
        "--project-dir",
        os.path.abspath("dbt_project"),
    ]
    try:
        proc = subprocess.run(
            args,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        ok = proc.returncode == 0
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        return ok, out
    except FileNotFoundError:
        return False, "dbt command not found. Ensure dbt-core is installed."


def run_dbt(state: Dict) -> Dict:
    slug: str = state["metric_slug"]
    ok, _ = _run_dbt("run", slug)
    # We don't fail the agent if dbt is missing; the API endpoint can expose status
    return {"dbt_run_ok": ok}


def finish(state: Dict) -> Dict:
    return {}

