import os
import re
from typing import Dict, Any, List, Tuple, Optional

from api.inference import _call_llm
from tools.warehouse import ensure_limit, preview as wh_preview, schema_text


def _as_dict(state) -> Dict[str, Any]:
    """Tolerate Pydantic v1/v2 State or plain dict."""
    if hasattr(state, "model_dump"):
        return state.model_dump()
    if hasattr(state, "dict"):
        return state.dict()
    return state if isinstance(state, dict) else {}


def _extract_sql_from_text(text: str) -> str:
    """Pull the first SQL statement from model output."""
    # ```sql ... ```
    fenced = re.search(r"```sql\s*(.*?)```", text, flags=re.I | re.S)
    if fenced:
        return fenced.group(1).strip()
    # plain SELECT/WITH block
    fallback = re.search(r"(?is)\b(select|with)\b.*", text)
    return (fallback.group(0).strip() if fallback else text.strip())


# ---------- Nodes ----------

def read_schema(state) -> Dict[str, Any]:
    """
    Introspect current DuckDB schema (tables/columns) for prompting.
    Returns: {"schema": "<promptable schema text>"}
    """
    return {"schema": schema_text()}


def gen_sql(state) -> Dict[str, Any]:
    """
    Generate SQL from `ask` + `schema` using the configured provider/model.
    Returns: {"sql": "<sanitized SELECT ... LIMIT 200>"}
    """
    data = _as_dict(state)
    ask = (data.get("ask") or "").strip()
    if not ask:
        raise ValueError("`ask` is required to generate SQL.")
    schema = (data.get("schema") or "").strip()

    provider = (os.getenv("LLM_PROVIDER") or "fireworks").lower()
    model = os.getenv("LLM_MODEL_GEN") or os.getenv("FIREWORKS_MODEL_ID", "accounts/fireworks/models/gpt-oss-20b")

    prompt = (
        "You convert business questions into DuckDB SQL ONLY.\n"
        f"Use these tables:\n{schema or 'No tables were provided.'}\n"
        "Return only SQL without commentary.\n"
        f"Question: {ask}\nSQL:"
    )

    raw = _call_llm(prompt, max_tokens=400, temperature=0.0, model=model, provider=provider)
    sql = ensure_limit(_extract_sql_from_text(raw), default_limit=200)
    return {"sql": sql}


def exec_sql(state) -> Dict[str, Any]:
    """
    Execute SQL against DuckDB and return preview rows + columns.
    Returns: {"head_rows": [...], "columns": [...]}
    """
    data = _as_dict(state)
    sql = ensure_limit((data.get("sql") or "").strip(), default_limit=200)
    df, cols = wh_preview(sql)
    head_rows = df.head(50).to_dict(orient="records")
    return {"head_rows": head_rows, "columns": cols}