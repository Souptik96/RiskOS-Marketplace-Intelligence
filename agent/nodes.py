# agent/nodes.py
import re
from typing import Dict, Any, Tuple
from tools.warehouse import preview as wh_preview, ensure_limit  # add ensure_limit import

def _as_dict(state) -> Dict[str, Any]:
    # Works for Pydantic v2 (model_dump), v1 (dict), or raw dict
    if hasattr(state, "model_dump"):
        return state.model_dump()
    if hasattr(state, "dict"):
        return state.dict()
    return state if isinstance(state, dict) else {}

def intent_parse(state) -> Dict[str, Any]:
    data = _as_dict(state)
    ask = (data.get("ask") or "").strip()
    metric_slug = (data.get("metric_slug") or None)
    if not ask:
        raise ValueError("`ask` is required for metric generation.")
    return {"ask": ask, "metric_slug": metric_slug}

def gen_sql(state) -> Dict[str, Any]:
    data = _as_dict(state)
    sql = (data.get("sql") or "").strip()
    # If your graph builds SQL here, keep it; then enforce a safe LIMIT:
    if sql:
        sql = ensure_limit(sql, default_limit=200)
    return {"sql": sql}

def exec_sql(state) -> Dict[str, Any]:
    data = _as_dict(state)
    sql = ensure_limit((data.get("sql") or "").strip(), default_limit=200)
    df, cols = wh_preview(sql)  # returns (DataFrame, List[str])
    head_rows = df.head(50).to_dict(orient="records")
    return {"head_rows": head_rows, "columns": cols}