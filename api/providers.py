import os
import json
import re

from .inference import _call_llm as _router_call


# ---------- Helper extractors (kept for compatibility) ----------

def extract_sql(text: str) -> str:
    """Prefer ```sql ...``` fenced blocks, else first SELECT/WITH clause."""
    m = re.search(r"```sql\s*(.*?)```", text, flags=re.I | re.S)
    if m:
        return m.group(1).strip().rstrip(";")

    m = re.search(r"```(.*?)```", text, flags=re.S)
    if m:
        potential_sql = m.group(1).strip()
        if re.search(r"(?is)\b(select|from|with|insert|update|delete)\b", potential_sql):
            return potential_sql.rstrip(";")

    m = re.search(r"(?is)\b(select|with)\b.*?(?:;|$)", text)
    if m:
        sql = m.group(0).strip()
        return sql.rstrip(";")

    return ""


def extract_json(text: str):
    """Best-effort JSON extraction from an LLM response."""
    m = re.search(r"```json\s*(\{.*?\})\s*```", text, flags=re.S)
    blob = m.group(1) if m else text
    m = re.search(r"\{.*\}", blob, flags=re.S)
    blob = m.group(0) if m else blob
    try:
        return json.loads(blob)
    except Exception:
        return {"raw": text}


# ---------- Unified provider call via HF Router ----------

def llm_call(kind: str, prompt: str) -> str:
    """Unified HF Router call. Model is taken from HF_ROUTER_MODEL env."""
    model = os.getenv("HF_ROUTER_MODEL")
    if not model:
        raise RuntimeError("Set HF_ROUTER_MODEL in env for HF Router.")
    return _router_call(prompt, max_tokens=256, temperature=0.0, model=model)
