import os
import re
import json
import subprocess
from pathlib import Path
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


def _slugify(s: str, max_len: int = 60) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", (s or "").strip().lower()).strip("_")
    return (slug[:max_len] or "metric").strip("_")


# ---------- Nodes ----------

def intent_parse(state) -> Dict[str, Any]:
    """
    Normalize incoming ask/slug and seed state.
    Input:  {"ask": "...", "metric_slug": optional}
    Output: {"ask": "...", "metric_slug": "<slug>"}
    """
    data = _as_dict(state)
    ask = (data.get("ask") or "").strip()
    slug = data.get("metric_slug")
    if not slug:
        # derive slug from ask (first few words)
        slug = _slugify("_".join(ask.split()[:6])) if ask else "metric"
    return {"ask": ask, "metric_slug": slug}


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


def validate_sql(state) -> Dict[str, Any]:
    """
    Basic static checks: SELECT/CTE only, LIMIT enforced.
    Returns: {"sql": "<sanitized>", "validation": {"issues": [...], "warnings": [...]} }
    """
    data = _as_dict(state)
    sql_in = (data.get("sql") or "").strip()
    issues: List[str] = []
    warnings: List[str] = []

    if not sql_in:
        issues.append("Empty SQL.")
        return {"sql": sql_in, "validation": {"issues": issues, "warnings": warnings}}

    lowered = sql_in.lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        issues.append("Only SELECT/CTE statements are allowed.")
    if any(k in lowered for k in ("insert", "update", "delete", "drop", "alter", "truncate")):
        issues.append("DML/DDL keywords detected.")

    sql_out = ensure_limit(sql_in, default_limit=200)
    if " limit " not in f" {lowered} ":
        warnings.append("LIMIT added to keep previews responsive.")

    return {"sql": sql_out, "validation": {"issues": issues, "warnings": warnings}}


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


def write_dbt(state) -> Dict[str, Any]:
    """
    Write a dbt model file for the metric.
    Creates: dbt_project/models/metrics/<slug>.sql (+ minimal <slug>.yml if absent)
    Returns: {"slug": slug, "dbt_model_path": str}
    """
    data = _as_dict(state)
    slug = _slugify(data.get("metric_slug") or data.get("slug") or "metric")
    sql = (data.get("sql") or "").strip()
    if not sql:
        return {"slug": slug, "dbt_model_path": None}

    models_dir = Path("dbt_project/models/metrics")
    models_dir.mkdir(parents=True, exist_ok=True)

    model_sql = models_dir / f"{slug}.sql"
    model_sql.write_text(
        "{{ config(materialized='table') }}\n-- auto-generated by agent\n" + sql + "\n",
        encoding="utf-8",
    )

    # Minimal schema YML (idempotent)
    yml = models_dir / f"{slug}.yml"
    if not yml.exists():
        yml_content = {
            "version": 2,
            "models": [
                {
                    "name": slug,
                    "description": f"Auto-generated metric model for '{slug}'",
                }
            ],
        }
        # naive dump to yaml without pyyaml dependency
        def _to_yaml(d: Dict[str, Any], indent: int = 0) -> str:
            sp = "  " * indent
            out = []
            for k, v in d.items():
                if isinstance(v, list):
                    out.append(f"{sp}{k}:")
                    for item in v:
                        out.append(f"{sp}- name: {item.get('name')}")
                        desc = item.get("description")
                        if desc:
                            out.append(f"{sp}  description: {json.dumps(desc)}")
                else:
                    out.append(f"{sp}{k}: {json.dumps(v)}")
            return "\n".join(out) + "\n"

        yml.write_text(_to_yaml(yml_content), encoding="utf-8")

    return {"slug": slug, "dbt_model_path": str(model_sql)}


def run_dbt(state) -> Dict[str, Any]:
    """
    Attempt to run dbt for the generated model. Soft-fail on Spaces.
    Returns: {"dbt_status": "ok|skipped|error", "dbt_output": "..."}
    """
    data = _as_dict(state)
    slug = _slugify(data.get("metric_slug") or data.get("slug") or "metric")

    project_dir = "dbt_project"
    profiles_dir = os.getenv("DBT_PROFILES_DIR", "dbt_project/profiles")

    cmd = ["dbt", "run", "--project-dir", project_dir, "--profiles-dir", profiles_dir, "--select", f"metrics.{slug}"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if proc.returncode == 0:
            return {"dbt_status": "ok", "dbt_output": proc.stdout[-4000:]}
        return {"dbt_status": "error", "dbt_output": (proc.stdout + "\n" + proc.stderr)[-4000:]}
    except FileNotFoundError:
        return {"dbt_status": "skipped", "dbt_output": "dbt not installed in runtime"}
    except subprocess.TimeoutExpired:
        return {"dbt_status": "error", "dbt_output": "dbt run timed out"}
    except Exception as e:
        return {"dbt_status": "error", "dbt_output": f"Exception: {e}"}


def finish(state) -> Dict[str, Any]:
    """
    Finalization hook; nothing to add—downstream API will read accumulated fields.
    """
    return {}