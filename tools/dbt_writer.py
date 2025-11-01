import os
from typing import Iterable, List

import yaml


JINJA_HEADER = "{{ config(materialized='table') }}\n"


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_metric(dbt_root: str, slug: str, sql: str, columns_meta: Iterable[str]) -> None:
    """Write a dbt model SQL and schema YAML for a metric.

    - SQL: dbt_project/models/metrics/<slug>.sql
      with Jinja header and SELECT body
    - YAML: dbt_project/models/metrics/<slug>.yml with not_null tests
    """
    metrics_dir = os.path.join(dbt_root, "models", "metrics")
    _ensure_dir(metrics_dir)

    sql_body = (sql or "").strip().rstrip(";") + "\n"
    sql_path = os.path.join(metrics_dir, f"{slug}.sql")
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write(JINJA_HEADER)
        f.write(sql_body)

    cols = list(columns_meta or [])
    yml_payload = {
        "version": 2,
        "models": [
            {
                "name": slug,
                "columns": [
                    {"name": c, "tests": ["not_null"]} for c in cols
                ],
            }
        ],
    }
    yml_path = os.path.join(metrics_dir, f"{slug}.yml")
    with open(yml_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(yml_payload, f, sort_keys=False)

