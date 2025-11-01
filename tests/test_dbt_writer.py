import os

import yaml

from tools.dbt_writer import write_metric, JINJA_HEADER


def test_writes_sql_and_yml(tmp_path):
    root = tmp_path / "dbt_project"
    write_metric(
        dbt_root=str(root),
        slug="test_metric",
        sql="SELECT 1 AS a",
        columns_meta=["a"],
    )

    sql_path = root / "models" / "metrics" / "test_metric.sql"
    yml_path = root / "models" / "metrics" / "test_metric.yml"
    assert sql_path.exists()
    assert yml_path.exists()

    sql_text = sql_path.read_text(encoding="utf-8")
    assert JINJA_HEADER in sql_text
    assert "SELECT" in sql_text

    data = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
    assert data["version"] == 2
    assert data["models"][0]["name"] == "test_metric"
    assert data["models"][0]["columns"][0]["name"] == "a"

