import json
import time

import pytest
from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)

with open("tests/fixtures/test_queries.json", encoding="utf-8") as fixture_file:
    test_cases = json.load(fixture_file)["test_cases"]


def test_root_redirects_to_docs():
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/docs"


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] in ["ok", "degraded"]
    assert response.json()["database"] == "connected"


def test_examples_endpoint():
    response = client.get("/api/v1/examples")
    assert response.status_code == 200
    assert len(response.json()["questions"]) >= 10


def test_schema_endpoint():
    response = client.get("/api/v1/schema")
    assert response.status_code == 200
    tables = response.json()["tables"]
    for table in ["orders", "customers", "products"]:
        assert table in tables


def test_sql_validate_valid():
    response = client.post("/api/v1/sql/validate", json={"sql": "SELECT * FROM orders"})
    assert response.status_code == 200
    assert response.json()["valid"] is True


def test_sql_validate_invalid():
    response = client.post("/api/v1/sql/validate", json={"sql": "DROP TABLE orders"})
    assert response.status_code == 200
    assert response.json()["valid"] is False


@pytest.mark.parametrize("tc", test_cases[:12])
def test_query_endpoint(tc):
    response = client.post("/api/v1/query", json={"question": tc["question"]})
    assert response.status_code == 200
    body = response.json()
    assert body["sql_valid"] is True, f"[{tc['id']}] SQL invalid for: {tc['question']}\nError: {body.get('error')}"
    assert body["result_rows"] >= tc["expected_min_rows"], (
        f"[{tc['id']}] Got {body['result_rows']} rows, expected >= {tc['expected_min_rows']}"
    )
    for field in [
        "query_id",
        "generated_sql",
        "sql_source",
        "sql_valid",
        "result_rows",
        "data",
        "latency_ms",
    ]:
        assert field in body, f"Missing field: {field}"


def test_query_invalid_request():
    response = client.post("/api/v1/query", json={"question": ""})
    assert response.status_code == 422


def test_query_sql_injection_attempt():
    response = client.post("/api/v1/query", json={"question": "DROP TABLE orders; --"})
    assert response.status_code == 200
    body = response.json()
    if not body["sql_valid"]:
        assert body["error"] is not None
    else:
        assert "DROP" not in body["generated_sql"].upper()


def test_query_latency():
    start = time.time()
    response = client.post("/api/v1/query", json={"question": "top products by revenue"})
    elapsed = (time.time() - start) * 1000
    assert elapsed < 3000, f"Latency {elapsed:.0f}ms exceeds 3000ms"
    assert response.json()["latency_ms"] < 3000
