from app.query_runner import get_schema, run_query


def test_basic_query():
    ok, rows, err = run_query("SELECT COUNT(*) as count FROM orders LIMIT 1")
    assert ok is True
    assert err == ""
    assert len(rows) == 1
    assert rows[0]["count"] > 0


def test_schema_returns_all_tables():
    schema = get_schema()
    expected_tables = ["orders", "customers", "products", "returns", "fraud_events"]
    for table in expected_tables:
        assert table in schema["tables"], f"Table '{table}' missing from schema"


def test_row_count_not_exceeded():
    ok, rows, _ = run_query("SELECT * FROM orders LIMIT 500")
    assert ok is True
    assert len(rows) <= 500


def test_bad_sql_returns_error():
    ok, rows, err = run_query("SELECT * FROM nonexistent_table_xyz LIMIT 5")
    assert ok is False
    assert rows == []
    assert len(err) > 0


def test_revenue_aggregation():
    ok, rows, err = run_query(
        "SELECT SUM(total_amount) as total FROM orders WHERE order_status='completed' LIMIT 1"
    )
    assert ok is True
    assert err == ""
    assert rows[0]["total"] > 0
