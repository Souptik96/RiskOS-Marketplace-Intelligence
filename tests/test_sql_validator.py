from app.sql_validator import sanitize_and_validate


def test_valid_select():
    valid, result = sanitize_and_validate("SELECT * FROM orders")
    assert valid is True
    assert "LIMIT" in result.upper()


def test_blocks_drop():
    valid, result = sanitize_and_validate("DROP TABLE orders")
    assert valid is False
    assert "allowed" in result.lower() or "forbidden" in result.lower()


def test_blocks_delete():
    valid, _ = sanitize_and_validate("DELETE FROM customers WHERE 1=1")
    assert valid is False


def test_blocks_insert():
    valid, _ = sanitize_and_validate("INSERT INTO orders VALUES (1, 2, 3)")
    assert valid is False


def test_blocks_update():
    valid, _ = sanitize_and_validate("UPDATE customers SET risk_score = 0")
    assert valid is False


def test_adds_limit_if_missing():
    valid, sql = sanitize_and_validate("SELECT * FROM orders")
    assert valid is True
    assert "LIMIT" in sql.upper()


def test_caps_limit_at_500():
    valid, sql = sanitize_and_validate("SELECT * FROM orders LIMIT 10000")
    assert valid is True
    assert "LIMIT 500" in sql.upper()


def test_empty_sql():
    valid, _ = sanitize_and_validate("")
    assert valid is False


def test_invalid_syntax():
    valid, _ = sanitize_and_validate("SELECT FROM WHERE")
    assert valid is False


def test_blocks_pragma():
    valid, _ = sanitize_and_validate("PRAGMA table_info(orders)")
    assert valid is False
