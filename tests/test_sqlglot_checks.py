import pytest

from tools.sqlglot_checks import sanitize
import sqlglot


def test_blocks_ddl_dml():
    for bad in [
        "DROP TABLE x",
        "DELETE FROM t WHERE 1=1",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a=1",
        "CREATE TABLE x(a int)",
    ]:
        with pytest.raises(ValueError):
            sanitize(bad)


def test_injects_limit_and_parseable():
    sql = "SELECT 1 AS a"
    out = sanitize(sql)
    assert "limit" in out.lower()
    # Ensure canonical SQL parses
    assert sqlglot.parse_one(out, read="duckdb") is not None


def test_cte_allowed_and_limited():
    sql = "WITH x AS (SELECT 1 a) SELECT * FROM x"
    out = sanitize(sql)
    assert "limit" in out.lower()
    assert sqlglot.parse_one(out, read="duckdb") is not None

