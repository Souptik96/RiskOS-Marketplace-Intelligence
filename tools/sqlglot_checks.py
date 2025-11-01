import re
from typing import Optional

import sqlglot
from sqlglot import exp as sqlexp


_DDL_DML_RE = re.compile(
    r"(?is)\b(drop|delete|update|insert|merge|alter|create|truncate|grant|revoke|vacuum|attach|copy|replace)\b"
)


def _ensure_limit(expression: sqlexp.Expression, default_limit: int = 200) -> sqlexp.Expression:
    if isinstance(expression, sqlexp.With):
        body = expression.this
        if isinstance(body, sqlexp.Select) and not body.args.get("limit"):
            body.set("limit", sqlexp.Limit(this=sqlexp.Literal.number(default_limit)))
        return expression
    if isinstance(expression, sqlexp.Select) and not expression.args.get("limit"):
        expression.set("limit", sqlexp.Limit(this=sqlexp.Literal.number(default_limit)))
    return expression


def sanitize(sql: str, dialect: str = "duckdb", default_limit: int = 200) -> str:
    """Validate and canonicalize SQL for safe DuckDB preview.

    - Reject non-SELECT/CTE or presence of DDL/DML keywords
    - Ensure a LIMIT (default 200)
    - Parse with sqlglot and re-emit canonical SQL
    """
    if not sql or not str(sql).strip():
        raise ValueError("Empty SQL")

    s = str(sql).strip().strip(";")
    if _DDL_DML_RE.search(s):
        raise ValueError("Dangerous statement blocked: contains DDL/DML keywords")

    try:
        expr = sqlglot.parse_one(s, read=dialect)
    except Exception as e:
        raise ValueError(f"SQL parse error: {e}") from e

    if not isinstance(expr, (sqlexp.Select, sqlexp.With)):
        raise ValueError("Only SELECT queries or CTEs are allowed")

    expr = _ensure_limit(expr, default_limit=default_limit)
    # Re-emit canonical SQL in the given dialect
    return expr.sql(dialect=dialect)

