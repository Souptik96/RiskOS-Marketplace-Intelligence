import os
import re
from typing import Tuple, List
import duckdb
import pandas as pd
from pathlib import Path

_CON = None  # module-level shared DuckDB connection


def _maybe_bootstrap(con: duckdb.DuckDBPyConnection) -> None:
    """
    Register a sample table if available so previews don't fail on empty schemas.
    Looks for data/daily_product_sales.csv and creates a table if not present.
    """
    # Create schema table if missing
    have_sample = False
    try:
        exists = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema='main' AND table_name='daily_product_sales'"
        ).fetchall()
        if exists:
            return
        # If file exists, create table from CSV
        csv_path = Path("data/daily_product_sales.csv")
        if csv_path.exists():
            con.execute("""
                CREATE TABLE daily_product_sales AS
                SELECT * FROM read_csv_auto('data/daily_product_sales.csv', dateformat='%Y-%m-%d', ignore_errors=true);
            """)
            have_sample = True
    except Exception:
        pass
    if not have_sample:
        # Nothing to bootstrap; it's fine—schema may be uploaded at runtime via UI
        return


def _get_con() -> duckdb.DuckDBPyConnection:
    global _CON
    if _CON is None:
        _CON = duckdb.connect()
        _maybe_bootstrap(_CON)
    return _CON


def ensure_limit(sql: str, default_limit: int = 200) -> str:
    """
    Ensure the SQL has a well-formed LIMIT clause; never produce 'DESC200'.
    Appends a semicolon to stabilize execution.
    """
    if not sql or not sql.strip():
        raise ValueError("Empty SQL")
    stmt = sql.strip().rstrip(";")

    # already has LIMIT <num>
    if re.search(r"(?is)\blimit\s+\d+\b", stmt):
        return stmt + ";"

    # append with a leading space to avoid 'DESC200' forms
    return f"{stmt} LIMIT {default_limit};"


def preview(sql: str, default_limit: int = 200) -> Tuple[pd.DataFrame, List[str]]:
    """
    Execute SQL (with enforced LIMIT) against a shared DuckDB connection.
    Returns (DataFrame, columns).
    """
    con = _get_con()
    stmt = ensure_limit(sql, default_limit)
    df = con.execute(stmt).df()
    return df, list(df.columns)


def schema_text() -> str:
    """
    Produce a concise, prompt-ready schema description from information_schema.
    """
    con = _get_con()
    rows = con.execute(
        """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main'
        ORDER BY table_name, ordinal_position
        """
    ).fetchall()
    layout = {}
    for table, column, dtype in rows:
        layout.setdefault(table, []).append(f"{column} {dtype}")
    if not layout:
        return ""
    return "\n".join([f"Table {tbl}({', '.join(cols)})." for tbl, cols in layout.items()])