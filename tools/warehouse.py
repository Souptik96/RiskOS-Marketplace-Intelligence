import os
from typing import List, Tuple

import duckdb
import pandas as pd


DB_PATH = os.path.join("data", "metrics.duckdb")
DATA_CSV = os.getenv("DATA_CSV", os.path.join("data", "daily_product_sales.csv"))


def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = duckdb.connect(DB_PATH)
    _ensure_seed(con)
    return con


def _ensure_seed(con: duckdb.DuckDBPyConnection) -> None:
    try:
        exists = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = 'daily_product_sales'
            """
        ).fetchone()
    except Exception:
        exists = None
    if not exists and os.path.exists(DATA_CSV):
        # Create a table from the demo CSV; ensure types are inferred sensibly
        con.execute(
            f"""
            CREATE OR REPLACE TABLE daily_product_sales AS
            SELECT * FROM read_csv_auto('{DATA_CSV.replace("'", "''")}')
            """
        )
        # Coerce day column to DATE if present
        try:
            con.execute(
                """
                ALTER TABLE daily_product_sales
                ALTER COLUMN day TYPE DATE USING CAST(day AS DATE)
                """
            )
        except Exception:
            pass


def schema_for_prompt(con: duckdb.DuckDBPyConnection) -> str:
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
    return "\n".join(
        [f"Table {tbl}({', '.join(cols)})." for tbl, cols in layout.items()]
    )


def preview(sql: str) -> Tuple[pd.DataFrame, List[str]]:
    """Execute a sanitized SELECT and return a DataFrame and column list."""
    con = get_duckdb_conn()
    try:
        df = con.execute(sql).df()
    finally:
        con.close()
    return df, list(df.columns)

