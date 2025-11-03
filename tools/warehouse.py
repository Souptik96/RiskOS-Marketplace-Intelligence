# tools/warehouse.py
import re
import duckdb
import pandas as pd
from typing import Tuple, List

# Add this helper
def ensure_limit(sql: str, default_limit: int = 200) -> str:
    """
    Ensure the SQL has a well-formed LIMIT clause; never produce 'DESC200'.
    """
    if not sql or not sql.strip():
        raise ValueError("Empty SQL")
    stmt = sql.strip().rstrip(";")

    # If already has LIMIT <num>, leave as-is
    if re.search(r"(?is)\blimit\s+\d+\b", stmt):
        return stmt + ";"

    # Insert a proper space + LIMIT
    return f"{stmt} LIMIT {default_limit};"

# Update preview to use the sanitizer
def preview(sql: str, default_limit: int = 200) -> Tuple[pd.DataFrame, List[str]]:
    stmt = ensure_limit(sql, default_limit)
    con = duckdb.connect()
    try:
        df = con.execute(stmt).df()
    finally:
        con.close()
    return df, list(df.columns)