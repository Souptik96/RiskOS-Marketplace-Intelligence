import sqlite3
import time
from typing import Dict, List, Tuple

from app.db import get_connection, get_schema_snapshot
from app.sql_validator import MAX_ROWS, sanitize_and_validate


QUERY_TIMEOUT_SECONDS = 10


def run_query(sql: str) -> Tuple[bool, List[Dict], str]:
    if not sql or not sql.strip():
        return False, [], "Empty SQL"

    valid, sanitized_or_error = sanitize_and_validate(sql)
    if not valid:
        return False, [], sanitized_or_error

    conn = None
    try:
        conn = get_connection()
        conn.execute("PRAGMA foreign_keys = ON")
        started = time.monotonic()

        def abort_if_slow() -> int:
            return 1 if time.monotonic() - started > QUERY_TIMEOUT_SECONDS else 0

        conn.set_progress_handler(abort_if_slow, 10_000)
        cursor = conn.cursor()
        cursor.execute(sanitized_or_error)
        rows = cursor.fetchmany(MAX_ROWS)
        return True, [dict(row) for row in rows], ""
    except sqlite3.OperationalError as exc:
        if "interrupted" in str(exc).lower():
            return False, [], "Query error: Query exceeded timeout"
        return False, [], f"Query error: {str(exc)}"
    except sqlite3.ProgrammingError as exc:
        return False, [], f"Programming error: {str(exc)}"
    except Exception as exc:
        return False, [], f"Unexpected error: {str(exc)[:100]}"
    finally:
        if conn:
            conn.set_progress_handler(None, 0)
            conn.close()


def get_schema() -> Dict:
    schema = get_schema_snapshot()
    schema["row_limits"] = {"max_rows": MAX_ROWS}
    return schema
