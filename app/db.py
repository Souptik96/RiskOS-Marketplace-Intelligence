import os
import sqlite3
from pathlib import Path
from typing import Dict, List


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = BASE_DIR / "database" / "marketplace.db"


def resolve_db_path(raw_path: str | None = None) -> Path:
    configured = raw_path or os.getenv("DB_PATH")
    if not configured:
        return DEFAULT_DB_PATH

    path = Path(configured)
    if not path.is_absolute():
        path = BASE_DIR / path
    return path


def get_connection() -> sqlite3.Connection:
    db_path = resolve_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def get_schema_snapshot() -> Dict:
    tables: Dict[str, List[Dict[str, object]]] = {}

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
        table_names = [row[0] for row in cursor.fetchall()]

        for table_name in table_names:
            cursor.execute(
                """
                SELECT name, type, "notnull"
                FROM pragma_table_info(?)
                ORDER BY cid
                """,
                (table_name,),
            )
            columns = [
                {
                    "name": row[0],
                    "type": row[1],
                    "nullable": not bool(row[2]),
                }
                for row in cursor.fetchall()
            ]
            tables[table_name] = columns

    return {"tables": tables}
