import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import resolve_db_path
from database.seed import seed_database


def main() -> None:
    db_path = resolve_db_path()
    if db_path.exists():
        db_path.unlink()

    counts = seed_database(db_path)
    for table, count in counts.items():
        print(f"{table}: {count}")


if __name__ == "__main__":
    main()
