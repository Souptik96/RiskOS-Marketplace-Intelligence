import re
from typing import Tuple

import sqlglot


ALLOWED_STATEMENTS = {"SELECT", "WITH"}
MAX_ROWS = 500
FORBIDDEN_KEYWORDS = (
    "DROP",
    "DELETE",
    "INSERT",
    "UPDATE",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "EXEC",
    "EXECUTE",
    "PRAGMA",
    "ATTACH",
    "DETACH",
    "REPLACE",
    "VACUUM",
)


def _enforce_limit(sql: str) -> str:
    if re.search(r"\bLIMIT\s+\d+\b", sql, flags=re.IGNORECASE):
        def replace_limit(match: re.Match[str]) -> str:
            limit_value = int(match.group(1))
            capped = min(limit_value, MAX_ROWS)
            return f"LIMIT {capped}"

        return re.sub(
            r"\bLIMIT\s+(\d+)\b",
            replace_limit,
            sql,
            count=1,
            flags=re.IGNORECASE,
        )

    return sql.rstrip(";") + f" LIMIT {MAX_ROWS}"


def validate_sql(sql: str) -> Tuple[bool, str]:
    if not sql or not sql.strip():
        return False, "Empty SQL"

    # Strip SQL comments
    candidate = re.sub(r'--.*?\n', ' ', sql)
    candidate = re.sub(r'/\*.*?\*/', ' ', candidate, flags=re.DOTALL)
    candidate = candidate.strip()

    try:
        parsed = sqlglot.parse(candidate, read="sqlite")
    except Exception as exc:
        return False, f"SQL parse error: {str(exc)[:100]}"

    if not parsed:
        return False, "No parseable SQL found"

    if len(parsed) != 1:
        return False, "Only single SELECT statements are allowed"

    first_word_match = re.match(r"^\s*([A-Za-z_]+)", candidate)
    first_word = first_word_match.group(1).upper() if first_word_match else ""
    if first_word not in ALLOWED_STATEMENTS:
        return False, f"Only SELECT statements allowed. Got: {first_word or 'UNKNOWN'}"

    sql_upper = candidate.upper()
    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", sql_upper):
            return False, f"Forbidden keyword detected: {keyword}"

    sanitized = parsed[0].sql(dialect="sqlite")
    sanitized = _enforce_limit(sanitized)
    return True, sanitized


def sanitize_and_validate(sql: str) -> Tuple[bool, str]:
    return validate_sql(sql)
