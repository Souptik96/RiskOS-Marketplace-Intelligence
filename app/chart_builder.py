from typing import Dict, List, Optional


def infer_chart_type(columns: List[str], rows: List[Dict]) -> str:
    col_lower = [column.lower() for column in columns]
    first_column = col_lower[0] if col_lower else ""

    time_keywords = ["month", "date", "week", "year", "day", "period"]
    if any(keyword in first_column for keyword in time_keywords) and len(columns) >= 2:
        return "line"

    if len(rows) <= 8 and any("pct" in column or "percent" in column or "share" in column for column in col_lower):
        return "pie"

    return "bar"


def build_chart_spec(rows: List[Dict], chart_type: Optional[str] = None) -> Dict:
    if not rows:
        return {}

    columns = list(rows[0].keys())
    if len(columns) < 2 or chart_type == "table":
        return {}

    if not chart_type:
        chart_type = infer_chart_type(columns, rows)

    x_col = columns[0]
    y_candidates: List[str] = []

    for column in columns[1:]:
        sample_values = [row[column] for row in rows[:5] if row[column] is not None]
        if sample_values and all(isinstance(value, (int, float)) for value in sample_values):
            y_candidates.append(column)

    if not y_candidates:
        return {}

    y_col = y_candidates[0]
    x_values = [str(row[x_col]) for row in rows]
    y_values = [row[y_col] for row in rows]

    if chart_type == "line":
        spec = {"type": "line", "data": {"x": x_values, "y": y_values, "labels": {"x": x_col, "y": y_col}}}
    elif chart_type == "pie":
        spec = {"type": "pie", "data": {"labels": x_values, "values": y_values}}
    else:
        spec = {"type": "bar", "data": {"x": x_values, "y": y_values, "labels": {"x": x_col, "y": y_col}}}

    if len(y_candidates) > 1:
        spec["additional_series"] = []
        for column in y_candidates[1:3]:
            spec["additional_series"].append({"name": column, "values": [row[column] for row in rows]})

    return spec
