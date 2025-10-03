import os
from datetime import date
from typing import Dict, Optional

import duckdb
import pandas as pd
import requests
import streamlit as st

try:
    from api import main as local_api
except ImportError:  # pragma: no cover
    local_api = None

st.set_page_config(page_title="DataWeaver Dashboard", layout="wide")

_raw_api_url = os.getenv("API_URL", "").strip()
if _raw_api_url in {"http://localhost:8000", "http://127.0.0.1:8000", "localhost"}:
    _raw_api_url = ""
API_URL = _raw_api_url.removesuffix("/")
CATEGORY_OPTIONS = ["electronics", "home", "beauty", "sports", "toys"]
REMOTE_ERROR_HINT = (
    "Unable to reach the remote API. Falling back to in-process mode. "
    "If you expect to use a remote backend, set the API_URL environment variable."
)
MISSING_MODEL_HINT = (
    "Missing model configuration. Ensure FIREWORKS_API_KEY is set and the "
    "LLM_MODEL_GEN/LLM_MODEL_REV environment variables point to valid models."
)
HAS_LLM = bool(os.getenv("FIREWORKS_API_KEY")) and bool(os.getenv("LLM_MODEL_GEN")) and bool(
    os.getenv("LLM_MODEL_REV")
)


@st.cache_data(show_spinner=False)
def load_dataset() -> pd.DataFrame:
    csv_path = os.getenv("DATA_CSV", "data/daily_product_sales.csv")
    df = pd.read_csv(csv_path, parse_dates=["day"])
    df["day"] = df["day"].dt.date
    return df


@st.cache_resource(show_spinner=False)
def get_duckdb_connection():
    con = duckdb.connect()
    con.register("daily_product_sales", load_dataset())
    return con


def _handle_local_failure(exc: Exception) -> None:
    st.error(f"Local execution failed: {exc}\n{MISSING_MODEL_HINT}")


def _generate_local_sql(question_or_sql: str) -> str:
    if local_api is None:
        return question_or_sql
    if hasattr(local_api, "_is_sql") and local_api._is_sql(question_or_sql):  # type: ignore[attr-defined]
        return local_api.sanitize(question_or_sql)
    if hasattr(local_api, "_fallback_sql_from_question"):
        sql = local_api._fallback_sql_from_question(question_or_sql)  # type: ignore[attr-defined]
        return local_api.sanitize(sql)
    return question_or_sql


def _execute_local(question_or_sql: str) -> Optional[Dict]:
    if HAS_LLM and local_api is not None:
        try:
            result = local_api.execute(q=question_or_sql)  # type: ignore[arg-type]
            if hasattr(result, "model_dump"):
                return result.model_dump()
            return result
        except Exception as exc:  # pragma: no cover - fall back to deterministic mode
            _handle_local_failure(exc)

    sql = _generate_local_sql(question_or_sql)
    con = get_duckdb_connection()
    try:
        rows = con.execute(sql).df().to_dict(orient="records")
    except Exception as exc:  # pragma: no cover
        _handle_local_failure(exc)
        return None
    review = _basic_review(question_or_sql, sql, rows)
    return {"sql": sql, "rows": rows, "review": review}


def _basic_review(question: str, sql: str, rows: Optional[list]) -> Dict:
    lowered = " ".join(sql.lower().split())
    analysis = {
        "has_limit": " limit " in f" {lowered} ",
        "targets_tables": [
            t for t in ("daily_product_sales", "orders", "products") if t in lowered
        ],
        "has_group_by": "group by" in lowered,
        "has_aggregation": any(
            token in lowered for token in ("sum(", "avg(", "count(", "min(", "max(", "group by")
        ),
    }
    issues = []
    warnings = []
    stripped = lowered.strip()
    if not stripped.startswith("select") and not stripped.startswith("with"):
        issues.append("Only SELECT/CTE statements are supported in dashboard mode.")
    forbidden = [kw for kw in ("insert", "update", "delete", "drop", "alter") if kw in lowered]
    if forbidden:
        issues.append(f"Detected disallowed keywords: {', '.join(forbidden)}")
    if not analysis["has_limit"]:
        warnings.append("Add LIMIT to keep dashboard renders responsive.")
    if not analysis["targets_tables"]:
        warnings.append("Query does not reference a known marketplace table.")
    if rows is not None and len(rows) == 0:
        warnings.append("Query returned zero rows for the selected filters.")

    summary = "SQL validated successfully."
    if issues:
        summary = "SQL failed validation due to critical issues."
    elif warnings:
        summary = "SQL is valid but review the warnings before finalizing."

    return {
        "valid": not issues,
        "summary": summary,
        "issues": issues,
        "warnings": warnings,
        "analysis": analysis,
        "question": question,
    }


@st.cache_data(show_spinner=False)
def get_remote_hint():
    return REMOTE_ERROR_HINT


def api_call(
    path: str,
    *,
    params: Optional[Dict] = None,
    payload: Optional[Dict] = None,
    method: str = "GET",
):
    """Call remote API when available, otherwise fall back to local implementation."""
    if API_URL:
        url = f"{API_URL}{path}"
        try:
            if method.upper() == "POST":
                response = requests.post(url, json=payload, timeout=60)
            else:
                response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            st.warning(f"{get_remote_hint()}\nDetails: {exc}")

    if path == "/execute":
        question = ""
        if params and "q" in params:
            question = params["q"]
        elif payload and "q" in payload:
            question = payload["q"]
        return _execute_local(question)

    if path == "/review":
        question = ""
        sql_text = ""
        if payload:
            question = payload.get("q", "")
            sql_text = payload.get("sql", "")
        review_payload = _basic_review(question, sql_text, None)
        return {"sql": sql_text, "review": review_payload}

    st.error("No local handler for this path; please configure API_URL.")
    return None

def render_ask_tab(tab):
    with tab:
        st.caption("Turn business questions into SQL, run them, and inspect AI reviews.")
        default_question = "Top 5 electronics products by revenue in Q3"
        question = st.text_input("Enter business question", default_question)
        if st.button("Generate & Run", key="ask_run"):
            if not question.strip():
                st.warning("Please enter a question first.")
                return
            with st.spinner("Generating SQL and running query..."):
                resp = api_call("/execute", params={"q": question})
            if not resp:
                return
            st.subheader("Generated SQL")
            st.code(resp.get("sql", ""), language="sql")
            rows = resp.get("rows", [])
            df = pd.DataFrame(rows)
            if df.empty:
                st.warning("No rows returned for this question.")
            else:
                st.dataframe(df, use_container_width=True)
                if {"product_title", "revenue"}.issubset(df.columns):
                    chart_df = df.set_index("product_title")["revenue"]
                    st.bar_chart(chart_df, use_container_width=True)
            review_payload = resp.get("review")
            if review_payload:
                st.subheader("AI Review")
                st.json(review_payload)


def render_review_tab(tab):
    with tab:
        st.caption("Validate SQL snippets or NL prompts before you run them.")
        default_question = "Top 10 beauty products in Q2"
        default_sql = (
            "SELECT category, SUM(revenue) AS revenue\n"
            "FROM daily_product_sales\n"
            "GROUP BY category\n"
            "ORDER BY revenue DESC\n"
            "LIMIT 5;"
        )
        col_question, col_sql = st.columns(2)
        question = col_question.text_area(
            "Business question (optional context)", default_question, height=160
        )
        sql_text = col_sql.text_area("SQL to review", default_sql, height=160)
        if st.button("Review", key="review_sql"):
            if not sql_text.strip():
                st.warning("Provide SQL to review.")
                return
            payload = {"q": question, "sql": sql_text}
            with st.spinner("Reviewing SQL..."):
                resp = api_call("/review", payload=payload, method="POST")
            if resp:
                st.json(resp)
                fixed_sql = resp.get("fixed_sql") if isinstance(resp, dict) else None
                if fixed_sql:
                    st.subheader("Suggested Fix")
                    st.code(fixed_sql, language="sql")


def render_dashboard_tab(tab):
    with tab:
        st.subheader("Auto-Generated Dashboard")
        st.caption("Filter and visualize sales data with live queries against the API.")

        with st.form("dashboard_filters"):
            selected_categories = st.multiselect(
                "Filter by category",
                options=CATEGORY_OPTIONS,
                default=CATEGORY_OPTIONS,
            )
            col_start, col_end = st.columns(2)
            with col_start:
                start_date = st.date_input("Start date", date(2024, 1, 1))
            with col_end:
                end_date = st.date_input("End date", date(2024, 12, 31))
            refresh = st.form_submit_button("Refresh Dashboard")

        if not refresh:
            return

        if start_date > end_date:
            st.error("Start date must be before end date.")
            return

        active_categories = selected_categories or CATEGORY_OPTIONS
        quoted = ", ".join(f"'{cat}'" for cat in active_categories)
        sql = (
            "SELECT product_title, category, day, units, revenue\n"
            "FROM daily_product_sales\n"
            f"WHERE category IN ({quoted}) AND day BETWEEN CAST('{start_date}' AS DATE) "
            f"AND CAST('{end_date}' AS DATE)\n"
            "ORDER BY day ASC, revenue DESC\n"
            "LIMIT 1000;"
        )

        with st.spinner("Running dashboard query..."):
            resp = api_call("/execute", params={"q": sql})

        if not resp:
            return

        df = pd.DataFrame(resp.get("rows", []))
        if df.empty:
            st.warning("No data available for the selected filters.")
            return

        if "day" in df.columns:
            df["day"] = pd.to_datetime(df["day"])

        st.write("### Result Sample")
        st.dataframe(df.head(25), use_container_width=True)

        if {"category", "revenue"}.issubset(df.columns):
            revenue_by_category = (
                df.groupby("category")["revenue"].sum().sort_values(ascending=False).reset_index()
            )
            st.bar_chart(
                revenue_by_category.set_index("category")["revenue"],
                use_container_width=True,
            )
            st.caption("Revenue by category")

        if {"day", "units"}.issubset(df.columns):
            units_over_time = df.groupby("day")["units"].sum().sort_index()
            st.line_chart(units_over_time, use_container_width=True)
            st.caption("Units sold over time")

        if {"product_title", "revenue"}.issubset(df.columns):
            top_products = (
                df.groupby("product_title")["revenue"].sum().sort_values(ascending=False)
            )
            top_products = top_products.head(10)
            st.write("### Top Products by Revenue")
            st.bar_chart(top_products, use_container_width=True)

        review_payload = resp.get("review")
        if review_payload:
            with st.expander("SQL review details", expanded=False):
                st.json(review_payload)

        st.subheader("Planned Enhancements")
        st.info(
            "Cohort analysis and geo heatmaps will appear here once customer and location tables are joined."
        )


def main() -> None:
    tab_ask, tab_review, tab_dashboard = st.tabs(
        ["Ask (Business to SQL)", "Review SQL", "Generate Dashboard"]
    )
    render_ask_tab(tab_ask)
    render_review_tab(tab_review)
    render_dashboard_tab(tab_dashboard)


if __name__ == "__main__":
    main()
