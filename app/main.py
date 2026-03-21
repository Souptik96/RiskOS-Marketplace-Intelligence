import time
import uuid

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from app.chart_builder import build_chart_spec
from app.nl_to_sql import convert_to_sql
from app.query_runner import get_schema, run_query
from app.schemas import QueryRequest, QueryResponse, SQLValidationRequest, SQLValidationResponse
from app.sql_validator import sanitize_and_validate


app = FastAPI(
    title="Marketplace Intelligence API",
    version="1.0.0",
    description="Natural Language → SQL → Dashboard for marketplace analytics",
    docs_url="/docs",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs", status_code=307)


@app.get("/health")
def health():
    try:
        ok, rows, _ = run_query("SELECT COUNT(*) AS count FROM orders LIMIT 1")
        db_ok = ok and len(rows) > 0
    except Exception:
        db_ok = False

    return {
        "status": "ok" if db_ok else "degraded",
        "database": "connected" if db_ok else "error",
        "version": "1.0.0",
    }


@app.post("/api/v1/query", response_model=QueryResponse)
def query(request: QueryRequest):
    start = time.time()
    query_id = str(uuid.uuid4())

    sql_result = convert_to_sql(request.question)
    raw_sql = sql_result["sql"]
    sql_source = sql_result["source"]

    valid, sql_or_error = sanitize_and_validate(raw_sql)
    if not valid:
        return QueryResponse(
            query_id=query_id,
            natural_language_input=request.question,
            generated_sql=raw_sql,
            sql_source=sql_source,
            sql_valid=False,
            result_rows=0,
            data=[],
            chart_type=None,
            chart_spec=None,
            latency_ms=int((time.time() - start) * 1000),
            error=sql_or_error,
        )

    sanitized_sql = sql_or_error
    success, rows, db_error = run_query(sanitized_sql)
    if not success:
        return QueryResponse(
            query_id=query_id,
            natural_language_input=request.question,
            generated_sql=sanitized_sql,
            sql_source=sql_source,
            sql_valid=True,
            result_rows=0,
            data=[],
            chart_type=None,
            chart_spec=None,
            latency_ms=int((time.time() - start) * 1000),
            error=db_error,
        )

    chart_override = None if request.chart_type in (None, "auto") else request.chart_type
    chart_spec = build_chart_spec(rows, chart_override) if rows and request.chart_type != "table" else {}
    chart_type = chart_spec.get("type") if chart_spec else None

    return QueryResponse(
        query_id=query_id,
        natural_language_input=request.question,
        generated_sql=sanitized_sql,
        sql_source=sql_source,
        sql_valid=True,
        result_rows=len(rows),
        data=rows,
        chart_type=chart_type,
        chart_spec=chart_spec,
        latency_ms=int((time.time() - start) * 1000),
        error=None,
    )


@app.post("/api/v1/sql/validate", response_model=SQLValidationResponse)
def validate_sql_endpoint(request: SQLValidationRequest):
    valid, result = sanitize_and_validate(request.sql)
    return SQLValidationResponse(valid=valid, sanitized_sql=result if valid else None, error=None if valid else result)


@app.get("/api/v1/schema")
def schema():
    return get_schema()


@app.get("/api/v1/examples")
def examples():
    return {
        "questions": [
            "Show me top 10 products by revenue",
            "What is our revenue by category this year?",
            "Show monthly revenue trend for the last 12 months",
            "Which customers have the highest risk scores?",
            "What are the most common fraud event types?",
            "Show me all flagged orders over $500",
            "What is the return rate by category?",
            "Which payment methods are most popular?",
            "Show profit margin by product category",
            "What is the average fulfillment time by category?",
            "Show new customer growth by month",
            "Which countries generate the most revenue?",
            "What are the most common return reasons?",
            "Show top 20 customers by lifetime value",
            "How many orders were cancelled last month?",
        ]
    }
