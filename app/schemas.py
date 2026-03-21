from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    question: str = Field(min_length=3, max_length=500)
    chart_type: Optional[str] = Field(None, pattern="^(bar|line|pie|table|auto)$")
    db: Optional[str] = None


class SQLValidationRequest(BaseModel):
    sql: str = Field(min_length=5, max_length=2000)


class QueryResponse(BaseModel):
    query_id: str
    natural_language_input: str
    generated_sql: str
    sql_source: str
    sql_valid: bool
    result_rows: int
    data: List[Dict[str, Any]]
    chart_type: Optional[str]
    chart_spec: Optional[Dict[str, Any]]
    latency_ms: int
    error: Optional[str] = None


class SQLValidationResponse(BaseModel):
    valid: bool
    sanitized_sql: Optional[str]
    error: Optional[str]
