from typing import List, Optional, Dict, Any

from pydantic import BaseModel, ConfigDict

from langgraph.graph import StateGraph, END

from . import nodes as N


class State(BaseModel):
    ask: str
    schema_prompt: Optional[str] = None
    sql_duckdb: Optional[str] = None
    preview_csv: Optional[str] = None
    metric_slug: Optional[str] = None
    columns: Optional[List[str]] = None

    # Allow passing/transferring extra fields, e.g., preview_rows, dbt status
    model_config = ConfigDict(extra="allow")


def _node(fn):
    def _inner(state: Dict[str, Any]) -> Dict[str, Any]:
        return fn(state) or {}
    return _inner


g = StateGraph(State)

g.add_node("intent_parse", _node(N.intent_parse))
g.add_node("read_schema", _node(N.read_schema))
g.add_node("gen_sql", _node(N.gen_sql))
g.add_node("validate_sql", _node(N.validate_sql))
g.add_node("exec_sql", _node(N.exec_sql))
g.add_node("write_dbt", _node(N.write_dbt))
g.add_node("run_dbt", _node(N.run_dbt))
g.add_node("finish", _node(N.finish))

g.set_entry_point("intent_parse")
g.add_edge("intent_parse", "read_schema")
g.add_edge("read_schema", "gen_sql")
g.add_edge("gen_sql", "validate_sql")
g.add_edge("validate_sql", "exec_sql")
g.add_edge("exec_sql", "write_dbt")
g.add_edge("write_dbt", "run_dbt")
g.add_edge("run_dbt", "finish")
g.add_edge("finish", END)

app = g.compile()

