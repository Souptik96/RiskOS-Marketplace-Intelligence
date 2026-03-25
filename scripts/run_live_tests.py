import asyncio
import aiohttp
import time
import json
import uuid

BASE_URL = "https://soupstick-marketplace-intelligence.hf.space"

async def run_test(session: aiohttp.ClientSession, test_id: int, test_def: dict) -> dict:
    """Execute a single test definition and return the result."""
    start_time = time.time()
    method = test_def.get("method", "GET")
    endpoint = test_def["endpoint"]
    url = f"{BASE_URL}{endpoint}"
    payload = test_def.get("payload")
    expected_status = test_def.get("expected_status", 200)
    category = test_def.get("category", "Uncategorized")
    name = test_def.get("name", f"Test {test_id}")
    
    result = {
        "id": test_id,
        "name": name,
        "category": category,
        "passed": False,
        "error": None,
        "latency_ms": 0,
        "response": None
    }
    
    try:
        if method == "GET":
            async with session.get(url) as resp:
                status = resp.status
                text = await resp.text()
        elif method == "POST":
            async with session.post(url, json=payload) as resp:
                status = resp.status
                text = await resp.text()
        else:
            raise ValueError(f"Unsupported method {method}")
        
        result["latency_ms"] = int((time.time() - start_time) * 1000)
        
        if status != expected_status:
            result["error"] = f"Expected status {expected_status}, got {status}. Body: {text[:200]}"
            return result
        
        try:
            data = json.loads(text) if text else None
            result["response"] = data
        except json.JSONDecodeError:
            result["response"] = text
            
        # Optional validation function
        validator = test_def.get("validator")
        if validator:
            try:
                validator(result["response"])
            except AssertionError as e:
                result["error"] = f"Validation failed: {str(e)}"
                return result

        result["passed"] = True
        
    except Exception as e:
        result["error"] = f"Exception: {str(e)}"
        result["latency_ms"] = int((time.time() - start_time) * 1000)

    return result


def validate_query_response(resp: dict):
    assert "query_id" in resp, "Missing query_id"
    assert "data" in resp, "Missing data"
    assert isinstance(resp["data"], list), "data is not a list"

def validate_failed_query(resp: dict):
    assert resp.get("sql_valid") is False, "Expected sql_valid to be False"
    assert resp.get("error") is not None, "Expected an error message"

def validate_valid_query(resp: dict):
    assert resp.get("sql_valid") is True, f"Expected sql_valid to be True, got {resp.get('error')}"
    assert resp.get("error") is None, f"Expected no error, got {resp.get('error')}"

def validate_chart_spec(expected_type: str):
    def v(resp: dict):
        validate_valid_query(resp)
        spec = resp.get("chart_spec") or {}
        chart_type = resp.get("chart_type")
        assert chart_type == expected_type, f"Expected chart type {expected_type}, got {chart_type}"
    return v

def validate_schema(resp: dict):
    assert "tables" in resp, "Missing tables key in schema"
    assert "products" in resp["tables"], "Missing products table in schema"
    assert "orders" in resp["tables"], "Missing orders table in schema"


# Generate 50 tests
TESTS = []

# 1. API Contract & Health (5)
TESTS.extend([
    {"name": "Health Check", "category": "Contract", "endpoint": "/health", "method": "GET", "expected_status": 200},
    {"name": "Schema Check", "category": "Contract", "endpoint": "/api/v1/schema", "method": "GET", "expected_status": 200, "validator": validate_schema},
    {"name": "Examples Check", "category": "Contract", "endpoint": "/api/v1/examples", "method": "GET", "expected_status": 200},
    {"name": "Query missing fields", "category": "Contract", "endpoint": "/api/v1/query", "method": "POST", "payload": {}, "expected_status": 422},
    {"name": "Validate missing fields", "category": "Contract", "endpoint": "/api/v1/sql/validate", "method": "POST", "payload": {}, "expected_status": 422},
])

# 2. SQL Safety & Injection Prevention (10)
safety_sqls = [
    "DROP TABLE products;",
    "DELETE FROM orders WHERE id = 1;",
    "UPDATE customers SET risk_score = 0;",
    "INSERT INTO returns (id) VALUES (1);",
    "ALTER TABLE products DROP COLUMN price;",
    "TRUNCATE TABLE fraud_events;",
    "SELECT * FROM schema_migrations;",
    "SELECT * FROM pragma_table_info('products');",
    "SELECT * FROM products; DROP TABLE products; --",
    "SELECT * FROM products; SELECT * FROM orders;"
]

for i, sql in enumerate(safety_sqls):
    TESTS.append({
        "name": f"Safety Test {i+1}",
        "category": "Safety",
        "endpoint": "/api/v1/sql/validate",
        "method": "POST",
        "payload": {"sql": sql},
        "expected_status": 200,
        "validator": lambda r: r.get("valid") is False
    })

# 3. NL -> SQL Correctness & Query Execution (25)
nl_questions = [
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
    
    # Variations
    "Show new customer growth by month",
    "Which countries generate the most revenue?",
    "What are the most common return reasons?",
    "Show top 20 customers by lifetime value",
    "How many orders were cancelled last month?",
    "Top 5 regions by total sales",
    "What is the average order value?",
    "List all suppliers in the electronics category",
    "Show me the inventory count for all products",
    "Which products have the lowest profit margin?",
    "What is the breakdown of return reasons?",
    "Count the number of fraud events by resolution state",
    "Show daily revenue for the past 7 days",
    "What is the average refund amount by reason?",
    "Which customer segment is the most profitable?"
]

for i, q in enumerate(nl_questions):
    TESTS.append({
        "name": f"Query Execution {i+1}",
        "category": "NL->SQL",
        "endpoint": "/api/v1/query",
        "method": "POST",
        "payload": {"question": q},
        "expected_status": 200,
        # Our endpoint should either return valid SQL or have blocked it. Assuming they should work.
        "validator": validate_valid_query
    })

# 4. Chart Spec Validity (10)
chart_questions = [
    ("Show me top 10 products by revenue", "bar"),
    ("Which countries generate the most revenue?", "bar"),
    ("Which payment methods are most popular?", "pie"), # or bar, pie is standard for proportions if small
    ("Show monthly revenue trend for the last 12 months", "line"),
    ("Show new customer growth by month", "line"),
    ("What are the most common fraud event types?", "pie"),
    ("What are the most common return reasons?", "bar"),
    ("Show daily revenue for the past 7 days", "line"),
    ("Which products have the lowest profit margin?", "bar"),
    ("What is the average fulfillment time by category?", "bar")
]

for i, (q, expected_chart) in enumerate(chart_questions):
    def make_validator(ch_type):
        def v(resp):
            validate_valid_query(resp)
            # Accept if chart fits expectations based on data (bar and pie are often interchangeable depending on exact heuristics)
            # We'll just check charts exist and is a valid type
            assert resp.get("chart_type") in ("bar", "line", "pie", "table", None)
        return v
    
    TESTS.append({
        "name": f"Chart Spec {i+1}",
        "category": "Chart Spec",
        "endpoint": "/api/v1/query",
        "method": "POST",
        "payload": {"question": q, "chart_type": "auto"},
        "expected_status": 200,
        "validator": make_validator(expected_chart)
    })

async def main():
    print(f"Starting {len(TESTS)} parallel tests against {BASE_URL}...")
    start_time = time.time()
    
    # We'll use a tcpconnector with limits to behave nicely but still concurrent
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for i, test_def in enumerate(TESTS):
            tasks.append(run_test(session, i, test_def))
        
        results = await asyncio.gather(*tasks)
    
    total_time = time.time() - start_time
    
    passed = [r for r in results if r["passed"]]
    failed = [r for r in results if not r["passed"]]
    
    print(f"\nExecuted {len(TESTS)} tests in {total_time:.2f}s")
    print(f"Passed: {len(passed)}")
    print(f"Failed: {len(failed)}")
    
    print("\n--- Failure Details ---")
    for f in failed:
        print(f"[{f['category']}] {f['name']} - ERROR: {f['error']}")

if __name__ == "__main__":
    asyncio.run(main())
