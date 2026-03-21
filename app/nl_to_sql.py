import os
import re
from typing import Callable, Dict, Optional

from openai import OpenAI


DB_SCHEMA_CONTEXT = """
You have access to a SQLite marketplace database with these tables:

products(product_id, name, category, subcategory, brand, unit_price, cost_price,
         stock_quantity, supplier_country, created_at)

customers(customer_id, country, region, customer_segment, account_age_days,
          total_lifetime_value, risk_score, created_at)

orders(order_id, customer_id, product_id, quantity, unit_price, total_amount,
       discount_applied, order_status, payment_method, is_flagged,
       order_date, fulfillment_days)

returns(return_id, order_id, customer_id, product_id, return_reason,
        refund_amount, return_date)

fraud_events(event_id, customer_id, order_id, event_type, amount_at_risk,
             resolved, event_date)

Rules:
- order_date and return_date and event_date are stored as TEXT in format 'YYYY-MM-DD HH:MM:SS'
- Use strftime() for date operations in SQLite
- order_status values: 'completed', 'returned', 'cancelled', 'pending'
- payment_method values: 'card', 'bank_transfer', 'crypto', 'cash'
- customer_segment values: 'retail', 'wholesale', 'enterprise'
- event_type values: 'chargeback', 'return_fraud', 'identity_theft', 'account_takeover'
- is_flagged is 0 or 1 (integer)
- All monetary values are in USD
"""

SYSTEM_PROMPT = f"""You are a SQL expert for a marketplace analytics system.
Convert natural language questions to syntactically correct SQLite SELECT queries.

{DB_SCHEMA_CONTEXT}

Critical rules:
- Output ONLY the SQL query. No explanation, no markdown, no backticks.
- Always use SELECT. Never use INSERT, UPDATE, DELETE, DROP, or any write operation.
- For "this month" use: strftime('%Y-%m', order_date) = strftime('%Y-%m', 'now')
- For "this year" use: strftime('%Y', order_date) = strftime('%Y', 'now')
- For "last N days" use: date(order_date) >= date('now', '-N days')
- Always include a LIMIT clause (max 500 rows)
- For revenue calculations use: SUM(total_amount)
- For profit calculations use: SUM(total_amount - (quantity * cost_price)) joining orders with products
- When asked about "top N", use ORDER BY ... DESC LIMIT N
- If a question is ambiguous, choose the most common interpretation
- If a question cannot be answered from the schema, output: ERROR: cannot answer from available data"""


def llm_to_sql(question: str) -> Optional[str]:
    api_key = os.getenv("LLM_API_KEY")
    if not api_key:
        return None

    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": question},
            ],
            max_tokens=300,
            temperature=0.1,
        )
        sql = (response.choices[0].message.content or "").strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()
        if sql.startswith("ERROR:"):
            return None
        return sql or None
    except Exception:
        return None


def _extract_top_n(question: str, default: int = 10) -> int:
    match = re.search(r"\b(\d{1,3})\b", question)
    if not match:
        return default
    return max(1, min(500, int(match.group(1))))


def _extract_amount_threshold(question: str) -> Optional[float]:
    match = re.search(
        r"(?:over|above|greater than|more than)\s*\$?(\d+(?:\.\d+)?)",
        question,
    )
    return float(match.group(1)) if match else None


def _order_date_filter(question: str, column: str = "o.order_date") -> str:
    question_lower = question.lower()
    if "this month" in question_lower:
        return f"strftime('%Y-%m', {column}) = strftime('%Y-%m', 'now')"
    if "this year" in question_lower:
        return f"strftime('%Y', {column}) = strftime('%Y', 'now')"
    if "last month" in question_lower:
        return f"strftime('%Y-%m', {column}) = strftime('%Y-%m', date('now', '-1 month'))"

    days_match = re.search(r"last\s+(\d+)\s+days", question_lower)
    if days_match:
        return f"date({column}) >= date('now', '-{days_match.group(1)} days')"

    months_match = re.search(r"last\s+(\d+)\s+months", question_lower)
    if months_match:
        return f"date({column}) >= date('now', '-{months_match.group(1)} months')"

    return ""


def _append_where(base_conditions: list[str], extra_condition: str) -> str:
    conditions = [condition for condition in base_conditions if condition]
    if extra_condition:
        conditions.append(extra_condition)
    return " WHERE " + " AND ".join(conditions) if conditions else ""


def _top_products_revenue(question: str) -> str:
    limit = _extract_top_n(question, 10)
    where_sql = _append_where(["o.order_status = 'completed'"], _order_date_filter(question))
    return f"""
        SELECT p.name, p.category, SUM(o.total_amount) AS revenue,
               COUNT(o.order_id) AS order_count
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        {where_sql}
        GROUP BY p.product_id, p.name, p.category
        ORDER BY revenue DESC LIMIT {limit}
    """


def _revenue_by_category(question: str) -> str:
    where_sql = _append_where(["o.order_status = 'completed'"], _order_date_filter(question))
    return f"""
        SELECT p.category, SUM(o.total_amount) AS revenue,
               COUNT(o.order_id) AS order_count,
               ROUND(AVG(o.total_amount), 2) AS avg_order_value
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        {where_sql}
        GROUP BY p.category
        ORDER BY revenue DESC LIMIT 20
    """


def _monthly_revenue(question: str) -> str:
    where_sql = _append_where(["order_status = 'completed'"], _order_date_filter(question, "order_date"))
    return f"""
        SELECT strftime('%Y-%m', order_date) AS month,
               SUM(total_amount) AS revenue,
               COUNT(order_id) AS order_count
        FROM orders
        {where_sql}
        GROUP BY month
        ORDER BY month DESC LIMIT 24
    """


def _revenue_by_country(question: str) -> str:
    where_sql = _append_where(["o.order_status = 'completed'"], _order_date_filter(question))
    return f"""
        SELECT c.country, SUM(o.total_amount) AS revenue,
               COUNT(o.order_id) AS order_count
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        {where_sql}
        GROUP BY c.country
        ORDER BY revenue DESC LIMIT 20
    """


def _flagged_orders(question: str) -> str:
    threshold = _extract_amount_threshold(question)
    extra = f"o.total_amount > {threshold:.2f}" if threshold is not None else ""
    where_sql = _append_where(["o.is_flagged = 1"], extra)
    return f"""
        SELECT o.order_id, c.country, p.name, o.total_amount,
               o.payment_method, o.order_date, c.risk_score
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN products p ON o.product_id = p.product_id
        {where_sql}
        ORDER BY o.total_amount DESC LIMIT 50
    """


def _high_risk_customers(_: str) -> str:
    return """
        SELECT c.customer_id, c.country, c.customer_segment,
               c.risk_score, c.total_lifetime_value,
               COUNT(o.order_id) AS total_orders
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        WHERE c.risk_score > 0.7
        GROUP BY c.customer_id
        ORDER BY c.risk_score DESC LIMIT 50
    """


def _fraud_events(_: str) -> str:
    return """
        SELECT event_type, COUNT(*) AS count,
               SUM(amount_at_risk) AS total_at_risk,
               ROUND(AVG(amount_at_risk), 2) AS avg_amount
        FROM fraud_events
        GROUP BY event_type
        ORDER BY total_at_risk DESC LIMIT 10
    """


def _chargebacks(question: str) -> str:
    where_sql = _append_where(["event_type = 'chargeback'"], _order_date_filter(question, "event_date"))
    return f"""
        SELECT strftime('%Y-%m', event_date) AS month,
               COUNT(*) AS count,
               SUM(amount_at_risk) AS total_at_risk
        FROM fraud_events
        {where_sql}
        GROUP BY month
        ORDER BY month DESC LIMIT 12
    """


def _low_stock_products(_: str) -> str:
    return """
        SELECT name, category, brand, stock_quantity, unit_price
        FROM products
        WHERE stock_quantity < 20
        ORDER BY stock_quantity ASC LIMIT 50
    """


def _top_categories(_: str) -> str:
    return """
        SELECT category, COUNT(*) AS product_count,
               ROUND(AVG(unit_price), 2) AS avg_price,
               SUM(stock_quantity) AS total_stock
        FROM products
        GROUP BY category
        ORDER BY product_count DESC LIMIT 20
    """


def _return_rate(_: str) -> str:
    return """
        SELECT p.category,
               COUNT(r.return_id) AS returns,
               COUNT(o.order_id) AS total_orders,
               ROUND(100.0 * COUNT(r.return_id) / COUNT(o.order_id), 2) AS return_rate_pct
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        LEFT JOIN returns r ON o.order_id = r.order_id
        GROUP BY p.category
        ORDER BY return_rate_pct DESC LIMIT 20
    """


def _return_reasons(_: str) -> str:
    return """
        SELECT return_reason, COUNT(*) AS count,
               SUM(refund_amount) AS total_refunded
        FROM returns
        GROUP BY return_reason
        ORDER BY count DESC LIMIT 10
    """


def _payment_methods(_: str) -> str:
    return """
        SELECT payment_method, COUNT(*) AS order_count,
               SUM(total_amount) AS revenue,
               ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM orders), 2) AS pct
        FROM orders
        WHERE order_status = 'completed'
        GROUP BY payment_method
        ORDER BY revenue DESC LIMIT 20
    """


def _new_customers(question: str) -> str:
    months_match = re.search(r"last\s+(\d+)\s+months", question.lower())
    where_sql = f" WHERE date(created_at) >= date('now', '-{months_match.group(1)} months')" if months_match else ""
    return f"""
        SELECT strftime('%Y-%m', created_at) AS month, COUNT(*) AS new_customers
        FROM customers
        {where_sql}
        GROUP BY month
        ORDER BY month DESC LIMIT 12
    """


def _customer_segments(_: str) -> str:
    return """
        SELECT customer_segment,
               COUNT(*) AS customer_count,
               ROUND(AVG(total_lifetime_value), 2) AS avg_ltv,
               ROUND(AVG(risk_score), 3) AS avg_risk_score
        FROM customers
        GROUP BY customer_segment
        ORDER BY customer_count DESC LIMIT 20
    """


def _top_customers(question: str) -> str:
    limit = _extract_top_n(question, 20)
    return f"""
        SELECT c.customer_id, c.country, c.customer_segment,
               c.total_lifetime_value, COUNT(o.order_id) AS order_count
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id
        ORDER BY c.total_lifetime_value DESC LIMIT {limit}
    """


def _order_status_breakdown(_: str) -> str:
    return """
        SELECT order_status, COUNT(*) AS count,
               SUM(total_amount) AS total_value,
               ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM orders), 2) AS pct
        FROM orders
        GROUP BY order_status
        ORDER BY count DESC LIMIT 20
    """


def _cancelled_orders(question: str) -> str:
    where_sql = _append_where(["order_status = 'cancelled'"], _order_date_filter(question, "order_date"))
    return f"""
        SELECT strftime('%Y-%m', order_date) AS month,
               COUNT(*) AS cancellations,
               SUM(total_amount) AS lost_revenue
        FROM orders
        {where_sql}
        GROUP BY month
        ORDER BY month DESC LIMIT 12
    """


def _profit_by_category(question: str) -> str:
    where_sql = _append_where(["o.order_status = 'completed'"], _order_date_filter(question))
    return f"""
        SELECT p.category,
               SUM(o.total_amount) AS revenue,
               SUM(o.quantity * p.cost_price) AS total_cost,
               ROUND(SUM(o.total_amount) - SUM(o.quantity * p.cost_price), 2) AS gross_profit,
               ROUND(
                   100.0 * (SUM(o.total_amount) - SUM(o.quantity * p.cost_price)) / SUM(o.total_amount),
                   2
               ) AS margin_pct
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        {where_sql}
        GROUP BY p.category
        ORDER BY gross_profit DESC LIMIT 20
    """


def _fulfillment_time(_: str) -> str:
    return """
        SELECT p.category,
               ROUND(AVG(o.fulfillment_days), 1) AS avg_days,
               MIN(o.fulfillment_days) AS min_days,
               MAX(o.fulfillment_days) AS max_days
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        WHERE o.order_status = 'completed' AND o.fulfillment_days IS NOT NULL
        GROUP BY p.category
        ORDER BY avg_days ASC LIMIT 20
    """


def _default_summary(_: str) -> str:
    return """
        SELECT 'total_orders' AS metric, COUNT(*) AS value FROM orders
        UNION ALL
        SELECT 'total_revenue', ROUND(SUM(total_amount), 2) FROM orders WHERE order_status = 'completed'
        UNION ALL
        SELECT 'flagged_orders', COUNT(*) FROM orders WHERE is_flagged = 1
        UNION ALL
        SELECT 'total_customers', COUNT(*) FROM customers
        UNION ALL
        SELECT 'total_products', COUNT(*) FROM products
        LIMIT 10
    """


RULE_PATTERNS: list[tuple[Callable[[str], str], tuple[str, ...]]] = [
    (_top_products_revenue, ("top", "product", "revenue")),
    (_revenue_by_category, ("revenue", "category")),
    (_monthly_revenue, ("monthly", "revenue")),
    (_monthly_revenue, ("revenue", "trend")),
    (_revenue_by_country, ("revenue", "country")),
    (_flagged_orders, ("flagged", "order")),
    (_high_risk_customers, ("risk", "customer")),
    (_fraud_events, ("fraud", "event")),
    (_chargebacks, ("chargeback",)),
    (_low_stock_products, ("stock",)),
    (_top_categories, ("categor", "summary")),
    (_return_rate, ("return", "rate")),
    (_return_reasons, ("return", "reason")),
    (_payment_methods, ("payment",)),
    (_new_customers, ("new", "customer")),
    (_customer_segments, ("segment",)),
    (_top_customers, ("top", "customer")),
    (_order_status_breakdown, ("order", "status")),
    (_cancelled_orders, ("cancel",)),
    (_profit_by_category, ("profit", "category")),
    (_profit_by_category, ("margin", "category")),
    (_fulfillment_time, ("fulfillment",)),
]


def keyword_match(question: str) -> str:
    question_lower = question.lower()
    best_score = (0.0, 0)
    best_builder: Optional[Callable[[str], str]] = None

    for builder, tokens in RULE_PATTERNS:
        matched_tokens = sum(1 for token in tokens if token in question_lower)
        if matched_tokens == 0:
            continue

        score = (matched_tokens / len(tokens), matched_tokens)
        if score > best_score:
            best_score = score
            best_builder = builder

    if best_builder:
        return best_builder(question).strip()

    return _default_summary(question).strip()


def convert_to_sql(question: str) -> Dict[str, str]:
    sql = llm_to_sql(question)
    source = "llm"

    if not sql:
        sql = keyword_match(question)
        source = "rule_based"

    return {"sql": sql.strip(), "source": source}
