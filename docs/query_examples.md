# Query Examples

The following 15 natural language questions serve as a benchmark for the NL→SQL system.

## 1. Revenue & Sales
1. **Total revenue**: "What is our total revenue for completed orders?"
   - `SELECT SUM(total_amount) FROM orders WHERE order_status = 'completed'`
2. **Category breakdown**: "Show revenue by product category"
   - `SELECT p.category, SUM(o.total_amount) as revenue FROM orders o JOIN products p ON o.product_id = p.product_id GROUP BY 1`
3. **Monthly trend**: "Show monthly sales for the last 12 months"
   - `SELECT strftime('%Y-%m', order_date) as month, SUM(total_amount) FROM orders WHERE order_date >= date('now', '-12 months') GROUP BY 1`

## 2. Risk & Fraud
4. **Flagged orders**: "List all flagged orders over $1000"
   - `SELECT * FROM orders WHERE is_flagged = 1 AND total_amount > 1000`
5. **High risk customers**: "Show me customers with risk scores above 0.8"
   - `SELECT * FROM customers WHERE risk_score > 0.8`
6. **Fraud event summary**: "Count fraud events by type"
   - `SELECT event_type, COUNT(*) FROM fraud_events GROUP BY 1`

## 3. Shipping & Fulfillment
7. **Avg fulfillment**: "Average days to fulfill orders by brand"
   - `SELECT p.brand, AVG(o.fulfillment_days) FROM orders o JOIN products p ON o.product_id = p.product_id GROUP BY 1`
8. **Pending items**: "How many pending orders do we have currently?"
   - `SELECT COUNT(*) FROM orders WHERE order_status = 'pending'`

## 4. Product Insights
9. **Low stock**: "Which products have less than 50 units left?"
   - `SELECT name, stock_quantity FROM products WHERE stock_quantity < 50`
10. **Top sellers**: "Top 5 products by quantity sold"
    - `SELECT p.name, SUM(o.quantity) as total FROM orders o JOIN products p ON o.product_id = p.product_id GROUP BY 1 ORDER BY 2 DESC LIMIT 5`

## 5. Refunds & Returns
11. **Return reasons**: "What are the top 3 reasons for returns?"
    - `SELECT return_reason, COUNT(*) FROM returns GROUP BY 1 ORDER BY 2 DESC LIMIT 3`
12. **Refund total**: "Total amount refunded this year"
    - `SELECT SUM(refund_amount) FROM returns WHERE strftime('%Y', return_date) = strftime('%Y', 'now')`

## 6. Geographic Distribution
13. **Country revenue**: "Which 5 countries generate the most revenue?"
    - `SELECT c.country, SUM(o.total_amount) FROM orders o JOIN customers c ON o.customer_id = c.customer_id GROUP BY 1 ORDER BY 2 DESC LIMIT 5`
14. **Regional risk**: "Average risk score by region"
    - `SELECT region, AVG(risk_score) FROM customers GROUP BY 1`

## 7. Complex Business Logic
15. **Profit Margin**: "What are the most profitable product categories?"
    - `SELECT p.category, SUM(o.total_amount - (o.quantity * p.cost_price)) as profit FROM orders o JOIN products p ON o.product_id = p.product_id GROUP BY 1 ORDER BY 2 DESC`
