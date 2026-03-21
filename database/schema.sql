CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    subcategory TEXT,
    brand TEXT,
    unit_price REAL NOT NULL,
    cost_price REAL NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    supplier_country TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    country TEXT NOT NULL,
    region TEXT,
    customer_segment TEXT,
    account_age_days INTEGER DEFAULT 0,
    total_lifetime_value REAL DEFAULT 0,
    risk_score REAL DEFAULT 0.0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT REFERENCES customers(customer_id),
    product_id TEXT REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    total_amount REAL NOT NULL,
    discount_applied REAL DEFAULT 0.0,
    order_status TEXT DEFAULT 'completed',
    payment_method TEXT,
    is_flagged INTEGER DEFAULT 0,
    order_date TEXT NOT NULL,
    fulfillment_days INTEGER
);

CREATE TABLE IF NOT EXISTS returns (
    return_id TEXT PRIMARY KEY,
    order_id TEXT REFERENCES orders(order_id),
    customer_id TEXT REFERENCES customers(customer_id),
    product_id TEXT REFERENCES products(product_id),
    return_reason TEXT,
    refund_amount REAL,
    return_date TEXT
);

CREATE TABLE IF NOT EXISTS fraud_events (
    event_id TEXT PRIMARY KEY,
    customer_id TEXT REFERENCES customers(customer_id),
    order_id TEXT REFERENCES orders(order_id),
    event_type TEXT,
    amount_at_risk REAL,
    resolved INTEGER DEFAULT 0,
    event_date TEXT
);
