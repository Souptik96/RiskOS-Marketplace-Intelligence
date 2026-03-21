import calendar
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Sequence

import numpy as np

from app.db import resolve_db_path


SEED = 42
NOW = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
ROOT_DIR = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT_DIR / "database" / "schema.sql"

PRODUCT_COUNT = 200
CUSTOMER_COUNT = 1000
ORDER_COUNT = 15000
RETURN_COUNT = 1500
FRAUD_EVENT_COUNT = 200


CATEGORY_CONFIG = {
    "Electronics": {
        "subcategories": ["Laptop", "Phone", "Tablet", "Headphones", "Camera"],
        "brands": ["TechPro", "NovaBrand", "VoltEdge", "PixelWare", "Auralink"],
        "adjectives": ["Ultra", "Prime", "Core", "Pulse", "Edge"],
        "price_range": (99, 2500),
        "suppliers": ["China", "Japan", "South Korea", "Taiwan"],
        "fulfillment_values": [2, 3, 3, 4, 5, 6],
    },
    "Clothing": {
        "subcategories": ["Jacket", "Sneakers", "Hoodie", "Dress", "Denim"],
        "brands": ["ThreadWorks", "UrbanTrail", "LuxeWeave", "HarborLine", "Northloom"],
        "adjectives": ["Classic", "Modern", "Studio", "Seasonal", "Signature"],
        "price_range": (18, 320),
        "suppliers": ["India", "Bangladesh", "Vietnam", "Turkey"],
        "fulfillment_values": [2, 3, 4, 5, 6],
    },
    "Home & Garden": {
        "subcategories": ["Lamp", "Planter", "Cookware", "Desk", "Bedding"],
        "brands": ["HearthCo", "GreenNest", "Oakline", "CasaMotive", "TerraVale"],
        "adjectives": ["Everyday", "Premium", "Compact", "Heritage", "Modern"],
        "price_range": (12, 780),
        "suppliers": ["Poland", "Germany", "China", "Vietnam"],
        "fulfillment_values": [2, 3, 4, 5, 7],
    },
    "Sports": {
        "subcategories": ["Bike", "Yoga Mat", "Treadmill", "Racket", "Backpack"],
        "brands": ["PeakMotion", "StrideLab", "SummitFlex", "TrailForge", "PulsePeak"],
        "adjectives": ["Active", "Elite", "Trail", "Pro", "Advance"],
        "price_range": (15, 1800),
        "suppliers": ["China", "Germany", "Taiwan", "Vietnam"],
        "fulfillment_values": [2, 3, 4, 5, 6],
    },
    "Books": {
        "subcategories": ["Novel", "Cookbook", "Workbook", "Biography", "Guide"],
        "brands": ["PaperTrail", "BlueShelf", "NorthPage", "Everbound", "SummitPress"],
        "adjectives": ["Essential", "Illustrated", "Collected", "Practical", "Field"],
        "price_range": (5, 65),
        "suppliers": ["US", "UK", "Germany", "India"],
        "fulfillment_values": [2, 3, 4],
    },
    "Beauty": {
        "subcategories": ["Serum", "Cleanser", "Palette", "Fragrance", "Moisturizer"],
        "brands": ["Veloura", "PureBloom", "LumaSkin", "AetherGlow", "MiraBelle"],
        "adjectives": ["Radiant", "Daily", "Restore", "Hydra", "Velvet"],
        "price_range": (8, 240),
        "suppliers": ["France", "South Korea", "US", "Japan"],
        "fulfillment_values": [1, 2, 2, 3, 4],
    },
    "Food": {
        "subcategories": ["Coffee", "Chocolate", "Protein Mix", "Snack Box", "Olive Oil"],
        "brands": ["Harvest Table", "SunVale", "DailyCrate", "Root & Grain", "Summit Pantry"],
        "adjectives": ["Organic", "Fresh", "Signature", "Artisan", "Premium"],
        "price_range": (6, 120),
        "suppliers": ["Italy", "Spain", "US", "India"],
        "fulfillment_values": [1, 1, 1, 2, 2, 3],
    },
    "Automotive": {
        "subcategories": ["Dash Cam", "Seat Cover", "Battery Pack", "Tool Kit", "LED Kit"],
        "brands": ["RoadPrime", "TorqueLine", "MotorVale", "DriveCore", "AutoForge"],
        "adjectives": ["Touring", "Garage", "Heavy Duty", "Pro", "Everyday"],
        "price_range": (14, 950),
        "suppliers": ["Germany", "Japan", "US", "China"],
        "fulfillment_values": [2, 3, 4, 5, 6],
    },
}

OTHER_COUNTRIES = ["France", "Australia", "Brazil", "Japan", "UAE", "Singapore", "Netherlands"]
REGION_MAP = {
    "US": "North America",
    "Canada": "North America",
    "UK": "Europe",
    "Germany": "Europe",
    "France": "Europe",
    "Netherlands": "Europe",
    "India": "Asia",
    "Japan": "Asia",
    "Singapore": "Asia",
    "UAE": "Middle East",
    "Australia": "Oceania",
    "Brazil": "South America",
}


def _format_ts(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _shift_months(base: datetime, months: int) -> datetime:
    month_index = base.month - 1 + months
    year = base.year + month_index // 12
    month = month_index % 12 + 1
    return datetime(year, month, 1, base.hour, base.minute, base.second)


def _allocate_counts(total: int, weights: Sequence[float]) -> List[int]:
    normalized = np.array(weights, dtype=float)
    normalized = normalized / normalized.sum()
    raw = normalized * total
    counts = np.floor(raw).astype(int)
    remainder = total - counts.sum()
    if remainder > 0:
        order = np.argsort(raw - counts)[::-1]
        for index in order[:remainder]:
            counts[index] += 1
    return counts.tolist()


def _build_products(rng: random.Random) -> tuple[List[tuple], Dict[str, Dict], set[str]]:
    margin_modes = ["high"] * 30 + ["low"] * 60 + ["normal"] * (PRODUCT_COUNT - 90)
    rng.shuffle(margin_modes)

    products: List[tuple] = []
    product_lookup: Dict[str, Dict] = {}
    product_ids: List[str] = []
    product_index = 1

    for category, config in CATEGORY_CONFIG.items():
        for _ in range(25):
            product_id = f"P{product_index:04d}"
            brand = rng.choice(config["brands"])
            adjective = rng.choice(config["adjectives"])
            subcategory = rng.choice(config["subcategories"])
            name = f"{brand} {adjective} {subcategory}"
            unit_price = round(rng.uniform(*config["price_range"]), 2)

            margin_mode = margin_modes[product_index - 1]
            if margin_mode == "high":
                cost_ratio = rng.uniform(0.38, 0.48)
            elif margin_mode == "low":
                cost_ratio = rng.uniform(0.76, 0.9)
            else:
                cost_ratio = rng.uniform(0.5, 0.7)

            cost_price = round(unit_price * cost_ratio, 2)
            stock_quantity = rng.randint(0, 500)
            supplier_country = rng.choice(config["suppliers"])
            created_at = NOW - timedelta(days=rng.randint(30, 1400))

            products.append(
                (
                    product_id,
                    name,
                    category,
                    subcategory,
                    brand,
                    unit_price,
                    cost_price,
                    stock_quantity,
                    supplier_country,
                    _format_ts(created_at),
                )
            )
            product_lookup[product_id] = {
                "category": category,
                "unit_price": unit_price,
                "cost_price": cost_price,
                "fulfillment_values": config["fulfillment_values"],
            }
            product_ids.append(product_id)
            product_index += 1

    top_products = set(rng.sample(product_ids, 30))
    return products, product_lookup, top_products


def _build_customers(rng: random.Random, np_rng: np.random.Generator) -> tuple[List[tuple], Dict[str, Dict]]:
    country_values = (
        ["US"] * 400
        + ["UK"] * 150
        + ["Germany"] * 100
        + ["Canada"] * 100
        + ["India"] * 80
        + [rng.choice(OTHER_COUNTRIES) for _ in range(170)]
    )
    segment_values = ["retail"] * 600 + ["wholesale"] * 250 + ["enterprise"] * 150
    rng.shuffle(country_values)
    rng.shuffle(segment_values)

    age_values = np.clip(np_rng.gamma(shape=2.5, scale=160.0, size=CUSTOMER_COUNT), 0, 2000).astype(int)
    risk_values = np.concatenate([np_rng.uniform(0.71, 0.99, size=150), np_rng.uniform(0.01, 0.69, size=850)])
    np_rng.shuffle(risk_values)

    customers: List[tuple] = []
    customer_lookup: Dict[str, Dict] = {}

    for index in range(CUSTOMER_COUNT):
        customer_id = f"C{index + 1:04d}"
        country = country_values[index]
        segment = segment_values[index]
        age_days = int(age_values[index])
        risk_score = round(float(risk_values[index]), 3)
        created_at = NOW - timedelta(days=age_days)
        customers.append(
            (
                customer_id,
                country,
                REGION_MAP.get(country, "International"),
                segment,
                age_days,
                0.0,
                risk_score,
                _format_ts(created_at),
            )
        )
        customer_lookup[customer_id] = {
            "segment": segment,
            "account_age_days": age_days,
            "risk_score": risk_score,
        }

    return customers, customer_lookup


def _build_order_dates(np_rng: np.random.Generator) -> List[datetime]:
    current_month_start = datetime(NOW.year, NOW.month, 1)
    months = [_shift_months(current_month_start, offset) for offset in range(-23, 1)]
    weights = []
    for index, month_start in enumerate(months):
        weight = 1.0 + (index * 0.035)
        if month_start.month == 12:
            weight *= 1.3
        elif month_start.month == 7:
            weight *= 1.2
        weights.append(weight)

    month_counts = _allocate_counts(ORDER_COUNT, weights)
    order_dates: List[datetime] = []
    for month_start, month_count in zip(months, month_counts):
        last_day = calendar.monthrange(month_start.year, month_start.month)[1]
        for _ in range(month_count):
            order_dates.append(
                month_start.replace(
                    day=int(np_rng.integers(1, last_day + 1)),
                    hour=int(np_rng.integers(0, 24)),
                    minute=int(np_rng.integers(0, 60)),
                    second=int(np_rng.integers(0, 60)),
                )
            )
    np_rng.shuffle(order_dates)
    return order_dates


def _build_orders(
    rng: random.Random,
    np_rng: np.random.Generator,
    product_lookup: Dict[str, Dict],
    customer_lookup: Dict[str, Dict],
    top_products: set[str],
) -> tuple[List[tuple], List[Dict]]:
    top_product_ids = list(top_products)
    other_product_ids = [product_id for product_id in product_lookup if product_id not in top_products]
    top_weights = np.array([1.0 + (30 - index) * 0.08 for index in range(len(top_product_ids))], dtype=float)
    top_weights = top_weights / top_weights.sum()
    other_weights = np.array([1.0 for _ in other_product_ids], dtype=float)
    other_weights = other_weights / other_weights.sum()

    product_assignments = list(np_rng.choice(top_product_ids, size=9000, replace=True, p=top_weights))
    product_assignments += list(np_rng.choice(other_product_ids, size=6000, replace=True, p=other_weights))
    rng.shuffle(product_assignments)

    statuses = ["completed"] * 12000 + ["returned"] * 1500 + ["cancelled"] * 1050 + ["pending"] * 450
    payments = ["card"] * 9750 + ["bank_transfer"] * 3000 + ["crypto"] * 1200 + ["cash"] * 1050
    rng.shuffle(statuses)
    rng.shuffle(payments)

    customer_ids = list(customer_lookup.keys())
    customer_weights = []
    for customer_id in customer_ids:
        info = customer_lookup[customer_id]
        weight = 1.0
        if info["segment"] == "wholesale":
            weight *= 1.3
        elif info["segment"] == "enterprise":
            weight *= 1.1
        weight *= 1.0 + min(info["account_age_days"] / 2000.0, 0.8)
        customer_weights.append(weight)
    customer_weights = np.array(customer_weights, dtype=float)
    customer_weights = customer_weights / customer_weights.sum()

    selected_customers = np_rng.choice(customer_ids, size=ORDER_COUNT, replace=True, p=customer_weights)
    order_dates = _build_order_dates(np_rng)

    order_records: List[Dict] = []
    for index in range(ORDER_COUNT):
        customer_id = str(selected_customers[index])
        product_id = str(product_assignments[index])
        customer = customer_lookup[customer_id]
        product = product_lookup[product_id]
        status = statuses[index]
        payment_method = payments[index]

        if customer["segment"] == "retail":
            quantity = int(np_rng.choice([1, 1, 1, 2, 2, 3]))
            discount_rate = rng.uniform(0.0, 0.08)
        elif customer["segment"] == "wholesale":
            quantity = int(np_rng.choice([2, 3, 4, 5, 6, 8, 10, 12]))
            discount_rate = rng.uniform(0.05, 0.18)
        else:
            quantity = int(np_rng.choice([3, 5, 8, 10, 12, 15, 20, 25]))
            discount_rate = rng.uniform(0.03, 0.12)

        unit_price = round(product["unit_price"] * rng.uniform(0.97, 1.03), 2)
        gross_amount = quantity * unit_price
        discount_applied = round(gross_amount * discount_rate, 2)
        total_amount = round(max(gross_amount - discount_applied, 1.0), 2)
        fulfillment_days = None if status == "pending" else int(rng.choice(product["fulfillment_values"]))

        order_records.append(
            {
                "order_id": f"O{index + 1:05d}",
                "customer_id": customer_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "discount_applied": discount_applied,
                "order_status": status,
                "payment_method": payment_method,
                "is_flagged": 0,
                "order_date": _format_ts(order_dates[index]),
                "fulfillment_days": fulfillment_days,
                "risk_score": customer["risk_score"],
            }
        )

    scores = []
    for record in order_records:
        score = 1.0 + (record["risk_score"] * 6.0)
        if record["payment_method"] == "crypto":
            score += 1.5
        if record["order_status"] == "returned":
            score += 1.0
        if record["total_amount"] > 1000:
            score += 1.2
        scores.append(score)

    probabilities = np.array(scores, dtype=float)
    probabilities = probabilities / probabilities.sum()
    flagged_indexes = np_rng.choice(np.arange(ORDER_COUNT), size=750, replace=False, p=probabilities)
    flagged_set = {int(index) for index in flagged_indexes}

    rows: List[tuple] = []
    for index, record in enumerate(order_records):
        if index in flagged_set:
            record["is_flagged"] = 1
        rows.append(
            (
                record["order_id"],
                record["customer_id"],
                record["product_id"],
                record["quantity"],
                record["unit_price"],
                record["total_amount"],
                record["discount_applied"],
                record["order_status"],
                record["payment_method"],
                record["is_flagged"],
                record["order_date"],
                record["fulfillment_days"],
            )
        )

    return rows, order_records


def _build_returns(rng: random.Random, order_records: List[Dict]) -> List[tuple]:
    returned_orders = [record for record in order_records if record["order_status"] == "returned"]
    return_reasons = (
        ["defective"] * 450
        + ["wrong_item"] * 300
        + ["not_as_described"] * 375
        + ["changed_mind"] * 225
        + ["fraud_suspected"] * 150
    )
    rng.shuffle(return_reasons)

    rows: List[tuple] = []
    for index, (order_record, reason) in enumerate(zip(returned_orders, return_reasons), start=1):
        order_date = datetime.strptime(order_record["order_date"], "%Y-%m-%d %H:%M:%S")
        refund_multiplier = {
            "defective": 1.0,
            "wrong_item": 1.0,
            "not_as_described": 0.95,
            "changed_mind": 0.8,
            "fraud_suspected": 1.0,
        }[reason]
        rows.append(
            (
                f"R{index:05d}",
                order_record["order_id"],
                order_record["customer_id"],
                order_record["product_id"],
                reason,
                round(order_record["total_amount"] * refund_multiplier, 2),
                _format_ts(order_date + timedelta(days=rng.randint(2, 35))),
            )
        )

    return rows


def _build_fraud_events(rng: random.Random, np_rng: np.random.Generator, order_records: List[Dict]) -> List[tuple]:
    candidate_orders = [record for record in order_records if record["risk_score"] > 0.7]
    weights = []
    for record in candidate_orders:
        weight = 1.0 + (record["risk_score"] * 5.0)
        if record["is_flagged"]:
            weight += 2.0
        if record["payment_method"] == "crypto":
            weight += 1.0
        weights.append(weight)
    probabilities = np.array(weights, dtype=float)
    probabilities = probabilities / probabilities.sum()

    selected_indexes = np_rng.choice(np.arange(len(candidate_orders)), size=FRAUD_EVENT_COUNT, replace=False, p=probabilities)
    event_types = ["chargeback"] * 80 + ["return_fraud"] * 60 + ["identity_theft"] * 40 + ["account_takeover"] * 20
    rng.shuffle(event_types)

    rows: List[tuple] = []
    for index, (candidate_index, event_type) in enumerate(zip(selected_indexes, event_types), start=1):
        order_record = candidate_orders[int(candidate_index)]
        order_date = datetime.strptime(order_record["order_date"], "%Y-%m-%d %H:%M:%S")
        rows.append(
            (
                f"F{index:04d}",
                order_record["customer_id"],
                order_record["order_id"],
                event_type,
                round(order_record["total_amount"] * rng.uniform(0.8, 1.35), 2),
                1 if rng.random() < 0.62 else 0,
                _format_ts(order_date + timedelta(days=rng.randint(0, 45))),
            )
        )

    return rows


def _update_customer_ltv(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE customers
        SET total_lifetime_value = COALESCE(
            (
                SELECT ROUND(SUM(total_amount), 2)
                FROM orders
                WHERE orders.customer_id = customers.customer_id
                  AND orders.order_status IN ('completed', 'returned')
            ),
            0
        )
        """
    )


def _verify_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for table in ["products", "customers", "orders", "returns", "fraud_events"]:
        counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return counts


def seed_database(db_path: str | Path | None = None) -> Dict[str, int]:
    rng = random.Random(SEED)
    np_rng = np.random.default_rng(SEED)
    database_path = resolve_db_path(str(db_path) if db_path else None)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(database_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))

        for table in ["fraud_events", "returns", "orders", "customers", "products"]:
            conn.execute(f"DELETE FROM {table}")

        products, product_lookup, top_products = _build_products(rng)
        customers, customer_lookup = _build_customers(rng, np_rng)
        orders, order_records = _build_orders(rng, np_rng, product_lookup, customer_lookup, top_products)
        returns = _build_returns(rng, order_records)
        fraud_events = _build_fraud_events(rng, np_rng, order_records)

        conn.executemany(
            """
            INSERT INTO products (
                product_id, name, category, subcategory, brand, unit_price,
                cost_price, stock_quantity, supplier_country, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            products,
        )
        conn.executemany(
            """
            INSERT INTO customers (
                customer_id, country, region, customer_segment, account_age_days,
                total_lifetime_value, risk_score, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            customers,
        )
        conn.executemany(
            """
            INSERT INTO orders (
                order_id, customer_id, product_id, quantity, unit_price, total_amount,
                discount_applied, order_status, payment_method, is_flagged,
                order_date, fulfillment_days
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            orders,
        )
        conn.executemany(
            """
            INSERT INTO returns (
                return_id, order_id, customer_id, product_id, return_reason,
                refund_amount, return_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            returns,
        )
        conn.executemany(
            """
            INSERT INTO fraud_events (
                event_id, customer_id, order_id, event_type, amount_at_risk,
                resolved, event_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            fraud_events,
        )

        _update_customer_ltv(conn)
        conn.commit()
        counts = _verify_counts(conn)

    expected = {
        "products": PRODUCT_COUNT,
        "customers": CUSTOMER_COUNT,
        "orders": ORDER_COUNT,
        "returns": RETURN_COUNT,
        "fraud_events": FRAUD_EVENT_COUNT,
    }
    for table, expected_count in expected.items():
        actual = counts[table]
        if actual != expected_count:
            raise RuntimeError(f"{table} count mismatch: expected {expected_count}, got {actual}")

    return counts


if __name__ == "__main__":
    result = seed_database()
    for table, count in result.items():
        print(f"{table}: {count}")
