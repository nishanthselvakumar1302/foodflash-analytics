"""
FoodFlash Analytics — Data Generation Script
=============================================
Generates realistic food delivery data for 5 tables and loads them
into PostgreSQL to simulate a real food delivery app's source database.

Tables created:
  - customers    : 5,000 rows
  - restaurants  : 500 rows
  - riders       : 300 rows
  - orders       : 50,000 rows
  - order_items  : ~150,000 rows (3 items per order on average)

Usage:
  python scripts/generate_data.py

Requirements:
  pip install faker sqlalchemy psycopg2-binary pandas python-dotenv
"""

# ─────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────
import os
import random
import uuid
import math
import time
import sys
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
from sqlalchemy import (
    create_engine, text,
    Column, String, Float, Integer, Boolean, DateTime, Date, Numeric
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# CONFIG — edit here OR use a .env file
# ─────────────────────────────────────────────
load_dotenv()  # loads variables from .env if it exists

DB_HOST     = os.getenv("PG_HOST",     "localhost")
DB_PORT     = os.getenv("PG_PORT",     "5432")
DB_NAME     = os.getenv("PG_DATABASE", "foodflash_db")
DB_USER     = os.getenv("PG_USER",     "postgres")
DB_PASSWORD = os.getenv("PG_PASSWORD", "Nishant_1302")   # ← change this if needed

RANDOM_SEED   = 42      # makes data reproducible
NUM_CUSTOMERS = 5_000
NUM_RESTAURANTS = 500
NUM_RIDERS    = 300
NUM_ORDERS    = 50_000
CHUNK_SIZE    = 5_000   # rows per DB insert batch

# ─────────────────────────────────────────────
# REFERENCE DATA — realistic Indian food delivery
# ─────────────────────────────────────────────
CITIES = [
    "Chennai", "Mumbai", "Delhi", "Bangalore",
    "Hyderabad", "Pune", "Kolkata", "Ahmedabad"
]

# City weights — bigger cities get more orders
CITY_WEIGHTS = [0.18, 0.22, 0.20, 0.17, 0.10, 0.07, 0.04, 0.02]

RESTAURANT_CATEGORIES = [
    "Biryani", "Pizza", "Chinese", "South Indian",
    "North Indian", "Desserts", "Burgers", "Rolls & Wraps",
    "Healthy Bowls", "Seafood"
]

# Category weights — biryani and south indian dominate in India
CATEGORY_WEIGHTS = [0.22, 0.15, 0.12, 0.18, 0.13, 0.06, 0.07, 0.04, 0.02, 0.01]

RESTAURANT_NAMES_BY_CATEGORY = {
    "Biryani":       ["Biryani Blues", "Paradise Biryani", "Behrouz Biryani",
                      "Biryani House", "Royal Biryani", "Hyderabadi Dum",
                      "Dum Pukht", "Biryani Pot", "Star Biryani", "Golden Spoon"],
    "Pizza":         ["Pizza Palace", "Slice Republic", "Cheesy Bites",
                      "The Pizza Lab", "Thin Crust Co", "Wood Fire Pizza",
                      "Margherita Magic", "Crust & Co", "Roma Pizza", "FirePizza"],
    "Chinese":       ["Chow King", "Wok Express", "Noodle Box", "Dragon Bowl",
                      "Ming Garden", "Golden Dragon", "Szechuan Street",
                      "Orient Express", "Bamboo Kitchen", "Jade Palace"],
    "South Indian":  ["Saravana Bhavan", "Murugan Idli", "Dosa Plaza",
                      "Udupi Garden", "Annapoorna", "MTR Express",
                      "Chettinad Kitchen", "Tiffin Box", "Idly Kadai", "Dosa Corner"],
    "North Indian":  ["Moti Mahal", "Punjabi Tadka", "Dal Makhani House",
                      "Butter Chicken Hut", "Dhaba Express", "Frontier Grill",
                      "Kabab Corner", "Curry Leaf", "Spice Route", "Tandoor Nights"],
    "Desserts":      ["Wow Momo Sweets", "Kulfi Corner", "Gulab Jamun Shop",
                      "Sweet Tooth", "Mithai Express", "Pastry Palace",
                      "Brownie Point", "Ice Cream Lab", "Halwa House", "Rabri Corner"],
    "Burgers":       ["Burger Singh", "Stack'd Burgers", "The Burger Club",
                      "Bun & Patty", "Grill House", "SmashBurger"],
    "Rolls & Wraps": ["Frankie Express", "Roll Station", "The Wrap Co",
                      "Kati Roll House", "Wrap & Roll"],
    "Healthy Bowls": ["Green Bowl", "Salad Days", "The Grain Bowl",
                      "Fit Food Co", "Nourish"],
    "Seafood":       ["Sea Spice", "Coastal Kitchen", "The Fish Curry",
                      "Prawn Palace", "Marine Plate"],
}

PAYMENT_METHODS  = ["UPI", "Card", "Cash on Delivery", "Wallet", "Net Banking"]
PAYMENT_WEIGHTS  = [0.48, 0.22, 0.15, 0.12, 0.03]

VEHICLE_TYPES    = ["Bike", "Scooter", "Cycle"]
VEHICLE_WEIGHTS  = [0.55, 0.38, 0.07]

LOYALTY_TIERS    = ["Bronze", "Silver", "Gold", "Platinum"]
LOYALTY_WEIGHTS  = [0.50, 0.28, 0.16, 0.06]

ORDER_STATUSES   = ["delivered", "cancelled", "pending"]
STATUS_WEIGHTS   = [0.75, 0.15, 0.10]

# Price range by category (min, max) in INR
PRICE_RANGE = {
    "Biryani":       (180, 650),
    "Pizza":         (200, 800),
    "Chinese":       (150, 500),
    "South Indian":  (80,  350),
    "North Indian":  (150, 600),
    "Desserts":      (60,  300),
    "Burgers":       (120, 450),
    "Rolls & Wraps": (80,  250),
    "Healthy Bowls": (150, 500),
    "Seafood":       (200, 900),
}

# Menu items by category
MENU_ITEMS = {
    "Biryani":       ["Chicken Biryani", "Mutton Biryani", "Veg Biryani",
                      "Egg Biryani", "Prawn Biryani", "Hyderabadi Biryani"],
    "Pizza":         ["Margherita", "Pepperoni", "Farmhouse", "Paneer Tikka",
                      "BBQ Chicken", "Four Cheese", "Veggie Supreme"],
    "Chinese":       ["Fried Rice", "Hakka Noodles", "Manchurian", "Spring Rolls",
                      "Chilli Paneer", "Schezwan Noodles", "Dim Sum"],
    "South Indian":  ["Masala Dosa", "Idli Sambar", "Vada", "Uttapam",
                      "Pongal", "Medu Vada", "Ghee Roast Dosa"],
    "North Indian":  ["Butter Chicken", "Dal Makhani", "Paneer Butter Masala",
                      "Naan", "Roti", "Chicken Tikka", "Mutton Rogan Josh"],
    "Desserts":      ["Gulab Jamun", "Kulfi", "Rasgulla", "Jalebi",
                      "Brownie", "Ice Cream", "Halwa"],
    "Burgers":       ["Aloo Tikki Burger", "Chicken Burger", "Veg Supreme",
                      "Classic Smash", "BBQ Crunch"],
    "Rolls & Wraps": ["Egg Roll", "Paneer Kathi Roll", "Chicken Frankie",
                      "Veg Wrap", "Seekh Roll"],
    "Healthy Bowls": ["Quinoa Bowl", "Protein Salad", "Buddha Bowl",
                      "Green Smoothie Bowl", "Grain Bowl"],
    "Seafood":       ["Fish Curry", "Prawn Masala", "Crab Fry",
                      "Grilled Fish", "Seafood Biryani"],
}

DELIVERY_FEE_RANGE = (15, 60)   # INR

# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────

def print_progress(label: str, current: int, total: int) -> None:
    """Prints a simple progress bar to the console."""
    pct   = int((current / total) * 40)
    bar   = "█" * pct + "░" * (40 - pct)
    print(f"\r  {label}: [{bar}] {current:,}/{total:,}", end="", flush=True)


def generate_realistic_timestamp(start_date: datetime, end_date: datetime) -> datetime:
    """
    Returns a random datetime biased toward peak meal hours:
    12pm–2pm (lunch) and 7pm–10pm (dinner).
    Off-peak hours still get ~30% of orders to keep data realistic.
    """
    total_seconds = int((end_date - start_date).total_seconds())
    base_ts = start_date + timedelta(seconds=random.randint(0, total_seconds))

    # 70% chance — shift time into a peak window
    if random.random() < 0.70:
        if random.random() < 0.40:
            # Lunch peak: 12:00–14:00
            peak_hour   = random.randint(12, 13)
            peak_minute = random.randint(0, 59)
        else:
            # Dinner peak: 19:00–22:00
            peak_hour   = random.randint(19, 21)
            peak_minute = random.randint(0, 59)
        base_ts = base_ts.replace(hour=peak_hour, minute=peak_minute, second=random.randint(0, 59))

    return base_ts


def delivery_minutes_for_status(status: str) -> int:
    """Returns realistic delivery duration in minutes based on order status."""
    if status == "delivered":
        # Normal: 20–55 min; occasionally late (>60 min)
        if random.random() < 0.18:
            return random.randint(60, 90)   # late delivery
        return random.randint(20, 55)
    elif status == "cancelled":
        return random.randint(5, 25)        # cancelled early
    else:
        return 0                            # pending — not delivered yet


# ─────────────────────────────────────────────
# DATA GENERATORS
# ─────────────────────────────────────────────

def generate_customers(n: int, fake: Faker) -> pd.DataFrame:
    """Generates n customer records with realistic Indian names and phone numbers."""
    print(f"\n[1/5] Generating {n:,} customers...")
    rows = []

    for i in range(n):
        city = random.choices(CITIES, weights=CITY_WEIGHTS)[0]
        rows.append({
            "customer_id":   str(uuid.uuid4()),
            "name":          fake.name(),
            "email":         fake.unique.email(),
            "phone":         f"+91 {random.randint(7000000000, 9999999999)}",
            "city":          city,
            "registered_at": fake.date_time_between(start_date="-3y", end_date="-1d"),
            "loyalty_tier":  random.choices(LOYALTY_TIERS, weights=LOYALTY_WEIGHTS)[0],
        })
        if (i + 1) % 500 == 0:
            print_progress("customers", i + 1, n)

    print_progress("customers", n, n)
    print()
    return pd.DataFrame(rows)


def generate_restaurants(n: int, fake: Faker) -> pd.DataFrame:
    """Generates n restaurant records with categories, ratings, and city assignments."""
    print(f"\n[2/5] Generating {n:,} restaurants...")
    rows = []

    for i in range(n):
        city     = random.choices(CITIES, weights=CITY_WEIGHTS)[0]
        category = random.choices(RESTAURANT_CATEGORIES, weights=CATEGORY_WEIGHTS)[0]
        base_names = RESTAURANT_NAMES_BY_CATEGORY.get(category, ["Restaurant"])
        name     = random.choice(base_names)
        suffix   = random.choice(["", f" - {city}", f" ({city})", ""])
        rows.append({
            "restaurant_id": str(uuid.uuid4()),
            "name":          name + suffix,
            "city":          city,
            "category":      category,
            "rating":        round(random.uniform(2.8, 5.0), 1),
            "is_premium":    random.random() < 0.20,   # 20% are premium
            "opened_at":     fake.date_between(start_date="-5y", end_date="-30d"),
        })
        if (i + 1) % 50 == 0:
            print_progress("restaurants", i + 1, n)

    print_progress("restaurants", n, n)
    print()
    return pd.DataFrame(rows)


def generate_riders(n: int, fake: Faker) -> pd.DataFrame:
    """Generates n delivery rider records."""
    print(f"\n[3/5] Generating {n:,} riders...")
    rows = []

    for i in range(n):
        city = random.choices(CITIES, weights=CITY_WEIGHTS)[0]
        rows.append({
            "rider_id":     str(uuid.uuid4()),
            "name":         fake.name(),
            "city":         city,
            "vehicle_type": random.choices(VEHICLE_TYPES, weights=VEHICLE_WEIGHTS)[0],
            "joined_at":    fake.date_between(start_date="-4y", end_date="-15d"),
            "is_active":    random.random() < 0.85,   # 85% are currently active
        })
        if (i + 1) % 30 == 0:
            print_progress("riders", i + 1, n)

    print_progress("riders", n, n)
    print()
    return pd.DataFrame(rows)


def generate_orders(
    n: int,
    customers_df: pd.DataFrame,
    restaurants_df: pd.DataFrame,
    riders_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generates n order records.
    - Each order is assigned to a customer, restaurant, and rider in the same city.
    - Order amount is based on restaurant category price range.
    - Timestamps are biased toward peak meal hours.
    """
    print(f"\n[4/5] Generating {n:,} orders...")

    # Pre-index customers and restaurants by city for fast lookup
    customers_by_city = {
        city: grp["customer_id"].tolist()
        for city, grp in customers_df.groupby("city")
    }
    restaurants_by_city = {
        city: list(zip(grp["restaurant_id"], grp["category"]))
        for city, grp in restaurants_df.groupby("city")
    }
    riders_by_city = {
        city: grp["rider_id"].tolist()
        for city, grp in riders_df[riders_df["is_active"]].groupby("city")
    }

    start_date = datetime.now() - timedelta(days=365)
    end_date   = datetime.now() - timedelta(hours=2)

    rows = []
    for i in range(n):
        city = random.choices(CITIES, weights=CITY_WEIGHTS)[0]

        # Fallback if a city has no customers/restaurants/riders
        customer_pool    = customers_by_city.get(city) or customers_df["customer_id"].tolist()
        restaurant_pool  = restaurants_by_city.get(city) or list(
            zip(restaurants_df["restaurant_id"], restaurants_df["category"])
        )
        rider_pool       = riders_by_city.get(city) or riders_df["rider_id"].tolist()

        customer_id            = random.choice(customer_pool)
        restaurant_id, category = random.choice(restaurant_pool)
        rider_id               = random.choice(rider_pool)

        status         = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0]
        placed_at      = generate_realistic_timestamp(start_date, end_date)
        delivery_mins  = delivery_minutes_for_status(status)
        delivered_at   = placed_at + timedelta(minutes=delivery_mins) if status == "delivered" else None

        price_min, price_max = PRICE_RANGE.get(category, (100, 500))
        order_amount   = round(random.uniform(price_min, price_max), 2)

        # Discount: 0–30% of order amount, more likely on weekends
        discount_pct   = random.choices([0, 0.10, 0.15, 0.20, 0.30], weights=[0.40, 0.25, 0.18, 0.12, 0.05])[0]
        discount_amount = round(order_amount * discount_pct, 2)
        delivery_fee   = round(random.uniform(*DELIVERY_FEE_RANGE), 2)

        rows.append({
            "order_id":       str(uuid.uuid4()),
            "customer_id":    customer_id,
            "restaurant_id":  restaurant_id,
            "rider_id":       rider_id,
            "status":         status,
            "order_amount":   order_amount,
            "discount_amount": discount_amount,
            "delivery_fee":   delivery_fee,
            "placed_at":      placed_at,
            "delivered_at":   delivered_at,
            "city":           city,
            "payment_method": random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0],
        })

        if (i + 1) % 1000 == 0:
            print_progress("orders", i + 1, n)

    print_progress("orders", n, n)
    print()
    return pd.DataFrame(rows)


def generate_order_items(
    orders_df: pd.DataFrame,
    restaurants_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generates order items — approximately 3 items per order on average.
    Each item is from the menu of the restaurant's category.
    """
    print(f"\n[5/5] Generating order items (avg 3 per order)...")

    # Build restaurant_id → category lookup
    rest_category = dict(zip(restaurants_df["restaurant_id"], restaurants_df["category"]))

    rows = []
    total = len(orders_df)

    for i, (_, order) in enumerate(orders_df.iterrows()):
        category  = rest_category.get(order["restaurant_id"], "North Indian")
        menu      = MENU_ITEMS.get(category, ["Special Dish"])
        num_items = random.choices([1, 2, 3, 4, 5], weights=[0.15, 0.30, 0.30, 0.15, 0.10])[0]

        for _ in range(num_items):
            price_min, price_max = PRICE_RANGE.get(category, (100, 500))
            unit_price = round(random.uniform(price_min / 2, price_max / 2), 2)
            rows.append({
                "item_id":    str(uuid.uuid4()),
                "order_id":   order["order_id"],
                "item_name":  random.choice(menu),
                "quantity":   random.choices([1, 2, 3], weights=[0.65, 0.25, 0.10])[0],
                "unit_price": unit_price,
            })

        if (i + 1) % 2000 == 0:
            print_progress("order_items", i + 1, total)

    print_progress("order_items", total, total)
    print()
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# DATABASE OPERATIONS
# ─────────────────────────────────────────────

def get_engine(host, port, db, user, password):
    """Creates and returns a SQLAlchemy engine for PostgreSQL."""
    conn_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    try:
        engine = create_engine(conn_string, echo=False, future=True)
        # Test the connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"  ✓ Connected to PostgreSQL → {host}:{port}/{db}")
        return engine
    except SQLAlchemyError as e:
        print(f"\n  ✗ Could not connect to PostgreSQL:\n    {e}")
        print("\n  Check your credentials in .env or at the top of this script.")
        sys.exit(1)


def create_tables(engine) -> None:
    """Drops and recreates all 5 FoodFlash tables in PostgreSQL."""
    ddl = """
        DROP TABLE IF EXISTS order_items CASCADE;
        DROP TABLE IF EXISTS orders CASCADE;
        DROP TABLE IF EXISTS riders CASCADE;
        DROP TABLE IF EXISTS restaurants CASCADE;
        DROP TABLE IF EXISTS customers CASCADE;

        CREATE TABLE customers (
            customer_id   VARCHAR(36) PRIMARY KEY,
            name          VARCHAR(120) NOT NULL,
            email         VARCHAR(200) NOT NULL UNIQUE,
            phone         VARCHAR(20),
            city          VARCHAR(60),
            registered_at TIMESTAMP,
            loyalty_tier  VARCHAR(20)
        );

        CREATE TABLE restaurants (
            restaurant_id VARCHAR(36) PRIMARY KEY,
            name          VARCHAR(150) NOT NULL,
            city          VARCHAR(60),
            category      VARCHAR(60),
            rating        NUMERIC(3,1),
            is_premium    BOOLEAN DEFAULT FALSE,
            opened_at     DATE
        );

        CREATE TABLE riders (
            rider_id     VARCHAR(36) PRIMARY KEY,
            name         VARCHAR(120) NOT NULL,
            city         VARCHAR(60),
            vehicle_type VARCHAR(20),
            joined_at    DATE,
            is_active    BOOLEAN DEFAULT TRUE
        );

        CREATE TABLE orders (
            order_id        VARCHAR(36) PRIMARY KEY,
            customer_id     VARCHAR(36) REFERENCES customers(customer_id),
            restaurant_id   VARCHAR(36) REFERENCES restaurants(restaurant_id),
            rider_id        VARCHAR(36) REFERENCES riders(rider_id),
            status          VARCHAR(20) NOT NULL,
            order_amount    NUMERIC(10,2),
            discount_amount NUMERIC(10,2) DEFAULT 0,
            delivery_fee    NUMERIC(8,2),
            placed_at       TIMESTAMP,
            delivered_at    TIMESTAMP,
            city            VARCHAR(60),
            payment_method  VARCHAR(30)
        );

        CREATE TABLE order_items (
            item_id    VARCHAR(36) PRIMARY KEY,
            order_id   VARCHAR(36) REFERENCES orders(order_id),
            item_name  VARCHAR(120),
            quantity   INTEGER DEFAULT 1,
            unit_price NUMERIC(8,2)
        );

        -- Indexes for common query patterns
        CREATE INDEX idx_orders_customer   ON orders(customer_id);
        CREATE INDEX idx_orders_restaurant ON orders(restaurant_id);
        CREATE INDEX idx_orders_rider      ON orders(rider_id);
        CREATE INDEX idx_orders_city       ON orders(city);
        CREATE INDEX idx_orders_placed_at  ON orders(placed_at);
        CREATE INDEX idx_orders_status     ON orders(status);
        CREATE INDEX idx_items_order       ON order_items(order_id);
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(ddl))
            conn.commit()
        print("  ✓ All 5 tables created with indexes")
    except SQLAlchemyError as e:
        print(f"\n  ✗ Error creating tables:\n    {e}")
        sys.exit(1)


def load_dataframe(df: pd.DataFrame, table_name: str, engine, chunk_size: int = CHUNK_SIZE) -> None:
    """
    Loads a DataFrame into PostgreSQL in chunks.
    Shows progress as each chunk is inserted.
    """
    total_rows = len(df)
    total_chunks = math.ceil(total_rows / chunk_size)
    loaded = 0

    try:
        for chunk_num, start in enumerate(range(0, total_rows, chunk_size)):
            chunk = df.iloc[start : start + chunk_size]
            chunk.to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                index=False,
                method="multi",    # faster batch insert
            )
            loaded += len(chunk)
            print_progress(f"  loading {table_name}", loaded, total_rows)

        print(f"\n  ✓ {table_name}: {total_rows:,} rows loaded")

    except SQLAlchemyError as e:
        print(f"\n  ✗ Error loading {table_name}:\n    {e}")
        sys.exit(1)


# ─────────────────────────────────────────────
# VERIFICATION
# ─────────────────────────────────────────────

def verify_counts(engine) -> None:
    """Queries each table and prints row counts to confirm successful load."""
    tables = ["customers", "restaurants", "riders", "orders", "order_items"]
    print("\n─── Verification — row counts ───────────────────")
    with engine.connect() as conn:
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count  = result.scalar()
            status = "✓" if count > 0 else "✗"
            print(f"  {status}  {table:<20} {count:>8,} rows")
    print("─────────────────────────────────────────────────")


def verify_sample(engine) -> None:
    """Prints a sample of orders with all joined fields to confirm data quality."""
    print("\n─── Sample orders (first 3) ─────────────────────")
    query = """
        SELECT
            o.order_id,
            c.name         AS customer_name,
            r.name         AS restaurant_name,
            r.category,
            o.city,
            o.status,
            o.order_amount,
            o.discount_amount,
            o.payment_method,
            o.placed_at
        FROM orders o
        JOIN customers   c ON o.customer_id   = c.customer_id
        JOIN restaurants r ON o.restaurant_id = r.restaurant_id
        LIMIT 3;
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows   = result.fetchall()
        cols   = result.keys()
        sample_df = pd.DataFrame(rows, columns=cols)
        for col in sample_df.columns:
            print(f"  {col}: {sample_df[col].tolist()}")
    print("─────────────────────────────────────────────────")


def verify_status_distribution(engine) -> None:
    """Confirms order status distribution matches expected weights."""
    print("\n─── Order status distribution ───────────────────")
    query = """
        SELECT
            status,
            COUNT(*)                                     AS count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
        FROM orders
        GROUP BY status
        ORDER BY count DESC;
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows   = result.fetchall()
        for row in rows:
            bar = "█" * int(row[2] / 2)
            print(f"  {row[0]:<15} {row[1]:>7,} rows  ({row[2]:>5}%)  {bar}")
    print("─────────────────────────────────────────────────")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main() -> None:
    overall_start = time.time()

    print("=" * 53)
    print("  FoodFlash Analytics — Data Generation")
    print("=" * 53)

    # Seed for reproducibility
    random.seed(RANDOM_SEED)
    fake = Faker("en_IN")     # Indian locale for realistic names
    Faker.seed(RANDOM_SEED)

    # ── 1. Connect to PostgreSQL ──────────────────────
    print("\n── Connecting to PostgreSQL ──")
    engine = get_engine(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)

    # ── 2. Create tables ─────────────────────────────
    print("\n── Creating tables ──")
    create_tables(engine)

    # ── 3. Generate all data ──────────────────────────
    print("\n── Generating data ──")
    customers_df   = generate_customers(NUM_CUSTOMERS, fake)
    restaurants_df = generate_restaurants(NUM_RESTAURANTS, fake)
    riders_df      = generate_riders(NUM_RIDERS, fake)
    orders_df      = generate_orders(NUM_ORDERS, customers_df, restaurants_df, riders_df)
    order_items_df = generate_order_items(orders_df, restaurants_df)

    print(f"\n  Data generation complete.")
    print(f"  customers   : {len(customers_df):>8,} rows")
    print(f"  restaurants : {len(restaurants_df):>8,} rows")
    print(f"  riders      : {len(riders_df):>8,} rows")
    print(f"  orders      : {len(orders_df):>8,} rows")
    print(f"  order_items : {len(order_items_df):>8,} rows")

    # ── 4. Load to PostgreSQL ─────────────────────────
    print("\n── Loading to PostgreSQL ──")
    load_dataframe(customers_df,   "customers",   engine)
    load_dataframe(restaurants_df, "restaurants", engine)
    load_dataframe(riders_df,      "riders",      engine)
    load_dataframe(orders_df,      "orders",      engine)
    load_dataframe(order_items_df, "order_items", engine)

    # ── 5. Verify ─────────────────────────────────────
    print("\n── Verifying load ──")
    verify_counts(engine)
    verify_status_distribution(engine)
    verify_sample(engine)

    elapsed = time.time() - overall_start
    print(f"\n✓ All done in {elapsed:.1f}s")
    print("  Next step: run scripts/extract_load_snowflake.py")
    print("=" * 53)


if __name__ == "__main__":
    main()
