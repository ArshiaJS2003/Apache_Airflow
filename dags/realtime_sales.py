"""
Real-time Sales Data Generator DAG
Runs every 10 seconds, generating simulated sales records into DuckDB.
"""

import random
from datetime import datetime, timedelta, timezone

import duckdb

from airflow.decorators import dag, task
from airflow.timetables.trigger import DeltaTriggerTimetable

DB_PATH = "/tmp/sales_data.duckdb"

PRODUCTS = [
    ("Laptop", 799.99, 1299.99),
    ("Phone", 299.99, 999.99),
    ("Headphones", 29.99, 349.99),
    ("Tablet", 199.99, 799.99),
    ("Smartwatch", 99.99, 499.99),
    ("Monitor", 149.99, 899.99),
    ("Keyboard", 19.99, 179.99),
    ("Mouse", 9.99, 89.99),
]

REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America"]
CHANNELS = ["Online", "In-Store", "Mobile App"]


@dag(
    dag_id="realtime_sales",
    description="Generates simulated real-time sales data every 10 seconds",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=DeltaTriggerTimetable(timedelta(seconds=10)),
    catchup=False,
    max_active_runs=1,
    tags=["sales", "realtime", "demo"],
)
def realtime_sales():
    @task
    def init_database():
        """Create the sales table if it doesn't exist."""
        con = duckdb.connect(DB_PATH)
        con.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                sale_id VARCHAR,
                sale_time TIMESTAMP,
                product VARCHAR,
                price DOUBLE,
                quantity INTEGER,
                total DOUBLE,
                region VARCHAR,
                channel VARCHAR
            )
        """)
        con.close()

    @task
    def generate_sales():
        """Generate 1-3 random sales records per run."""
        con = duckdb.connect(DB_PATH)
        num_records = random.randint(1, 3)

        for _ in range(num_records):
            product, min_price, max_price = random.choice(PRODUCTS)
            price = round(random.uniform(min_price, max_price), 2)
            quantity = random.randint(1, 5)
            total = round(price * quantity, 2)
            region = random.choice(REGIONS)
            channel = random.choice(CHANNELS)
            sale_id = f"SALE-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}"

            con.execute(
                """
                INSERT INTO sales VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    sale_id,
                    datetime.now(timezone.utc),
                    product,
                    price,
                    quantity,
                    total,
                    region,
                    channel,
                ],
            )

        con.close()
        return f"Generated {num_records} sales records"

    @task
    def cleanup_old_data():
        """Keep only the last 24 hours of data to prevent unbounded growth."""
        con = duckdb.connect(DB_PATH)
        con.execute("""
            DELETE FROM sales
            WHERE sale_time < NOW() - INTERVAL '24 hours'
        """)
        con.close()

    init_database() >> generate_sales() >> cleanup_old_data()


realtime_sales()
